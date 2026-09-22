const test=require('node:test');
const assert=require('node:assert/strict');
const {createDatabase,uid,actor,admin,users,session,flight}=require('./helpers/database');

test('owner boundaries survive raw SQL, client payloads, and privileged cleanup',async()=>{
  const db=await createDatabase();
  try {
    await users(db);await session(db);await flight(db,{tracking:30});
    await db.query("insert into live_snapshots(tracking_session_id,provider) values($1,'fixture')",[uid(30)]);
    await actor(db,2);
    assert.equal((await db.query('select * from tracking_sessions')).rows.length,0);
    assert.equal((await db.query('select * from live_snapshots')).rows.length,0);
    await assert.rejects(flight(db,{id:41,user:2,tracking:30}),e=>e.code==='23503');
    await assert.rejects(flight(db,{id:42,user:2,tracking:999}),e=>e.code==='23503');
    await actor(db,1);
    await assert.rejects(db.query("update tracking_sessions set provider='client' where id=$1",[uid(30)]),e=>e.code==='42501');
    await db.query("update user_flights set provider_name='client',user_label='My flight' where id=$1",[uid(40)]);
    const saved=(await db.query('select provider_name,user_label from user_flights')).rows[0];
    assert.deepEqual(saved,{provider_name:null,user_label:'My flight'});
    await db.query("update user_flights set deleted_at=now(),lifecycle_state='deleted' where id=$1",[uid(40)]);
    assert.equal((await db.query('select session_status from tracking_sessions')).rows[0].session_status,'paused');
    await admin(db);
    await db.query('delete from tracking_sessions where id=$1',[uid(30)]);
    assert.equal((await db.query('select tracking_session_id from user_flights where id=$1',[uid(40)])).rows[0].tracking_session_id,null);
  }finally{await db.close();}
});

test('unnecessary privileges are removed and notification writes are narrow',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    const extras=await db.query(`select c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname='public' and c.relkind='r' and
      (has_table_privilege('anon',c.oid,'TRUNCATE') or has_table_privilege('authenticated',c.oid,'TRIGGER'))`);
    assert.equal(extras.rows.length,0);
    await db.query("insert into notifications(id,user_id,notification_type,title,body) values($1,$2,'fixture','Title','Body')",[uid(60),uid(1)]);
    await actor(db,1);
    await db.query('update notifications set read_at=now() where id=$1',[uid(60)]);
    await assert.rejects(db.query("update notifications set title='Forged' where id=$1",[uid(60)]),e=>e.code==='42501');
    for(const table of ['flight_instances','api_usage_logs','flight_snapshots'])await assert.rejects(db.query(`select * from ${table}`),e=>e.code==='42501');
    await admin(db);
    const publication=await db.query("select tablename from pg_publication_tables where pubname='supabase_realtime'");
    assert.deepEqual(publication.rows,[{tablename:'live_snapshots'}]);
  }finally{await db.close();}
});

test('Circle scopes, recipient preferences, revocation and invite replay are enforced',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    const hash='a'.repeat(64);
    await db.query("insert into friend_invites(inviter_user_id,token_hash) values($1,$2)",[uid(1),hash]);
    let result=(await db.query('select runwy_accept_circle_invite($1,$2) as result',[hash,uid(2)])).rows[0].result;
    const relationship=result.relationship_id;
    assert.equal(result.relationship_created,true);
    result=(await db.query('select runwy_accept_circle_invite($1,$2) as result',[hash,uid(2)])).rows[0].result;
    assert.equal(result.relationship_created,false);
    await assert.rejects(db.query('select runwy_accept_circle_invite($1,$2)',[hash,uid(3)]),e=>e.code==='22023');
    await flight(db,{id:40,visibility:'private'});
    await flight(db,{id:41,visibility:'circle',state:'active'});
    await flight(db,{id:42,visibility:'circle',state:'archived',date:'2020-01-01'});
    await flight(db,{id:43,visibility:'circle',state:'upcoming',date:'2020-01-01'});
    const allowed=async(id,alerts=false)=>(await db.query('select runwy_circle_flight_allowed($1,$2,$3) as allowed',[uid(id),uid(2),alerts])).rows[0].allowed;
    assert.equal(await allowed(40),false);
    assert.equal(await allowed(41),true);
    assert.equal(await allowed(42),false);
    assert.equal(await allowed(43),false);
    await db.query("update friend_permissions set share_scope='selected_flights' where owner_user_id=$1",[uid(1)]);
    assert.equal(await allowed(41),false);
    await actor(db,1);
    await db.query('select runwy_set_circle_flights($1,$2)',[relationship,[uid(41)]]);
    await admin(db);
    assert.equal(await allowed(41),true);
    await db.query('update friend_permissions set can_view_live=false where owner_user_id=$1',[uid(1)]);
    assert.equal(await allowed(41),false);
    await db.query("update friend_permissions set share_scope='all_flights' where owner_user_id=$1",[uid(1)]);
    assert.equal(await allowed(43),false);
    await db.query("update friend_permissions set can_view_live=true,share_scope='all_flights',can_view_history=true where owner_user_id=$1",[uid(1)]);
    assert.equal(await allowed(42),true);
    const overview=(await db.query('select runwy_circle_overview($1,1,0) as data',[uid(2)])).rows[0].data;
    assert.equal(overview.total_shared_flights,3);assert.equal(overview.has_more,true);
    assert.equal(overview.members[0].live_flight_count,1);
    assert.equal(overview.shared_flights.length,1);
    await actor(db,3);
    await assert.rejects(db.query('select runwy_remove_circle_member($1)',[relationship]),e=>e.code==='42501');
    await actor(db,2);
    await db.query('select runwy_update_circle_notifications($1,$2)',[relationship,JSON.stringify({enabled:false})]);
    await admin(db);
    assert.equal((await db.query("select runwy_circle_alert_allowed($1,$2,'GATE_CHANGED') as allowed",[relationship,uid(2)])).rows[0].allowed,false);
    await actor(db,2);
    await db.query('select runwy_remove_circle_member($1)',[relationship]);
    await admin(db);assert.equal(await allowed(41),false);
    await assert.rejects(db.query('select runwy_accept_circle_invite($1,$2)',[hash,uid(2)]),e=>e.code==='22023');
  }finally{await db.close();}
});

test('canonical linking merges the losing occurrence and old-device retries stay deduplicated',async()=>{
  const db=await createDatabase();
  try {
    await users(db);await session(db);
    await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date) values($1,'fixture','TT','1','2030-01-01')",[uid(50)]);
    await flight(db,{id:40,tracking:30,instance:50});
    await flight(db,{id:41});
    const resolve=()=>db.query('select * from runwy_link_user_flight($1,$2,$3,$4)',[uid(1),uid(41),uid(50),JSON.stringify({notificationEnabled:false})]);
    const linked=(await resolve()).rows[0];assert.equal(linked.id,uid(40));assert.equal(linked.notification_enabled,false);
    assert.equal((await resolve()).rows[0].id,uid(40));
    assert.equal((await db.query("select count(*)::int n from user_flights where deleted_at is null")).rows[0].n,1);
    await actor(db,1);
    await db.query('update user_flights set deleted_at=null,lifecycle_state=\'upcoming\' where id=$1',[uid(41)]);
    assert.equal((await db.query("select count(*)::int n from user_flights where deleted_at is null")).rows[0].n,1);
    await admin(db);
    await assert.rejects(db.query('delete from flight_instances where id=$1',[uid(50)]),e=>['23503','23001'].includes(e.code));
  }finally{await db.close();}
});

test('push registration rotates and reassigns a token atomically across compatibility stores',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    const register=(user,token)=>db.query("select * from runwy_register_push_device($1,$2,'device-1','ios','production')",[uid(user),token]);
    await register(1,'a'.repeat(64));await register(1,'b'.repeat(64));await register(2,'b'.repeat(64));
    const active=(await db.query('select user_id,device_token from device_tokens where is_active')).rows;
    assert.deepEqual(active,[{user_id:uid(2),device_token:'b'.repeat(64)}]);
    assert.deepEqual((await db.query('select user_id,apns_token from push_devices where push_enabled')).rows,[{user_id:uid(2),apns_token:'b'.repeat(64)}]);
    await db.query("select runwy_disable_push_device($1,'device-1',null)",[uid(2)]);
    assert.equal((await db.query('select count(*)::int n from device_tokens where is_active')).rows[0].n,0);
    assert.equal((await db.query('select count(*)::int n from push_devices where push_enabled')).rows[0].n,0);
  }finally{await db.close();}
});

test('retention preserves user records and the last finalized snapshot',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date,is_final) values($1,'fixture','TT','1','2020-01-01',true)",[uid(50)]);
    await flight(db,{instance:50});
    await db.query("insert into flight_snapshots(flight_instance_id,created_at) values($1,now()-interval '101 days'),($1,now()-interval '100 days'),($1,now())",[uid(50)]);
    await db.query('select runwy_prune_operational_history(500)');
    assert.equal((await db.query('select count(*)::int n from flight_snapshots')).rows[0].n,1);
    assert.equal((await db.query('select count(*)::int n from user_flights')).rows[0].n,1);
    await db.query("update flight_snapshots set created_at=now()-interval '200 days'");
    await db.query('select runwy_prune_operational_history(500)');
    assert.equal((await db.query('select count(*)::int n from flight_snapshots')).rows[0].n,1);
  }finally{await db.close();}
});

test('account deletion requires storage cleanup and blocks uploads after deletion begins',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    await db.query("insert into storage.objects(bucket_id,name,owner_id) values('ticket-stickers',$1,$2)",[uid(1)+'/fixture.png',uid(1)]);
    await actor(db,1);
    await assert.rejects(db.query('select delete_current_user_account()'),e=>e.code==='23514');
    await assert.rejects(db.query('select runwy_account_deletion_batch($1)',[uid(2)]),e=>e.code==='42501');
    await admin(db);
    const batch=(await db.query('select runwy_account_deletion_batch($1) as objects',[uid(1)])).rows[0].objects;
    assert.equal(batch.length,1);
    await actor(db,1);
    assert.equal((await db.query('select runwy_account_accepts_storage() as allowed')).rows[0].allowed,false);
    await admin(db);
    // Synthetic fixture: stand in for successful Storage API deletion only.
    await db.query('delete from storage.objects where owner_id=$1',[uid(1)]);
    assert.equal((await db.query('select runwy_finish_account_deletion($1) as deleted',[uid(1)])).rows[0].deleted,true);
    assert.equal((await db.query('select count(*)::int n from auth.users where id=$1',[uid(2)])).rows[0].n,1);
    assert.equal((await db.query('select status from runwy_security.account_deletion_jobs where user_id=$1',[uid(1)])).rows[0].status,'complete');
  }finally{await db.close();}
});

test('migration versions and schema reproduction are deterministic',async()=>{
  const {migrations,verify}=require('../scripts/db-migrate');
  const files=migrations();assert.equal(files.length,14);
  assert.equal(files[0],'20260922000000_production_baseline.sql');
  const db=await createDatabase();
  try{assert.ok(Object.values(await verify(db)).every(Boolean));}finally{await db.close();}
});

test('tracking persistence reconciles both unique identities without losing a saved trip',async()=>{
  const db=await createDatabase();
  try {
    await users(db);await session(db);
    await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date) values($1,'fixture','TT','1','2030-01-01')",[uid(50)]);
    await flight(db,{id:40,instance:50});
    await db.query("update user_flights set source_type='tracked' where id=$1",[uid(40)]);
    await flight(db,{id:41,tracking:30});
    await db.query("update user_flights set source_type='trip',user_label='My trip' where id=$1",[uid(41)]);
    const {createTrackingStore}=require('../src/tracking-store');
    const query=(sql,params)=>db.query(sql,params);
    const store=createTrackingStore({pool:{query,connect:async()=>({query,release(){}})},
      normalizeFlightCode:v=>String(v||'').toUpperCase(),normalizeAirportCode:v=>v||null,
      displayFlightCode:n=>n.flightNumber});
    const input={userId:uid(1),flightId:uid(30),query:{date:'2030-01-01',flightNumber:'TT1'},
      normalized:{flightInstanceId:uid(50),flightNumber:'TT1',airlineCode:'TT',stateRevision:5,livePosition:{latitude:12,longitude:34},trackPoints:[{latitude:12}],rawProviderPayload:{secret:'fixture'},
        departureAirportIata:'AAA',arrivalAirportIata:'BBB',status:'scheduled',
        departureTimes:{scheduled:'2030-01-01T10:00:00Z'},arrivalTimes:{scheduled:'2030-01-01T12:00:00Z'}},
      provider:'fixture',providerFlightId:'fixture-1',rawProviderPayload:{}};
    await store.persistTrackingSnapshot(input);await store.persistTrackingSnapshot(input);
    await store.persistTrackingSnapshot({...input,normalized:{...input.normalized,stateRevision:4,livePosition:{latitude:99}}});
    const full=(await db.query('select canonical_snapshot_json from runwy_security.live_snapshot_payloads where tracking_session_id=$1',[uid(30)])).rows[0].canonical_snapshot_json;
    assert.equal(full.livePosition.latitude,12);
    await actor(db,1);
    const basic=(await db.query('select canonical_snapshot_json,raw_provider_payload_json from live_snapshots')).rows[0];
    assert.equal(basic.canonical_snapshot_json.livePosition,null);assert.deepEqual(basic.canonical_snapshot_json.trackPoints,[]);
    assert.deepEqual(basic.raw_provider_payload_json,{});
    await assert.rejects(db.query('select * from runwy_security.live_snapshot_payloads'),e=>e.code==='42501');
    await admin(db);
    assert.deepEqual((await db.query("select id,tracking_session_id,source_type,user_label from user_flights where deleted_at is null")).rows,
      [{id:uid(40),tracking_session_id:uid(30),source_type:'trip',user_label:'My trip'}]);
    assert.equal((await db.query('select canonical_id from user_flight_aliases where alias_id=$1',[uid(41)])).rows[0].canonical_id,uid(40));
  }finally{await db.close();}
});

test('queued Circle deliveries lose authorization when a recipient mutes or a relationship is removed',async()=>{
  const db=await createDatabase();
  try {
    await users(db);
    await db.query("insert into friend_invites(inviter_user_id,token_hash) values($1,$2)",[uid(1),'b'.repeat(64)]);
    const relationship=(await db.query('select runwy_accept_circle_invite($1,$2) r',['b'.repeat(64),uid(2)])).rows[0].r.relationship_id;
    await flight(db,{visibility:'circle',state:'active'});
    await db.query("insert into flight_events(id,event_type) values($1,'DEPARTED')",[uid(80)]);
    await db.query('insert into notification_deliveries(id,user_id,user_flight_id,flight_event_id) values($1,$2,$3,$4)',[uid(81),uid(2),uid(40),uid(80)]);
    const allowed=async()=>{await admin(db);return (await db.query('select runwy_notification_delivery_allowed($1) allowed',[uid(81)])).rows[0].allowed;};
    assert.equal(await allowed(),true);
    await actor(db,2);await db.query('select runwy_update_circle_notifications($1,$2)',[relationship,{enabled:false}]);
    assert.equal(await allowed(),false);
    await actor(db,2);await db.query('select runwy_update_circle_notifications($1,$2)',[relationship,{enabled:true}]);
    assert.equal(await allowed(),true);
    await actor(db,1);await db.query('select runwy_remove_circle_member($1)',[relationship]);
    assert.equal(await allowed(),false);
  }finally{await db.close();}
});
