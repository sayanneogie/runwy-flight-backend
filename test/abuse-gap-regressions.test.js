const test=require('node:test');
const assert=require('node:assert/strict');
const {createDatabase,uid,users,actor,admin,flight,session}=require('./helpers/database');
const {createFlightWeatherService}=require('../src/shared-flight/weather');
const {createFlightCache,createMemoryRedis}=require('../src/shared-flight/cache');
const {createPostgresSharedFlightRepository}=require('../src/shared-flight/repository');
const {PostgresRateLimitStore}=require('../src/request-protection');
const {validAlertPreferences}=require('../src/shared-flight/routes');

test('direct SQL rejects oversized records, bulk writes and aggregate growth, while deletes release usage',async()=>{
 const db=await createDatabase();try{
  await users(db);await actor(db,1);await flight(db);
  await assert.rejects(db.query('update user_flights set user_label=$1 where id=$2',['x'.repeat(1048576),uid(40)]),e=>e.code==='23514');
  await assert.rejects(db.query(`insert into user_flights(user_id,display_flight_number,origin_iata,destination_iata,scheduled_departure)
    select $1,'TT1','AAA','BBB','2030-01-01'::timestamptz from generate_series(1,1500)`,[uid(1)]),e=>e.code==='PT429');
  assert.equal((await db.query('select count(*)::int n from user_flights')).rows[0].n,1);
  await admin(db);
  const before=(await db.query('select * from runwy_security.account_resource_usage where user_id=$1',[uid(1)])).rows[0];
  await db.query('update runwy_security.account_resource_usage set row_count=10000 where user_id=$1',[uid(1)]);
  await actor(db,1);await assert.rejects(flight(db,{id:41}),e=>e.code==='PT429');
  await admin(db);await db.query('update runwy_security.account_resource_usage set row_count=$2 where user_id=$1',[uid(1),before.row_count]);
  await db.query('delete from user_flights where id=$1',[uid(40)]);
  const after=(await db.query('select * from runwy_security.account_resource_usage where user_id=$1',[uid(1)])).rows[0];
  assert.equal(Number(after.row_count),Number(before.row_count)-1);assert.ok(Number(after.logical_bytes)<Number(before.logical_bytes));
 }finally{await db.close();}
});

test('malformed alert JSON is rejected, including injected casts and invalid quiet hours',async()=>{
 const db=await createDatabase();try{
  await users(db);
  await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date) values($1,'fixture','TT','1','2030-01-01')",[uid(50)]);
  await flight(db,{instance:50});await flight(db,{id:41,user:2,instance:50});await actor(db,1);
  for(const value of [{high:'not-a-boolean'},{high:{}},{quietHours:{startHour:'1);drop table user_flights;--'}},{quietHours:{startHour:24}}]){
   assert.equal(validAlertPreferences(value,true),false);
   await assert.rejects(db.query('update user_flights set alert_settings_json=$1 where id=$2',[JSON.stringify(value),uid(40)]),e=>e.code==='23514');
  }
  assert.equal(validAlertPreferences({inboundAircraft:true,flightPlans:false,quietHours:{startHour:22,endHour:7}},true),true);
  await admin(db);await db.exec('alter table user_flights drop constraint runwy_alert_preferences_shape; alter table user_flights drop constraint runwy_alert_settings_shape');
  await db.query(`update user_flights set alert_preferences='{"high":"not-a-boolean"}',alert_settings_json='{"gateChange":"not-a-boolean"}' where id=$1`,[uid(40)]);
  const repo=createPostgresSharedFlightRepository({query:(s,p)=>db.query(s,p)});
  const targets=await repo.listNotificationTargets(uid(50),'high','GATE_CHANGED');
  assert.equal(targets.length,1);assert.equal(targets[0].userFlight.user_id,uid(2));
 }finally{await db.close();}
});

test('tracking capacity reservation is atomic and paused active bridges still consume capacity',async()=>{
 const db=await createDatabase();try{
  await users(db);
  const reserve=i=>db.query(`select runwy_create_tracking_session($1,'fixture',null,$2,'TT','AAA','BBB','2030-01-01','test','{}',2) id`,[uid(1),'TT'+i]);
  const results=await Promise.allSettled(Array.from({length:8},(_,i)=>reserve(i)));
  assert.equal(results.filter(x=>x.status==='fulfilled').length,2);
  assert.ok(results.filter(x=>x.status==='rejected').every(x=>x.reason.code==='PT429'));
  const first=results[0].value.rows[0].id;
  assert.equal((await reserve(0)).rows[0].id,first);
  await db.query(`insert into user_flights(user_id,tracking_session_id,display_flight_number,origin_iata,destination_iata,scheduled_departure)
    values($1,$2,'TT0','AAA','BBB','2030-01-01')`,[uid(1),first]);
  await db.query("update tracking_sessions set session_status='paused' where id=$1",[first]);
  await assert.rejects(reserve(99),e=>e.code==='PT429');
 }finally{await db.close();}
});

test('token registration requires device identity, serializes per-account caps and permits rotation at cap',async()=>{
 const db=await createDatabase();try{
  await users(db);
  const register=(i,device='device-'+i)=>db.query("select runwy_register_push_device($1,$2,$3,'ios','production')",[uid(1),i.toString(16).padStart(64,'0'),device]);
  await assert.rejects(register(1,null),e=>e.code==='22023');
  const result=await Promise.allSettled(Array.from({length:25},(_,i)=>register(i)));
  assert.equal(result.filter(x=>x.status==='fulfilled').length,20);
  await register(99,'device-0');
  assert.equal((await db.query('select count(*)::int n from device_tokens where is_active')).rows[0].n,20);
  await db.exec("alter table device_tokens disable trigger user;alter table push_devices disable trigger user;update device_tokens set updated_at=now()-interval '91 days';update push_devices set updated_at=now()-interval '91 days';alter table device_tokens enable trigger user;alter table push_devices enable trigger user");
  await register(100);
  assert.equal((await db.query('select count(*)::int n from device_tokens where is_active')).rows[0].n,1);
 }finally{await db.close();}
});

test('weather coalesces bursts, shares observations without sharing flight summaries, and backs off failures',async()=>{
 let calls=0,logs=0;let fail=false;
 const weather=createFlightWeatherService({cache:createFlightCache(createMemoryRedis()),repository:{logApiUsage:async()=>logs++},
 weatherProvider:{name:'fixture',isConfigured:()=>true,fetchWeather:async()=>{calls++;await new Promise(r=>setTimeout(r,15));if(fail)throw Error('offline');return {};}}});
 const row={origin_airport:'DEL',status:'scheduled',airline_code:'TT',flight_number:'1',scheduled_departure_at:new Date(Date.now()+3600000).toISOString()};
 const result=await Promise.all(Array.from({length:20},()=>weather.insightForFlight(row)));
 assert.ok(result.every(x=>x.available));assert.equal(calls,1);assert.equal(logs,1);
 assert.match((await weather.insightForFlight({...row,flight_number:'2'})).summary,/TT2/);
 fail=true;const other={...row,origin_airport:'BOM'};
 await weather.insightForFlight(other);await weather.insightForFlight(other);assert.equal(calls,2);
});

test('weather budgets are shared and no weather work is done after budget denial',async()=>{
 const db=await createDatabase();try{
  const results=await Promise.all(Array.from({length:20},()=>db.query('select runwy_reserve_weather_budget(2) allowed')));
  assert.equal(results.filter(x=>x.rows[0].allowed).length,2);
  let calls=0;
  const weather=createFlightWeatherService({repository:{reserveWeatherBudget:async()=>false},weatherProvider:{isConfigured:()=>true,fetchWeather:async()=>calls++}});
  const value=await weather.insightForFlight({origin_airport:'DEL',status:'scheduled',scheduled_departure_at:new Date(Date.now()+3600000).toISOString()});
  assert.equal(value.reason,'daily_budget_reached');assert.equal(calls,0);
 }finally{await db.close();}
});

test('blocked request bursts reuse a bounded local denial instead of writing the DB for each retry',async()=>{
 let calls=0;const store=new PostgresRateLimitStore(async()=>{calls++;return {rows:[{total_hits:6,reset_at:new Date(Date.now()+60000)}]};},'test',5);
 store.init({windowMs:60000});for(let i=0;i<250;i++)assert.equal((await store.increment('user')).totalHits,6);
 assert.equal(calls,1);
});

test('storage allowance counts all private buckets together and blocks excess bytes and object counts',async()=>{
 const db=await createDatabase();try{
  await users(db);
  const insert=(name,bucket,size)=>db.query('insert into storage.objects(bucket_id,name,metadata) values($1,$2,$3)',[bucket,uid(1)+'/'+name,{size}]);
  await insert('one','profile-avatars',134217728);await insert('two','ticket-stickers',134217728);
  await assert.rejects(insert('three','ticket-stickers',1),e=>e.code==='PT429');
  await db.exec('delete from storage.objects;alter table storage.objects disable trigger user');
  await db.query("insert into storage.objects(bucket_id,name,metadata) select 'ticket-stickers',$1||'/'||i,'{}'::jsonb from generate_series(1,2000) i",[uid(1)]);
  await db.exec('alter table storage.objects enable trigger user');
  await assert.rejects(insert('next','profile-avatars',1),e=>e.code==='PT429');
 }finally{await db.close();}
});

test('canonical subscription cap cannot be bypassed with client-supplied past dates or archived labels',async()=>{
 const db=await createDatabase();try{
  await users(db);
  for(let i=0;i<21;i++){
   const id=uid(100+i);
   await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date) values($1,$2,'TT',$3,'2030-01-01')",[id,'cap-'+i,String(i)]);
   const save=()=>db.query(`insert into user_flights(user_id,flight_instance_id,display_flight_number,origin_iata,destination_iata,scheduled_departure,lifecycle_state)
    values($1,$2,$3,'AAA','BBB','2020-01-01','archived')`,[uid(1),id,'TT'+i]);
   if(i<20)await save();else await assert.rejects(save(),e=>e.code==='PT429');
  }
 }finally{await db.close();}
});
