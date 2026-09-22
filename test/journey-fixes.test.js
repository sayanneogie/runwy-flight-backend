const test=require('node:test'),assert=require('node:assert/strict');
const {createDatabase,uid,users,actor,admin,session}=require('./helpers/database');
const {createSharedFlightQueue}=require('../src/shared-flight/queue');
const {createMemoryRedis}=require('../src/shared-flight/cache');
const {createMemorySharedFlightRepository,createPostgresSharedFlightRepository}=require('../src/shared-flight/repository');
const {createSharedFlightService}=require('../src/shared-flight/service');
const pause=ms=>new Promise(r=>setTimeout(r,ms));
test('queue bounds concurrency, drains failures, preserves deduplication',async()=>{
 const queue=createSharedFlightQueue({concurrency:3,maxPendingJobs:10,onError(){}});let active=0,peak=0,done=0,release;const gate=new Promise(r=>release=r);
 queue.process('test',async job=>{peak=Math.max(peak,++active);await gate;active--;done++;if(job.data.n===2)throw Error('fixture')});
 for(let n=0;n<10;n++)await queue.add('test',{n},{dedupe:true});assert.equal((await queue.add('test',{n:1},{dedupe:true})).deduped,true);
 await assert.rejects(queue.add('test',{n:11}),/queue is full/);await pause(10);assert.equal(peak,3);release();await pause(20);assert.equal(done,10);assert.equal(queue.jobs.length,0);queue.close();
});
test('cache expires unused entries and preserves locks at capacity',async()=>{
 const cache=createMemoryRedis({maxEntries:2,sweepIntervalMs:5});try{await cache.set('lock','token',{px:1000,nx:true});await cache.set('old','data',{px:10});await assert.rejects(cache.set('third','x'),/capacity/);await pause(30);assert.equal(cache.__values.size,1);assert.equal(await cache.get('lock'),'token');await cache.set('third','x');}finally{cache.close()}
});
test('foreign viewer and rejected reservations perform no provider work',async()=>{
 const repository=createMemorySharedFlightRepository();let calls=0,jobs=0;const service=createSharedFlightService({repository,provider:{name:'fixture',fetchFlightByNumber:async()=>{calls++;throw Error('unexpected')}},queue:{process(){},async add(){jobs++;}}});
 repository.__memory.flights.set('x',{id:'foreign',fresh_until:'2000-01-01'});assert.equal(await service.registerActiveViewer('attacker','foreign'),null);assert.equal(jobs,0);
 repository.reserveFlightWork=async()=>{throw Object.assign(Error('quota'),{code:'PT429'})};await assert.rejects(service.saveUserFlight('user',{airline:'TT',number:'1',date:'2030-01-01',origin:'AAA',destination:'BBB'}),{code:'PT429'});
 repository.__memory.userFlights.set('manual',{id:'manual',user_id:'user',display_flight_number:'TT1',scheduled_departure:'2030-01-01',origin_iata:'AAA',destination_iata:'BBB',lifecycle_state:'upcoming'});assert.equal((await service.ensureUserFlightLiveCoverage('user',{ids:['manual']})).covered,0);assert.equal(calls,0);service.cache.redis.close?.();
});
test('sticker CAS rejects equal revisions and disregards device clocks',async()=>{
 const db=await createDatabase();try{await users(db);await db.query("select runwy_grant_sticker_cloud_access($1,now()+interval '5 minutes')",[uid(1)]);await actor(db,1);
 await db.query("insert into ticket_souvenirs(user_id,flight_id,flight_payload,collected_at,updated_at,revision) values($1,$2,'{\"edit\":\"A\"}',now(),'2099-01-01',1)",[uid(1),uid(90)]);
 assert.ok(new Date((await db.query('select updated_at from ticket_souvenirs')).rows[0].updated_at).getFullYear()<2099);
 await assert.rejects(db.query("update ticket_souvenirs set flight_payload='{}',revision=1"),{code:'PT409'});
 await db.query("update ticket_souvenirs set flight_payload='{\"edit\":\"B\"}',revision=2,updated_at='1900-01-01'");await assert.rejects(db.query("update ticket_souvenirs set flight_payload='{}',revision=2"),{code:'PT409'});
 assert.equal((await db.query('select flight_payload from ticket_souvenirs')).rows[0].flight_payload.edit,'B');
 await db.query("insert into ticket_souvenirs(user_id,flight_id,flight_payload,collected_at,revision) values($1,$2,'{}',now(),3) on conflict(user_id,flight_id) do update set revision=excluded.revision,flight_payload=excluded.flight_payload",[uid(1),uid(90)]);assert.equal(Number((await db.query('select revision from ticket_souvenirs')).rows[0].revision),3);
 }finally{await db.close()}
});
test('Live Activity DB cap permits idempotent replacement',async()=>{
 const db=await createDatabase();try{await users(db);await session(db);await db.query("insert into flight_instances(id,flight_key,airline_code,flight_number,departure_date) values($1,'f','TT','1','2030-01-01')",[uid(50)]);
 const put=n=>db.query("insert into live_activity_tokens(user_id,flight_instance_id,activity_id,push_token,environment) values($1,$2,$3,repeat('a',64),'production') on conflict(user_id,activity_id) do update set push_token=excluded.push_token",[uid(1),uid(50),'a'+n]);for(let n=0;n<4;n++)await put(n);await put(0);await assert.rejects(put(4),{code:'PT429'});assert.equal((await db.query('select * from live_activity_tokens')).rows.length,4);
 }finally{await db.close()}
});
test('work reservations bound simultaneous distinct flights and release/expire',async()=>{
 const db=await createDatabase();try{await users(db);const repo=createPostgresSharedFlightRepository({query:(s,p)=>db.query(s,p)}),input={airline:'TT',number:'1',date:'2030-01-01',origin:'AAA',destination:'BBB'};
 const token=await repo.reserveFlightWork(uid(1),input);await assert.rejects(repo.reserveFlightWork(uid(1),input),{code:'PT429'});await repo.releaseFlightWork(uid(1),token);await repo.reserveFlightWork(uid(1),input);
 await db.exec("update runwy_security.flight_work_reservations set expires_at=now()-interval '1 second'");await repo.reserveFlightWork(uid(1),input);await db.exec('delete from runwy_security.flight_work_reservations');for(let i=0;i<20;i++)await repo.reserveFlightWork(uid(1),{...input,number:String(i+1)});await assert.rejects(repo.reserveFlightWork(uid(1),{...input,number:'99'}),{code:'PT429'});
 }finally{await db.close()}
});
test('required gateway blocks direct reads and forged headers in read-only transactions',async()=>{
 const db=await createDatabase();try{await users(db);await db.query("update runwy_security.data_gateway_config set required=true,secret_hash=encode(sha256('fixture-secret'::bytea),'hex')");await actor(db,1);await db.exec("select set_config('request.method','GET',false)");await assert.rejects(db.query('select runwy_data_api_guard()'),{code:'PT403'});
 await db.exec(`select set_config('request.headers','{"x-runwy-data-gateway":"forged"}',false)`);await assert.rejects(db.query('select runwy_data_api_guard()'),{code:'PT403'});
 await db.exec(`select set_config('request.headers','{"x-runwy-data-gateway":"fixture-secret"}',false);begin read only;select runwy_data_api_guard();rollback`);
 await admin(db);for(let i=0;i<8;i++)assert.equal((await db.query('select runwy_charge_data_bytes($1,8388608) allowed',[uid(1)])).rows[0].allowed,true);assert.equal((await db.query('select runwy_charge_data_bytes($1,1) allowed',[uid(1)])).rows[0].allowed,false);
 }finally{await db.close()}
});
test('gateway charges failed upstream attempts and only forwards user credentials',async()=>{
 const express=require('express'),{mountDataGateway}=require('../src/data-gateway'),app=express();let calls=0;const counts=new Map();app.use(express.json());app.use((req,res,next)=>{req.auth={userId:uid(1)};next()});
 const query=async(sql,p)=>{if(sql.includes('runwy_consume_rate_limit')){const n=(counts.get(p[0])||0)+1;counts.set(p[0],n);return{rows:[{total_hits:n,reset_at:new Date(Date.now()+60000)}]}}return{rows:[{allowed:true}]}};
 mountDataGateway(app,{query,supabaseURL:'https://fixture.invalid',anonKey:'anon',secret:'s'.repeat(32),fetchImpl:async(url,opts)=>{calls++;assert.equal(url.origin,'https://fixture.invalid');assert.equal(opts.headers.authorization,'Bearer user-token');assert.equal(opts.headers['x-runwy-data-gateway'],'s'.repeat(32));return new Response('{"error":"constraint"}',{status:400})}});
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));const base='http://127.0.0.1:'+server.address().port;
 try{for(let i=0;i<122;i++){const r=await fetch(base+'/v1/data/rest/v1/user_flights',{method:'POST',headers:{Authorization:'Bearer user-token','Content-Type':'application/json','x-runwy-data-gateway':'forged'},body:'{}'});await r.text();assert.equal(r.status,i<120?400:429)}assert.equal(calls,120);assert.equal((await fetch(base+'/v1/data/rest/v1/rpc/runwy_consume_rate_limit')).status,404);
 }finally{server.closeAllConnections();await new Promise(r=>server.close(r))}
});
