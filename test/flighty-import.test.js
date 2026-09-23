const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createDatabase, uid, actor, admin, users } = require('./helpers/database');
const migration = fs.readFileSync(path.join(__dirname, '../supabase/migrations/20260923120000_flighty_import.sql'), 'utf8');
function input(n = 50, overrides = {}) {
  return { metadata: {
    id: uid(n), source: 'flighty', sourceType: 'import', sourceRecordID: `fixture-${n}`,
    importedAt: '2026-09-23T00:00:00Z', flightDate: '2020-09-13', departureTimeZone: 'Asia/Kolkata',
    airlineCode: 'AI', airlineAliases: ['AIC', 'AI'], airlineName: 'Air India', flightNumber: '101',
    originIATA: 'DEL', destinationIATA: 'FCO', cancelled: false,
    scheduledGateDeparture: '2020-09-13T17:25:00Z', actualGateDeparture: '2020-09-13T17:30:00Z',
    scheduledGateArrival: '2020-09-14T03:00:00Z', actualGateArrival: '2020-09-14T03:05:00Z',
    departureTerminal: '3', departureGate: '42', arrivalTerminal: '1', arrivalGate: 'B9',
    notes: 'Synthetic fixture, no personal information', tailNumber: 'TEST-ONLY', ...overrides
  }, tracked_snapshot: { provider: 'flighty', departureTimes: { scheduled: '2020-09-13T17:25:00Z' } } };
}
const save = async (db, rows) => (await db.query('select * from runwy_import_flighty_batch($1)', [JSON.stringify(rows)])).rows;

test('Flighty preserves fields, has no provider linkage, and is idempotent', async () => {
  const db = await createDatabase();
  try {
    await db.exec(migration); await users(db); await actor(db, 1);
    assert.equal((await save(db, [input()]))[0].inserted, true);
    assert.equal((await save(db, [input()]))[0].inserted, false);
    assert.equal((await save(db, [input(51)]))[0].id, uid(50));
    const rows = (await db.query('select * from user_flights')).rows;
    assert.equal(rows.length, 1); const row = rows[0];
    assert.equal(row.source_type, 'flighty'); assert.equal(row.user_id, uid(1));
    assert.equal(row.tracking_session_id, null); assert.equal(row.provider_flight_id, null);
    assert.equal(row.flight_instance_id, null); assert.equal(row.provider_name, null);
    assert.equal(row.departure_gate, '42'); assert.equal(row.arrival_terminal, '1');
    assert.equal(row.import_metadata.tailNumber, 'TEST-ONLY');
    assert.equal((await db.query('select * from tracking_sessions')).rows.length, 0);
    await actor(db, 2); assert.equal((await db.query('select * from user_flights')).rows.length, 0);
    assert.equal((await save(db, [input(52)]))[0].inserted, true);
  } finally { await db.close(); }
});

test('manual/tracked canonical match uses local date, aliases and numeric normalization without overwrite', async () => {
  const db = await createDatabase();
  try {
    await db.exec(migration); await users(db); await actor(db, 1);
    await db.query(`insert into user_flights(id,user_id,display_flight_number,origin_iata,destination_iata,
      scheduled_departure,source_type,departure_gate) values($1,$2,' aic 00101 ','DEL','FCO','2020-09-13T17:00:00Z','tracked','LIVE')`, [uid(45), uid(1)]);
    assert.equal((await save(db, [input()]))[0].id, uid(45));
    const row = (await db.query('select * from user_flights')).rows[0];
    assert.equal(row.departure_gate, 'LIVE'); assert.equal(row.source_type, 'tracked'); assert.equal(row.import_metadata, null);
    assert.equal((await save(db, [input(51, { flightDate: '2020-09-14', scheduledGateDeparture: '2020-09-14T17:25:00Z' })]))[0].inserted, true);
  } finally { await db.close(); }
});

test('batch rollback, RLS, batch bounds and date-only imports', async () => {
  const db = await createDatabase();
  try {
    await db.exec(migration); await users(db); await actor(db, 1);
    await assert.rejects(save(db, [input(), input(51, { originIATA: '????' })]));
    assert.equal((await db.query('select * from user_flights')).rows.length, 0);
    await assert.rejects(save(db, Array.from({length: 201}, (_,i) => input(100+i))));
    await assert.rejects(save(db, [input(51, { airlineAliases: ['AI', '.*'] })]));
    await save(db, [input(51, { scheduledGateDeparture: null, actualGateDeparture: null, sourceRecordID: null })]);
    const row = (await db.query('select * from user_flights')).rows[0];
    assert.equal(new Date(row.scheduled_departure).toISOString(), '2020-09-12T18:30:00.000Z');
    assert.equal(row.import_metadata.scheduledGateDeparture, null);
    await admin(db); await db.exec("select set_config('request.jwt.claim.sub','',false);set role anon");
    await assert.rejects(save(db, [input()]));
  } finally { await db.close(); }
});

test('1,200 rows import in six transactions and a replay adds none', async () => {
  const db = await createDatabase();
  try {
    await db.exec(migration); await users(db); await actor(db, 1);
    const rows = Array.from({length: 1200}, (_,i) => input(100+i, {flightNumber: String(i+1)}));
    for (let i=0;i<rows.length;i+=200) assert.equal((await save(db,rows.slice(i,i+200))).filter(r=>r.inserted).length,200);
    for (let i=0;i<rows.length;i+=200) assert.equal((await save(db,rows.slice(i,i+200))).filter(r=>r.inserted).length,0);
    assert.equal((await db.query('select count(*)::int n from user_flights')).rows[0].n,1200);
  } finally { await db.close(); }
});

test('import cannot replace an existing occurrence through the legacy archive trigger', async () => {
  const db = await createDatabase();
  try {
    await db.exec(migration); await users(db); await actor(db,1);
    await save(db,[input()]);
    // Inconsistent source Date with an identical schedule: never tombstone the
    // saved row even when the legacy minute-level index rejects this insert.
    await assert.rejects(save(db,[input(55,{flightDate:'2020-09-14'})]));
    const rows=(await db.query('select id,deleted_at from user_flights')).rows;
    assert.deepEqual(rows,[{id:uid(50),deleted_at:null}]);
  } finally { await db.close(); }
});

test('data gateway forwards only the Flighty RPC to Supabase with user credentials', async () => {
  const express=require('express'), {mountDataGateway}=require('../src/data-gateway');
  const app=express(); let calls=0;
  app.use(express.json()); app.use((req,res,next)=>{req.auth={userId:uid(1)};next();});
  const query=async(sql)=>({rows:sql.includes('runwy_consume_rate_limit')?[{total_hits:1,reset_at:new Date(Date.now()+60000)}]:[{allowed:true}]});
  mountDataGateway(app,{query,supabaseURL:'https://fixture.invalid',anonKey:'anon',secret:'s'.repeat(32),fetchImpl:async(url,options)=>{
    calls++; assert.equal(url.href,'https://fixture.invalid/rest/v1/rpc/runwy_import_flighty_batch');
    assert.equal(options.headers.authorization,'Bearer fixture-user');
    assert.equal(JSON.parse(options.body).p_flights.length,1);
    return new Response('[]',{status:200});
  }});
  const server=app.listen(0,'127.0.0.1'); await new Promise(resolve=>server.once('listening',resolve));
  try {
    const response=await fetch(`http://127.0.0.1:${server.address().port}/v1/data/rest/v1/rpc/runwy_import_flighty_batch`,{
      method:'POST',headers:{Authorization:'Bearer fixture-user','Content-Type':'application/json'},body:JSON.stringify({p_flights:[input()]})
    });
    assert.equal(response.status,200); assert.equal(calls,1);
  } finally { server.closeAllConnections(); await new Promise(resolve=>server.close(resolve)); }
});
