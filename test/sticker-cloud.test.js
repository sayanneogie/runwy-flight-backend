const test=require('node:test');
const assert=require('node:assert/strict');
const {createDatabase,users,uid,actor,admin,flight}=require('./helpers/database');
const {stickerCloudAccessHandler}=require('../src/sticker-cloud');

test('free flight sync stays enabled while sticker images and decorated-ticket cloud rows require verified membership',async()=>{
 const db=await createDatabase();try{
  await users(db);
  await db.exec('grant select,insert,update,delete on storage.objects to authenticated');
  await actor(db,1);
  await flight(db);
  const upload=()=>db.query("insert into storage.objects(bucket_id,name,metadata) values('ticket-stickers',$1,'{\"size\":100}')",[uid(1)+'/sticker.png']);
  const souvenir=()=>db.query('insert into ticket_souvenirs(user_id,flight_id,flight_payload,collected_at) values($1,$2,$3,now())',[uid(1),uid(40),{}]);
  await assert.rejects(upload(),e=>e.code==='42501');
  await assert.rejects(souvenir(),e=>e.code==='42501');
  await assert.rejects(db.query("select runwy_grant_sticker_cloud_access($1,now()+interval '1 day')",[uid(1)]),e=>e.code==='42501');
  await assert.rejects(db.query("insert into runwy_security.sticker_cloud_access values($1,now()+interval '1 day')",[uid(1)]),e=>e.code==='42501');
  await admin(db);
  await db.query("select runwy_grant_sticker_cloud_access($1,now()+interval '1 day')",[uid(1)]);
  assert.equal((await db.query("select verified_until<=now()+interval '5 minutes' bounded from runwy_security.sticker_cloud_access")).rows[0].bounded,true);
  await actor(db,1);await upload();await souvenir();
  assert.equal((await db.query("select * from storage.objects where bucket_id='ticket-stickers'")).rows.length,1);
  await actor(db,2);
  assert.equal((await db.query('select * from ticket_souvenirs')).rows.length,0);
  await admin(db);await db.query("select runwy_grant_sticker_cloud_access($1,now()+interval '1 minute')",[uid(2)]);
  await actor(db,2);assert.equal((await db.query('select * from ticket_souvenirs')).rows.length,0);
  assert.equal((await db.query("select * from storage.objects where bucket_id='ticket-stickers'")).rows.length,0);
  await admin(db);await db.query("update runwy_security.sticker_cloud_access set verified_until=now()-interval '1 second' where user_id=$1",[uid(1)]);
  await actor(db,1);
  assert.equal((await db.query('select * from ticket_souvenirs')).rows.length,0);
  assert.equal((await db.query("select * from storage.objects where bucket_id='ticket-stickers'")).rows.length,0);
  await flight(db,{id:41});assert.equal((await db.query('select * from user_flights')).rows.length,2);
  await db.query("insert into storage.objects(bucket_id,name,metadata) values('profile-avatars',$1,'{\"size\":100}')",[uid(1)+'/avatar.png']);
  await admin(db);
  // Expiry hides backups without deleting the user's previously paid uploads.
  assert.equal((await db.query('select * from ticket_souvenirs')).rows.length,1);
  assert.equal((await db.query("select * from storage.objects where bucket_id='ticket-stickers'")).rows.length,1);
 }finally{await db.close();}
});

test('access endpoint trusts server membership only and never extends cached verification',async()=>{
 const calls=[];let access={paid:false,verified:true};
 const handler=stickerCloudAccessHandler({paidAccess:{membership:async()=>access},query:async(sql,params)=>{
  calls.push({sql,params});return {rows:[{expires_at:params[1]}]};
 }});
 const invoke=async()=>{const result={status:200};const res={status(n){result.status=n;return this;},json(body){result.body=body;return this;}};
  await handler({auth:{userId:uid(1)},body:{paid:true,expiresAt:'2100-01-01'}},res);return result;};
 assert.equal((await invoke()).status,403);assert.ok(calls[0].sql.startsWith('delete'));
 access={paid:false,verified:false};assert.equal((await invoke()).status,503);
 access={paid:true,verified:true,until:Date.now()+60000};
 const result=await invoke();assert.equal(result.status,200);assert.equal(result.body.allowed,true);
 assert.equal(calls.at(-1).params[1],new Date(access.until).toISOString());
 access={paid:true,verified:true,until:Date.now()-1};assert.equal((await invoke()).status,403);
});
