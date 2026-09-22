const fs = require("node:fs");
const path = require("node:path");
const { PGlite } = require("@electric-sql/pglite");

const root = path.join(__dirname, "../..");
const uid = (n) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

async function createDatabase({ migrate = true } = {}) {
  const db = new PGlite();
  await db.exec(`
    create role anon; create role authenticated; create role service_role bypassrls;
    create schema auth; create schema storage;
    create table auth.users(id uuid primary key, email text, raw_user_meta_data jsonb default '{}');
    create function auth.uid() returns uuid language sql stable as $$
      select nullif(current_setting('request.jwt.claim.sub',true),'')::uuid $$;
    create table storage.buckets(id text primary key, name text, public boolean,
      file_size_limit bigint, allowed_mime_types text[]);
    create table storage.objects(id uuid primary key default gen_random_uuid(), bucket_id text,
      name text, owner uuid, owner_id text, metadata jsonb);
    alter table storage.objects enable row level security;
    create function storage.foldername(text) returns text[] language sql immutable as $$
      select (string_to_array($1,'/'))[1:array_length(string_to_array($1,'/'),1)-1] $$;
    grant usage on schema public,auth,storage to anon,authenticated,service_role;
    revoke create on schema public from public;
    grant all on auth.users to service_role;
  `);
  await db.exec(fs.readFileSync(path.join(root, "supabase/migrations/20260922000000_production_baseline.sql"), "utf8"));
  if (migrate) {
    const files = fs.readdirSync(path.join(root,"supabase/migrations"))
      .filter(f=>/^20260922\d{6}_.*\.sql$/.test(f) && !f.startsWith('20260922000000_')).sort();
    for (const file of files) {
      try { await db.exec("begin;\n"+fs.readFileSync(path.join(root,"supabase/migrations",file),"utf8")+"\ncommit;"); }
      catch(e) { e.message = `${file}: ${e.message}`; throw e; }
    }
  }
  return db;
}
async function actor(db, n) {
  await db.exec(`reset role;select set_config('request.jwt.claim.sub','${uid(n)}',false);set role authenticated`);
}
async function admin(db) { await db.exec("reset role"); }
async function users(db, count = 3) {
  for(let i=1;i<=count;i++) await db.query("insert into auth.users(id,email) values ($1,$2)",[uid(i),`test${i}@example.invalid`]);
}
async function session(db, id=30, owner=1) {
  await db.query("insert into public.tracking_sessions(id,owner_user_id,provider,flight_number) values($1,$2,'fixture','TEST1')",[uid(id),uid(owner)]);
}
async function flight(db, {id=40,user=1,tracking=null,instance=null,visibility='private',state='upcoming',date='2030-01-01'} = {}) {
  return db.query(`insert into public.user_flights(id,user_id,tracking_session_id,flight_instance_id,
    display_flight_number,origin_iata,destination_iata,scheduled_departure,visibility,lifecycle_state)
    values($1,$2,$3,$4,'TEST1','AAA','BBB',$5,$6,$7) returning *`,[uid(id),uid(user),tracking&&uid(tracking),instance&&uid(instance),date,visibility,state]);
}
module.exports={createDatabase,uid,actor,admin,users,session,flight};
