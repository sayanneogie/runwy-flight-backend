"use strict";
const fs=require("node:fs");
const path=require("node:path");
const crypto=require("node:crypto");
const {Client}=require("pg");
const root=path.join(__dirname,"..");
const BASELINE="20260922000000";
const signatureFile=path.join(root,"supabase/schema/pre-baseline-signature.json");

async function signature(client) {
  // Schema only: no application rows, statistics, connection strings or secrets.
  const definitions={
    columns:`select n.nspname,c.relname,a.attname,format_type(a.atttypid,a.atttypmod) as type,a.attnotnull,
      pg_get_expr(d.adbin,d.adrelid) as default_value from pg_attribute a join pg_class c on c.oid=a.attrelid
      join pg_namespace n on n.oid=c.relnamespace left join pg_attrdef d on d.adrelid=c.oid and d.adnum=a.attnum
      where n.nspname in('public','runwy_security') and c.relkind='r' and a.attnum>0 and not a.attisdropped order by 1,2,a.attnum`,
    constraints:`select n.nspname,c.relname,k.conname,pg_get_constraintdef(k.oid,true) as definition
      from pg_constraint k join pg_class c on c.oid=k.conrelid join pg_namespace n on n.oid=c.relnamespace
      where n.nspname in('public','runwy_security') order by 1,2,3`,
    indexes:`select schemaname,tablename,indexname,indexdef from pg_indexes where schemaname in('public','runwy_security') order by 1,2,3`,
    policies:`select * from pg_policies where schemaname in('public','runwy_security','storage') order by schemaname,tablename,policyname`,
    functions:`select n.nspname,p.oid::regprocedure::text as signature,pg_get_functiondef(p.oid) as definition
      from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname in('public','runwy_security')
      and p.prokind='f' and not exists(select 1 from pg_depend d where d.classid='pg_proc'::regclass and d.objid=p.oid and d.deptype='e') order by 1,2`,
  };
  const parts={};for(const [name,sql]of Object.entries(definitions))parts[name]=(await client.query(sql)).rows;
  return {sha256:crypto.createHash('sha256').update(JSON.stringify(parts)).digest('hex'),parts};
}
function migrations() {
  const files=fs.readdirSync(path.join(root,"supabase/migrations")).filter(f=>f.endsWith('.sql')).sort();
  const seen=new Set();
  for(const f of files){if(!/^\d{14}_[a-z0-9_]+\.sql$/.test(f))throw Error(`Invalid migration version: ${f}`);
    const v=f.slice(0,14);if(seen.has(v))throw Error(`Duplicate migration version: ${v}`);seen.add(v);}
  return files;
}
async function verify(client) {
  const {rows}=await client.query(`select
    not exists(select 1 from pg_tables where schemaname in('public','runwy_security') and not rowsecurity) as all_rls,
    exists(select 1 from pg_constraint where conname='user_flights_tracking_owner_fkey') as owner_fk,
    exists(select 1 from pg_publication_tables where pubname='supabase_realtime' and schemaname='public' and tablename='live_snapshots') as realtime,
    not has_table_privilege('authenticated','public.tracking_sessions','UPDATE') as operational_writes_denied,
    not has_table_privilege('anon','public.user_flights','TRUNCATE') as truncate_denied,
    not has_function_privilege('authenticated','public.runwy_accept_circle_invite(text,uuid)','EXECUTE') as invite_actor_protected,
    not exists(select 1 from pg_tables t where t.schemaname='public' and
      (has_table_privilege('anon',format('%I.%I',t.schemaname,t.tablename),'TRUNCATE')
       or has_table_privilege('authenticated',format('%I.%I',t.schemaname,t.tablename),'TRIGGER'))) as excess_grants_denied,
    not exists(select 1 from unnest(array['flight_instances','flight_instance_aliases','flight_snapshots',
      'flight_definitions','flight_events','api_usage_logs']) t(name)
      where has_table_privilege('anon','public.'||name,'SELECT')
         or has_table_privilege('authenticated','public.'||name,'SELECT')) as internal_tables_private,
    not has_table_privilege('authenticated','public.device_tokens','INSERT') as token_registration_server_only,
    not exists(select 1 from pg_constraint c join pg_namespace n on n.oid=c.connamespace
      where n.nspname in('public','runwy_security') and not c.convalidated) as all_constraints_valid,
    not exists(select 1 from public.user_flights uf left join public.tracking_sessions ts on ts.id=uf.tracking_session_id
      where uf.tracking_session_id is not null and (ts.id is null or ts.owner_user_id<>uf.user_id)) as valid_session_owners`);
  for(const [name,passed]of Object.entries(rows[0]))if(passed!==true)throw Error(`Verification failed: ${name}`);
  return rows[0];
}
async function main(){
  const args=process.argv.slice(2);const envIndex=args.indexOf('--env-file');
  require('dotenv').config({path:envIndex>=0?args[envIndex+1]:path.join(root,'.env')});
  if(!process.env.DATABASE_URL)throw Error('DATABASE_URL is required');
  const client=new Client({connectionString:process.env.DATABASE_URL,
    ssl:process.env.DATABASE_SSL==='true'?{rejectUnauthorized:process.env.DATABASE_SSL_REJECT_UNAUTHORIZED==='true'}:undefined,
    connectionTimeoutMillis:10000,statement_timeout:30000,application_name:'runwy-schema-migrations'});
  await client.connect();
  try{
    await client.query('set search_path=public,extensions');
    if(args.includes('--capture-baseline-signature')){
      const result=await signature(client);
      fs.writeFileSync(signatureFile,JSON.stringify(result,null,2)+'\n');
      console.log(JSON.stringify({capturedSchemaSignature:result.sha256}));return;
    }
    if(args.includes('--verify')){console.log(JSON.stringify(await verify(client)));return;}
    const files=migrations();
    const historyExists=(await client.query("select to_regclass('supabase_migrations.schema_migrations') is not null as present")).rows[0].present;
    if(!historyExists && args.includes('--apply'))await client.query(`create schema if not exists supabase_migrations;
      create table supabase_migrations.schema_migrations(version text primary key,statements text[],name text);`);
    const history=historyExists?(await client.query('select version,name from supabase_migrations.schema_migrations order by version')).rows:[];
    if(args.includes('--adopt-baseline')){
      if(history.some(r=>r.version===BASELINE)){console.log('Baseline already adopted');return;}
      const expected=JSON.parse(fs.readFileSync(signatureFile,'utf8'));
      await client.query('begin');
      try{
        await client.query("select pg_advisory_xact_lock(hashtextextended('runwy:schema-migrations',0))");
        const actual=await signature(client);
        if(actual.sha256!==expected.sha256)throw Error('Live schema differs from reviewed baseline; adoption stopped');
        await client.query(`create table if not exists runwy_security.legacy_migration_history
          (version text primary key,name text,statements text[],archived_at timestamptz not null default now());
          alter table runwy_security.legacy_migration_history enable row level security;
          insert into runwy_security.legacy_migration_history(version,name,statements)
            select version,name,statements from supabase_migrations.schema_migrations on conflict(version) do nothing;
          delete from supabase_migrations.schema_migrations;`);
        await client.query('insert into supabase_migrations.schema_migrations(version,name,statements) values($1,$2,$3)',
          [BASELINE,'production_baseline',[`Existing production baseline verified: ${expected.sha256}. Original migration records preserved in runwy_security.legacy_migration_history.`]]);
        await client.query('commit');console.log('Baseline adopted; original migration records preserved');
      }catch(e){await client.query('rollback');throw e;}
      return;
    }
    const known=new Set(files.map(f=>f.slice(0,14)));
    const untracked=history.filter(r=>!known.has(r.version));
    if(untracked.length)throw Error('Legacy history requires verified --adopt-baseline before forward migrations');
    const applied=new Set(history.map(r=>r.version));const pending=files.filter(f=>!applied.has(f.slice(0,14)));
    if(!args.includes('--apply')){console.log(JSON.stringify({applied:history,pending}));return;}
    for(const file of pending){
      const version=file.slice(0,14),name=file.slice(15,-4);const sql=fs.readFileSync(path.join(root,'supabase/migrations',file),'utf8');
      await client.query('begin');
      try{
        await client.query("select pg_advisory_xact_lock(hashtextextended('runwy:schema-migrations',0))");
        if(!(await client.query('select 1 from supabase_migrations.schema_migrations where version=$1',[version])).rowCount){
          await client.query(sql);
          await client.query('insert into supabase_migrations.schema_migrations(version,name,statements) values($1,$2,$3)',[version,name,[sql]]);
        }
        await client.query('commit');console.log(`Applied ${file}`);
      }catch(e){await client.query('rollback');throw e;}
    }
    console.log(JSON.stringify({verification:await verify(client)}));
  }finally{await client.end();}
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={signature,migrations,verify};
