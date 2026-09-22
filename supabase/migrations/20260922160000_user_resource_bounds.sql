-- Limits are enforced at the database boundary, including direct Data API writes.
create table runwy_security.account_resource_usage (
 user_id uuid primary key, row_count bigint not null default 0, logical_bytes bigint not null default 0
);
alter table runwy_security.account_resource_usage enable row level security;
insert into runwy_security.account_resource_usage
select user_id,count(*),sum(bytes) from (
 select user_id,octet_length(to_jsonb(t)::text) bytes from public.user_flights t union all
 select user_id,octet_length(to_jsonb(t)::text) from public.profiles t union all
 select user_id,octet_length(to_jsonb(t)::text) from public.user_settings t union all
 select user_id,octet_length(to_jsonb(t)::text) from public.ticket_souvenirs t
) t group by user_id;

create function public.runwy_valid_alert_preferences(value jsonb, detailed boolean)
returns boolean language plpgsql immutable set search_path='' as $$
declare k text; v jsonb; q record;
begin
 if value is null or jsonb_typeof(value)<>'object' or octet_length(value::text)>4096 then return false; end if;
 for k,v in select * from jsonb_each(value) loop
  if k='quietHours' and detailed then
   if jsonb_typeof(v)<>'object' then return false; end if;
   for q in select * from jsonb_each(v) loop
    if q.key not in('startHour','endHour','startMinute','endMinute') or jsonb_typeof(q.value)<>'number'
       or q.value::text !~ '^[0-9]{1,2}$' then return false; end if;
    if (q.value::text)::int > (case when q.key in('startHour','endHour') then 23 else 59 end) then return false; end if;
   end loop;
  elsif jsonb_typeof(v)<>'boolean' or (case when detailed then k not in('gateChange','delayUpdates','boardingTime','takeoffLanding','baggageClaim','inboundAircraft','flightPlans','enabled','low','medium','high','critical')
   else k not in('low','medium','high','critical') end) then return false; end if;
 end loop;
 return true;
end $$;
revoke all on function public.runwy_valid_alert_preferences(jsonb,boolean) from public,anon,authenticated;
grant execute on function public.runwy_valid_alert_preferences(jsonb,boolean) to authenticated,service_role;
alter table public.user_flights add constraint runwy_alert_preferences_shape check(public.runwy_valid_alert_preferences(alert_preferences,false));
alter table public.user_flights add constraint runwy_alert_settings_shape check(public.runwy_valid_alert_preferences(alert_settings_json,true));

create function public.runwy_guard_user_resource()
returns trigger language plpgsql security definer set search_path='' as $$
declare actor uuid; payload jsonb; n integer; counter record; size_limit integer;
begin
 actor:=case when tg_op='DELETE' then old.user_id else new.user_id end;
 perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||actor::text,0));
 if tg_op='DELETE' then return old; end if;
 payload:=to_jsonb(new);
 size_limit:=case tg_table_name when 'user_flights' then 524288 when 'ticket_souvenirs' then 131072 else 16384 end;
 if octet_length(payload::text)>size_limit then raise exception 'Record exceeds its storage limit' using errcode='23514'; end if;
 if tg_table_name='user_flights' and (length(payload->>'user_label')>200 or length(payload->>'display_flight_number')>24) then
  raise exception 'Flight label or number is too long' using errcode='23514'; end if;

 return new;
end $$;
revoke all on function public.runwy_guard_user_resource() from public,anon,authenticated;

create function public.runwy_account_resource_delta()
returns trigger language plpgsql security definer set search_path='' as $$
declare actor uuid; delta_bytes bigint; delta_rows integer; usage record; n integer; counter record;
begin
 actor:=case when tg_op='DELETE' then old.user_id else new.user_id end;
 if not exists(select 1 from auth.users where id=actor) then
  delete from runwy_security.account_resource_usage where user_id=actor; return null;
 end if;
 if current_setting('role',true)='authenticated' then
  n:=coalesce(nullif(current_setting('runwy.client_write_rows',true),''),'0')::integer+1;
  if n>1000 then raise exception 'Split synchronization into smaller batches' using errcode='PT429'; end if;
  perform set_config('runwy.client_write_rows',n::text,true);
  select * into counter from public.runwy_consume_rate_limit('db-user-write',encode(sha256(actor::text::bytea),'hex'),60000,2000);
  if counter.total_hits>2000 then raise exception 'Write quota reached; retry later' using errcode='PT429'; end if;
 end if;
 delta_rows:=case tg_op when 'INSERT' then 1 when 'DELETE' then -1 else 0 end;
 delta_bytes:=case when tg_op='DELETE' then 0 else octet_length(to_jsonb(new)::text) end
  -case when tg_op='INSERT' then 0 else octet_length(to_jsonb(old)::text) end;
 insert into runwy_security.account_resource_usage as u(user_id,row_count,logical_bytes) values(actor,delta_rows,delta_bytes)
 on conflict(user_id) do update set row_count=u.row_count+excluded.row_count,logical_bytes=u.logical_bytes+excluded.logical_bytes
 returning * into usage;
 if (delta_rows>0 and usage.row_count>10000) or (delta_bytes>0 and usage.logical_bytes>134217728) then
  raise exception 'Account storage allowance reached' using errcode='PT429'; end if;
 return null;
end $$;
revoke all on function public.runwy_account_resource_delta() from public,anon,authenticated;
do $$ declare t text; begin foreach t in array array['user_flights','profiles','user_settings','ticket_souvenirs'] loop
 execute format('create trigger a0_resource_guard before insert or update or delete on public.%I for each row execute function public.runwy_guard_user_resource()',t);
 execute format('create trigger zz_resource_usage after insert or update or delete on public.%I for each row execute function public.runwy_account_resource_delta()',t);
end loop;end $$;

-- RPCs and reads through PostgREST also have a per-user request allowance.
-- Reads cannot update counters in PostgREST's read-only transaction: limit them
-- with a bounded statement timeout; mutating REST requests consume this bucket.
create function public.runwy_data_api_guard()
returns void language plpgsql security definer set search_path='' as $$
declare counter record; actor uuid:=auth.uid(); method text:=current_setting('request.method',true);
begin
 if current_setting('role',true)<>'authenticated' or actor is null then return; end if;
 if method in('POST','PATCH','PUT','DELETE') then
  select * into counter from public.runwy_consume_rate_limit('db-user-request',encode(sha256(actor::text::bytea),'hex'),60000,120);
  if counter.total_hits>120 then raise exception 'Request quota reached; retry later' using errcode='PT429'; end if;
 end if;
end $$;
revoke all on function public.runwy_data_api_guard() from public,anon;
grant execute on function public.runwy_data_api_guard() to authenticated;
do $$ begin if exists(select 1 from pg_roles where rolname='authenticator') then
 alter role authenticator set pgrst.db_pre_request='public.runwy_data_api_guard';
end if;end $$;
notify pgrst,'reload config';
