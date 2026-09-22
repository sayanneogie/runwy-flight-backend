-- Server-owned revisions: clock skew cannot pin records or choose a winner.
alter table public.ticket_souvenirs add column revision bigint not null default 0 check (revision>=0);
create or replace function public.prevent_stale_ticket_souvenir_write() returns trigger
language plpgsql set search_path='' as $$
begin
 if tg_op='UPDATE' and new.revision<>old.revision+1 then
  raise exception 'Sticker changed on another device; refresh before saving' using errcode='PT409';
 end if;
 if tg_op='INSERT' and new.revision not in (0,1) then
  -- INSERT ... ON CONFLICT runs this trigger before the update trigger;
  -- the latter performs the authoritative revision check for existing rows.
  if not exists(select 1 from public.ticket_souvenirs where user_id=new.user_id and flight_id=new.flight_id) then
   raise exception 'Invalid initial sticker revision' using errcode='PT409';
  end if;
 end if;
 new.updated_at:=clock_timestamp();
 return new;
end $$;
-- Existing trigger was UPDATE-only; include INSERT to stop future clock poisoning.
drop trigger if exists prevent_stale_ticket_souvenir_write on public.ticket_souvenirs;
do $$ declare t record; begin
 for t in select tgname from pg_trigger where tgrelid='public.ticket_souvenirs'::regclass and tgfoid='public.prevent_stale_ticket_souvenir_write()'::regprocedure loop
 execute format('drop trigger %I on public.ticket_souvenirs',t.tgname);
 end loop;
end $$;
create trigger souvenir_revision before insert or update on public.ticket_souvenirs
for each row execute function public.prevent_stale_ticket_souvenir_write();
-- Repair old client clock values without changing content.
update public.ticket_souvenirs set revision=revision+1,updated_at=now();

create function public.runwy_bound_live_activity_tokens() returns trigger
language plpgsql security definer set search_path='' as $$
begin
 if tg_op='UPDATE' and not new.is_active then return new; end if;
 perform pg_advisory_xact_lock(hashtextextended('runwy:live-activities:'||new.user_id::text,0));
 if length(new.activity_id)>256 or length(new.push_token)>512 or length(coalesce(new.last_content_phase,''))>64 then
  raise exception 'Invalid Live Activity token' using errcode='23514';
 end if;
 delete from public.live_activity_tokens where user_id=new.user_id and id<>new.id and activity_id<>new.activity_id
 and updated_at<now()-interval '30 days';
 if tg_op='INSERT' then
  update public.live_activity_tokens lat set is_active=false
   where lat.user_id=new.user_id and lat.is_active and exists(select 1 from public.flight_instances fi where fi.id=lat.flight_instance_id and fi.is_final);
  -- Keep at most 44 inactive history records alongside the 20 active slots.
  delete from public.live_activity_tokens where id in (
   select id from public.live_activity_tokens where user_id=new.user_id and not is_active and activity_id<>new.activity_id
   order by updated_at desc offset 43
  );
 end if;
 if (select count(*) from public.live_activity_tokens where user_id=new.user_id and id<>new.id and activity_id<>new.activity_id)>=64 then
  raise exception 'Live Activity record limit reached' using errcode='PT429';
 end if;
 if new.is_active and ((select count(*) from public.live_activity_tokens where user_id=new.user_id and id<>new.id and activity_id<>new.activity_id and is_active)>=20
 or (select count(*) from public.live_activity_tokens where user_id=new.user_id and flight_instance_id=new.flight_instance_id and id<>new.id and activity_id<>new.activity_id and is_active)>=4) then
  raise exception 'Live Activity device limit reached' using errcode='PT429';
 end if;
 return new;
end $$;
revoke all on function public.runwy_bound_live_activity_tokens() from public,anon,authenticated;
create trigger bound_live_activity_tokens before insert or update on public.live_activity_tokens
for each row execute function public.runwy_bound_live_activity_tokens();

create table runwy_security.flight_work_reservations (
 token uuid primary key default gen_random_uuid(),user_id uuid not null references auth.users(id) on delete cascade,
 work_key text not null, tracking boolean not null, expires_at timestamptz not null,
 unique(user_id,work_key)
);
alter table runwy_security.flight_work_reservations enable row level security;
create index flight_work_expiry on runwy_security.flight_work_reservations(expires_at);
create function public.runwy_reserve_flight_work(p_user uuid,p_number text,p_date date,p_origin text,p_destination text,p_tracking boolean,p_limit integer)
returns uuid language plpgsql security definer set search_path='' as $$
declare n integer; existing boolean; lease uuid; k text; maximum integer; quota record;
begin
 if p_user is null or p_number is null or length(p_number)>24 or p_date is null then raise exception 'Invalid flight work' using errcode='23514'; end if;
 perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||p_user::text,0));
 delete from runwy_security.flight_work_reservations where user_id=p_user and expires_at<=now();
 k:=(case when p_tracking then 'tracking:' else 'coverage:' end)||p_number||':'||p_date::text||':'||coalesce(p_origin,'')||':'||coalesce(p_destination,'');
 if exists(select 1 from runwy_security.flight_work_reservations where user_id=p_user and work_key=k) then
  raise exception 'Flight operation already in progress; retry later' using errcode='PT429';
 end if;
 if p_tracking then
  maximum:=least(20,greatest(1,coalesce(p_limit,5)));
  select count(*),coalesce(bool_or(regexp_replace(upper(flight_number),'[^A-Z0-9]','','g')=p_number and travel_date=p_date
   and coalesce(origin_iata,'')=coalesce(p_origin,'') and coalesce(destination_iata,'')=coalesce(p_destination,'')),false)
  into n,existing from public.tracking_sessions ts where owner_user_id=p_user and
   (session_status in('pending','active','errored') or (session_status='paused' and exists(select 1 from public.user_flights uf where uf.tracking_session_id=ts.id and uf.deleted_at is null and uf.lifecycle_state<>'deleted' and public.runwy_subscription_is_live(uf.flight_instance_id,ts.id))));
 else
  maximum:=20;
  select count(*),coalesce(bool_or(regexp_replace(upper(display_flight_number),'[^A-Z0-9]','','g')=p_number and scheduled_departure::date=p_date
   and coalesce(origin_iata,'')=coalesce(p_origin,'') and coalesce(destination_iata,'')=coalesce(p_destination,'')),false)
  into n,existing from public.user_flights where user_id=p_user and deleted_at is null and lifecycle_state<>'deleted'
   and public.runwy_subscription_is_live(flight_instance_id,tracking_session_id);
 end if;
 n:=n+(select count(*) from runwy_security.flight_work_reservations where user_id=p_user and tracking=p_tracking);
 if not existing and n>=maximum then raise exception 'Active flight allowance reached' using errcode='PT429'; end if;
 if (select count(*) from runwy_security.flight_work_reservations where user_id=p_user)>=20 then
  raise exception 'Too many flight operations' using errcode='PT429'; end if;
 select * into quota from public.runwy_consume_rate_limit('flight-work-minute',encode(sha256(p_user::text::bytea),'hex'),60000,60);
 if quota.total_hits>60 then raise exception 'Flight lookup allowance reached; retry later' using errcode='PT429'; end if;
 insert into runwy_security.rate_limits as r(namespace,key_hash,hits,expires_at)
 values('flight-work-day',encode(sha256(p_user::text::bytea),'hex'),1,(date_trunc('day',now() at time zone 'UTC')+interval '1 day') at time zone 'UTC')
 on conflict(namespace,key_hash) do update set hits=case when r.expires_at<=now() then 1 else least(r.hits+1,501) end,
 expires_at=case when r.expires_at<=now() then excluded.expires_at else r.expires_at end returning hits into n;
 if n>500 then raise exception 'Daily flight lookup allowance reached' using errcode='PT429'; end if;
 insert into runwy_security.flight_work_reservations(user_id,work_key,tracking,expires_at)
 values(p_user,k,p_tracking,now()+interval '2 minutes') returning token into lease;
 return lease;
end $$;
revoke all on function public.runwy_reserve_flight_work(uuid,text,date,text,text,boolean,integer) from public,anon,authenticated;
grant execute on function public.runwy_reserve_flight_work(uuid,text,date,text,text,boolean,integer) to service_role;
grant delete on runwy_security.flight_work_reservations to service_role;
