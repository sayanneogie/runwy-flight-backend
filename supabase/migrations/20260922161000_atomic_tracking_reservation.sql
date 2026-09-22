-- Derive costly subscription lifetime from server-owned data, never client dates or labels.
create function public.runwy_subscription_is_live(p_instance uuid,p_session uuid) returns boolean
language sql stable security definer set search_path='' as $$
 select coalesce(
  (select not fi.is_final and coalesce(fi.scheduled_arrival_at,fi.scheduled_departure_at+interval '24 hours',fi.departure_date::timestamptz+interval '2 days')>now()
   from public.flight_instances fi where fi.id=p_instance),
  (select ts.session_status in('pending','active','errored') or ts.travel_date::timestamptz+interval '2 days'>now()
   from public.tracking_sessions ts where ts.id=p_session),false)
$$;
revoke all on function public.runwy_subscription_is_live(uuid,uuid) from public,anon,authenticated;
-- Reserve capacity under an account lock, before a caller starts persistence.
create function public.runwy_create_tracking_session(
 p_user uuid,p_provider text,p_provider_id text,p_number text,p_airline text,p_origin text,
 p_destination text,p_date date,p_source text,p_query jsonb,p_limit integer
) returns uuid language plpgsql security definer set search_path='' as $$
declare result uuid; active_count integer; maximum integer:=least(20,greatest(1,coalesce(p_limit,5)));
begin
 perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||p_user::text,0));
 select id into result from public.tracking_sessions
 where owner_user_id=p_user and session_status in('pending','active','paused') and provider=p_provider
 and ((p_provider_id is not null and provider_flight_id=p_provider_id) or
  (p_provider_id is null and flight_number=p_number and travel_date=p_date
   and coalesce(origin_iata,'')=coalesce(p_origin,'') and coalesce(destination_iata,'')=coalesce(p_destination,'')))
 order by created_at desc limit 1;
 if result is not null and exists(select 1 from public.tracking_sessions ts where ts.id=result and
  (ts.session_status in('pending','active') or exists(select 1 from public.user_flights uf
    where uf.tracking_session_id=ts.id and uf.deleted_at is null and uf.lifecycle_state<>'deleted'
    and public.runwy_subscription_is_live(uf.flight_instance_id,uf.tracking_session_id)))) then return result; end if;
 select count(*) into active_count from public.tracking_sessions ts where owner_user_id=p_user and
 (session_status in('pending','active','errored') or (session_status='paused' and exists(
  select 1 from public.user_flights uf where uf.tracking_session_id=ts.id and uf.deleted_at is null
   and uf.lifecycle_state<>'deleted' and public.runwy_subscription_is_live(uf.flight_instance_id,uf.tracking_session_id))));
 if active_count>=maximum then raise exception 'Active tracking limit reached' using errcode='PT429'; end if;
 if result is not null then update public.tracking_sessions set session_status='pending' where id=result; return result; end if;
 insert into public.tracking_sessions(owner_user_id,provider,provider_flight_id,flight_number,airline_code,
 origin_iata,destination_iata,travel_date,session_status,created_source,metadata_json)
 values(p_user,p_provider,p_provider_id,p_number,p_airline,p_origin,p_destination,p_date,'pending',p_source,jsonb_build_object('query',p_query))
 returning id into result;
 return result;
end $$;
revoke all on function public.runwy_create_tracking_session(uuid,text,text,text,text,text,text,date,text,jsonb,integer) from public,anon,authenticated;
grant execute on function public.runwy_create_tracking_session(uuid,text,text,text,text,text,text,date,text,jsonb,integer) to service_role;

-- Canonical subscriptions can also be saved without a legacy tracking session.
create function public.runwy_bound_live_subscriptions() returns trigger language plpgsql security definer set search_path='' as $$
declare active_count integer;
begin
 if new.deleted_at is not null or new.lifecycle_state='deleted'
 or (new.flight_instance_id is null and new.tracking_session_id is null)
 or not public.runwy_subscription_is_live(new.flight_instance_id,new.tracking_session_id) then return new; end if;
 if tg_op='UPDATE' then
  if old.deleted_at is null and old.lifecycle_state<>'deleted'
   and (old.flight_instance_id is not null or old.tracking_session_id is not null)
   and public.runwy_subscription_is_live(old.flight_instance_id,old.tracking_session_id) then return new; end if;
 end if;
 perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||new.user_id::text,0));
 select count(*) into active_count from public.user_flights where user_id=new.user_id and id<>new.id
 and deleted_at is null and lifecycle_state<>'deleted' and (flight_instance_id is not null or tracking_session_id is not null)
 and public.runwy_subscription_is_live(flight_instance_id,tracking_session_id);
 if active_count>=20 then raise exception 'Active flight allowance reached' using errcode='PT429'; end if;
 return new;
end $$;
revoke all on function public.runwy_bound_live_subscriptions() from public,anon,authenticated;
create trigger zz_bound_live_subscriptions before insert or update on public.user_flights
for each row execute function public.runwy_bound_live_subscriptions();
