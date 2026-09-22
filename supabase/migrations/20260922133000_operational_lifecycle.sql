alter table public.device_tokens add column device_id text;
update public.device_tokens dt set device_id=pd.device_id from public.push_devices pd
  where pd.apns_token=dt.device_token and pd.user_id=dt.user_id;
-- Do not choose a winner silently if production ever acquires an active clash.
create unique index device_tokens_active_token_environment_unique
  on public.device_tokens(device_token,environment) where is_active;
create index device_tokens_user_device_idx on public.device_tokens(user_id,device_id);
revoke insert,update,delete on public.device_tokens,public.push_devices from authenticated;

create function public.runwy_register_push_device(p_user uuid,p_token text,p_device text,p_platform text,p_environment text)
returns public.device_tokens language plpgsql security definer set search_path='' as $$
declare result public.device_tokens;
begin
  if p_user is null or length(p_token)<1 or length(p_token)>512 or p_environment not in('sandbox','production') then
    raise exception 'Invalid push registration' using errcode='22023'; end if;
  if p_device is not null then
    perform pg_advisory_xact_lock(hashtextextended('apns-device:'||p_user::text||':'||p_device,0));
  end if;
  perform pg_advisory_xact_lock(hashtextextended('apns:'||p_token,0));
  update public.device_tokens set is_active=false,updated_at=now()
    where is_active and ((device_token=p_token and (user_id<>p_user or environment<>p_environment))
      or (user_id=p_user and p_device is not null and device_id=p_device and device_token<>p_token));
  update public.push_devices set push_enabled=false,updated_at=now()
    where push_enabled and ((apns_token=p_token and user_id<>p_user)
      or (user_id=p_user and p_device is not null and device_id=p_device and apns_token<>p_token));
  insert into public.device_tokens(user_id,device_token,device_id,platform,environment,is_active)
    values(p_user,p_token,p_device,coalesce(p_platform,'ios'),p_environment,true)
    on conflict(user_id,device_token) do update set device_id=coalesce(excluded.device_id,device_tokens.device_id),
      platform=excluded.platform,environment=excluded.environment,is_active=true,updated_at=now()
    returning * into result;
  insert into public.push_devices(apns_token,user_id,device_id,platform,push_enabled)
    values(p_token,p_user,coalesce(p_device,result.device_id),coalesce(p_platform,'ios'),true)
    on conflict(apns_token) do update set user_id=excluded.user_id,
      device_id=coalesce(excluded.device_id,push_devices.device_id),platform=excluded.platform,
      push_enabled=true,updated_at=now();
  return result;
end $$;
revoke all on function public.runwy_register_push_device(uuid,text,text,text,text) from public,anon,authenticated;
grant execute on function public.runwy_register_push_device(uuid,text,text,text,text) to service_role;

create function public.runwy_disable_push_device(p_user uuid,p_device text,p_token text default null)
returns void language plpgsql security definer set search_path='' as $$
begin
  if p_user is null and p_token is null then raise exception 'Registration identity required'; end if;
  update public.device_tokens set is_active=false,updated_at=now() where is_active
    and (p_user is null or user_id=p_user)
    and ((p_device is not null and device_id=p_device) or (p_token is not null and device_token=p_token));
  update public.push_devices set push_enabled=false,updated_at=now() where push_enabled
    and (p_user is null or user_id=p_user)
    and ((p_device is not null and device_id=p_device) or (p_token is not null and apns_token=p_token));
end $$;
revoke all on function public.runwy_disable_push_device(uuid,text,text) from public,anon,authenticated;
grant execute on function public.runwy_disable_push_device(uuid,text,text) to service_role;

-- Bound raw operational history. User flights, tombstones, souvenirs, events,
-- delivery/dedupe records and the latest snapshot per flight are not deleted.
create function public.runwy_prune_operational_history(p_limit integer default 500)
returns jsonb language plpgsql security definer set search_path='' as $$
declare batch integer:=greatest(1,least(p_limit,2000)); snapshots integer; api_logs integer;
  event_logs integer; expired_cache integer;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('runwy:operational-retention',0)) then
    return jsonb_build_object('skipped',true); end if;
  with expired as (
    select fs.id from public.flight_snapshots fs join public.flight_instances fi on fi.id=fs.flight_instance_id
    where fi.is_final and fs.created_at<now()-interval '90 days' and exists(
      select 1 from public.flight_snapshots newer where newer.flight_instance_id=fs.flight_instance_id
        and (newer.created_at>fs.created_at or (newer.created_at=fs.created_at and newer.id>fs.id)))
    order by fs.created_at limit batch
  ) delete from public.flight_snapshots where id in(select id from expired);
  get diagnostics snapshots=row_count;
  with expired as (select id from public.api_usage_logs where created_at<now()-interval '90 days'
    order by created_at limit batch) delete from public.api_usage_logs where id in(select id from expired);
  get diagnostics api_logs=row_count;
  with expired as (select id from public.flight_event_logs where created_at<now()-interval '90 days'
    order by created_at limit batch) delete from public.flight_event_logs where id in(select id from expired);
  get diagnostics event_logs=row_count;
  with expired as (select cache_key from public.provider_response_cache where expires_at<now()
    order by expires_at limit batch) delete from public.provider_response_cache where cache_key in(select cache_key from expired);
  get diagnostics expired_cache=row_count;
  return jsonb_build_object('snapshots',snapshots,'api_logs',api_logs,'event_logs',event_logs,'expired_cache',expired_cache);
end $$;
revoke all on function public.runwy_prune_operational_history(integer) from public,anon,authenticated;
grant execute on function public.runwy_prune_operational_history(integer) to service_role;
