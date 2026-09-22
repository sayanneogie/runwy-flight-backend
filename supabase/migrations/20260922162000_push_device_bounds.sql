create or replace function public.runwy_register_push_device(p_user uuid,p_token text,p_device text,p_platform text,p_environment text)
returns public.device_tokens language plpgsql security definer set search_path='' as $$
declare result public.device_tokens;
begin
  p_token:=lower(p_token);
  if p_user is null or p_token is null or length(p_token) not between 64 and 512 or p_token !~ '^[a-fA-F0-9]+$' or p_device is null or length(btrim(p_device)) not between 1 and 128 or p_platform is distinct from 'ios' or p_environment is null or p_environment not in('sandbox','production') then
    raise exception 'Invalid push registration' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||p_user::text,0));
  -- Token expiry is lazy as well as enforced at delivery time.
  update public.device_tokens set is_active=false where user_id=p_user and is_active and updated_at<now()-interval '90 days';
  update public.push_devices set push_enabled=false where user_id=p_user and push_enabled and updated_at<now()-interval '90 days';
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
  if (select count(*) from public.device_tokens where user_id=p_user and is_active and device_token<>p_token)>=20 then
    raise exception 'Active device allowance reached' using errcode='PT429'; end if;
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
