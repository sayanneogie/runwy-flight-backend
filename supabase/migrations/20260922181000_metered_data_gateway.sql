-- Staged cutover: deploy gateway and compatible app, then explicitly require it.
-- Keeping this false during rollout is not equivalent to closing direct access.
create table runwy_security.data_gateway_config (
 id boolean primary key default true check(id),required boolean not null default false,
 secret_hash text check(secret_hash is null or length(secret_hash)=64)
);
alter table runwy_security.data_gateway_config enable row level security;
insert into runwy_security.data_gateway_config(id) values(true);
create table runwy_security.data_transfer_usage (
 user_id uuid primary key references auth.users(id) on delete cascade, bytes bigint not null, resets_at timestamptz not null
);
alter table runwy_security.data_transfer_usage enable row level security;
create function public.runwy_charge_data_bytes(p_user uuid,p_bytes bigint) returns boolean
language plpgsql security definer set search_path='' as $$
declare total bigint;
begin
 if p_bytes<0 or p_bytes>8388608 then return false; end if;
 insert into runwy_security.data_transfer_usage as u(user_id,bytes,resets_at) values(p_user,p_bytes,now()+interval '1 minute')
 on conflict(user_id) do update set bytes=case when u.resets_at<=now() then excluded.bytes else least(u.bytes+excluded.bytes,67108865) end,
 resets_at=case when u.resets_at<=now() then excluded.resets_at else u.resets_at end returning bytes into total;
 return total<=67108864;
end $$;
revoke all on function public.runwy_charge_data_bytes(uuid,bigint) from public,anon,authenticated;
grant execute on function public.runwy_charge_data_bytes(uuid,bigint) to service_role;
create or replace function public.runwy_data_api_guard() returns void
language plpgsql security definer set search_path='' as $$
declare counter record; actor uuid:=auth.uid(); method text:=current_setting('request.method',true);
 config record; supplied text;
begin
 if current_setting('role',true) not in ('authenticated','anon') then return; end if;
 select * into config from runwy_security.data_gateway_config where id;
 supplied:=coalesce(nullif(current_setting('request.headers',true),'')::jsonb->>'x-runwy-data-gateway','');
 if config.required then
  if actor is null or config.secret_hash is null or encode(sha256(supplied::bytea),'hex')<>config.secret_hash then
   raise exception 'Please update Runwy to continue cloud sync' using errcode='PT403';
  end if;
  return; -- independently committed gateway quotas count reads and failures
 end if;
 if current_setting('role',true)<>'authenticated' or actor is null then return; end if;
 if method in('POST','PATCH','PUT','DELETE') then
  select * into counter from public.runwy_consume_rate_limit('db-user-request',encode(sha256(actor::text::bytea),'hex'),60000,120);
  if counter.total_hits>120 then raise exception 'Request quota reached; retry later' using errcode='PT429'; end if;
 end if;
end $$;
notify pgrst,'reload config';
