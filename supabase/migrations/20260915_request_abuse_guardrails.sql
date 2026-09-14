begin;

create schema if not exists runwy_security;
revoke all on schema runwy_security from public, anon, authenticated;

create table if not exists runwy_security.rate_limits (
  namespace text not null,
  key_hash text not null,
  hits integer not null,
  expires_at timestamptz not null,
  primary key (namespace, key_hash)
);
create index if not exists rate_limits_expiry_idx on runwy_security.rate_limits (expires_at);
alter table runwy_security.rate_limits enable row level security;
revoke all on runwy_security.rate_limits from public, anon, authenticated;

create or replace function public.runwy_consume_rate_limit(
  p_namespace text, p_key_hash text, p_window_ms integer, p_limit integer
) returns table(total_hits integer, reset_at timestamptz)
language plpgsql security definer set search_path = '' as $$
declare
  current_time_value timestamptz := clock_timestamp();
begin
  if length(p_namespace) > 80 or p_namespace is null or p_key_hash !~ '^[a-f0-9]{64}$'
     or p_key_hash is null or p_window_ms is null or p_window_ms not between 1000 and 3600000
     or p_limit is null or p_limit not between 1 and 1000000 then
    raise exception 'Invalid rate-limit parameters';
  end if;
  return query
    insert into runwy_security.rate_limits as counters(namespace, key_hash, hits, expires_at)
    values (p_namespace, p_key_hash, 1, current_time_value + p_window_ms * interval '1 millisecond')
    on conflict (namespace, key_hash) do update
    set hits = case when counters.expires_at <= current_time_value then 1
                    else least(counters.hits + 1, p_limit + 1) end,
        expires_at = case when counters.expires_at <= current_time_value
                     then current_time_value + p_window_ms * interval '1 millisecond'
                     else counters.expires_at end
    returning counters.hits, counters.expires_at;
end;
$$;
revoke all on function public.runwy_consume_rate_limit(text,text,integer,integer) from public, anon, authenticated;
grant execute on function public.runwy_consume_rate_limit(text,text,integer,integer) to service_role;

create or replace function public.runwy_cleanup_rate_limits()
returns void language sql security definer set search_path = '' as $$
  delete from runwy_security.rate_limits where (namespace, key_hash) in (
    select namespace, key_hash from runwy_security.rate_limits
    where expires_at < now() - interval '5 minutes' order by expires_at limit 10000
  );
$$;
revoke all on function public.runwy_cleanup_rate_limits() from public, anon, authenticated;
grant execute on function public.runwy_cleanup_rate_limits() to service_role;

create table if not exists runwy_security.provider_daily_budgets (
  budget_day date not null,
  bucket text not null check (bucket in ('general', 'tracked')),
  reserved_units integer not null check (reserved_units >= 0),
  primary key (budget_day, bucket)
);
alter table runwy_security.provider_daily_budgets enable row level security;
revoke all on runwy_security.provider_daily_budgets from public, anon, authenticated;

create or replace function public.runwy_reserve_flightaware_budget(
  p_bucket text, p_limit integer, p_units integer
) returns table(allowed boolean, used_units integer)
language plpgsql security definer set search_path = '' as $$
declare
  day_value date := (now() at time zone 'UTC')::date;
  current_units integer;
begin
  if p_bucket not in ('general', 'tracked') or p_bucket is null
     or p_limit is null or p_limit < 1 or p_units is null or p_units < 1 then
    raise exception 'Invalid provider budget parameters';
  end if;
  -- Seed the first reservation from existing usage so deployment does not reset today's budget.
  if not exists (select 1 from runwy_security.provider_daily_budgets where budget_day = day_value and bucket = p_bucket) then
  insert into runwy_security.provider_daily_budgets(budget_day, bucket, reserved_units)
  select day_value, p_bucket, coalesce(sum(coalesce(cost_estimate, 1)), 0)::integer
  from public.api_usage_logs
  where provider = 'flightaware' and endpoint like 'aeroapi:flight:%'
    and ((p_bucket = 'tracked' and endpoint = 'aeroapi:flight:tracked_flight')
      or (p_bucket = 'general' and endpoint <> 'aeroapi:flight:tracked_flight'))
    and created_at >= day_value::timestamp at time zone 'UTC'
  on conflict (budget_day, bucket) do nothing;
  end if;

  select reserved_units into current_units from runwy_security.provider_daily_budgets
  where budget_day = day_value and bucket = p_bucket for update;
  if p_units > p_limit - current_units then
    return query select false, current_units;
    return;
  end if;
  update runwy_security.provider_daily_budgets set reserved_units = reserved_units + p_units
  where budget_day = day_value and bucket = p_bucket;
  return query select true, current_units + p_units;
end;
$$;
revoke all on function public.runwy_reserve_flightaware_budget(text,integer,integer) from public, anon, authenticated;
grant execute on function public.runwy_reserve_flightaware_budget(text,integer,integer) to service_role;

commit;
