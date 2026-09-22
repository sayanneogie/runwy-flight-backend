create extension if not exists pgcrypto;

create or replace function public.runwy_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.flight_definitions (
  id uuid primary key default gen_random_uuid(),
  airline_code text not null,
  flight_number text not null,
  airline_name text,
  typical_origin_airport text,
  typical_destination_airport text,
  provider text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (airline_code, flight_number)
);

create table if not exists public.flight_instances (
  id uuid primary key default gen_random_uuid(),
  flight_key text unique not null,
  provider_flight_id text,
  airline_code text not null,
  flight_number text not null,
  departure_date date not null,
  origin_airport text,
  destination_airport text,
  scheduled_departure_at timestamptz,
  scheduled_arrival_at timestamptz,
  estimated_departure_at timestamptz,
  estimated_arrival_at timestamptz,
  actual_departure_at timestamptz,
  actual_arrival_at timestamptz,
  status text not null default 'unknown',
  status_detail text,
  gate text,
  terminal text,
  baggage_belt text,
  position_lat double precision,
  position_lon double precision,
  altitude integer,
  ground_speed integer,
  heading integer,
  provider text,
  provider_alert_id text,
  provider_alert_status text not null default 'unavailable',
  provider_alert_created_at timestamptz,
  provider_alert_expires_at timestamptz,
  last_webhook_received_at timestamptz,
  live_data_source text not null default 'on_demand',
  streaming_status text not null default 'disabled',
  stream_registered_at timestamptz,
  last_stream_event_at timestamptz,
  last_poll_reason text,
  refresh_priority text not null default 'normal',
  data_confidence text default 'unknown',
  normalized_data jsonb,
  raw_provider_response jsonb,
  last_fetched_at timestamptz,
  fresh_until timestamptz,
  needs_revalidation boolean default false,
  is_final boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  constraint flight_instances_confidence_check
    check (data_confidence in ('unknown', 'high', 'medium', 'low', 'suspicious')),
  constraint flight_instances_provider_alert_status_check
    check (provider_alert_status in ('active', 'unavailable', 'failed', 'expired')),
  constraint flight_instances_refresh_priority_check
    check (refresh_priority in ('none', 'minimal', 'low', 'normal', 'high', 'critical')),
  constraint flight_instances_live_data_source_check
    check (live_data_source in ('on_demand', 'provider_alert', 'streaming')),
  constraint flight_instances_streaming_status_check
    check (streaming_status in ('disabled', 'pending', 'active', 'failed', 'expired'))
);

alter table if exists public.flight_instances
  add column if not exists provider_alert_id text,
  add column if not exists provider_alert_status text not null default 'unavailable',
  add column if not exists provider_alert_created_at timestamptz,
  add column if not exists provider_alert_expires_at timestamptz,
  add column if not exists last_webhook_received_at timestamptz,
  add column if not exists live_data_source text not null default 'on_demand',
  add column if not exists streaming_status text not null default 'disabled',
  add column if not exists stream_registered_at timestamptz,
  add column if not exists last_stream_event_at timestamptz,
  add column if not exists last_poll_reason text,
  add column if not exists refresh_priority text not null default 'normal';

create table if not exists public.flight_instance_aliases (
  id uuid primary key default gen_random_uuid(),
  alias_key text unique not null,
  flight_instance_id uuid references public.flight_instances(id) on delete cascade,
  created_at timestamptz default now()
);

create table if not exists public.flight_snapshots (
  id uuid primary key default gen_random_uuid(),
  flight_instance_id uuid references public.flight_instances(id) on delete cascade,
  status text,
  estimated_departure_at timestamptz,
  estimated_arrival_at timestamptz,
  actual_departure_at timestamptz,
  actual_arrival_at timestamptz,
  gate text,
  terminal text,
  baggage_belt text,
  position_lat double precision,
  position_lon double precision,
  altitude integer,
  ground_speed integer,
  heading integer,
  raw_provider_response jsonb,
  normalized_data jsonb,
  created_at timestamptz default now()
);

create table if not exists public.flight_events (
  id uuid primary key default gen_random_uuid(),
  flight_instance_id uuid references public.flight_instances(id) on delete cascade,
  event_type text not null,
  event_severity text not null default 'low',
  old_value jsonb,
  new_value jsonb,
  summary text,
  provider text,
  provider_event_time timestamptz,
  confidence text default 'medium',
  notification_required boolean default false,
  created_at timestamptz default now(),
  constraint flight_events_type_check
    check (event_type in (
      'SCHEDULED', 'DELAYED', 'RESCHEDULED', 'CANCELLED', 'DEPARTED', 'AIRBORNE',
      'LANDED', 'ARRIVED', 'TAXIING', 'TAKEOFF_ROLL', 'TAXI_IN', 'ARRIVED_AT_GATE',
      'GATE_CHANGED', 'TERMINAL_CHANGED', 'BAGGAGE_BELT_ASSIGNED',
      'DIVERTED', 'RETURNED_TO_GATE', 'WEATHER_ADVISORY', 'UNKNOWN_CHANGE', 'PROVIDER_DATA_SUSPICIOUS'
    )),
  constraint flight_events_severity_check
    check (event_severity in ('low', 'medium', 'high', 'critical')),
  constraint flight_events_confidence_check
    check (confidence in ('high', 'medium', 'low', 'suspicious'))
);

alter table public.user_flights
  add column if not exists flight_instance_id uuid references public.flight_instances(id) on delete cascade,
  add column if not exists notification_enabled boolean default true,
  add column if not exists alert_preferences jsonb default '{"low": false, "medium": true, "high": true, "critical": true}'::jsonb,
  add column if not exists trip_id uuid,
  add column if not exists user_label text,
  add column if not exists visibility text default 'private',
  add column if not exists added_at timestamptz default now();

alter table public.user_flights
  alter column notification_enabled set default true,
  alter column alert_preferences set default '{"low": false, "medium": true, "high": true, "critical": true}'::jsonb,
  alter column visibility set default 'private',
  alter column added_at set default now();

create table if not exists public.notification_deliveries (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  flight_instance_id uuid references public.flight_instances(id) on delete cascade,
  flight_event_id uuid references public.flight_events(id) on delete cascade,
  channel text not null default 'apns',
  status text not null default 'pending',
  sent_at timestamptz,
  opened_at timestamptz,
  error text,
  created_at timestamptz default now(),
  unique (user_id, flight_event_id, channel)
);

create table if not exists public.device_tokens (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  device_token text not null,
  platform text not null default 'ios',
  environment text not null,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (user_id, device_token)
);

create table if not exists public.api_usage_logs (
  id uuid primary key default gen_random_uuid(),
  provider text not null,
  endpoint text not null,
  flight_key text,
  user_id uuid,
  status_code integer,
  response_time_ms integer,
  cache_status text,
  cost_estimate numeric,
  error text,
  created_at timestamptz default now()
);

alter table if exists public.flight_events
  drop constraint if exists flight_events_type_check;

alter table if exists public.flight_events
  add constraint flight_events_type_check
    check (event_type in (
      'SCHEDULED', 'DELAYED', 'RESCHEDULED', 'CANCELLED', 'DEPARTED', 'AIRBORNE',
      'LANDED', 'ARRIVED', 'TAXIING', 'TAKEOFF_ROLL', 'TAXI_IN', 'ARRIVED_AT_GATE',
      'GATE_CHANGED', 'TERMINAL_CHANGED', 'BAGGAGE_BELT_ASSIGNED',
      'DIVERTED', 'RETURNED_TO_GATE', 'WEATHER_ADVISORY', 'UNKNOWN_CHANGE', 'PROVIDER_DATA_SUSPICIOUS'
    ));

create unique index if not exists user_flights_user_flight_instance_unique
  on public.user_flights (user_id, flight_instance_id)
  where flight_instance_id is not null;
create index if not exists flight_instances_lookup_idx
  on public.flight_instances (airline_code, flight_number, departure_date, origin_airport, destination_airport);
create index if not exists flight_instances_refresh_idx
  on public.flight_instances (fresh_until, is_final, needs_revalidation, provider_alert_status, refresh_priority);
create index if not exists flight_instances_provider_alert_idx
  on public.flight_instances (provider, provider_alert_status, provider_alert_expires_at);
create index if not exists flight_instances_streaming_idx
  on public.flight_instances (provider, live_data_source, streaming_status, last_stream_event_at);
create index if not exists flight_events_instance_created_idx
  on public.flight_events (flight_instance_id, created_at desc);
create index if not exists flight_snapshots_instance_created_idx
  on public.flight_snapshots (flight_instance_id, created_at desc);
create index if not exists notification_deliveries_user_created_idx
  on public.notification_deliveries (user_id, created_at desc);
create index if not exists device_tokens_user_active_idx
  on public.device_tokens (user_id, is_active);
create index if not exists api_usage_logs_provider_created_idx
  on public.api_usage_logs (provider, created_at desc);

do $$
begin
  if to_regclass('public.push_devices') is not null then
    insert into public.device_tokens (user_id, device_token, platform, environment, is_active)
    select
      pd.user_id,
      pd.apns_token,
      coalesce(nullif(pd.platform, ''), 'ios'),
      'production',
      coalesce(pd.push_enabled, true)
    from public.push_devices pd
    where pd.user_id is not null
      and pd.apns_token is not null
    on conflict (user_id, device_token) do update set
      platform = excluded.platform,
      is_active = excluded.is_active,
      updated_at = now();
  end if;
end $$;

drop trigger if exists flight_definitions_touch_updated_at on public.flight_definitions;
create trigger flight_definitions_touch_updated_at
before update on public.flight_definitions
for each row execute function public.runwy_touch_updated_at();

drop trigger if exists flight_instances_touch_updated_at on public.flight_instances;
create trigger flight_instances_touch_updated_at
before update on public.flight_instances
for each row execute function public.runwy_touch_updated_at();

drop trigger if exists device_tokens_touch_updated_at on public.device_tokens;
create trigger device_tokens_touch_updated_at
before update on public.device_tokens
for each row execute function public.runwy_touch_updated_at();

alter table public.user_flights enable row level security;
alter table public.notification_deliveries enable row level security;
alter table public.device_tokens enable row level security;

drop policy if exists user_flights_own_select on public.user_flights;
create policy user_flights_own_select on public.user_flights
for select using (auth.uid() = user_id);

drop policy if exists user_flights_own_insert on public.user_flights;
create policy user_flights_own_insert on public.user_flights
for insert with check (auth.uid() = user_id);

drop policy if exists user_flights_own_update on public.user_flights;
create policy user_flights_own_update on public.user_flights
for update using (auth.uid() = user_id) with check (auth.uid() = user_id);

drop policy if exists user_flights_own_delete on public.user_flights;
create policy user_flights_own_delete on public.user_flights
for delete using (auth.uid() = user_id);

drop policy if exists notification_deliveries_own_select on public.notification_deliveries;
create policy notification_deliveries_own_select on public.notification_deliveries
for select using (auth.uid() = user_id);

drop policy if exists device_tokens_own_select on public.device_tokens;
create policy device_tokens_own_select on public.device_tokens
for select using (auth.uid() = user_id);

drop policy if exists device_tokens_own_insert on public.device_tokens;
create policy device_tokens_own_insert on public.device_tokens
for insert with check (auth.uid() = user_id);

drop policy if exists device_tokens_own_update on public.device_tokens;
create policy device_tokens_own_update on public.device_tokens
for update using (auth.uid() = user_id) with check (auth.uid() = user_id);

drop policy if exists device_tokens_own_delete on public.device_tokens;
create policy device_tokens_own_delete on public.device_tokens
for delete using (auth.uid() = user_id);

revoke insert, update, delete on public.flight_instances from anon, authenticated;
revoke insert, update, delete on public.flight_events from anon, authenticated;
revoke insert, update, delete on public.flight_snapshots from anon, authenticated;
