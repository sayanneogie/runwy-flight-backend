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

create table if not exists public.profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  avatar_url text,
  email text,
  auth_provider text,
  onboarding_completed boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists public.profiles
  add column if not exists display_name text,
  add column if not exists avatar_url text,
  add column if not exists email text,
  add column if not exists auth_provider text,
  add column if not exists onboarding_completed boolean not null default false,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create table if not exists public.user_settings (
  user_id uuid primary key references auth.users(id) on delete cascade,
  preferred_theme text not null default 'system',
  distance_unit text not null default 'kilometers',
  uses_24_hour_time boolean not null default false,
  default_airport_code text not null default '',
  validate_with_provider_enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint user_settings_preferred_theme_check
    check (preferred_theme in ('system', 'light', 'dark')),
  constraint user_settings_distance_unit_check
    check (distance_unit in ('kilometers', 'miles'))
);

alter table if exists public.user_settings
  add column if not exists preferred_theme text not null default 'system',
  add column if not exists distance_unit text not null default 'kilometers',
  add column if not exists uses_24_hour_time boolean not null default false,
  add column if not exists default_airport_code text not null default '',
  add column if not exists validate_with_provider_enabled boolean not null default true,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

update public.user_settings
set
  preferred_theme = case
    when preferred_theme in ('system', 'light', 'dark') then preferred_theme
    else 'system'
  end,
  distance_unit = case
    when distance_unit in ('kilometers', 'miles') then distance_unit
    else 'kilometers'
  end,
  uses_24_hour_time = coalesce(uses_24_hour_time, false),
  default_airport_code = coalesce(default_airport_code, ''),
  validate_with_provider_enabled = coalesce(validate_with_provider_enabled, true)
where
  preferred_theme is null
  or preferred_theme not in ('system', 'light', 'dark')
  or distance_unit is null
  or distance_unit not in ('kilometers', 'miles')
  or uses_24_hour_time is null
  or default_airport_code is null
  or validate_with_provider_enabled is null;

alter table public.user_settings
  alter column preferred_theme set default 'system',
  alter column preferred_theme set not null,
  alter column distance_unit set default 'kilometers',
  alter column distance_unit set not null,
  alter column uses_24_hour_time set default false,
  alter column uses_24_hour_time set not null,
  alter column default_airport_code set default '',
  alter column default_airport_code set not null,
  alter column validate_with_provider_enabled set default true,
  alter column validate_with_provider_enabled set not null,
  alter column created_at set default now(),
  alter column created_at set not null,
  alter column updated_at set default now(),
  alter column updated_at set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_settings_preferred_theme_check'
      and conrelid = 'public.user_settings'::regclass
  ) then
    alter table public.user_settings
      add constraint user_settings_preferred_theme_check
      check (preferred_theme in ('system', 'light', 'dark'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_settings_distance_unit_check'
      and conrelid = 'public.user_settings'::regclass
  ) then
    alter table public.user_settings
      add constraint user_settings_distance_unit_check
      check (distance_unit in ('kilometers', 'miles'));
  end if;
end;
$$;

create table if not exists public.user_flights (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  source_type text not null default 'manual_search',
  lifecycle_state text not null default 'upcoming',
  tracking_session_id uuid,
  display_flight_number text,
  marketing_airline_code text,
  marketing_airline_name text,
  operating_airline_code text,
  operating_airline_name text,
  origin_iata text,
  destination_iata text,
  scheduled_departure timestamptz,
  scheduled_arrival timestamptz,
  estimated_departure timestamptz,
  estimated_arrival timestamptz,
  actual_departure timestamptz,
  actual_arrival timestamptz,
  departure_terminal text,
  departure_gate text,
  arrival_terminal text,
  arrival_gate text,
  aircraft_type text,
  status text,
  delay_minutes integer,
  distance_km numeric,
  flight_time_minutes integer,
  route_polyline jsonb,
  tracked_snapshot jsonb,
  calendar_source_text text,
  provider_name text,
  provider_flight_id text,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint user_flights_source_type_check
    check (source_type in (
      'manual_search',
      'calendar_import',
      'tracked',
      'recovered',
      'manual_verified',
      'auto_archive'
    )),
  constraint user_flights_lifecycle_state_check
    check (lifecycle_state in (
      'upcoming',
      'active',
      'landed',
      'archived',
      'deleted'
    ))
);

alter table if exists public.user_flights
  add column if not exists user_id uuid references auth.users(id) on delete cascade,
  add column if not exists source_type text not null default 'manual_search',
  add column if not exists lifecycle_state text not null default 'upcoming',
  add column if not exists tracking_session_id uuid,
  add column if not exists display_flight_number text,
  add column if not exists marketing_airline_code text,
  add column if not exists marketing_airline_name text,
  add column if not exists operating_airline_code text,
  add column if not exists operating_airline_name text,
  add column if not exists origin_iata text,
  add column if not exists destination_iata text,
  add column if not exists scheduled_departure timestamptz,
  add column if not exists scheduled_arrival timestamptz,
  add column if not exists estimated_departure timestamptz,
  add column if not exists estimated_arrival timestamptz,
  add column if not exists actual_departure timestamptz,
  add column if not exists actual_arrival timestamptz,
  add column if not exists departure_terminal text,
  add column if not exists departure_gate text,
  add column if not exists arrival_terminal text,
  add column if not exists arrival_gate text,
  add column if not exists aircraft_type text,
  add column if not exists status text,
  add column if not exists delay_minutes integer,
  add column if not exists distance_km numeric,
  add column if not exists flight_time_minutes integer,
  add column if not exists route_polyline jsonb,
  add column if not exists tracked_snapshot jsonb,
  add column if not exists calendar_source_text text,
  add column if not exists provider_name text,
  add column if not exists provider_flight_id text,
  add column if not exists deleted_at timestamptz,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_flights_source_type_check'
      and conrelid = 'public.user_flights'::regclass
  ) then
    alter table public.user_flights
      add constraint user_flights_source_type_check
      check (source_type in (
        'manual_search',
        'calendar_import',
        'tracked',
        'recovered',
        'manual_verified',
        'auto_archive'
      ));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_flights_lifecycle_state_check'
      and conrelid = 'public.user_flights'::regclass
  ) then
    alter table public.user_flights
      add constraint user_flights_lifecycle_state_check
      check (lifecycle_state in (
        'upcoming',
        'active',
        'landed',
        'archived',
        'deleted'
      ));
  end if;
end;
$$;

create table if not exists public.entitlements (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  provider text not null default 'revenuecat',
  product_id text,
  entitlement_key text not null,
  is_active boolean not null default false,
  expires_at timestamptz,
  last_synced_at timestamptz not null default now(),
  raw_payload jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint entitlements_unique_user_entitlement unique (user_id, entitlement_key)
);

alter table if exists public.entitlements
  add column if not exists user_id uuid references auth.users(id) on delete cascade,
  add column if not exists provider text not null default 'revenuecat',
  add column if not exists product_id text,
  add column if not exists entitlement_key text,
  add column if not exists is_active boolean not null default false,
  add column if not exists expires_at timestamptz,
  add column if not exists last_synced_at timestamptz not null default now(),
  add column if not exists raw_payload jsonb,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'entitlements_unique_user_entitlement'
      and conrelid = 'public.entitlements'::regclass
  ) then
    alter table public.entitlements
      add constraint entitlements_unique_user_entitlement
      unique (user_id, entitlement_key);
  end if;
end;
$$;

create index if not exists profiles_email_idx
  on public.profiles (email);

create index if not exists user_flights_user_state_idx
  on public.user_flights (user_id, lifecycle_state, scheduled_departure desc);

create index if not exists user_flights_tracking_session_idx
  on public.user_flights (tracking_session_id)
  where tracking_session_id is not null;

create index if not exists user_flights_user_deleted_idx
  on public.user_flights (user_id, deleted_at);

create index if not exists entitlements_user_active_idx
  on public.entitlements (user_id, is_active, entitlement_key);

drop trigger if exists profiles_touch_updated_at on public.profiles;
create trigger profiles_touch_updated_at
before update on public.profiles
for each row
execute function public.runwy_touch_updated_at();

drop trigger if exists user_settings_touch_updated_at on public.user_settings;
create trigger user_settings_touch_updated_at
before update on public.user_settings
for each row
execute function public.runwy_touch_updated_at();

drop trigger if exists user_flights_touch_updated_at on public.user_flights;
create trigger user_flights_touch_updated_at
before update on public.user_flights
for each row
execute function public.runwy_touch_updated_at();

drop trigger if exists entitlements_touch_updated_at on public.entitlements;
create trigger entitlements_touch_updated_at
before update on public.entitlements
for each row
execute function public.runwy_touch_updated_at();

alter table public.profiles enable row level security;
alter table public.user_settings enable row level security;
alter table public.user_flights enable row level security;
alter table public.entitlements enable row level security;

drop policy if exists "profiles_select_self" on public.profiles;
create policy "profiles_select_self"
on public.profiles
for select
to authenticated
using (auth.uid() = user_id);

drop policy if exists "profiles_insert_self" on public.profiles;
create policy "profiles_insert_self"
on public.profiles
for insert
to authenticated
with check (auth.uid() = user_id);

drop policy if exists "profiles_update_self" on public.profiles;
create policy "profiles_update_self"
on public.profiles
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "user_settings_select_self" on public.user_settings;
create policy "user_settings_select_self"
on public.user_settings
for select
to authenticated
using (auth.uid() = user_id);

drop policy if exists "user_settings_insert_self" on public.user_settings;
create policy "user_settings_insert_self"
on public.user_settings
for insert
to authenticated
with check (auth.uid() = user_id);

drop policy if exists "user_settings_update_self" on public.user_settings;
create policy "user_settings_update_self"
on public.user_settings
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "user_flights_select_owner" on public.user_flights;
create policy "user_flights_select_owner"
on public.user_flights
for select
to authenticated
using (auth.uid() = user_id);

drop policy if exists "user_flights_insert_owner" on public.user_flights;
create policy "user_flights_insert_owner"
on public.user_flights
for insert
to authenticated
with check (auth.uid() = user_id);

drop policy if exists "user_flights_update_owner" on public.user_flights;
create policy "user_flights_update_owner"
on public.user_flights
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "user_flights_delete_owner" on public.user_flights;
create policy "user_flights_delete_owner"
on public.user_flights
for delete
to authenticated
using (auth.uid() = user_id);

drop policy if exists "entitlements_select_self" on public.entitlements;
create policy "entitlements_select_self"
on public.entitlements
for select
to authenticated
using (auth.uid() = user_id);
