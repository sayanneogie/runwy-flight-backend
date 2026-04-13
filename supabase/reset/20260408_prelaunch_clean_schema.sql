-- Runwy prelaunch clean baseline
-- Destructive reset for app-owned tables. Intended for prelaunch / non-launched environments only.

begin;

create extension if not exists pgcrypto;

drop table if exists public.notifications cascade;
drop table if exists public.push_devices cascade;
drop table if exists public.live_snapshots cascade;
drop table if exists public.tracking_sessions cascade;
drop table if exists public.entitlements cascade;
drop table if exists public.user_flights cascade;
drop table if exists public.user_settings cascade;
drop table if exists public.profiles cascade;
drop table if exists public.flight_watchers cascade;
drop table if exists public.past_flights cascade;
drop table if exists public.flight_segments cascade;
drop table if exists public.calendar_import_candidates cascade;
drop table if exists public.import_requests cascade;
drop table if exists public.usage_logs cascade;
drop table if exists public.audit_logs cascade;
drop table if exists public.past_flight_recovery_audit_logs cascade;
drop table if exists public.past_flight_recovery_candidates cascade;
drop table if exists public.past_flight_recovery_query_cache cascade;
drop table if exists public.past_flight_recovery_requests cascade;
drop table if exists public.runwy_flight_subscriptions cascade;
drop table if exists public.runwy_push_devices cascade;
drop table if exists public.runwy_tracked_flights cascade;
drop table if exists public.user_backup_snapshots cascade;
drop table if exists public.airlines cascade;
drop table if exists public.airports cascade;
drop table if exists public.friend_permissions cascade;
drop table if exists public.friend_relationships cascade;
drop table if exists public.friend_invites cascade;

create or replace function public.runwy_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function public.runwy_validate_friend_permission_pair()
returns trigger
language plpgsql
as $$
declare
  relationship_row record;
begin
  select *
  into relationship_row
  from public.friend_relationships
  where id = new.relationship_id;

  if not found then
    raise exception 'friend_permissions relationship_id % does not exist', new.relationship_id;
  end if;

  if not (
    (new.owner_user_id = relationship_row.user_a and new.viewer_user_id = relationship_row.user_b)
    or
    (new.owner_user_id = relationship_row.user_b and new.viewer_user_id = relationship_row.user_a)
  ) then
    raise exception 'friend_permissions owner/viewer pair must match relationship members';
  end if;

  return new;
end;
$$;

create table public.profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  display_name text not null default 'Traveler',
  avatar_url text,
  email text,
  auth_provider text,
  onboarding_completed boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.user_settings (
  user_id uuid primary key references auth.users(id) on delete cascade,
  preferred_theme text not null default 'system'
    check (preferred_theme in ('light', 'dark', 'system')),
  distance_unit text not null default 'km'
    check (distance_unit in ('km', 'miles')),
  uses_24_hour_time boolean not null default false,
  default_airport_code text not null default '',
  validate_with_provider_enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.user_flights (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  source_type text not null default 'manual_search'
    check (
      source_type in (
        'manual_search',
        'calendar_import',
        'tracked',
        'manual_verified',
        'manual_recovery',
        'history_snapshot',
        'history_repair'
      )
    ),
  lifecycle_state text not null default 'upcoming'
    check (lifecycle_state in ('upcoming', 'active', 'landed', 'archived', 'deleted')),
  tracking_session_id uuid,
  display_flight_number text not null,
  marketing_airline_code text,
  marketing_airline_name text,
  operating_airline_code text,
  operating_airline_name text,
  origin_iata text not null,
  destination_iata text not null,
  scheduled_departure timestamptz not null,
  scheduled_arrival timestamptz,
  estimated_departure timestamptz,
  estimated_arrival timestamptz,
  actual_departure timestamptz,
  actual_arrival timestamptz,
  departure_terminal text,
  departure_gate text,
  arrival_terminal text,
  arrival_gate text,
  baggage_claim text,
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
  notifications_enabled boolean not null default true,
  alert_settings_json jsonb not null default jsonb_build_object(
    'gateChange', true,
    'delayUpdates', true,
    'boardingTime', true,
    'takeoffLanding', false,
    'baggageClaim', true,
    'quietHours', jsonb_build_object(
      'startHour', 22,
      'startMinute', 0,
      'endHour', 7,
      'endMinute', 0
    )
  ),
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint user_flights_alert_settings_is_object
    check (jsonb_typeof(alert_settings_json) = 'object'),
  constraint user_flights_user_tracking_unique unique (user_id, tracking_session_id)
);

create table public.tracking_sessions (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  provider text not null,
  provider_flight_id text,
  flight_number text not null,
  airline_code text,
  origin_iata text,
  destination_iata text,
  travel_date date,
  session_status text not null default 'pending'
    check (session_status in ('pending', 'active', 'paused', 'completed', 'cancelled', 'errored')),
  created_source text,
  metadata_json jsonb not null default '{}'::jsonb,
  next_poll_after timestamptz,
  last_snapshot_at timestamptz,
  polling_stopped_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.live_snapshots (
  tracking_session_id uuid primary key references public.tracking_sessions(id) on delete cascade,
  provider text not null,
  provider_flight_id text,
  flight_number text,
  airline_code text,
  departure_airport_iata text,
  arrival_airport_iata text,
  snapshot_status text,
  terminal text,
  gate text,
  baggage_claim text,
  delay_minutes integer,
  departure_times_json jsonb not null default '{}'::jsonb,
  arrival_times_json jsonb not null default '{}'::jsonb,
  alerts_json jsonb not null default '{}'::jsonb,
  metrics_json jsonb not null default '{}'::jsonb,
  canonical_snapshot_json jsonb not null default '{}'::jsonb,
  raw_provider_payload_json jsonb not null default '{}'::jsonb,
  provider_last_updated_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.push_devices (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  device_id text,
  apns_token text not null unique,
  platform text not null default 'ios',
  push_enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.friend_invites (
  id uuid primary key default gen_random_uuid(),
  inviter_user_id uuid not null references auth.users(id) on delete cascade,
  token_hash text not null unique,
  status text not null default 'pending'
    check (status in ('pending', 'accepted', 'revoked', 'expired')),
  default_share_scope text not null default 'future_flights'
    check (default_share_scope in ('future_flights', 'all_flights', 'selected_flights')),
  message text,
  expires_at timestamptz not null default (now() + interval '7 days'),
  accepted_by_user_id uuid references auth.users(id) on delete set null,
  accepted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.friend_relationships (
  id uuid primary key default gen_random_uuid(),
  user_a uuid not null references auth.users(id) on delete cascade,
  user_b uuid not null references auth.users(id) on delete cascade,
  relationship_status text not null default 'active'
    check (relationship_status in ('active', 'blocked', 'removed')),
  created_by_user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint friend_relationships_distinct_users check (user_a <> user_b),
  constraint friend_relationships_canonical_order check (user_a::text < user_b::text),
  constraint friend_relationships_unique_pair unique (user_a, user_b)
);

create table public.friend_permissions (
  id uuid primary key default gen_random_uuid(),
  relationship_id uuid not null references public.friend_relationships(id) on delete cascade,
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  viewer_user_id uuid not null references auth.users(id) on delete cascade,
  share_scope text not null default 'future_flights'
    check (share_scope in ('future_flights', 'all_flights', 'selected_flights')),
  can_view_live boolean not null default true,
  can_view_history boolean not null default false,
  can_receive_alerts boolean not null default true,
  notify_departure boolean not null default true,
  notify_arrival boolean not null default true,
  notify_delay boolean not null default true,
  notify_gate_change boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint friend_permissions_distinct_users check (owner_user_id <> viewer_user_id),
  constraint friend_permissions_unique_direction unique (owner_user_id, viewer_user_id)
);

create table public.notifications (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  tracking_session_id uuid references public.tracking_sessions(id) on delete set null,
  friend_relationship_id uuid references public.friend_relationships(id) on delete set null,
  notification_type text not null,
  delivery_channel text not null default 'push',
  delivery_status text not null default 'queued',
  title text not null,
  body text not null,
  payload_json jsonb not null default '{}'::jsonb,
  scheduled_for timestamptz,
  sent_at timestamptz,
  read_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.entitlements (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  provider text not null default 'revenuecat',
  product_id text,
  entitlement_key text not null,
  is_active boolean not null default false,
  expires_at timestamptz,
  last_synced_at timestamptz not null default now(),
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint entitlements_unique_user_entitlement unique (user_id, entitlement_key)
);

create index profiles_email_idx
  on public.profiles (email);

create index user_flights_user_state_idx
  on public.user_flights (user_id, lifecycle_state, scheduled_departure desc);

create index user_flights_user_deleted_idx
  on public.user_flights (user_id, deleted_at);

create index user_flights_provider_idx
  on public.user_flights (provider_name, provider_flight_id);

create index tracking_sessions_owner_status_idx
  on public.tracking_sessions (owner_user_id, session_status, updated_at desc);

create index tracking_sessions_due_idx
  on public.tracking_sessions (session_status, next_poll_after);

create index tracking_sessions_provider_idx
  on public.tracking_sessions (provider, provider_flight_id);

create index push_devices_user_enabled_idx
  on public.push_devices (user_id, push_enabled);

create index friend_invites_inviter_idx
  on public.friend_invites (inviter_user_id, status, expires_at desc);

create index friend_relationships_user_a_idx
  on public.friend_relationships (user_a, relationship_status);

create index friend_relationships_user_b_idx
  on public.friend_relationships (user_b, relationship_status);

create index friend_permissions_viewer_idx
  on public.friend_permissions (viewer_user_id, owner_user_id);

create index notifications_user_created_idx
  on public.notifications (user_id, created_at desc);

create index notifications_tracking_idx
  on public.notifications (tracking_session_id, created_at desc);

create index entitlements_user_active_idx
  on public.entitlements (user_id, is_active, entitlement_key);

create trigger profiles_touch_updated_at
before update on public.profiles
for each row
execute function public.runwy_touch_updated_at();

create trigger user_settings_touch_updated_at
before update on public.user_settings
for each row
execute function public.runwy_touch_updated_at();

create trigger user_flights_touch_updated_at
before update on public.user_flights
for each row
execute function public.runwy_touch_updated_at();

create trigger tracking_sessions_touch_updated_at
before update on public.tracking_sessions
for each row
execute function public.runwy_touch_updated_at();

create trigger live_snapshots_touch_updated_at
before update on public.live_snapshots
for each row
execute function public.runwy_touch_updated_at();

create trigger push_devices_touch_updated_at
before update on public.push_devices
for each row
execute function public.runwy_touch_updated_at();

create trigger friend_invites_touch_updated_at
before update on public.friend_invites
for each row
execute function public.runwy_touch_updated_at();

create trigger friend_relationships_touch_updated_at
before update on public.friend_relationships
for each row
execute function public.runwy_touch_updated_at();

create trigger friend_permissions_touch_updated_at
before update on public.friend_permissions
for each row
execute function public.runwy_touch_updated_at();

create trigger friend_permissions_validate_pair
before insert or update on public.friend_permissions
for each row
execute function public.runwy_validate_friend_permission_pair();

create trigger notifications_touch_updated_at
before update on public.notifications
for each row
execute function public.runwy_touch_updated_at();

create trigger entitlements_touch_updated_at
before update on public.entitlements
for each row
execute function public.runwy_touch_updated_at();

alter table public.profiles enable row level security;
alter table public.user_settings enable row level security;
alter table public.user_flights enable row level security;
alter table public.tracking_sessions enable row level security;
alter table public.live_snapshots enable row level security;
alter table public.push_devices enable row level security;
alter table public.friend_invites enable row level security;
alter table public.friend_relationships enable row level security;
alter table public.friend_permissions enable row level security;
alter table public.notifications enable row level security;
alter table public.entitlements enable row level security;

create policy "profiles_select_self"
on public.profiles
for select
to authenticated
using (auth.uid() = user_id);

create policy "profiles_insert_self"
on public.profiles
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "profiles_update_self"
on public.profiles
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "user_settings_select_self"
on public.user_settings
for select
to authenticated
using (auth.uid() = user_id);

create policy "user_settings_insert_self"
on public.user_settings
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "user_settings_update_self"
on public.user_settings
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "user_flights_select_owner"
on public.user_flights
for select
to authenticated
using (auth.uid() = user_id);

create policy "user_flights_insert_owner"
on public.user_flights
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "user_flights_update_owner"
on public.user_flights
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "user_flights_delete_owner"
on public.user_flights
for delete
to authenticated
using (auth.uid() = user_id);

create policy "tracking_sessions_select_owner"
on public.tracking_sessions
for select
to authenticated
using (auth.uid() = owner_user_id);

create policy "tracking_sessions_insert_owner"
on public.tracking_sessions
for insert
to authenticated
with check (auth.uid() = owner_user_id);

create policy "tracking_sessions_update_owner"
on public.tracking_sessions
for update
to authenticated
using (auth.uid() = owner_user_id)
with check (auth.uid() = owner_user_id);

create policy "live_snapshots_select_owner"
on public.live_snapshots
for select
to authenticated
using (
  exists (
    select 1
    from public.tracking_sessions ts
    where ts.id = public.live_snapshots.tracking_session_id
      and ts.owner_user_id = auth.uid()
  )
);

create policy "push_devices_select_self"
on public.push_devices
for select
to authenticated
using (auth.uid() = user_id);

create policy "push_devices_insert_self"
on public.push_devices
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "push_devices_update_self"
on public.push_devices
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "push_devices_delete_self"
on public.push_devices
for delete
to authenticated
using (auth.uid() = user_id);

create policy "friend_invites_select_own"
on public.friend_invites
for select
to authenticated
using (
  auth.uid() = inviter_user_id
  or auth.uid() = accepted_by_user_id
);

create policy "friend_relationships_select_members"
on public.friend_relationships
for select
to authenticated
using (
  auth.uid() = user_a
  or auth.uid() = user_b
);

create policy "friend_permissions_select_members"
on public.friend_permissions
for select
to authenticated
using (
  auth.uid() = owner_user_id
  or auth.uid() = viewer_user_id
);

create policy "friend_permissions_update_owner"
on public.friend_permissions
for update
to authenticated
using (auth.uid() = owner_user_id)
with check (auth.uid() = owner_user_id);

create policy "notifications_select_self"
on public.notifications
for select
to authenticated
using (auth.uid() = user_id);

create policy "notifications_update_self"
on public.notifications
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "entitlements_select_self"
on public.entitlements
for select
to authenticated
using (auth.uid() = user_id);

commit;
