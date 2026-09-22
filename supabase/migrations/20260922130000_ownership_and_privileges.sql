-- Forward-only hardening. Existing user records and compatibility columns remain.
set local lock_timeout = '5s';

create table if not exists runwy_security.legacy_migration_history (
  version text primary key,name text,statements text[],archived_at timestamptz not null default now()
);
alter table runwy_security.legacy_migration_history enable row level security;

alter table public.tracking_sessions
  add constraint tracking_sessions_id_owner_unique unique (id, owner_user_id);
alter table public.user_flights
  add constraint user_flights_tracking_owner_fkey
  foreign key (tracking_session_id, user_id)
  references public.tracking_sessions(id, owner_user_id)
  on delete set null (tracking_session_id);

-- Canonical history is not disposable while a user still has a saved record.
alter table public.user_flights drop constraint user_flights_flight_instance_id_fkey;
alter table public.user_flights add constraint user_flights_flight_instance_id_fkey
  foreign key (flight_instance_id) references public.flight_instances(id) on delete restrict;

-- Keep the existing cleanup behavior, with an independent owner check at its
-- privileged write. pg_get_functiondef avoids replacing newer delivery logic.
do $$
declare definition text;
begin
  select pg_get_functiondef('public.cleanup_deleted_user_flight_notifications()'::regprocedure)
    into definition;
  if position('where id = new.tracking_session_id;' in definition) = 0 then
    raise exception 'Unexpected cleanup trigger definition; review before applying';
  end if;
  definition := replace(definition, 'where id = new.tracking_session_id;',
    'where id = new.tracking_session_id and owner_user_id = new.user_id;');
  execute definition;
end $$;

drop policy if exists tracking_sessions_select_visible on public.tracking_sessions;
drop policy if exists tracking_sessions_select_owner on public.tracking_sessions;
create policy tracking_sessions_select_owner on public.tracking_sessions
  for select to authenticated using (owner_user_id = (select auth.uid()));
drop policy if exists live_snapshots_select_visible on public.live_snapshots;
drop policy if exists live_snapshots_select_owner on public.live_snapshots;
create policy live_snapshots_select_owner on public.live_snapshots
  for select to authenticated using (exists (
    select 1 from public.tracking_sessions ts
    where ts.id = live_snapshots.tracking_session_id and ts.owner_user_id = (select auth.uid())
  ));

alter table public.friend_relationships add constraint friend_relationships_canonical_order
  check (user_a < user_b);
create or replace function public.runwy_validate_friend_permission_pair()
returns trigger language plpgsql set search_path = '' as $$
begin
  if not exists (
    select 1 from public.friend_relationships r where r.id = new.relationship_id
    and ((r.user_a = new.owner_user_id and r.user_b = new.viewer_user_id)
      or (r.user_b = new.owner_user_id and r.user_a = new.viewer_user_id))
  ) then raise exception 'Permission users must match relationship members' using errcode = '23514'; end if;
  return new;
end $$;
create trigger friend_permissions_validate_pair before insert or update
  on public.friend_permissions for each row execute function public.runwy_validate_friend_permission_pair();

-- Stop automatically exposing future public objects to API roles.
alter default privileges for role postgres in schema public revoke all on tables from anon, authenticated;
alter default privileges for role postgres in schema public revoke all on sequences from anon, authenticated;
alter default privileges for role postgres in schema public revoke execute on functions from public, anon, authenticated;
-- Global function defaults must also be revoked: per-schema defaults cannot
-- subtract PostgreSQL's default PUBLIC EXECUTE grant.
alter default privileges for role postgres revoke execute on functions from public, anon, authenticated;

do $$
declare obj record;
begin
  for obj in select tablename from pg_tables where schemaname = 'public' loop
    execute format('revoke all on table public.%I from public, anon, authenticated', obj.tablename);
  end loop;
end $$;
grant select, insert, update on public.profiles, public.user_settings, public.ticket_souvenirs to authenticated;
grant select, insert, update, delete on public.user_flights to authenticated;
-- Existing device clients remain compatible until all registrations use the
-- server operation. Their owner policies still apply.
grant select, insert, update, delete on public.device_tokens, public.push_devices to authenticated;
grant select on public.tracking_sessions, public.live_snapshots, public.live_activity_tokens,
  public.friend_relationships, public.friend_permissions, public.friend_invites,
  public.notifications, public.notification_deliveries, public.entitlements,
  public.user_achievements to authenticated;
grant update (read_at) on public.notifications to authenticated;
grant update (celebrated_at) on public.user_achievements to authenticated;
grant update (share_scope, can_view_live, can_view_history, can_receive_alerts,
  notify_departure, notify_arrival, notify_delay, notify_gate_change) on public.friend_permissions to authenticated;

drop policy if exists user_flights_own_select on public.user_flights;
drop policy if exists user_flights_own_insert on public.user_flights;
drop policy if exists user_flights_own_update on public.user_flights;
drop policy if exists user_flights_own_delete on public.user_flights;

-- Trigger functions are not client RPCs. Legacy ordinary RPCs referenced tables
-- removed by the reset; revoke them instead of reviving obsolete dependencies.
revoke all on function public.can_access_tracking_session(uuid,uuid),
  public.is_tracking_session_owner(uuid,uuid),
  public.sync_user_backup_after_past_flight_import(uuid),
  public.cleanup_deleted_user_flight_notification_artifacts(),
  public.cleanup_deleted_user_flight_notifications(), public.handle_new_auth_user(),
  public.increment_live_snapshot_version(), public.prevent_stale_ticket_souvenir_write(),
  public.reconcile_user_flight_history_occurrence(), public.runwy_touch_updated_at(),
  public.set_updated_at_timestamp(), public.touch_tracking_session_from_snapshot(),
  public.touch_user_achievement_updated_at(), public.runwy_validate_friend_permission_pair()
  from public, anon, authenticated;

create or replace function public.runwy_preserve_tracked_flight_fields()
returns trigger language plpgsql set search_path = '' as $$
begin
  if current_user = 'authenticated' then new.flight_instance_id := old.flight_instance_id; end if;
  -- Backend writes remain revision-controlled. Old app versions may include
  -- stale provider values; retain server values rather than failing their save.
  if current_user = 'authenticated' and old.tracking_session_id is not null then
    new := jsonb_populate_record(new, jsonb_build_object(
      'flight_instance_id', old.flight_instance_id,
      'tracking_session_id', old.tracking_session_id,
      'status', old.status, 'estimated_departure', old.estimated_departure,
      'estimated_arrival', old.estimated_arrival, 'actual_departure', old.actual_departure,
      'actual_arrival', old.actual_arrival, 'departure_terminal', old.departure_terminal,
      'departure_gate', old.departure_gate, 'arrival_terminal', old.arrival_terminal,
      'arrival_gate', old.arrival_gate, 'baggage_claim', old.baggage_claim,
      'aircraft_type', old.aircraft_type, 'delay_minutes', old.delay_minutes,
      'distance_km', old.distance_km, 'flight_time_minutes', old.flight_time_minutes,
      'route_polyline', old.route_polyline, 'tracked_snapshot', old.tracked_snapshot,
      'provider_name', old.provider_name, 'provider_flight_id', old.provider_flight_id));
  end if;
  if new.notification_enabled is distinct from old.notification_enabled then
    new.notifications_enabled := new.notification_enabled;
  elsif new.notifications_enabled is distinct from old.notifications_enabled then
    new.notification_enabled := new.notifications_enabled;
  end if;
  return new;
end $$;
create trigger aa_preserve_tracked_flight_fields before update on public.user_flights
  for each row execute function public.runwy_preserve_tracked_flight_fields();
revoke all on function public.runwy_preserve_tracked_flight_fields() from public,anon,authenticated;

create or replace function public.runwy_skip_unchanged_update()
returns trigger language plpgsql set search_path = '' as $$
begin
  -- Backend timestamps include explicit display acknowledgements used by
  -- deletion reconciliation; only suppress redundant direct client sync.
  if current_user='authenticated' and (to_jsonb(new) - 'updated_at') = (to_jsonb(old) - 'updated_at') then return null; end if;
  return new;
end $$;
create trigger zz_skip_unchanged_update before update on public.user_flights
  for each row execute function public.runwy_skip_unchanged_update();
create trigger zz_skip_unchanged_update before update on public.user_settings
  for each row execute function public.runwy_skip_unchanged_update();
create trigger zz_skip_unchanged_update before update on public.profiles
  for each row execute function public.runwy_skip_unchanged_update();
revoke all on function public.runwy_skip_unchanged_update() from public,anon,authenticated;

-- Leading keys used by fanout, ownership validation, and FK cleanup. Small
-- current tables allow these transactional builds within the lock timeout.
create index user_flights_instance_idx on public.user_flights(flight_instance_id) where flight_instance_id is not null;
create index user_flights_session_idx on public.user_flights(tracking_session_id) where tracking_session_id is not null;
create index notification_deliveries_instance_idx on public.notification_deliveries(flight_instance_id);
create index notification_deliveries_event_idx on public.notification_deliveries(flight_event_id);
create index friend_permissions_relationship_idx on public.friend_permissions(relationship_id);
create index live_activity_tokens_session_idx on public.live_activity_tokens(tracking_session_id) where tracking_session_id is not null;
create index notification_delivery_tokens_device_idx on public.notification_delivery_tokens(device_token_id);
create index flight_instance_aliases_instance_idx on public.flight_instance_aliases(flight_instance_id);

-- Only the owner-readable projection participates in Postgres Changes.
do $$ begin
  if not exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
    create publication supabase_realtime;
  end if;
  if not exists (select 1 from pg_publication_tables where pubname = 'supabase_realtime'
    and schemaname = 'public' and tablename = 'live_snapshots') then
    alter publication supabase_realtime add table public.live_snapshots;
  end if;
end $$;
