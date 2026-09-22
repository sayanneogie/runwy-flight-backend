alter table public.user_flights add constraint user_flights_id_user_unique unique(id,user_id);
alter table public.tracking_sessions add column flight_instance_id uuid
  references public.flight_instances(id) on delete set null;
update public.tracking_sessions set flight_instance_id=(metadata_json->>'sharedFlightInstanceId')::uuid
  where metadata_json->>'sharedFlightInstanceId' is not null;
create index tracking_sessions_instance_idx on public.tracking_sessions(flight_instance_id) where flight_instance_id is not null;
create function public.runwy_project_session_instance()
returns trigger language plpgsql set search_path='' as $$
begin
  new.flight_instance_id:=nullif(new.metadata_json->>'sharedFlightInstanceId','')::uuid;
  return new;
end $$;
create trigger project_session_instance before insert or update of metadata_json on public.tracking_sessions
  for each row execute function public.runwy_project_session_instance();
revoke all on function public.runwy_project_session_instance() from public,anon,authenticated;
create table public.user_flight_aliases (
  alias_id uuid primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  canonical_id uuid not null,
  created_at timestamptz not null default now(),
  check (alias_id<>canonical_id),
  foreign key(canonical_id,user_id) references public.user_flights(id,user_id) on delete cascade
);
create index user_flight_aliases_canonical_idx on public.user_flight_aliases(canonical_id,user_id);
create index user_flight_aliases_user_idx on public.user_flight_aliases(user_id);
alter table public.user_flight_aliases enable row level security;
grant all on public.user_flight_aliases to service_role;
grant select on public.user_flight_aliases to authenticated;
create policy user_flight_aliases_owner on public.user_flight_aliases for select to authenticated
  using(user_id=(select auth.uid()));

create function public.runwy_link_user_flight(p_user uuid,p_row uuid,p_instance uuid,p_patch jsonb default '{}')
returns public.user_flights language plpgsql security definer set search_path='' as $$
declare source public.user_flights; survivor public.user_flights; target uuid;
begin
  if p_user is null or p_instance is null then raise exception 'Missing flight identity' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended(p_user::text||':'||p_instance::text,0));
  select coalesce((select canonical_id from public.user_flight_aliases where alias_id=p_row and user_id=p_user),p_row) into target;
  select * into source from public.user_flights where id=target and user_id=p_user for update;
  if not found then raise exception 'Saved flight not found' using errcode='22023'; end if;
  if source.flight_instance_id is not null and source.flight_instance_id<>p_instance then
    raise exception 'Saved flight already linked to a different occurrence' using errcode='23514'; end if;
  select * into survivor from public.user_flights where user_id=p_user and flight_instance_id=p_instance for update;
  if survivor.id is not null and survivor.id<>source.id then
    -- Move references before tombstoning so the cleanup trigger cannot delete
    -- the survivor's pending notifications or pause its session.
    update public.notification_deliveries set user_flight_id=survivor.id where user_flight_id=source.id;
    update public.user_achievements set user_flight_id=survivor.id where user_flight_id=source.id and user_id=p_user;
    delete from public.friend_flight_shares s where s.user_flight_id=source.id and exists(
      select 1 from public.friend_flight_shares t where t.relationship_id=s.relationship_id and t.user_flight_id=survivor.id);
    update public.friend_flight_shares set user_flight_id=survivor.id where user_flight_id=source.id;
    update public.user_flights set tracking_session_id=null,flight_instance_id=null,
      deleted_at=now(),lifecycle_state='deleted' where id=source.id;
    update public.user_flights set
      tracking_session_id=coalesce(tracking_session_id,source.tracking_session_id),
      user_label=coalesce(user_label,source.user_label),
      -- A trip/import represents the user's journey, rather than watch-only tracking.
      source_type=case when source_type='tracked' and source.source_type<>'tracked' then source.source_type else source_type end,
      alert_settings_json=coalesce(source.alert_settings_json,alert_settings_json)
      where id=survivor.id;
    update public.user_flight_aliases set canonical_id=survivor.id where canonical_id=source.id;
    insert into public.user_flight_aliases(alias_id,user_id,canonical_id) values(source.id,p_user,survivor.id)
      on conflict(alias_id) do update set canonical_id=excluded.canonical_id;
    target:=survivor.id;
  end if;
  update public.user_flights uf set flight_instance_id=p_instance,
    notification_enabled=coalesce((p_patch->>'notificationEnabled')::boolean,uf.notification_enabled),
    notifications_enabled=coalesce((p_patch->>'notificationEnabled')::boolean,uf.notifications_enabled),
    alert_preferences=coalesce(p_patch->'alertPreferences',uf.alert_preferences),
    aircraft_type=coalesce(nullif(trim(coalesce(fi.normalized_data->>'aircraftType',fi.normalized_data->>'aircraft_type','')),''),uf.aircraft_type),
    status=coalesce(fi.status,uf.status),provider_name=coalesce(fi.provider,uf.provider_name),
    provider_flight_id=coalesce(fi.provider_flight_id,uf.provider_flight_id),updated_at=now()
    from public.flight_instances fi where uf.id=target and uf.user_id=p_user and fi.id=p_instance;
  select * into survivor from public.user_flights where id=target and user_id=p_user;
  return survivor;
end $$;
revoke all on function public.runwy_link_user_flight(uuid,uuid,uuid,jsonb) from public,anon,authenticated;
grant execute on function public.runwy_link_user_flight(uuid,uuid,uuid,jsonb) to service_role;

-- Called inside the same transaction as a tracking upsert. Reconcile the
-- session identity before PostgreSQL checks the separate canonical unique key.
create function public.runwy_prepare_tracking_user_flight(p_user uuid,p_session uuid,p_instance uuid)
returns void language plpgsql security definer set search_path='' as $$
declare source_id uuid;
begin
  perform pg_advisory_xact_lock(hashtextextended(p_user::text||':'||p_instance::text,0));
  if not exists(select 1 from public.tracking_sessions where id=p_session and owner_user_id=p_user) then
    raise exception 'Tracking session owner mismatch' using errcode='23503'; end if;
  select id into source_id from public.user_flights where user_id=p_user and tracking_session_id=p_session for update;
  if source_id is not null then perform public.runwy_link_user_flight(p_user,source_id,p_instance,'{}'); end if;
  update public.user_flights set tracking_session_id=p_session where user_id=p_user and flight_instance_id=p_instance
    and tracking_session_id is distinct from p_session;
end $$;
revoke all on function public.runwy_prepare_tracking_user_flight(uuid,uuid,uuid) from public,anon,authenticated;
grant execute on function public.runwy_prepare_tracking_user_flight(uuid,uuid,uuid) to service_role;

create function public.runwy_lock_flight_identity()
returns trigger language plpgsql set search_path='' as $$
begin
  if current_user='authenticated' and new.flight_instance_id is not null then
    raise exception 'Canonical links are managed by the tracking service' using errcode='42501';
  end if;
  if new.flight_instance_id is not null then
    perform pg_advisory_xact_lock(hashtextextended(new.user_id::text||':'||new.flight_instance_id::text,0));
  end if;
  return new;
end $$;
create trigger aa_lock_flight_identity before insert on public.user_flights
  for each row execute function public.runwy_lock_flight_identity();
revoke all on function public.runwy_lock_flight_identity() from public,anon,authenticated;

-- Old devices can retry an obsolete local ID after reconciliation. Redirect
-- their editable preferences; never resurrect the discarded occurrence.
create function public.runwy_redirect_user_flight_alias()
returns trigger language plpgsql set search_path='' as $$
declare target uuid;
begin
  select canonical_id into target from public.user_flight_aliases
    where alias_id=new.id and user_id=new.user_id;
  if target is null then return new; end if;
  update public.user_flights set notification_enabled=new.notification_enabled,
    notifications_enabled=new.notifications_enabled,alert_preferences=new.alert_preferences,
    alert_settings_json=new.alert_settings_json,
    deleted_at=case when new.deleted_at is not null then new.deleted_at else deleted_at end,
    lifecycle_state=case when new.deleted_at is not null then 'deleted' else lifecycle_state end
    where id=target and user_id=new.user_id;
  return null;
end $$;
create trigger aa_redirect_user_flight_alias before insert or update on public.user_flights
  for each row execute function public.runwy_redirect_user_flight_alias();
revoke all on function public.runwy_redirect_user_flight_alias() from public,anon,authenticated;
