-- One sharing predicate is used by overview, tracking detail and notification
-- delivery. Private flights stay private; selected scope needs explicit rows.
create table public.friend_flight_shares (
  relationship_id uuid not null references public.friend_relationships(id) on delete cascade,
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  user_flight_id uuid not null references public.user_flights(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (relationship_id, user_flight_id)
);
create index friend_flight_shares_flight_idx on public.friend_flight_shares(user_flight_id);
alter table public.friend_flight_shares enable row level security;
grant all on public.friend_flight_shares to service_role;

create table public.circle_notification_preferences (
  relationship_id uuid not null references public.friend_relationships(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  enabled boolean not null default true,
  notify_departure boolean not null default true,
  notify_arrival boolean not null default true,
  notify_delay boolean not null default true,
  notify_gate_change boolean not null default true,
  notify_baggage boolean not null default true,
  updated_at timestamptz not null default now(),
  primary key (relationship_id,user_id)
);
create index circle_notification_preferences_user_idx on public.circle_notification_preferences(user_id);
alter table public.circle_notification_preferences enable row level security;
grant all on public.circle_notification_preferences to service_role;
grant select on public.circle_notification_preferences to authenticated;
create policy circle_notification_preferences_self on public.circle_notification_preferences
  for select to authenticated using (user_id=(select auth.uid()));
alter table public.friend_permissions add column notify_baggage boolean not null default true;
grant update(notify_baggage) on public.friend_permissions to authenticated;

create function public.runwy_circle_flight_allowed(p_flight_id uuid,p_viewer uuid,p_for_alert boolean default false)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.user_flights uf
    join public.friend_permissions fp on fp.owner_user_id=uf.user_id and fp.viewer_user_id=p_viewer
    join public.friend_relationships fr on fr.id=fp.relationship_id
    where uf.id=p_flight_id and uf.deleted_at is null and uf.lifecycle_state<>'deleted'
      and uf.visibility='circle' and fr.relationship_status='active'
      and ((fr.user_a=fp.owner_user_id and fr.user_b=p_viewer)
        or (fr.user_b=fp.owner_user_id and fr.user_a=p_viewer))
      and (fp.share_scope='all_flights'
        or (fp.share_scope='future_flights' and (uf.lifecycle_state in ('upcoming','active')
          or (p_for_alert and coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,uf.scheduled_departure)>now()-interval '24 hours')))
        or (fp.share_scope='selected_flights' and exists (
          select 1 from public.friend_flight_shares s where s.relationship_id=fp.relationship_id
            and s.user_flight_id=uf.id and s.owner_user_id=uf.user_id)))
      and (case when p_for_alert then fp.can_view_live and fp.can_receive_alerts
          and coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,uf.scheduled_departure)>now()-interval '24 hours'
        when uf.lifecycle_state in ('landed','archived') then fp.can_view_history
        when uf.lifecycle_state='active' then fp.can_view_live
        else true end)
  );
$$;
revoke all on function public.runwy_circle_flight_allowed(uuid,uuid,boolean) from public,anon,authenticated;
grant execute on function public.runwy_circle_flight_allowed(uuid,uuid,boolean) to service_role;

create function public.runwy_circle_alert_allowed(p_relationship uuid,p_viewer uuid,p_event text)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.friend_permissions fp
    join public.friend_relationships fr on fr.id=fp.relationship_id and fr.relationship_status='active'
    left join public.circle_notification_preferences np on np.relationship_id=fp.relationship_id and np.user_id=p_viewer
    where fp.relationship_id=p_relationship and fp.viewer_user_id=p_viewer
      and fp.can_receive_alerts and fp.can_view_live and coalesce(np.enabled,true)
      and case upper(p_event)
        when 'DELAYED' then fp.notify_delay and coalesce(np.notify_delay,true)
        when 'RESCHEDULED' then fp.notify_delay and coalesce(np.notify_delay,true)
        when 'CANCELLED' then fp.notify_delay and coalesce(np.notify_delay,true)
        when 'DIVERTED' then fp.notify_delay and coalesce(np.notify_delay,true)
        when 'GATE_CHANGED' then fp.notify_gate_change and coalesce(np.notify_gate_change,true)
        when 'TERMINAL_CHANGED' then fp.notify_gate_change and coalesce(np.notify_gate_change,true)
        when 'BAGGAGE_BELT_ASSIGNED' then fp.notify_baggage and coalesce(np.notify_baggage,true)
        when 'BAGGAGE_BELT_CHANGED' then fp.notify_baggage and coalesce(np.notify_baggage,true)
        when 'TRIP_STARTING' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'BOARDING' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'TAXIING' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'DEPARTED' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'TAKEOFF_ROLL' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'AIRBORNE' then fp.notify_departure and coalesce(np.notify_departure,true)
        when 'LANDED' then fp.notify_arrival and coalesce(np.notify_arrival,true)
        when 'ARRIVED' then fp.notify_arrival and coalesce(np.notify_arrival,true)
        when 'TAXI_IN' then fp.notify_arrival and coalesce(np.notify_arrival,true)
        when 'ARRIVED_AT_GATE' then fp.notify_arrival and coalesce(np.notify_arrival,true)
        else false end
  );
$$;
revoke all on function public.runwy_circle_alert_allowed(uuid,uuid,text) from public,anon,authenticated;
grant execute on function public.runwy_circle_alert_allowed(uuid,uuid,text) to service_role;

create function public.runwy_remove_circle_member(p_relationship_id uuid)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare actor uuid := auth.uid();
begin
  if actor is null then raise exception 'Sign in required' using errcode='42501'; end if;
  perform 1 from public.friend_relationships where id=p_relationship_id
    and actor in (user_a,user_b) for update;
  if not found then raise exception 'Relationship not found' using errcode='42501'; end if;
  update public.friend_relationships set relationship_status='removed',updated_at=now() where id=p_relationship_id;
  update public.friend_permissions set can_view_live=false,can_view_history=false,can_receive_alerts=false
    where relationship_id=p_relationship_id;
  delete from public.friend_flight_shares where relationship_id=p_relationship_id;
  -- Revoke already queued circle notifications as well as future fanout.
  delete from public.notifications where friend_relationship_id=p_relationship_id;
  return jsonb_build_object('removed',true);
end $$;
revoke all on function public.runwy_remove_circle_member(uuid) from public,anon;
grant execute on function public.runwy_remove_circle_member(uuid) to authenticated;

create function public.runwy_update_circle_notifications(p_relationship_id uuid,p_preferences jsonb)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare actor uuid:=auth.uid(); result jsonb;
begin
  if actor is null or not exists(select 1 from public.friend_relationships
    where id=p_relationship_id and actor in(user_a,user_b) and relationship_status='active')
    then raise exception 'Active relationship required' using errcode='42501'; end if;
  if jsonb_typeof(p_preferences)<>'object' then raise exception 'Invalid preferences' using errcode='22023'; end if;
  insert into public.circle_notification_preferences(relationship_id,user_id,enabled,
    notify_departure,notify_arrival,notify_delay,notify_gate_change,notify_baggage)
    values(p_relationship_id,actor,coalesce((p_preferences->>'enabled')::boolean,true),
      coalesce((p_preferences->>'notify_departure')::boolean,true),coalesce((p_preferences->>'notify_arrival')::boolean,true),
      coalesce((p_preferences->>'notify_delay')::boolean,true),coalesce((p_preferences->>'notify_gate_change')::boolean,true),
      coalesce((p_preferences->>'notify_baggage')::boolean,true))
    on conflict(relationship_id,user_id) do update set enabled=excluded.enabled,
      notify_departure=excluded.notify_departure,notify_arrival=excluded.notify_arrival,
      notify_delay=excluded.notify_delay,notify_gate_change=excluded.notify_gate_change,
      notify_baggage=excluded.notify_baggage,updated_at=now();
  select to_jsonb(p) into result from public.circle_notification_preferences p
    where relationship_id=p_relationship_id and user_id=actor;
  return result;
end $$;
revoke all on function public.runwy_update_circle_notifications(uuid,jsonb) from public,anon;
grant execute on function public.runwy_update_circle_notifications(uuid,jsonb) to authenticated;

-- Owner-only selection, with a composite ownership check for every flight.
create function public.runwy_set_circle_flights(p_relationship_id uuid,p_flight_ids uuid[])
returns jsonb language plpgsql security definer set search_path = '' as $$
declare actor uuid:=auth.uid(); viewer uuid;
begin
  select case when user_a=actor then user_b else user_a end into viewer
    from public.friend_relationships where id=p_relationship_id and actor in(user_a,user_b)
      and relationship_status='active' for update;
  if actor is null or viewer is null then raise exception 'Active relationship required' using errcode='42501'; end if;
  if cardinality(p_flight_ids)>500 or exists(select 1 from unnest(p_flight_ids) id
    where not exists(select 1 from public.user_flights uf where uf.id=id and uf.user_id=actor
      and uf.deleted_at is null)) then raise exception 'Invalid flight selection' using errcode='22023'; end if;
  delete from public.friend_flight_shares where relationship_id=p_relationship_id and owner_user_id=actor;
  insert into public.friend_flight_shares(relationship_id,owner_user_id,user_flight_id)
    select p_relationship_id,actor,id from (select distinct unnest(p_flight_ids) id) ids;
  update public.friend_permissions set share_scope='selected_flights'
    where relationship_id=p_relationship_id and owner_user_id=actor and viewer_user_id=viewer;
  -- Explicit selections still respect the user's global private/circle choice.
  return jsonb_build_object('selected',coalesce(cardinality(p_flight_ids),0));
end $$;
revoke all on function public.runwy_set_circle_flights(uuid,uuid[]) from public,anon;
grant execute on function public.runwy_set_circle_flights(uuid,uuid[]) to authenticated;

-- The Edge handler verifies its user; only service_role can supply an actor.
create function public.runwy_accept_circle_invite(p_token_hash text,p_actor uuid)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare inv public.friend_invites; rel public.friend_relationships; a uuid; b uuid;
  created boolean:=false;
begin
  if p_actor is null or p_token_hash !~ '^[a-f0-9]{64}$' then raise exception 'Invalid invite' using errcode='22023'; end if;
  select * into inv from public.friend_invites where token_hash=p_token_hash for update;
  if not found or inv.inviter_user_id=p_actor then raise exception 'Invite unavailable' using errcode='22023'; end if;
  if inv.status='accepted' and inv.accepted_by_user_id=p_actor then
    select * into rel from public.friend_relationships where user_a=least(inv.inviter_user_id,p_actor)
      and user_b=greatest(inv.inviter_user_id,p_actor);
    if rel.relationship_status<>'active' then raise exception 'Invite unavailable' using errcode='22023'; end if;
    return jsonb_build_object('relationship_created',false,'relationship_id',rel.id,'inviter_user_id',inv.inviter_user_id,'share_scope',inv.default_share_scope);
  end if;
  if inv.status<>'pending' or inv.expires_at<=now() then raise exception 'Invite unavailable' using errcode='22023'; end if;
  a:=least(inv.inviter_user_id,p_actor); b:=greatest(inv.inviter_user_id,p_actor);
  -- Different invites for the same pair serialize on the same key as well.
  perform pg_advisory_xact_lock(hashtextextended(a::text||':'||b::text,0));
  select * into rel from public.friend_relationships where user_a=a and user_b=b for update;
  if found and rel.relationship_status='blocked' then raise exception 'Invite unavailable' using errcode='22023'; end if;
  if rel.id is null then
    insert into public.friend_relationships(user_a,user_b,created_by_user_id) values(a,b,p_actor) returning * into rel;
    created:=true;
  elsif rel.relationship_status='removed' then
    update public.friend_relationships set relationship_status='active',updated_at=now() where id=rel.id;
    -- A new invitation and acceptance explicitly renew consent in both directions.
    delete from public.friend_permissions where relationship_id=rel.id;
  end if;
  insert into public.friend_permissions(relationship_id,owner_user_id,viewer_user_id,share_scope,can_view_live,can_view_history,can_receive_alerts)
    values(rel.id,inv.inviter_user_id,p_actor,inv.default_share_scope,true,false,true),
      (rel.id,p_actor,inv.inviter_user_id,'future_flights',true,false,true)
    on conflict(owner_user_id,viewer_user_id) do nothing;
  update public.friend_invites set status='accepted',accepted_by_user_id=p_actor,accepted_at=now(),updated_at=now() where id=inv.id;
  return jsonb_build_object('relationship_created',created,'relationship_id',rel.id,'inviter_user_id',inv.inviter_user_id,'share_scope',inv.default_share_scope);
end $$;
revoke all on function public.runwy_accept_circle_invite(text,uuid) from public,anon,authenticated;
grant execute on function public.runwy_accept_circle_invite(text,uuid) to service_role;

create function public.runwy_circle_overview(p_viewer uuid,p_limit integer default 100,p_offset integer default 0)
returns jsonb language sql stable security definer set search_path = '' as $$
  with members as (
    select fr.id,fp.owner_user_id as user_id,coalesce(p.display_name,'Traveler') as display_name,
      p.avatar_url as picture_url,fp.share_scope,fp.can_receive_alerts,
      coalesce(to_jsonb(np),jsonb_build_object('enabled',true,'notify_departure',true,
        'notify_arrival',true,'notify_delay',true,'notify_gate_change',true,'notify_baggage',true)) as notification_preferences
    from public.friend_relationships fr join public.friend_permissions fp on fp.relationship_id=fr.id and fp.viewer_user_id=p_viewer
    left join public.profiles p on p.user_id=fp.owner_user_id
    left join public.circle_notification_preferences np on np.relationship_id=fr.id and np.user_id=p_viewer
    where fr.relationship_status='active' and p_viewer in(fr.user_a,fr.user_b)
  ), visible as materialized (
    select uf.* from public.user_flights uf join members m on m.user_id=uf.user_id
    where public.runwy_circle_flight_allowed(uf.id,p_viewer,false)
  ), counts as (
    select user_id,count(*) filter(where scheduled_departure>=now()) as upcoming,
      count(*) filter(where lifecycle_state='active') as live from visible group by user_id
  ), page as (
    select jsonb_build_object('id',uf.id,'owner_user_id',uf.user_id,'owner_display_name',m.display_name,
      'owner_picture_url',m.picture_url,'airline_name',coalesce(uf.marketing_airline_name,uf.operating_airline_name,'Shared flight'),
      'flight_number',uf.display_flight_number,'origin_iata',uf.origin_iata,'destination_iata',uf.destination_iata,
      'route_title',uf.origin_iata||' to '||uf.destination_iata,'departure_summary','Departs '||to_char(uf.scheduled_departure at time zone 'UTC','DD Mon HH24:MI')||' UTC',
      'status_summary',coalesce(uf.status,'Upcoming'),'is_live',uf.lifecycle_state='active',
      'departure_at',uf.scheduled_departure,'arrival_at',uf.scheduled_arrival) as item
    from visible uf join members m on m.user_id=uf.user_id
    order by (uf.lifecycle_state='active') desc,uf.scheduled_departure desc,uf.id
    limit greatest(1,least(p_limit,200)) offset greatest(0,p_offset)
  ) select jsonb_build_object(
    'members',coalesce((select jsonb_agg(to_jsonb(m)||jsonb_build_object('upcoming_flight_count',coalesce(c.upcoming,0),'live_flight_count',coalesce(c.live,0)) order by m.display_name,m.id) from members m left join counts c on c.user_id=m.user_id),'[]'::jsonb),
    'shared_flights',coalesce((select jsonb_agg(item) from page),'[]'::jsonb),
    'total_shared_flights',(select count(*) from visible),
    'has_more',(select count(*) from visible)>greatest(0,p_offset)+greatest(1,least(p_limit,200)));
$$;
revoke all on function public.runwy_circle_overview(uuid,integer,integer) from public,anon,authenticated;
grant execute on function public.runwy_circle_overview(uuid,integer,integer) to service_role;

create function public.runwy_notification_delivery_allowed(p_delivery uuid)
returns boolean language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.notification_deliveries nd
    join public.user_flights uf on uf.id=nd.user_flight_id
    join public.flight_events fe on fe.id=nd.flight_event_id
    where nd.id=p_delivery and uf.deleted_at is null and uf.lifecycle_state<>'deleted'
      and (case when nd.user_id=uf.user_id then uf.notification_enabled
        else public.runwy_circle_flight_allowed(uf.id,nd.user_id,true) and exists(
          select 1 from public.friend_permissions fp where fp.owner_user_id=uf.user_id
            and fp.viewer_user_id=nd.user_id
            and public.runwy_circle_alert_allowed(fp.relationship_id,nd.user_id,fe.event_type)) end)
  );
$$;
revoke all on function public.runwy_notification_delivery_allowed(uuid) from public,anon,authenticated;
grant execute on function public.runwy_notification_delivery_allowed(uuid) to service_role;
