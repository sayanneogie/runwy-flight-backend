-- Stale lifecycle labels must not expose past flights through future-only scope.
create or replace function public.runwy_circle_flight_allowed(p_flight_id uuid,p_viewer uuid,p_for_alert boolean default false)
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
        or (fp.share_scope='future_flights' and ((uf.lifecycle_state in ('upcoming','active')
            and coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,
              uf.scheduled_departure + case when uf.lifecycle_state='active' then interval '24 hours' else interval '0' end)>=now())
          or (p_for_alert and coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,uf.scheduled_departure)>now()-interval '24 hours')))
        or (fp.share_scope='selected_flights' and exists (
          select 1 from public.friend_flight_shares s where s.relationship_id=fp.relationship_id
            and s.user_flight_id=uf.id and s.owner_user_id=uf.user_id)))
      and (case when p_for_alert then fp.can_view_live and fp.can_receive_alerts
          and coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,uf.scheduled_departure)>now()-interval '24 hours'
        when uf.lifecycle_state in ('landed','archived')
          or coalesce(uf.actual_arrival,uf.estimated_arrival,uf.scheduled_arrival,
            uf.scheduled_departure + case when uf.lifecycle_state='active' then interval '24 hours' else interval '0' end)<now()
          then fp.can_view_history
        when uf.lifecycle_state='active' then fp.can_view_live
        else true end)
  );
$$;
