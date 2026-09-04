-- Legacy tracking bridge rows are reused when the same saved occurrence is
-- tracked again. Before the application upsert cleared deleted_at, a reused
-- row could become active while retaining its old tombstone. The deletion
-- cleanup trigger would then pause the tracking session even though a current,
-- canonical shared-flight row still existed for the user.
--
-- Repair only bridges that have a live canonical sibling for the same flight
-- occurrence. This avoids resurrecting flights the user intentionally deleted.
with repairable_bridges as (
  select bridge.id, bridge.tracking_session_id
  from public.user_flights bridge
  where bridge.tracking_session_id is not null
    and bridge.deleted_at is not null
    and coalesce(bridge.lifecycle_state, '') <> 'deleted'
    and exists (
      select 1
      from public.user_flights canonical
      where canonical.user_id = bridge.user_id
        and canonical.id <> bridge.id
        and canonical.flight_instance_id is not null
        and canonical.deleted_at is null
        and coalesce(canonical.lifecycle_state, '') <> 'deleted'
        and regexp_replace(upper(coalesce(canonical.display_flight_number, '')), '[^A-Z0-9]', '', 'g')
          = regexp_replace(upper(coalesce(bridge.display_flight_number, '')), '[^A-Z0-9]', '', 'g')
        and upper(coalesce(canonical.origin_iata, '')) = upper(coalesce(bridge.origin_iata, ''))
        and upper(coalesce(canonical.destination_iata, '')) = upper(coalesce(bridge.destination_iata, ''))
        and canonical.scheduled_departure is not null
        and bridge.scheduled_departure is not null
        and abs(extract(epoch from (canonical.scheduled_departure - bridge.scheduled_departure))) <= 1800
    )
),
reactivated as (
  update public.user_flights bridge
  set deleted_at = null,
      updated_at = now()
  from repairable_bridges repair
  where bridge.id = repair.id
  returning bridge.tracking_session_id
)
update public.tracking_sessions session
set session_status = 'paused',
    next_poll_after = null,
    polling_stopped_reason = 'shared_flight_instance_owns_provider_refresh',
    updated_at = now()
where session.id in (select tracking_session_id from reactivated);
