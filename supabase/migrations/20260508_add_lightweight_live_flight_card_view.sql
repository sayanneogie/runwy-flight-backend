create or replace view public.user_live_flight_cards
with (security_invoker = true) as
select
  uf.user_id,
  uf.id as user_flight_id,
  uf.tracking_session_id,
  ts.provider,
  ts.flight_number,
  ts.airline_code,
  ts.origin_iata,
  ts.destination_iata,
  ts.travel_date,
  ts.session_status,
  ts.last_snapshot_at,
  ls.provider_last_updated_at,
  ls.updated_at as snapshot_updated_at,
  coalesce(
    ls.canonical_snapshot_json #>> '{status}',
    ls.canonical_snapshot_json #>> '{flightStatus}'
  ) as snapshot_status,
  coalesce(
    ls.canonical_snapshot_json #>> '{departureTimes,scheduled}',
    ls.canonical_snapshot_json #>> '{departure,scheduled}',
    ls.canonical_snapshot_json #>> '{scheduledDeparture}'
  ) as scheduled_departure,
  coalesce(
    ls.canonical_snapshot_json #>> '{arrivalTimes,scheduled}',
    ls.canonical_snapshot_json #>> '{arrival,scheduled}',
    ls.canonical_snapshot_json #>> '{scheduledArrival}'
  ) as scheduled_arrival,
  coalesce(
    ls.canonical_snapshot_json #>> '{departureTimes,estimated}',
    ls.canonical_snapshot_json #>> '{departure,estimated}',
    ls.canonical_snapshot_json #>> '{estimatedDeparture}'
  ) as estimated_departure,
  coalesce(
    ls.canonical_snapshot_json #>> '{arrivalTimes,estimated}',
    ls.canonical_snapshot_json #>> '{arrival,estimated}',
    ls.canonical_snapshot_json #>> '{estimatedArrival}'
  ) as estimated_arrival,
  coalesce(
    ls.canonical_snapshot_json #>> '{livePosition,latitude}',
    ls.canonical_snapshot_json #>> '{position,latitude}'
  ) as latest_latitude,
  coalesce(
    ls.canonical_snapshot_json #>> '{livePosition,longitude}',
    ls.canonical_snapshot_json #>> '{position,longitude}'
  ) as latest_longitude,
  coalesce(
    ls.canonical_snapshot_json #>> '{livePosition,heading}',
    ls.canonical_snapshot_json #>> '{position,heading}'
  ) as latest_heading,
  coalesce(
    ls.canonical_snapshot_json #>> '{livePosition,recordedAt}',
    ls.canonical_snapshot_json #>> '{position,recordedAt}'
  ) as latest_position_recorded_at
from public.user_flights uf
join public.tracking_sessions ts
  on ts.id = uf.tracking_session_id
left join public.live_snapshots ls
  on ls.tracking_session_id = uf.tracking_session_id
where
  uf.deleted_at is null
  and uf.lifecycle_state <> 'deleted'
  and uf.tracking_session_id is not null;

comment on view public.user_live_flight_cards is
  'Lightweight authenticated flight-card projection. Use this instead of reading full live_snapshots.canonical_snapshot_json for list/map cards; keep full snapshot reads for selected detail views only.';

grant select on public.user_live_flight_cards to authenticated;
