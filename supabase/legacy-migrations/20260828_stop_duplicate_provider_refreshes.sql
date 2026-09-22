-- Shared flight instances own provider refreshes after the legacy tracking
-- bridge has linked the two records. Keeping both schedulers active causes the
-- same FlightAware flight to be fetched twice and can feed realtime refresh
-- loops in connected clients.
update public.tracking_sessions
set
  metadata_json = coalesce(metadata_json, '{}'::jsonb) ||
    jsonb_build_object('providerRefreshOwner', 'shared_flight_instance'),
  session_status = 'paused',
  next_poll_after = null,
  polling_stopped_reason = 'shared_flight_instance_owns_provider_refresh',
  updated_at = now()
where metadata_json ? 'sharedFlightInstanceId';

-- Old test sessions outside the live tracking window must not count against
-- the production safety cap or wake a dedicated legacy poller.
update public.tracking_sessions
set
  session_status = 'paused',
  next_poll_after = null,
  polling_stopped_reason = 'expired_tracking_window',
  updated_at = now()
where session_status in ('pending', 'active', 'errored')
  and travel_date is not null
  and (
    travel_date < (now() - interval '1 day')::date
    or travel_date > (now() + interval '2 days')::date
  );

-- Repair provider rows that already contain authoritative movement timestamps
-- but were left in the scheduled state. Terminal rows must not be selected by
-- periodic lifecycle recovery again.
update public.flight_instances
set
  status = case
    when actual_arrival_at is not null then 'arrived_at_gate'
    when actual_departure_at is not null and status in ('unknown', 'scheduled', 'boarding', 'delayed') then 'taxiing'
    else status
  end,
  is_final = actual_arrival_at is not null or is_final,
  fresh_until = case
    when actual_arrival_at is not null then greatest(coalesce(fresh_until, now()), now() + interval '12 hours')
    else fresh_until
  end,
  normalized_data = case
    when actual_arrival_at is not null then
      coalesce(normalized_data, '{}'::jsonb) || jsonb_build_object('status', 'arrived_at_gate')
    when actual_departure_at is not null and status in ('unknown', 'scheduled', 'boarding', 'delayed') then
      coalesce(normalized_data, '{}'::jsonb) || jsonb_build_object('status', 'taxiing')
    else normalized_data
  end,
  updated_at = now()
where
  (actual_arrival_at is not null and (is_final = false or status not in ('landed', 'arrived', 'arrived_at_gate')))
  or
  (actual_departure_at is not null and status in ('unknown', 'scheduled', 'boarding', 'delayed'));
