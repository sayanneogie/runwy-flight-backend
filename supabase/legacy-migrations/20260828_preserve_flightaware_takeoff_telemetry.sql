-- Repair the one diagnosed AI 2418 snapshot written before the adapter began
-- preserving distinct OUT/OFF/ON/IN timestamps. Future snapshots are handled
-- by the application code; keep this backfill deliberately instance-scoped.
update public.flight_instances
set normalized_data = coalesce(normalized_data, '{}'::jsonb) || jsonb_build_object(
  'departureTimes', jsonb_build_object(
    'scheduled', raw_provider_response ->> 'scheduled_out',
    'estimated', raw_provider_response ->> 'estimated_out',
    'actual', raw_provider_response ->> 'actual_out'
  ),
  'takeoffTimes', jsonb_build_object(
    'scheduled', raw_provider_response ->> 'scheduled_off',
    'estimated', raw_provider_response ->> 'estimated_off',
    'actual', raw_provider_response ->> 'actual_off'
  ),
  'landingTimes', jsonb_build_object(
    'scheduled', raw_provider_response ->> 'scheduled_on',
    'estimated', raw_provider_response ->> 'estimated_on',
    'actual', raw_provider_response ->> 'actual_on'
  ),
  'arrivalTimes', jsonb_build_object(
    'scheduled', raw_provider_response ->> 'scheduled_in',
    'estimated', raw_provider_response ->> 'estimated_in',
    'actual', raw_provider_response ->> 'actual_in'
  )
)
where id = '4a26ec3a-c4f8-495c-bd7c-4ca6c8ae7d03'::uuid
  and provider = 'flightaware'
  and provider_flight_id = 'AIC2418-1787733745-airline-111p'
  and raw_provider_response ->> 'actual_off' = '2026-08-28T11:07:35Z'
  and coalesce(normalized_data -> 'takeoffTimes' ->> 'actual', '') = '';

-- AeroAPI track altitude is expressed in hundreds of feet. The exact-value
-- predicates make this correction idempotent and prevent unrelated rewrites.
update public.flight_instances
set altitude = altitude * 100,
    normalized_data = jsonb_set(
      coalesce(normalized_data, '{}'::jsonb),
      '{position,altitude}',
      to_jsonb(altitude * 100),
      true
    )
where id = '4a26ec3a-c4f8-495c-bd7c-4ca6c8ae7d03'::uuid
  and provider = 'flightaware'
  and provider_flight_id = 'AIC2418-1787733745-airline-111p'
  and status = 'enroute'
  and altitude = 378
  and normalized_data -> 'position' ->> 'altitude' = '378'
  and ground_speed = 456;
