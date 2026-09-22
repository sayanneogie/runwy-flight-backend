-- FlightAware impending-departure alerts are normalized into TRIP_STARTING
-- events for notification fanout. Keep the database constraint aligned with
-- the event types emitted by shared-flight/service.js.
alter table if exists public.flight_events
  drop constraint if exists flight_events_type_check;

alter table if exists public.flight_events
  add constraint flight_events_type_check
    check (event_type in (
      'SCHEDULED', 'DELAYED', 'RESCHEDULED', 'CANCELLED', 'DEPARTED', 'AIRBORNE',
      'LANDED', 'ARRIVED', 'TAXIING', 'TAKEOFF_ROLL', 'TAXI_IN', 'ARRIVED_AT_GATE',
      'GATE_CHANGED', 'TERMINAL_CHANGED', 'BAGGAGE_BELT_ASSIGNED',
      'DIVERTED', 'RETURNED_TO_GATE', 'WEATHER_ADVISORY', 'TRIP_STARTING',
      'UNKNOWN_CHANGE', 'PROVIDER_DATA_SUSPICIOUS'
    ));
