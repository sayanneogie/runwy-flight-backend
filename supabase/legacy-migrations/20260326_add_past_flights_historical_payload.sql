alter table public.past_flights
    add column if not exists route_polyline jsonb,
    add column if not exists tracked_snapshot jsonb;

comment on column public.past_flights.route_polyline is
    'Persisted traced route coordinates for archived flights so past-flight maps can render without re-querying providers.';

comment on column public.past_flights.tracked_snapshot is
    'Historical provider payload for archived flights, including timings, delay context, and any captured track points.';
