alter table public.user_flights
  add column if not exists final_route_capture_status text,
  add column if not exists final_route_capture_attempted_at timestamptz,
  add column if not exists final_route_capture_completed_at timestamptz,
  add column if not exists final_route_capture_next_attempt_at timestamptz,
  add column if not exists final_route_capture_error text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_flights_final_route_capture_status_check'
      and conrelid = 'public.user_flights'::regclass
  ) then
    alter table public.user_flights
      add constraint user_flights_final_route_capture_status_check
      check (
        final_route_capture_status is null
        or final_route_capture_status in ('pending', 'in_progress', 'failed', 'captured', 'no_track')
      );
  end if;
end;
$$;

create index if not exists user_flights_final_route_capture_due_idx
  on public.user_flights (
    lifecycle_state,
    final_route_capture_status,
    final_route_capture_next_attempt_at,
    estimated_arrival
  )
  where deleted_at is null
    and tracking_session_id is null
    and provider_flight_id is not null;
