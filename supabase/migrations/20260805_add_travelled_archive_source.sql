alter table public.user_flights
  drop constraint if exists user_flights_source_type_check;

alter table public.user_flights
  add constraint user_flights_source_type_check
  check (source_type in (
    'manual_search',
    'calendar_import',
    'tracked',
    'recovered',
    'manual_verified',
    'manual_recovery',
    'history_snapshot',
    'history_repair',
    'auto_archive',
    'travelled_archive'
  ));
