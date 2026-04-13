alter table public.user_flights
  add column if not exists notifications_enabled boolean;

alter table public.user_flights
  add column if not exists alert_settings_json jsonb;

update public.user_flights
set notifications_enabled = true
where notifications_enabled is null;

update public.user_flights
set alert_settings_json = jsonb_build_object(
  'gateChange', true,
  'delayUpdates', true,
  'boardingTime', true,
  'takeoffLanding', false,
  'baggageClaim', true,
  'quietHours', jsonb_build_object(
    'startHour', 22,
    'startMinute', 0,
    'endHour', 7,
    'endMinute', 0
  )
)
where alert_settings_json is null;

alter table public.user_flights
  alter column notifications_enabled set default true;

alter table public.user_flights
  alter column notifications_enabled set not null;

alter table public.user_flights
  alter column alert_settings_json set default jsonb_build_object(
    'gateChange', true,
    'delayUpdates', true,
    'boardingTime', true,
    'takeoffLanding', false,
    'baggageClaim', true,
    'quietHours', jsonb_build_object(
      'startHour', 22,
      'startMinute', 0,
      'endHour', 7,
      'endMinute', 0
    )
  );

alter table public.user_flights
  alter column alert_settings_json set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'user_flights_alert_settings_is_object'
  ) then
    alter table public.user_flights
      add constraint user_flights_alert_settings_is_object
      check (jsonb_typeof(alert_settings_json) = 'object');
  end if;
end $$;
