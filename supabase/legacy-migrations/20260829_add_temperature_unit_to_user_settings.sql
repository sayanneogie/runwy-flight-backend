alter table if exists public.user_settings
  add column if not exists temperature_unit text not null default 'celsius';

update public.user_settings
set temperature_unit = 'celsius'
where temperature_unit is null
   or temperature_unit not in ('celsius', 'fahrenheit');

alter table public.user_settings
  drop constraint if exists user_settings_temperature_unit_check;

alter table public.user_settings
  add constraint user_settings_temperature_unit_check
  check (temperature_unit in ('celsius', 'fahrenheit'));
