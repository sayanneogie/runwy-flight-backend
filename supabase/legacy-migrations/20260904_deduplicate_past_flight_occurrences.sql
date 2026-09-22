alter table public.user_flights
  drop constraint if exists user_flights_source_type_check;

alter table public.user_flights
  add constraint user_flights_source_type_check
  check (source_type in (
    'trip',
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

-- Older app builds used `manual_search` for ordinary home-feed journeys. Those
-- rows must not be interpreted as deliberately repeated manual history items
-- after they become terminal.
update public.user_flights
set source_type = 'trip',
    updated_at = greatest(updated_at, now())
where source_type = 'manual_search'
  and lifecycle_state in ('upcoming', 'active', 'landed', 'deleted');

update public.user_flights
set lifecycle_state = 'archived',
    updated_at = greatest(updated_at, now())
where lifecycle_state = 'landed'
  and source_type not in ('trip', 'tracked');

-- Keep the richest row for an exact historical occurrence and tombstone the
-- rest. Tombstoning preserves auditability while excluding duplicates from all
-- current app reads.
with ranked_history as (
  select
    id,
    row_number() over (
      partition by
        user_id,
        regexp_replace(upper(display_flight_number), '[^A-Z0-9]', '', 'g'),
        upper(trim(origin_iata)),
        upper(trim(destination_iata)),
        date_trunc('minute', scheduled_departure at time zone 'UTC')
      order by
        case source_type
          when 'travelled_archive' then 0
          when 'history_repair' then 1
          when 'history_snapshot' then 2
          else 3
        end,
        case
          when jsonb_typeof(route_polyline) = 'array' then jsonb_array_length(route_polyline)
          else 0
        end desc,
        (tracked_snapshot is not null) desc,
        updated_at desc,
        id
    ) as occurrence_rank
  from public.user_flights
  where lifecycle_state = 'archived'
    and tracking_session_id is null
    and deleted_at is null
    and display_flight_number is not null
    and origin_iata is not null
    and destination_iata is not null
    and scheduled_departure is not null
)
update public.user_flights as duplicate
set lifecycle_state = 'deleted',
    deleted_at = now(),
    updated_at = now()
from ranked_history
where duplicate.id = ranked_history.id
  and ranked_history.occurrence_rank > 1;

create or replace function public.reconcile_user_flight_history_occurrence()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.lifecycle_state = 'archived'
     and new.source_type not in ('trip', 'tracked')
     and new.tracking_session_id is null
     and new.deleted_at is null
     and new.display_flight_number is not null
     and new.origin_iata is not null
     and new.destination_iata is not null
     and new.scheduled_departure is not null then
    update public.user_flights as existing
    set lifecycle_state = 'deleted',
        deleted_at = now(),
        updated_at = now()
    where existing.id <> new.id
      and existing.user_id = new.user_id
      and existing.lifecycle_state = 'archived'
      and existing.source_type not in ('trip', 'tracked')
      and existing.tracking_session_id is null
      and existing.deleted_at is null
      and regexp_replace(upper(existing.display_flight_number), '[^A-Z0-9]', '', 'g')
        = regexp_replace(upper(new.display_flight_number), '[^A-Z0-9]', '', 'g')
      and upper(trim(existing.origin_iata)) = upper(trim(new.origin_iata))
      and upper(trim(existing.destination_iata)) = upper(trim(new.destination_iata))
      and date_trunc('minute', existing.scheduled_departure at time zone 'UTC')
        = date_trunc('minute', new.scheduled_departure at time zone 'UTC');
  end if;

  return new;
end;
$$;

drop trigger if exists reconcile_user_flight_history_occurrence_trigger
  on public.user_flights;

create trigger reconcile_user_flight_history_occurrence_trigger
before insert or update of
  lifecycle_state,
  source_type,
  tracking_session_id,
  deleted_at,
  display_flight_number,
  origin_iata,
  destination_iata,
  scheduled_departure
on public.user_flights
for each row
execute function public.reconcile_user_flight_history_occurrence();

create unique index if not exists user_flights_history_occurrence_unique
  on public.user_flights (
    user_id,
    (regexp_replace(upper(display_flight_number), '[^A-Z0-9]', '', 'g')),
    (upper(trim(origin_iata))),
    (upper(trim(destination_iata))),
    (date_trunc('minute', scheduled_departure at time zone 'UTC'))
  )
  where lifecycle_state = 'archived'
    and source_type not in ('trip', 'tracked')
    and tracking_session_id is null
    and deleted_at is null
    and display_flight_number is not null
    and origin_iata is not null
    and destination_iata is not null
    and scheduled_departure is not null;
