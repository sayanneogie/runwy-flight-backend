-- Tie every queued delivery to the saved flight that created it so deleting one
-- user's flight never affects another user's copy of the same shared instance.
alter table public.notification_deliveries
  add column if not exists user_flight_id uuid references public.user_flights(id) on delete cascade;

create index if not exists notification_deliveries_user_flight_idx
  on public.notification_deliveries (user_flight_id)
  where user_flight_id is not null;

create or replace function public.cleanup_deleted_user_flight_notification_artifacts()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  deleted_flight public.user_flights%rowtype;
begin
  if tg_op = 'DELETE' then
    deleted_flight := old;
  else
    deleted_flight := new;
    if deleted_flight.deleted_at is null
       and coalesce(deleted_flight.lifecycle_state, '') <> 'deleted' then
      return new;
    end if;
  end if;

  delete from public.notifications n
   where n.payload_json ->> 'user_flight_id' = deleted_flight.id::text
      or (
        n.user_id = deleted_flight.user_id
        and (
          (deleted_flight.tracking_session_id is not null
            and (n.tracking_session_id = deleted_flight.tracking_session_id
              or n.payload_json ->> 'tracking_session_id' = deleted_flight.tracking_session_id::text))
          or (deleted_flight.flight_instance_id is not null
            and n.payload_json ->> 'flight_instance_id' = deleted_flight.flight_instance_id::text)
        )
      );

  delete from public.notification_deliveries d
   where d.user_flight_id = deleted_flight.id
      or (
        d.user_id = deleted_flight.user_id
        and deleted_flight.flight_instance_id is not null
        and d.flight_instance_id = deleted_flight.flight_instance_id
      );

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;

drop trigger if exists cleanup_deleted_user_flight_notifications on public.user_flights;
create trigger cleanup_deleted_user_flight_notifications
after insert or update or delete
on public.user_flights
for each row
execute function public.cleanup_deleted_user_flight_notification_artifacts();
