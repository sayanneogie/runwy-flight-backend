-- A client may tombstone user_flights directly through PostgREST. Keep the
-- notification cleanup invariant in the database so it cannot be bypassed by
-- a particular UI or API path.
create or replace function public.cleanup_deleted_user_flight_notifications()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.deleted_at is null and coalesce(new.lifecycle_state, '') <> 'deleted' then
    return new;
  end if;

  if tg_op = 'UPDATE'
     and old.deleted_at is not null
     and coalesce(old.lifecycle_state, '') = 'deleted' then
    return new;
  end if;

  delete from public.notification_deliveries
  where user_flight_id = new.id
     or (
       user_id = new.user_id
       and new.flight_instance_id is not null
       and flight_instance_id = new.flight_instance_id
     );

  delete from public.notifications
  where payload_json ->> 'user_flight_id' = new.id::text
     or payload_json ->> 'userFlightId' = new.id::text
     or (
       user_id = new.user_id
       and (
         (new.tracking_session_id is not null and tracking_session_id = new.tracking_session_id)
         or (new.tracking_session_id is not null and payload_json ->> 'tracking_session_id' = new.tracking_session_id::text)
         or (new.flight_instance_id is not null and payload_json ->> 'flight_instance_id' = new.flight_instance_id::text)
       )
     );

  if new.tracking_session_id is not null
     and not exists (
       select 1
       from public.user_flights uf
       where uf.tracking_session_id = new.tracking_session_id
         and uf.deleted_at is null
         and coalesce(uf.lifecycle_state, '') <> 'deleted'
     ) then
    update public.tracking_sessions
    set
      session_status = 'paused',
      next_poll_after = null,
      polling_stopped_reason = 'user_flight_deleted',
      updated_at = now()
    where id = new.tracking_session_id;
  end if;

  return new;
end;
$$;

drop trigger if exists cleanup_deleted_user_flight_notifications
  on public.user_flights;

create trigger cleanup_deleted_user_flight_notifications
after insert or update on public.user_flights
for each row
execute function public.cleanup_deleted_user_flight_notifications();

-- Clean up artifacts left by deletions that happened before this trigger was
-- installed. This is intentionally repeatable and safe during deployment.
delete from public.notification_deliveries nd
using public.user_flights uf
where (uf.deleted_at is not null or coalesce(uf.lifecycle_state, '') = 'deleted')
  and (
    nd.user_flight_id = uf.id
    or (
      nd.user_id = uf.user_id
      and uf.flight_instance_id is not null
      and nd.flight_instance_id = uf.flight_instance_id
    )
  );

delete from public.notifications n
using public.user_flights uf
where (uf.deleted_at is not null or coalesce(uf.lifecycle_state, '') = 'deleted')
  and (
    n.payload_json ->> 'user_flight_id' = uf.id::text
    or n.payload_json ->> 'userFlightId' = uf.id::text
    or (
      n.user_id = uf.user_id
      and (
        (uf.tracking_session_id is not null and n.tracking_session_id = uf.tracking_session_id)
        or (uf.tracking_session_id is not null and n.payload_json ->> 'tracking_session_id' = uf.tracking_session_id::text)
        or (uf.flight_instance_id is not null and n.payload_json ->> 'flight_instance_id' = uf.flight_instance_id::text)
      )
    )
  );

update public.tracking_sessions ts
set
  session_status = 'paused',
  next_poll_after = null,
  polling_stopped_reason = 'user_flight_deleted',
  updated_at = now()
where not exists (
  select 1
  from public.user_flights uf
  where uf.tracking_session_id = ts.id
    and uf.deleted_at is null
    and coalesce(uf.lifecycle_state, '') <> 'deleted'
)
and exists (
  select 1
  from public.user_flights uf
  where uf.tracking_session_id = ts.id
    and (uf.deleted_at is not null or coalesce(uf.lifecycle_state, '') = 'deleted')
);
