-- Durable, per-device APNs work. A row is written before the network call, so
-- deploys cannot lose definite retryable failures and partial multi-device
-- success never causes already-accepted tokens to be called again.
alter table public.notification_deliveries
  add column if not exists updated_at timestamptz not null default now();

create table if not exists public.notification_delivery_tokens (
  id uuid primary key default gen_random_uuid(),
  notification_delivery_id uuid not null
    references public.notification_deliveries(id) on delete cascade,
  device_token_id uuid not null
    references public.device_tokens(id) on delete cascade,
  payload_json jsonb not null default '{}'::jsonb,
  status text not null default 'queued'
    check (status in ('queued', 'sending', 'retry', 'accepted', 'permanent_failed', 'uncertain')),
  attempt_count integer not null default 0,
  next_attempt_at timestamptz not null default now(),
  locked_until timestamptz,
  last_attempt_at timestamptz,
  accepted_at timestamptz,
  apns_id text,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (notification_delivery_id, device_token_id)
);

create index if not exists notification_delivery_tokens_due_idx
  on public.notification_delivery_tokens (next_attempt_at, created_at)
  where status in ('queued', 'retry');

create index if not exists notification_delivery_tokens_stale_sending_idx
  on public.notification_delivery_tokens (locked_until)
  where status = 'sending';

alter table public.notification_delivery_tokens enable row level security;
revoke all on public.notification_delivery_tokens from anon, authenticated;

comment on table public.notification_delivery_tokens is
  'Durable per-device APNs outbox. Ambiguous sends become uncertain rather than being blindly retried.';

-- Deleting one of two legacy rows for the same displayed occurrence must not
-- erase the surviving row's delivery ledger. This replacement keeps the broad
-- cleanup behavior only when the removed row is the last active occurrence.
create or replace function public.cleanup_deleted_user_flight_notifications()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  has_active_sibling boolean := false;
begin
  if new.deleted_at is null and coalesce(new.lifecycle_state, '') <> 'deleted' then
    return new;
  end if;

  if tg_op = 'UPDATE'
     and old.deleted_at is not null
     and coalesce(old.lifecycle_state, '') = 'deleted' then
    return new;
  end if;

  if new.display_flight_number is not null
     and new.origin_iata is not null
     and new.destination_iata is not null
     and new.scheduled_departure is not null then
    select exists (
      select 1
      from public.user_flights sibling
      where sibling.user_id = new.user_id
        and sibling.id <> new.id
        and sibling.deleted_at is null
        and coalesce(sibling.lifecycle_state, '') <> 'deleted'
        and regexp_replace(upper(coalesce(sibling.display_flight_number, '')), '[^A-Z0-9]', '', 'g')
          = regexp_replace(upper(new.display_flight_number), '[^A-Z0-9]', '', 'g')
        and upper(coalesce(sibling.origin_iata, '')) = upper(new.origin_iata)
        and upper(coalesce(sibling.destination_iata, '')) = upper(new.destination_iata)
        and abs(extract(epoch from (sibling.scheduled_departure - new.scheduled_departure))) <= 1800
    ) into has_active_sibling;
  end if;

  delete from public.notification_deliveries
  where user_flight_id = new.id
     or (
       not has_active_sibling
       and user_id = new.user_id
       and new.flight_instance_id is not null
       and flight_instance_id = new.flight_instance_id
     );

  delete from public.notifications
  where payload_json ->> 'user_flight_id' = new.id::text
     or payload_json ->> 'userFlightId' = new.id::text
     or (
       not has_active_sibling
       and user_id = new.user_id
       and (
         (new.tracking_session_id is not null and tracking_session_id = new.tracking_session_id)
         or (new.tracking_session_id is not null and payload_json ->> 'tracking_session_id' = new.tracking_session_id::text)
         or (new.flight_instance_id is not null and payload_json ->> 'flight_instance_id' = new.flight_instance_id::text)
       )
     );

  if new.tracking_session_id is not null
     and not exists (
       select 1 from public.user_flights uf
       where uf.tracking_session_id = new.tracking_session_id
         and uf.deleted_at is null
         and coalesce(uf.lifecycle_state, '') <> 'deleted'
     ) then
    update public.tracking_sessions
    set session_status = 'paused',
        next_poll_after = null,
        polling_stopped_reason = 'user_flight_deleted',
        updated_at = now()
    where id = new.tracking_session_id;
  end if;

  return new;
end;
$$;
