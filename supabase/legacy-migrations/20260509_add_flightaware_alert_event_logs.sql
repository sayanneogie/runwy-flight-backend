-- FlightAware POST alert ingestion log.
-- This table is intentionally separate from public.flight_events:
-- flight_event_logs records raw provider webhook facts and dedupe decisions,
-- while flight_events records meaningful Runwy state changes used for fanout.

create table if not exists public.flight_event_logs (
  id uuid primary key default gen_random_uuid(),
  flight_instance_id uuid references public.flight_instances(id) on delete set null,
  flight_key text not null,
  fa_flight_id text,
  ident text,
  event_type text not null,
  event_status text,
  event_time timestamptz,
  source text not null default 'flightaware',
  raw_payload jsonb not null,
  normalized_payload jsonb not null,
  dedupe_key text not null unique,
  created_at timestamptz default now()
);

create index if not exists flight_event_logs_flight_instance_idx
  on public.flight_event_logs (flight_instance_id, created_at desc);

create index if not exists flight_event_logs_flight_key_idx
  on public.flight_event_logs (flight_key, created_at desc);

create index if not exists flight_event_logs_fa_flight_id_idx
  on public.flight_event_logs (fa_flight_id, created_at desc)
  where fa_flight_id is not null;

alter table public.flight_event_logs enable row level security;

revoke insert, update, delete on public.flight_event_logs from anon, authenticated;

drop policy if exists flight_event_logs_no_client_select on public.flight_event_logs;
create policy flight_event_logs_no_client_select on public.flight_event_logs
for select using (false);
