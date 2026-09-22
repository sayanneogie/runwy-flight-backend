alter table public.flight_instances
  add column if not exists state_revision bigint not null default 0;

alter table public.flight_snapshots
  add column if not exists state_revision bigint not null default 0;

alter table public.flight_events
  add column if not exists state_revision bigint not null default 0;

alter table public.live_snapshots
  add column if not exists canonical_revision bigint not null default 0;

create index if not exists idx_flight_instances_canonical_order
  on public.flight_instances (id, state_revision, last_stream_event_at);

create index if not exists idx_live_snapshots_canonical_order
  on public.live_snapshots (tracking_session_id, canonical_revision, provider_last_updated_at);
