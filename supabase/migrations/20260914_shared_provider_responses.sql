-- Apply before deploying the shared request coordinator.
create table if not exists public.provider_response_cache (
  cache_key text primary key,
  response jsonb not null,
  requested_at timestamptz not null,
  expires_at timestamptz not null
);
create index if not exists provider_response_cache_expiry_idx on public.provider_response_cache (expires_at);
alter table public.provider_response_cache enable row level security;
revoke all on public.provider_response_cache from anon, authenticated;

alter table public.api_usage_logs add column if not exists provider_path text;
alter table public.api_usage_logs add column if not exists request_reason text;

-- APNs copy must describe the validated event, even if a newer update arrives
-- before fanout/retry runs. Delivery remains private to each recipient.
alter table public.flight_events add column if not exists flight_snapshot jsonb;

-- Completion is per event, not inferred from the first recipient's delivery.
alter table public.flight_events add column if not exists fanout_completed_at timestamptz;
create index if not exists flight_events_pending_fanout_idx
  on public.flight_events (created_at) where notification_required = true and fanout_completed_at is null;
