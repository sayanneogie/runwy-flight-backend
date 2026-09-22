begin;

-- Fail quickly if active traffic prevents acquiring the migration's locks.
set local lock_timeout = '5s';
set local statement_timeout = '30s';

-- These tables are internal to Railway's provider and notification pipeline.
-- iOS reads owner-scoped user_flights/live_snapshots instead; Circle summaries
-- are served by authenticated Edge Functions using their service-role client.
-- No anon/authenticated policies are needed on these internal tables.
alter table public.flight_definitions enable row level security;
alter table public.flight_instances enable row level security;
alter table public.flight_instance_aliases enable row level security;
alter table public.flight_snapshots enable row level security;
alter table public.flight_events enable row level security;
alter table public.api_usage_logs enable row level security;

-- RLS filters rows; revoke the client grants as well, including privileges
-- such as TRUNCATE that RLS does not cover. Remove PUBLIC grants so they cannot
-- supply inherited access to either client role.
revoke all privileges on table
  public.flight_definitions,
  public.flight_instances,
  public.flight_instance_aliases,
  public.flight_snapshots,
  public.flight_events,
  public.api_usage_logs
from public, anon, authenticated;

-- Preserve existing postgres/service_role grants and RLS bypass for backend
-- operations. User ownership policies, Circle permissions, and Storage policies
-- are intentionally outside this migration.
commit;
