-- Cross-process provider-call leases prevent separate Railway replicas from
-- purchasing the same FlightAware result at the same time. Leases expire
-- automatically so a crashed worker cannot permanently block a flight.

create table if not exists public.provider_request_leases (
  lock_key text primary key,
  lease_token uuid not null,
  expires_at timestamptz not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists provider_request_leases_expires_at_idx
  on public.provider_request_leases (expires_at);

alter table public.provider_request_leases enable row level security;
revoke all on public.provider_request_leases from anon, authenticated;

comment on table public.provider_request_leases is
  'Short-lived distributed leases used by backend workers to deduplicate paid provider calls.';
