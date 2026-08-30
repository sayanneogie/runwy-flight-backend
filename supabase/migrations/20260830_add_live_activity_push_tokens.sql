create table if not exists public.live_activity_tokens (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  flight_instance_id uuid not null references public.flight_instances(id) on delete cascade,
  tracking_session_id uuid references public.tracking_sessions(id) on delete set null,
  activity_id text not null,
  local_flight_id text,
  push_token text not null,
  environment text not null check (environment in ('sandbox', 'production')),
  is_active boolean not null default true,
  last_sent_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, activity_id)
);

create index if not exists live_activity_tokens_flight_active_idx
  on public.live_activity_tokens (flight_instance_id, is_active);

alter table public.live_activity_tokens enable row level security;

drop policy if exists live_activity_tokens_own_select on public.live_activity_tokens;
create policy live_activity_tokens_own_select on public.live_activity_tokens
  for select using (auth.uid() = user_id);

revoke insert, update, delete on public.live_activity_tokens from anon, authenticated;
