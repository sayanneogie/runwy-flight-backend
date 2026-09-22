create extension if not exists pgcrypto;

create or replace function public.runwy_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.friend_invites (
  id uuid primary key default gen_random_uuid(),
  inviter_user_id uuid not null references auth.users(id) on delete cascade,
  token_hash text not null unique,
  status text not null default 'pending' check (status in ('pending', 'accepted', 'revoked', 'expired')),
  default_share_scope text not null default 'future_flights' check (default_share_scope in ('future_flights', 'all_flights', 'selected_flights')),
  message text,
  expires_at timestamptz not null default (now() + interval '7 days'),
  accepted_by_user_id uuid references auth.users(id) on delete set null,
  accepted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.friend_relationships (
  id uuid primary key default gen_random_uuid(),
  user_a uuid not null references auth.users(id) on delete cascade,
  user_b uuid not null references auth.users(id) on delete cascade,
  relationship_status text not null default 'active' check (relationship_status in ('active', 'blocked', 'removed')),
  created_by_user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint friend_relationships_distinct_users check (user_a <> user_b),
  constraint friend_relationships_unique_pair unique (user_a, user_b)
);

create table if not exists public.friend_permissions (
  id uuid primary key default gen_random_uuid(),
  relationship_id uuid not null references public.friend_relationships(id) on delete cascade,
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  viewer_user_id uuid not null references auth.users(id) on delete cascade,
  share_scope text not null default 'future_flights' check (share_scope in ('future_flights', 'all_flights', 'selected_flights')),
  can_view_live boolean not null default true,
  can_view_history boolean not null default false,
  can_receive_alerts boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint friend_permissions_distinct_users check (owner_user_id <> viewer_user_id),
  constraint friend_permissions_unique_direction unique (owner_user_id, viewer_user_id)
);

create index if not exists friend_invites_inviter_idx
  on public.friend_invites (inviter_user_id, status, expires_at desc);

create index if not exists friend_relationships_user_a_idx
  on public.friend_relationships (user_a, relationship_status);

create index if not exists friend_relationships_user_b_idx
  on public.friend_relationships (user_b, relationship_status);

create index if not exists friend_permissions_viewer_idx
  on public.friend_permissions (viewer_user_id, owner_user_id);

drop trigger if exists friend_invites_touch_updated_at on public.friend_invites;
create trigger friend_invites_touch_updated_at
before update on public.friend_invites
for each row
execute function public.runwy_touch_updated_at();

drop trigger if exists friend_relationships_touch_updated_at on public.friend_relationships;
create trigger friend_relationships_touch_updated_at
before update on public.friend_relationships
for each row
execute function public.runwy_touch_updated_at();

drop trigger if exists friend_permissions_touch_updated_at on public.friend_permissions;
create trigger friend_permissions_touch_updated_at
before update on public.friend_permissions
for each row
execute function public.runwy_touch_updated_at();

alter table public.friend_invites enable row level security;
alter table public.friend_relationships enable row level security;
alter table public.friend_permissions enable row level security;

drop policy if exists "friend_invites_select_own" on public.friend_invites;
create policy "friend_invites_select_own"
on public.friend_invites
for select
to authenticated
using (
  auth.uid() = inviter_user_id
  or auth.uid() = accepted_by_user_id
);

drop policy if exists "friend_relationships_select_members" on public.friend_relationships;
create policy "friend_relationships_select_members"
on public.friend_relationships
for select
to authenticated
using (
  auth.uid() = user_a
  or auth.uid() = user_b
);

drop policy if exists "friend_permissions_select_members" on public.friend_permissions;
create policy "friend_permissions_select_members"
on public.friend_permissions
for select
to authenticated
using (
  auth.uid() = owner_user_id
  or auth.uid() = viewer_user_id
);

drop policy if exists "friend_permissions_update_owner" on public.friend_permissions;
create policy "friend_permissions_update_owner"
on public.friend_permissions
for update
to authenticated
using (auth.uid() = owner_user_id)
with check (auth.uid() = owner_user_id);
