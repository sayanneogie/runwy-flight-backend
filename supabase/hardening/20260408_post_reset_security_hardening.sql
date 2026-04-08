begin;

create or replace function public.runwy_validate_friend_permission_pair()
returns trigger
language plpgsql
as $$
declare
  relationship_row record;
begin
  select *
  into relationship_row
  from public.friend_relationships
  where id = new.relationship_id;

  if not found then
    raise exception 'friend_permissions relationship_id % does not exist', new.relationship_id;
  end if;

  if not (
    (new.owner_user_id = relationship_row.user_a and new.viewer_user_id = relationship_row.user_b)
    or
    (new.owner_user_id = relationship_row.user_b and new.viewer_user_id = relationship_row.user_a)
  ) then
    raise exception 'friend_permissions owner/viewer pair must match relationship members';
  end if;

  return new;
end;
$$;

update public.friend_relationships
set
  user_a = least(user_a::text, user_b::text)::uuid,
  user_b = greatest(user_a::text, user_b::text)::uuid
where user_a::text > user_b::text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'friend_relationships_canonical_order'
      and conrelid = 'public.friend_relationships'::regclass
  ) then
    alter table public.friend_relationships
      add constraint friend_relationships_canonical_order
      check (user_a::text < user_b::text);
  end if;
end
$$;

drop trigger if exists friend_permissions_validate_pair on public.friend_permissions;
create trigger friend_permissions_validate_pair
before insert or update on public.friend_permissions
for each row
execute function public.runwy_validate_friend_permission_pair();

drop policy if exists "tracking_sessions_select_visible" on public.tracking_sessions;
drop policy if exists "tracking_sessions_select_owner" on public.tracking_sessions;
create policy "tracking_sessions_select_owner"
on public.tracking_sessions
for select
to authenticated
using (auth.uid() = owner_user_id);

drop policy if exists "live_snapshots_select_visible" on public.live_snapshots;
drop policy if exists "live_snapshots_select_owner" on public.live_snapshots;
create policy "live_snapshots_select_owner"
on public.live_snapshots
for select
to authenticated
using (
  exists (
    select 1
    from public.tracking_sessions ts
    where ts.id = public.live_snapshots.tracking_session_id
      and ts.owner_user_id = auth.uid()
  )
);

commit;
