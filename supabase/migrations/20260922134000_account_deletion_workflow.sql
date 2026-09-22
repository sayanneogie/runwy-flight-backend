create table runwy_security.account_deletion_jobs (
  user_id uuid primary key,
  status text not null default 'pending' check(status in('pending','complete')),
  requested_at timestamptz not null default now(),
  completed_at timestamptz
);
alter table runwy_security.account_deletion_jobs enable row level security;

create function public.runwy_account_accepts_storage()
returns boolean language sql stable security definer set search_path='' as $$
  select auth.uid() is not null and exists(select 1 from auth.users where id=auth.uid())
    and not exists(select 1 from runwy_security.account_deletion_jobs where user_id=auth.uid());
$$;
revoke all on function public.runwy_account_accepts_storage() from public,anon;
grant execute on function public.runwy_account_accepts_storage() to authenticated,service_role;
create policy runwy_storage_account_active_insert on storage.objects as restrictive
  for insert to authenticated with check(bucket_id not in('profile-avatars','ticket-stickers') or public.runwy_account_accepts_storage());
create policy runwy_storage_account_active_update on storage.objects as restrictive
  for update to authenticated using(bucket_id not in('profile-avatars','ticket-stickers') or public.runwy_account_accepts_storage())
  with check(bucket_id not in('profile-avatars','ticket-stickers') or public.runwy_account_accepts_storage());

create function public.runwy_account_deletion_batch(p_actor uuid)
returns jsonb language plpgsql security definer set search_path='' as $$
declare objects jsonb;
begin
  if not exists(select 1 from auth.users where id=p_actor) then raise exception 'Account not found' using errcode='22023'; end if;
  insert into runwy_security.account_deletion_jobs(user_id) values(p_actor) on conflict(user_id) do nothing;
  select coalesce(jsonb_agg(to_jsonb(files)),'[]'::jsonb) into objects from (
    select bucket_id,name from storage.objects where bucket_id in('profile-avatars','ticket-stickers')
      and (owner_id=p_actor::text or split_part(name,'/',1)=p_actor::text)
      order by bucket_id,name limit 500
  ) files;
  return objects;
end $$;
revoke all on function public.runwy_account_deletion_batch(uuid) from public,anon,authenticated;
grant execute on function public.runwy_account_deletion_batch(uuid) to service_role;

create or replace function public.delete_current_user_account()
returns boolean language plpgsql security definer set search_path='' as $$
declare actor uuid:=auth.uid();
begin
  if actor is null then raise exception 'Authentication required' using errcode='42501'; end if;
  -- Compatibility for old clients that already delete their files first. Direct
  -- callers cannot orphan private blobs by skipping the Storage API stage.
  if exists(select 1 from storage.objects where bucket_id in('profile-avatars','ticket-stickers')
    and (owner_id=actor::text or split_part(name,'/',1)=actor::text)) then
    raise exception 'Delete private storage through the account deletion service first' using errcode='23514';
  end if;
  delete from public.api_usage_logs where user_id=actor;
  delete from public.friend_invites where accepted_by_user_id=actor;
  delete from auth.users where id=actor;
  if not found then raise exception 'Authenticated account no longer exists' using errcode='P0002'; end if;
  update runwy_security.account_deletion_jobs set status='complete',completed_at=now() where user_id=actor;
  return true;
end $$;
revoke all on function public.delete_current_user_account() from public,anon;
grant execute on function public.delete_current_user_account() to authenticated;

create function public.runwy_finish_account_deletion(p_actor uuid)
returns boolean language plpgsql security definer set search_path='' as $$
begin
  perform 1 from runwy_security.account_deletion_jobs where user_id=p_actor and status='pending' for update;
  if not found then raise exception 'Deletion was not requested' using errcode='42501'; end if;
  perform set_config('request.jwt.claim.sub',p_actor::text,true);
  return public.delete_current_user_account();
end $$;
revoke all on function public.runwy_finish_account_deletion(uuid) from public,anon,authenticated;
grant execute on function public.runwy_finish_account_deletion(uuid) to service_role;
