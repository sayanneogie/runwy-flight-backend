-- Short-lived access is issued only after the backend verifies RevenueCat.
-- Never trust app-supplied membership flags or writable user metadata.
create table runwy_security.sticker_cloud_access (
 user_id uuid primary key references auth.users(id) on delete cascade,
 verified_until timestamptz not null
);
alter table runwy_security.sticker_cloud_access enable row level security;
grant select,delete on runwy_security.sticker_cloud_access to service_role;
create function public.runwy_grant_sticker_cloud_access(p_user uuid,p_until timestamptz)
returns timestamptz language plpgsql security definer set search_path='' as $$
declare deadline timestamptz:=least(p_until,now()+interval '5 minutes');
begin
 if p_user is null or p_until is null or deadline<=now() then raise exception 'Fresh membership verification required' using errcode='22023'; end if;
 insert into runwy_security.sticker_cloud_access(user_id,verified_until) values(p_user,deadline)
 on conflict(user_id) do update set verified_until=excluded.verified_until;
 return deadline;
end $$;
revoke all on function public.runwy_grant_sticker_cloud_access(uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.runwy_grant_sticker_cloud_access(uuid,timestamptz) to service_role;
create function public.runwy_has_sticker_cloud_access() returns boolean
language sql stable security definer set search_path='' as $$
 select exists(select 1 from runwy_security.sticker_cloud_access where user_id=auth.uid() and verified_until>now())
$$;
revoke all on function public.runwy_has_sticker_cloud_access() from public,anon;
grant execute on function public.runwy_has_sticker_cloud_access() to authenticated;
-- Restrictive policies are ANDed with existing owner policies.
create policy ticket_sticker_paid_read on storage.objects as restrictive for select to authenticated
 using(bucket_id<>'ticket-stickers' or (select public.runwy_has_sticker_cloud_access()));
create policy ticket_sticker_paid_insert on storage.objects as restrictive for insert to authenticated
 with check(bucket_id<>'ticket-stickers' or (select public.runwy_has_sticker_cloud_access()));
create policy ticket_sticker_paid_update on storage.objects as restrictive for update to authenticated
 using(bucket_id<>'ticket-stickers' or (select public.runwy_has_sticker_cloud_access()))
 with check(bucket_id<>'ticket-stickers' or (select public.runwy_has_sticker_cloud_access()));
create policy ticket_souvenir_paid_read on public.ticket_souvenirs as restrictive for select to authenticated
 using((select public.runwy_has_sticker_cloud_access()));
create policy ticket_souvenir_paid_insert on public.ticket_souvenirs as restrictive for insert to authenticated
 with check((select public.runwy_has_sticker_cloud_access()));
create policy ticket_souvenir_paid_update on public.ticket_souvenirs as restrictive for update to authenticated
 using((select public.runwy_has_sticker_cloud_access())) with check((select public.runwy_has_sticker_cloud_access()));
-- No changes to user_flights: free flight synchronization remains owner-authorized.
-- Existing stickers are retained on expiry; account deletion still works without payment.
