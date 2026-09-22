begin;

insert into storage.buckets (
  id,
  name,
  public,
  file_size_limit,
  allowed_mime_types
)
values (
  'profile-avatars',
  'profile-avatars',
  false,
  2097152,
  array['image/jpeg', 'image/png']
)
on conflict (id) do update
set
  name = excluded.name,
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists "profile_avatars_circle_read" on storage.objects;
create policy "profile_avatars_circle_read"
on storage.objects
for select
to authenticated
using (
  bucket_id = 'profile-avatars'
  and (
    (storage.foldername(name))[1] = auth.uid()::text
    or exists (
      select 1
      from public.friend_relationships relationship
      where relationship.relationship_status = 'active'
        and (
          (
            relationship.user_a = auth.uid()
            and relationship.user_b::text = (storage.foldername(name))[1]
          )
          or (
            relationship.user_b = auth.uid()
            and relationship.user_a::text = (storage.foldername(name))[1]
          )
        )
    )
  )
);

drop policy if exists "profile_avatars_owner_insert" on storage.objects;
create policy "profile_avatars_owner_insert"
on storage.objects
for insert
to authenticated
with check (
  bucket_id = 'profile-avatars'
  and (storage.foldername(name))[1] = auth.uid()::text
);

drop policy if exists "profile_avatars_owner_update" on storage.objects;
create policy "profile_avatars_owner_update"
on storage.objects
for update
to authenticated
using (
  bucket_id = 'profile-avatars'
  and (storage.foldername(name))[1] = auth.uid()::text
)
with check (
  bucket_id = 'profile-avatars'
  and (storage.foldername(name))[1] = auth.uid()::text
);

drop policy if exists "profile_avatars_owner_delete" on storage.objects;
create policy "profile_avatars_owner_delete"
on storage.objects
for delete
to authenticated
using (
  bucket_id = 'profile-avatars'
  and (storage.foldername(name))[1] = auth.uid()::text
);

commit;
