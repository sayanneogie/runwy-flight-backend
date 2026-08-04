begin;

insert into storage.buckets (
  id,
  name,
  public,
  file_size_limit,
  allowed_mime_types
)
values (
  'flight-liveries',
  'flight-liveries',
  true,
  2097152,
  array[
    'image/webp',
    'image/png',
    'image/jpeg'
  ]
)
on conflict (id) do update
set
  name = excluded.name,
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists "flight_liveries_public_read" on storage.objects;
create policy "flight_liveries_public_read"
on storage.objects
for select
to public
using (bucket_id = 'flight-liveries');

commit;
