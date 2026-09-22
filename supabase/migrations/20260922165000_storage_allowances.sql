-- Serialize additions for the app-owned private buckets. Storage API still owns blobs.
create function public.runwy_bound_storage() returns trigger language plpgsql security definer set search_path='' as $$
declare account_prefix text; bytes bigint; objects bigint; incoming bigint;
begin
 if new.bucket_id not in('profile-avatars','ticket-stickers') then return new; end if;
 account_prefix:=split_part(new.name,'/',1);
 perform pg_advisory_xact_lock(hashtextextended('runwy:storage:'||account_prefix,0));
 if length(new.name)>1024 then raise exception 'Object path too long' using errcode='23514'; end if;
 if new.metadata->>'size' is not null and new.metadata->>'size' !~ '^[0-9]{1,12}$' then
  raise exception 'Invalid object size' using errcode='23514'; end if;
 incoming:=coalesce((new.metadata->>'size')::bigint,0);
 select count(*),coalesce(sum(case when metadata->>'size' ~ '^[0-9]{1,12}$' then (metadata->>'size')::bigint else 0 end),0)
 into objects,bytes from storage.objects where bucket_id in('profile-avatars','ticket-stickers')
 and name like account_prefix||'/%' and id<>new.id;
 if objects>=2000 or bytes+incoming>268435456 then raise exception 'Account upload allowance reached' using errcode='PT429'; end if;
 return new;
end $$;
revoke all on function public.runwy_bound_storage() from public,anon,authenticated;
create trigger runwy_bound_storage before insert or update on storage.objects for each row execute function public.runwy_bound_storage();
