alter table public.notifications
  add column if not exists dedupe_key text;

create unique index if not exists notifications_user_dedupe_key_uidx
  on public.notifications (user_id, dedupe_key)
  where dedupe_key is not null;
