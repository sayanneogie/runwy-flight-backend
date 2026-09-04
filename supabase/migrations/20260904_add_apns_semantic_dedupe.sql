-- Claim each passenger-visible flight event once per user before calling APNs.
-- The partial unique index is the concurrency boundary for queue retries,
-- duplicate provider events, and multiple workers processing the same change.
alter table public.notification_deliveries
  add column if not exists dedupe_key text;

create unique index if not exists notification_deliveries_user_dedupe_key_channel_uidx
  on public.notification_deliveries (user_id, dedupe_key, channel)
  where dedupe_key is not null;
