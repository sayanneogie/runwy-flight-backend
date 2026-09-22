alter table public.live_activity_tokens
  add column if not exists last_content_phase text;
