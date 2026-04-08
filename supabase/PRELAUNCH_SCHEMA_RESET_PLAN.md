# Runwy Prelaunch Schema Reset Plan

This file defines the recommended clean-slate database shape for Runwy before public launch.

## Why Reset Instead Of Keep Migrating

Runwy has no launched user base yet. That means we do not need to preserve a legacy app-owned schema just to avoid disruption.

We should keep:

- the existing Supabase project
- `auth.users`
- any auth/provider setup that is already working

But we should stop growing the hybrid app schema and instead replace it with a trimmed baseline that matches the actual product.

## Current Repo Reality

The codebase already proves these app-owned tables are actively depended on:

- `tracking_sessions`
- `user_flights`
- `live_snapshots`
- `notifications`
- `push_devices`
- `friend_invites`
- `friend_relationships`
- `friend_permissions`

The repo also still has app code or docs expecting:

- `past_flights`
- `flight_watchers`
- `profiles`
- `user_settings`
- `entitlements`

References:

- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/src/tracking-store.js`
- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/src/server.js`
- `/Users/sayanneogie/Documents/New project/runwy/App/Features/FlightTracking/Recovery/RecoverPastFlightService.swift`
- `/Users/sayanneogie/Documents/New project/runwy/ARCHITECTURE.md`

## Final Minimal Target Schema

This is the recommended final app schema.

### Keep / Create

- `profiles`
- `user_settings`
- `user_flights`
- `tracking_sessions`
- `live_snapshots`
- `push_devices`
- `notifications`
- `friend_invites`
- `friend_relationships`
- `friend_permissions`
- `entitlements`

### Drop / Do Not Recreate

- `past_flights`
- `flight_segments`
- `flight_watchers`
- `airports`
- `airlines`
- `import_requests`
- `calendar_import_candidates`
- `usage_logs`
- `audit_logs`

## Core Design Decisions

### 1. `user_flights` is the single flight truth

Do not keep a separate `past_flights` table in the final design.

Everything the user owns should live in `user_flights`, with lifecycle state controlling where it appears:

- `upcoming`
- `active`
- `landed`
- `archived`
- `deleted`

Historical imports, tracked flights, calendar imports, and manually added flights should all become rows in the same table.

### 2. `tracking_sessions` is operational, not the main product record

`tracking_sessions` should exist only for live tracking lifecycle and provider polling.

`tracking_sessions` should remain an operational table for provider polling and live-session state.

It is acceptable to keep:

- `owner_user_id`

even though ownership also exists on `user_flights`, because the worker code uses that field heavily for hot queries and notification fan-out.

### 3. Friend sharing should use permissions, not `flight_watchers`

`flight_watchers` is not needed in the trimmed model.

Sharing should be based on:

- `friend_relationships`
- `friend_permissions`
- read-safe queries/functions over the owner's `user_flights`

If a friend can view your shared flights, the backend should derive that from Circle permissions, not a second watcher table.

### 4. Airports and airlines stay app-local unless proven necessary

Static reference data like airport names, cities, and airline branding should stay in bundled app data or provider payloads unless there is a strong server-side need.

There is no reason to start with `airports` and `airlines` tables in the minimal baseline.

## Recommended Table Shapes

### `profiles`

Purpose:

- display name
- avatar
- lightweight app identity metadata

Columns:

- `user_id uuid primary key references auth.users(id) on delete cascade`
- `display_name text not null`
- `avatar_url text`
- `email text`
- `auth_provider text`
- `onboarding_completed boolean not null default false`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `user_settings`

Purpose:

- cross-device preferences

Columns:

- `user_id uuid primary key references auth.users(id) on delete cascade`
- `preferred_theme text not null default 'system'`
- `distance_unit text not null default 'km'`
- `uses_24_hour_time boolean not null default false`
- `default_airport_code text not null default ''`
- `validate_with_provider_enabled boolean not null default true`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `user_flights`

Purpose:

- one canonical row per owned/imported flight

Columns:

- `id uuid primary key`
- `user_id uuid not null references auth.users(id) on delete cascade`
- `source_type text not null`
- `lifecycle_state text not null`
- `display_flight_number text not null`
- `marketing_airline_code text`
- `marketing_airline_name text`
- `operating_airline_code text`
- `operating_airline_name text`
- `origin_iata text not null`
- `destination_iata text not null`
- `origin_city text`
- `destination_city text`
- `scheduled_departure timestamptz`
- `scheduled_arrival timestamptz`
- `estimated_departure timestamptz`
- `estimated_arrival timestamptz`
- `actual_departure timestamptz`
- `actual_arrival timestamptz`
- `departure_terminal text`
- `departure_gate text`
- `arrival_terminal text`
- `arrival_gate text`
- `baggage_claim text`
- `status text`
- `delay_minutes integer`
- `distance_km numeric`
- `flight_time_minutes integer`
- `aircraft_type text`
- `provider_name text`
- `provider_flight_id text`
- `route_polyline jsonb`
- `tracked_snapshot jsonb`
- `source_payload jsonb`
- `calendar_source_text text`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`
- `archived_at timestamptz`
- `deleted_at timestamptz`

Notes:

- `source_type` should cover values like `manual_search`, `calendar_import`, `tracked`, `historical_recovery`
- `lifecycle_state` should cover values like `upcoming`, `active`, `landed`, `archived`, `deleted`

### `tracking_sessions`

Purpose:

- provider lifecycle
- polling cadence
- live tracking worker ownership

Columns:

- `id uuid primary key default gen_random_uuid()`
- `owner_user_id uuid not null references auth.users(id) on delete cascade`
- `provider text not null`
- `provider_flight_id text`
- `flight_number text not null`
- `airline_code text`
- `origin_iata text`
- `destination_iata text`
- `travel_date date`
- `session_status text not null`
- `next_poll_after timestamptz`
- `polling_stopped_reason text`
- `metadata_json jsonb not null default '{}'::jsonb`
- `created_source text`
- `last_snapshot_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Notes:

- keep `owner_user_id` intentionally as an operational denormalization
- keep `user_flights.tracking_session_id` as the app-facing soft link

### `live_snapshots`

Purpose:

- latest canonical tracked state for a session

Columns:

- `tracking_session_id uuid primary key references public.tracking_sessions(id) on delete cascade`
- `provider text not null`
- `provider_flight_id text`
- `flight_number text`
- `airline_code text`
- `departure_airport_iata text`
- `arrival_airport_iata text`
- `snapshot_status text`
- `terminal text`
- `gate text`
- `baggage_claim text`
- `delay_minutes integer`
- `departure_times_json jsonb not null default '{}'::jsonb`
- `arrival_times_json jsonb not null default '{}'::jsonb`
- `alerts_json jsonb not null default '{}'::jsonb`
- `metrics_json jsonb not null default '{}'::jsonb`
- `canonical_snapshot_json jsonb not null default '{}'::jsonb`
- `raw_provider_payload_json jsonb not null default '{}'::jsonb`
- `provider_last_updated_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `push_devices`

Purpose:

- device token registration

Columns:

- `id uuid primary key default gen_random_uuid()`
- `user_id uuid not null references auth.users(id) on delete cascade`
- `device_id text`
- `apns_token text not null unique`
- `platform text not null default 'ios'`
- `push_enabled boolean not null default true`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `notifications`

Purpose:

- durable user notification records

Columns:

- `id uuid primary key default gen_random_uuid()`
- `user_id uuid not null references auth.users(id) on delete cascade`
- `tracking_session_id uuid references public.tracking_sessions(id) on delete set null`
- `friend_relationship_id uuid references public.friend_relationships(id) on delete set null`
- `notification_type text not null`
- `delivery_channel text not null default 'push'`
- `delivery_status text not null default 'queued'`
- `title text not null`
- `body text not null`
- `payload_json jsonb not null default '{}'::jsonb`
- `scheduled_for timestamptz`
- `sent_at timestamptz`
- `read_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `friend_invites`

Keep the current shape from:

- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/supabase/migrations/20260403_create_flight_circle.sql`

### `friend_relationships`

Keep the current shape from:

- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/supabase/migrations/20260403_create_flight_circle.sql`

### `friend_permissions`

Keep the current directional sharing shape, but expand it slightly.

Columns to preserve:

- `relationship_id`
- `owner_user_id`
- `viewer_user_id`
- `share_scope`

Columns to keep:

- `can_view_live`
- `can_view_history`

Columns to add:

- `notify_departure boolean not null default true`
- `notify_arrival boolean not null default true`
- `notify_delay boolean not null default true`
- `notify_gate_change boolean not null default true`

Remove the coarse-only model over time:

- `can_receive_alerts`

### `entitlements`

Purpose:

- lightweight mirror of RevenueCat-backed state

Columns:

- `user_id uuid not null references auth.users(id) on delete cascade`
- `provider text not null default 'revenuecat'`
- `product_id text`
- `entitlement_key text not null`
- `is_active boolean not null`
- `expires_at timestamptz`
- `last_synced_at timestamptz not null default now()`
- `raw_payload jsonb not null default '{}'::jsonb`
- unique `(user_id, entitlement_key)`

## Fresh Baseline RLS Rules

Minimum direct access rules:

- `profiles`: self only
- `user_settings`: self only
- `user_flights`: owner only
- `tracking_sessions`: owner through `user_flights`
- `live_snapshots`: owner through `tracking_sessions -> user_flights`
- `push_devices`: self only
- `notifications`: self only
- `entitlements`: self only
- Circle tables: use current member/owner RLS

Friend viewing should not be granted by raw table access to all rows. Use a dedicated function or secure query path that applies `friend_permissions`.

## Code Paths That Must Change For This Reset

### 1. Remove `past_flights` table dependency

Current file:

- `/Users/sayanneogie/Documents/New project/runwy/App/Features/FlightTracking/Recovery/RecoverPastFlightService.swift`

Current behavior:

- reads and writes `/rest/v1/past_flights`

Target behavior:

- write recovered historical flights into `user_flights`
- query archived history from `user_flights where lifecycle_state in ('archived', 'landed')`

### 2. Remove `flight_watchers` dependency

Current files:

- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/src/tracking-store.js`
- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/src/server.js`

Current behavior:

- watcher rows are used for notification recipients and shared access

Target behavior:

- derive recipients from `friend_permissions`
- no separate watcher table

### 3. Rework `tracking_sessions` ownership

Current file:

- `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/src/tracking-store.js`

Current behavior:

- `tracking_sessions.owner_user_id`

Target behavior:

- `tracking_sessions.user_flight_id`
- join back to `user_flights.user_id`

### 4. Keep local cache, not local truth

Current files:

- `/Users/sayanneogie/Documents/New project/runwy/App/Features/FlightTracking/Planning/PlannedFlightStore.swift`
- `/Users/sayanneogie/Documents/New project/runwy/ViewModels/AppSessionViewModels.swift`

Target behavior:

- local stores remain startup caches only
- Supabase rows are the truth

## Reset Sequence

Because there are no launched users yet, the clean sequence is:

1. Freeze schema changes on the additive path.
2. Do not run `/Users/sayanneogie/Documents/New project/backend/aviationstack-proxy/supabase/migrations/20260407_add_core_user_data_tables.sql` in production as the final baseline.
3. Create a new fresh baseline migration set for the tables in this document.
4. Update backend worker/server SQL to the new table contracts.
5. Update iOS app code that still depends on `past_flights`.
6. Update Circle/friend notification decisioning to use `friend_permissions`, not `flight_watchers`.
7. Only after that, apply the new baseline to Supabase.

## Recommended Next Deliverables

1. Write a new baseline SQL migration for the final table set in this document.
2. Delete or supersede the additive migration approach.
3. Rework backend SQL in `tracking-store.js` and `server.js` to match the new schema.
4. Repoint app history sync from `past_flights` to `user_flights`.

## Bottom Line

For prelaunch Runwy, the clean trimmed schema should be:

- one canonical `user_flights` table
- one operational `tracking_sessions` table
- one latest-state `live_snapshots` table
- one Circle permission model
- one notification/device model
- one profile/settings model
- one entitlement model

Everything else should either stay local, be derived, or be deferred until a real product need proves it.
