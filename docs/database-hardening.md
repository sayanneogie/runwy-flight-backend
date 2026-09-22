# Database hardening — 22 September 2026

The database keeps shared flight facts separate from each user's saved travel records. This release closes the ownership, sharing, duplicate-identity and deployment gaps without resetting production or discarding saved history.

## Migration contract

`supabase/migrations` is now the executable sequence: one schema baseline and five forward migrations, all with unique 14-digit versions. The 29 historical scripts are preserved in `supabase/legacy-migrations` for reference and regression tests. Do not replay that directory: it contains overlapping versions and references to tables retired in the production reset.

A fresh Supabase project supplies its usual auth, storage, extension schemas and API roles, then applies the six current migrations. The isolated PostgreSQL tests reproduce the application schema with minimal Supabase fixtures. They are not a substitute for a managed Supabase backup restore test.

Existing production must **adopt**, rather than execute, the baseline. Adoption compares its schema to the reviewed schema-only SHA-256 manifest in `supabase/schema/pre-baseline-signature.json`. It aborts on drift. Original Supabase migration records, including their SQL, are copied into the RLS-protected `runwy_security.legacy_migration_history` before replacing the active history with the baseline record. No application rows are reset.

Use a credentials file outside Git, with DATABASE_URL and DATABASE_SSL. Never paste connection strings into commands or logs.

```sh
node scripts/db-migrate.js --adopt-baseline --env-file /secure/path/backend.env
node scripts/db-migrate.js --apply --env-file /secure/path/backend.env
node scripts/db-migrate.js --verify --env-file /secure/path/backend.env
node scripts/db-migrate.js --env-file /secure/path/backend.env
```

Each forward migration and its history record commit atomically under a migration advisory lock. On failure, stop and inspect the failing migration; completed versions are skipped on retry. Do not recapture the manifest merely to bypass a drift warning. Review the drift first. Do not run baseline adoption on a different existing project.

Apply database migrations before deploying the backend. Deploy the named `accept-friend-invite`, `flight-circle-overview`, and `delete-account` Edge Functions after the migrations. Do not prune unrelated functions. The backend refuses startup when its required database functions are missing. Verify its `/health` build SHA after deployment.

## Authorization and sharing

- Tracking sessions and raw live snapshots are owner-only. A composite foreign key proves every saved-flight/session reference belongs to the same user. Privileged cleanup independently checks the owner.
- Circle clients receive a limited overview through an authenticated Edge Function. Private flights remain private. Future, all-flight and explicit selected-flight scopes are enforced with history/live permissions. No existing private flight is made shareable by this release.
- `runwy_set_circle_flights` accepts only the caller's saved flight IDs. It selects the relationship's scope; each selected flight must also have `visibility='circle'`. Owner sharing-selection UI remains separate product work; no flight is opted in implicitly.
- Member removal persists on the server and revokes both directions. Incoming alert preferences are stored separately from the owner's sharing consent. Overview, fanout and queued delivery authorization use the same permission predicates.
- Invite acceptance claims the invite, creates or reactivates the pair, and grants directional permissions atomically. Replays by a different account fail. Replays by the same account do not overwrite existing active preferences.
- API roles have the operations/columns they need; internal flight tables and operational mutation are not client-writable. Future object defaults no longer automatically grant client access.
- Only `live_snapshots` is added to Realtime. The owner RLS boundary applies to subscriptions.

## Record identity and lifecycle

Canonical linking is transactional, serializes by user/physical flight, preserves the surviving ID, moves references and tombstones a proven duplicate. Alias records prevent older devices from resurrecting the losing ID. Tracking persistence reconciles both canonical and session unique keys before upserting. Canonical deletion is restricted while a user has a saved record. This release does not heuristically collapse historical rows merely because their flight number/date look alike.

`tracking_sessions.flight_instance_id` is a typed, indexed FK. The metadata projection trigger maintains compatibility with existing writers. New queries use the typed link.

`device_tokens` is the authoritative ordinary APNs registration. One server transaction handles rotation, account reassignment and the legacy `push_devices` mirror. Live Activity tokens and per-token outbox records retain their distinct roles.

The API runs a bounded hourly cleanup: up to 500 rows per category, retaining 90 days of finalized-flight snapshots and diagnostic logs. At least the latest snapshot of each flight survives. Expired provider-cache entries are pruned. Saved flights, tombstones, souvenirs, canonical events and delivery/deduplication records are retained. This intentionally starts conservatively; evaluate a shorter raw-payload window using observed storage growth and provider contracts.

Redundant direct-client updates are skipped when only updated_at changes. Backend timestamp acknowledgements are preserved. Pool size is explicit (DATABASE_POOL_MAX, default 8 per API replica; protection pool adds up to 3). Completed in-memory queue jobs release their payloads and pending jobs have a cap; durable database leases/outbox remain responsible for recovery.

## Account deletion and retirement

The authenticated `delete-account` Edge Function records a durable deletion job, blocks new private uploads, removes objects through the Storage API in bounded batches, then deletes the account. An interrupted request resumes when retried. The compatibility RPC refuses to delete auth data while private objects remain. No routine SQL deletes storage object metadata.

Do not retire `flight-liveries` until supported installed builds are confirmed independent of it. Use the Storage API to empty/delete the bucket before removing its policy. The current bucket and artwork are retained. Broken legacy RPCs lose client execution privileges; compatibility tables/columns remain until callers are retired. Distance units stay on-device in the updated iOS source; the DB column remains solely for old-build compatibility.

## Remaining operational work

Completed daily physical backups were verified; PITR was disabled. A successful production-backup restore, a narrower Railway runtime DB credential, current billing headroom/Auth configuration, and representative load/Realtime device testing still require separate operational validation. This release does not change billing plans, rotate a deployed DB credential, remove compatibility assets, or claim that these checks have passed.

The iOS Circle controls and account deletion client must ship in a new app build to reach installed devices. Server migrations alone cannot update installed apps.
