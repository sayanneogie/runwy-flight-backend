# Database abuse fixes — 22 September 2026

Six reproduced findings are addressed by migrations 20260922160000–20260922165000 and the accompanying backend/client changes.

| Finding | Enforced behavior |
| --- | --- |
| Direct write/storage abuse | Owner resource locks; 512 KiB saved-flight, 128 KiB souvenir, 16 KiB profile/settings rows; 200-character labels. Combined account allowance: 10,000 rows and 128 MiB serialized JSON across those tables. Authenticated writes: 1,000 changed rows per transaction and 2,000/minute. Mutating Data API requests: 120/minute. Private Storage buckets share 2,000 objects / 256 MiB per account. |
| Notification poisoning | SQL and HTTP alert-shape validation, including legacy severity keys and app quiet hours. Fanout tolerates malformed legacy values without casts, and selects only recipient identifiers instead of full saved-flight JSON. |
| Tracking race | A service-only SQL operation serializes capacity reservation and session reuse. Counts paused bridges with live subscriptions. Canonical subscriptions have a separate hard cap of 20. Server-owned canonical dates/status determine activity, so client dates and archived labels cannot evade the cap. Existing configured tracking limit is retained, bounded to 1–20. |
| Weather amplification | Same-process in-flight deduplication, shared DB leases and observation cache, 8-second provider timeout, failure backoff, maximum 32 simultaneous local keys, and shared 2,000-call UTC-day budget. User-triggered weather requires owner/Circle authorization. |
| Push token accumulation | Both registration routes carry device identity; SQL validates hex tokens, identity, platform and environment. Account lock and 20-active-token cap preserve rotation. Tokens older than 90 days are excluded at delivery and disabled lazily at registration. |
| Direct premium snapshot reads | Public Realtime snapshot is sanitized. Full canonical/provider payloads are in a private RLS table. Backend persistence updates both atomically and preserves revision ordering. New bounded snapshot endpoint verifies ownership and server-side paid membership. |

Known limiter denials are cached locally until reset, capped at 10,000 keys. Detailed operational health requires the existing administrator identity.

## Verification

334 Node tests pass, including isolated PostgreSQL rejection, owner isolation, canonical-cap bypass attempts, storage allowances, malformed preference fanout, concurrent reservation and push-cap tests. The actual tracking-store persistence path verifies private/public snapshots, repeated upserts and stale revision rejection. Twenty concurrent weather calls make one mocked provider call. Two hundred fifty sequential already-denied limiter checks make one DB call. Swift parser checks pass for the two edited client files; no simulator build was run.

All six migrations were exercised against the production schema inside a transaction that was rolled back before deployment. Existing maximum flight row was 195,125 serialized bytes and souvenir row 47,466 bytes, below the limits. The production preflight also validated all existing alert JSON against the new constraints. Storage's managed table does not permit our role to create an index; the quota trigger uses its existing bucket/name access path and requires only the granted trigger privilege.

## Client rollout

Local iOS edits batch cloud writes into groups of 100 and fetch snapshots from `/v1/tracking/snapshots?ids=...` with the existing access token. Basic status and Realtime subscriptions remain compatible with old app builds. Updated cloud-sync retrieval of full paid telemetry requires the new app build. The iOS changes are not included in this backend repository or shipped to users by a backend Git push.

## Limits and remaining work

- PostgREST GET/HEAD transactions are read-only: they cannot increment these SQL request counters. Direct reads still rely on RLS, existing statement timeouts and bounded data, not a complete per-user read-rate quota. A gateway/proxy plus revoking direct API reads is a separate client migration.
- Failed SQL transactions roll back their quota counter increments. These controls bound accepted growth and per-transaction work, not every attempted request or database CPU cost. Production was not subjected to a destructive saturation test.
- Account JSON bytes are logical payload size, not physical disk/index/WAL size. Operational growth, table bloat, inactive token history and notification history still need monitoring/retention decisions.
- Existing unpaginated saved-flight listing, broader scalar/date validation, mandatory JWT expiry checks, least-privilege backend credentials, PITR and restore rehearsal remain follow-up items from the scan. These changes do not claim that all possible vulnerabilities are excluded.
- Multi-process weather exclusion is implemented with PostgreSQL leases; the automated burst test uses isolated PostgreSQL and mocked providers, not a full multi-replica load environment.

Keep the new schema when rolling back application code. Restoring an older app server alone will lose the new private-snapshot write/read integration and tracking reservation protection; review a coordinated rollback rather than blindly reverting the schema. No original user records are intentionally deleted by these migrations.
