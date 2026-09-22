# User journey safety fixes — 22 September 2026

Implementation is local and tested. These changes have **not been deployed or applied to production**. The Data API guard is intentionally staged; direct Supabase access remains possible until the final cutover below.

## Changes

1. A displayed-flight manifest no longer deletes occurrences simply because one device omitted them. Exact duplicate occurrences may still be consolidated. Explicit owner-scoped deletion remains available. The iOS historical snapshot writer also stops deleting omitted cloud history.
2. Souvenirs carry a monotonically increasing revision. Updates must supply the next revision or receive HTTP 409. Database timestamps are server-owned; existing future timestamps are repaired without changing the payload. The app persists an account-scoped revision baseline, detects local/remote conflicts, preserves both versions until the user chooses, and checks for edits made during downloads. Old clients cannot silently overwrite a newer souvenir. Image names are treated as immutable; the app no longer deletes other devices' assets during a stale sync.
3. A Data API gateway forwards the user's JWT (never a service-role credential), allows only known tables/RPCs, and independently charges request attempts, including upstream failures. Limits: 300 attempts/min/user, 120 mutation attempts/min/user, 500 rows/page, 2 MiB request, 8 MiB response, 64 MiB returned bytes/min/user. Oversized responses are cancelled and charged. Existing general/IP/global ingress limits also apply. A database pre-request guard can require the private gateway header, closing the direct REST bypass even for GET/read-only transactions. Secrets stay on the server, with only a hash in a private DB table. Auth, Storage and Realtime are not routed through this gateway.
4. The shared-flight worker queue runs at most eight jobs concurrently, with at most 10,000 total pending/running/delayed jobs. Deferred coverage now uses the queue. In-memory cache entries are capped at 10,000 with active expiry every 30 seconds; live locks are not evicted to admit more work. Coverage batches stop advancing after the client disconnects. Saved-flight API reads are paginated at 100 rows/page.
5. Active-viewer registration checks owner/Circle read permission before touching watcher state or scheduling refresh work.
6. Live Activity registration requires verified paid membership. SQL caps active tokens at 20/account and four/flight, with a maximum 64 retained records/account and bounded inactive history. Final-flight tokens retire on registration cleanup; successful end delivery retires its token. Registration sends initial state only to its own newly registered token ID rather than every token for the flight. Expired token records are cleaned lazily; legitimate replacement at the cap is supported.
7. Tracking and canonical save/coverage reserve capacity in a separate committed transaction before provider lookup. Reservations serialize per account, expire after two minutes, and release in `finally`. New flight work is limited to 60 reservations/minute and 500/day UTC per user; a bridged tracking operation can consume both a tracking and coverage reservation. Final database subscription/session constraints still enforce actual stored capacity. Quota errors propagate instead of triggering legacy provider fallback. Concurrent changes can still make a final write fail after lookup; reservations are not a transaction spanning the external provider.

## Validation

- 344 backend tests passed, including eight new regression tests for concurrent worker limits, cache expiry, unauthorized refresh rejection, zero provider calls on reservation denial, CAS/clock skew, token cap/replacement, reservation expiry, direct-access rejection, download allowance and failed-request accounting.
- Existing manifest tests now assert preservation of absent/empty lists, and still verify duplicate consolidation.
- All migrations reproduce in isolated PGlite. Schema verification checks the new trigger and privilege boundaries.
- iOS simulator-target build succeeded for the actual client routing/conflict UI changes. Final targeted backend suite: 123 tests passed after the last reconciliation refinement. No simulator app interaction or App Store release was performed.
- No live user-data mutation, production flood, provider/APNs load or real multi-connection PostgreSQL stress test was performed.

## Coordinated rollout

1. Review migrations `20260922180000_journey_conflicts_and_work_bounds.sql` and `20260922181000_metered_data_gateway.sql`. Back up first and apply through the normal migration runner. Migration 180000 intentionally causes legacy souvenir updates without a new revision to fail safely instead of overwriting data. Schedule this with the app update; ordinary flight sync remains free.
2. Set a cryptographically random `RUNWY_DATA_GATEWAY_SECRET` of at least 32 characters on the backend. Set `SUPABASE_URL` and `SUPABASE_ANON_KEY` for the same project. Keep the secret out of clients, Git, logs and reports.
3. Deploy the backend, then run `scripts/configure-data-gateway.js --prepare` using the same secret and DB credentials. `RUNWY_ENV_FILE` can point to a private environment file. Prepare does not disable an already-required gateway. Backend startup requires the new database functions.
4. Install/release the updated iOS app. Exercise sign-in/profile, free flight sync, calendar history, paid sticker conflict choices, explicit deletion and account deletion with two devices. Verify gateway traffic and older-client behavior before cutover.
5. Run `scripts/configure-data-gateway.js --require`. This is the security cutover: direct authenticated REST requests without the private gateway header now fail with 403/update guidance. Test a direct request fails, a gateway request succeeds, and repeated failed writes receive 429. **Until this step, J2 remains open on the live direct REST route.**
6. Monitor 409/429/503 rates, queue depth, provider budget, DB pool waits and egress. Keep the revised app/backend/migrations aligned. Do not roll back to code that expects timestamp-based souvenir updates.

## Material limits

- The pre-request guard covers REST. Storage download bandwidth, Realtime and hosted Auth abuse require their own gateway/platform controls and are not closed by this change.
- Retaining immutable sticker assets avoids a cross-device deletion race. Unreferenced images can consume the existing 2,000-object/256-MiB allowance until safe revision-aware garbage collection or account deletion. This change prioritizes preservation over opportunistic destructive cleanup.
- A caller that knows the current revision can deliberately replace its own ticket. Revisions prevent accidental stale writes, not edits authorized by the same account.
- Auth claim/revocation hardening, least-privilege service credentials, broad historical retention, restoration drills and measured production capacity remain separate audit follow-ups.
