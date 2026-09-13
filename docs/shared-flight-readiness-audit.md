# Shared-flight architecture readiness audit

Date: 2026-09-14. Branch: `codex/shared-flight-efficiency`.

## Verdict

The initial implementation was not ready for production rollout without fixes. This audit found and corrected concurrency, freshness, notification recovery, validation, and cost-control defects. After those fixes, all available local checks pass. The branch is ready for a controlled rollout with the migration applied first; it is not yet proven by live production operation.

No production database changes, deployments, service shutdowns, paid FlightAware requests, or real APNs sends were performed in this audit. The current app remains on its existing backend.

## Findings fixed

| Area | Failure mode | Correction and coverage |
| --- | --- | --- |
| Shared cache lifetime | A stricter caller could receive data cached using another worker's longer TTL. | Reuse respects both stored expiration and caller TTL; regression test reproduced the failure before correction. |
| Lease ownership | A request that lost its lease could still return its result after a fenced cache write was rejected. | Cache writes return ownership success; a lost lease prevents returning accepted data. Regression and database fencing checks. |
| Timestamp precision | PostgreSQL Date objects lost milliseconds when converted to HTTP headers. | Explicit ISO timestamps; regression coverage. |
| Cross-worker UI consistency | Process-local cache could hide a newer canonical gate or status. | Revision/freshness check against shared storage before cache return; regression coverage. |
| Duplicate discovery | A late initial lookup could overwrite a newer operational record. | Insert-only discovery; explicit timezone/date repair uses compare-and-set. Memory regression and PostgreSQL verification. |
| Partial APNs fanout | A crash after notifying the first recipient could prevent recovery for remaining users. | Persistent whole-event completion marker. Interrupted three-recipient test verifies remaining deliveries and no duplicate accepted sends. |
| Revoked notification access | Queued retries could use permissions captured before Circle access or preferences changed. | Current target/device authorization checked before send. Revocation retry test. |
| Identity/time validation | Invalid dates and replacement IDs without a verifiable scheduled occurrence could pass. | Calendar validation, local-date checks when an offset is supplied, and stronger replacement identity requirements. Leap-day and malformed-time cases. |
| Stored route points | Null coordinates could be coerced to zero; impossible coordinates could be stored. | Reject missing, boolean, and out-of-range coordinates while preserving legitimate zero coordinates. |
| Stream ordering | A future-dated event could advance the ordering watermark and block valid updates. | Reject invalid timestamps and events beyond the two-minute clock-skew allowance. Follow-up valid event tested. |
| Migration safety | New code could start accepting traffic with the required migration absent. | Startup schema probes fail with the migration name before listening. Missing-schema test. |
| Concurrent costs | Different flights could independently pass the daily budget check and exceed it. | Short database transaction/advisory lock reserves capacity before HTTP. Exhaustion, rollback, bucket separation and SQL-engine checks. Reservations are conservative request units, not billing receipts. |
| Revalidation | Suspicious-data jobs were added with execution disabled. | Executable 60-second delayed revalidation, deduplication and startup recovery. Original confirmed state remains intact. |
| Long-running memory | Diagnostic job history grew without bound. | Bounded history with monotonically increasing job IDs; discarded history does not cancel execution. |

## Final verification

- **319 tests passed**, zero failed, skipped or cancelled, using `FLIGHT_DATA_PROVIDER=flightaware DISABLE_PROVIDER_CALLS=true npm test`.
- An HTTP integration test enables the fetch path only against an in-process localhost fixture. It rejects any unexpected destination. Twenty concurrent exact-ID lookups issue one HTTP request; forced refresh gets new data; wrong provider IDs are rejected; position fallback and route loading share one track response.
- Offline stress: **2,000 requests across 20 coordinator instances**, covering **120 distinct endpoint/occurrence keys**, produce **120 loads** with correct data isolation.
- Offline revision contention: **100 rounds of 20 competing updates** accept exactly one revision per round.
- An isolated PGlite PostgreSQL engine verifies repeatable migration, transaction rollback, stale-write rejection, safe duplicate discovery, immutable event snapshots, fanout completion, backend-only cache access, response reuse, lease fencing, and budget reservations/bucket separation.
- `git diff --check` passes.

The stress coordinator/repository uses shared in-process stores. The PGlite check validates real PostgreSQL SQL semantics but does not emulate several Railway hosts or full production database topology. Neither is represented as a live production load test.

## Required rollout evidence

1. Apply the additive migration, then deploy the API while keeping the existing poller available. Verify startup/health and schema compatibility.
2. Observe a full actual flight through predeparture, takeoff, airborne tracking, landing, baggage where available, and final route capture. Compare request counts and update latency with the baseline. Include a traveler and a permitted Circle recipient.
3. Confirm APNs acceptance and receipt on a real device; exercise retry/restart behavior in a staging environment with test devices rather than generating test alerts for real travelers.
4. Confirm API route capture uses stored points and has no paid final-route downloads before stopping the legacy poller.
5. Pause Firehose while access is unavailable. Future Firehose activation needs an actual delivery check before reducing API polling.

No local test can prove that FlightAware never reports an incorrect real-world fact, or that APNs delivers every accepted notification. The system rejects detectable mismatches and preserves confirmed data; ambiguous APNs transport outcomes remain deliberately non-retried to avoid duplicate alerts. A final production-readiness sign-off requires the rollout evidence above.

## Reproduce

```sh
FLIGHT_DATA_PROVIDER=flightaware DISABLE_PROVIDER_CALLS=true npm test
node scripts/stress-shared-flight.cjs
npm install --prefix /tmp/runwy-db-verification --no-audit --no-fund @electric-sql/pglite
node scripts/verify-shared-flight-db.cjs /tmp/runwy-db-verification/node_modules/@electric-sql/pglite
```
