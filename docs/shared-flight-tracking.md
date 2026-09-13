# Shared flight tracking and provider request reuse

## Data flow

FlightAware API → shared response cache → identity/ordering validation → canonical flight + event transaction → app projection, Live Activity, recipient-specific APNs outbox.

The shared flight service remains the refresh owner. Existing phase-aware polling intervals and lifecycle checks are unchanged. Existing per-flight database leases remain; a new response-level lease also covers callers purchasing the same provider URL. Exact provider IDs distinguish flight occurrences; status, position, and track URLs have separate cache entries. Position fallback and a direct track request can reuse the same track response.

Opening details uses cached provider data within the existing TTL. Explicit forced refreshes and provider-event refreshes can bypass older responses. Requests started before an event cannot satisfy its forced refresh. Copies into local caches retain the original expiration, rather than extending the life of data on every read. Current operational-details refreshes continue to skip position enrichment.

## Accuracy and notification rules

- Wrong origin, wrong destination (without established diversion), weak identity, and wrong flight number are rejected before shared state or alerts change.
- A replacement provider ID must stay within six hours of the original scheduled departure. Estimated departure changes do not change flight identity. Larger schedule-ID replacements require revalidation rather than silently joining another day's service.
- Destination changes after departure are interpreted as diversions only for the same provider occurrence or explicit diversion evidence.
- Canonical writes compare state revisions atomically, preventing an older reader from overwriting a newer commit. Rejected commits produce no events.
- Cached exact-ID data older than a confirmed stream/webhook event is ignored; older timestamped positions cannot move the plane backwards.
- Each notification event retains the flight snapshot that created it. Later changes cannot rewrite its delay time, gate, or destination during fanout/retry.
- Recipients, permissions, Circle filtering, personalization, device tokens, and durable delivery deduplication remain independent. Sharing a flight event does not share a user's notification payload.

These checks prevent detectable identity and ordering errors. They cannot independently prove that every fact supplied by FlightAware is correct. Suspicious inputs retain the last accepted state and follow the existing revalidation path.

## Storage and cost

`provider_response_cache` holds short-lived provider JSON, requested time, and expiry under hashed URLs. It holds no API keys or user identifiers. Access is backend-only with RLS and no anon/authenticated grants. A timed-out lease owner cannot overwrite another owner's cached response. Failed responses and provider error envelopes are not cached. Unavailable coordination fails the refresh rather than making an uncoordinated duplicate paid request; existing confirmed state remains available.

`api_usage_logs.provider_path` and `request_reason` identify actual outbound calls. Shared-response hits are recorded as `shared_response`, cost zero; existing outbound budget buckets remain unchanged. Local memory cache hits do not add an outbound log. `cost_estimate` remains an application request-unit counter, not an exact FlightAware invoice calculation.

The API now claims final route jobs every 30 seconds using the existing durable database claim/retry mechanism. Its route job uses stored canonical points only, preserving production's provider-disabled poller behavior. The legacy poller remains compatible during rollout. No provider polling frequency was increased or reduced.

## Rollout

1. Apply `supabase/migrations/20260914_shared_provider_responses.sql` before deploying this code. It is additive and repeatable; old code continues to work with it.
2. Deploy API code. Keep current cache TTLs, active-flight polling intervals, budgets, and notification settings. Confirm health, shared-response hits, outbound path logs, canonical projections, and APNs outbox progress.
3. Observe at least one full tracked flight, including a final route capture; compare notification latency, route completeness, errors, and outbound requests per flight-hour against the baseline. Lower call counts must not come from increased failed refreshes.
4. Stop the legacy poller only after the API route job is verified. Its `force:true` entrypoint means setting `ENABLE_TRACKING_POLLER=false` alone is insufficient; stop the service.
5. Pause the Firehose service while account access is unavailable. Retain its integration and configuration for a future switch. Do not turn off API polling until real Firehose delivery has been verified.

Rollback: redeploy the previous API commit and resume the poller if stopped. Leave the additive schema in place; it is backward compatible. No production migration, deployment, or service shutdown was performed while implementing this branch.

## Validation

- `FLIGHT_DATA_PROVIDER=flightaware DISABLE_PROVIDER_CALLS=true npm test`
- Isolated PostgreSQL-engine verification (no production credentials):
  `npm install --prefix /tmp/runwy-db-verification --no-audit --no-fund @electric-sql/pglite`
  `node scripts/verify-shared-flight-db.cjs /tmp/runwy-db-verification/node_modules/@electric-sql/pglite`

The isolated check covers repeatable migration, canonical revision checks, atomic event creation, event snapshots, database-backed cross-worker reuse, and lease-owner fencing. Production deployment and real APNs delivery remain rollout checks.
