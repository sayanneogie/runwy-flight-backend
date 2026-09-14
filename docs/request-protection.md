# Request and provider abuse protection

## Request path

Railway's automatic Fastly DDoS protection filters traffic before it reaches Node.
The API then applies, in order:

1. A bounded local IP quota, 200 requests/second per process, and 64 active requests.
2. Postgres quotas: 6,000 requests/minute across the API and 600/minute per client network.
3. For all three FlightAware webhook aliases, secret verification and a shared 600/minute quota.
4. A 100 KB JSON body limit, authentication, and shared per-user quotas.

`GET` and `HEAD /health` only use the local barrier, so liveness doesn't depend on
a database round trip. Every other path, including invalid paths, is covered by
the shared ingress quotas. Search aliases share one bucket. Test notification
quotas are per user; changing `X-Device-Id` does not reset any quota.

The API quota defaults to 300/minute and search to 60/minute. Existing deployment
environment overrides remain effective (production had API quota 60/minute).
These are fixed windows starting at the first request, not a concurrency guarantee.

On Railway, the quota identity uses the first IP in the edge-overwritten
`X-Forwarded-For` header, following Railway's explicit contract. Selecting the
last hop would incorrectly group users behind Fastly. This behavior is enabled
only when `RAILWAY_ENVIRONMENT_ID` is present. Other deployments use Express's
proxy trust (zero hops by default), configurable with `TRUST_PROXY_HOPS` after
verifying their topology. IPv6 clients share a /56 network quota. Unauthenticated
device IDs are never used for rate-limit identity.

## Storage and failure behavior

The private `runwy_security` schema stores hashed identities and counters.
Atomic upserts share quotas across replicas and restarts. Anonymous and signed-in
database client roles cannot read, mutate, or invoke these controls. Only backend
database credentials and the Supabase service role can consume quotas.

The limiter has a separate three-connection database pool, two-second acquisition
and query deadlines, and a 1.5-second server statement deadline. A failed quota
check returns 503 with `Retry-After`; it never falls back to a fresh memory quota.
Production requires the database. Local tests can use memory-only stores.
Expired counter rows are removed in bounded batches every minute by the API.

Node has a 15-second incoming request deadline, 10-second header deadline,
five-second keepalive, and 100 requests per socket. Supabase token verification
has a five-second timeout, and malformed/expired tokens are rejected locally.

## Supabase Edge Functions

The four Circle functions have bounded local counters and concurrency, plus
durable per-function global (600/minute) and client-network (60/minute) quotas.
Authenticated actions also have per-user quotas: invite creation 10/minute,
acceptance 30/minute, overview 60/minute. Hosted Supabase's `cf-connecting-ip`
header supplies client identity; missing/invalid values share an `unknown` quota.
The global cap still applies regardless of client headers.

Bodies are limited to 16 KB of actual streamed bytes with a five-second deadline.
Invite tokens must be 64 hexadecimal characters before database lookup. Error
responses have retry guidance and are not cached. Production's existing
`verify_jwt=false` setting is preserved: protected handlers still call Supabase
Auth to verify users, including asymmetric signing-key tokens.

## Provider budgets

`runwy_reserve_flightaware_budget` reserves estimated FlightAware flight-call
units atomically before the external request, shared by API and workers. It seeds
today's counter from existing usage on first use. Concurrent requests cannot all
spend the same remaining capacity. Failed calls retain their reservations and
usage-log failures do not reopen the budget. General/search and tracked-flight
reserves retain the existing limits and UTC day boundary.

This limits the FlightAware flight calls routed through the budget wrapper. It
does not cap all Railway/Supabase billing, FlightAware alert subscriptions, or
other providers. `DISABLE_PROVIDER_CALLS=true` remains the provider emergency stop.
Production webhook registration uses the configured `WEBHOOK_PUBLIC_BASE_URL`,
preventing caller-supplied host headers from redirecting callbacks containing secrets.

## Rollout and validation

1. Run `npm ci`, `npm test`, and `npm audit --omit=dev`.
2. Run `npx deno check supabase/functions/{preview-friend-invite,create-friend-invite,accept-friend-invite,flight-circle-overview}/index.ts`
   and `npx deno test supabase/functions/_shared/http_test.ts`.
3. Apply only the additive migration with the production database environment:
   `node scripts/migrate-request-protection.js --apply`.
4. Deploy the API and both workers from the same commit; old worker versions
   do not use atomic reservations, so the rollout must include them.
5. Deploy the four named Circle functions. Do not prune other deployed functions.
6. Verify `/health`, unauthenticated 401 responses with rate-limit headers,
   header-spoof resistance, and an invalid invite token's bounded response.

Tests exercise the actual migration in embedded Postgres, replica/restart
behavior, concurrent budget reservations, database-role permissions, IPv6
normalization, middleware ordering, webhook aliases, and storage outages.

## Monitoring and incident response

Node emits aggregated `request_protection` counters once a minute when nonzero;
Edge Functions emit `edge_request_protection` summaries. They contain no tokens,
webhook secrets, full URLs, raw IP addresses, or request bodies. Watch repeated
429s/503s, database latency, CPU/memory, and provider-budget exhaustion.

Railway's optional Under Attack mode requires browser clearance and can reject
native iOS clients. It is not enabled by this change. Prefer the API-compatible
quotas and provider emergency switch for this service.

Railway's spending alerts/caps and resource monitors are separate dashboard
settings. A hard spending cap can stop services; do not set an arbitrary dollar
cap without the owner's budget decision.

References: [Railway DDoS protection](https://railway.com/changelog/2026-02-20-domains),
[Express proxy trust](https://expressjs.com/en/guide/behind-proxies/),
[Railway client-IP contract](https://station.railway.com/questions/which-header-should-i-rely-on-for-real-c-d78a6f96),
[Supabase gateway log headers](https://supabase.com/docs/guides/observability/advanced-log-filtering).
