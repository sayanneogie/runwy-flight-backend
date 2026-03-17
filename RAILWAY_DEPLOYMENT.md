# Railway Deployment

This backend now expects two Railway services:

- `runwy-api`
- `runwy-flight-poller`

Both services deploy from the same repo path:

- root directory: `/`

## Service Layout

### API service

- start command: `node src/server.js`
- healthcheck path: `/health`
- public domain: required

### Poller service

- start command: `node src/flight-poller.js`
- healthcheck path: none
- public domain: not required

The API service should not run the poller loop. The worker owns polling.

## Shared Required Variables

Set these on both services:

- `DATABASE_URL`
- `DATABASE_SSL`
- `FLIGHT_DATA_PROVIDER`
- provider secret for the chosen provider:
  - `AVIATIONSTACK_KEY`
  - or `FLIGHTAWARE_API_KEY`
- auth verification config:
  - `SUPABASE_JWT_SECRET`
  - or `SUPABASE_URL` plus `SUPABASE_ANON_KEY`

## API-only Variables

Set these on the API service if needed:

- `PORT`
- `ENABLE_TRACKING_POLLER=false`
- `WEBHOOK_SHARED_SECRET`
- `WEBHOOK_PUBLIC_BASE_URL` (optional override if the public webhook URL should not be inferred from the request host)
- `APNS_KEY_ID`
- `APNS_TEAM_ID`
- `APNS_BUNDLE_ID`
- `APNS_PRIVATE_KEY`
- `APNS_PRIVATE_KEY_BASE64`
- `APNS_USE_SANDBOX`

## Poller-only Variables

Optional tuning for the poller service:

- `POLLER_INTERVAL_MS`
- `POLLER_BATCH_SIZE`
- `TRACKING_POLLER_LOG_SUMMARY`
- `WEBHOOK_REFRESH_MIN_INTERVAL_MS`

Optional shared fetch tuning:

- `CACHE_TTL_MS`
- `FLIGHTAWARE_POSITION_CACHE_TTL_MS`
- `STALE_FETCH_REFRESH_THRESHOLD_MS`
- `SEARCH_LIVE_ENRICH_LIMIT`
- `FLIGHTAWARE_ENABLE_MAP_FALLBACK`
- `DISABLE_PROVIDER_CALLS`
- `PROVIDER_CALLS_ENABLED`

Optional safety rails:

- `MAX_ACTIVE_TRACKING_SESSIONS_PER_USER`

Notes:

- `MAX_ACTIVE_TRACKING_SESSIONS_PER_USER` defaults to `20` outside production and effectively disabled in production.
- Set `MAX_ACTIVE_TRACKING_SESSIONS_PER_USER=0` to disable the limit explicitly.
- `FLIGHTAWARE_ENABLE_MAP_FALLBACK` defaults to `false` to avoid expensive map fallback calls unless you intentionally enable them.
- `WEBHOOK_REFRESH_MIN_INTERVAL_MS` defaults to `900000` (15 minutes) so repeated webhook bursts do not keep re-refreshing the same tracked flight.
- When wiring FlightAware alerts, prefer a webhook URL like `/v1/webhooks/flightaware?secret=...` so the provider can authenticate without relying on custom headers.
- Runwy auto-creates FlightAware post alerts from `/v1/track` when provider calls are enabled and `WEBHOOK_SHARED_SECRET` is set.
- Set `DISABLE_PROVIDER_CALLS=true` for an emergency hard stop on all provider-backed FlightAware/Aviationstack fetches. Stored tracked data and past-flight data remain readable, but new search/track requests will stop using the provider.
- `PROVIDER_CALLS_ENABLED=false` is supported as an equivalent positive/negative toggle if you prefer that style.

## Recommended Railway Settings

### Root directory

Set the root directory for both services to:

- `/`

### Watch paths

This repo is backend-only, so the default repo watch path is fine.

### Healthcheck

Only configure the API service healthcheck:

- `/health`

Expected API health response:

- `ok: true`
- `persistence: "supabase-postgres"`
- `pollerEnabled: false`
- `providerCallsEnabled: true` unless you intentionally enable the hard stop

## Rollout Order

1. Apply Supabase migrations.
2. Deploy or restart `runwy-api`.
3. Confirm `GET /health` returns `200`.
4. Deploy or restart `runwy-flight-poller`.
5. Confirm worker logs show `Flight poller worker started`.
6. Run [LIVE_TRACKING_VERIFICATION.md](/Users/sayanneogie/Documents/New%20project/runwy/LIVE_TRACKING_VERIFICATION.md).

## Failure Checks

If the API service fails at boot with auth verification errors:

- set `SUPABASE_JWT_SECRET`
- or set both `SUPABASE_URL` and `SUPABASE_ANON_KEY`

If the poller starts but no rows update:

- confirm `DATABASE_URL` points at Supabase Postgres
- confirm `tracking_sessions.next_poll_after` is due for active sessions
- confirm provider credentials are valid

If Railway keeps redeploying both services for unrelated app changes:

- verify Railway is connected to the backend-only repo and root directory is `/`

## Why Railway should not use `npm run ...`

Use direct Node entrypoints in Railway:

- API: `node src/server.js`
- Poller: `node src/flight-poller.js`

This avoids the noisy npm runtime warning:

- `npm warn config production Use '--omit=dev' instead.`

That warning is harmless, but direct Node commands keep deploy logs clean and avoid involving npm in the runtime process.

## References

- Railway monorepo guide: [Deploying a Monorepo](https://docs.railway.com/guides/monorepo)
- Railway healthchecks reference: [Healthchecks](https://docs.railway.com/reference/healthchecks)
- Railway Express guide for `/health` conventions: [Deploy Node.js & Express API with Autoscaling, Secrets, and Zero Downtime](https://docs.railway.com/guides/deploy-node-express-api-with-auto-scaling-secrets-and-zero-downtime)
