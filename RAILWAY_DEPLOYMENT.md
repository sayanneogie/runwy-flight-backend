# Railway Deployment

This backend now expects two Railway services:

- `runwy-api`
- `runwy-flight-poller`

Both services deploy from the same repo path:

- root directory: `/backend/aviationstack-proxy`

## Service Layout

### API service

- start command: `npm run start:api`
- healthcheck path: `/health`
- public domain: required

### Poller service

- start command: `npm run start:poller`
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
- `WEBHOOK_SHARED_SECRET`
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

Optional shared fetch tuning:

- `STALE_FETCH_REFRESH_THRESHOLD_MS`

## Recommended Railway Settings

### Root directory

Set the root directory for both services to:

- `/backend/aviationstack-proxy`

### Watch paths

To avoid unnecessary deploys from iOS-only changes, set watch paths for both services to:

- `/backend/aviationstack-proxy/**`

### Healthcheck

Only configure the API service healthcheck:

- `/health`

Expected API health response:

- `ok: true`
- `persistence: "supabase-postgres"`
- `pollerEnabled: false`

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

- verify root directory and watch paths are both scoped to `/backend/aviationstack-proxy`

## References

- Railway monorepo guide: [Deploying a Monorepo](https://docs.railway.com/guides/monorepo)
- Railway healthchecks reference: [Healthchecks](https://docs.railway.com/reference/healthchecks)
- Railway Express guide for `/health` conventions: [Deploy Node.js & Express API with Autoscaling, Secrets, and Zero Downtime](https://docs.railway.com/guides/deploy-node-express-api-with-auto-scaling-secrets-and-zero-downtime)
