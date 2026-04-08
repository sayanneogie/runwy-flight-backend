# Flight Data Proxy

Express proxy service for Runwy that keeps provider API keys server-side and exposes app-friendly endpoints for iOS.

Supported providers:
- Aviationstack
- FlightAware AeroAPI

Optional live-tracking transport:
- FlightAware Firehose

## Why this exists
- The iOS app never receives or stores provider API keys.
- Rate limiting protects provider quota.
- In-memory caching reduces repeated provider calls.
- Response format is normalized into a provider-agnostic tracked-flight model.
- Optional Postgres persistence stores tracking sessions, live snapshots, and push devices.
- Optional APNs dispatch sends delay/cancellation pushes to subscribed devices.
- Optional FlightAware Firehose worker streams live tracked-flight updates into `live_snapshots` for the iOS app.

## Requirements
- Node.js 18+
- At least one provider key:
  - `AVIATIONSTACK_KEY` when `FLIGHT_DATA_PROVIDER=aviationstack`
  - `FLIGHTAWARE_API_KEY` when `FLIGHT_DATA_PROVIDER=flightaware`
- Optional but recommended:
  - `DATABASE_URL` for durable tracked-flight + device storage
  - APNs key configuration for true background push alerts
  - Firehose credentials if you want moving live tracked flights without a poller:
    - `FIREHOSE_USERNAME` = your FlightAware account username
    - `FIREHOSE_API_KEY` = your FlightAware Firehose API key

## Setup
1. Copy env template:
   - `cp .env.example .env`
2. Choose provider:
   - `FLIGHT_DATA_PROVIDER=flightaware` (recommended for richer ops data)
3. Set provider key(s) in `.env`.
4. Optional: configure `DATABASE_URL` (+ `DATABASE_SSL=true` on managed PG with SSL).
5. Optional: configure APNs (`APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_BUNDLE_ID`, and private key).
6. Local debug only: if you intentionally bypass auth with `ALLOW_INSECURE_NO_AUTH=true`, also set `DEBUG_USER_ID` or send `X-Debug-User-Id` on requests.
7. Install deps:
   - `npm install`
8. Run:
   - Preflight checks: `npm run doctor`
   - API only: `npm run start:api`
   - Poller worker: `npm run start:poller`
   - Firehose worker: `npm run start:firehose`
   - Single-process fallback: `npm run start:all`
   - API defaults to `http://localhost:8787`
9. Deploy on Railway:
   - follow [RAILWAY_DEPLOYMENT.md](RAILWAY_DEPLOYMENT.md)
   - use direct Node start commands on Railway instead of `npm run ...`:
     - API: `node src/server.js`
     - Poller: `node src/flight-poller.js`

## Endpoints

### GET `/health`
Returns runtime configuration summary:
- active provider
- persistence mode (`memory` or `postgres`)
- whether APNs keys are configured
- whether the poller is running in this process
- whether a Firehose worker is configured/running in this process

Note:
- when you deploy the Firehose worker as a separate Railway service, the API service can still report `firehoseEnabled: false` because `/health` only reflects the current process.

### GET `/v1/airports`
Returns the airport catalog used by the iOS app to resolve IATA codes into names, cities, and coordinates.

Response (shape):
```json
{
  "version": "ourairports-2026-03-11",
  "airports": [
    {
      "code": "MNL",
      "name": "Ninoy Aquino International Airport",
      "city": "Pasay / Manila",
      "countryCode": "PH",
      "coordinate": { "latitude": 14.5086, "longitude": 121.0198 }
    }
  ],
  "aliases": {
    "Manila": "MNL",
    "Ninoy Aquino International Airport": "MNL"
  }
}
```

Notes:
- `/v1/track` still uses AeroAPI for the initial flight lookup and session creation.
- Continuous live movement should come from the Firehose worker writing new `live_snapshots` rows, which the iOS app already consumes through Supabase realtime.

Notes:
- This endpoint is intentionally readable without bearer auth so the app can refresh airport metadata on launch.
- The checked-in catalog is generated from OurAirports via `npm run build:airports`.

### POST `/v1/track`
Request:
```json
{
  "flightNumber": "AI203",
  "date": "2026-02-22",
  "departureIata": "DEL",
  "arrivalIata": "BOM"
}
```

Notes:
- When provider calls are enabled and `WEBHOOK_SHARED_SECRET` is configured, Runwy now attempts to auto-create a FlightAware post alert for the tracked flight so webhook updates can arrive without manual alert setup.
- The webhook target is inferred from the incoming request host by default. If you need to override it behind a proxy/custom domain setup, set `WEBHOOK_PUBLIC_BASE_URL`.

Response (shape):
```json
{
  "flightId": "uuid",
  "normalized": {
    "airlineCode": "AI",
    "flightNumber": "AI203",
    "departureAirportIata": "DEL",
    "arrivalAirportIata": "BOM",
    "departureTimes": { "scheduled": "...", "estimated": "...", "actual": null },
    "arrivalTimes": { "scheduled": "...", "estimated": "...", "actual": null },
    "status": "scheduled",
    "terminal": "3",
    "gate": "22",
    "delayMinutes": 12,
    "inboundFlight": {
      "flightNumber": "AI202",
      "originAirportIata": "DEL",
      "estimatedArrival": "...",
      "status": "enroute"
    },
    "recentHistory": [
      {
        "flightNumber": "AI203",
        "departureAirportIata": "DEL",
        "arrivalAirportIata": "BOM",
        "departureTime": "...",
        "arrivalTime": "...",
        "status": "landed"
      }
    ],
    "alerts": {
      "statusChanged": false,
      "delayedNow": false,
      "cancelledNow": false,
      "previousStatus": null,
      "currentStatus": "scheduled"
    },
    "provider": "flightaware",
    "lastUpdated": "2026-02-22T00:00:00Z"
  }
}
```

### GET `/v1/flights/:flightId`
Returns refreshed `normalized` data with updated `alerts` flags (useful for delay/cancellation notification logic).

### GET `/v1/search?flightNumber=AI203&date=2026-02-22&dep=DEL`
Returns normalized candidate list.

### POST `/v1/devices/push-token`
Registers/updates a device APNs token for background alerts.

Headers:
- `X-Device-Id: <stable-device-id-from-app>`

Body:
```json
{
  "token": "<apns-device-token-hex>",
  "platform": "ios"
}
```

### POST `/v1/devices/push-token/remove`
Disables push delivery for the calling device.

Headers:
- `X-Device-Id: <stable-device-id-from-app>`

### POST `/v1/webhooks/flightaware`
Provider webhook endpoint that refreshes matched tracked flights and dispatches APNs when status transitions indicate:
- `delayedNow`
- `cancelledNow`

If `WEBHOOK_SHARED_SECRET` is set, authenticate either way:
- query string: `/v1/webhooks/flightaware?secret=<secret>`
- or header: `X-Runwy-Webhook-Secret: <secret>`

## Deployment notes
- Put this service behind HTTPS and a reverse proxy in production.
- Set `DATABASE_URL` for production so tracking/push subscriptions survive restarts.
- Use `APNS_USE_SANDBOX=false` for production APNs.
- Keep logs generic; never log secrets.
- Railway should be split into at least two services:
  - API service: `node src/server.js`
  - Firehose worker: `node src/flight-firehose.js`
- Poller is now optional and should stay off unless you intentionally want it as a fallback:
  - Poller worker: `node src/flight-poller.js`
- On Railway, prefer direct Node start commands over `npm run ...` to avoid npm runtime warnings.
- If you use a custom API command, keep `ENABLE_TRACKING_POLLER=false`.
- For a Firehose-only worker, it is fine to set `DISABLE_PROVIDER_CALLS=true` there so the worker does not make AeroAPI refresh calls.
