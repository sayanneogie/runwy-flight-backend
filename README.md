# Flight Data Proxy

Express proxy service for Runwy that keeps provider API keys server-side and exposes app-friendly endpoints for iOS.

Supported providers:
- Aviationstack
- FlightAware AeroAPI

## Why this exists
- The iOS app never receives or stores provider API keys.
- Rate limiting protects provider quota.
- In-memory caching reduces repeated provider calls.
- Response format is normalized into a provider-agnostic tracked-flight model.
- Optional Postgres persistence stores tracking sessions, live snapshots, and push devices.
- Optional APNs dispatch sends delay/cancellation pushes to subscribed devices.

## Requirements
- Node.js 18+
- At least one provider key:
  - `AVIATIONSTACK_KEY` when `FLIGHT_DATA_PROVIDER=aviationstack`
  - `FLIGHTAWARE_API_KEY` when `FLIGHT_DATA_PROVIDER=flightaware`
- Optional but recommended:
  - `DATABASE_URL` for durable tracked-flight + device storage
  - APNs key configuration for true background push alerts

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
   - Single-process fallback: `npm run start:all`
   - API defaults to `http://localhost:8787`
9. Deploy on Railway:
   - follow [RAILWAY_DEPLOYMENT.md](RAILWAY_DEPLOYMENT.md)

## Endpoints

### GET `/health`
Returns runtime configuration summary:
- active provider
- persistence mode (`memory` or `postgres`)
- whether APNs keys are configured
- whether the poller is running in this process

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

If `WEBHOOK_SHARED_SECRET` is set, send it in:
- `X-Runwy-Webhook-Secret: <secret>`

## Deployment notes
- Put this service behind HTTPS and a reverse proxy in production.
- Set `DATABASE_URL` for production so tracking/push subscriptions survive restarts.
- Use `APNS_USE_SANDBOX=false` for production APNs.
- Keep logs generic; never log secrets.
- Railway should be split into at least two services:
  - API service: `npm run start:api`
  - Poller worker: `npm run start:poller`
- If you use a custom API command instead of `npm run start:api`, keep `ENABLE_TRACKING_POLLER=false`.
