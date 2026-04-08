const test = require("node:test");
const assert = require("node:assert/strict");

const { createTrackingStore } = require("../src/tracking-store");

function makeStore(options = {}) {
  const queries = [];
  const pool = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (typeof options.queryHandler === "function") {
        return options.queryHandler(sql, params, queries);
      }
      return { rows: [] };
    },
  };

  const store = createTrackingStore({
    pool,
    memoryTrackedFlights: new Map(),
    memoryPushDevices: new Map(),
    maxMemoryTrackedFlights: 10,
    maxMemoryPushDevices: 10,
    defaultPollerBatchSize: 25,
    maxActiveTrackingSessionsPerUser: 20,
    providerName: "flightaware",
    normalizeFlightCode(input) {
      return String(input || "").trim().toUpperCase();
    },
    normalizeAirportCode(input) {
      const value = String(input || "").trim().toUpperCase();
      return value ? value.slice(0, 3) : null;
    },
    parseAirlineCode(input) {
      const match = String(input || "").trim().toUpperCase().match(/^[A-Z]+/);
      return match ? match[0] : null;
    },
    displayFlightCode(normalized) {
      return String(normalized?.flightNumber || "");
    },
    enforceMapSizeLimit() {},
  });

  return { store, queries };
}

function makeNormalized(overrides = {}) {
  const departureTimes = {
    scheduled: null,
    estimated: null,
    actual: null,
    ...(overrides.departureTimes || {}),
  };
  const arrivalTimes = {
    scheduled: null,
    estimated: null,
    actual: null,
    ...(overrides.arrivalTimes || {}),
  };

  return {
    airlineCode: "AI",
    flightNumber: "AI203",
    departureAirportIata: "DEL",
    arrivalAirportIata: "BOM",
    departureTimes,
    arrivalTimes,
    status: "scheduled",
    terminal: null,
    gate: null,
    baggageClaim: null,
    delayMinutes: null,
    alerts: null,
    metrics: null,
    provider: "flightaware",
    lastUpdated: new Date().toISOString(),
    ...overrides,
    departureTimes,
    arrivalTimes,
  };
}

function trackingSessionUpdateParams(queries) {
  const update = queries.find(({ sql }) => sql.includes("update public.tracking_sessions"));
  assert.ok(update, "expected tracking session update query");
  return update.params;
}

function assertApproxDuration(actualMs, expectedMs, toleranceMs = 15_000) {
  assert.ok(
    actualMs >= expectedMs - toleranceMs && actualMs <= expectedMs + toleranceMs,
    `expected ${actualMs}ms to be within ${toleranceMs}ms of ${expectedMs}ms`
  );
}

async function persistSnapshot(normalized, query) {
  const { store, queries } = makeStore();
  await store.persistTrackingSnapshot({
    flightId: "11111111-1111-1111-1111-111111111111",
    userId: "22222222-2222-2222-2222-222222222222",
    query,
    normalized,
    provider: "flightaware",
    providerFlightId: "FAKE123",
    rawProviderPayload: { fa_flight_id: "FAKE123" },
  });
  return trackingSessionUpdateParams(queries);
}

test("far future flights poll once per day", async () => {
  const now = Date.now();
  const departure = new Date(now + 15 * 24 * 60 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      departureTimes: { scheduled: departure },
    }),
    {
      flightNumber: "AI203",
      date: departure.slice(0, 10),
      departureIata: "DEL",
      arrivalIata: "BOM",
    }
  );

  const nextPollAfterMs = new Date(params[9]).getTime();
  assertApproxDuration(nextPollAfterMs - now, 24 * 60 * 60_000);
});

test("flights within 12 hours poll every 2 hours", async () => {
  const now = Date.now();
  const departure = new Date(now + 8 * 60 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      departureTimes: { scheduled: departure },
    }),
    {
      flightNumber: "AI203",
      date: departure.slice(0, 10),
      departureIata: "DEL",
      arrivalIata: "BOM",
    }
  );

  const nextPollAfterMs = new Date(params[9]).getTime();
  assertApproxDuration(nextPollAfterMs - now, 2 * 60 * 60_000);
});

test("flights within 2 hours poll every 15 minutes", async () => {
  const now = Date.now();
  const departure = new Date(now + 90 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      departureTimes: { scheduled: departure },
    }),
    {
      flightNumber: "AI203",
      date: departure.slice(0, 10),
      departureIata: "DEL",
      arrivalIata: "BOM",
    }
  );

  const nextPollAfterMs = new Date(params[9]).getTime();
  assertApproxDuration(nextPollAfterMs - now, 15 * 60_000);
});

test("departed flights schedule a single final arrival-based refresh", async () => {
  const now = Date.now();
  const departure = new Date(now - 30 * 60_000).toISOString();
  const arrival = new Date(now + 3 * 60 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      status: "enroute",
      departureTimes: { actual: departure },
      arrivalTimes: { estimated: arrival },
    }),
    {
      flightNumber: "AI203",
      date: arrival.slice(0, 10),
      departureIata: "DEL",
      arrivalIata: "BOM",
    }
  );

  const nextPollAfterMs = new Date(params[9]).getTime();
  const expectedRefreshMs = new Date(arrival).getTime() + 15 * 60_000;
  assertApproxDuration(nextPollAfterMs, expectedRefreshMs);
});

test("landed flights complete tracking and stop polling", async () => {
  const now = Date.now();
  const departure = new Date(now - 3 * 60 * 60_000).toISOString();
  const arrival = new Date(now - 15 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      status: "landed",
      departureTimes: { actual: departure },
      arrivalTimes: { actual: arrival },
    }),
    {
      flightNumber: "AI203",
      date: arrival.slice(0, 10),
      departureIata: "DEL",
      arrivalIata: "BOM",
    }
  );

  assert.equal(params[9], null);
  assert.equal(params[10], "completed");
});

test("expired due rows are paused before returning to the poller", async () => {
  const now = new Date().toISOString();
  const staleTravelDate = "2026-03-10";
  const { store, queries } = makeStore({
    queryHandler(sql) {
      if (sql.includes("from public.tracking_sessions ts") && sql.includes("ts.next_poll_after <=")) {
        return {
          rows: [
            {
              id: "11111111-1111-1111-1111-111111111111",
              owner_user_id: "22222222-2222-2222-2222-222222222222",
              provider: "flightaware",
              provider_flight_id: "FAKE123",
              flight_number: "AI203",
              airline_code: "AI",
              origin_iata: "DEL",
              destination_iata: "BOM",
              travel_date: staleTravelDate,
              metadata_json: {
                query: {
                  flightNumber: "AI203",
                  date: staleTravelDate,
                  departureIata: "DEL",
                  arrivalIata: "BOM",
                },
              },
              session_status: "active",
              next_poll_after: "2026-03-13T09:00:00.000Z",
              polling_stopped_reason: null,
              last_snapshot_at: now,
              updated_at: now,
              canonical_snapshot_json: {
                airlineCode: "AI",
                flightNumber: "AI203",
                departureAirportIata: "DEL",
                arrivalAirportIata: "BOM",
                status: "scheduled",
                departureTimes: { scheduled: "2026-03-10T10:00:00.000Z" },
                arrivalTimes: { scheduled: "2026-03-10T12:00:00.000Z" },
                lastUpdated: now,
              },
              provider_last_updated_at: now,
              snapshot_updated_at: now,
            },
          ],
        };
      }

      return { rows: [] };
    },
  });

  const dueRows = await store.listDueTrackingRows();
  assert.equal(dueRows.length, 0);

  const pauseUpdate = queries.find(
    ({ sql }) => sql.includes("update public.tracking_sessions") && sql.includes("expired_tracking_window")
  );
  assert.ok(pauseUpdate, "expected expired rows to be paused");
  assert.deepEqual(pauseUpdate.params[0], ["11111111-1111-1111-1111-111111111111"]);
});

test("providerFlightIdentifier prefers FlightAware ICAO ident when fa_flight_id is missing", () => {
  const { store } = makeStore();

  assert.equal(
    store.providerFlightIdentifier(
      {
        ident_iata: "6E6383",
        ident: "IGO6383",
      },
      "flightaware"
    ),
    "IGO6383"
  );
});
