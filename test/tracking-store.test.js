const test = require("node:test");
const assert = require("node:assert/strict");

const { archivedRoutePointsForNormalized, createTrackingStore } = require("../src/tracking-store");

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
    buildArchivedRoutePolyline: options.buildArchivedRoutePolyline,
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

test("push registration retires older APNs tokens for the same device", async () => {
  const { store, queries } = makeStore();

  await store.upsertPushDevice({
    apnsToken: "new-token",
    deviceId: "device-1",
    userId: "11111111-1111-4111-8111-111111111111",
    platform: "ios",
  });

  assert.equal(queries.length, 3);
  assert.match(queries[0].sql, /update public\.device_tokens/);
  assert.match(queries[1].sql, /update public\.push_devices/);
  assert.match(queries[2].sql, /insert into public\.push_devices/);
  assert.deepEqual(queries[0].params, [
    "new-token",
    "11111111-1111-4111-8111-111111111111",
    "device-1",
  ]);
});

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

test("live snapshot projection is revisioned and rejects older envelopes atomically", async () => {
  const { store, queries } = makeStore();
  await store.persistTrackingSnapshot({
    flightId: "11111111-1111-1111-1111-111111111111",
    userId: "22222222-2222-2222-2222-222222222222",
    query: { flightNumber: "AI203", date: "2026-09-01", departureIata: "DEL", arrivalIata: "BOM" },
    normalized: makeNormalized({ stateRevision: 42, lastUpdated: "2026-09-01T12:30:00.000Z" }),
    provider: "flightaware",
    providerFlightId: "FAKE123",
    rawProviderPayload: {},
  });

  const projection = queries.find(({ sql }) => sql.includes("insert into public.live_snapshots"));
  assert.ok(projection);
  assert.match(projection.sql, /canonical_revision/);
  assert.match(projection.sql, /excluded\.canonical_revision > coalesce/);
  assert.equal(projection.params.at(-1), 42);
});

test("terminal snapshots expose actual breadcrumbs for archive persistence", () => {
  const points = [
    { latitude: 41.8, longitude: 12.2 },
    { latitude: 50.0, longitude: -30.0 },
    { latitude: 40.6, longitude: -73.8 },
  ];

  assert.deepEqual(
    archivedRoutePointsForNormalized(makeNormalized({ status: "arrived_at_gate", trackPoints: points })),
    points
  );
  assert.equal(
    archivedRoutePointsForNormalized(makeNormalized({ status: "enroute", trackPoints: points })),
    null
  );
  assert.equal(
    archivedRoutePointsForNormalized(makeNormalized({ status: "landed", trackPoints: points.slice(0, 2) })),
    null
  );
});

test("terminal archive routes can close incomplete breadcrumbs at airport endpoints", () => {
  const points = [
    { latitude: 41.8, longitude: 12.2 },
    { latitude: 50.0, longitude: -30.0 },
    { latitude: 42.48, longitude: -71.08 },
  ];
  const completed = [
    { latitude: 41.8003, longitude: 12.2389 },
    ...points,
    { latitude: 40.6413, longitude: -73.7781 },
  ];

  assert.deepEqual(
    archivedRoutePointsForNormalized(
      makeNormalized({ status: "arrived_at_gate", trackPoints: points }),
      () => completed
    ),
    completed
  );
});

test("terminal snapshots copy the longer actual route into matching archived flights", async () => {
  const completedPoints = [
    { latitude: 41.8003, longitude: 12.2389 },
    { latitude: 41.8, longitude: 12.2 },
    { latitude: 50.0, longitude: -30.0 },
    { latitude: 40.6, longitude: -73.8 },
    { latitude: 40.6413, longitude: -73.7781 },
  ];
  const { store, queries } = makeStore({
    buildArchivedRoutePolyline: () => completedPoints,
  });
  const departure = "2026-08-04T04:10:00.000Z";
  const points = [
    { latitude: 41.8, longitude: 12.2 },
    { latitude: 50.0, longitude: -30.0 },
    { latitude: 40.6, longitude: -73.8 },
  ];

  await store.persistTrackingSnapshot({
    flightId: "11111111-1111-1111-1111-111111111111",
    userId: "22222222-2222-2222-2222-222222222222",
    query: {
      flightNumber: "AI101",
      date: "2026-08-04",
      departureIata: "FCO",
      arrivalIata: "JFK",
    },
    normalized: makeNormalized({
      flightNumber: "AI101",
      departureAirportIata: "FCO",
      arrivalAirportIata: "JFK",
      departureTimes: { scheduled: departure },
      status: "arrived_at_gate",
      trackPoints: points,
    }),
    provider: "flightaware",
    providerFlightId: "AIC101-example",
    rawProviderPayload: {},
  });

  const archiveUpdate = queries.find(
    ({ sql }) =>
      sql.includes("update public.user_flights") &&
      sql.includes("final_route_capture_next_attempt_at") &&
      sql.includes("arrival_terminal")
  );
  assert.ok(archiveUpdate, "expected a terminal route archive update");
  assert.deepEqual(JSON.parse(archiveUpdate.params[1]), completedPoints);
  assert.equal(archiveUpdate.params[2], "AI101");
  assert.equal(archiveUpdate.params[8], 5);
  assert.match(archiveUpdate.sql, /lifecycle_state in \('active', 'landed', 'archived'\)/);
  assert.match(archiveUpdate.sql, /final_route_capture_status/);
  assert.match(archiveUpdate.sql, /then 'landed'/);
});

test("tracked detail snapshots prefer the newest baggage column over stale canonical JSON", async () => {
  const { store } = makeStore({
    queryHandler(sql) {
      if (!sql.includes("from public.tracking_sessions ts")) return { rows: [] };
      return {
        rows: [
          {
            id: "11111111-1111-1111-1111-111111111111",
            owner_user_id: "22222222-2222-2222-2222-222222222222",
            session_status: "active",
            provider: "flightaware",
            flight_number: "EK379",
            origin_iata: "HKT",
            destination_iata: "DXB",
            travel_date: "2026-08-04",
            baggage_claim: "6A",
            canonical_snapshot_json: {
              flightNumber: "EK379",
              baggageClaim: "5A",
              status: "enroute",
            },
          },
        ],
      };
    },
  });

  const tracked = await store.fetchTrackingRowByID(
    "11111111-1111-1111-1111-111111111111"
  );

  assert.equal(tracked.normalized.baggageBelt, "6A");
  assert.equal(tracked.normalized.baggageClaim, "6A");
});

test("far future flights sleep until the pre-departure polling window", async () => {
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
  assertApproxDuration(nextPollAfterMs - now, (15 * 24 - 12) * 60 * 60_000);
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

test("departed flights keep polling before the final arrival refresh", async () => {
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
  assertApproxDuration(nextPollAfterMs - now, 15 * 60_000);
});

test("departed flights still schedule a final refresh after estimated arrival", async () => {
  const now = Date.now();
  const departure = new Date(now - 4 * 60 * 60_000).toISOString();
  const arrival = new Date(now - 5 * 60_000).toISOString();
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

test("recently landed flights use bounded post-arrival refresh checkpoints", async () => {
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

  const nextPollAfterMs = new Date(params[9]).getTime();
  assertApproxDuration(nextPollAfterMs - now, 15 * 60_000);
  assert.equal(params[10], "active");
  assert.equal(params[11], null);
});

test("landed flights complete after the post-arrival tracking window", async () => {
  const now = Date.now();
  const departure = new Date(now - 5 * 60 * 60_000).toISOString();
  const arrival = new Date(now - 2 * 60 * 60_000).toISOString();
  const params = await persistSnapshot(
    makeNormalized({
      status: "landed",
      departureTimes: { actual: departure },
      landingTimes: { actual: arrival },
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
  assert.equal(params[11], "post_arrival_window_complete");
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

test("stale enroute due rows are paused before returning to the poller", async () => {
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
              next_poll_after: "2026-03-11T03:00:00.000Z",
              polling_stopped_reason: null,
              last_snapshot_at: "2026-03-10T14:00:00.000Z",
              updated_at: "2026-03-10T14:00:00.000Z",
              canonical_snapshot_json: {
                airlineCode: "AI",
                flightNumber: "AI203",
                departureAirportIata: "DEL",
                arrivalAirportIata: "BOM",
                status: "enroute",
                departureTimes: { actual: "2026-03-10T10:00:00.000Z" },
                arrivalTimes: { estimated: "2026-03-10T12:00:00.000Z" },
                lastUpdated: "2026-03-10T14:00:00.000Z",
              },
              provider_last_updated_at: "2026-03-10T14:00:00.000Z",
              snapshot_updated_at: "2026-03-10T14:00:00.000Z",
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
  assert.ok(pauseUpdate, "expected stale enroute rows to be paused");
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
