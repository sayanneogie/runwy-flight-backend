"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const { createFlightCache, createMemoryRedis } = require("../src/shared-flight/cache");
const { airportCodesForCity } = require("../src/airport-catalog");
const {
  createMemorySharedFlightRepository,
  createPostgresSharedFlightRepository,
} = require("../src/shared-flight/repository");
const { createSharedFlightService, preserveKnownOperationalFields, isOlderStreamEvent } = require("../src/shared-flight/service");
const { createProviderAdapter } = require("../src/shared-flight/provider-adapter");
const { notificationDedupeKey } = require("../src/shared-flight/notifications");
const {
  compareFlightState,
  deriveFlightLifecyclePhase,
  displayStatusForPhase,
  getFlightFreshnessTTL,
  mapNormalizedToDb,
  normalizeSearchParams,
  reconcileDiversionContext,
} = require("../src/shared-flight/state");

test("diversion keeps the booked route identity and records the operational airport", () => {
  const existing = {
    flight_key: "DL-2307-2026-08-30-MSP-BIS",
    airline_code: "DL",
    flight_number: "2307",
    departure_date: "2026-08-30",
    origin_airport: "MSP",
    destination_airport: "BIS",
    status: "enroute",
    actual_departure_at: "2026-08-30T23:17:00.000Z",
    normalized_data: { destination: "BIS", aircraftType: "A220" },
  };
  const requested = { airline: "DL", number: "2307", date: "2026-08-30", origin: "MSP", destination: "BIS" };
  const reconciled = reconcileDiversionContext(
    normalizedFlight({
      airlineCode: "DL",
      flightNumber: "2307",
      origin: "MSP",
      destination: "FAR",
      status: "enroute",
      actualDepartureAt: "2026-08-30T23:17:00.000Z",
    }),
    requested,
    existing
  );
  const db = mapNormalizedToDb(reconciled, { ...requested, existingRow: existing });

  assert.equal(reconciled.originalDestination, "BIS");
  assert.equal(reconciled.diversionAirport, "FAR");
  assert.equal(db.destination_airport, "BIS");
  assert.equal(db.flight_key, existing.flight_key);
  assert.equal(db.normalized_data.diversionAirport, "FAR");
});

test("canonical stream ordering rejects replayed events but accepts equal or newer events", () => {
  const current = "2026-09-01T12:30:00.000Z";
  assert.equal(isOlderStreamEvent(current, "2026-09-01T12:29:59.000Z"), true);
  assert.equal(isOlderStreamEvent(current, current), false);
  assert.equal(isOlderStreamEvent(current, "2026-09-01T12:30:01.000Z"), false);
});

test("diversion and aircraft swaps emit actionable events without ending airborne lifecycle", () => {
  const now = Date.parse("2026-08-31T00:30:00.000Z");
  const oldState = {
    status: "enroute",
    destination_airport: "BIS",
    normalized_data: { originalDestination: "BIS", aircraftType: "A220", aircraftRegistration: "N101DU" },
  };
  const newState = {
    status: "enroute",
    destination_airport: "BIS",
    normalized_data: {
      originalDestination: "BIS",
      diversionAirport: "FAR",
      isDiverted: true,
      aircraftType: "A319",
      aircraftRegistration: "N202DU",
    },
  };
  const events = compareFlightState(oldState, newState, now);

  assert.ok(events.some((event) => event.event_type === "DIVERTED" && event.new_value.diversionAirport === "FAR"));
  assert.ok(events.some((event) => event.event_type === "AIRCRAFT_CHANGED"));
  assert.ok(["airborne", "approaching"].includes(deriveFlightLifecyclePhase({
    status: "diverted",
    normalized_data: {
      takeoffTimes: { actual: "2026-08-30T23:20:00.000Z" },
      diversionAirport: "FAR",
    },
    estimated_arrival_at: "2026-08-31T01:00:00.000Z",
  }, now).phase));
});
const { buildWeatherInsight } = require("../src/shared-flight/weather");

function normalizedFlight(overrides = {}) {
  return {
    providerFlightId: "provider-sq509",
    airlineCode: "SQ",
    flightNumber: "509",
    origin: "BLR",
    destination: "SIN",
    status: "scheduled",
    scheduledDepartureAt: "2026-05-27T18:30:00.000Z",
    scheduledArrivalAt: "2026-05-28T02:00:00.000Z",
    estimatedDepartureAt: "2026-05-27T18:30:00.000Z",
    estimatedArrivalAt: "2026-05-28T02:00:00.000Z",
    actualDepartureAt: null,
    actualArrivalAt: null,
    gate: "A4",
    terminal: "2",
    baggageBelt: null,
    position: { lat: null, lon: null, altitude: null, groundSpeed: null, heading: null },
    provider: "test",
    dataConfidence: "high",
    rawProviderResponse: { ok: true },
    ...overrides,
  };
}

function makeService(providerFlight = normalizedFlight(), options = {}) {
  let calls = 0;
  const providerOptions = [];
  const repository = createMemorySharedFlightRepository();
  const queue = options.queue;
  const provider = {
    name: "test",
    alertConfigurationChangedAt: options.alertConfigurationChangedAt,
    async fetchFlightByNumber(_params, fetchOptions = {}) {
      calls += 1;
      providerOptions.push(fetchOptions);
      if (options.delayMs) await new Promise((resolve) => setTimeout(resolve, options.delayMs));
      return typeof providerFlight === "function" ? providerFlight(calls) : providerFlight;
    },
    ensureFlightAlert: options.ensureFlightAlert,
    ensureInboundFlightAlert: options.ensureInboundFlightAlert,
    ensureFlightStream: options.ensureFlightStream,
  };
  if (typeof options.fetchFlightByProviderId === "function") {
    provider.supportsProviderId = true;
    provider.fetchFlightByProviderId = options.fetchFlightByProviderId;
  }
  const service = createSharedFlightService({
    repository,
    provider,
    streamingEnabled: options.streamingEnabled === true,
    apiPollingEnabled: options.apiPollingEnabled,
    apiActivePollMs: options.apiActivePollMs,
    apiPredeparturePollMs: options.apiPredeparturePollMs,
    apiPredepartureWindowMs: options.apiPredepartureWindowMs,
    apiPostArrivalPollMs: options.apiPostArrivalPollMs,
    apiPostArrivalWindowMs: options.apiPostArrivalWindowMs,
    queue,
    cache: createFlightCache(createMemoryRedis()),
    weather: options.weather,
    wait: options.wait || ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
    apns: options.apns,
    liveActivities: options.liveActivities,
    stateProjection: options.stateProjection,
  });
  return { service, repository, providerCalls: () => calls, providerOptions };
}

test("arrival visit ordinal counts every completed travel source and deduplicates copies", async () => {
  const repository = createMemorySharedFlightRepository();
  const rows = repository.__memory.userFlights;
  const base = {
    user_id: "traveler",
    origin_iata: "BOM",
    destination_iata: "DEL",
    display_flight_number: "AI 101",
    scheduled_departure: "2026-01-10T04:00:00.000Z",
    lifecycle_state: "archived",
    deleted_at: null,
  };

  rows.set("recovered", { ...base, id: "recovered", source_type: "manual_recovery" });
  rows.set("duplicate", { ...base, id: "duplicate", source_type: "history_snapshot" });
  rows.set("older", {
    ...base,
    id: "older",
    source_type: "calendar_import",
    scheduled_departure: "2025-06-02T04:00:00.000Z",
  });
  rows.set("observer", {
    ...base,
    id: "observer",
    source_type: "tracked",
    scheduled_departure: "2024-03-01T04:00:00.000Z",
  });

  assert.deepEqual(airportCodesForCity("DEL"), ["DEL"]);
  assert.equal(await repository.arrivalVisitOrdinalForUser("traveler", "DEL"), 3);
});

test("shared refresh preserves known gates when the provider temporarily omits them", () => {
  const merged = preserveKnownOperationalFields(
    normalizedFlight({
      gate: null,
      terminal: "3",
      departureGate: null,
      departureTerminal: "3",
      arrivalGate: null,
      arrivalTerminal: "1",
    }),
    {
      gate: "A6",
      terminal: "3",
      baggage_belt: null,
      normalized_data: {
        departureGate: "A6",
        departureTerminal: "3",
        arrivalGate: "C23",
        arrivalTerminal: "1",
      },
    }
  );

  assert.equal(merged.gate, "A6");
  assert.equal(merged.departureGate, "A6");
  assert.equal(merged.arrivalGate, "C23");
  assert.equal(merged.departureTerminal, "3");
  assert.equal(merged.arrivalTerminal, "1");
});

test("shared refresh accumulates real provider positions into the flown breadcrumb trail", () => {
  const merged = preserveKnownOperationalFields(
    normalizedFlight({
      status: "enroute",
      position: {
        lat: 12.68033,
        lon: 81.78772,
        heading: 113,
        recordedAt: "2026-09-01T19:49:46.000Z",
      },
    }),
    {
      normalized_data: {
        status: "enroute",
        position: {
          lat: 14.20244,
          lon: 78.35589,
          heading: 112,
          recordedAt: "2026-09-01T19:19:44.000Z",
        },
      },
    }
  );

  assert.deepEqual(
    merged.trackPoints.map(({ latitude, longitude }) => ({ latitude, longitude })),
    [
      { latitude: 14.20244, longitude: 78.35589 },
      { latitude: 12.68033, longitude: 81.78772 },
    ]
  );
});

test("shared refresh rejects a taxiing regression after takeoff is confirmed", () => {
  const actualTakeoff = "2026-09-03T17:59:46.000Z";
  const merged = preserveKnownOperationalFields(
    normalizedFlight({
      status: "taxiing",
      actualDepartureAt: "2026-09-03T17:38:00.000Z",
      takeoffTimes: {
        scheduled: "2026-09-03T17:25:00.000Z",
        estimated: null,
        actual: null,
      },
      position: {
        lat: null,
        lon: null,
        altitude: null,
        groundSpeed: null,
        recordedAt: "2026-09-04T01:06:44.000Z",
      },
    }),
    {
      status: "enroute",
      actual_departure_at: "2026-09-03T17:38:00.000Z",
      actual_arrival_at: null,
      normalized_data: {
        status: "enroute",
        takeoffTimes: {
          scheduled: "2026-09-03T17:25:00.000Z",
          estimated: actualTakeoff,
          actual: actualTakeoff,
        },
      },
    }
  );

  assert.equal(merged.status, "enroute");
  assert.equal(merged.takeoffTimes.actual, actualTakeoff);
  assert.equal(merged.actualTakeoffAt, actualTakeoff);
});

test("airborne status aliases do not emit duplicate takeoff notifications", () => {
  const events = compareFlightState(
    {
      status: "airborne",
      normalized_data: { takeoffTimes: { actual: "2026-09-04T04:28:25.000Z" } },
    },
    {
      status: "enroute",
      normalized_data: { takeoffTimes: { actual: "2026-09-04T04:28:25.000Z" } },
      data_confidence: "high",
    }
  );

  assert.equal(events.some((event) => event.event_type === "AIRBORNE"), false);
});

test("shared refresh rejects a taxiing regression after confirmed arrival", () => {
  const actualArrival = "2026-09-04T02:16:00.000Z";
  const merged = preserveKnownOperationalFields(
    normalizedFlight({
      status: "taxiing",
      actualArrivalAt: null,
      arrivalTimes: { actual: null },
    }),
    {
      status: "landed",
      actual_arrival_at: actualArrival,
      normalized_data: {
        status: "landed",
        arrivalTimes: { actual: actualArrival },
      },
    }
  );

  assert.equal(merged.status, "landed");
  assert.equal(merged.actualArrivalAt, actualArrival);
  assert.equal(merged.arrivalTimes.actual, actualArrival);
});

test("touchdown status aliases do not emit duplicate arrival notifications", () => {
  const actualArrival = "2026-09-04T02:16:00.000Z";
  const events = compareFlightState(
    { status: "landed", actual_arrival_at: actualArrival },
    { status: "arrived", actual_arrival_at: actualArrival, data_confidence: "high" }
  );

  assert.equal(events.some((event) => ["LANDED", "ARRIVED"].includes(event.event_type)), false);
});

test("shared refresh preserves resolved inbound details when the provider returns only its ID", () => {
  const estimatedArrival = "2026-09-01T08:26:00.000Z";
  const merged = preserveKnownOperationalFields(
    normalizedFlight({
      inboundFlight: {
        providerFlightId: "IGO508-instance",
        flightNumber: null,
        originAirportIata: null,
        destinationAirportIata: "RDP",
        estimatedArrival: null,
        status: null,
      },
    }),
    {
      normalized_data: {
        inboundFlight: {
          providerFlightId: "IGO508-instance",
          flightNumber: "6E508",
          originAirportIata: "BLR",
          destinationAirportIata: "RDP",
          estimatedArrival,
          status: "enroute",
          providerAlertId: "inbound-alert",
          providerAlertStatus: "active",
        },
      },
    }
  );

  assert.equal(merged.inboundFlight.flightNumber, "6E508");
  assert.equal(merged.inboundFlight.originAirportIata, "BLR");
  assert.equal(merged.inboundFlight.destinationAirportIata, "RDP");
  assert.equal(merged.inboundFlight.estimatedArrival, estimatedArrival);
  assert.equal(merged.inboundFlight.status, "enroute");
  assert.equal(merged.inboundFlight.providerAlertId, "inbound-alert");
});

test("production provider adapter exposes shared alert registration callbacks", async () => {
  let calls = 0;
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async () => [],
    normalizeRecord: (record) => record,
    ensureFlightAlert: async (flight) => {
      calls += 1;
      return { providerAlertId: `alert-${flight.id}`, status: "active" };
    },
  });

  const result = await adapter.ensureFlightAlert({ id: "shared-flight" });
  assert.equal(calls, 1);
  assert.equal(result.providerAlertId, "alert-shared-flight");
});

test("provider adapter exposes inbound alert registration callbacks", async () => {
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async () => [],
    normalizeRecord: (record) => record,
    ensureInboundFlightAlert: async (_flight, inbound) => ({
      providerAlertId: `inbound-${inbound.providerFlightId}`,
      status: "active",
    }),
  });

  const result = await adapter.ensureInboundFlightAlert(
    { id: "parent-flight" },
    { providerFlightId: "AI202-instance" }
  );
  assert.equal(result.providerAlertId, "inbound-AI202-instance");
});

test("provider adapter forwards forced refresh options for an exact flight instance", async () => {
  let receivedOptions = null;
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async () => [],
    fetchByProviderId: async (_providerFlightId, options) => {
      receivedOptions = options;
      return {
        fa_flight_id: "EK354-instance",
        ident_iata: "EK354",
        origin: { code_iata: "DXB" },
        destination: { code_iata: "SIN" },
      };
    },
    normalizeRecord: () => normalizedFlight({ providerFlightId: "EK354-instance" }),
  });

  await adapter.fetchFlightByProviderId("EK354-instance", { forceRefresh: true });

  assert.deepEqual(receivedOptions, { forceRefresh: true });
});

test("provider adapter forwards detail-refresh options to enrichment", async () => {
  let enrichmentOptions = null;
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async () => [],
    fetchByProviderId: async () => ({ fa_flight_id: "SQ509-instance" }),
    normalizeRecord: () => normalizedFlight({ providerFlightId: "SQ509-instance" }),
    enrichNormalized: async (normalized, _record, _query, _params, options) => {
      enrichmentOptions = options;
      return normalized;
    },
  });

  await adapter.fetchFlightByProviderId("SQ509-instance", { skipLivePosition: true });

  assert.deepEqual(enrichmentOptions, { skipLivePosition: true });
});

test("provider adapter preserves separate gate-out and wheels-off timestamps", async () => {
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async () => [{ fa_flight_id: "AIC2418-instance" }],
    normalizeRecord: () => normalizedFlight({
      departureTimes: {
        scheduled: "2026-08-28T11:00:00.000Z",
        estimated: "2026-08-28T11:00:00.000Z",
        actual: "2026-08-28T10:53:00.000Z",
      },
      takeoffTimes: {
        scheduled: "2026-08-28T11:00:00.000Z",
        estimated: "2026-08-28T11:03:00.000Z",
        actual: "2026-08-28T11:07:35.000Z",
      },
    }),
    selectRecord: (records) => records[0],
  });

  const result = await adapter.fetchFlightByNumber({
    airline: "AI", number: "2418", date: "2026-08-28", origin: "BLR", destination: "DEL",
  });

  assert.equal(result.departureTimes.actual, "2026-08-28T10:53:00.000Z");
  assert.equal(result.takeoffTimes.actual, "2026-08-28T11:07:35.000Z");
});

test("shared flight search preserves the origin timezone offset for the provider query", async () => {
  let providerQuery = null;
  const adapter = createProviderAdapter({
    providerName: "flightaware",
    fetchFlights: async (query) => {
      providerQuery = query;
      return [{ fa_flight_id: "AKJ1516-instance" }];
    },
    normalizeRecord: () => normalizedFlight({
      airlineCode: "QP",
      flightNumber: "QP1516",
      origin: "BOM",
      destination: "BLR",
    }),
    selectRecord: (records) => records[0],
  });
  const params = normalizeSearchParams({
    airline: "QP",
    number: "1516",
    date: "2026-09-03",
    origin: "BOM",
    destination: "BLR",
    timezoneOffsetMinutes: 330,
  });

  const result = await adapter.fetchFlightByNumber(params);

  assert.equal(providerQuery.timezoneOffsetMinutes, 330);
  assert.equal(result.timezoneOffsetMinutes, 330);
});

test("shared flight search repairs an adjacent UTC-day occurrence cached under the local date", async () => {
  const { service, providerCalls } = makeService((call) => normalizedFlight({
    providerFlightId: call === 1 ? "QP1516-next-day" : "QP1516-current",
    airlineCode: "QP",
    flightNumber: "1516",
    origin: "BOM",
    destination: "BLR",
    scheduledDepartureAt: call === 1
      ? "2026-09-03T19:10:00.000Z"
      : "2026-09-02T19:10:00.000Z",
    scheduledArrivalAt: call === 1
      ? "2026-09-03T21:05:00.000Z"
      : "2026-09-02T21:05:00.000Z",
  }));
  const input = {
    airline: "QP",
    number: "1516",
    date: "2026-09-03",
    origin: "BOM",
    destination: "BLR",
  };

  await service.searchFlight(input);
  const repaired = await service.searchFlight({ ...input, timezoneOffsetMinutes: 330 });

  assert.equal(providerCalls(), 2);
  assert.equal(repaired.providerFlightId, "QP1516-current");
  assert.equal(repaired.scheduledDepartureAt, "2026-09-02T19:10:00.000Z");
});

test("1000 users searching the same missing flight cause only one provider call", async () => {
  const { service, providerCalls } = makeService(normalizedFlight(), { delayMs: 300 });
  const requests = Array.from({ length: 1000 }, () =>
    service.searchFlight({ airline: "sq", number: "509", date: "2026-05-27", origin: "blr", destination: "sin" })
  );
  const responses = await Promise.all(requests);
  assert.equal(providerCalls(), 1);
  assert.ok(responses.every((response) => response.flightKey === "SQ-509-2026-05-27-BLR-SIN" || response.status === "pending"));
});

test("1000 users searching the same fresh flight cause zero new provider calls", async () => {
  const { service, providerCalls } = makeService();
  await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const before = providerCalls();
  const responses = await Promise.all(
    Array.from({ length: 1000 }, () =>
      service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" })
    )
  );
  assert.equal(providerCalls(), before);
  assert.ok(responses.every((response) => response.source === "redis"));
});

test("separate backend replicas share a provider lease", async () => {
  const repository = createMemorySharedFlightRepository();
  let providerCalls = 0;
  const provider = {
    name: "test",
    async fetchFlightByNumber() {
      providerCalls += 1;
      await new Promise((resolve) => setTimeout(resolve, 300));
      return normalizedFlight();
    },
  };
  const createReplica = () => createSharedFlightService({
    repository,
    provider,
    cache: createFlightCache(createMemoryRedis()),
    wait: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  });
  const firstReplica = createReplica();
  const secondReplica = createReplica();
  const input = { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" };

  const responses = await Promise.all([
    firstReplica.searchFlight(input),
    secondReplica.searchFlight(input),
  ]);

  assert.equal(providerCalls, 1);
  assert.ok(responses.some((response) => response.flightInstanceId));
  assert.ok(responses.some((response) => response.status === "pending"));
});

test("provider leases can only be released by their owner", async () => {
  const repository = createMemorySharedFlightRepository();
  const token = await repository.acquireProviderRequestLease("provider:flight", 1_000);

  assert.ok(token);
  assert.equal(await repository.releaseProviderRequestLease("provider:flight", "not-the-owner"), false);
  assert.equal(await repository.acquireProviderRequestLease("provider:flight", 1_000), null);
  assert.equal(await repository.releaseProviderRequestLease("provider:flight", token), true);
  assert.ok(await repository.acquireProviderRequestLease("provider:flight", 1_000));
});

test("stale flight data is returned immediately and only one refresh is queued", async () => {
  const { service, repository } = makeService();
  const fresh = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightByKeyOrAlias(fresh.flightKey);
  row.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(row);
  await service.cache.redis.del(`flight:${fresh.flightKey}`);

  const responses = await Promise.all(
    Array.from({ length: 10 }, () =>
      service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" })
    )
  );

  assert.ok(responses.every((response) => response.freshness === "stale" && response.isRefreshing === true));
  assert.equal(service.queue.jobs.filter((job) => job.name === "refreshFlightJob").length, 1);
});

test("delayed flight creates an event, updates shared row, and queues fanout", async () => {
  const { service, repository } = makeService(() => normalizedFlight({ estimatedDepartureAt: "2026-05-27T19:05:00.000Z" }));
  const initial = await repository.upsertFlightFromNormalized(normalizedFlight(), { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN", flightKey: "SQ-509-2026-05-27-BLR-SIN" }, "2026-05-01T00:00:00.000Z");
  await service.refreshFlightJob({ data: { flight_key: initial.flight_key, flight_instance_id: initial.id, reason: "forced" } });
  const updated = await repository.findFlightByKeyOrAlias(initial.flight_key);
  assert.equal(updated.estimated_departure_at, "2026-05-27T19:05:00.000Z");
  assert.ok([...repository.__memory.events.values()].some((event) => event.event_type === "DELAYED"));
  assert.ok(service.queue.jobs.some((job) => job.name === "fanoutNotificationJob"));
});

test("cancelled flight creates critical event and no duplicate notification deliveries", async () => {
  const sent = [];
  const { service, repository } = makeService(() => normalizedFlight({ status: "cancelled" }), {
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN", flightKey: "SQ-509-2026-05-27-BLR-SIN" }, "2026-05-01T00:00:00.000Z");
  for (const userId of ["u1", "u2"]) {
    await repository.upsertUserFlight(userId, row.id, { alertPreferences: { low: true, medium: true, high: true, critical: true } });
    await repository.upsertDeviceToken(userId, { deviceToken: `token-${userId}`, environment: "sandbox" });
  }
  await service.refreshFlightJob({ data: { flight_key: row.flight_key, flight_instance_id: row.id, reason: "forced" } });
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "CANCELLED");
  assert.equal(event.event_severity, "critical");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal(repository.__memory.deliveries.size, 2);
  assert.deepEqual(sent.sort(), ["token-u1", "token-u2"]);
});

test("semantic notification keys merge provider aliases but retain real value changes", () => {
  const flight = { id: "flight-occurrence-1" };
  assert.equal(
    notificationDedupeKey(flight, {
      id: "event-1",
      event_type: "AIRBORNE",
      new_value: { status: "airborne" },
    }),
    notificationDedupeKey(flight, {
      id: "event-2",
      event_type: "DEPARTED",
      new_value: { status: "enroute" },
    })
  );

  const gateB7 = notificationDedupeKey(flight, {
    event_type: "GATE_CHANGED",
    old_value: { gate: "A4" },
    new_value: { gate: "B7" },
  });
  assert.equal(gateB7, notificationDedupeKey(flight, {
    event_type: "GATE_CHANGED",
    old_value: { gate: "A5" },
    new_value: { gate: "B7" },
  }));
  assert.notEqual(gateB7, notificationDedupeKey(flight, {
    event_type: "GATE_CHANGED",
    old_value: { gate: "B7" },
    new_value: { gate: "C2" },
  }));
});

test("separate takeoff event rows produce one APNs fanout per user", async () => {
  const sent = [];
  const { service, repository } = makeService(normalizedFlight(), {
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
    flightKey: "SQ-509-2026-05-27-BLR-SIN",
  }, "2026-05-27T10:00:00.000Z");
  await repository.upsertUserFlight("u1", row.id, {
    alertPreferences: { low: true, medium: true, high: true, critical: true },
  });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const events = await repository.insertEvents(row.id, [
    {
      event_type: "AIRBORNE",
      event_severity: "medium",
      old_value: { status: "taxiing" },
      new_value: { status: "airborne" },
      summary: "Flight is airborne",
      notification_required: true,
      confidence: "high",
    },
    {
      event_type: "DEPARTED",
      event_severity: "medium",
      old_value: { status: "airborne" },
      new_value: { status: "enroute" },
      summary: "Flight departed",
      notification_required: true,
      confidence: "high",
    },
  ], "test");

  for (const event of events) {
    await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  }

  assert.deepEqual(sent, ["token-u1"]);
  assert.equal(repository.__memory.deliveries.size, 1);
  assert.equal(repository.__memory.appNotifications.size, 1);
  assert.equal([...repository.__memory.appNotifications.values()][0].delivery_status, "sent");
});

test("durable APNs outbox retries a definite rejection without resending accepted events", async () => {
  let calls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    apns: {
      sendFlightEvent: async () => {
        calls += 1;
        return calls === 1
          ? { ok: false, status: 503, reason: "ServiceUnavailable" }
          : { ok: true, apnsId: "accepted-on-retry" };
      },
    },
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN",
    flightKey: "SQ-509-2026-05-27-BLR-SIN",
  }, "2026-05-27T10:00:00.000Z");
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { medium: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });
  const [event] = await repository.insertEvents(row.id, [{
    event_type: "AIRBORNE",
    event_severity: "medium",
    old_value: { status: "taxiing" },
    new_value: { status: "airborne" },
    notification_required: true,
  }], "test");

  assert.equal((await service.fanoutNotificationJob({ data: { flight_event_id: event.id } })).sent, 0);
  const tokenDelivery = [...repository.__memory.deliveryTokens.values()][0];
  assert.equal(tokenDelivery.status, "retry");
  tokenDelivery.next_attempt_at = new Date(Date.now() - 1_000).toISOString();

  assert.equal(await service.recoverDurableApnsOutbox(), 1);
  assert.equal(calls, 2);
  assert.equal([...repository.__memory.deliveryTokens.values()][0].status, "accepted");
  assert.equal([...repository.__memory.deliveries.values()][0].status, "sent");
  assert.equal([...repository.__memory.appNotifications.values()][0].delivery_status, "sent");

  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal(calls, 2);
});

test("ambiguous APNs transport failures are not retried into duplicate alerts", async () => {
  let calls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    apns: {
      sendFlightEvent: async () => {
        calls += 1;
        throw new Error("APNs request timed out");
      },
    },
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN",
    flightKey: "SQ-509-2026-05-27-BLR-SIN",
  }, "2026-05-27T10:00:00.000Z");
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { medium: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });
  const [event] = await repository.insertEvents(row.id, [{
    event_type: "LANDED",
    event_severity: "medium",
    old_value: { status: "airborne" },
    new_value: { status: "landed" },
    notification_required: true,
  }], "test");

  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal([...repository.__memory.deliveryTokens.values()][0].status, "uncertain");
  assert.equal(await service.recoverDurableApnsOutbox(), 0);
  assert.equal(calls, 1);
});

test("gate change only emits on real change and respects alert preferences", async () => {
  const noChange = compareFlightState(
    { status: "scheduled", gate: "A4", scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "scheduled", gate: "A4", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-27T08:00:00.000Z")
  );
  assert.equal(noChange.some((event) => event.event_type === "GATE_CHANGED"), false);

  const changed = compareFlightState(
    { status: "scheduled", gate: "A4", scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "scheduled", gate: "B7", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-27T08:00:00.000Z")
  );
  assert.equal(changed.find((event) => event.event_type === "GATE_CHANGED").notification_required, true);

  const firstAssignment = compareFlightState(
    { status: "scheduled", gate: null, scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "scheduled", gate: "B7", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-27T08:00:00.000Z")
  );
  assert.equal(firstAssignment.find((event) => event.event_type === "GATE_CHANGED").new_value.gate, "B7");

  const { service, repository } = makeService();
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" }, "2026-05-27T00:00:00.000Z");
  const event = (await repository.insertEvents(row.id, changed, "test"))[0];
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: false, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal(repository.__memory.deliveries.size, 0);
});

test("newly saved user flights receive low severity travel notifications by default", async () => {
  const sent = [];
  const { service, repository } = makeService(normalizedFlight(), {
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });

  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const [event] = await repository.insertEvents(saved.flight.flightInstanceId, [
    {
      event_type: "BAGGAGE_BELT_ASSIGNED",
      event_severity: "low",
      old_value: { baggageBelt: null },
      new_value: { baggageBelt: "7" },
      summary: "Baggage belt assigned: 7",
      notification_required: true,
      confidence: "high",
    },
  ], "test");

  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });

  assert.deepEqual(sent, ["token-u1"]);
  assert.equal(repository.__memory.deliveries.size, 1);
});

test("taxi, takeoff, and baggage belt shared events are meaningful and notify", () => {
  const taxi = compareFlightState(
    { status: "boarding", scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "taxiing", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-27T18:00:00.000Z")
  );
  assert.equal(taxi.find((event) => event.event_type === "TAXIING")?.notification_required, true);

  const takeoff = compareFlightState(
    { status: "taxiing", scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "takeoff_roll", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-27T18:29:00.000Z")
  );
  assert.equal(takeoff.find((event) => event.event_type === "TAKEOFF_ROLL")?.event_severity, "high");

  const baggage = compareFlightState(
    { status: "landed", baggage_belt: null, scheduled_departure_at: "2026-05-27T18:30:00.000Z" },
    { status: "landed", baggage_belt: "7", scheduled_departure_at: "2026-05-27T18:30:00.000Z", data_confidence: "high" },
    Date.parse("2026-05-28T02:10:00.000Z")
  );
  assert.equal(baggage.find((event) => event.event_type === "BAGGAGE_BELT_ASSIGNED")?.notification_required, true);

  const earlyBaggage = compareFlightState(
    {
      status: "enroute",
      baggage_belt: null,
      estimated_arrival_at: "2026-05-28T04:30:00.000Z",
    },
    {
      status: "enroute",
      baggage_belt: "11",
      estimated_arrival_at: "2026-05-28T04:30:00.000Z",
      data_confidence: "high",
    },
    Date.parse("2026-05-28T02:00:00.000Z")
  );
  assert.equal(
    earlyBaggage.find((event) => event.event_type === "BAGGAGE_BELT_ASSIGNED")?.notification_required,
    false
  );
});

test("positive takeoff evidence emits AIRBORNE when stale status was already enroute", () => {
  const oldState = {
    status: "enroute",
    altitude: 0,
    ground_speed: 1,
    normalized_data: {
      takeoffTimes: { actual: null },
      livePosition: { altitudeFeet: 0, groundSpeedKnots: 1 },
    },
  };
  const newState = {
    ...oldState,
    altitude: 12_000,
    ground_speed: 245,
    normalized_data: {
      takeoffTimes: { actual: "2026-08-29T17:52:00.000Z" },
      livePosition: { altitudeFeet: 12_000, groundSpeedKnots: 245 },
    },
  };

  const events = compareFlightState(oldState, newState);
  const airborne = events.filter((event) => event.event_type === "AIRBORNE");
  assert.equal(airborne.length, 1);
  assert.equal(airborne[0].notification_required, true);
});

test("shared fanout mirrors takeoff landing and baggage events into app notifications", async () => {
  const sent = [];
  const { service, repository } = makeService(normalizedFlight(), {
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
    flightKey: "SQ-509-2026-05-27-BLR-SIN",
  }, "2026-05-27T10:00:00.000Z");

  await repository.upsertUserFlight("u1", row.id, {
    alertPreferences: { low: true, medium: true, high: true, critical: true },
  });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const events = await repository.insertEvents(row.id, [
    {
      event_type: "AIRBORNE",
      event_severity: "medium",
      old_value: { status: "taxiing" },
      new_value: { status: "airborne" },
      summary: "Flight is airborne",
      notification_required: true,
      confidence: "high",
    },
    {
      event_type: "LANDED",
      event_severity: "medium",
      old_value: { status: "airborne" },
      new_value: { status: "landed" },
      summary: "Flight has landed",
      notification_required: true,
      confidence: "high",
    },
    {
      event_type: "BAGGAGE_BELT_ASSIGNED",
      event_severity: "low",
      old_value: { baggageBelt: null },
      new_value: { baggageBelt: "7" },
      summary: "Baggage belt assigned: 7",
      notification_required: true,
      confidence: "high",
    },
  ], "test");

  for (const event of events) {
    await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  }

  const notifications = [...repository.__memory.appNotifications.values()];
  assert.deepEqual(
    notifications.map((notification) => notification.notification_type).sort(),
    ["flight_arrived", "flight_baggage_claim", "flight_departed"]
  );
  assert.ok(notifications.every((notification) => notification.delivery_status === "sent"));
  assert.deepEqual(sent.sort(), ["token-u1", "token-u1", "token-u1"]);
});

test("deleting a saved flight removes owner and circle notification artifacts", async () => {
  const { service, repository } = makeService();
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  const userFlight = saved.userFlight;
  const flightInstanceId = saved.flight.flightInstanceId;
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const [event] = await repository.insertEvents(flightInstanceId, [{
    event_type: "GATE_CHANGED",
    event_severity: "high",
    old_value: { gate: "A4" },
    new_value: { gate: "B7" },
    summary: "Gate changed to B7",
    notification_required: true,
    confidence: "high",
  }], "test");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });

  await repository.createAppNotification({
    userId: "u2",
    flightInstanceId,
    flightEventId: "circle-event",
    notificationType: "flight_gate_changed",
    title: "Circle flight gate changed",
    body: "Gate B7",
    payload: { user_flight_id: userFlight.id, owner_user_id: "u1" },
  });
  await repository.createNotificationDelivery(
    "u2",
    flightInstanceId,
    "circle-event",
    "apns",
    userFlight.id
  );

  assert.equal(repository.__memory.appNotifications.size, 2);
  assert.equal(repository.__memory.deliveries.size, 2);

  const deleted = await service.deleteUserFlight("u1", userFlight.id);

  assert.ok(deleted.deleted_at);
  assert.equal(deleted.notification_enabled, false);
  assert.equal(repository.__memory.appNotifications.size, 0);
  assert.equal(repository.__memory.deliveries.size, 0);

  const [postDeleteEvent] = await repository.insertEvents(flightInstanceId, [{
    event_type: "BAGGAGE_BELT_CHANGED",
    event_severity: "high",
    old_value: { baggageBelt: null },
    new_value: { baggageBelt: "7" },
    summary: "Baggage belt assigned: 7",
    notification_required: true,
    confidence: "high",
  }], "test");
  const postDeleteFanout = await service.fanoutNotificationJob({
    data: { flight_event_id: postDeleteEvent.id },
  });

  assert.equal(postDeleteFanout.sent, 0);
  assert.equal(repository.__memory.appNotifications.size, 0);
  assert.equal(repository.__memory.deliveries.size, 0);
});

test("notification fanout rejects an active canonical row when a normalized deleted twin exists", async () => {
  const { service, repository } = makeService();
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  const active = saved.userFlight;
  const deletedAt = new Date(Date.parse(active.created_at) + 1_000).toISOString();

  repository.__memory.userFlights.set("deleted-local-copy", {
    ...active,
    id: "deleted-local-copy",
    flight_instance_id: null,
    display_flight_number: "SQ509",
    lifecycle_state: "deleted",
    notification_enabled: false,
    notifications_enabled: false,
    deleted_at: deletedAt,
    updated_at: deletedAt,
  });

  const targets = await repository.listNotificationTargets(
    saved.flight.flightInstanceId,
    "low",
    "TRIP_STARTING"
  );

  assert.equal(active.display_flight_number, "SQ 509");
  assert.equal(targets.length, 0);
});

test("saving a previously deleted shared flight reactivates its notification row", async () => {
  const { service, repository } = makeService();
  const input = {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  };
  const first = await service.saveUserFlight("u1", input);
  await service.deleteUserFlight("u1", first.userFlight.id);

  const restored = await service.saveUserFlight("u1", input);

  assert.equal(restored.userFlight.id, first.userFlight.id);
  assert.equal(restored.userFlight.deleted_at, undefined);
  assert.equal((await repository.listUserFlights("u1")).length, 1);
});

test("displayed-flight reconciliation removes stale tracking ownership and keeps only matching occurrences", async () => {
  const { service, repository } = makeService();
  const rows = repository.__memory.userFlights;
  const base = {
    user_id: "u1",
    display_flight_number: "AI 101",
    origin_iata: "DEL",
    destination_iata: "FCO",
    lifecycle_state: "active",
    notification_enabled: true,
    notifications_enabled: true,
    deleted_at: null,
  };
  rows.set("current", {
    ...base,
    id: "current-server-row",
    flight_instance_id: "current-flight-instance",
    tracking_session_id: "current-tracking-session",
    scheduled_departure: "2026-09-03T17:15:00.000Z",
  });
  rows.set("wrong-day", {
    ...base,
    id: "wrong-day-server-row",
    flight_instance_id: "wrong-day-flight-instance",
    tracking_session_id: "wrong-day-tracking-session",
    scheduled_departure: "2026-09-04T17:15:00.000Z",
  });
  rows.set("history", {
    ...base,
    id: "history-row",
    lifecycle_state: "archived",
    flight_instance_id: "history-flight-instance",
    scheduled_departure: "2025-09-03T17:15:00.000Z",
  });
  rows.set("other-user", {
    ...base,
    id: "other-user-row",
    user_id: "u2",
    flight_instance_id: "wrong-day-flight-instance",
    scheduled_departure: "2026-09-04T17:15:00.000Z",
  });

  const result = await service.reconcileDisplayedUserFlights("u1", {
    flights: [{
      id: "different-local-id",
      flightNumber: "AI101",
      origin: "DEL",
      destination: "FCO",
      scheduledDeparture: "2026-09-03T17:15:00.000Z",
    }],
  });

  assert.equal(result.displayed, 1);
  assert.equal(result.checked, 2);
  assert.equal(result.kept, 1);
  assert.equal(result.removed, 1);
  assert.deepEqual(result.removedUserFlightIds, ["wrong-day-server-row"]);
  assert.deepEqual(result.stoppedTrackingSessionIds, ["wrong-day-tracking-session"]);
  assert.deepEqual(result.orphanedFlightInstanceIds, []);
  assert.equal(rows.get("current").deleted_at, null);
  assert.ok(rows.get("wrong-day").deleted_at);
  assert.equal(rows.get("history").deleted_at, null);
  assert.equal(rows.get("other-user").deleted_at, null);
});

test("an empty displayed-flight manifest removes every upcoming or active subscription", async () => {
  const { service, repository } = makeService();
  repository.__memory.userFlights.set("stale", {
    id: "stale-row",
    user_id: "u1",
    flight_instance_id: "orphaned-flight-instance",
    tracking_session_id: "stale-tracking-session",
    display_flight_number: "DL2307",
    origin_iata: "MSP",
    destination_iata: "FAR",
    scheduled_departure: "2026-08-30T23:00:00.000Z",
    lifecycle_state: "active",
    notification_enabled: true,
    notifications_enabled: true,
    deleted_at: null,
  });

  const result = await service.reconcileDisplayedUserFlights("u1", { flights: [] });

  assert.equal(result.removed, 1);
  assert.deepEqual(result.orphanedFlightInstanceIds, ["orphaned-flight-instance"]);
  assert.ok(repository.__memory.userFlights.get("stale").deleted_at);
});

test("displayed-flight reconciliation consolidates duplicate rows for one occurrence", async () => {
  const { service, repository } = makeService();
  const base = {
    user_id: "u1",
    display_flight_number: "AI101",
    origin_iata: "FCO",
    destination_iata: "JFK",
    scheduled_departure: "2026-09-04T04:10:00.000Z",
    lifecycle_state: "active",
    notification_enabled: true,
    deleted_at: null,
  };
  repository.__memory.userFlights.set("client", { ...base, id: "client-row", flight_instance_id: "shared-flight" });
  repository.__memory.userFlights.set("mirror", { ...base, id: "tracked-mirror", flight_instance_id: "shared-flight", source_type: "tracked" });

  const result = await service.reconcileDisplayedUserFlights("u1", {
    flights: [{
      id: "client-row",
      flightNumber: "AI101",
      origin: "FCO",
      destination: "JFK",
      scheduledDeparture: "2026-09-04T04:10:00.000Z",
    }],
  });

  assert.equal(result.kept, 1);
  assert.equal(result.removed, 1);
  assert.equal(result.duplicatesConsolidated, 1);
  assert.equal(repository.__memory.userFlights.get("client").deleted_at, null);
  assert.ok(repository.__memory.userFlights.get("mirror").deleted_at);
});

test("displayed-flight reconciliation rejects an incomplete manifest before deleting anything", async () => {
  const { service, repository } = makeService();
  repository.__memory.userFlights.set("active", {
    id: "active-row",
    user_id: "u1",
    lifecycle_state: "active",
    deleted_at: null,
  });

  await assert.rejects(
    service.reconcileDisplayedUserFlights("u1", { flights: [{ flightNumber: "AI101" }] }),
    (error) => error.statusCode === 400
  );
  assert.equal(repository.__memory.userFlights.get("active").deleted_at, null);
});

test("Postgres shared-flight upsert clears tombstones and notification lookup follows tracking bridges", async () => {
  const statements = [];
  const pool = {
    async query(sql) {
      statements.push(String(sql));
      return { rows: [], rowCount: 0 };
    },
  };
  const repository = createPostgresSharedFlightRepository(pool);

  await repository.upsertUserFlight("user-1", "flight-1", {});
  await repository.hasActiveUserFlights("flight-1");
  await repository.listNotificationTargets("flight-1", "high", "LANDED");

  assert.match(statements[0], /on conflict[\s\S]*deleted_at = null/i);
  assert.match(statements[1], /sharedFlightInstanceId/);
  assert.match(statements[2], /sharedFlightInstanceId/);
});

test("stream update targets can be found by provider id or canonical flight number", async () => {
  const repository = createMemorySharedFlightRepository();
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
    flightKey: "SQ-509-2026-05-27-BLR-SIN",
  }, "2026-05-27T10:00:00.000Z");

  assert.equal((await repository.listStreamUpdateTargets({ providerFlightId: row.provider_flight_id })).length, 1);
  assert.equal((await repository.listStreamUpdateTargets({ flightNumber: "SQ509", departureDate: "2026-05-27" })).length, 1);
  assert.equal((await repository.listStreamUpdateTargets({ flightNumber: "SQ509", departureDate: "2026-05-28" })).length, 0);
});

test("suspicious provider data does not overwrite trusted state and queues revalidation", async () => {
  const { service, repository } = makeService(() => normalizedFlight({ airlineCode: "AI" }));
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN", flightKey: "SQ-509-2026-05-27-BLR-SIN" }, "2026-05-01T00:00:00.000Z");
  await service.refreshFlightJob({ data: { flight_key: row.flight_key, flight_instance_id: row.id, reason: "forced" } });
  const after = await repository.findFlightByKeyOrAlias(row.flight_key);
  assert.equal(after.airline_code, "SQ");
  assert.equal(after.data_confidence, "suspicious");
  assert.ok([...repository.__memory.events.values()].some((event) => event.event_type === "PROVIDER_DATA_SUSPICIOUS"));
  assert.ok(service.queue.jobs.some((job) => job.name === "revalidateSuspiciousFlightJob"));
});

test("RLS migration protects user-specific rows and shared flight mutation", () => {
  const sql = fs.readFileSync(path.join(__dirname, "../supabase/migrations/20260509_create_shared_flight_state.sql"), "utf8");
  assert.match(sql, /alter table public\.user_flights enable row level security/i);
  assert.match(sql, /auth\.uid\(\) = user_id/i);
  assert.match(sql, /revoke insert, update, delete on public\.flight_instances from anon, authenticated/i);
});

test("provider refresh cleanup migration pauses bridged sessions and finalizes arrivals", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260828_stop_duplicate_provider_refreshes.sql"),
    "utf8"
  );

  assert.match(sql, /providerRefreshOwner/);
  assert.match(sql, /shared_flight_instance/);
  assert.match(sql, /next_poll_after = null/);
  assert.match(sql, /actual_arrival_at is not null/);
  assert.match(sql, /is_final = actual_arrival_at is not null or is_final/);
});

test("provider request lease migration protects distributed call locks", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260828_add_provider_request_leases.sql"),
    "utf8"
  );

  assert.match(sql, /create table if not exists public\.provider_request_leases/i);
  assert.match(sql, /lock_key text primary key/i);
  assert.match(sql, /expires_at timestamptz not null/i);
  assert.match(sql, /revoke all .* anon, authenticated/i);
});

test("APNs semantic dedupe migration enforces one delivery claim per user and event meaning", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260904_add_apns_semantic_dedupe.sql"),
    "utf8"
  );

  assert.match(sql, /add column if not exists dedupe_key text/i);
  assert.match(sql, /create unique index if not exists notification_deliveries_user_dedupe_key_channel_uidx/i);
  assert.match(sql, /\(user_id, dedupe_key, channel\)/i);
  assert.match(sql, /where dedupe_key is not null/i);
});

test("durable APNs migration persists per-device work and safe recovery states", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260904_add_durable_apns_outbox.sql"),
    "utf8"
  );

  assert.match(sql, /create table if not exists public\.notification_delivery_tokens/i);
  assert.match(sql, /unique \(notification_delivery_id, device_token_id\)/i);
  assert.match(sql, /'queued', 'sending', 'retry', 'accepted', 'permanent_failed', 'uncertain'/i);
  assert.match(sql, /create or replace function public\.cleanup_deleted_user_flight_notifications/i);
});

test("past-flight occurrence migration separates trips and enforces one history row", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260904_deduplicate_past_flight_occurrences.sql"),
    "utf8"
  );

  assert.match(sql, /'trip'/i);
  assert.match(sql, /set source_type = 'trip'/i);
  assert.match(sql, /row_number\(\) over/i);
  assert.match(sql, /create or replace function public\.reconcile_user_flight_history_occurrence/i);
  assert.match(sql, /create unique index if not exists user_flights_history_occurrence_unique/i);
  assert.match(sql, /date_trunc\('minute', scheduled_departure at time zone 'UTC'\)/i);
});

test("deleted-flight cleanup removes queued notifications and pauses orphaned tracking", () => {
  const sql = fs.readFileSync(
    path.join(__dirname, "../supabase/migrations/20260828_cleanup_deleted_flight_notifications.sql"),
    "utf8"
  );
  const serverSource = fs.readFileSync(path.join(__dirname, "../src/server.js"), "utf8");

  assert.match(sql, /create trigger cleanup_deleted_user_flight_notifications/i);
  assert.match(sql, /delete from public\.notification_deliveries/i);
  assert.match(sql, /delete from public\.notifications/i);
  assert.match(sql, /polling_stopped_reason = 'user_flight_deleted'/i);
  assert.match(serverSource, /join public\.user_flights uf[\s\S]*uf\.deleted_at is null[\s\S]*lifecycle_state[\s\S]*<> 'deleted'/i);
  assert.match(serverSource, /hasActiveNotificationSubscription\(flightId\)/);
});

test("Redis locks expire safely and release checks token ownership", async () => {
  const cache = createFlightCache(createMemoryRedis());
  const token = await cache.acquireLock("fetch_lock:test", 25);
  assert.ok(token);
  assert.equal(await cache.releaseLock("fetch_lock:test", "wrong-token"), false);
  assert.equal(await cache.releaseLock("fetch_lock:test", token), true);
  const expiring = await cache.acquireLock("fetch_lock:test", 10);
  assert.ok(expiring);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.ok(await cache.acquireLock("fetch_lock:test", 10));
});

test("final states receive long freshness TTLs and are not refreshed aggressively", () => {
  const ttl = getFlightFreshnessTTL({ status: "landed", is_final: true }, Date.parse("2026-05-09T00:00:00.000Z"), () => 0);
  assert.equal(ttl, 12 * 60 * 60);
});

test("shared lifecycle phase never infers airborne from the schedule", () => {
  const now = Date.parse("2026-05-10T11:32:00.000Z");
  const lifecycle = deriveFlightLifecyclePhase(
    {
      status: "scheduled",
      scheduled_departure_at: "2026-05-10T08:55:00.000Z",
      estimated_departure_at: "2026-05-10T08:55:00.000Z",
      scheduled_arrival_at: "2026-05-10T11:35:00.000Z",
      estimated_arrival_at: "2026-05-10T11:35:00.000Z",
    },
    now
  );

  assert.equal(lifecycle.phase, "scheduled");
  assert.equal(lifecycle.confidence, "schedule_only");
  assert.equal(displayStatusForPhase(lifecycle.phase, "scheduled"), "scheduled");
});

test("gate-out timestamp does not masquerade as takeoff while provider says taxiing", () => {
  const lifecycle = deriveFlightLifecyclePhase({
    status: "taxiing",
    actual_departure_at: "2026-08-28T10:53:00.000Z",
    scheduled_departure_at: "2026-08-28T11:00:00.000Z",
    estimated_arrival_at: "2026-08-28T13:34:00.000Z",
    normalized_data: {
      takeoffTimes: { actual: null },
      position: { lat: null, lon: null },
    },
  }, Date.parse("2026-08-28T11:12:00.000Z"));

  assert.equal(lifecycle.phase, "taxi_out");
  assert.equal(lifecycle.confidence, "provider_confirmed");
});

test("actual OFF timestamp confirms airborne lifecycle", () => {
  const lifecycle = deriveFlightLifecyclePhase({
    status: "scheduled",
    actual_departure_at: "2026-08-28T10:53:00.000Z",
    estimated_arrival_at: "2026-08-28T13:34:00.000Z",
    normalized_data: {
      takeoffTimes: { actual: "2026-08-28T11:03:00.000Z" },
    },
  }, Date.parse("2026-08-28T11:12:00.000Z"));

  assert.equal(lifecycle.phase, "airborne");
  assert.equal(lifecycle.reason, "actual_takeoff_present");
});

test("fresh zero-altitude low-speed telemetry vetoes schedule-based airborne inference", () => {
  const lifecycle = deriveFlightLifecyclePhase({
    status: "enroute",
    actual_departure_at: "2026-08-28T17:29:00.000Z",
    estimated_arrival_at: "2026-08-29T01:00:00.000Z",
    position_lat: 28.5562,
    position_lon: 77.1,
    altitude: 0,
    ground_speed: 1,
  }, Date.parse("2026-08-28T17:32:00.000Z"));

  assert.equal(lifecycle.phase, "taxi_out");
  assert.equal(lifecycle.confidence, "position_confirmed");
  assert.equal(lifecycle.reason, "ground_telemetry_present");
});

test("fresh airborne telemetry vetoes a contradictory premature landing", () => {
  const now = Date.now();
  const landingAt = new Date(now - 30_000).toISOString();
  const recordedAt = new Date(now - 15_000).toISOString();
  const normalized = normalizedFlight({
    status: "landed",
    actualArrivalAt: landingAt,
    estimatedArrivalAt: new Date(now + 10 * 60_000).toISOString(),
    position: {
      lat: 29.75,
      lon: -98.25,
      altitude: 10_708,
      groundSpeed: 380,
      heading: 250,
      airGround: "A",
      recordedAt,
    },
  });

  const db = mapNormalizedToDb(normalized, {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  const lifecycle = deriveFlightLifecyclePhase(db, now);

  assert.equal(db.status, "enroute");
  assert.equal(db.is_final, false);
  assert.equal(lifecycle.phase, "approaching");
  assert.equal(lifecycle.reason, "fresh_airborne_position_vetoed_terminal_state");
});

test("stale airborne telemetry does not block a confirmed landing", () => {
  const now = Date.now();
  const lifecycle = deriveFlightLifecyclePhase({
    status: "landed",
    actual_arrival_at: new Date(now - 30_000).toISOString(),
    position_lat: 29.75,
    position_lon: -98.25,
    altitude: 10_708,
    ground_speed: 380,
    normalized_data: {
      position: { recordedAt: new Date(now - 10 * 60_000).toISOString() },
    },
  }, now);

  assert.equal(lifecycle.phase, "landed");
  assert.equal(lifecycle.reason, "actual_arrival_present");
});

test("webhook-backed flights avoid scheduled polling unless actively viewed or unsafe", () => {
  const now = Date.parse("2026-05-27T08:00:00.000Z");
  const webhookActive = {
    status: "scheduled",
    provider_alert_status: "active",
    provider_alert_expires_at: "2026-05-28T23:00:00.000Z",
    scheduled_departure_at: "2026-05-27T17:00:00.000Z",
    data_confidence: "high",
  };

  assert.equal(getFlightFreshnessTTL(webhookActive, now, () => 0), 9 * 60 * 60);
  assert.equal(
    getFlightFreshnessTTL({ ...webhookActive, scheduled_departure_at: "2026-06-03T17:00:00.000Z" }, now, () => 0),
    7 * 24 * 60 * 60 + 9 * 60 * 60
  );
  assert.equal(
    getFlightFreshnessTTL({ ...webhookActive, scheduled_departure_at: "2026-05-27T10:00:00.000Z" }, now, () => 0),
    2 * 60 * 60
  );
  assert.equal(
    getFlightFreshnessTTL({ ...webhookActive, status: "airborne" }, now, () => 0),
    30 * 60
  );
  assert.equal(
    getFlightFreshnessTTL({ ...webhookActive, status: "airborne" }, now, () => 0, { activeViewerCount: 1 }),
    30
  );
});

test("webhook-backed stale flights do not poll during scheduled search", async () => {
  const { service, repository, providerCalls } = makeService(normalizedFlight({
    scheduledDepartureAt: "2099-06-03T17:00:00.000Z",
    scheduledArrivalAt: "2099-06-04T01:00:00.000Z",
    estimatedDepartureAt: "2099-06-03T17:00:00.000Z",
    estimatedArrivalAt: "2099-06-04T01:00:00.000Z",
  }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2099-06-03", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  await repository.updateProviderAlert(row.id, {
    providerAlertId: "alert-sq509",
    status: "active",
    expiresAt: "2099-06-04T23:00:00.000Z",
  });
  const alerted = await repository.findFlightById(row.id);
  alerted.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(alerted);
  await service.cache.redis.del(`flight:${flight.flightKey}`);

  const before = providerCalls();
  const response = await service.searchFlight({ airline: "SQ", number: "509", date: "2099-06-03", origin: "BLR", destination: "SIN" });

  assert.equal(providerCalls(), before);
  assert.equal(response.freshness, "fresh");
  assert.equal(response.isRefreshing, false);
  assert.equal(service.queue.jobs.some((job) => job.name === "refreshFlightJob"), false);
});

test("webhook-backed stale flights do not poll inside the 24 hour departure window", async () => {
  const { service, repository, providerCalls } = makeService();
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  await repository.updateProviderAlert(row.id, {
    providerAlertId: "alert-sq509",
    status: "active",
    expiresAt: new Date(Date.now() + 30 * 60 * 60_000).toISOString(),
  });
  const alerted = await repository.findFlightById(row.id);
  alerted.scheduled_departure_at = new Date(Date.now() + 3 * 60 * 60_000).toISOString();
  alerted.estimated_departure_at = alerted.scheduled_departure_at;
  alerted.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(alerted);
  await service.cache.redis.del(`flight:${flight.flightKey}`);

  const before = providerCalls();
  const response = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });

  assert.equal(providerCalls(), before);
  assert.equal(response.freshness, "fresh");
  assert.equal(response.isRefreshing, false);
  assert.equal(service.queue.jobs.some((job) => job.name === "refreshFlightJob"), false);
});

test("saving a flight creates one shared provider alert when the adapter supports it", async () => {
  let alertCalls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    ensureFlightAlert: async () => {
      alertCalls += 1;
      return {
        providerAlertId: "alert-sq509",
        status: "active",
        expiresAt: "2026-05-28T23:00:00.000Z",
        refreshPriority: "minimal",
      };
    },
  });

  const first = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  const second = await service.saveUserFlight("u2", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });

  const row = await repository.findFlightById(first.flight.flightInstanceId);
  assert.equal(first.flight.flightInstanceId, second.flight.flightInstanceId);
  assert.equal(alertCalls, 1);
  assert.equal(row.provider_alert_status, "active");
  assert.equal(row.provider_alert_id, "alert-sq509");
});

test("a flight inside three hours registers one exact inbound aircraft alert", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 10 * 60 * 60_000).toISOString();
  let inboundAlertCalls = 0;
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    inboundFlight: {
      providerFlightId: "AI202-instance",
      flightNumber: "AI202",
      originAirportIata: "DEL",
      destinationAirportIata: "BLR",
      estimatedArrival: new Date(Date.now() + 75 * 60_000).toISOString(),
      status: "scheduled",
    },
  }), {
    ensureInboundFlightAlert: async (_flight, inbound) => {
      inboundAlertCalls += 1;
      assert.equal(inbound.providerFlightId, "AI202-instance");
      return { providerAlertId: "alert-inbound-ai202", status: "active" };
    },
  });

  const saved = await service.saveUserFlight("u1", {
    airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN",
  });
  const row = await repository.findFlightById(saved.flight.flightInstanceId);

  assert.equal(inboundAlertCalls, 1);
  assert.equal(row.normalized_data.inboundFlight.providerAlertId, "alert-inbound-ai202");
  assert.equal(row.normalized_data.inboundFlight.providerAlertStatus, "active");
});

test("an active but incomplete inbound aircraft retries detail resolution with reserved budget", async () => {
  const departure = new Date(Date.now() + 37 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  let detailCalls = 0;
  let inboundAlertCalls = 0;
  const { service, repository } = makeService(normalizedFlight({
    providerFlightId: "DAL2307-instance",
    airlineCode: "DL",
    flightNumber: "2307",
    origin: "MSP",
    destination: "BIS",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    inboundFlight: {
      providerFlightId: "DAL2521-instance",
      destinationAirportIata: "MSP",
      providerAlertId: "alert-inbound-dal2521",
      providerAlertStatus: "active",
    },
  }), {
    fetchFlightByProviderId: async (providerFlightId, fetchOptions) => {
      detailCalls += 1;
      assert.equal(providerFlightId, "DAL2521-instance");
      assert.equal(fetchOptions.budgetEndpoint, "inbound_flight_instance");
      return normalizedFlight({
        providerFlightId,
        airlineCode: "DL",
        flightNumber: "2521",
        origin: "FAR",
        destination: "MSP",
        estimatedArrivalAt: new Date(Date.now() + 20 * 60_000).toISOString(),
      });
    },
    ensureInboundFlightAlert: async () => {
      inboundAlertCalls += 1;
      return { providerAlertId: "replacement", status: "active" };
    },
  });

  const saved = await service.saveUserFlight("u1", {
    airline: "DL", number: "2307", date: departure.slice(0, 10), origin: "MSP", destination: "BIS",
  });
  const row = await repository.findFlightById(saved.flight.flightInstanceId);

  assert.equal(detailCalls, 1);
  assert.equal(inboundAlertCalls, 0);
  assert.equal(saved.flight.inboundFlight.originAirportIata, "FAR");
  assert.ok(saved.flight.inboundFlight.estimatedArrival);
  assert.equal(row.normalized_data.inboundFlight.originAirportIata, "FAR");
  assert.ok(row.normalized_data.inboundFlight.estimatedArrival);
  assert.equal(row.normalized_data.inboundFlight.providerAlertStatus, "active");
});

test("an older active inbound alert is upgraded once to include landing events", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 10 * 60 * 60_000).toISOString();
  const changedAt = new Date(Date.now() - 60_000).toISOString();
  let inboundAlertCalls = 0;
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    inboundFlight: {
      providerFlightId: "AI202-instance",
      flightNumber: "AI202",
      originAirportIata: "DEL",
      destinationAirportIata: "BLR",
      estimatedArrival: new Date(Date.now() + 75 * 60_000).toISOString(),
      status: "airborne",
      providerAlertId: "alert-inbound-ai202",
      providerAlertStatus: "active",
      providerAlertCreatedAt: new Date(Date.now() - 10 * 60_000).toISOString(),
    },
  }), {
    alertConfigurationChangedAt: changedAt,
    ensureInboundFlightAlert: async () => {
      inboundAlertCalls += 1;
      return {
        providerAlertId: "alert-inbound-ai202",
        status: "active",
        createdAt: new Date().toISOString(),
      };
    },
  });

  const saved = await service.saveUserFlight("u1", {
    airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN",
  });
  await service.ensureInboundFlightMonitoring(saved.flight.flightInstanceId, "second-check");

  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  assert.equal(inboundAlertCalls, 1);
  assert.ok(Date.parse(row.normalized_data.inboundFlight.providerAlertCreatedAt) >= Date.parse(changedAt));
});

test("an inbound wheels-off callback creates one deduplicated parent-flight APNs event", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 10 * 60 * 60_000).toISOString();
  const inboundArrival = new Date(Date.now() + 70 * 60_000).toISOString();
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    inboundFlight: {
      providerFlightId: "AI202-instance",
      flightNumber: "AI202",
      originAirportIata: "DEL",
      destinationAirportIata: "BLR",
      estimatedArrival: inboundArrival,
      status: "scheduled",
    },
  }), {
    ensureInboundFlightAlert: async () => ({ providerAlertId: "alert-inbound-ai202", status: "active" }),
  });
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN",
  });
  const callback = {
    event: "off",
    fa_flight_id: "AI202-instance",
    ident_iata: "AI202",
    origin_iata: "DEL",
    destination_iata: "BLR",
    actual_out: new Date().toISOString(),
    estimated_in: inboundArrival,
  };

  await service.processFlightAwareAlertWebhook(callback);
  await service.processFlightAwareAlertWebhook(callback);

  const events = [...repository.__memory.events.values()].filter((event) =>
    event.flight_instance_id === saved.flight.flightInstanceId && event.event_type === "INBOUND_DEPARTED"
  );
  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  assert.equal(events.length, 1);
  assert.equal(row.normalized_data.inboundFlight.status, "airborne");
});

test("an inbound wheels-down callback creates one deduplicated parent-flight APNs event", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 10 * 60 * 60_000).toISOString();
  const inboundArrival = new Date().toISOString();
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    inboundFlight: {
      providerFlightId: "AI202-instance",
      flightNumber: "AI202",
      originAirportIata: "DEL",
      destinationAirportIata: "BLR",
      estimatedArrival: inboundArrival,
      status: "airborne",
    },
  }), {
    ensureInboundFlightAlert: async () => ({ providerAlertId: "alert-inbound-ai202", status: "active" }),
  });
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN",
  });
  const callback = {
    event: "on",
    fa_flight_id: "AI202-instance",
    ident_iata: "AI202",
    origin_iata: "DEL",
    destination_iata: "BLR",
    actual_on: inboundArrival,
  };

  await service.processFlightAwareAlertWebhook(callback);
  await service.processFlightAwareAlertWebhook(callback);

  const events = [...repository.__memory.events.values()].filter((event) =>
    event.flight_instance_id === saved.flight.flightInstanceId && event.event_type === "INBOUND_ARRIVED"
  );
  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  assert.equal(events.length, 1);
  assert.equal(row.normalized_data.inboundFlight.status, "landed");
});

test("lifecycle recovery upgrades an older active provider alert only once", async () => {
  let alertCalls = 0;
  const changedAt = new Date(Date.now() - 1_000).toISOString();
  const { service, repository } = makeService(normalizedFlight(), {
    alertConfigurationChangedAt: changedAt,
    ensureFlightAlert: async (flight) => {
      alertCalls += 1;
      return {
        providerAlertId: flight.provider_alert_id,
        status: "active",
        createdAt: new Date().toISOString(),
      };
    },
  });
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  row.estimated_departure_at = new Date(Date.now() + 30 * 60_000).toISOString();
  row.scheduled_departure_at = row.estimated_departure_at;
  row.estimated_arrival_at = new Date(Date.now() + 3 * 60 * 60_000).toISOString();
  row.scheduled_arrival_at = row.estimated_arrival_at;
  await repository.updateFlight(row);
  await repository.updateProviderAlert(row.id, {
    providerAlertId: "old-alert",
    status: "active",
    createdAt: "2026-08-01T00:00:00.000Z",
  });

  await service.recoverLifecycleCatchups("first_recovery");
  await service.recoverLifecycleCatchups("second_recovery");

  assert.equal(alertCalls, 1);
  assert.ok(Date.parse((await repository.findFlightById(row.id)).provider_alert_created_at) >= Date.parse(changedAt));
});

test("lifecycle recovery repairs missing alerts and reschedules active flights after restart", async () => {
  let alertCalls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    ensureFlightAlert: async () => {
      alertCalls += 1;
      return { providerAlertId: "alert-recovered", status: "active" };
    },
  });
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  row.estimated_departure_at = new Date(Date.now() + 30 * 60_000).toISOString();
  row.scheduled_departure_at = row.estimated_departure_at;
  row.estimated_arrival_at = new Date(Date.now() + 8 * 60 * 60_000).toISOString();
  row.scheduled_arrival_at = row.estimated_arrival_at;
  await repository.updateFlight(row);

  const recovered = await service.recoverLifecycleCatchups("test_restart");
  const updated = await repository.findFlightById(row.id);

  assert.equal(recovered.checked, 1);
  assert.ok(recovered.scheduled >= 3);
  assert.equal(alertCalls, 1);
  assert.equal(updated.provider_alert_status, "active");
  assert.ok(service.queue.jobs.some((job) => job.name === "departureCatchupJob"));
  assert.ok(service.queue.jobs.some((job) => job.name === "arrivalCatchupJob"));
});

test("lifecycle recovery does not requeue catchups whose windows are already past", async () => {
  const departure = new Date(Date.now() - 6 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() - 3 * 60 * 60_000).toISOString();
  const { service, providerCalls } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }));
  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  service.queue.jobs.length = 0;
  const before = providerCalls();
  const recovered = await service.recoverLifecycleCatchups("periodic_recovery");

  assert.equal(recovered.checked, 1);
  assert.equal(recovered.scheduled, 0);
  assert.equal(providerCalls(), before);
  assert.equal(service.queue.jobs.some((job) => job.name.endsWith("CatchupJob")), false);
});

test("actual provider timestamps override a stale scheduled status", async () => {
  const departure = new Date(Date.now() - 4 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() - 30 * 60_000).toISOString();
  const { service, repository } = makeService(normalizedFlight({
    status: "scheduled",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    actualArrivalAt: arrival,
  }));

  const flight = await service.searchFlight({
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });
  const row = await repository.findFlightById(flight.flightInstanceId);

  assert.equal(row.status, "arrived_at_gate");
  assert.equal(row.is_final, true);
  assert.equal(row.normalized_data.status, "arrived_at_gate");
});

test("saving a flight mirrors canonical fields into the user flight row", async () => {
  const { service, repository } = makeService();

  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });

  const userFlight = saved.userFlight;
  assert.equal(userFlight.flight_instance_id, saved.flight.flightInstanceId);
  assert.equal(userFlight.display_flight_number, "SQ 509");
  assert.equal(userFlight.origin_iata, "BLR");
  assert.equal(userFlight.destination_iata, "SIN");
  assert.equal(userFlight.scheduled_departure, "2026-05-27T18:30:00.000Z");
  assert.equal(userFlight.provider_name, "test");
  assert.equal(userFlight.provider_flight_id, "provider-sq509");

  const listed = await repository.listUserFlights("u1");
  assert.equal(listed.length, 1);
  assert.equal(listed[0].userFlight.display_flight_number, "SQ 509");
});

test("live coverage links direct user flight rows to provider alerts", async () => {
  let alertCalls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    ensureFlightAlert: async () => {
      alertCalls += 1;
      return {
        providerAlertId: "alert-sq509",
        status: "active",
        expiresAt: "2026-05-28T23:00:00.000Z",
      };
    },
  });
  const directRow = {
    id: "11111111-1111-4111-8111-111111111111",
    user_id: "u1",
    flight_instance_id: null,
    display_flight_number: "SQ 509",
    origin_iata: "BLR",
    destination_iata: "SIN",
    scheduled_departure: "2026-05-27T18:30:00.000Z",
    lifecycle_state: "upcoming",
    notifications_enabled: true,
    alert_settings_json: { gateChange: true, delayUpdates: true, boardingTime: true, takeoffLanding: true, baggageClaim: true },
  };
  repository.__memory.userFlights.set("direct:u1:sq509", directRow);

  const result = await service.ensureUserFlightLiveCoverage("u1", { ids: [directRow.id] });
  const repaired = [...repository.__memory.userFlights.values()].find((row) => row.id === directRow.id);

  assert.equal(result.checked, 1);
  assert.equal(result.covered, 1);
  assert.equal(result.failed.length, 0);
  assert.ok(repaired.flight_instance_id);
  assert.equal(repaired.notification_enabled, true);
  assert.equal(repaired.alert_preferences.high, true);
  assert.equal(alertCalls, 1);
});

test("active viewer heartbeat records temporary watcher state and queues stale refresh", async () => {
  const { service, repository } = makeService();
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  row.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(row);

  const heartbeat = await service.registerActiveViewer("u1", flight.flightInstanceId);

  assert.equal(heartbeat.flightInstanceId, flight.flightInstanceId);
  assert.equal(heartbeat.activeViewerTtlSeconds, 90);
  assert.ok(service.queue.jobs.some((job) => job.name === "refreshFlightJob" && job.data.reason === "active_viewer"));
});

test("streaming switch keeps one shared provider alert as a delivery safety net", async () => {
  let streamCalls = 0;
  let alertCalls = 0;
  const { service, repository } = makeService(normalizedFlight(), {
    streamingEnabled: true,
    ensureFlightStream: async () => {
      streamCalls += 1;
      return { status: "active", refreshPriority: "minimal" };
    },
    ensureFlightAlert: async () => {
      alertCalls += 1;
      return { providerAlertId: "alert-sq509", status: "active" };
    },
  });

  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });

  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  assert.equal(streamCalls, 1);
  assert.equal(alertCalls, 1);
  assert.equal(row.live_data_source, "streaming");
  assert.equal(row.streaming_status, "active");
});

test("streamed updates change shared state and queue fanout without REST provider calls", async () => {
  const { service, repository, providerCalls } = makeService(normalizedFlight(), { streamingEnabled: true });
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  const before = providerCalls();

  await service.applyStreamedFlightUpdate(saved.flight.flightInstanceId, normalizedFlight({
    status: "cancelled",
    liveDataSource: "streaming",
    streamingStatus: "active",
  }), { eventTime: "2026-05-27T10:00:00.000Z" });

  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  assert.equal(providerCalls(), before);
  assert.equal(row.status, "cancelled");
  assert.equal(row.live_data_source, "streaming");
  assert.equal(row.last_stream_event_at, "2026-05-27T10:00:00.000Z");
  assert.ok([...repository.__memory.events.values()].some((event) => event.event_type === "CANCELLED"));
  assert.ok(service.queue.jobs.some((job) => job.name === "fanoutNotificationJob"));
});

test("position-only streamed updates still project to app state and Live Activities", async () => {
  let projected = 0;
  const { service } = makeService(normalizedFlight(), {
    streamingEnabled: true,
    stateProjection: { syncFlightState: async () => { projected += 1; return { synced: 1 }; } },
    liveActivities: { sendFlightState: async () => ({ sent: 1 }) },
  });
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: "2026-05-27",
    origin: "BLR",
    destination: "SIN",
  });
  service.queue.jobs.length = 0;

  await service.applyStreamedFlightUpdate(saved.flight.flightInstanceId, normalizedFlight({
    position: { lat: 12.97, lon: 77.59, altitude: 18_000, groundSpeed: 410, heading: 120 },
    liveDataSource: "streaming",
    streamingStatus: "active",
  }), { eventTime: "2026-05-27T10:01:00.000Z" });

  assert.equal(projected, 1);
  assert.ok(!service.queue.jobs.some((job) => job.name === "legacyStateProjectionJob"));
  assert.ok(service.queue.jobs.some((job) => job.name === "liveActivityUpdateJob"));
});

test("stream-backed stale flights do not enqueue provider refreshes", async () => {
  const { service, repository, providerCalls } = makeService();
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  await repository.updateStreamingState(row.id, { status: "active", liveDataSource: "streaming" });
  const streamed = await repository.findFlightById(row.id);
  streamed.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(streamed);
  await service.cache.redis.del(`flight:${flight.flightKey}`);

  const before = providerCalls();
  const response = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" });

  assert.equal(providerCalls(), before);
  assert.equal(response.freshness, "fresh");
  assert.equal(response.isRefreshing, false);
  assert.equal(service.queue.jobs.some((job) => job.name === "refreshFlightJob"), false);
});

test("active detail fetch refreshes overdue scheduled shared flights into live state", async () => {
  const pastDeparture = new Date(Date.now() - 12 * 60_000).toISOString();
  const futureArrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: pastDeparture,
        estimatedDepartureAt: pastDeparture,
        scheduledArrivalAt: futureArrival,
        estimatedArrivalAt: futureArrival,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: pastDeparture,
        estimatedDepartureAt: pastDeparture,
        actualDepartureAt: pastDeparture,
        scheduledArrivalAt: futureArrival,
        estimatedArrivalAt: futureArrival,
        position: { lat: 13.5, lon: 77.8, altitude: 18000, groundSpeed: 430, heading: 12 },
      }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: pastDeparture.slice(0, 10), origin: "BLR", destination: "SIN" });
  const response = await service.flightWithWeatherInsight(flight.flightInstanceId, { reason: "test_detail" });

  assert.equal(providerCalls(), 2);
  assert.equal(response.status, "enroute");
  assert.equal(response.actualDepartureAt, pastDeparture);
  assert.equal(response.position.altitude, 18000);

  await service.flightWithWeatherInsight(flight.flightInstanceId, { reason: "test_detail" });
  assert.equal(providerCalls(), 2);
});

test("saved flights schedule bounded catchups plus one shared API poll", async () => {
  const departure = new Date(Date.now() + 30 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 150 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }));

  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  assert.equal(service.queue.jobs.filter((job) => job.name === "departureCatchupJob").length, 2);
  assert.equal(service.queue.jobs.filter((job) => job.name === "arrivalCatchupJob").length, 1);
  assert.equal(service.queue.jobs.filter((job) => job.name === "arrivalDetailRefreshJob").length, 5);
  assert.equal(service.queue.jobs.filter((job) => job.name === "refreshFlightJob").length, 0);
  assert.equal(service.queue.jobs.filter((job) => job.name === "apiFlightPollJob").length, 1);
});

test("API-only active flights schedule one deduplicated two-minute poll for every user", async () => {
  const departure = new Date(Date.now() - 20 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 120 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    status: "airborne",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }));

  const input = {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  };
  await service.saveUserFlight("u1", input);
  await service.saveUserFlight("u2", input);

  const polls = service.queue.jobs.filter((job) => job.name === "apiFlightPollJob");
  assert.equal(polls.length, 1);
  assert.ok(polls[0].options.delayMs <= 2 * 60_000);
  assert.ok(polls[0].options.delayMs > 0);
});

test("shared API polling is disabled when streaming is enabled", async () => {
  const departure = new Date(Date.now() - 5 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 90 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    status: "airborne",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }), {
    streamingEnabled: true,
    ensureFlightStream: async () => ({ status: "active", liveDataSource: "streaming" }),
  });

  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  assert.equal(service.queue.jobs.some((job) => job.name === "apiFlightPollJob"), false);
});

test("API-only polling does not create an overflowing timer for distant flights", async () => {
  const departure = new Date(Date.now() + 10 * 24 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 10 * 24 * 60 * 60_000 + 2 * 60 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }));

  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  assert.equal(service.queue.jobs.some((job) => job.name === "apiFlightPollJob"), false);
});

test("streaming flights retain bounded departure and arrival catchups", async () => {
  const departure = new Date(Date.now() + 30 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 150 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }), {
    streamingEnabled: true,
    ensureFlightStream: async () => ({ status: "active", liveDataSource: "streaming" }),
  });

  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  assert.equal(service.queue.jobs.filter((job) => job.name === "departureCatchupJob").length, 2);
  assert.equal(service.queue.jobs.filter((job) => job.name === "arrivalCatchupJob").length, 1);
});

test("saved flights with missing departure details schedule bounded pre-departure refreshes", async () => {
  const departure = new Date(Date.now() + 150 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 330 * 60_000).toISOString();
  const { service } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    gate: null,
    terminal: null,
  }));

  await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });

  const jobs = service.queue.jobs.filter((job) => job.name === "departureDetailRefreshJob");
  assert.deepEqual(jobs.map((job) => job.data.stage), ["t-2h", "t-30m"]);
  assert.ok(jobs.every((job) => job.options.delayMs >= 0));
});

test("departure detail refresh fills terminal and gate before departure", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 5 * 60 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        gate: null,
        terminal: null,
      })
    : normalizedFlight({
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        gate: "D8",
        terminal: "2",
      }));

  const flight = await service.searchFlight({
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });
  await service.departureDetailRefreshJob({
    data: { flight_instance_id: flight.flightInstanceId, stage: "t-2h" },
  });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(row.gate, "D8");
  assert.equal(row.terminal, "2");
});

test("departure detail refresh skips provider calls when gate and terminal are complete", async () => {
  const departure = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 5 * 60 * 60_000).toISOString();
  const { service, providerCalls } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    gate: "D8",
    terminal: "2",
  }));

  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.departureDetailRefreshJob({
    data: { flight_instance_id: flight.flightInstanceId, stage: "t-2h" },
  });

  assert.equal(providerCalls(), 1);
});

test("arrival catchup reconciles a streamed flight when both lifecycle events were missed", async () => {
  const departure = new Date(Date.now() - 2 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() - 10 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      })
    : normalizedFlight({
        status: "landed",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        actualArrivalAt: arrival,
      }));

  const flight = await service.searchFlight({
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });
  await repository.updateStreamingState(flight.flightInstanceId, {
    status: "active",
    liveDataSource: "streaming",
  });

  await service.arrivalCatchupJob({ data: { flight_instance_id: flight.flightInstanceId } });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(row.status, "landed");
  assert.equal(row.actual_arrival_at, arrival);
});

test("post-arrival refresh replaces a stale exact provider occurrence with the completed match", async () => {
  const departure = new Date(Date.now() - 4 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() - 35 * 60_000).toISOString();
  let exactProviderCalls = 0;
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        providerFlightId: "AKJ1824-stale",
        airlineCode: "QP",
        flightNumber: "1824",
        origin: "DEL",
        destination: "BLR",
        status: "taxiing",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      })
    : normalizedFlight({
        providerFlightId: "AKJ1824-completed",
        airlineCode: "QP",
        flightNumber: "1824",
        origin: "DEL",
        destination: "BLR",
        status: "landed",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        actualArrivalAt: arrival,
      }), {
    fetchFlightByProviderId: async () => {
      exactProviderCalls += 1;
      return normalizedFlight({
        providerFlightId: "AKJ1824-stale",
        airlineCode: "QP",
        flightNumber: "1824",
        origin: "DEL",
        destination: "BLR",
        status: "taxiing",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      });
    },
  });

  const flight = await service.searchFlight({
    airline: "QP",
    number: "1824",
    date: departure.slice(0, 10),
    origin: "DEL",
    destination: "BLR",
  });
  await service.arrivalCatchupJob({ data: { flight_instance_id: flight.flightInstanceId } });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(exactProviderCalls, 1);
  assert.equal(providerCalls(), 2);
  assert.equal(row.provider_flight_id, "AKJ1824-completed");
  assert.equal(row.status, "landed");
  assert.equal(row.actual_arrival_at, arrival);
});

test("arrival detail refresh fills destination gate terminal and baggage before arrival", async () => {
  const departure = new Date(Date.now() - 6 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, repository, providerCalls, providerOptions } = makeService((calls) => calls === 1
    ? normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        arrivalGate: null,
        arrivalTerminal: null,
        baggageBelt: null,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        arrivalGate: "S1",
        arrivalTerminal: "4S",
        baggageBelt: "6",
      }));

  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.arrivalDetailRefreshJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "t-2h" } });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(row.normalized_data.arrivalGate, "S1");
  assert.equal(row.normalized_data.arrivalTerminal, "4S");
  assert.equal(row.baggage_belt, "6");
  assert.equal(providerOptions.at(-1).skipLivePosition, true);
});

test("arrival detail refresh still enriches an active streamed flight", async () => {
  const departure = new Date(Date.now() - 3 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 60 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        arrivalGate: null,
        arrivalTerminal: null,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        arrivalGate: "C3",
        arrivalTerminal: "2",
      }));

  const flight = await service.searchFlight({
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });
  const streamedRow = await repository.findFlightById(flight.flightInstanceId);
  streamedRow.live_data_source = "streaming";
  streamedRow.streaming_status = "active";
  await repository.updateFlight(streamedRow);

  await service.arrivalDetailRefreshJob({
    data: { flight_instance_id: flight.flightInstanceId, stage: "t-60m" },
  });

  const refreshed = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(refreshed.normalized_data.arrivalGate, "C3");
  assert.equal(refreshed.normalized_data.arrivalTerminal, "2");
});

test("arrival detail refresh skips provider calls once destination details are complete", async () => {
  const departure = new Date(Date.now() - 6 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, providerCalls } = makeService(normalizedFlight({
    status: "enroute",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    arrivalGate: "S1",
    arrivalTerminal: "4S",
    baggageBelt: "6",
  }));

  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.arrivalDetailRefreshJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "t-2h" } });

  assert.equal(providerCalls(), 1);
});

test("pre-arrival detail refresh does not poll only for a missing baggage belt", async () => {
  const departure = new Date(Date.now() - 3 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 60 * 60_000).toISOString();
  const { service, providerCalls } = makeService(normalizedFlight({
    status: "enroute",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    arrivalGate: "C3",
    arrivalTerminal: "2",
    baggageBelt: null,
  }));

  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.arrivalDetailRefreshJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "t-60m" } });

  assert.equal(providerCalls(), 1);
});

test("post-arrival detail refresh still polls for a missing baggage belt", async () => {
  const departure = new Date(Date.now() - 5 * 60 * 60_000).toISOString();
  const arrival = new Date(Date.now() - 5 * 60_000).toISOString();
  const { service, providerCalls } = makeService(normalizedFlight({
    status: "landed",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    actualArrivalAt: arrival,
    arrivalGate: "C3",
    arrivalTerminal: "2",
    baggageBelt: null,
  }));

  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.arrivalDetailRefreshJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "post-5m" } });

  assert.equal(providerCalls(), 2);
});

test("departure catchup performs one live refresh after overdue departure", async () => {
  const departure = new Date(Date.now() - 4 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.departureCatchupJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "first" } });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(row.status, "enroute");
  assert.equal(row.actual_departure_at, departure);
});

test("overdue schedule provider ID rebinds to the live operating occurrence", async () => {
  const departure = new Date(Date.now() - 30 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 90 * 60_000).toISOString();
  let exactProviderCalls = 0;
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        providerFlightId: "IGO481-schedule",
        airlineCode: "6E",
        flightNumber: "481",
        origin: "AMD",
        destination: "BLR",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      })
    : normalizedFlight({
        providerFlightId: "IGO23EC-operational",
        airlineCode: "6E",
        flightNumber: "481",
        origin: "AMD",
        destination: "BLR",
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        position: { lat: 18.4, lon: 74.1, altitude: 37_025, groundSpeed: 431, heading: 160 },
      }), {
    fetchFlightByProviderId: async () => {
      exactProviderCalls += 1;
      return normalizedFlight({
        providerFlightId: "IGO481-schedule",
        airlineCode: "6E",
        flightNumber: "481",
        origin: "AMD",
        destination: "BLR",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
      });
    },
  });

  const flight = await service.searchFlight({
    airline: "6E",
    number: "481",
    date: departure.slice(0, 10),
    origin: "AMD",
    destination: "BLR",
  });
  await service.departureCatchupJob({
    data: { flight_instance_id: flight.flightInstanceId, stage: "restart_recovery" },
  });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(exactProviderCalls, 1);
  assert.equal(providerCalls(), 2);
  assert.equal(row.provider_flight_id, "IGO23EC-operational");
  assert.equal(row.status, "enroute");
  assert.equal(row.actual_departure_at, departure);
  assert.equal(row.altitude, 37_025);
});

test("overdue tracked flight refresh uses reserved FlightAware capacity", async () => {
  const departure = new Date(Date.now() - 30 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 90 * 60_000).toISOString();
  const { service, providerOptions } = makeService(normalizedFlight({
    providerFlightId: "IGO481-schedule",
    airlineCode: "6E",
    flightNumber: "481",
    origin: "AMD",
    destination: "BLR",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }));

  const flight = await service.searchFlight({
    airline: "6E",
    number: "481",
    date: departure.slice(0, 10),
    origin: "AMD",
    destination: "BLR",
  });
  await service.departureCatchupJob({
    data: { flight_instance_id: flight.flightInstanceId, stage: "restart_recovery" },
  });

  assert.equal(providerOptions.at(-1).budgetEndpoint, "tracked_flight");
});

test("departure catchup still refreshes after gate-out while takeoff is unconfirmed", async () => {
  const departure = new Date(Date.now() - 4 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        status: "taxiing",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        takeoffTimes: { actual: null },
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
        scheduledArrivalAt: arrival,
        estimatedArrivalAt: arrival,
        takeoffTimes: { actual: departure },
      }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });

  await service.departureCatchupJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "final" } });

  assert.equal(providerCalls(), 2);
  assert.equal((await repository.findFlightById(flight.flightInstanceId)).status, "enroute");
});

test("lifecycle recovery replaces a missed takeoff timer after a deploy", async () => {
  const departure = new Date(Date.now() - 30 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 2 * 60 * 60_000).toISOString();
  const { service, repository } = makeService(normalizedFlight({
    status: "taxiing",
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    actualDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
    takeoffTimes: { actual: null },
  }));
  const saved = await service.saveUserFlight("u1", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
  });
  const row = await repository.findFlightById(saved.flight.flightInstanceId);
  row.last_fetched_at = new Date(Date.now() - 20 * 60_000).toISOString();
  row.updated_at = row.last_fetched_at;
  await repository.updateFlight(row);

  const result = await service.recoverLifecycleCatchups("api_startup");

  assert.equal(result.checked, 1);
  assert.ok(service.queue.jobs.some((job) =>
    job.name === "departureCatchupJob" && job.data.stage === "restart_recovery"
  ));
});

test("weather insights are cached by airport hour and can create one advisory event", async () => {
  let weatherCalls = 0;
  const departure = new Date(Date.now() + 4 * 60 * 60_000).toISOString();
  const date = departure.slice(0, 10);
  const weather = {
    async insightForFlight(row) {
      weatherCalls += 1;
      return {
        available: true,
        provider: "weatherkit",
        airportCode: row.origin_airport,
        airportRole: "departure",
        forecastTime: row.estimated_departure_at,
        generatedAt: new Date().toISOString(),
        title: "Departure Weather",
        summary: `${row.airline_code}${row.flight_number} is on time and weather at ${row.origin_airport} looks favorable for departure.`,
        severity: "low",
        notificationRequired: true,
      };
    },
  };
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
  }), { weather });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
  }), { airline: "SQ", number: "509", date, origin: "BLR", destination: "SIN", flightKey: `SQ-509-${date}-BLR-SIN` }, departure);

  await service.weatherInsightJob({ data: { flight_instance_id: row.id, reason: "test" } });
  await service.weatherInsightJob({ data: { flight_instance_id: row.id, reason: "test" } });

  assert.equal(weatherCalls, 2);
  assert.equal([...repository.__memory.events.values()].filter((event) => event.event_type === "WEATHER_ADVISORY").length, 1);
  assert.ok(service.queue.jobs.some((job) => job.name === "fanoutNotificationJob"));
});

test("five-hour reminder is durable and excludes tracking-only followers", async () => {
  const departure = new Date(Date.now() + 5 * 60 * 60_000 + 5 * 60_000).toISOString();
  const arrival = new Date(Date.now() + 8 * 60 * 60_000).toISOString();
  const weather = {
    async insightForFlight(row) {
      return {
        available: true,
        provider: "weatherkit",
        airportCode: row.origin_airport,
        airportRole: "departure",
        forecastTime: departure,
        generatedAt: new Date().toISOString(),
        severity: "low",
        notificationRequired: true,
        conditionCode: "Clear",
        temperatureC: 28,
      };
    },
  };
  const { service, repository } = makeService(normalizedFlight({
    scheduledDepartureAt: departure,
    estimatedDepartureAt: departure,
    scheduledArrivalAt: arrival,
    estimatedArrivalAt: arrival,
  }), { weather });

  const traveler = await service.saveUserFlight("traveler", {
    airline: "SQ",
    number: "509",
    date: departure.slice(0, 10),
    origin: "BLR",
    destination: "SIN",
    sourceType: "manual_search",
  });
  await repository.upsertUserFlight("tracker", traveler.flight.flightInstanceId, {
    sourceType: "tracked",
  });

  assert.ok(service.queue.jobs.some((job) =>
    job.name === "preflightReminderJob" &&
    job.data.flight_instance_id === traveler.flight.flightInstanceId
  ));

  const event = await service.preflightReminderJob({
    data: { flight_instance_id: traveler.flight.flightInstanceId },
  });
  assert.equal(event.event_type, "TRIP_STARTING");
  assert.equal(event.new_value.weatherInsight.temperatureC, 28);

  await service.weatherInsightJob({
    data: { flight_instance_id: traveler.flight.flightInstanceId, reason: "test" },
  });
  assert.equal(
    [...repository.__memory.events.values()].filter((item) => item.event_type === "WEATHER_ADVISORY").length,
    0
  );

  const targets = await repository.listNotificationTargets(
    traveler.flight.flightInstanceId,
    "low",
    "TRIP_STARTING"
  );
  assert.deepEqual(targets.map((target) => target.userFlight.user_id), ["traveler"]);
});

test("WeatherKit response is normalized into a conservative flight weather insight", () => {
  const insight = buildWeatherInsight({
    raw: {
      hourlyForecast: {
        hours: [{
          forecastStart: "2026-05-27T18:00:00.000Z",
          conditionCode: "Clear",
          temperature: 28,
          windSpeed: 12,
          precipitationChance: 0.05,
          visibility: 10000,
        }],
      },
      weatherAlerts: { alerts: [] },
    },
    airport: { code: "BLR", name: "Kempegowda", coordinate: { latitude: 13.2, longitude: 77.7 } },
    target: { role: "departure", airportCode: "BLR", forecastTime: "2026-05-27T18:00:00.000Z" },
    row: { airline_code: "SQ", flight_number: "509", status: "scheduled", scheduled_departure_at: "2026-05-27T18:00:00.000Z" },
    nowMs: Date.parse("2026-05-27T14:00:00.000Z"),
  });

  assert.equal(insight.available, true);
  assert.equal(insight.severity, "low");
  assert.equal(insight.notificationRequired, true);
  assert.match(insight.summary, /weather at BLR looks favorable/i);
});
