"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const { createFlightCache, createMemoryRedis } = require("../src/shared-flight/cache");
const { createMemorySharedFlightRepository } = require("../src/shared-flight/repository");
const { createSharedFlightService } = require("../src/shared-flight/service");
const { compareFlightState, getFlightFreshnessTTL } = require("../src/shared-flight/state");
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
  const repository = createMemorySharedFlightRepository();
  const queue = options.queue;
  const provider = {
    name: "test",
    async fetchFlightByNumber() {
      calls += 1;
      if (options.delayMs) await new Promise((resolve) => setTimeout(resolve, options.delayMs));
      return typeof providerFlight === "function" ? providerFlight(calls) : providerFlight;
    },
    ensureFlightAlert: options.ensureFlightAlert,
    ensureFlightStream: options.ensureFlightStream,
  };
  const service = createSharedFlightService({
    repository,
    provider,
    streamingEnabled: options.streamingEnabled === true,
    queue,
    cache: createFlightCache(createMemoryRedis()),
    weather: options.weather,
    wait: options.wait || ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
    apns: options.apns,
  });
  return { service, repository, providerCalls: () => calls };
}

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
    apns: { sendFlightEvent: async ({ token }) => sent.push(token.device_token) },
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

  const { service, repository } = makeService();
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), { airline: "SQ", number: "509", date: "2026-05-27", origin: "BLR", destination: "SIN" }, "2026-05-27T00:00:00.000Z");
  const event = (await repository.insertEvents(row.id, changed, "test"))[0];
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: false, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal(repository.__memory.deliveries.size, 0);
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
    scheduledDepartureAt: "2026-06-03T17:00:00.000Z",
    scheduledArrivalAt: "2026-06-04T01:00:00.000Z",
    estimatedDepartureAt: "2026-06-03T17:00:00.000Z",
    estimatedArrivalAt: "2026-06-04T01:00:00.000Z",
  }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-06-03", origin: "BLR", destination: "SIN" });
  const row = await repository.findFlightById(flight.flightInstanceId);
  await repository.updateProviderAlert(row.id, {
    providerAlertId: "alert-sq509",
    status: "active",
    expiresAt: "2026-06-04T23:00:00.000Z",
  });
  const alerted = await repository.findFlightById(row.id);
  alerted.fresh_until = "2026-05-01T00:00:00.000Z";
  await repository.updateFlight(alerted);
  await service.cache.redis.del(`flight:${flight.flightKey}`);

  const before = providerCalls();
  const response = await service.searchFlight({ airline: "SQ", number: "509", date: "2026-06-03", origin: "BLR", destination: "SIN" });

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
    expiresAt: "2026-05-28T23:00:00.000Z",
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

test("streaming switch registers shared flights for stream updates instead of provider alerts", async () => {
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
  assert.equal(alertCalls, 0);
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
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: pastDeparture,
        estimatedDepartureAt: pastDeparture,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: pastDeparture,
        estimatedDepartureAt: pastDeparture,
        actualDepartureAt: pastDeparture,
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

test("saved flights schedule low-call departure and arrival catchups without polling loops", async () => {
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
  assert.equal(service.queue.jobs.filter((job) => job.name === "refreshFlightJob").length, 0);
});

test("departure catchup performs one live refresh after overdue departure", async () => {
  const departure = new Date(Date.now() - 4 * 60_000).toISOString();
  const { service, repository, providerCalls } = makeService((calls) => calls === 1
    ? normalizedFlight({
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
      })
    : normalizedFlight({
        status: "enroute",
        scheduledDepartureAt: departure,
        estimatedDepartureAt: departure,
        actualDepartureAt: departure,
      }));
  const flight = await service.searchFlight({ airline: "SQ", number: "509", date: departure.slice(0, 10), origin: "BLR", destination: "SIN" });
  await service.departureCatchupJob({ data: { flight_instance_id: flight.flightInstanceId, stage: "first" } });

  const row = await repository.findFlightById(flight.flightInstanceId);
  assert.equal(providerCalls(), 2);
  assert.equal(row.status, "enroute");
  assert.equal(row.actual_departure_at, departure);
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
