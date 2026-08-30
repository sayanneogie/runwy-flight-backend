"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { createFlightCache, createMemoryRedis } = require("../src/shared-flight/cache");
const { createMemorySharedFlightRepository } = require("../src/shared-flight/repository");
const { createSharedFlightService } = require("../src/shared-flight/service");
const {
  generateFlightAwareAlertDedupeKey,
  normalizeFlightAwareAlert,
  targetMatchesAlert,
} = require("../src/flightaware-alerts");

function normalizedFlight(overrides = {}) {
  return {
    providerFlightId: "AI2814-2026-05-09",
    airlineCode: "AI",
    flightNumber: "2814",
    origin: "BLR",
    destination: "DEL",
    status: "scheduled",
    scheduledDepartureAt: "2026-05-09T16:30:00.000Z",
    scheduledArrivalAt: "2026-05-09T19:25:00.000Z",
    estimatedDepartureAt: "2026-05-09T16:30:00.000Z",
    estimatedArrivalAt: "2026-05-09T19:25:00.000Z",
    actualDepartureAt: null,
    actualArrivalAt: null,
    gate: "D9",
    terminal: "2",
    baggageBelt: null,
    position: { lat: null, lon: null, altitude: null, groundSpeed: null, heading: null },
    provider: "flightaware",
    dataConfidence: "high",
    rawProviderResponse: { ok: true },
    ...overrides,
  };
}

async function makeAlertService(options = {}) {
  const repository = createMemorySharedFlightRepository();
  const service = createSharedFlightService({
    repository,
    provider: { name: "flightaware", fetchFlightByNumber: async () => normalizedFlight() },
    cache: createFlightCache(createMemoryRedis()),
    apns: options.apns,
  });
  const row = await repository.upsertFlightFromNormalized(normalizedFlight(), {
    airline: "AI",
    number: "2814",
    date: "2026-05-09",
    origin: "BLR",
    destination: "DEL",
    flightKey: "AI-2814-2026-05-09-BLR-DEL",
  }, "2026-05-09T10:00:00.000Z");
  return { repository, service, row };
}

test("normalizes FlightAware alert payloads into Runwy flight events", () => {
  const normalized = normalizeFlightAwareAlert({
    event: "departure",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: { code: "BLR" },
    destination: { code: "DEL" },
    scheduled_out: "2026-05-09T16:30:00Z",
    actual_out: "2026-05-09T16:42:00Z",
  });

  assert.equal(normalized.event_type, "flight_departed");
  assert.equal(normalized.flight_key, "AI-2814-2026-05-09-BLR-DEL");
  assert.equal(normalized.fa_flight_id, "AI2814-2026-05-09");
  assert.equal(normalized.origin, "BLR");
  assert.equal(normalized.destination, "DEL");
});

test("normalizes terse FlightAware ON and IN arrival event codes", () => {
  for (const event of ["on", "in"]) {
    const normalized = normalizeFlightAwareAlert({
      event,
      ident: "AI2814",
      fa_flight_id: "AI2814-2026-05-09",
      origin: "BLR",
      destination: "DEL",
      actual_in: "2026-05-09T19:30:00Z",
    });

    assert.equal(normalized.event_type, "flight_arrived");
  }
});

test("normalizes production AeroAPI ON callbacks using IATA route and touchdown time", () => {
  const normalized = normalizeFlightAwareAlert({
    event_code: "on",
    summary: "ABY496 arrived at BLR from SHJ",
    flight: {
      ident: "ABY496",
      ident_iata: "G9496",
      fa_flight_id: "ABY496-instance",
      origin: "OMSJ",
      origin_iata: "SHJ",
      destination: "VOBL",
      destination_iata: "BLR",
      actual_out: "2026-08-29T18:16:00Z",
      actual_on: "2026-08-29T22:24:55Z",
      scheduled_out: "2026-08-29T18:10:00Z",
    },
  });

  assert.equal(normalized.event_type, "flight_arrived");
  assert.equal(normalized.origin, "SHJ");
  assert.equal(normalized.destination, "BLR");
  assert.equal(normalized.airlineCode, "G9");
  assert.equal(normalized.actual_in, "2026-08-29T22:24:55.000Z");
  assert.equal(normalized.event_time, "2026-08-29T22:24:55.000Z");
});

test("minutes-out callbacks do not mark a flight landed", () => {
  const normalized = normalizeFlightAwareAlert({
    event_code: "minutes_out",
    summary: "ABY496 is expected to arrive at BLR in 5 min",
    flight: {
      ident_iata: "G9496",
      fa_flight_id: "ABY496-instance",
      origin_iata: "SHJ",
      destination_iata: "BLR",
      estimated_on: "2026-08-29T22:25:52Z",
    },
  });

  assert.equal(normalized.event_type, "flight_arrival_soon");
});

test("arrival callbacks without actual ON or IN time require verification", async () => {
  const { repository, service, row } = await makeAlertService();
  const result = await service.processFlightAwareAlertWebhook({
    event_code: "arrival",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    event_time: "2026-05-09T19:25:00Z",
  });
  const updated = await repository.findFlightById(row.id);
  const verification = service.queue.jobs.find((job) =>
    job.name === "refreshFlightJob" && job.data.trigger === "unconfirmed_arrival_alert"
  );

  assert.equal(result.matchedFlights, 1);
  assert.equal(result.appliedEvents, 0);
  assert.equal(updated.status, "scheduled");
  assert.ok(verification);
  assert.equal([...repository.__memory.events.values()].some((event) => event.event_type === "LANDED"), false);
});

test("normalizes a FlightAware OUT event as taxiing", () => {
  const normalized = normalizeFlightAwareAlert({
    event: "OUT",
    ident_iata: "AI2418",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-08-28T11:00:00.000Z",
    actual_out: "2026-08-28T10:53:00.000Z",
  });

  assert.equal(normalized.event_type, "flight_taxiing");
  assert.equal(normalized.actual_out, "2026-08-28T10:53:00.000Z");
  assert.equal(normalized.actual_off, null);
});

test("keeps FlightAware gate-out and wheels-off timestamps separate", async () => {
  const { repository, service, row } = await makeAlertService();
  const actualOut = "2026-05-09T16:42:00Z";
  const actualOff = "2026-05-09T16:55:00Z";

  const taxi = await service.processFlightAwareAlertWebhook({
    event: "OUT",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    actual_out: actualOut,
  });
  const taxiRow = await repository.findFlightById(row.id);

  assert.equal(taxi.appliedEvents, 1);
  assert.equal(taxiRow.status, "taxiing");
  assert.equal(taxiRow.normalized_data.departureTimes.actual, "2026-05-09T16:42:00.000Z");
  assert.equal(taxiRow.normalized_data.takeoffTimes.actual, null);

  const takeoff = await service.processFlightAwareAlertWebhook({
    event: "OFF",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    actual_out: actualOut,
    actual_off: actualOff,
  });
  const airborneRow = await repository.findFlightById(row.id);

  assert.equal(takeoff.appliedEvents, 1);
  assert.equal(airborneRow.status, "airborne");
  assert.equal(airborneRow.normalized_data.departureTimes.actual, "2026-05-09T16:42:00.000Z");
  assert.equal(airborneRow.normalized_data.takeoffTimes.actual, "2026-05-09T16:55:00.000Z");
});

test("impending departure remains preflight and does not masquerade as takeoff", () => {
  const normalized = normalizeFlightAwareAlert({
    event: "impending_departure",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-05-09T16:30:00Z",
    event_time: "2026-05-09T15:30:00Z",
    minutes_until_departure: 60,
  });

  assert.equal(normalized.event_type, "flight_departure_soon");
  assert.equal(normalized.minutes_until_departure, 60);
  assert.match(normalized.human_readable_summary, /starts in about 60 minutes/);
});

test("impending departure webhook creates a Trip Starting Soon APNs event without making the flight airborne", async () => {
  const sent = [];
  const { repository, service, row } = await makeAlertService({
    apns: {
      sendFlightEvent: async ({ token, event }) => {
        sent.push({ token: token.device_token, type: event.event_type });
        return { ok: true };
      },
    },
  });
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: true, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const result = await service.processFlightAwareAlertWebhook({
    event: "impending_departure",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-05-09T16:30:00Z",
    event_time: "2026-05-09T15:30:00Z",
    minutes_until_departure: 60,
  });
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "TRIP_STARTING");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  const updated = await repository.findFlightById(row.id);

  assert.equal(result.appliedEvents, 1);
  assert.equal(updated.status, "scheduled");
  assert.equal(event.notification_required, true);
  assert.deepEqual(sent, [{ token: "token-u1", type: "TRIP_STARTING" }]);
});

test("dedupe key is stable for duplicate FlightAware alert payloads", () => {
  const raw = {
    event: "arrival",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    actual_in: "2026-05-09T19:30:00Z",
  };
  const alert = normalizeFlightAwareAlert(raw);
  assert.equal(generateFlightAwareAlertDedupeKey(alert, raw), generateFlightAwareAlertDedupeKey(alert, raw));
});

test("duplicate webhook does not create duplicate APNs deliveries", async () => {
  const sent = [];
  const { repository, service, row } = await makeAlertService({
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: true, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  const payload = {
    event: "departure",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: { code: "BLR" },
    destination: { code: "DEL" },
    actual_out: "2026-05-09T16:42:00Z",
  };

  const first = await service.processFlightAwareAlertWebhook(payload);
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "AIRBORNE");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  const second = await service.processFlightAwareAlertWebhook(payload);

  assert.equal(first.appliedEvents, 1);
  assert.equal(second.duplicateEvents, 1);
  assert.equal(repository.__memory.deliveries.size, 1);
  assert.deepEqual(sent, ["token-u1"]);
});

test("unknown FlightAware payload is logged and does not crash", async () => {
  const { repository, service } = await makeAlertService();
  const result = await service.processFlightAwareAlertWebhook({ event: "mystery", ident: "AI2814" });
  assert.equal(result.unknownEvents, 1);
  assert.equal(repository.__memory.flightEventLogs.size, 1);
  assert.equal(repository.__memory.events.size, 0);
});

test("small delay webhook updates state but does not fan out notification", async () => {
  const { repository, service } = await makeAlertService();
  const result = await service.processFlightAwareAlertWebhook({
    event: "delay",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-05-09T16:30:00Z",
    estimated_out: "2026-05-09T16:34:00Z",
  });
  assert.equal(result.appliedEvents, 1);
  assert.equal([...repository.__memory.events.values()].some((item) => item.notification_required), false);
});

test("notification preference filtering prevents webhook APNs fanout", async () => {
  const sent = [];
  const { repository, service, row } = await makeAlertService({
    apns: { sendFlightEvent: async ({ token }) => { sent.push(token.device_token); return { ok: true }; } },
  });
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: false, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "token-u1", environment: "sandbox" });

  await service.processFlightAwareAlertWebhook({
    event: "departure",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    actual_out: "2026-05-09T16:42:00Z",
  });
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "AIRBORNE");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });

  assert.equal(repository.__memory.deliveries.size, 0);
  assert.deepEqual(sent, []);
});

test("no subscribed users case stores event without APNs delivery", async () => {
  const { repository, service } = await makeAlertService();
  await service.processFlightAwareAlertWebhook({
    event: "cancelled",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-05-09T16:30:00Z",
  });
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "CANCELLED");
  const result = await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });
  assert.equal(result.sent, 0);
  assert.equal(repository.__memory.deliveries.size, 0);
});

test("APNs invalid token result marks shared device token inactive", async () => {
  const { repository, service, row } = await makeAlertService({
    apns: { sendFlightEvent: async () => ({ ok: false, reason: "Unregistered" }) },
  });
  await repository.upsertUserFlight("u1", row.id, { alertPreferences: { low: true, medium: true, high: true, critical: true } });
  await repository.upsertDeviceToken("u1", { deviceToken: "dead-token", environment: "sandbox" });

  await service.processFlightAwareAlertWebhook({
    event: "cancelled",
    ident: "AI2814",
    fa_flight_id: "AI2814-2026-05-09",
    origin: "BLR",
    destination: "DEL",
    scheduled_out: "2026-05-09T16:30:00Z",
  });
  const event = [...repository.__memory.events.values()].find((item) => item.event_type === "CANCELLED");
  await service.fanoutNotificationJob({ data: { flight_event_id: event.id } });

  const token = [...repository.__memory.deviceTokens.values()].find((item) => item.device_token === "dead-token");
  assert.equal(token.is_active, false);
});

test("FlightAware alert matching requires the exact flight instance date and route", async () => {
  const { service } = await makeAlertService();
  const wrongDate = await service.processFlightAwareAlertWebhook({
    event: "departure",
    ident: "AI2814",
    fa_flight_id: "different-provider-id",
    origin: "BLR",
    destination: "DEL",
    actual_out: "2026-05-10T16:42:00Z",
  });
  assert.equal(wrongDate.matchedFlights, 0);
});

test("exact FlightAware instance ids survive UTC date-boundary mismatches", () => {
  const alert = normalizeFlightAwareAlert({
    event: "on",
    ident: "ABY496",
    fa_flight_id: "ABY496-instance",
    origin: "SHJ",
    destination: "BLR",
    scheduled_out: "2026-08-29T19:25:00.000Z",
    actual_on: "2026-08-29T23:30:00.000Z",
  });

  assert.equal(alert.actual_in, "2026-08-29T23:30:00.000Z");
  assert.equal(targetMatchesAlert({
    provider_flight_id: "ABY496-instance",
    departure_date: "2026-08-28T18:30:00.000Z",
    origin_airport: "SHJ",
    destination_airport: "BLR",
  }, alert), true);
});
