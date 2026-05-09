"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { createFlightCache, createMemoryRedis } = require("../src/shared-flight/cache");
const { createMemorySharedFlightRepository } = require("../src/shared-flight/repository");
const { createSharedFlightService } = require("../src/shared-flight/service");
const {
  generateFlightAwareAlertDedupeKey,
  normalizeFlightAwareAlert,
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
