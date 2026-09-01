"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";
process.env.PROVIDER_CALLS_ENABLED = "false";
process.env.DISABLE_PROVIDER_CALLS = "true";

const { __test__ } = require("../src/server");

function flight(overrides = {}) {
  return {
    id: "00000000-0000-4000-8000-000000000001",
    status: "scheduled",
    scheduled_departure_at: "2026-08-30T18:00:00.000Z",
    estimated_departure_at: "2026-08-30T18:10:00.000Z",
    scheduled_arrival_at: "2026-08-30T22:00:00.000Z",
    estimated_arrival_at: "2026-08-30T21:50:00.000Z",
    normalized_data: {},
    ...overrides,
  };
}

test("Live Activity state is driven by the canonical lifecycle phase", () => {
  const now = Date.now();
  const actualDeparture = new Date(now - 3 * 60 * 60_000).toISOString();
  const value = flight({
    status: "enroute",
    scheduled_departure_at: new Date(now - 3 * 60 * 60_000 - 9 * 60_000).toISOString(),
    estimated_departure_at: actualDeparture,
    actual_departure_at: actualDeparture,
    scheduled_arrival_at: new Date(now + 3 * 60 * 60_000 + 10 * 60_000).toISOString(),
    estimated_arrival_at: new Date(now + 3 * 60 * 60_000).toISOString(),
    normalized_data: {
      takeoffTimes: { actual: actualDeparture },
    },
  });

  assert.equal(__test__.liveActivityPhase(value), "cruise");
  const state = __test__.liveActivityContentState(value, new Date(now));
  assert.equal(state.phase, "cruise");
  assert.ok(state.progress > 0.49 && state.progress < 0.51);
  assert.equal(state.departureDelayMinutes, 9);
});

test("shared-owned Firehose subscriptions do not write a second personal snapshot", () => {
  assert.equal(
    __test__.shouldDirectlyProjectFirehoseTrackedRecord({
      metadata: { providerRefreshOwner: "shared_flight_instance" },
    }),
    false
  );
  assert.equal(
    __test__.shouldDirectlyProjectFirehoseTrackedRecord({ metadata: {} }),
    true
  );
});

test("Live Activity delivery overlays a linked tracking snapshot onto a stale shared flight", () => {
  const sharedFlight = flight({
    status: "scheduled",
    actual_departure_at: null,
    estimated_arrival_at: null,
  });
  const trackingSnapshot = {
    status: "enroute",
    departureTimes: {
      scheduled: "2026-09-01T11:50:00.000Z",
      actual: "2026-09-01T11:48:00.000Z",
    },
    arrivalTimes: {
      scheduled: "2026-09-01T14:30:00.000Z",
      estimated: "2026-09-01T14:24:00.000Z",
    },
    takeoffTimes: { actual: "2026-09-01T11:48:00.000Z" },
    landingTimes: { estimated: "2026-09-01T14:24:00.000Z" },
    progressPercent: 28,
    baggageClaim: "6A",
    lastUpdated: "2026-09-01T12:36:05.000Z",
  };

  const deliveryFlight = __test__.liveActivityFlightByApplyingTrackingSnapshot(
    sharedFlight,
    trackingSnapshot,
    "6A"
  );
  const state = __test__.liveActivityContentState(
    deliveryFlight,
    new Date("2026-09-01T12:36:05.000Z")
  );

  assert.equal(__test__.liveActivityPhase(deliveryFlight), "cruise");
  assert.equal(state.phase, "cruise");
  assert.equal(state.baggageClaim, "6A");
  assert.ok(state.progress > 0.20, `expected airborne progress, received ${state.progress}`);
});

test("Live Activity delivery cannot overlay an older projection revision", () => {
  const sharedFlight = flight({
    status: "enroute",
    state_revision: 12,
    actual_departure_at: "2026-09-01T11:48:00.000Z",
  });
  const staleProjection = {
    stateRevision: 11,
    status: "scheduled",
    lastUpdated: "2026-09-01T12:20:00.000Z",
  };

  const delivery = __test__.liveActivityDeliveryFlight(
    sharedFlight,
    staleProjection,
    null,
    11
  );
  assert.equal(delivery, sharedFlight);
  assert.equal(__test__.liveActivityPhase(delivery), "cruise");
});

test("Live Activity progress follows an airborne flight toward arrival", () => {
  const now = new Date("2026-09-01T11:30:00.000Z");
  const value = flight({
    status: "enroute",
    actual_departure_at: "2026-09-01T09:10:00.000Z",
    estimated_arrival_at: "2026-09-01T11:35:00.000Z",
    normalized_data: {
      takeoffTimes: { actual: "2026-09-01T09:10:00.000Z" },
      landingTimes: { estimated: "2026-09-01T11:35:00.000Z" },
      progressPercent: 97,
    },
  });

  const state = __test__.liveActivityContentState(value, now);
  assert.equal(state.phase, "descent");
  assert.ok(state.progress > 0.77, `expected near-arrival path progress, received ${state.progress}`);
  assert.ok(state.progress <= 0.80);
});

test("shared flight projection retains the complete flown breadcrumb trail", () => {
  const trackPoints = [
    { latitude: 23.62, longitude: 87.24, recordedAt: "2026-09-01T09:10:00.000Z" },
    { latitude: 20.50, longitude: 82.00, recordedAt: "2026-09-01T10:15:00.000Z" },
    { latitude: 13.20, longitude: 77.60, recordedAt: "2026-09-01T11:30:00.000Z" },
  ];
  const projected = __test__.trackedPayloadFromSharedFlight({
    airlineCode: "6E",
    flightNumber: "509",
    origin: "RDP",
    destination: "BLR",
    status: "arrived_at_gate",
    normalized_data: { trackPoints },
    position: {},
  });

  assert.equal(projected.trackPoints.length, 3);
  assert.deepEqual(
    projected.trackPoints.map(({ latitude, longitude }) => ({ latitude, longitude })),
    trackPoints.map(({ latitude, longitude }) => ({ latitude, longitude }))
  );
});

test("Live Activity landing state never advances from the arrival schedule alone", () => {
  const overdue = flight({
    estimated_arrival_at: "2026-08-29T21:50:00.000Z",
  });
  assert.equal(__test__.liveActivityPhase(overdue), "scheduled");

  const landed = flight({
    status: "landed",
    normalized_data: { landingTimes: { actual: "2026-08-30T21:48:00.000Z" } },
  });
  assert.equal(__test__.liveActivityPhase(landed), "landed");
  assert.equal(__test__.liveActivityContentState(landed).bannerTitle, "Landed");
});

test("Live Activity APNs stale date follows arrival after takeoff", () => {
  const now = new Date("2026-08-30T19:00:00.000Z");
  const value = flight({
    status: "enroute",
    actual_departure_at: "2026-08-30T18:15:00.000Z",
    estimated_arrival_at: "2026-08-30T21:50:00.000Z",
  });

  assert.equal(
    __test__.liveActivityStaleDateUnix(value, now),
    Math.floor(new Date("2026-08-30T22:20:00.000Z").getTime() / 1000)
  );
});

test("Live Activity stale date keeps a late taxiing flight fresh for another hour", () => {
  const now = new Date("2026-08-30T19:30:00.000Z");
  const value = flight({
    status: "taxiing",
    estimated_departure_at: "2026-08-30T18:10:00.000Z",
  });

  assert.equal(
    __test__.liveActivityStaleDateUnix(value, now),
    Math.floor(new Date("2026-08-30T20:30:00.000Z").getTime() / 1000)
  );
});

test("Live Activity tokens survive transient APNs failures", () => {
  assert.equal(__test__.isPermanentLiveActivityTokenFailure({ status: 500, reason: "InternalServerError" }), false);
  assert.equal(__test__.isPermanentLiveActivityTokenFailure({ status: 403, reason: "ExpiredProviderToken" }), false);
  assert.equal(__test__.isPermanentLiveActivityTokenFailure({ status: 410, reason: "Unregistered" }), true);
});

test("Live Activity token registration never regresses a newer local phase", () => {
  assert.equal(__test__.shouldSendInitialLiveActivityState("scheduled", "cruise"), false);
  assert.equal(__test__.shouldSendInitialLiveActivityState("taxiOut", "departed"), false);
  assert.equal(__test__.shouldSendInitialLiveActivityState("cruise", "scheduled"), true);
  assert.equal(__test__.shouldSendInitialLiveActivityState("arrivedAtGate", "cruise"), true);
  assert.equal(__test__.shouldSendInitialLiveActivityState("scheduled", null), true);
});

test("arrived Live Activity remains updateable for baggage changes", () => {
  const arrivedAt = "2026-09-01T11:01:00.000Z";
  const shortlyAfterArrival = new Date("2026-09-01T11:06:00.000Z");
  const value = flight({
    status: "arrived_at_gate",
    actual_arrival_at: arrivedAt,
    normalized_data: { baggageBelt: "5A", arrivalTimes: { actual: arrivedAt } },
  });

  assert.equal(__test__.shouldEndLiveActivity(value, shortlyAfterArrival), false);
  assert.equal(__test__.liveActivityContentState(value, shortlyAfterArrival).baggageClaim, "5A");

  // The canonical column may be updated before an older normalized snapshot.
  // Live Activities must prefer the newest baggage assignment.
  value.baggage_belt = "6A";
  assert.equal(__test__.liveActivityContentState(value, shortlyAfterArrival).baggageClaim, "6A");
  assert.equal(
    __test__.shouldEndLiveActivity(value, new Date("2026-09-01T11:47:00.000Z")),
    false
  );
  assert.equal(
    __test__.shouldEndLiveActivity(value, new Date("2026-09-01T12:02:00.000Z")),
    true
  );
});
