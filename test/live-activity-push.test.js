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
  const state = __test__.liveActivityContentState(value);
  assert.equal(state.phase, "cruise");
  assert.equal(state.progress, 0.5);
  assert.equal(state.departureDelayMinutes, 9);
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
