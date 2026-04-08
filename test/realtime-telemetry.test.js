const test = require("node:test");
const assert = require("node:assert/strict");

const { mergeRealtimeTelemetry } = require("../src/realtime-telemetry");

test("mergeRealtimeTelemetry preserves previous live position when next refresh omits it", () => {
  const previous = {
    status: "enroute",
    progressPercent: 41,
    livePosition: {
      latitude: 12.9,
      longitude: 77.5,
      headingDegrees: 82,
      groundSpeedKnots: 428,
      altitudeFeet: 33000,
      recordedAt: "2026-03-18T10:00:00.000Z",
    },
    trackPoints: [
      {
        latitude: 12.4,
        longitude: 77.1,
        recordedAt: "2026-03-18T09:45:00.000Z",
      },
      {
        latitude: 12.9,
        longitude: 77.5,
        recordedAt: "2026-03-18T10:00:00.000Z",
      },
    ],
    lastUpdated: "2026-03-18T10:00:00.000Z",
  };

  const next = {
    status: "scheduled",
    progressPercent: null,
    livePosition: null,
    lastUpdated: "2026-03-18T10:00:05.000Z",
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.deepEqual(merged.livePosition, previous.livePosition);
  assert.deepEqual(merged.trackPoints, previous.trackPoints);
  assert.equal(merged.progressPercent, 41);
  assert.equal(merged.lastUpdated, "2026-03-18T10:00:05.000Z");
});

test("mergeRealtimeTelemetry prefers the newer live position", () => {
  const previous = {
    status: "enroute",
    progressPercent: 41,
    livePosition: {
      latitude: 12.9,
      longitude: 77.5,
      recordedAt: "2026-03-18T10:00:00.000Z",
    },
    lastUpdated: "2026-03-18T10:00:00.000Z",
  };

  const next = {
    status: "enroute",
    progressPercent: 44,
    livePosition: {
      latitude: 13.2,
      longitude: 77.9,
      recordedAt: "2026-03-18T10:00:30.000Z",
    },
    trackPoints: [
      {
        latitude: 12.9,
        longitude: 77.5,
        recordedAt: "2026-03-18T10:00:00.000Z",
      },
      {
        latitude: 13.2,
        longitude: 77.9,
        recordedAt: "2026-03-18T10:00:30.000Z",
      },
    ],
    lastUpdated: "2026-03-18T10:00:30.000Z",
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.deepEqual(merged.livePosition, next.livePosition);
  assert.deepEqual(merged.trackPoints, next.trackPoints);
  assert.equal(merged.progressPercent, 44);
  assert.equal(merged.lastUpdated, "2026-03-18T10:00:30.000Z");
});

test("mergeRealtimeTelemetry clears live position for terminal states", () => {
  const previous = {
    status: "enroute",
    progressPercent: 88,
    livePosition: {
      latitude: 12.9,
      longitude: 77.5,
      recordedAt: "2026-03-18T10:00:00.000Z",
    },
    trackPoints: [
      {
        latitude: 12.4,
        longitude: 77.1,
        recordedAt: "2026-03-18T09:45:00.000Z",
      },
      {
        latitude: 12.9,
        longitude: 77.5,
        recordedAt: "2026-03-18T10:00:00.000Z",
      },
    ],
    lastUpdated: "2026-03-18T10:00:00.000Z",
  };

  const next = {
    status: "landed",
    progressPercent: null,
    livePosition: null,
    arrivalTimes: {
      actual: "2026-03-18T10:12:00.000Z",
    },
    lastUpdated: "2026-03-18T10:12:00.000Z",
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.equal(merged.livePosition, null);
  assert.deepEqual(merged.trackPoints, previous.trackPoints);
  assert.equal(merged.progressPercent, 100);
  assert.equal(merged.lastUpdated, "2026-03-18T10:12:00.000Z");
});
