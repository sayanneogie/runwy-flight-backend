const test = require("node:test");
const assert = require("node:assert/strict");

const {
  resolveFirehoseTimeMode,
  trackedRowBackfillStartMs,
} = require("../src/firehose-runtime");

function trackedRow(overrides = {}) {
  return {
    normalized: {
      status: "enroute",
      departureTimes: {
        actual: "2026-03-18T10:00:00.000Z",
      },
      takeoffTimes: null,
      trackPoints: [],
      ...overrides.normalized,
    },
    ...overrides,
  };
}

test("trackedRowBackfillStartMs starts near departure for active airborne flights with sparse history", () => {
  const nowMs = new Date("2026-03-18T12:00:00.000Z").getTime();
  const result = trackedRowBackfillStartMs(trackedRow(), {
    nowMs,
    maxBackfillHours: 8,
    preDepartureMinutes: 15,
    minTrackPoints: 8,
  });

  assert.equal(result, new Date("2026-03-18T09:45:00.000Z").getTime());
});

test("trackedRowBackfillStartMs skips flights that already have enough breadcrumb points", () => {
  const result = trackedRowBackfillStartMs(
    trackedRow({
      normalized: {
        status: "enroute",
        departureTimes: { actual: "2026-03-18T10:00:00.000Z" },
        trackPoints: Array.from({ length: 8 }, (_, index) => ({
          latitude: 10 + index,
          longitude: 20 + index,
          recordedAt: `2026-03-18T10:${String(index).padStart(2, "0")}:00.000Z`,
        })),
      },
    }),
    {
      nowMs: new Date("2026-03-18T12:00:00.000Z").getTime(),
      minTrackPoints: 8,
    }
  );

  assert.equal(result, null);
});

test("resolveFirehoseTimeMode prefers the earliest needed replay point", () => {
  const mode = resolveFirehoseTimeMode({
    lastGoodPitr: "1768797000",
    trackedRows: [
      trackedRow({
        normalized: {
          status: "enroute",
          departureTimes: { actual: "2026-01-19T01:00:00.000Z" },
          trackPoints: [],
        },
      }),
    ],
    nowMs: new Date("2026-01-19T03:00:00.000Z").getTime(),
    preDepartureMinutes: 15,
    minTrackPoints: 8,
    maxBackfillHours: 8,
  });

  assert.equal(mode, "pitr 1768783500");
});

test("resolveFirehoseTimeMode uses live mode when no replay is needed", () => {
  const mode = resolveFirehoseTimeMode({
    trackedRows: [
      trackedRow({
        normalized: {
          status: "scheduled",
          departureTimes: { scheduled: "2026-03-19T10:00:00.000Z" },
          trackPoints: [],
        },
      }),
    ],
    nowMs: new Date("2026-03-18T12:00:00.000Z").getTime(),
  });

  assert.equal(mode, "live");
});
