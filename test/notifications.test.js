"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";

const { __test__ } = require("../src/server.js");

test("initial landed snapshots emit arrived notifications when the landing is recent", () => {
  const now = new Date().toISOString();

  const alerts = __test__.deriveAlertFlags(null, {
    status: "landed",
    landingTimes: { actual: now },
    arrivalTimes: { actual: null },
  });

  assert.equal(alerts.arrivedNow, true);
  assert.equal(alerts.departedNow, false);
});

test("initial in-air snapshots emit departure notifications when takeoff is recent", () => {
  const now = new Date().toISOString();

  const alerts = __test__.deriveAlertFlags(null, {
    status: "departed",
    takeoffTimes: { actual: now, estimated: null },
    departureTimes: { actual: null },
    arrivalTimes: { actual: null },
  });

  assert.equal(alerts.departedNow, true);
  assert.equal(alerts.arrivedNow, false);
});

test("owner arrival notifications honor takeoff and landing alert preferences", () => {
  assert.equal(
    __test__.ownerNotificationPreferenceConditionForEventType("flight_arrived"),
    "coalesce((uf.alert_settings_json ->> 'takeoffLanding')::boolean, true) = true"
  );
  assert.equal(
    __test__.ownerNotificationPreferenceConditionForEventType("flight_departed"),
    "coalesce((uf.alert_settings_json ->> 'takeoffLanding')::boolean, true) = true"
  );
});

test("flight circle recipients honor departure and arrival alert toggles", () => {
  assert.equal(
    __test__.circleNotificationPreferenceConditionForEventType("flight_departed"),
    "fp.notify_departure = true"
  );
  assert.equal(
    __test__.circleNotificationPreferenceConditionForEventType("flight_arrived"),
    "fp.notify_arrival = true"
  );
});

test("same-day FlightAware alerts are skipped while future alerts remain eligible", () => {
  const today = "2026-04-26";

  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      { startDate: today, endDate: "2026-04-28" },
      `${today}T08:00:00.000Z`
    ),
    {
      eligible: false,
      reason: "start_date_not_in_future",
      detail: `Skipping FlightAware alert auto-create because start date ${today} is not after current UTC date ${today}.`,
    }
  );

  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      { startDate: "2026-04-27", endDate: "2026-04-29" },
      `${today}T08:00:00.000Z`
    ),
    {
      eligible: true,
      reason: null,
      detail: null,
    }
  );
});

test("FlightAware alert payload uses canonical ident/origin/destination keys", () => {
  const payload = __test__.buildFlightAwareAlertPayload({
    targetUrl: "https://runwy.example.com/v1/webhooks/flightaware?secret=test",
    context: {
      flightNumber: "AI2418",
      departureIata: "DEL",
      arrivalIata: "BOM",
      startDate: "2026-04-27",
      endDate: "2026-04-29",
    },
  });

  assert.equal(payload.ident, "AI2418");
  assert.equal(payload.origin, "DEL");
  assert.equal(payload.destination, "BOM");
  assert.equal(payload.start, "2026-04-27");
  assert.equal(payload.end, "2026-04-29");
  assert.equal(payload.target_url, "https://runwy.example.com/v1/webhooks/flightaware?secret=test");
  assert.ok(!("ident_iata" in payload));
  assert.ok(!("origin_iata" in payload));
  assert.ok(!("destination_iata" in payload));
});
