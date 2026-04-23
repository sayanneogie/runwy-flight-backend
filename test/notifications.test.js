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
