"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  notificationBody,
  notificationPayload,
  notificationTitle,
} = require("../src/shared-flight/notifications");

const flight = {
  id: "flight-id",
  airline_code: "AI",
  flight_number: "101",
  origin_airport: "BLR",
  destination_airport: "SIN",
  scheduled_departure_at: "2026-08-07T10:00:00.000Z",
};

test("five-hour traveller reminder uses a time-aware greeting and origin-local departure time", () => {
  const event = {
    id: "event-id",
    event_type: "TRIP_STARTING",
    provider_event_time: "2026-08-07T05:00:00.000Z",
    new_value: { scheduledDepartureAt: "2026-08-07T10:00:00.000Z" },
  };

  const payload = notificationPayload(flight, event, { isCircle: false, isTraveler: true });

  assert.equal(payload.aps.alert.title, "Good morning ⛅️");
  assert.match(payload.aps.alert.body, /^You have a flight today\./);
  assert.match(payload.aps.alert.body, /AI 101/);
  assert.match(payload.aps.alert.body, /3:30 PM local time/);
});

test("five-hour Circle reminder names the traveller", () => {
  const event = {
    id: "event-id",
    event_type: "TRIP_STARTING",
    provider_event_time: "2026-08-07T05:00:00.000Z",
    new_value: { scheduledDepartureAt: "2026-08-07T10:00:00.000Z" },
  };

  const payload = notificationPayload(flight, event, {
    isCircle: true,
    isTraveler: false,
    ownerDisplayName: "Maya Patel",
  });

  assert.equal(payload.aps.alert.title, "✈️ Maya has a flight today");
  assert.match(payload.aps.alert.body, /^Maya has a flight scheduled for today\./);
  assert.match(payload.aps.alert.body, /3:30 PM local time/);
});

test("shared takeoff, landing, and baggage notifications use the requested emojis", () => {
  assert.equal(notificationTitle(flight, { event_type: "TAKEOFF_ROLL" }), "✈️ Taking Off");
  assert.equal(notificationTitle(flight, { event_type: "LANDED" }), "✈️ Flight Landed");
  assert.equal(
    notificationTitle(flight, { event_type: "BAGGAGE_BELT_ASSIGNED", old_value: null }),
    "🧳 Baggage Belt Assigned"
  );
  assert.equal(
    notificationBody(
      flight,
      { event_type: "BAGGAGE_BELT_ASSIGNED", old_value: null, new_value: { baggageBelt: "14" } },
      { isCircle: true, ownerDisplayName: "Maya Patel" }
    ),
    "Maya's luggage for flight AI 101 will be on belt 14."
  );
});
