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
  scheduled_arrival_at: "2026-08-07T14:30:00.000Z",
  departure_terminal: "2",
  departure_gate: "A4",
  status: "scheduled",
};

test("five-hour traveller reminder uses a time-aware greeting and origin-local departure time", () => {
  const event = {
    id: "event-id",
    event_type: "TRIP_STARTING",
    provider_event_time: "2026-08-07T05:00:00.000Z",
    new_value: {
      scheduledDepartureAt: "2026-08-07T10:00:00.000Z",
      weatherInsight: { temperatureC: 28, conditionCode: "Clear" },
    },
  };

  const payload = notificationPayload(flight, event, {
    isCircle: false,
    isTraveler: true,
    temperatureUnit: "fahrenheit",
  });

  assert.equal(payload.aps.alert.title, "BLR ✈️ SIN · 82°F ☀️");
  assert.match(payload.aps.alert.body, /^Good morning! Your flight today is on time\./);
  assert.match(payload.aps.alert.body, /AI 101/);
  assert.match(payload.aps.alert.body, /↗ BLR Terminal 2 · Gate A4 at 3:30 PM/);
  assert.match(payload.aps.alert.body, /↘ SIN at 10:30 PM/);
});

test("five-hour Circle reminder names the traveller", () => {
  const event = {
    id: "event-id",
    event_type: "TRIP_STARTING",
    provider_event_time: "2026-08-07T05:00:00.000Z",
    new_value: {
      scheduledDepartureAt: "2026-08-07T10:00:00.000Z",
      weatherInsight: { temperatureC: 28, conditionCode: "MostlyCloudy" },
    },
  };

  const payload = notificationPayload(flight, event, {
    isCircle: true,
    isTraveler: false,
    ownerDisplayName: "Maya Patel",
    recipientDisplayName: "Sayan Neogie",
    temperatureUnit: "celsius",
  });

  assert.equal(payload.aps.alert.title, "BLR ✈️ SIN · 28°C ☁️");
  assert.match(payload.aps.alert.body, /^Hey Sayan, today Maya has a flight from /);
  assert.match(payload.aps.alert.body, /3:30 PM local time/);
  assert.match(payload.aps.alert.body, /AI 101 · BLR → SIN/);
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

test("gate-change APNs carries the new gate for immediate client reconciliation", () => {
  const payload = notificationPayload(flight, {
    id: "gate-event",
    event_type: "GATE_CHANGED",
    old_value: { gate: "A4" },
    new_value: { gate: "B7" },
  });

  assert.equal(payload.flight_instance_id, "flight-id");
  assert.equal(payload.gate, "B7");
  assert.equal(payload.aps["content-available"], 1);
  assert.equal(payload.runwy.type, "flight_gate_change");
  assert.equal(payload.runwy.flightId, "flight-id");
  assert.equal(payload.runwy.flightInstanceId, "flight-id");
  assert.equal(payload.runwy.status, "scheduled");
  assert.equal(payload.runwy.departureGate, "B7");
  assert.equal(payload.runwy.gate, "B7");
});

test("taxiing APNs carries canonical state for app and Live Activity reconciliation", () => {
  const payload = notificationPayload(
    {
      ...flight,
      status: "taxiing",
      estimated_departure_at: "2026-08-07T10:12:00.000Z",
      last_fetched_at: "2026-08-07T10:05:00.000Z",
      normalized_data: {
        status: "taxiing",
        computedPhase: "taxi_out",
        departureTerminal: "2",
        departureGate: "C4",
        takeoffTimes: { actual: null },
      },
    },
    {
      id: "taxi-event",
      event_type: "TAXIING",
      new_value: { status: "taxiing" },
    }
  );

  assert.equal(payload.runwy.type, "flight_taxiing");
  assert.equal(payload.runwy.status, "taxiing");
  assert.equal(payload.runwy.computedPhase, "taxi_out");
  assert.equal(payload.runwy.departureTerminal, "2");
  assert.equal(payload.runwy.departureGate, "C4");
  assert.equal(payload.runwy.departureEstimatedAt, "2026-08-07T10:12:00.000Z");
  assert.equal(payload.runwy.lastUpdatedAt, "2026-08-07T10:05:00.000Z");
  assert.equal(payload.aps["content-available"], 1);
});

test("shared APNs routes through the user's tracking session", () => {
  const payload = notificationPayload(
    flight,
    {
      id: "gate-event",
      event_type: "GATE_CHANGED",
      old_value: { gate: "A4" },
      new_value: { gate: "B7" },
    },
    {
      userFlightId: "user-flight-id",
      trackingSessionId: "tracking-session-id",
    }
  );

  assert.equal(payload.flight_instance_id, "flight-id");
  assert.equal(payload.tracking_session_id, "tracking-session-id");
  assert.equal(payload.runwy.flightId, "tracking-session-id");
  assert.equal(payload.deep_link, "runwy://flights/tracking-session-id");
});

test("inbound takeoff notification names the last airport, departure city, and ETA", () => {
  const originalNow = Date.now;
  Date.now = () => Date.parse("2026-08-07T08:00:00.000Z");
  try {
    const event = {
      event_type: "INBOUND_DEPARTED",
      new_value: {
        inboundFlight: {
          flightNumber: "AI 202",
          originAirportIata: "DEL",
          estimatedArrival: "2026-08-07T10:15:00.000Z",
        },
      },
    };
    assert.equal(notificationTitle(flight, event), "✈️ Your Aircraft Is on the Way");
    assert.equal(
      notificationBody(flight, event),
      "AI 202 has taken off from DEL and is expected at BLR in 2h 15m."
    );
  } finally {
    Date.now = originalNow;
  }
});

test("inbound landing notification names the aircraft and the traveler's departure airport", () => {
  const event = {
    event_type: "INBOUND_ARRIVED",
    new_value: {
      inboundFlight: {
        flightNumber: "AI 202",
        originAirportIata: "DEL",
        destinationAirportIata: "BLR",
        status: "landed",
      },
    },
  };

  assert.equal(notificationTitle(flight, event), "✈️ Your Aircraft Has Landed");
  assert.equal(
    notificationBody(flight, event),
    "AI 202, the inbound aircraft for AI 101, has landed at BLR."
  );
});

test("traveller landing notifications use the rich destination welcome format", () => {
  const payload = notificationPayload(
    {
      ...flight,
      status: "landed",
      actual_arrival_at: "2026-08-07T14:40:00.000Z",
      normalized_data: {
        arrivalTimezone: "Asia/Singapore",
        arrivalTerminal: "1",
        arrivalGate: "A8",
        landingTimes: { actual: "2026-08-07T14:32:00.000Z" },
      },
    },
    { id: "landing-event", event_type: "LANDED", new_value: { status: "landed" } },
    {
      isCircle: false,
      isTraveler: true,
      temperatureUnit: "celsius",
      visitOrdinal: 4,
      weatherInsight: { available: true, temperatureC: 28, conditionCode: "Drizzle" },
    }
  );

  assert.equal(payload.aps.alert.title, "✈️ Welcome to Singapore! 🌧️ 28°");
  assert.match(payload.aps.alert.body, /Taxiing for 8m\./);
  assert.match(payload.aps.alert.body, /SIN • Terminal 1 • Gate A8/);
  assert.match(payload.aps.alert.body, /10:40 PM local time \(10m late\)/);
  assert.match(payload.aps.alert.body, /This is your 4th time here\./);
});
