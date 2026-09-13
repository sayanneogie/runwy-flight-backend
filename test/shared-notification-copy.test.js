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

test("three-hour traveller reminder uses a time-aware greeting and origin-local departure time", () => {
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

  assert.equal(payload.aps.alert.title, "Right on schedule ✈️ 82°F ☀️");
  assert.match(payload.aps.alert.body, /^Your flight AI 101 is on time\./);
  assert.match(payload.aps.alert.body, /AI 101/);
  assert.match(payload.aps.alert.body, /↗ BLR T2 · Gate A4 at 3:30 PM/);
  assert.doesNotMatch(payload.aps.alert.body, /↘/);
});

test("three-hour Circle reminder names the traveller", () => {
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

  assert.equal(payload.aps.alert.title, "Maya has a flight today ✈️");
  assert.match(payload.aps.alert.body, /^Hey Sayan, Maya has a flight from /);
  assert.match(payload.aps.alert.body, /3:30 PM local time/);
  assert.doesNotMatch(payload.aps.alert.body, /AI 101 · BLR → SIN/);
});

test("shared takeoff, landing, and baggage notifications use the requested emojis", () => {
  assert.equal(notificationTitle(flight, { event_type: "TAKEOFF_ROLL" }), "And we're off ✈️");
  assert.equal(notificationTitle(flight, { event_type: "LANDED" }), "AI 101 has landed ✈️");
  assert.equal(
    notificationTitle(flight, { event_type: "BAGGAGE_BELT_ASSIGNED", old_value: null }),
    "Bags this way 🧳"
  );
  assert.equal(
    notificationBody(
      flight,
      { event_type: "BAGGAGE_BELT_ASSIGNED", old_value: null, new_value: { baggageBelt: "14" } },
      { isCircle: true, ownerDisplayName: "Maya Patel" }
    ),
    "Baggage for AI 101 is assigned to Belt 14"
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

  assert.equal(payload.aps.alert.title, "✈️ Welcome to Singapore. 🌧️ 28°");
  assert.match(payload.aps.alert.body, /Landed at SIN\./);
  assert.match(payload.aps.alert.body, /Taxiing to Gate A8/);
  assert.match(payload.aps.alert.body, /10 min late/);
  assert.match(payload.aps.alert.body, /This is your 4th time in Singapore/);
});

test("revised departure alerts use live details and departure-local times", () => {
  const demo = {
    ...flight, origin_airport: "DEL", destination_airport: "FCO",
    departure_terminal: "3", departure_gate: "A12",
    scheduled_departure_at: "2026-09-13T17:40:00Z",
    estimated_departure_at: "2026-09-13T18:50:00Z",
  };
  const cases = [
    ["DELAYED", {}, "Running a little late ⏱️", "AI 101 is delayed by 1h 10m. New departure: 12:20 AM"],
    ["CANCELLED", {}, "Flight canceled 😐", "AI 101 has been canceled. Contact airline support"],
    ["GATE_CHANGED", { gate: "A16" }, "Gate has changed ✈️", "Flight gate for AI 101 moved from A12 → A16."],
    ["TERMINAL_CHANGED", { terminal: "2" }, "Terminal switch ↗", "AI 101 is now departing from Terminal 2."],
    ["RESCHEDULED", {}, "A change of plans 🗓️", "AI 101 has a new departure time: 12:20 AM."],
    ["AIRCRAFT_CHANGED", { aircraftType: "A350-900" }, "New ride ✈️", "Aircraft changed: AI 101 is now flying on an A350-900."],
    ["BOARDING", {}, "Time to board 🎫", "AI 101 is boarding now at Gate A12."],
    ["TAXIING", {}, "Heading for the runway ✈️", "AI 101 is taxiing for takeoff."],
    ["TAKEOFF_ROLL", {}, "And we're off ✈️", "AI 101 is taking off for Rome."],
  ];
  for (const [type, value, title, body] of cases) {
    const event = { event_type: type, old_value: { gate: "A12" }, new_value: value };
    assert.equal(notificationTitle(demo, event), title);
    assert.equal(notificationBody(demo, event), body);
  }
  assert.equal(notificationBody({ ...demo, departure_gate: null }, { event_type: "BOARDING" }), "AI 101 is boarding now.");
  assert.equal(notificationBody(demo, { event_type: "GATE_CHANGED", new_value: { gate: "A16" } }), "Flight gate for AI 101 moved to A16.");
});

test("arrival messages distinguish Circle, tracking, gates, diversions and belts", () => {
  const demo = { ...flight, destination_airport: "FCO", normalized_data: { arrivalGate: "E12" } };
  const landed = { event_type: "LANDED" };
  const circle = { isCircle: true, ownerDisplayName: "Sayan Neogie" };
  assert.equal(notificationTitle(demo, landed, circle), "Sayan has landed ✈️");
  assert.equal(notificationBody(demo, landed, circle), "Sayan’s flight AI 101 has landed in Rome");
  assert.equal(notificationBody(demo, landed), "The flight you were tracking has landed in Rome.");
  assert.equal(notificationTitle(demo, landed, { isCircle: true }), "AI 101 has landed ✈️");
  const cases = [
    ["DIVERTED", { diversionAirport: "MXP" }, "Flight diverted 👀", "Your flight has been diverted to MXP (Milan)"],
    ["TAXI_IN", {}, "Heading to the gate", "AI 101 is taxiing to gate E12."],
    ["ARRIVED_AT_GATE", {}, "Journey complete ✨", "AI 101 has reached Gate E12."],
    ["BAGGAGE_BELT_ASSIGNED", { baggageBelt: "7" }, "Bags this way 🧳 · Belt 7", "Baggage for AI 101 is assigned to Belt 7"],
  ];
  for (const [type, value, title, body] of cases) {
    const event = { event_type: type, new_value: value };
    assert.equal(notificationTitle(demo, event), title);
    assert.equal(notificationBody(demo, event), body);
  }
  const changed = { event_type: "BAGGAGE_BELT_ASSIGNED", old_value: { baggageBelt: "7" }, new_value: { baggageBelt: "9" } };
  assert.equal(notificationTitle(demo, changed), "New baggage belt 🧳");
  assert.equal(notificationBody(demo, changed), "Head to Belt 9 instead; AI 101 baggage has been reassigned to Belt 9.");
  assert.equal(notificationBody({ ...demo, normalized_data: {} }, { event_type: "TAXI_IN" }), "AI 101 is taxiing to the gate.");
});

test("Circle wording uses member names without changing traveler alerts", () => {
  const context = { isCircle: true, ownerDisplayName: "Sayan Neogie", recipientDisplayName: "Alex Smith" };
  const cases = [
    ["BOARDING", "Sayan is now boarding for 🎫", "Sayan's flight AI 101 is boarding now."],
    ["CANCELLED", "Sayan's Flight is canceled 😬", "AI 101 has been canceled. Contact airline support"],
    ["TAXIING", "Heading for the runway ✈️", "AI 101 is taxiing for takeoff. Send em a safe-flight text"],
  ];
  for (const [type, title, body] of cases) {
    assert.equal(notificationTitle(flight, { event_type: type }, context), title);
    assert.equal(notificationBody(flight, { event_type: type }, context), body);
  }
  assert.equal(notificationTitle(flight, { event_type: "DELAYED" }, context), "Sayan's flight is delayed 😐");
  assert.match(notificationBody(flight, { event_type: "DELAYED" }, context), /^Sayan's flight AI 101 is delayed\. New departure:/);
  assert.equal(notificationBody(flight, { event_type: "DIVERTED", new_value: { diversionAirport: "MXP" } }, context), "Sayan's flight has been diverted to MXP (Milan).");
  assert.equal(notificationTitle(flight, { event_type: "CANCELLED" }), "Flight canceled 😐");
});

test("delay durations handle midnight, offsets, whole hours and unavailable times", () => {
  const demo = { ...flight, scheduled_departure_at: "2026-09-13T23:10:00+05:30" };
  for (const [estimated, duration] of [
    ["2026-09-14T00:20:00+05:30", "1h 10m"],
    ["2026-09-14T01:10:00+05:30", "2h"],
    ["2026-09-13T18:00:00Z", "20m"],
  ]) {
    const event = { event_type: "DELAYED", new_value: { estimatedDepartureAt: estimated } };
    assert.ok(notificationBody(demo, event).startsWith(`AI 101 is delayed by ${duration}.`));
    assert.ok(notificationBody(demo, event, { isCircle: true, ownerDisplayName: "Sayan Neogie" }).startsWith(`Sayan's flight AI 101 is delayed by ${duration}.`));
  }
  for (const estimated of [null, "invalid", "2026-09-13T17:00:00Z", demo.scheduled_departure_at]) {
    assert.doesNotMatch(notificationBody({ ...demo, estimated_departure_at: estimated }, { event_type: "DELAYED" }), /delayed by/);
  }
  assert.doesNotMatch(notificationBody({ ...demo, scheduled_departure_at: null, estimated_departure_at: "2026-09-14T00:20:00+05:30" }, { event_type: "DELAYED" }), /delayed by/);
});
