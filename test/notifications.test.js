"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";
process.env.WEBHOOK_SHARED_SECRET = process.env.WEBHOOK_SHARED_SECRET || "test webhook secret";

const { __test__ } = require("../src/server.js");

test("test push payload is a visible fixed APNs alert", () => {
  assert.deepEqual(__test__.testPushNotificationPayload(), {
    aps: {
      alert: {
        title: "Runwy Test Notification",
        body: "Closed-app notifications are working.",
      },
      sound: "RunwyNotification.caf",
    },
    runwy: {
      type: "push_test",
    },
  });
});

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

test("inbound aircraft landing emits a pre-departure alert flag", () => {
  const alerts = __test__.deriveAlertFlags(
    {
      status: "scheduled",
      inboundFlight: {
        status: "enroute",
      },
    },
    {
      status: "scheduled",
      inboundFlight: {
        flightNumber: "AS533",
        status: "landed",
      },
    }
  );

  assert.equal(alerts.inboundArrivedNow, true);
  assert.equal(alerts.departedNow, false);
  assert.equal(alerts.arrivedNow, false);
});

test("delay increases emit notifications even when provider status stays scheduled", () => {
  const alerts = __test__.deriveAlertFlags(
    {
      status: "scheduled",
      delayMinutes: 0,
    },
    {
      status: "scheduled",
      delayMinutes: 24,
    }
  );

  assert.equal(alerts.delayedNow, true);
  assert.equal(alerts.departedNow, false);
});

test("delay notifications are suppressed for landed flights", () => {
  const alerts = __test__.deriveAlertFlags(
    {
      status: "enroute",
      delayMinutes: 12,
    },
    {
      status: "landed",
      delayMinutes: 24,
    }
  );

  assert.equal(alerts.delayedNow, false);
  assert.equal(alerts.arrivedNow, true);
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
  assert.equal(
    __test__.ownerNotificationPreferenceConditionForEventType("flight_takeoff_roll"),
    "coalesce((uf.alert_settings_json ->> 'takeoffLanding')::boolean, true) = true"
  );
  assert.equal(
    __test__.ownerNotificationPreferenceConditionForEventType("flight_taxiing"),
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
  assert.equal(
    __test__.circleNotificationPreferenceConditionForEventType("flight_inbound_arrived"),
    "fp.notify_departure = true"
  );
  assert.equal(
    __test__.circleNotificationPreferenceConditionForEventType("flight_takeoff_roll"),
    "fp.notify_departure = true"
  );
});

test("owner inbound-aircraft notifications honor takeoff and landing alert preferences", () => {
  assert.equal(
    __test__.ownerNotificationPreferenceConditionForEventType("flight_inbound_arrived"),
    "coalesce((uf.alert_settings_json ->> 'takeoffLanding')::boolean, true) = true"
  );
});

test("same-day FlightAware alerts use open windows before and after departure", () => {
  const today = "2026-04-26";

  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      {
        startDate: today,
        endDate: "2026-04-28",
        departureTime: `${today}T18:30:00.000Z`,
        timezoneOffsetMinutes: 0,
      },
      `${today}T08:00:00.000Z`
    ),
    {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "open",
    }
  );

  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      {
        startDate: today,
        endDate: "2026-04-28",
        departureTime: `${today}T06:30:00.000Z`,
        timezoneOffsetMinutes: 0,
      },
      `${today}T08:00:00.000Z`
    ),
    {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "open",
    }
  );

  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      {
        startDate: "2026-04-27",
        endDate: "2026-04-29",
        timezoneOffsetMinutes: 0,
      },
      `${today}T08:00:00.000Z`
    ),
    {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "bounded",
    }
  );
});

test("already-airborne overnight flights use open FlightAware alerts", () => {
  assert.deepEqual(
    __test__.flightAwareAlertCreationDisposition(
      {
        startDate: "2026-04-25",
        endDate: "2026-04-27",
        departureTime: "2026-04-25T22:30:00.000Z",
        timezoneOffsetMinutes: 0,
        status: "enroute",
      },
      "2026-04-26T08:00:00.000Z"
    ),
    {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "open",
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
      windowStrategy: "bounded",
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
  assert.equal(payload.events.out, true);
  assert.equal(payload.events.in, true);
});

test("FlightAware alert payload prefers the exact provider flight id", () => {
  const payload = __test__.buildFlightAwareAlertPayload({
    targetUrl: "https://runwy.example.com/v1/webhooks/flightaware?secret=test",
    context: {
      providerFlightId: "BAW276-1785473700-airline-0000",
      flightNumber: "BA276",
      departureIata: "HYD",
      arrivalIata: "LHR",
      startDate: "2026-07-31",
      endDate: "2026-08-02",
      windowStrategy: "open",
    },
  });

  assert.equal(payload.ident, "BAW276-1785473700-airline-0000");
  assert.equal(payload.events.on, true);
  assert.equal(payload.events.in, true);
});

test("existing FlightAware alerts are updated in place for configuration changes", async () => {
  const originalFetch = global.fetch;
  const requests = [];
  global.fetch = async (url, options = {}) => {
    requests.push({ url: String(url), options });
    return { ok: true, status: 200, text: async () => "{}" };
  };

  try {
    const result = await __test__.updateFlightAwareAlert({
      alertId: "alert-existing",
      targetUrl: "https://runwy.example.com/v1/webhooks/flightaware?secret=test",
      context: {
        providerFlightId: "AIC2418-instance",
        flightNumber: "AI2418",
        departureIata: "BLR",
        arrivalIata: "DEL",
        startDate: "2026-08-28",
        endDate: "2026-08-30",
        windowStrategy: "bounded",
      },
    });

    const update = requests.find((request) => request.url.endsWith("/alerts/alert-existing"));
    assert.equal(result.alertId, "alert-existing");
    assert.equal(update.options.method, "PUT");
    assert.equal(JSON.parse(update.options.body).events.out, true);
  } finally {
    global.fetch = originalFetch;
  }
});

test("FlightAware alert ids are extracted from scalar, array, and object responses", () => {
  assert.equal(__test__.flightAwareAlertIDFromPayload(12345), "12345");
  assert.equal(__test__.flightAwareAlertIDFromPayload("67890"), "67890");
  assert.equal(
    __test__.flightAwareAlertIDFromPayload({ alerts: [{ alert_id: 24680 }] }),
    "24680"
  );
  assert.equal(
    __test__.flightAwareAlertIDFromPayload([{ id: "alert-1" }]),
    "alert-1"
  );
});

test("FlightAware lifecycle webhooks bypass freshness throttling", () => {
  const tracked = {
    normalized: { status: "scheduled" },
    lastUpdated: new Date().toISOString(),
  };

  assert.equal(__test__.webhookStatusFromEvent({ event_code: "off" }), "departed");
  assert.equal(__test__.webhookStatusFromEvent({ event_code: "on" }), "landed");
  assert.equal(__test__.webhookStatusFromEvent({ event_code: "in" }), "arrived_at_gate");
  assert.equal(
    __test__.shouldRefreshTrackedRecordFromWebhook(tracked, { event_code: "off" }),
    true
  );
});

test("FlightAware alert callback URL includes webhook shared secret", () => {
  const targetUrl = __test__.flightAwareWebhookTargetURL({
    get(header) {
      if (header === "X-Forwarded-Host") return "runwy-api.example.com";
      if (header === "X-Forwarded-Proto") return "https";
      return "";
    },
  });

  const parsed = new URL(targetUrl);
  assert.equal(parsed.origin, "https://runwy-api.example.com");
  assert.equal(parsed.pathname, "/v1/webhooks/flightaware");
  assert.equal(parsed.searchParams.get("secret"), "test webhook secret");
});

test("same-day FlightAware alert payload omits date bounds for open window alerts", () => {
  const payload = __test__.buildFlightAwareAlertPayload({
    targetUrl: "https://runwy.example.com/v1/webhooks/flightaware?secret=test",
    context: {
      flightNumber: "SQ509",
      departureIata: "BLR",
      arrivalIata: "SIN",
      startDate: "2026-04-26",
      endDate: "2026-04-28",
      windowStrategy: "open",
    },
  });

  assert.equal(payload.ident, "SQ509");
  assert.equal(payload.origin, "BLR");
  assert.equal(payload.destination, "SIN");
  assert.equal(payload.target_url, "https://runwy.example.com/v1/webhooks/flightaware?secret=test");
  assert.ok(!("start" in payload));
  assert.ok(!("end" in payload));
});

test("empty FlightAware track trails preserve a fallback live position", () => {
  const livePosition = {
    latitude: 12.9716,
    longitude: 77.5946,
    altitudeFeet: 34000,
    heading: 12,
    groundspeedKnots: 431,
    recordedAt: "2026-04-27T11:32:00.000Z",
    source: "flightaware",
  };

  assert.deepEqual(
    __test__.coalesceFlightAwareTrackTrail(
      {
        trackPoints: [],
        livePosition: null,
      },
      livePosition
    ),
    {
      trackPoints: [],
      livePosition,
    }
  );
});

test("inbound aircraft notifications include arrival airport and departure countdown", () => {
  const departureTime = new Date(Date.now() + 76 * 60 * 1000).toISOString();
  const payload = __test__.notificationPayloadFor(
    {
      flightNumber: "AS533",
      airlineCode: "AS",
      departureAirportIata: "SEA",
      arrivalAirportIata: "LAX",
      departureTimes: {
        scheduled: null,
        estimated: departureTime,
        actual: null,
      },
      inboundFlight: {
        flightNumber: "AS204",
        status: "landed",
      },
      alerts: {
        inboundArrivedNow: true,
      },
    },
    "flight-id"
  );

  assert.equal(payload.aps.alert.title, "Inbound Aircraft Landed");
  assert.equal(payload.runwy.type, "flight_inbound_arrived");
  assert.match(payload.aps.alert.body, /has landed at SEA\./);
  assert.match(payload.aps.alert.body, /Departure is in 1h 16m\./);
});

test("arrival notifications include destination, weather, taxi, gate, timing, and visit count", () => {
  const payload = __test__.notificationPayloadFor(
    {
      flightNumber: "6E123",
      airlineCode: "6E",
      departureAirportIata: "DEL",
      arrivalAirportIata: "BLR",
      arrivalTimezone: "Asia/Kolkata",
      landingTimes: {
        actual: "2026-07-31T11:10:00.000Z",
      },
      arrivalTimes: {
        scheduled: "2026-07-31T11:35:00.000Z",
        estimated: "2026-07-31T11:18:00.000Z",
        actual: null,
      },
      arrivalTerminal: "1",
      arrivalGate: "A1",
      weatherInsight: {
        available: true,
        conditionCode: "PartlyCloudy",
        temperatureC: 28,
      },
      status: "landed",
      alerts: {
        arrivedNow: true,
      },
    },
    "flight-id",
    { visitOrdinal: 66 }
  );

  assert.equal(payload.aps.alert.title, "✈️ Welcome to Bengaluru! 🌤️ 28°");
  assert.match(payload.aps.alert.body, /Taxiing for 8m\./);
  assert.match(payload.aps.alert.body, /BLR • Terminal 1 • Gate A1/);
  assert.match(payload.aps.alert.body, /4:48 PM local time \(17m early\)/);
  assert.match(payload.aps.alert.body, /66th time here\./);
  assert.equal(payload.flight_instance_id, "flight-id");
  assert.equal(payload.deep_link, "runwy://flights/flight-id");
});

test("tracking-only arrival notifications use observer language without a welcome", () => {
  const payload = __test__.notificationPayloadFor(
    {
      flightNumber: "101",
      airlineCode: "AI",
      departureAirportIata: "FCO",
      arrivalAirportIata: "JFK",
      departureCity: "Rome",
      arrivalCity: "New York",
      arrivalTimezone: "America/New_York",
      arrivalTimes: {
        scheduled: "2026-08-07T20:44:00.000Z",
        estimated: "2026-08-07T20:44:00.000Z",
        actual: "2026-08-07T20:44:00.000Z",
      },
      status: "landed",
      alerts: {
        arrivedNow: true,
      },
    },
    "flight-id",
    { isOwner: true, isTraveler: false }
  );

  assert.equal(payload.aps.alert.title, "✈️ Tracked Flight Landed");
  assert.equal(
    payload.aps.alert.body,
    "Flight AI 101, Rome to New York, that you were tracking has landed at 4:44 PM local time."
  );
  assert.equal(payload.runwy.trackingOnly, true);
  assert.doesNotMatch(payload.aps.alert.title, /Welcome/);
});

test("circle takeoff notifications identify the traveler and use the plane emoji", () => {
  const payload = __test__.notificationPayloadFor(
    {
      flightNumber: "AI101",
      airlineCode: "AI",
      departureAirportIata: "FCO",
      arrivalAirportIata: "JFK",
      departureCity: "Rome",
      arrivalCity: "New York",
      status: "airborne",
      alerts: { departedNow: true },
    },
    "flight-id",
    { isOwner: false, isTraveler: false, travelerName: "Maya Patel" }
  );

  assert.equal(payload.aps.alert.title, "✈️ Flight Took Off");
  assert.equal(payload.aps.alert.body, "Maya's flight AI 101, Rome to New York, is now in the air.");
});

test("landing and baggage assignment create separate notification events", () => {
  const events = __test__.notificationEventsFor(
    {
      flightNumber: "6E123",
      airlineCode: "6E",
      departureAirportIata: "DEL",
      arrivalAirportIata: "BLR",
      departureCity: "Delhi",
      arrivalCity: "Bengaluru",
      status: "landed",
      baggageClaim: "3",
      alerts: {
        arrivedNow: true,
        baggageBeltAssignedNow: true,
        gateChangedNow: true,
      },
    },
    "flight-id"
  );

  assert.deepEqual(
    events.map((event) => event.type),
    ["flight_arrived", "flight_baggage_claim"]
  );
  assert.equal(events[1].title, "🧳 Baggage Claim Assigned");
  assert.equal(events[1].body, "Your luggage for flight 6E 123, Delhi to Bengaluru will be on belt 3.");
  assert.equal(
    __test__.notificationDedupeKey("flight-id", events[0]),
    "arrival-welcome:flight-id"
  );
  assert.equal(
    __test__.notificationDedupeKey("flight-id", events[1]),
    "baggage:flight-id:3"
  );
});

test("circle baggage notifications identify the traveler and flight", () => {
  const events = __test__.notificationEventsFor(
    {
      flightNumber: "AI101",
      airlineCode: "AI",
      departureAirportIata: "FCO",
      arrivalAirportIata: "JFK",
      departureCity: "Rome",
      arrivalCity: "New York",
      status: "landed",
      baggageClaim: "6",
      alerts: { baggageBeltAssignedNow: true },
    },
    "flight-id",
    { isOwner: false, travelerName: "Maya Patel" }
  );

  assert.equal(events[0].title, "🧳 Baggage Claim Assigned");
  assert.equal(events[0].body, "Maya's luggage for flight AI 101, Rome to New York will be on belt 6.");
});

test("unstable baggage assignments are silent until the final arrival hour", () => {
  const now = Date.now();
  const normalized = {
    flightNumber: "EK379",
    airlineCode: "EK",
    departureCity: "Phuket",
    arrivalCity: "Dubai",
    status: "enroute",
    baggageClaim: "7",
    arrivalTimes: {
      estimated: new Date(now + 2 * 60 * 60_000).toISOString(),
    },
    alerts: { baggageBeltAssignedNow: true },
  };

  assert.deepEqual(__test__.notificationEventsFor(normalized, "flight-id"), []);

  normalized.arrivalTimes.estimated = new Date(now + 45 * 60_000).toISOString();
  const [event] = __test__.notificationEventsFor(normalized, "flight-id");
  assert.equal(event.type, "flight_baggage_claim");
  assert.equal(event.body, "Your luggage for flight EK 379, Phuket to Dubai will be on belt 7.");
});

test("reliable baggage reassignments name both the previous and new belt", () => {
  const [event] = __test__.notificationEventsFor(
    {
      flightNumber: "EK379",
      airlineCode: "EK",
      departureCity: "Phuket",
      arrivalCity: "Dubai",
      status: "landed",
      baggageClaim: "12",
      alerts: { baggageBeltAssignedNow: true },
    },
    "flight-id",
    { previousNotifiedBaggageBelt: "7" }
  );

  assert.equal(event.title, "🧳 Baggage Claim Changed");
  assert.equal(
    event.body,
    "Your luggage for flight EK 379, Phuket to Dubai changed from belt 7 to belt 12."
  );
  assert.equal(event.payload.runwy.previousBaggageBelt, "7");
});

test("arrival visit counts use readable ordinals", () => {
  assert.equal(__test__.ordinalNumber(1), "1st");
  assert.equal(__test__.ordinalNumber(2), "2nd");
  assert.equal(__test__.ordinalNumber(3), "3rd");
  assert.equal(__test__.ordinalNumber(11), "11th");
  assert.equal(__test__.ordinalNumber(66), "66th");
});
