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

test("mergeRealtimeTelemetry preserves gates and terminals when a later snapshot omits them", () => {
  const previous = {
    status: "enroute",
    departureGate: "A6",
    departureTerminal: "3",
    arrivalGate: "C23",
    arrivalTerminal: "1",
    gate: "A6",
    terminal: "3",
  };
  const next = {
    status: "enroute",
    departureGate: null,
    departureTerminal: "3",
    arrivalGate: null,
    arrivalTerminal: "1",
    gate: null,
    terminal: "3",
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.equal(merged.departureGate, "A6");
  assert.equal(merged.arrivalGate, "C23");
  assert.equal(merged.gate, "A6");
  assert.equal(merged.departureTerminal, "3");
  assert.equal(merged.arrivalTerminal, "1");
});

test("mergeRealtimeTelemetry preserves an inbound aircraft assignment when a later snapshot omits it", () => {
  const inboundFlight = {
    providerFlightId: "QTR17-20260904-instance",
    flightNumber: "QR17",
    originAirportIata: "DOH",
    destinationAirportIata: "DUB",
    estimatedArrival: "2026-09-04T13:55:00.000Z",
    status: "enroute",
  };

  const merged = mergeRealtimeTelemetry(
    { status: "scheduled", inboundFlight },
    { status: "scheduled", inboundFlight: null }
  );

  assert.deepEqual(merged.inboundFlight, inboundFlight);
});

test("mergeRealtimeTelemetry fills a sparse inbound assignment from its last resolved details", () => {
  const previousInbound = {
    providerFlightId: "QTR17-20260904-instance",
    flightNumber: "QR17",
    originAirportIata: "DOH",
    destinationAirportIata: "DUB",
    estimatedArrival: "2026-09-04T13:55:00.000Z",
    status: "enroute",
  };

  const merged = mergeRealtimeTelemetry(
    { status: "scheduled", inboundFlight: previousInbound },
    {
      status: "scheduled",
      inboundFlight: {
        providerFlightId: "QTR17-20260904-instance",
        flightNumber: null,
        originAirportIata: null,
        destinationAirportIata: "DUB",
        estimatedArrival: null,
        status: null,
      },
    }
  );

  assert.equal(merged.inboundFlight.flightNumber, "QR17");
  assert.equal(merged.inboundFlight.originAirportIata, "DOH");
  assert.equal(merged.inboundFlight.estimatedArrival, "2026-09-04T13:55:00.000Z");
  assert.equal(merged.inboundFlight.status, "enroute");
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

test("mergeRealtimeTelemetry appends a newer partial track batch without dropping prior breadcrumbs", () => {
  const previous = {
    status: "enroute",
    trackPoints: [
      { latitude: 41.8, longitude: 12.2, recordedAt: "2026-09-02T14:00:00.000Z" },
      { latitude: 39.5, longitude: -20.1, recordedAt: "2026-09-02T16:00:00.000Z" },
      { latitude: 33.2, longitude: -49.3, recordedAt: "2026-09-02T19:00:00.000Z" },
    ],
  };
  const next = {
    status: "enroute",
    trackPoints: [
      { latitude: 25.5, longitude: -79.8, recordedAt: "2026-09-02T22:43:00.000Z" },
      { latitude: 25.8, longitude: -80.3, recordedAt: "2026-09-02T22:55:00.000Z" },
    ],
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.deepEqual(merged.trackPoints, [...previous.trackPoints, ...next.trackPoints]);
});

test("mergeRealtimeTelemetry rejects the provider zero-coordinate sentinel", () => {
  const merged = mergeRealtimeTelemetry(
    {
      status: "enroute",
      trackPoints: [
        { latitude: 41.8, longitude: 12.2, recordedAt: "2026-09-02T14:00:00.000Z" },
      ],
    },
    {
      status: "landed",
      trackPoints: [
        { latitude: 0, longitude: 0, recordedAt: "2026-09-02T22:56:00.000Z" },
        { latitude: 25.8, longitude: -80.3, recordedAt: "2026-09-02T22:57:00.000Z" },
      ],
    }
  );

  assert.equal(merged.trackPoints.length, 2);
  assert.equal(merged.trackPoints.some((point) => point.latitude === 0 && point.longitude === 0), false);
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

test("mergeRealtimeTelemetry keeps live tracking active through a diversion", () => {
  const previous = {
    status: "enroute",
    livePosition: {
      latitude: 46.1,
      longitude: -96.2,
      recordedAt: "2026-08-31T00:20:00.000Z",
    },
    progressPercent: 74,
  };
  const next = {
    status: "diverted",
    originalArrivalAirportIata: "BIS",
    diversionAirportIata: "FAR",
    livePosition: null,
    progressPercent: null,
    lastUpdated: "2026-08-31T00:21:00.000Z",
  };

  const merged = mergeRealtimeTelemetry(previous, next);

  assert.deepEqual(merged.livePosition, previous.livePosition);
  assert.equal(merged.progressPercent, 74);
  assert.equal(merged.diversionAirportIata, "FAR");
});
