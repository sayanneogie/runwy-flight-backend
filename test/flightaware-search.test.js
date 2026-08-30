const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";
process.env.PROVIDER_CALLS_ENABLED = "true";
process.env.DISABLE_PROVIDER_CALLS = "false";
process.env.FLIGHT_DATA_PROVIDER = "flightaware";
process.env.FLIGHTAWARE_API_KEY = "test-flightaware-key";

const { __test__ } = require("../src/server.js");

test("FlightAware reserves bounded capacity for user searches", () => {
  assert.equal(__test__.flightAwareDailyBudgetLimitForEndpoint("flight_instance"), 500);
  assert.equal(__test__.flightAwareDailyBudgetLimitForEndpoint("position"), 500);
  assert.equal(__test__.flightAwareDailyBudgetLimitForEndpoint("operational"), 600);
  assert.equal(__test__.flightAwareDailyBudgetLimitForEndpoint("schedules"), 600);
});

test("opening flight details does not bypass FlightAware caches", () => {
  assert.deepEqual(
    __test__.trackedProviderRefreshOptions({ includeLivePosition: true }),
    { forceRefresh: false }
  );
  assert.deepEqual(
    __test__.trackedProviderRefreshOptions({ forceProviderRefresh: true }),
    { forceRefresh: true }
  );
});

test("FlightAware search keeps schedule results when the operational source fails", async () => {
  const scheduled = {
    ident_iata: "AI101",
    origin_iata: "DEL",
    destination_iata: "JFK",
    scheduled_out: "2026-08-11T17:20:00Z",
  };

  const rows = await __test__.fetchFlightAwareSearchSources(
    [
      async function scheduleSource() {
        return [scheduled];
      },
      async function operationalSource() {
        throw new Error("Provider error (502)");
      },
    ],
    { flightNumber: "AI101", date: "2026-08-11" },
    { mergeAll: true }
  );

  assert.deepEqual(rows, [scheduled]);
});

test("FlightAware search falls back after its preferred source fails", async () => {
  const scheduled = {
    ident_iata: "AI101",
    origin_iata: "DEL",
    destination_iata: "JFK",
    scheduled_out: "2026-08-11T17:20:00Z",
  };

  const rows = await __test__.fetchFlightAwareSearchSources(
    [
      async function operationalSource() {
        throw new Error("Provider error (502)");
      },
      async function scheduleSource() {
        return [scheduled];
      },
    ],
    { flightNumber: "AI101", date: "2026-08-11" }
  );

  assert.deepEqual(rows, [scheduled]);
});

test("FlightAware search reports an outage when every source fails", async () => {
  await assert.rejects(
    __test__.fetchFlightAwareSearchSources(
      [
        async function operationalSource() {
          throw new Error("operational unavailable");
        },
        async function scheduleSource() {
          throw new Error("schedule unavailable");
        },
      ],
      { flightNumber: "AI101", date: "2026-08-11" },
      { mergeAll: true }
    ),
    /operational unavailable/
  );
});

test("extractFlightAwareSearchRows returns scheduled payload rows", () => {
  const payload = {
    scheduled: [
      { ident_iata: "6E6992", origin_iata: "DEL", destination_iata: "BOM" },
      { ident_iata: "6E6993", origin_iata: "BOM", destination_iata: "DEL" },
    ],
    num_pages: 1,
  };

  const rows = __test__.extractFlightAwareSearchRows(payload);

  assert.equal(rows.length, 2);
  assert.equal(rows[0].ident_iata, "6E6992");
  assert.equal(rows[1].ident_iata, "6E6993");
});

test("FlightAware IATA and ICAO copies collapse into one real occurrence", () => {
  const records = [
    {
      fa_flight_id: "DAL2307-20260830-schedule",
      ident_iata: "DL2307",
      ident: "DAL2307",
      origin_iata: "MSP",
      destination_iata: "BIS",
      scheduled_out: "2026-08-30T23:15:00Z",
      scheduled_in: "2026-08-31T00:45:00Z",
    },
    {
      fa_flight_id: "DAL2307-1788131700-airline",
      ident: "DAL2307",
      origin_iata: "MSP",
      destination_iata: "BIS",
      scheduled_out: "2026-08-30T23:15:00Z",
      scheduled_in: "2026-08-31T00:45:00Z",
    },
  ];

  const deduped = __test__.dedupeFlightAwareRecords(records, { flightNumber: "DL2307" });
  assert.equal(deduped.length, 1);
  assert.equal(deduped[0].ident_iata, "DL2307");
});

test("occurrence dedupe preserves two real same-day flights at different times", () => {
  const records = [
    {
      ident_iata: "AI101",
      origin_iata: "DEL",
      destination_iata: "JFK",
      scheduled_out: "2026-08-30T10:00:00Z",
    },
    {
      ident: "AIC101",
      origin_iata: "DEL",
      destination_iata: "JFK",
      scheduled_out: "2026-08-30T11:00:00Z",
    },
  ];

  assert.equal(
    __test__.dedupeFlightAwareRecords(records, { flightNumber: "AI101" }).length,
    2
  );
});

test("flightAwareScheduleQueryItems scopes the primary lookup and can open a codeshare fallback", () => {
  const params = __test__.flightAwareScheduleQueryItems({
    flightNumber: "AI101",
    departureIata: "del",
    arrivalIata: "blr",
  });

  assert.equal(params.get("airline"), "AI");
  assert.equal(params.get("flight_number"), "101");
  assert.equal(params.get("origin"), "DEL");
  assert.equal(params.get("destination"), "BLR");
  assert.equal(params.get("max_pages"), "1");

  const fallbackParams = __test__.flightAwareScheduleQueryItems(
    { flightNumber: "AA5091" },
    { includeAirline: false }
  );
  assert.equal(fallbackParams.get("airline"), null);
  assert.equal(fallbackParams.get("flight_number"), "5091");
});

test("FlightAware schedule search resolves a marketing flight filed under its regional operator", async () => {
  const originalFetch = global.fetch;
  const requestedURLs = [];
  global.fetch = async (url) => {
    const requestedURL = new URL(url);
    requestedURLs.push(requestedURL);
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          scheduled: requestedURL.searchParams.has("airline") ? [] : [
            {
              fa_flight_id: "JIA5091-20260830-schedule",
              ident: "JIA5091",
              ident_iata: "OH5091",
              codeshares: ["AAL5091"],
              codeshares_iata: ["AA5091"],
              origin_iata: "DCA",
              destination_iata: "CLE",
              scheduled_out: "2026-08-30T21:48:00Z",
            },
            {
              fa_flight_id: "OTHER5091-20260830-schedule",
              ident: "OTHER5091",
              ident_iata: "ZZ5091",
              origin_iata: "AAA",
              destination_iata: "BBB",
              scheduled_out: "2026-08-30T12:00:00Z",
            },
          ],
        };
      },
    };
  };

  const query = {
    flightNumber: "AA5091",
    date: "2026-08-30",
    timezoneOffsetMinutes: -240,
  };

  try {
    const rows = await __test__.fetchFlightAwareScheduleFlights(query);
    assert.equal(requestedURLs.length, 2);
    assert.equal(requestedURLs[0].searchParams.get("airline"), "AA");
    assert.equal(requestedURLs[1].searchParams.get("airline"), null);
    assert.equal(requestedURLs[1].searchParams.get("flight_number"), "5091");
    assert.equal(rows.length, 1);
    assert.equal(rows[0].fa_flight_id, "JIA5091-20260830-schedule");

    const normalized = __test__.normalizeWithContext(
      rows[0],
      rows,
      query,
      __test__.normalizeRecordFromFlightAware,
      null
    );
    assert.equal(normalized.flightNumber, "AA5091");
    assert.equal(normalized.airlineCode, "AA");
    assert.equal(normalized.departureAirportIata, "DCA");
    assert.equal(normalized.arrivalAirportIata, "CLE");
  } finally {
    global.fetch = originalFetch;
  }
});

test("airport-pair searches never call FlightAware", async () => {
  const originalFetch = global.fetch;
  let providerCalls = 0;
  global.fetch = async () => {
    providerCalls += 1;
    throw new Error("provider should not be called");
  };

  try {
    assert.deepEqual(
      await __test__.fetchFlightAwareOperationalFlights({
        date: "2026-08-27",
        departureIata: "FCO",
        arrivalIata: "JFK",
      }),
      []
    );
    assert.deepEqual(
      await __test__.fetchFlightAwareFlights({
        date: "2026-08-27",
        departureIata: "FCO",
        arrivalIata: "JFK",
      }),
      []
    );
    assert.equal(providerCalls, 0);
  } finally {
    global.fetch = originalFetch;
  }
});

test("forced FlightAware detail refresh bypasses a cached blank gate", async () => {
  const originalFetch = global.fetch;
  let providerCalls = 0;
  global.fetch = async () => {
    providerCalls += 1;
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          flights: [
            {
              fa_flight_id: "AIC9876-20260827",
              ident_iata: "AI9876",
              origin_iata: "DEL",
              destination_iata: "LHR",
              scheduled_out: "2026-08-27T08:00:00Z",
              gate_origin: providerCalls > 1 ? "24" : null,
            },
          ],
        };
      },
    };
  };

  const query = {
    flightNumber: "AI9876",
    date: "2026-08-27",
    departureIata: "DEL",
    arrivalIata: "LHR",
    timezoneOffsetMinutes: 330,
  };

  try {
    const first = await __test__.fetchFlightAwareFlights(query);
    const cached = await __test__.fetchFlightAwareFlights(query);
    const refreshed = await __test__.fetchFlightAwareFlights(query, { forceRefresh: true });

    assert.equal(first[0].gate_origin, null);
    assert.equal(cached[0].gate_origin, null);
    assert.equal(refreshed[0].gate_origin, "24");
    // A forced refresh reruns both the operational and schedule sources; the
    // middle read must remain cache-only.
    assert.equal(providerCalls, 4);
  } finally {
    global.fetch = originalFetch;
  }
});

test("the retired route endpoint returns Gone without provider access", async () => {
  const { app } = require("../src/server.js");
  const layer = app._router.stack.find(
    (candidate) => candidate.route?.path === "/v1/search/route" && candidate.route?.methods?.get
  );
  assert.ok(layer, "expected the retired route endpoint to remain as a compatibility response");

  let statusCode = null;
  let payload = null;
  const response = {
    status(code) {
      statusCode = code;
      return this;
    },
    json(value) {
      payload = value;
      return this;
    },
  };

  await layer.route.stack[0].handle({}, response);
  assert.equal(statusCode, 410);
  assert.match(payload.error, /removed/i);
});

test("extractFlightAwareSearchRows deduplicates schedule and operational copies", () => {
  const rows = __test__.extractFlightAwareSearchRows({
    flights: [
      {
        fa_flight_id: "IGO2113-schedule-copy",
        ident_iata: "6E2113",
        origin_iata: "DEL",
        destination_iata: "BLR",
        scheduled_out: "2026-08-10T04:30:00Z",
      },
      {
        fa_flight_id: "IGO2113-operational-copy",
        ident_iata: "6E2113",
        origin_iata: "DEL",
        destination_iata: "BLR",
        scheduled_out: "2026-08-10T04:30:00Z",
      },
    ],
  });

  assert.equal(rows.length, 1);
});

test("extractFlightAwareSearchRows flattens airport-to-airport segment wrappers", () => {
  const rows = __test__.extractFlightAwareSearchRows({
    flights: [
      {
        segments: [
          {
            ident_iata: "6E2313",
            origin: { code_iata: "BLR" },
            destination: { code_iata: "DEL" },
            scheduled_out: "2026-08-09T18:00:00Z",
          },
        ],
      },
      {
        segments: [
          {
            ident_iata: "AI2820",
            origin: { code_iata: "BLR" },
            destination: { code_iata: "DEL" },
            scheduled_out: "2026-08-09T19:10:00Z",
          },
        ],
      },
    ],
  });

  assert.equal(rows.length, 2);
  assert.equal(rows[0].ident_iata, "6E2313");
  assert.equal(rows[1].ident_iata, "AI2820");
});

test("normalizeRecordFromFlightAware keeps schedule IATA fields", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident: "IGO6992",
    ident_iata: "6E6992",
    scheduled_out: "2026-03-24T10:30:00Z",
    scheduled_in: "2026-03-24T12:40:00Z",
    origin: "VIDP",
    origin_iata: "DEL",
    destination: "VABB",
    destination_iata: "BOM",
    aircraft_type: "A20N",
  });

  assert.equal(normalized.flightNumber, "6E6992");
  assert.equal(normalized.departureAirportIata, "DEL");
  assert.equal(normalized.arrivalAirportIata, "BOM");
  assert.equal(normalized.aircraftType, "A20N");
  assert.equal(normalized.departureTimes.scheduled, "2026-03-24T10:30:00.000Z");
  assert.equal(normalized.arrivalTimes.scheduled, "2026-03-24T12:40:00.000Z");
});

test("FlightAware airline parsing does not consume the first flight-number digit", () => {
  const airIndia = __test__.normalizeRecordFromFlightAware({
    ident_iata: "AI2015",
    origin_iata: "DEL",
    destination_iata: "LHR",
  });
  const indigo = __test__.normalizeRecordFromFlightAware({
    ident_iata: "6E609",
    origin_iata: "IXB",
    destination_iata: "DEL",
  });

  assert.equal(airIndia.airlineCode, "AI");
  assert.equal(indigo.airlineCode, "6E");
});

test("normalizeRecordFromFlightAware derives delay from actual departure", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident_iata: "TG324",
    scheduled_out: "2026-04-29T06:10:00Z",
    actual_out: "2026-04-29T06:18:00Z",
    origin_iata: "DEL",
    destination_iata: "BKK",
  });

  assert.equal(normalized.delayMinutes, 8);
});

test("normalizeRecordFromFlightAware reads FlightAware gate and terminal aliases", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident_iata: "6E508",
    scheduled_out: "2026-06-13T05:55:00Z",
    scheduled_in: "2026-06-13T08:25:00Z",
    origin_iata: "BLR",
    destination_iata: "RDP",
    gateOut: "17",
    terminalOut: "T1",
    gateIn: "--",
    terminalIn: "MAIN",
  });

  assert.equal(normalized.departureGate, "17");
  assert.equal(normalized.departureTerminal, "T1");
  assert.equal(normalized.gate, "17");
  assert.equal(normalized.terminal, "T1");
  assert.equal(normalized.arrivalGate, "--");
  assert.equal(normalized.arrivalTerminal, "MAIN");
});

test("normalizeRecordFromFlightAware reads nested airport operational fields", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident_iata: "6E174",
    origin: { code_iata: "PNQ", gate: "7", terminal: "1" },
    destination: { code_iata: "BLR", gate: "C3", terminal: "2" },
  });

  assert.equal(normalized.departureGate, "7");
  assert.equal(normalized.departureTerminal, "1");
  assert.equal(normalized.arrivalGate, "C3");
  assert.equal(normalized.arrivalTerminal, "2");
});

test("normalizeRecordFromFlightAware preserves the exact inbound aircraft identity and ETA", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    fa_flight_id: "SQ509-instance",
    ident_iata: "SQ509",
    origin: { code_iata: "BLR" },
    destination: { code_iata: "SIN" },
    scheduled_out: "2026-08-31T10:00:00.000Z",
    scheduled_in: "2026-08-31T14:00:00.000Z",
    inbound_fa_flight_id: "AI202-instance",
    inbound_ident_iata: "AI202",
    inbound_origin_iata: "DEL",
    inbound_estimated_in: "2026-08-31T09:05:00.000Z",
    inbound_status: "Scheduled",
  });

  assert.equal(normalized.inboundFlight.providerFlightId, "AI202-instance");
  assert.equal(normalized.inboundFlight.flightNumber, "AI202");
  assert.equal(normalized.inboundFlight.originAirportIata, "DEL");
  assert.equal(normalized.inboundFlight.destinationAirportIata, "BLR");
  assert.equal(normalized.inboundFlight.estimatedArrival, "2026-08-31T09:05:00.000Z");
});

test("normalizeRecordFromFlightAware converts AeroAPI altitude hundreds to feet", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident_iata: "AI2418",
    origin: { code_iata: "BLR" },
    destination: { code_iata: "DEL" },
    last_position: {
      latitude: 16.13584,
      longitude: 77.81175,
      altitude: 378,
      groundspeed: 456,
    },
  });

  assert.equal(normalized.livePosition.altitudeFeet, 37_800);
  assert.equal(normalized.livePosition.groundSpeedKnots, 456);
});

test("normalizeRecordFromAviationstack preserves both airport operation sides", () => {
  const normalized = __test__.normalizeRecordFromAviationstack({
    flight: { iata: "6E174" },
    departure: { iata: "PNQ", gate: "7", terminal: "1" },
    arrival: { iata: "BLR", gate: "C3", terminal: "2", baggage: "A1" },
  });

  assert.equal(normalized.departureGate, "7");
  assert.equal(normalized.departureTerminal, "1");
  assert.equal(normalized.arrivalGate, "C3");
  assert.equal(normalized.arrivalTerminal, "2");
  assert.equal(normalized.baggageClaim, "A1");
  assert.equal(normalized.gate, "7");
  assert.equal(normalized.terminal, "1");
});

test("reconcileOperationalStatus does not depart future scheduled flights with estimated takeoff", () => {
  const normalized = __test__.normalizeRecordFromFlightAware({
    ident: "AIC2418",
    ident_iata: "AI2418",
    fa_flight_id: "AIC2418-1777979795-schedule-1141p",
    status: "Scheduled",
    scheduled_out: "2026-05-07T11:00:00Z",
    scheduled_off: "2026-05-07T11:10:00Z",
    estimated_off: "2026-05-07T11:10:00Z",
    scheduled_in: "2026-05-07T13:55:00Z",
    origin_iata: "BLR",
    destination_iata: "DEL",
  });

  const reconciled = __test__.reconcileOperationalStatus(normalized);

  assert.equal(reconciled.status, "scheduled");
  assert.equal(reconciled.departureTimes.scheduled, "2026-05-07T11:00:00.000Z");
  assert.equal(reconciled.takeoffTimes.estimated, "2026-05-07T11:10:00.000Z");
});

test("reconcileOperationalStatus keeps a live ground position in taxiing state", () => {
  const reconciled = __test__.reconcileOperationalStatus({
    status: "enroute",
    departureTimes: { actual: "2026-08-28T17:29:00.000Z" },
    takeoffTimes: { actual: null },
    livePosition: {
      latitude: 28.5562,
      longitude: 77.1,
      altitudeFeet: 0,
      groundSpeedKnots: 1,
    },
  });

  assert.equal(reconciled.status, "taxiing");
});

test("reconcileOperationalStatus rejects a premature landing while fresh telemetry is airborne", () => {
  const reconciled = __test__.reconcileOperationalStatus({
    status: "landed",
    landingTimes: { actual: new Date(Date.now() - 30_000).toISOString() },
    livePosition: {
      latitude: 29.75,
      longitude: -98.25,
      altitudeFeet: 10_708,
      groundSpeedKnots: 380,
      airGround: "A",
      recordedAt: new Date(Date.now() - 15_000).toISOString(),
    },
  });

  assert.equal(reconciled.status, "enroute");
});

test("reconcileOperationalStatus does not accept a future actual-arrival timestamp", () => {
  const reconciled = __test__.reconcileOperationalStatus({
    status: "enroute",
    landingTimes: { actual: new Date(Date.now() + 15 * 60_000).toISOString() },
  });

  assert.equal(reconciled.status, "enroute");
});

test("scoreCandidate matches schedule codeshares through actual_ident_iata", () => {
  const score = __test__.scoreCandidate(
    {
      ident_iata: "B64341",
      actual_ident_iata: "AA1504",
      scheduled_out: "2026-03-24T00:00:00Z",
      scheduled_in: "2026-03-24T01:23:00Z",
      origin_iata: "LGA",
      destination_iata: "DCA",
    },
    {
      flightNumber: "AA1504",
      date: "2026-03-24",
      departureIata: "LGA",
      arrivalIata: "DCA",
    },
    __test__.normalizeRecordFromFlightAware
  );

  assert.ok(score >= 10);
});

test("shouldPreferFlightAwareSchedules switches outside the live window", () => {
  const reference = Date.parse("2026-03-19T00:00:00Z");

  assert.equal(__test__.shouldPreferFlightAwareSchedules("2026-03-19", reference), false);
  assert.equal(__test__.shouldPreferFlightAwareSchedules("2026-03-20", reference), false);
  assert.equal(__test__.shouldPreferFlightAwareSchedules("2026-03-24", reference), true);
  assert.equal(__test__.shouldPreferFlightAwareSchedules("2026-03-14", reference), true);
});

test("isFutureFlightAwareQueryDate respects the request timezone boundary", () => {
  const reference = Date.parse("2026-03-19T18:30:00Z");

  assert.equal(__test__.isFutureFlightAwareQueryDate("2026-03-20", reference, 330), false);
  assert.equal(__test__.isFutureFlightAwareQueryDate("2026-03-21", reference, 330), true);
  assert.equal(__test__.isFutureFlightAwareQueryDate("2026-03-18", reference), false);
});

test("flightAwareOperationalBounds expands a local day into the correct UTC instants", () => {
  assert.deepEqual(__test__.flightAwareOperationalBounds("2026-04-23", 330), {
    start: "2026-04-22T18:30:00Z",
    end: "2026-04-23T18:29:59Z",
  });
  assert.equal(__test__.flightAwareOperationalBounds("bad-date", 330), null);
});

test("flightAwareOccurrenceBounds covers an origin-local day before the origin timezone is known", () => {
  assert.deepEqual(
    __test__.flightAwareOccurrenceBounds({
      flightNumber: "AA258",
      date: "2026-08-04",
      timezoneOffsetMinutes: 330,
    }),
    {
      start: "2026-08-03T10:00:00Z",
      end: "2026-08-05T11:59:59Z",
    }
  );

  assert.deepEqual(
    __test__.flightAwareOccurrenceBounds({
      flightNumber: "AA258",
      departureIata: "PHL",
      date: "2026-08-04",
      timezoneOffsetMinutes: -240,
    }),
    {
      start: "2026-08-04T04:00:00Z",
      end: "2026-08-05T03:59:59Z",
    }
  );
});

test("scoreCandidate prioritizes an airborne adjacent-date occurrence over a scheduled match", () => {
  const query = { flightNumber: "AA258", date: "2026-08-05" };
  const airborneScore = __test__.scoreCandidate(
    {
      ident_iata: "AA258",
      status: "En Route",
      scheduled_out: "2026-08-05T01:05:00Z",
      scheduled_in: "2026-08-05T06:50:00Z",
      origin_iata: "PHL",
      destination_iata: "LIS",
    },
    query,
    __test__.normalizeRecordFromFlightAware
  );
  const scheduledScore = __test__.scoreCandidate(
    {
      ident_iata: "AA258",
      status: "Scheduled",
      scheduled_out: "2026-08-05T21:05:00Z",
      scheduled_in: "2026-08-06T02:50:00Z",
      origin_iata: "PHL",
      destination_iata: "LIS",
    },
    query,
    __test__.normalizeRecordFromFlightAware
  );

  assert.ok(airborneScore > scheduledScore);
});

test("flightAwareHistoryBounds widens to UTC day coverage for the selected local day", () => {
  assert.deepEqual(__test__.flightAwareHistoryBounds("2026-04-23", 330), {
    start: "2026-04-22",
    end: "2026-04-24",
  });
  assert.equal(__test__.flightAwareHistoryBounds("bad-date"), null);
});

test("classifyFlightAwareAuthProbeResult flags invalid credentials", () => {
  const result = __test__.classifyFlightAwareAuthProbeResult({
    statusCode: 401,
    checkedAt: "2026-03-19T00:00:00.000Z",
  });

  assert.equal(result.ok, false);
  assert.equal(result.state, "invalid_credentials");
  assert.equal(result.statusCode, 401);
});

test("classifyFlightAwareAuthProbeResult treats 404 as auth accepted", () => {
  const result = __test__.classifyFlightAwareAuthProbeResult({
    statusCode: 404,
    checkedAt: "2026-03-19T00:00:00.000Z",
  });

  assert.equal(result.ok, true);
  assert.equal(result.state, "ok");
  assert.equal(result.statusCode, 404);
});

test("classifyFlightAwareAuthProbeResult preserves timeout failures", () => {
  const result = __test__.classifyFlightAwareAuthProbeResult({
    checkedAt: "2026-03-19T00:00:00.000Z",
    error: new Error("The operation was aborted due to timeout"),
  });

  assert.equal(result.ok, null);
  assert.equal(result.state, "timeout");
});

test("healthBuildInfo exposes schedule-aware search marker", () => {
  const buildInfo = __test__.healthBuildInfo();

  assert.equal(buildInfo.version, "1.0.0");
  assert.equal(buildInfo.features.scheduleAwareSearch, true);
  assert.equal(buildInfo.features.scheduleWindowHours, 48);
  assert.ok(typeof buildInfo.startedAt === "string");
});
