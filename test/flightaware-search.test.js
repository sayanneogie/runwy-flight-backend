const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";
process.env.PROVIDER_CALLS_ENABLED = "true";
process.env.DISABLE_PROVIDER_CALLS = "false";
process.env.FLIGHT_DATA_PROVIDER = "flightaware";
process.env.FLIGHTAWARE_API_KEY = "test-flightaware-key";

const { __test__ } = require("../src/server.js");

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

test("flightAwareScheduleQueryItems scopes a known flight to its resolved airports", () => {
  const params = __test__.flightAwareScheduleQueryItems({
    flightNumber: "AI101",
    departureIata: "del",
    arrivalIata: "blr",
  });

  assert.equal(params.get("airline"), "AI");
  assert.equal(params.get("flight_number"), "101");
  assert.equal(params.get("origin"), "DEL");
  assert.equal(params.get("destination"), "BLR");
  assert.equal(params.get("max_pages"), "5");
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
    assert.equal(providerCalls, 2);
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
