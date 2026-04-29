const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ALLOW_INSECURE_NO_AUTH = "true";
process.env.PROVIDER_CALLS_ENABLED = "true";
process.env.DISABLE_PROVIDER_CALLS = "false";
process.env.FLIGHT_DATA_PROVIDER = "flightaware";
process.env.FLIGHTAWARE_API_KEY = "test-flightaware-key";

const { __test__ } = require("../src/server.js");

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
