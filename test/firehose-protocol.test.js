const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildFirehoseInitCommand,
  firehoseMessageFlightNumber,
  firehoseMessageProviderFlightId,
  firehoseMessageTimestampMs,
  firehoseMessageType,
  isFirehoseErrorMessage,
  isFirehoseKeepaliveMessage,
  parseFirehoseJSONLine,
} = require("../src/firehose-protocol");

test("buildFirehoseInitCommand includes core command parts", () => {
  const command = buildFirehoseInitCommand({
    timeMode: "pitr 1710670800",
    version: "36.0",
    username: "demo-user",
    password: "demo-pass",
    userAgent: "runwy-firehose",
    keepaliveSeconds: 60,
    events: ["flifo", "departure", "position"],
    idents: ["EK517", "AI203"],
    minSecondsBetweenAirborne: 15,
  });

  assert.match(command, /^pitr 1710670800 version 36\.0 username demo-user password demo-pass/);
  assert.match(command, /events "flifo departure position"/);
  assert.match(command, /idents "AI203 EK517"|idents "EK517 AI203"/);
  assert.match(command, /min_seconds_between_airborne 15/);
  assert.ok(command.endsWith("\n"));
});

test("buildFirehoseInitCommand removes incompatible extended data events when flifo is present", () => {
  const command = buildFirehoseInitCommand({
    username: "demo-user",
    password: "demo-pass",
    events: ["flifo", "extendedFlightInfo", "flightplan", "position"],
  });

  assert.match(command, /events "flifo position"/);
  assert.doesNotMatch(command, /extendedFlightInfo/);
  assert.doesNotMatch(command, /flightplan/);
});

test("buildFirehoseInitCommand accepts whitespace-delimited event strings", () => {
  const command = buildFirehoseInitCommand({
    username: "demo-user",
    password: "demo-pass",
    events: "flifo departure arrival cancellation position",
    idents: "ai2904 aic2904 ai2484",
  });

  assert.match(command, /events "flifo departure arrival cancellation position"/);
  assert.match(command, /idents "AI2904 AIC2904 AI2484"|idents "AI2904 AI2484 AIC2904"|idents "AIC2904 AI2904 AI2484"|idents "AIC2904 AI2484 AI2904"|idents "AI2484 AI2904 AIC2904"|idents "AI2484 AIC2904 AI2904"/);
});

test("parseFirehoseJSONLine handles position messages", () => {
  const message = parseFirehoseJSONLine(
    '{"pitr":"1591763773","type":"position","ident":"UEA2236","id":"UEA2236-1591584600-schedule-0391","clock":"1591763767","lat":"29.84762","lon":"104.34529"}'
  );

  assert.equal(firehoseMessageType(message), "position");
  assert.equal(firehoseMessageFlightNumber(message), "UEA2236");
  assert.equal(
    firehoseMessageProviderFlightId(message),
    "UEA2236-1591584600-schedule-0391"
  );
  assert.equal(firehoseMessageTimestampMs(message), 1591763767000);
  assert.equal(isFirehoseKeepaliveMessage(message), false);
  assert.equal(isFirehoseErrorMessage(message), false);
});

test("parseFirehoseJSONLine handles keepalive and invalid JSON", () => {
  const keepalive = parseFirehoseJSONLine('{"type":"keepalive","pitr":"1589808413"}');
  assert.equal(isFirehoseKeepaliveMessage(keepalive), true);
  assert.equal(parseFirehoseJSONLine("not-json"), null);
});
