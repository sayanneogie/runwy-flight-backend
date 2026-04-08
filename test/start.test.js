"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { resolveEntrypoint } = require("../src/start.js");

test("defaults to API server", () => {
  assert.deepEqual(resolveEntrypoint(), {
    modulePath: "server.js",
    label: "API server",
  });
});

test("selects Firehose worker when firehose is enabled", () => {
  assert.deepEqual(resolveEntrypoint({ firehoseEnabled: true }), {
    modulePath: "flight-firehose.js",
    label: "Firehose worker",
  });
});

test("selects Firehose worker for the Railway firehose service", () => {
  assert.deepEqual(
    resolveEntrypoint({ serviceName: "runwy-flight-firehose" }),
    {
      modulePath: "flight-firehose.js",
      label: "Firehose worker",
    }
  );
});

test("selects poller worker for the Railway poller service", () => {
  assert.deepEqual(resolveEntrypoint({ serviceName: "runwy-flight-poller" }), {
    modulePath: "flight-poller.js",
    label: "tracking poller",
  });
});
