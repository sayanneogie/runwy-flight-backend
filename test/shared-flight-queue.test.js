"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { createSharedFlightQueue } = require("../src/shared-flight/queue");

test("background job failures are contained and release their dedupe key", async () => {
  const failures = [];
  const queue = createSharedFlightQueue({
    onError(error, job) {
      failures.push({ error, job });
    },
  });

  queue.process("failingJob", async () => {
    throw new Error("database constraint rejected event");
  });

  const first = await queue.add(
    "failingJob",
    { flight_instance_id: "flight-1" },
    { dedupe: true, dedupeKey: "failing:flight-1" }
  );
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(failures.length, 1);
  assert.equal(failures[0].error.message, "database constraint rejected event");
  assert.equal(failures[0].job.id, first.id);

  const retry = await queue.add(
    "failingJob",
    { flight_instance_id: "flight-1" },
    { dedupe: true, dedupeKey: "failing:flight-1", runImmediately: false }
  );
  assert.equal(retry.deduped, undefined);
});
