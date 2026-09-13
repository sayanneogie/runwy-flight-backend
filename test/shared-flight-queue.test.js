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

test("queue history stays bounded while delayed work still executes", async () => {
  const queue = createSharedFlightQueue({ maxHistory: 5 });
  const executed = [];
  queue.process('work', async (job) => executed.push(job.id));
  for (let i = 0; i < 30; i++) await queue.add('work', { flight_instance_id: `f${i}` }, { delayMs: 5 });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(executed.length, 30);
  assert.equal(new Set(executed).size, 30);
  assert.equal(queue.jobs.length, 5);
});
