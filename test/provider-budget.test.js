"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { reserveProviderBudget } = require("../src/provider-budget");
const options = { endpoint: 'aeroapi:flight:tracked_flight', tracked: true, limit: 10, units: 1, path: '/flights/id', reason: 'test' };

test("budget reservation locks before counting and commits before returning", async () => {
  const steps = [];
  const client = { query: async (sql) => {
    steps.push(sql);
    return { rows: sql.includes('sum(') ? [{ units: 9 }] : sql.includes('returning id') ? [{ id: 'reservation' }] : [] };
  }, release: () => steps.push('release') };
  assert.equal(await reserveProviderBudget({ connect: async () => client }, options), 'reservation');
  assert.equal(steps[0], 'begin');
  assert.ok(steps[1].includes('pg_advisory_xact_lock'));
  assert.ok(steps[2].includes('sum('));
  assert.ok(steps[3].includes("'reserved'"));
  assert.deepEqual(steps.slice(-2), ['commit', 'release']);
});

test("an exhausted budget rolls back without inserting a reservation", async () => {
  const steps = [];
  const client = { query: async (sql) => { steps.push(sql); return { rows: [{ units: 10 }] }; }, release: () => steps.push('release') };
  await assert.rejects(reserveProviderBudget({ connect: async () => client }, options), { code: 'FLIGHTAWARE_DAILY_BUDGET_EXHAUSTED', statusCode: 429 });
  assert.ok(!steps.some((sql) => sql.includes('insert into')));
  assert.deepEqual(steps.slice(-2), ['rollback', 'release']);
});
