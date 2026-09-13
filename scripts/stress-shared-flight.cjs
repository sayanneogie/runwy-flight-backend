"use strict";
// Deterministic, offline contention test. No provider calls or APNs requests.
const assert = require("node:assert/strict");
const { createProviderRequestCoordinator, createResponseStore } = require("../src/provider-request-coordinator");
const { createMemorySharedFlightRepository } = require("../src/shared-flight/repository");
const { normalizeSearchParams } = require("../src/shared-flight/state");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
(async () => {
  const store = createResponseStore();
  const workers = Array.from({ length: 20 }, () => createProviderRequestCoordinator({ store, wait: () => sleep(1) }));
  const purchases = new Map();
  const requests = Array.from({ length: 2000 }, (_, i) => {
    const occurrence = i % 40;
    const endpoint = i % 3;
    const url = `https://offline.test/flights/${occurrence}/${endpoint}`;
    return workers[i % workers.length].request(url, { ttlMs: 60_000, load: async () => {
      purchases.set(url, (purchases.get(url) || 0) + 1);
      await sleep(occurrence % 5);
      return Response.json({ occurrence, endpoint });
    } }).then(async (response) => assert.deepEqual(await response.json(), { occurrence, endpoint }));
  });
  await Promise.all(requests);
  assert.equal(purchases.size, 120);
  assert.ok([...purchases.values()].every((n) => n === 1));
  const repository = createMemorySharedFlightRepository();
  const params = normalizeSearchParams({ airline: "AI", number: "101", date: "2026-09-14", origin: "DEL", destination: "FCO" });
  let row = await repository.upsertFlightFromNormalized({ airlineCode: "AI", flightNumber: "101", origin: "DEL", destination: "FCO", providerFlightId: "exact", status: "scheduled" }, params, new Date().toISOString());
  for (let round = 0; round < 100; round++) {
    const competing = await Promise.all(Array.from({ length: 20 }, (_, i) => repository.commitCanonicalState({ ...row, gate: `G${round}-${i}` }, [], "test")));
    assert.equal(competing.filter((result) => result.applied).length, 1);
    row = await repository.findFlightById(row.id);
    assert.equal(row.state_revision, round + 1);
  }
  console.log("PASS: 2,000 requests across 20 workers / 120 endpoint-occurrences -> 120 provider loads; 100 rounds of 20 competing revisions -> one commit per round.");
})().catch((error) => { console.error(error); process.exitCode = 1; });
