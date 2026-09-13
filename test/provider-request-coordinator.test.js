"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { createProviderRequestCoordinator, createResponseStore } = require("../src/provider-request-coordinator");
const url = "https://example.test/flights/AI101-occurrence/position";
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("independent workers share one provider response for the same endpoint", async () => {
  const store = createResponseStore();
  const workers = Array.from({ length: 6 }, () => createProviderRequestCoordinator({ store, wait: () => delay(1) }));
  let calls = 0;
  const load = async () => { calls++; await delay(15); return Response.json({ lat: 28 }); };
  const results = await Promise.all(workers.map((worker) => worker.request(url, { ttlMs: 1000, load })));
  assert.equal(calls, 1);
  for (const response of results) assert.deepEqual(await response.json(), { lat: 28 });
});

test("different dates/provider IDs and different endpoints never share data", async () => {
  const worker = createProviderRequestCoordinator();
  let calls = 0;
  for (const path of ["id-today", "id-tomorrow", "id-today/position", "id-today/track"]) {
    const response = await worker.request(`https://example.test/flights/${path}`, {
      ttlMs: 1000, load: async () => { calls++; return Response.json({ path }); },
    });
    assert.equal((await response.json()).path, path);
  }
  assert.equal(calls, 4);
});

test("forced event refresh cannot reuse a response started before the event", async () => {
  const worker = createProviderRequestCoordinator();
  let calls = 0;
  const load = async () => Response.json({ revision: ++calls });
  await worker.request(url, { ttlMs: 1000, load });
  await delay(5);
  const response = await worker.request(url, { ttlMs: 1000, forceRefresh: true, load });
  assert.equal((await response.json()).revision, 2);
});

test("expired responses are refreshed and cache hits preserve original expiry", async () => {
  const worker = createProviderRequestCoordinator();
  let calls = 0;
  const load = async () => Response.json({ revision: ++calls });
  await worker.request(url, { ttlMs: 40, load });
  const hit = await worker.request(url, { ttlMs: 1000, load });
  assert.ok(Date.parse(hit.headers.get("x-runwy-cache-expires-at")) < Date.now() + 100);
  await delay(50);
  assert.equal((await (await worker.request(url, { ttlMs: 1000, load })).json()).revision, 2);
});

test("failed and malformed provider responses are not cached", async () => {
  for (const loadBad of [() => new Response("error", { status: 503 }), () => Response.json({ error: "bad" }), () => new Response("not json")]) {
    const worker = createProviderRequestCoordinator();
    await worker.request(url, { ttlMs: 1000, load: loadBad });
    const good = await worker.request(url, { ttlMs: 1000, load: () => Response.json({ valid: true }) });
    assert.deepEqual(await good.json(), { valid: true });
  }
});

test("coordination timeout does not issue another paid request", async () => {
  let calls = 0;
  const worker = createProviderRequestCoordinator({ store: { read: async () => null, claim: async () => null }, waitMs: 1, wait: () => delay(2) });
  await assert.rejects(worker.request(url, { ttlMs: 1000, load: () => { calls++; } }), /still in progress/);
  assert.equal(calls, 0);
});
