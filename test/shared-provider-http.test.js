"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");

test("real fetch path deduplicates status and track fallback without external traffic", async () => {
  const counts = new Map();
  const server = http.createServer((req, res) => {
    counts.set(req.url, (counts.get(req.url) || 0) + 1);
    res.setHeader("content-type", "application/json");
    if (req.url.endsWith("/position")) { res.statusCode = 404; res.end('{}'); return; }
    if (req.url.endsWith("/track")) { res.end(JSON.stringify([{ latitude: 28, longitude: 77, timestamp: new Date().toISOString() }])); return; }
    const id = req.url.split('/').at(-1);
    res.end(JSON.stringify({ flights: [{ fa_flight_id: id === 'wrong' ? 'OTHER-OCCURRENCE' : id, gate: `G${counts.get(req.url)}` }] }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const actualFetch = global.fetch;
  try {
    process.env.FLIGHT_DATA_PROVIDER = "flightaware";
    process.env.FLIGHTAWARE_API_KEY = "offline-fixture";
    process.env.FLIGHTAWARE_BASE_URL = "https://provider.invalid";
    process.env.DISABLE_PROVIDER_CALLS = "false";
    process.env.PROVIDER_CALLS_ENABLED = "true";
    process.env.DATABASE_URL = "";
    process.env.ALLOW_INSECURE_NO_AUTH = "true";
    global.fetch = (url, options) => {
      assert.ok(String(url).startsWith("https://provider.invalid/"), "external network is forbidden");
      return actualFetch(`http://127.0.0.1:${server.address().port}/${String(url).slice('https://provider.invalid/'.length)}`, options);
    };
    const { __test__: provider } = require("../src/server");
    const results = await Promise.all(Array.from({ length: 20 }, () => provider.fetchFlightAwareFlightByProviderId("exact")));
    assert.equal(counts.get('/flights/exact'), 1);
    assert.ok(results.every((row) => row.fa_flight_id === 'exact' && row.gate === 'G1'));
    assert.ok(Number.isFinite(Date.parse(results[0].__runwyFetchedAt)));
    await new Promise((resolve) => setTimeout(resolve, 5));
    const forced = await provider.fetchFlightAwareFlightByProviderId("exact", { forceRefresh: true });
    assert.equal(forced.gate, "G2");
    assert.equal(await provider.fetchFlightAwareFlightByProviderId("wrong"), null);
    const position = await provider.fetchFlightAwareLivePosition("telemetry");
    assert.equal(position.latitude, 28);
    const track = await provider.fetchFlightAwareTrackTrail("telemetry");
    assert.equal(track.trackPoints.length, 1);
    assert.equal(counts.get('/flights/telemetry/track'), 1, "fallback and route seed reuse the same paid result");
    assert.equal(counts.get('/flights/telemetry/position'), 1);
  } finally {
    global.fetch = actualFetch;
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});
