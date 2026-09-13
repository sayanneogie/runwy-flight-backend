"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { createPaidAccess, paidEntitlement, withoutLiveTelemetry } = require("../src/paid-access");
const now = Date.parse("2026-09-14T00:00:00Z");
const payload = (expires, grace = null) => ({ subscriber: { entitlements: { first_class: {
  product_identifier: "monthly", purchase_date: "2026-09-01T00:00:00Z", expires_date: expires,
  grace_period_expires_date: grace,
} } } });
test("membership checks expiry, lifetime, grace and missing entitlement", () => {
  assert.equal(paidEntitlement(payload(null), now), true);
  assert.equal(paidEntitlement(payload("2026-10-01"), now), true);
  assert.equal(paidEntitlement(payload("2026-09-13"), now), false);
  assert.equal(paidEntitlement(payload("2026-09-14"), now), false);
  assert.equal(paidEntitlement(payload("2026-09-13", "2026-09-15"), now), true);
  assert.equal(paidEntitlement(payload("invalid"), now), false);
  assert.equal(paidEntitlement({ isPro: true }, now), false);
});
test("concurrent membership checks share a request, expiration ends cached access", async () => {
  let calls = 0, clock = now;
  const access = createPaidAccess({ apiKey: "test", now: () => clock, fetchImpl: async () => {
    calls++; return { ok: true, json: async () => payload(new Date(now + 1000).toISOString()) };
  } });
  const results = await Promise.all([access.membership("user"), access.membership("user")]);
  assert.equal(calls, 1);
  assert.ok(results.every(r => r.paid));
  clock += 1000;
  assert.equal((await access.membership("user")).paid, false);
  assert.equal(calls, 2);
});
test("free-only flight denies telemetry; mixed flight retains paid owner's access", async () => {
  let users = ["free"];
  const access = createPaidAccess({ apiKey: "test", now: () => now,
    query: async () => ({ rows: users.map(user_id => ({ user_id })) }),
    fetchImpl: async url => ({ ok: true, json: async () => url.endsWith("paid") ? payload(null) : { subscriber: { entitlements: {} } } }),
  });
  assert.deepEqual(await access.flight("instance"), { paid: false, verified: true });
  users = ["free", "paid"];
  assert.equal((await access.flight("instance")).paid, true);
});
test("verification failures withhold paid work without classifying user as expired", async () => {
  const access = createPaidAccess({ apiKey: "test", query: async () => ({ rows: [{ user_id: "paid" }] }),
    fetchImpl: async () => ({ ok: false }) });
  assert.deepEqual(await access.flight("instance"), { paid: false, verified: false });
  assert.equal((await createPaidAccess({}).membership("user")).verified, false);
});
test("database UUIDs resolve the same case-sensitive RevenueCat ID as Swift", async () => {
  let requested;
  const access = createPaidAccess({ apiKey: "test", fetchImpl: async url => {
    requested = url;
    return { ok: true, json: async () => payload(null) };
  } });
  await access.membership("abcdef01-abcd-1234-abcd-abcdef123456");
  assert.ok(requested.endsWith("ABCDEF01-ABCD-1234-ABCD-ABCDEF123456"));
});
test("free responses remove nested live telemetry without removing status and gates", () => {
  const input = { flight: { status: "enroute", departureGate: "A1", livePosition: { latitude: 1 },
    trackPoints: [1], inboundFlight: { position: { lat: 2 } } } };
  const result = withoutLiveTelemetry(input);
  assert.equal(result.flight.departureGate, "A1");
  assert.equal(result.flight.status, "enroute");
  assert.equal(result.flight.livePosition, null);
  assert.deepEqual(result.flight.trackPoints, []);
  assert.equal(result.flight.inboundFlight.position, null);
  assert.equal(input.flight.livePosition.latitude, 1);
});
test("queued Circle delivery is blocked if its flight owner loses membership", async () => {
  const access = createPaidAccess({ apiKey: "test", now: () => now,
    query: async () => ({ rows: [{ user_id: "paid", owner_user_id: "free" }] }),
    fetchImpl: async url => ({ ok: true, json: async () => url.endsWith("paid") ? payload(null) : { subscriber: { entitlements: {} } } }),
  });
  assert.equal((await access.delivery("queued")).paid, false);
});
test("enabled provider cannot be reached by forced free-only telemetry", () => {
  const server = require.resolve("../src/server");
  const script = `const assert = require('node:assert/strict');
    const { __test__: api } = require(${JSON.stringify(server)});
    let calls = 0;
    global.fetch = async () => { calls++; throw new Error('Unexpected network'); };
    api.paidAccess.flight = async () => ({paid:false, verified:true});
    (async () => {
      assert.equal(await api.fetchFlightAwareLivePosition('free', {forceRefresh:true}), null);
      assert.deepEqual(await api.fetchFlightAwareTrackTrail('free', {forceRefresh:true}), {trackPoints:[],livePosition:null});
      assert.equal(calls, 0);
    })().catch(() => {process.exitCode=1});`;
  const result = spawnSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 15000,
    env: { ...process.env, DISABLE_PROVIDER_CALLS: "false", FLIGHT_DATA_PROVIDER: "flightaware",
      FLIGHTAWARE_API_KEY: "test-only", DATABASE_URL: "", ALLOW_INSECURE_NO_AUTH: "true" } });
  assert.equal(result.status, 0, result.stderr);
});
