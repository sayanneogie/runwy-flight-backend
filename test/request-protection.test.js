const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { PGlite } = require("@electric-sql/pglite");
const { PostgresRateLimitStore, createLocalIngress, clientNetwork, clientKey, createSharedLimiter } = require("../src/request-protection");
const express = require("express");

async function database() {
  const db = new PGlite();
  await db.exec(`create role anon; create role authenticated; create role service_role;
    create table public.api_usage_logs(provider text, endpoint text, cost_estimate integer, created_at timestamptz default now());`);
  await db.exec(fs.readFileSync(path.join(__dirname, "../supabase/legacy-migrations/20260915_request_abuse_guardrails.sql"), "utf8"));
  return db;
}

test("shared quotas survive server replacement, isolate identities, and reset only after expiry", async () => {
  const db = await database();
  try {
    const query = (sql, params) => db.query(sql, params);
    const first = new PostgresRateLimitStore(query, "api", 3);
    const second = new PostgresRateLimitStore(query, "api", 3);
    first.init({ windowMs: 60000 }); second.init({ windowMs: 60000 });
    const counts = await Promise.all([first, second, first, second, second].map(store => store.increment("one-user")));
    assert.deepEqual(counts.map(x => x.totalHits), [1, 2, 3, 4, 4]);
    assert.equal((await second.increment("another-user")).totalHits, 1);
    const restarted = new PostgresRateLimitStore(query, "api", 3);
    restarted.init({ windowMs: 60000 });
    assert.equal((await restarted.increment("one-user")).totalHits, 4);
    await db.exec("update runwy_security.rate_limits set expires_at = now() - interval '1 second'");
    // The local denial remains valid until its original expiry, even if an operator edits the DB counter.
    assert.equal((await restarted.increment("one-user")).totalHits, 4);
    for (const value of restarted.denials.values()) value.resetTime = new Date(0);
    assert.equal((await restarted.increment("one-user")).totalHits, 1);
    const { rows } = await db.query("select key_hash from runwy_security.rate_limits");
    assert.ok(rows.every(row => /^[a-f0-9]{64}$/.test(row.key_hash)));
  } finally { await db.close(); }
});

test("provider reservations atomically enforce limits, preserve today's usage and separate tracking reserve", async () => {
  const db = await database();
  try {
    await db.exec("insert into public.api_usage_logs(provider,endpoint,cost_estimate) values ('flightaware','aeroapi:flight:operational',2)");
    const reserve = (bucket, limit, units = 1) => db.query("select * from public.runwy_reserve_flightaware_budget($1,$2,$3)", [bucket, limit, units]);
    const burst = await Promise.all(Array.from({ length: 20 }, () => reserve("general", 5)));
    assert.equal(burst.filter(x => x.rows[0].allowed).length, 3);
    assert.equal((await reserve("general", 5)).rows[0].allowed, false);
    assert.equal((await reserve("general", 6)).rows[0].allowed, true);
    assert.equal((await reserve("tracked", 2, 2)).rows[0].allowed, true);
    assert.equal((await reserve("tracked", 2)).rows[0].allowed, false);
    // Failed outbound requests retain reservations; missing usage logs cannot reopen the budget.
    await db.exec("delete from public.api_usage_logs");
    assert.equal((await reserve("general", 6)).rows[0].allowed, false);
  } finally { await db.close(); }
});

test("client database roles cannot modify or invoke quota counters", async () => {
  const db = await database();
  try {
    await db.exec("set role anon");
    await assert.rejects(db.query("select * from public.runwy_consume_rate_limit($1,$2,$3,$4)", ["api", "a".repeat(64), 60000, 5]), /permission denied/);
    await db.exec("reset role; set role authenticated");
    await assert.rejects(db.query("select * from runwy_security.rate_limits"), /permission denied/);
    await assert.rejects(db.query("select * from public.runwy_reserve_flightaware_budget('general',500,1)"), /permission denied/);
    await db.exec("reset role; set role service_role");
    assert.equal((await db.query("select * from public.runwy_consume_rate_limit($1,$2,$3,$4)", ["api", "b".repeat(64), 60000, 5])).rows[0].total_hits, 1);
  } finally { await db.close(); }
});

test("IPv6 address rotation and IPv4 mapped notation share a network quota", () => {
  assert.equal(clientNetwork("::ffff:192.0.2.7"), clientNetwork("192.0.2.7"));
  assert.equal(clientNetwork("2001:db8:abcd:1201::1"), clientNetwork("2001:db8:abcd:12ff::9"));
  assert.notEqual(clientNetwork("2001:db8:abcd:1201::1"), clientNetwork("2001:db8:abcd:1301::1"));
});

test("Railway uses its edge-overwritten client address across CDN paths; other hosts ignore that header", () => {
  const request = { app: { get: () => true }, get: () => "192.0.2.10, 198.51.100.20", ip: "198.51.100.20" };
  assert.equal(clientKey(request), "192.0.2.10");
  request.get = () => "192.0.2.10";
  assert.equal(clientKey(request), "192.0.2.10");
  request.app.get = () => false;
  assert.equal(clientKey(request), "198.51.100.20");
});

function response() {
  return Object.assign(new EventEmitter(), {
    headers: {}, set(k, v) { this.headers[k] = v; return this; },
    status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; },
  });
}

test("local concurrency and key caps reject work without evicting active offender quotas", () => {
  const guard = createLocalIngress({ maxConcurrent: 1, maxKeys: 1, perIPPerMinute: 2 });
  const a = { ip: "192.0.2.1" }; const b = { ip: "192.0.2.2" };
  let work = 0;
  const first = response(); guard(a, first, () => work++);
  const busy = response(); guard(b, busy, () => work++); assert.equal(busy.statusCode, 503);
  first.emit("finish"); first.emit("close");
  const other = response(); guard(b, other, () => work++); assert.equal(other.statusCode, 503);
  const second = response(); guard(a, second, () => work++); second.emit("finish");
  const limited = response(); guard(a, limited, () => work++); assert.equal(limited.statusCode, 429);
  assert.equal(work, 2);
});

test("shared store failure rejects requests without executing downstream handlers", async () => {
  const app = express();
  app.use(createSharedLimiter({ namespace: "test", limit: 3, query: async () => { throw new Error("database offline"); } }));
  app.get("/", (_req, res) => res.send("should never run"));
  const server = app.listen(0, "127.0.0.1");
  try {
    await new Promise(resolve => server.once("listening", resolve));
    const result = await fetch(`http://127.0.0.1:${server.address().port}`);
    assert.equal(result.status, 503);
    assert.equal(result.headers.get("retry-after"), "5");
  } finally { await new Promise(resolve => server.close(resolve)); }
});
