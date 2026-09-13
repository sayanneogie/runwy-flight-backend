"use strict";
// Isolated PostgreSQL-engine checks. Usage:
// npm install --prefix /tmp/runwy-db-verification --no-audit --no-fund @electric-sql/pglite
// node scripts/verify-shared-flight-db.cjs /tmp/runwy-db-verification/node_modules/@electric-sql/pglite
const { PGlite } = require(process.argv[2] || "@electric-sql/pglite");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { createPostgresSharedFlightRepository } = require("../src/shared-flight/repository");
const { createResponseStore, createProviderRequestCoordinator } = require("../src/provider-request-coordinator");
const { reserveProviderBudget } = require("../src/provider-budget");
const root = path.resolve(__dirname, "..");
(async () => {
  const db = new PGlite();
  try {
    await db.exec("create role anon; create role authenticated;");
    const baseline = fs.readFileSync(path.join(root, "supabase/migrations/20260509_create_shared_flight_state.sql"), "utf8");
    for (const table of ["flight_instances", "flight_snapshots", "flight_events", "api_usage_logs"]) {
      const ddl = baseline.match(new RegExp(`create table if not exists public\\.${table} \\([\\s\\S]*?\\n\\);`));
      assert.ok(ddl, table);
      await db.exec(ddl[0]);
    }
    for (const table of ["flight_instances", "flight_snapshots", "flight_events"]) {
      await db.exec(`alter table public.${table} add column state_revision bigint not null default 0;`);
    }
    for (const file of ["20260828_add_provider_request_leases.sql", "20260914_shared_provider_responses.sql", "20260914_shared_provider_responses.sql"]) {
      await db.exec(fs.readFileSync(path.join(root, "supabase/migrations", file), "utf8"));
    }
    const pool = { query: (sql, values) => db.query(sql, values), connect: async () => ({ query: (sql, values) => db.query(sql, values), release() {} }) };
    const repository = createPostgresSharedFlightRepository(pool);
    const initial = (await db.query(`insert into public.flight_instances
      (flight_key,airline_code,flight_number,departure_date,provider_flight_id,status,gate,normalized_data)
      values ('AI-101-2026-09-14-DEL-FCO','AI','101','2026-09-14','exact-occurrence','scheduled','A12','{}') returning *`)).rows[0];
    const changed = await repository.commitCanonicalState({ ...initial, gate: "A16" }, [{ event_type: "GATE_CHANGED", event_severity: "medium", notification_required: true }], "test");
    assert.equal(changed.applied, true);
    const stale = await repository.commitCanonicalState({ ...initial, gate: "A99" }, [{ event_type: "GATE_CHANGED", event_severity: "medium", notification_required: true }], "test");
    assert.equal(stale.applied, false);
    assert.equal(stale.flight.gate, "A16");
    assert.equal((await db.query("select count(*)::int as n from public.flight_events")).rows[0].n, 1);
    await repository.commitCanonicalState({ ...changed.flight, gate: "A20" }, [], "test");
    assert.equal((await repository.getEventWithFlight(changed.events[0].id)).flight.gate, "A16");
    const beforeFailure = await repository.findFlightById(initial.id);
    await assert.rejects(repository.commitCanonicalState({ ...beforeFailure, gate: "BAD" }, [{ event_type: "INVALID_EVENT", event_severity: "medium" }], "test"));
    assert.equal((await repository.findFlightById(initial.id)).gate, "A20", "state and event roll back together");
    const discovered = await repository.upsertFlightFromNormalized({ airlineCode: "AI", flightNumber: "101", providerFlightId: "old", origin: "DEL", destination: "FCO", status: "scheduled", gate: "OLD" },
      { airline: "AI", number: "101", date: "2026-09-14", origin: "DEL", destination: "FCO" }, new Date().toISOString());
    assert.equal(discovered.gate, "A20", "late discovery must not overwrite state");
    assert.ok((await repository.listPendingNotificationEventIds()).includes(changed.events[0].id));
    await repository.completeNotificationFanout(changed.events[0].id);
    assert.ok(!(await repository.listPendingNotificationEventIds()).includes(changed.events[0].id));
    await db.exec("set role anon");
    await assert.rejects(db.query("select * from public.provider_response_cache"), /permission denied/);
    await db.exec("reset role");
    const store = createResponseStore(pool);
    const first = createProviderRequestCoordinator({ store });
    const second = createProviderRequestCoordinator({ store });
    let calls = 0;
    const options = { ttlMs: 5000, load: async () => { calls++; return Response.json({ exact: true }); } };
    const responses = await Promise.all([first.request("https://provider.test/flights/exact", options), second.request("https://provider.test/flights/exact", options)]);
    for (const response of responses) assert.deepEqual(await response.json(), { exact: true });
    assert.equal(calls, 1);
    const token = await store.claim("fenced", 90_000);
    await store.release("fenced", token);
    await store.claim("fenced", 90_000);
    await store.write("fenced", token, { response: {}, requested_at: new Date().toISOString(), expires_at: new Date(Date.now() + 1000).toISOString() });
    assert.equal(await store.read("fenced"), null);
    await store.cleanup();
    const budget = { endpoint: 'aeroapi:flight:tracked_flight', tracked: true, units: 1, limit: 2, path: '/flights/id', reason: 'verification' };
    await reserveProviderBudget(pool, budget);
    await reserveProviderBudget(pool, budget);
    await assert.rejects(reserveProviderBudget(pool, budget), { code: 'FLIGHTAWARE_DAILY_BUDGET_EXHAUSTED' });
    await reserveProviderBudget(pool, { ...budget, endpoint: 'aeroapi:flight:operational', tracked: false, limit: 1 });
    assert.equal((await db.query("select count(*)::int as n from public.api_usage_logs where cache_status='reserved'")).rows[0].n, 3);
    console.log("PASS: repeatable migration, stale-write rejection, transaction rollback, safe duplicate discovery, APNs snapshots/fanout completion, cache RLS, cross-worker response reuse, lease fencing, atomic budget reservation and bucket isolation.");
  } finally { await db.close(); }
})().catch((error) => { console.error(error); process.exitCode = 1; });
