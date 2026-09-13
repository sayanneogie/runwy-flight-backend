"use strict";

const { createHash, randomUUID } = require("node:crypto");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Stores only provider responses, never request headers, API keys or user data.
function createResponseStore(pool = null) {
  const records = new Map();
  const leases = new Map();
  return {
    async read(key) {
      if (!pool) return records.get(key) || null;
      return (await pool.query("select response, requested_at, expires_at from public.provider_response_cache where cache_key=$1", [key])).rows[0] || null;
    },
    async claim(key, ttlMs) {
      const token = randomUUID();
      if (!pool) {
        if (leases.get(key)?.expiresAt > Date.now()) return null;
        leases.set(key, { token, expiresAt: Date.now() + ttlMs });
        return token;
      }
      const result = await pool.query(`insert into public.provider_request_leases (lock_key, lease_token, expires_at)
        values ($1,$2,now()+($3::bigint * interval '1 millisecond'))
        on conflict (lock_key) do update set lease_token=excluded.lease_token, expires_at=excluded.expires_at, updated_at=now()
        where public.provider_request_leases.expires_at <= now() returning lease_token`, [key, token, ttlMs]);
      return result.rows[0]?.lease_token || null;
    },
    async write(key, token, record) {
      if (!pool) {
        if (leases.get(key)?.token !== token || leases.get(key).expiresAt <= Date.now()) return;
        records.set(key, record);
        // Bounded memory for non-database development mode.
        if (records.size > 2000) records.delete(records.keys().next().value);
        return;
      }
      await pool.query(`insert into public.provider_response_cache (cache_key,response,requested_at,expires_at)
        select $1,$3::jsonb,$4::timestamptz,$5::timestamptz
        where exists (select 1 from public.provider_request_leases where lock_key=$1 and lease_token=$2::uuid and expires_at>now())
        on conflict (cache_key) do update set response=excluded.response, requested_at=excluded.requested_at, expires_at=excluded.expires_at
        where public.provider_response_cache.requested_at <= excluded.requested_at`,
      [key, token, record.response, record.requested_at, record.expires_at]);
    },
    async release(key, token) {
      if (!pool) {
        if (leases.get(key)?.token === token) leases.delete(key);
        return;
      }
      await pool.query("delete from public.provider_request_leases where lock_key=$1 and lease_token=$2::uuid", [key, token]);
    },
    async cleanup() {
      if (pool) await pool.query("delete from public.provider_response_cache where expires_at < now() - interval '1 day'");
    },
  };
}

function createProviderRequestCoordinator({ store = createResponseStore(), wait = sleep, waitMs = 35_000 } = {}) {
  async function request(url, { ttlMs, forceRefresh = false, load, onReuse = async () => {} }) {
    const key = `response:${createHash("sha256").update(url).digest("hex")}`;
    const requestedAfter = forceRefresh ? Date.now() : 0;
    const deadline = Date.now() + waitMs;
    const reusable = (record) => record && Date.parse(record.expires_at) > Date.now() && Date.parse(record.requested_at) >= requestedAfter;
    const restore = (record) => new Response(record.response.body, { status: record.response.status, headers: { "content-type": "application/json", "x-runwy-cache-expires-at": record.expires_at, "x-runwy-requested-at": record.requested_at } });
    while (true) {
      const cached = await store.read(key);
      if (reusable(cached)) { await onReuse(); return restore(cached); }
      const token = await store.claim(key, 90_000);
      if (token) {
        try {
          // A request may have completed between our read and lease acquisition.
          const latest = await store.read(key);
          if (reusable(latest)) { await onReuse(); return restore(latest); }
          const startedAt = Date.now();
          const response = await load();
          if (response.ok) {
            const body = await response.clone().text();
            // Never retain malformed JSON or provider error envelopes as data.
            try {
              const parsed = JSON.parse(body);
              if (parsed && !parsed.error && !parsed.errors) {
                await store.write(key, token, { response: { body, status: response.status },
                  requested_at: new Date(startedAt).toISOString(),
                  expires_at: new Date(startedAt + ttlMs).toISOString() });
              }
            } catch (error) {
              if (!(error instanceof SyntaxError)) throw error;
            }
          }
          return response;
        } finally { await store.release(key, token); }
      }
      if (Date.now() >= deadline) throw new Error("Shared provider request still in progress; retry using the last confirmed flight state");
      await wait(100);
    }
  }
  return { request, cleanup: () => store.cleanup() };
}

module.exports = { createProviderRequestCoordinator, createResponseStore };
