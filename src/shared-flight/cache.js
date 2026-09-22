"use strict";

const crypto = require("node:crypto");

function createMemoryRedis({ maxEntries = 10000, sweepIntervalMs = 30000 } = {}) {
  const values = new Map();

  function isExpired(record) {
    return record?.expiresAt && record.expiresAt <= Date.now();
  }

  function sweep() {
    for (const [key, record] of values) if (isExpired(record)) values.delete(key);
  }
  const timer = setInterval(sweep, sweepIntervalMs);
  timer.unref?.();
  return {
    close() { clearInterval(timer); values.clear(); },
    async get(key) {
      const record = values.get(key);
      if (!record || isExpired(record)) {
        values.delete(key);
        return null;
      }
      return record.value;
    },
    async set(key, value, options = {}) {
      const ttlMs = Number(options.px || options.PX || (options.ex ? options.ex * 1000 : 0));
      if ((options.nx || options.NX) && values.has(key) && !isExpired(values.get(key))) {
        return null;
      }
      if (!values.has(key) && values.size >= maxEntries) {
        sweep();
        // Never evict an active lock or rate counter to accommodate new work.
        if (values.size >= maxEntries) throw Object.assign(new Error("Cache capacity reached"), { statusCode: 503 });
      }
      values.set(key, { value, expiresAt: ttlMs > 0 ? Date.now() + ttlMs : null });
      return "OK";
    },
    async del(key) {
      return values.delete(key) ? 1 : 0;
    },
    async incr(key) {
      const next = Number((await this.get(key)) || 0) + 1;
      await this.set(key, String(next));
      return next;
    },
    async expire(key, seconds) {
      const record = values.get(key);
      if (!record) return 0;
      record.expiresAt = Date.now() + Number(seconds) * 1000;
      return 1;
    },
    async releaseLock(key, token) {
      const current = await this.get(key);
      if (current !== token) return 0;
      return this.del(key);
    },
    __values: values,
  };
}

function createFlightCache(redis = createMemoryRedis()) {
  async function getJSON(key) {
    const raw = await redis.get(key);
    if (!raw) return null;
    try {
      return typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (_error) {
      return null;
    }
  }

  async function setJSON(key, value, ttlSeconds) {
    const ttlMs = Math.max(1, Math.round(Number(ttlSeconds || 1) * 1000));
    await redis.set(key, JSON.stringify(value), { px: ttlMs });
  }

  async function acquireLock(key, ttlMs) {
    const token = crypto.randomUUID();
    const result = await redis.set(key, token, { nx: true, px: ttlMs });
    return result === "OK" ? token : null;
  }

  async function releaseLock(key, token) {
    if (!token) return false;
    if (typeof redis.releaseLock === "function") {
      return (await redis.releaseLock(key, token)) === 1;
    }
    const current = await redis.get(key);
    if (current !== token) return false;
    await redis.del(key);
    return true;
  }

  return { redis, getJSON, setJSON, acquireLock, releaseLock };
}

module.exports = { createFlightCache, createMemoryRedis };
