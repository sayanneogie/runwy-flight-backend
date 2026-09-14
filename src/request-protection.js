"use strict";

const crypto = require("node:crypto");
const ipaddr = require("ipaddr.js");
const rateLimit = require("express-rate-limit");

function positiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function clientNetwork(value) {
  try {
    const address = ipaddr.process(String(value || ""));
    if (address.kind() === "ipv4") return address.toString();
    // Rotating IPv6 addresses within one allocation must not reset the quota.
    const bytes = address.toByteArray();
    bytes.fill(0, 7);
    return `${ipaddr.fromByteArray(bytes).toNormalizedString()}/56`;
  } catch { return "unknown"; }
}

function clientKey(req) {
  return clientNetwork(req.ip || req.socket?.remoteAddress);
}

function createProtectionMetrics(logger = console) {
  let counts = Object.create(null);
  return {
    record(event) { counts[event] = (counts[event] || 0) + 1; },
    flush() {
      if (Object.keys(counts).length) logger.warn(JSON.stringify({ event: "request_protection", counts }));
      counts = Object.create(null);
    },
  };
}

function rejectRequest(res, status, seconds = 60) {
  res.set("Retry-After", String(Math.max(1, seconds)));
  res.set("Cache-Control", "no-store");
  return res.status(status).json({
    error: status === 429 ? "Too many requests. Please try again shortly." : "Service is busy. Please try again shortly.",
    retryAfterSeconds: Math.max(1, seconds),
  });
}

// A bounded, dependency-free first barrier. Rejected floods never query Postgres.
function createLocalIngress({ maxConcurrent = 64, requestsPerSecond = 200, perIPPerMinute = 1200,
  maxKeys = 10000, now = Date.now, metrics = createProtectionMetrics() } = {}) {
  const clients = new Map();
  let active = 0;
  let second = 0;
  let secondHits = 0;
  let nextSweep = 0;
  return (req, res, next) => {
    res.once("finish", () => {
      if (res.statusCode === 401) metrics.record("authentication_rejected");
    });
    const timestamp = now();
    if (timestamp >= nextSweep) {
      for (const [key, value] of clients) if (value.resetAt <= timestamp) clients.delete(key);
      nextSweep = timestamp + 60000;
    }
    const currentSecond = Math.floor(timestamp / 1000);
    if (second !== currentSecond) { second = currentSecond; secondHits = 0; }
    if (++secondHits > requestsPerSecond || active >= maxConcurrent) {
      metrics.record("local_capacity_rejected");
      return rejectRequest(res, 503, 1);
    }
    const key = clientKey(req);
    let entry = clients.get(key);
    if (!entry || entry.resetAt <= timestamp) {
      if (!entry && clients.size >= maxKeys) {
        metrics.record("local_key_capacity_rejected");
        return rejectRequest(res, 503, 60);
      }
      entry = { hits: 0, resetAt: timestamp + 60000 };
      clients.set(key, entry);
    }
    entry.hits = Math.min(perIPPerMinute + 1, entry.hits + 1);
    if (entry.hits > perIPPerMinute) {
      metrics.record("local_ip_rejected");
      return rejectRequest(res, 429, Math.ceil((entry.resetAt - timestamp) / 1000));
    }
    active++;
    let released = false;
    const release = () => { if (!released) { released = true; active--; } };
    res.once("finish", release);
    res.once("close", release);
    next();
  };
}

class PostgresRateLimitStore {
  constructor(query, namespace, limit) {
    this.query = query;
    this.prefix = `${namespace}:`;
    this.limit = limit;
    this.localKeys = false;
  }
  init(options) { this.windowMs = options.windowMs; }
  async increment(key) {
    const hash = crypto.createHash("sha256").update(String(key)).digest("hex");
    const { rows } = await this.query(
      "select * from public.runwy_consume_rate_limit($1, $2, $3, $4)",
      [this.prefix, hash, this.windowMs, this.limit],
    );
    if (!rows[0]) throw new Error("Rate-limit store returned no counter");
    return { totalHits: Number(rows[0].total_hits), resetTime: new Date(rows[0].reset_at) };
  }
  // Every request counts, including errors and disconnects. No refunds or resets.
  async decrement() {}
  async resetKey() {}
}

function createSharedLimiter({ query, namespace, limit, keyGenerator = clientKey,
  skip, metrics = createProtectionMetrics() }) {
  const middleware = rateLimit({
    windowMs: 60000,
    max: limit,
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator,
    skip,
    ...(query ? { store: new PostgresRateLimitStore(query, namespace, limit) } : {}),
    handler(req, res) {
      metrics.record(`${namespace}_rejected`);
      const remaining = (req.rateLimit?.resetTime?.getTime() || Date.now() + 60000) - Date.now();
      return rejectRequest(res, 429, Math.ceil(remaining / 1000));
    },
  });
  return (req, res, next) => middleware(req, res, (error) => {
    if (!error) return next();
    metrics.record("rate_store_unavailable");
    // Never silently switch to per-process quotas during a database outage.
    return rejectRequest(res, 503, 5);
  });
}

module.exports = { positiveInteger, clientNetwork, clientKey, createProtectionMetrics,
  createLocalIngress, createSharedLimiter, PostgresRateLimitStore, rejectRequest };
