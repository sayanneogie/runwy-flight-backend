require("dotenv").config();

const crypto = require("node:crypto");
const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");
const { version: PACKAGE_VERSION = "0.0.0" } = require("../package.json");
const { getAirportCatalog } = require("./airport-catalog");
const {
  validatePushTokenPayload,
  validateRouteSearchQuery,
  validateSearchQuery,
  validateTrackPayload,
} = require("./request-schemas");
const {
  firehoseMessageFlightNumber,
  firehoseMessageProviderFlightId,
  firehoseMessageTimestampMs,
  firehoseMessageType,
} = require("./firehose-protocol");
const { createFirehoseRuntime } = require("./firehose-runtime");
const { mergeRealtimeTelemetry } = require("./realtime-telemetry");
const { createTrackingStore } = require("./tracking-store");
const { createTrackingPollerRuntime } = require("./tracking-poller-runtime");

const PORT = Number(process.env.PORT || 8787);
const FLIGHT_DATA_PROVIDER = (process.env.FLIGHT_DATA_PROVIDER || "aviationstack").toLowerCase();

const AVIATIONSTACK_KEY = process.env.AVIATIONSTACK_KEY;
const AVIATIONSTACK_BASE_URL = requireHTTPSBaseURL(
  "AVIATIONSTACK_BASE_URL",
  process.env.AVIATIONSTACK_BASE_URL || "https://api.aviationstack.com/v1"
);

const FLIGHTAWARE_API_KEY = process.env.FLIGHTAWARE_API_KEY;
const FLIGHTAWARE_BASE_URL = requireHTTPSBaseURL(
  "FLIGHTAWARE_BASE_URL",
  process.env.FLIGHTAWARE_BASE_URL || "https://aeroapi.flightaware.com/aeroapi"
);
const FIREHOSE_HOST = String(process.env.FIREHOSE_HOST || "firehose.flightaware.com").trim();
const FIREHOSE_PORT = toPositiveNumber(process.env.FIREHOSE_PORT, 1501);
const FIREHOSE_USERNAME = String(process.env.FIREHOSE_USERNAME || "").trim();
const FIREHOSE_PASSWORD = String(
  process.env.FIREHOSE_PASSWORD || process.env.FIREHOSE_API_KEY || ""
).trim();
const FIREHOSE_VERSION = String(process.env.FIREHOSE_VERSION || "36.0").trim() || "36.0";
const FIREHOSE_USER_AGENT = String(process.env.FIREHOSE_USER_AGENT || "runwy-firehose").trim() || "runwy-firehose";
const FIREHOSE_KEEPALIVE_SECONDS = toPositiveNumber(process.env.FIREHOSE_KEEPALIVE_SECONDS, 60);
const FIREHOSE_TRACKED_SET_REFRESH_MS = toPositiveNumber(
  process.env.FIREHOSE_TRACKED_SET_REFRESH_MS,
  60_000
);
const FIREHOSE_RECONNECT_DELAY_MS = toPositiveNumber(process.env.FIREHOSE_RECONNECT_DELAY_MS, 5_000);
const FIREHOSE_MIN_SECONDS_BETWEEN_AIRBORNE = toNonNegativeNumber(
  process.env.FIREHOSE_MIN_SECONDS_BETWEEN_AIRBORNE,
  15
);
const FIREHOSE_BACKFILL_MAX_HOURS = toPositiveNumber(
  process.env.FIREHOSE_BACKFILL_MAX_HOURS,
  8
);
const FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES = toNonNegativeNumber(
  process.env.FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
  15
);
const FIREHOSE_BACKFILL_MIN_TRACK_POINTS = Math.max(
  1,
  Math.round(toPositiveNumber(process.env.FIREHOSE_BACKFILL_MIN_TRACK_POINTS, 8))
);
const ENABLE_FIREHOSE_WORKER =
  String(process.env.ENABLE_FIREHOSE_WORKER || "false").toLowerCase() === "true";
const FIREHOSE_EVENTS = Object.freeze(
  parseListEnv(process.env.FIREHOSE_EVENTS, [
    "flifo",
    "departure",
    "arrival",
    "cancellation",
    "position",
  ])
);
const FIREHOSE_TRACK_LOOKAHEAD_MS =
  toPositiveNumber(process.env.FIREHOSE_TRACK_LOOKAHEAD_HOURS, 2) * 60 * 60_000;
const FIREHOSE_POST_ARRIVAL_BUFFER_MS =
  toPositiveNumber(process.env.FIREHOSE_POST_ARRIVAL_BUFFER_MINUTES, 45) * 60_000;
const WEBHOOK_PUBLIC_BASE_URL = optionalHTTPSBaseURL(
  "WEBHOOK_PUBLIC_BASE_URL",
  process.env.WEBHOOK_PUBLIC_BASE_URL ||
    process.env.PUBLIC_BASE_URL ||
    (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : "") ||
    process.env.RAILWAY_STATIC_URL ||
    ""
);

const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_SSL = String(process.env.DATABASE_SSL || "false").toLowerCase() === "true";
const NODE_ENV = String(process.env.NODE_ENV || "development").toLowerCase();
const IS_PRODUCTION = NODE_ENV === "production";

const CACHE_TTL_MS = toPositiveNumber(process.env.CACHE_TTL_MS, 5 * 60_000);
const FLIGHTAWARE_POSITION_CACHE_TTL_MS = toPositiveNumber(
  process.env.FLIGHTAWARE_POSITION_CACHE_TTL_MS,
  5 * 60_000
);
const FLIGHTAWARE_SCHEDULE_WINDOW_MS = 48 * 60 * 60_000;
const RATE_LIMIT_PER_MINUTE = toPositiveNumber(process.env.RATE_LIMIT_PER_MINUTE, 60);
const WEBHOOK_SHARED_SECRET = (process.env.WEBHOOK_SHARED_SECRET || "").trim();

const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim().replace(/\/+$/, "");
const SUPABASE_ANON_KEY = (process.env.SUPABASE_ANON_KEY || "").trim();
const SUPABASE_JWT_SECRET = process.env.SUPABASE_JWT_SECRET || "";
const ALLOW_INSECURE_NO_AUTH = String(process.env.ALLOW_INSECURE_NO_AUTH || "false").toLowerCase() === "true";
const AUTH_CACHE_TTL_MS = toPositiveNumber(process.env.AUTH_CACHE_TTL_MS, 5 * 60_000);
const MAX_AUTH_CACHE_ENTRIES = toPositiveNumber(process.env.MAX_AUTH_CACHE_ENTRIES, 5_000);
const POLLER_INTERVAL_MS = toPositiveNumber(process.env.POLLER_INTERVAL_MS, 2 * 60_000);
const POLLER_BATCH_SIZE = toPositiveNumber(process.env.POLLER_BATCH_SIZE, 25);
const STALE_FETCH_REFRESH_THRESHOLD_MS = toPositiveNumber(
  process.env.STALE_FETCH_REFRESH_THRESHOLD_MS,
  10 * 60_000
);
const ENABLE_TRACKING_POLLER = String(process.env.ENABLE_TRACKING_POLLER || "false").toLowerCase() === "true";
const FLIGHTAWARE_ENABLE_MAP_FALLBACK =
  String(process.env.FLIGHTAWARE_ENABLE_MAP_FALLBACK || "false").toLowerCase() === "true";
const SEARCH_LIVE_ENRICH_LIMIT = toNonNegativeNumber(process.env.SEARCH_LIVE_ENRICH_LIMIT, 0);
const TRACKING_POLLER_LOG_SUMMARY =
  String(process.env.TRACKING_POLLER_LOG_SUMMARY || "true").toLowerCase() === "true";
const MAX_ACTIVE_TRACKING_SESSIONS_PER_USER = toNonNegativeNumber(
  process.env.MAX_ACTIVE_TRACKING_SESSIONS_PER_USER,
  IS_PRODUCTION ? Number.MAX_SAFE_INTEGER : 20
);
const WEBHOOK_REFRESH_MIN_INTERVAL_MS = toPositiveNumber(
  process.env.WEBHOOK_REFRESH_MIN_INTERVAL_MS,
  15 * 60_000
);
const DISABLE_PROVIDER_CALLS = String(process.env.DISABLE_PROVIDER_CALLS || "false").toLowerCase() === "true";
const PROVIDER_CALLS_ENABLED =
  !DISABLE_PROVIDER_CALLS &&
  String(process.env.PROVIDER_CALLS_ENABLED || "true").toLowerCase() !== "false";
const HEALTH_PROVIDER_AUTH_CACHE_TTL_MS = toPositiveNumber(
  process.env.HEALTH_PROVIDER_AUTH_CACHE_TTL_MS,
  6 * 60 * 60_000
);

const MAX_PROVIDER_CACHE_ENTRIES = toPositiveNumber(process.env.MAX_PROVIDER_CACHE_ENTRIES, 2_000);
const MAX_MEMORY_TRACKED_FLIGHTS = toPositiveNumber(process.env.MAX_MEMORY_TRACKED_FLIGHTS, 10_000);
const MAX_MEMORY_PUSH_DEVICES = toPositiveNumber(process.env.MAX_MEMORY_PUSH_DEVICES, 25_000);
const SERVER_STARTED_AT = new Date().toISOString();
const BUILD_INFO = Object.freeze({
  version: PACKAGE_VERSION,
  startedAt: SERVER_STARTED_AT,
  railwayServiceName: String(process.env.RAILWAY_SERVICE_NAME || "").trim() || null,
  railwayEnvironmentName: String(process.env.RAILWAY_ENVIRONMENT_NAME || "").trim() || null,
  railwayDeploymentId: String(process.env.RAILWAY_DEPLOYMENT_ID || "").trim() || null,
  gitCommitSha:
    String(
      process.env.RAILWAY_GIT_COMMIT_SHA ||
      process.env.SOURCE_VERSION ||
      ""
    ).trim() || null,
  gitBranch:
    String(
      process.env.RAILWAY_GIT_BRANCH ||
      process.env.VERCEL_GIT_COMMIT_REF ||
      ""
    ).trim() || null,
  features: Object.freeze({
    scheduleAwareSearch: true,
    scheduleWindowHours: Math.round(FLIGHTAWARE_SCHEDULE_WINDOW_MS / 60 / 60_000),
  }),
});

const APNS_KEY_ID = process.env.APNS_KEY_ID || "";
const APNS_TEAM_ID = process.env.APNS_TEAM_ID || "";
const APNS_BUNDLE_ID = process.env.APNS_BUNDLE_ID || "";
const APNS_PRIVATE_KEY = process.env.APNS_PRIVATE_KEY || "";
const APNS_PRIVATE_KEY_BASE64 = process.env.APNS_PRIVATE_KEY_BASE64 || "";
const APNS_USE_SANDBOX = String(process.env.APNS_USE_SANDBOX || "true").toLowerCase() === "true";
const FLIGHTAWARE_AUTO_ALERT_EVENTS = Object.freeze({
  arrival: true,
  cancelled: true,
  departure: true,
  diverted: true,
  filed: true,
  out: false,
  off: true,
  on: true,
  in: false,
  hold_start: false,
  hold_end: false,
});
const FLIGHTAWARE_AUTO_ALERT_IMPENDING_DEPARTURE_MINUTES = Object.freeze([120, 60, 15]);
const FLIGHTAWARE_AUTO_ALERT_IMPENDING_ARRIVAL_MINUTES = Object.freeze([30]);

if (PROVIDER_CALLS_ENABLED && FLIGHT_DATA_PROVIDER === "aviationstack" && !AVIATIONSTACK_KEY) {
  console.error("Missing AVIATIONSTACK_KEY environment variable.");
  process.exit(1);
}

if (PROVIDER_CALLS_ENABLED && FLIGHT_DATA_PROVIDER === "flightaware" && !FLIGHTAWARE_API_KEY) {
  console.error("Missing FLIGHTAWARE_API_KEY environment variable.");
  process.exit(1);
}

if (!["aviationstack", "flightaware"].includes(FLIGHT_DATA_PROVIDER)) {
  console.error(`Unsupported FLIGHT_DATA_PROVIDER: ${FLIGHT_DATA_PROVIDER}`);
  process.exit(1);
}

if (!ALLOW_INSECURE_NO_AUTH && !SUPABASE_JWT_SECRET && !(SUPABASE_URL && SUPABASE_ANON_KEY)) {
  console.error(
    "Missing auth verification config. Set SUPABASE_JWT_SECRET or both SUPABASE_URL and SUPABASE_ANON_KEY."
  );
  process.exit(1);
}

if (!WEBHOOK_SHARED_SECRET) {
  console.warn(
    "WEBHOOK_SHARED_SECRET is not configured. /v1/webhooks/flightaware will reject all requests."
  );
}

const app = express();
app.disable("x-powered-by");
app.use(helmet());
app.use(express.json({ limit: "100kb" }));

const authTokenCache = new Map();

const limiter = rateLimit({
  windowMs: 60_000,
  max: RATE_LIMIT_PER_MINUTE,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const userID = String(req.auth?.userId || "").trim();
    if (userID) {
      return `user:${userID.slice(0, 128)}`;
    }
    const ip = req.ip || req.socket?.remoteAddress || "unknown";
    const deviceID = normalizedHeaderDeviceID(req);
    if (deviceID) {
      return `ip:${ip}|device:${deviceID.slice(0, 128)}`;
    }
    return `ip:${ip}`;
  },
});

app.use("/v1", authenticateRequest);
app.use("/v1", limiter);

const providerCache = new Map();
const providerInFlightRequests = new Map();
const memoryTrackedFlights = new Map();
const memoryPushDevices = new Map();

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_SSL ? { rejectUnauthorized: false } : undefined,
    })
  : null;

if (pool) {
  pool.on("error", (error) => {
    console.error("Postgres pool error", error);
  });
}

setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of providerCache.entries()) {
    if (entry.expiresAt <= now) {
      providerCache.delete(key);
    }
  }
  for (const [key, entry] of authTokenCache.entries()) {
    if (entry.expiresAt <= now) {
      authTokenCache.delete(key);
    }
  }
}, 60_000).unref();

const apnsTokenCache = {
  token: null,
  expiresAt: 0,
};

function toPositiveNumber(rawValue, fallback) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function toNonNegativeNumber(rawValue, fallback) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.floor(parsed);
}

function parseListEnv(rawValue, fallback = []) {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return [...fallback];
  }

  return String(rawValue)
    .split(/[,\s]+/)
    .map((value) => String(value || "").trim())
    .filter(Boolean);
}

function requireHTTPSBaseURL(envName, value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch (_error) {
    console.error(`Invalid ${envName} URL: ${value}`);
    process.exit(1);
  }

  if (parsed.protocol !== "https:") {
    console.error(`${envName} must use HTTPS: ${value}`);
    process.exit(1);
  }

  return parsed.toString().replace(/\/+$/, "");
}

function optionalHTTPSBaseURL(envName, value) {
  const normalized = String(value || "").trim();
  if (!normalized) return "";
  return requireHTTPSBaseURL(envName, normalized);
}

function safeHTTPSBaseURL(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return "";

  let parsed;
  try {
    parsed = new URL(normalized);
  } catch (_error) {
    return "";
  }

  if (parsed.protocol !== "https:") {
    return "";
  }

  return parsed.toString().replace(/\/+$/, "");
}

function enforceMapSizeLimit(map, maxEntries, onEvict) {
  while (map.size > maxEntries) {
    const oldestKey = map.keys().next().value;
    if (typeof oldestKey === "undefined") break;
    const oldestValue = map.get(oldestKey);
    map.delete(oldestKey);
    if (typeof onEvict === "function") {
      onEvict(oldestKey, oldestValue);
    }
  }
}

function tokenCacheKey(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

function base64UrlDecodeToBuffer(value) {
  if (!value) return null;
  let normalized = String(value).replace(/-/g, "+").replace(/_/g, "/");
  while (normalized.length % 4 !== 0) {
    normalized += "=";
  }

  try {
    return Buffer.from(normalized, "base64");
  } catch (_error) {
    return null;
  }
}

function decodeJWT(token) {
  const segments = String(token || "").split(".");
  if (segments.length !== 3) throw new Error("Invalid JWT format");

  const headerBuffer = base64UrlDecodeToBuffer(segments[0]);
  const payloadBuffer = base64UrlDecodeToBuffer(segments[1]);
  const signatureBuffer = base64UrlDecodeToBuffer(segments[2]);
  if (!headerBuffer || !payloadBuffer || !signatureBuffer) {
    throw new Error("Invalid JWT encoding");
  }

  const header = JSON.parse(headerBuffer.toString("utf8"));
  const payload = JSON.parse(payloadBuffer.toString("utf8"));

  return {
    header,
    payload,
    signatureBuffer,
    signingInput: `${segments[0]}.${segments[1]}`,
  };
}

function timingSafeEqualBuffer(a, b) {
  if (!Buffer.isBuffer(a) || !Buffer.isBuffer(b)) return false;
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function timingSafeEqualText(a, b) {
  const left = Buffer.from(String(a || ""), "utf8");
  const right = Buffer.from(String(b || ""), "utf8");
  return timingSafeEqualBuffer(left, right);
}

function isUUID(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(value || "").trim()
  );
}

function validateJWTLifetime(payload) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  if (Number.isFinite(payload?.nbf) && nowSeconds < payload.nbf) {
    throw new Error("Token not active");
  }
  if (Number.isFinite(payload?.exp) && nowSeconds >= payload.exp) {
    throw new Error("Token expired");
  }
}

function verifySupabaseJWTWithSecret(token) {
  const decoded = decodeJWT(token);
  if (decoded.header?.alg !== "HS256") {
    throw new Error("Unsupported JWT algorithm");
  }

  const expectedSignature = crypto
    .createHmac("sha256", SUPABASE_JWT_SECRET)
    .update(decoded.signingInput)
    .digest();

  if (!timingSafeEqualBuffer(decoded.signatureBuffer, expectedSignature)) {
    throw new Error("Invalid JWT signature");
  }

  validateJWTLifetime(decoded.payload);
  const userId = String(decoded.payload?.sub || "").trim();
  if (!userId) {
    throw new Error("JWT is missing user subject");
  }

  return {
    userId,
    tokenExpiresAtMs: Number.isFinite(decoded.payload?.exp) ? decoded.payload.exp * 1000 : null,
  };
}

async function verifySupabaseTokenViaAuthAPI(token) {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error("Supabase Auth API credentials are missing");
  }
  if (!SUPABASE_URL.startsWith("https://")) {
    throw new Error("SUPABASE_URL must use HTTPS");
  }

  const response = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    method: "GET",
    headers: {
      apikey: SUPABASE_ANON_KEY,
      authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Supabase Auth API rejected token (${response.status})`);
  }

  const payload = await response.json();
  const userId = String(payload?.id || "").trim();
  if (!userId) {
    throw new Error("Supabase Auth API response missing user id");
  }

  const decoded = decodeJWT(token);
  validateJWTLifetime(decoded.payload);

  return {
    userId,
    tokenExpiresAtMs: Number.isFinite(decoded.payload?.exp) ? decoded.payload.exp * 1000 : null,
  };
}

function normalizedHeaderDeviceID(req) {
  const raw = String(req.get("X-Device-Id") || "").trim();
  if (!raw) return null;
  return raw.slice(0, 128);
}

function webhookSecretFromRequest(req) {
  const headerSecret = String(req.get("X-Runwy-Webhook-Secret") || "").trim();
  if (headerSecret) {
    return headerSecret;
  }

  const secretQueryValue =
    req.query?.secret ||
    req.query?.token ||
    req.query?.webhook_secret ||
    req.query?.webhookSecret;
  const querySecret = String(secretQueryValue || "").trim();
  return querySecret || "";
}

function inferredHTTPSBaseURLFromRequest(req) {
  const forwardedHost = String(req.get("X-Forwarded-Host") || req.get("Host") || "")
    .split(",")[0]
    .trim();
  if (!forwardedHost) return "";

  const forwardedProto = String(req.get("X-Forwarded-Proto") || "")
    .split(",")[0]
    .trim()
    .toLowerCase();
  const scheme = forwardedProto === "https" ? "https" : "https";
  return safeHTTPSBaseURL(`${scheme}://${forwardedHost}`);
}

function flightAwareWebhookTargetURL(req) {
  if (!WEBHOOK_SHARED_SECRET) return null;

  const baseURL = inferredHTTPSBaseURLFromRequest(req) || WEBHOOK_PUBLIC_BASE_URL;
  if (!baseURL) return null;

  const target = new URL("/v1/webhooks/flightaware", `${baseURL}/`);
  target.searchParams.set("secret", WEBHOOK_SHARED_SECRET);
  return target.toString();
}

function scopedDeviceID(userId, rawDeviceID) {
  const deviceID = String(rawDeviceID || "").trim();
  if (!deviceID) return "";
  const normalizedUserID = String(userId || "").trim();
  if (!normalizedUserID) return deviceID.slice(0, 128);
  return `${normalizedUserID}:${deviceID}`.slice(0, 192);
}

function shouldBypassAuthForRequest(req) {
  return req.path === "/airports" || req.path === "/webhooks/flightaware";
}

async function authenticateRequest(req, res, next) {
  if (shouldBypassAuthForRequest(req)) {
    return next();
  }

  if (ALLOW_INSECURE_NO_AUTH) {
    const debugUserId = String(req.get("X-Debug-User-Id") || process.env.DEBUG_USER_ID || "").trim();
    if (!isUUID(debugUserId)) {
      return res.status(401).json({ error: "Missing valid X-Debug-User-Id in ALLOW_INSECURE_NO_AUTH mode" });
    }
    req.auth = { userId: debugUserId };
    return next();
  }

  const authHeader = String(req.get("Authorization") || "");
  const bearerPrefix = "Bearer ";
  if (!authHeader.startsWith(bearerPrefix)) {
    return res.status(401).json({ error: "Missing Authorization bearer token" });
  }

  const token = authHeader.slice(bearerPrefix.length).trim();
  if (!token) {
    return res.status(401).json({ error: "Missing Authorization bearer token" });
  }

  const cacheKey = tokenCacheKey(token);
  const now = Date.now();
  const cached = authTokenCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    req.auth = { userId: cached.userId };
    return next();
  }

  try {
    let verification;
    if (SUPABASE_JWT_SECRET) {
      try {
        verification = verifySupabaseJWTWithSecret(token);
      } catch (verificationError) {
        if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
          throw verificationError;
        }
        verification = await verifySupabaseTokenViaAuthAPI(token);
      }
    } else {
      verification = await verifySupabaseTokenViaAuthAPI(token);
    }

    const maxExpiry = now + AUTH_CACHE_TTL_MS;
    const tokenExpiry = Number.isFinite(verification.tokenExpiresAtMs)
      ? verification.tokenExpiresAtMs
      : maxExpiry;
    const cacheExpiry = Math.min(maxExpiry, tokenExpiry);

    authTokenCache.set(cacheKey, {
      userId: verification.userId,
      expiresAt: cacheExpiry,
    });
    enforceMapSizeLimit(authTokenCache, MAX_AUTH_CACHE_ENTRIES);

    req.auth = { userId: verification.userId };
    return next();
  } catch (_error) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}

function normalizeFlightCode(input) {
  return String(input || "")
    .toUpperCase()
    .replace(/\s+/g, "");
}

function normalizeAirportCode(input) {
  const value = String(input || "").toUpperCase().trim();
  if (!value) return null;
  return value.length >= 3 ? value.slice(0, 3) : value;
}

function normalizeIataAirportCode(input) {
  const value = String(input || "").toUpperCase().trim();
  if (!value) return null;
  return /^[A-Z0-9]{3}$/.test(value) ? value : null;
}

function normalizeStatus(rawStatus) {
  const value = String(rawStatus || "").toLowerCase().trim();
  if (!value) return "scheduled";
  if (value.includes("cancel")) return "cancelled";
  if (value.includes("divert")) return "diverted";
  if (value.includes("land")) return "landed";
  if (value.includes("board")) return "boarding";
  if (value.includes("delay")) return "delayed";
  if (value.includes("active") || value.includes("airborne") || value.includes("en-route") || value.includes("enroute")) return "enroute";
  if (value.includes("depart") || value.includes("off")) return "departed";
  return "scheduled";
}

function reconcileOperationalStatus(normalized) {
  if (!normalized || typeof normalized !== "object") {
    return normalized;
  }

  const landingActual = normalized.landingTimes?.actual || normalized.arrivalTimes?.actual;
  if (landingActual) {
    return {
      ...normalized,
      status: "landed",
    };
  }

  const airborneSignal =
    normalized.livePosition ||
    normalized.takeoffTimes?.actual ||
    (Number.isFinite(Number(normalized.progressPercent)) && Number(normalized.progressPercent) > 0);

  if (airborneSignal) {
    return {
      ...normalized,
      status: "enroute",
    };
  }

  const departedSignal = normalized.departureTimes?.actual || normalized.takeoffTimes?.estimated;
  if (departedSignal && ["cancelled", "scheduled", "boarding", "delayed"].includes(normalized.status)) {
    return {
      ...normalized,
      status: "departed",
    };
  }

  return normalized;
}

function isoOrNull(value) {
  if (!value) return null;

  if (typeof value === "number" && Number.isFinite(value)) {
    const normalizedEpoch = Math.abs(value) < 1e12 ? value * 1000 : value;
    const date = new Date(normalizedEpoch);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  if (typeof value === "string" && /^\d+(\.\d+)?$/.test(value.trim())) {
    const numericValue = Number(value);
    const normalizedEpoch = Math.abs(numericValue) < 1e12 ? numericValue * 1000 : numericValue;
    const date = new Date(normalizedEpoch);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function parseAirlineCode(flightNumber) {
  const code = normalizeFlightCode(flightNumber);
  if (code.length < 3) return null;
  const match = code.match(/^[A-Z0-9]{2,3}/);
  return match ? match[0] : null;
}

function calculateDelayMinutes(departureTimes) {
  if (!departureTimes?.scheduled || !departureTimes?.estimated) return null;
  const scheduled = new Date(departureTimes.scheduled).getTime();
  const estimated = new Date(departureTimes.estimated).getTime();
  if (!Number.isFinite(scheduled) || !Number.isFinite(estimated)) return null;
  return Math.max(0, Math.round((estimated - scheduled) / 60_000));
}

function finiteNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeLivePosition({
  latitude,
  longitude,
  headingDegrees,
  groundSpeedKnots,
  altitudeFeet,
  recordedAt,
}) {
  const lat = finiteNumberOrNull(latitude);
  const lon = finiteNumberOrNull(longitude);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }

  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }

  return {
    latitude: lat,
    longitude: lon,
    headingDegrees: finiteNumberOrNull(headingDegrees),
    groundSpeedKnots: finiteNumberOrNull(groundSpeedKnots),
    altitudeFeet: finiteNumberOrNull(altitudeFeet),
    recordedAt: isoOrNull(recordedAt),
  };
}

function normalizeLivePositionFromAviationstack(record) {
  const live = record?.live || {};
  const directKnots =
    finiteNumberOrNull(live.ground_speed_knots) ??
    finiteNumberOrNull(live.groundspeed_knots) ??
    finiteNumberOrNull(live.speed_knots);
  const horizontalKmh = finiteNumberOrNull(live.speed_horizontal);
  const groundSpeedKnots =
    directKnots ?? (horizontalKmh === null ? null : horizontalKmh * 0.539957);

  return normalizeLivePosition({
    latitude: live.latitude,
    longitude: live.longitude,
    headingDegrees: live.direction ?? live.heading,
    groundSpeedKnots,
    altitudeFeet: live.altitude,
    recordedAt: live.updated,
  });
}

function normalizeLivePositionFromFlightAware(record) {
  const lastPosition = record?.last_position || {};

  return normalizeLivePosition({
    latitude:
      lastPosition.latitude ??
      lastPosition.lat ??
      record?.latitude ??
      record?.lat,
    longitude:
      lastPosition.longitude ??
      lastPosition.lon ??
      lastPosition.lng ??
      record?.longitude ??
      record?.lon ??
      record?.lng,
    headingDegrees:
      lastPosition.heading ??
      lastPosition.track ??
      record?.heading ??
      record?.track,
    groundSpeedKnots:
      lastPosition.groundspeed ??
      lastPosition.ground_speed ??
      lastPosition.speed ??
      record?.groundspeed ??
      record?.ground_speed,
    altitudeFeet:
      lastPosition.altitude ??
      lastPosition.reported_altitude ??
      record?.altitude,
    recordedAt:
      lastPosition.date ??
      record?.updated ??
      record?.filed_time,
  });
}

const MAX_TRACK_POINTS = 1_800;
const MIN_TRACK_POINT_SPACING_MS = 15_000;
const MIN_TRACK_POINT_DISTANCE_METERS = 750;
const TRACK_POINT_REPLACE_WINDOW_MS = 12_000;
const TRACK_POINT_REPLACE_DISTANCE_METERS = 120;

function distanceBetweenCoordinatesMeters(left, right) {
  if (!left || !right) return Number.POSITIVE_INFINITY;

  const lat1 = Number(left.latitude) * Math.PI / 180;
  const lon1 = Number(left.longitude) * Math.PI / 180;
  const lat2 = Number(right.latitude) * Math.PI / 180;
  const lon2 = Number(right.longitude) * Math.PI / 180;

  if (![lat1, lon1, lat2, lon2].every(Number.isFinite)) {
    return Number.POSITIVE_INFINITY;
  }

  const deltaLat = lat2 - lat1;
  const deltaLon = lon2 - lon1;
  const haversine =
    Math.sin(deltaLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(deltaLon / 2) ** 2;

  return 2 * 6_371_000 * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
}

function normalizeTrackPoints(trackPoints) {
  if (!Array.isArray(trackPoints)) {
    return [];
  }

  return trackPoints
    .map((point) =>
      normalizeLivePosition({
        latitude: point?.latitude,
        longitude: point?.longitude,
        headingDegrees: point?.headingDegrees,
        groundSpeedKnots: point?.groundSpeedKnots,
        altitudeFeet: point?.altitudeFeet,
        recordedAt: point?.recordedAt,
      })
    )
    .filter(Boolean)
    .sort((left, right) => {
      const leftRecordedAt = new Date(left.recordedAt || 0).getTime();
      const rightRecordedAt = new Date(right.recordedAt || 0).getTime();
      return leftRecordedAt - rightRecordedAt;
    });
}

function compactTrackPoints(trackPoints) {
  const sortedPoints = normalizeTrackPoints(trackPoints);
  if (!sortedPoints.length) {
    return [];
  }

  const compacted = [];

  for (const point of sortedPoints) {
    if (!compacted.length) {
      compacted.push(point);
      continue;
    }

    const previousPoint = compacted[compacted.length - 1];
    const previousRecordedAtMs = new Date(previousPoint.recordedAt || 0).getTime();
    const nextRecordedAtMs = new Date(point.recordedAt || 0).getTime();
    const distanceMeters = distanceBetweenCoordinatesMeters(previousPoint, point);

    const hasComparableTimestamps =
      Number.isFinite(previousRecordedAtMs) && Number.isFinite(nextRecordedAtMs);
    const elapsedMs = hasComparableTimestamps
      ? nextRecordedAtMs - previousRecordedAtMs
      : Number.POSITIVE_INFINITY;

    if (
      (hasComparableTimestamps && nextRecordedAtMs === previousRecordedAtMs) ||
      (distanceMeters <= TRACK_POINT_REPLACE_DISTANCE_METERS &&
        (!hasComparableTimestamps || Math.abs(elapsedMs) <= TRACK_POINT_REPLACE_WINDOW_MS))
    ) {
      compacted[compacted.length - 1] = point;
      continue;
    }

    if (elapsedMs < MIN_TRACK_POINT_SPACING_MS && distanceMeters < MIN_TRACK_POINT_DISTANCE_METERS) {
      continue;
    }

    compacted.push(point);
  }

  return compacted.slice(-MAX_TRACK_POINTS);
}

function appendTrackPoint(trackPoints, livePosition) {
  const nextPoint = normalizeLivePosition(livePosition || {});
  if (!nextPoint) {
    return compactTrackPoints(trackPoints);
  }

  return compactTrackPoints([...(Array.isArray(trackPoints) ? trackPoints : []), nextPoint]);
}

function mergeTrackPoints(previousTrackPoints, nextTrackPoints, nextLivePosition = null) {
  let merged = [];

  for (const point of normalizeTrackPoints(previousTrackPoints)) {
    merged = appendTrackPoint(merged, point);
  }

  for (const point of normalizeTrackPoints(nextTrackPoints)) {
    merged = appendTrackPoint(merged, point);
  }

  merged = appendTrackPoint(merged, nextLivePosition);
  return merged.length ? merged : null;
}

function normalizeRecordFromAviationstack(record) {
  const airlineCode = record?.airline?.iata || record?.airline?.icao || null;
  const flightNumberRaw =
    record?.flight?.iata ||
    `${record?.flight?.airline_iata || ""}${record?.flight?.number || ""}`;

  const departure = record?.departure || {};
  const arrival = record?.arrival || {};
  const live = record?.live || {};

  const departureTimes = {
    scheduled: isoOrNull(departure.scheduled),
    estimated: isoOrNull(departure.estimated),
    actual: isoOrNull(departure.actual),
  };
  const takeoffTimes = {
    scheduled: isoOrNull(departure.scheduled_runway || departure.runway_scheduled),
    estimated: isoOrNull(departure.estimated_runway || departure.runway_estimated),
    actual: isoOrNull(departure.actual_runway || departure.runway_actual),
  };

  const arrivalTimes = {
    scheduled: isoOrNull(arrival.scheduled),
    estimated: isoOrNull(arrival.estimated),
    actual: isoOrNull(arrival.actual),
  };
  const landingTimes = {
    scheduled: isoOrNull(arrival.scheduled_runway || arrival.runway_scheduled),
    estimated: isoOrNull(arrival.estimated_runway || arrival.runway_estimated),
    actual: isoOrNull(arrival.actual_runway || arrival.runway_actual),
  };

  return {
    airlineCode,
    providerFlightId: null,
    flightNumber: normalizeFlightCode(flightNumberRaw),
    departureAirportIata: normalizeAirportCode(departure.iata),
    arrivalAirportIata: normalizeAirportCode(arrival.iata),
    departureTimes,
    takeoffTimes,
    landingTimes,
    arrivalTimes,
    status: normalizeStatus(record?.flight_status),
    terminal: departure.terminal || arrival.terminal || null,
    gate: departure.gate || arrival.gate || null,
    delayMinutes: calculateDelayMinutes(departureTimes),
    inboundFlight: null,
    recentHistory: [],
    alerts: null,
    progressPercent: finiteNumberOrNull(live.progress ?? live.progress_percent),
    livePosition: normalizeLivePositionFromAviationstack(record),
    trackPoints: [],
    provider: "aviationstack",
    lastUpdated: isoOrNull(live.updated) || new Date().toISOString(),
  };
}

function normalizeRecordFromFlightAware(record) {
  const flightNumber = normalizeFlightCode(
    record?.ident_iata ||
      record?.actual_ident_iata ||
      record?.ident ||
      record?.actual_ident ||
      record?.fa_flight_id ||
      record?.flight_number
  );

  const originIata =
    normalizeIataAirportCode(
      record?.origin?.code_iata ||
        record?.origin_iata ||
        record?.origin_lid ||
        record?.origin?.airport_code ||
        (typeof record?.origin === "string" ? record.origin : null)
    ) || null;

  const destinationIata =
    normalizeIataAirportCode(
      record?.destination?.code_iata ||
        record?.destination_iata ||
        record?.destination_lid ||
        record?.destination?.airport_code ||
        (typeof record?.destination === "string" ? record.destination : null)
    ) || null;

  const departureTimes = {
    scheduled: isoOrNull(record?.scheduled_out || record?.scheduled_departure_time || record?.filed_departure_time),
    estimated: isoOrNull(record?.estimated_out || record?.estimated_departure_time),
    actual: isoOrNull(record?.actual_out || record?.actual_departure_time),
  };
  const takeoffTimes = {
    scheduled: isoOrNull(record?.scheduled_off || record?.scheduled_takeoff_time),
    estimated: isoOrNull(record?.estimated_off || record?.estimated_takeoff_time),
    actual: isoOrNull(record?.actual_off || record?.actual_takeoff_time),
  };

  const arrivalTimes = {
    scheduled: isoOrNull(record?.scheduled_in || record?.scheduled_arrival_time),
    estimated: isoOrNull(record?.estimated_in || record?.estimated_arrival_time),
    actual: isoOrNull(record?.actual_in || record?.actual_arrival_time),
  };
  const landingTimes = {
    scheduled: isoOrNull(record?.scheduled_on || record?.scheduled_landing_time),
    estimated: isoOrNull(record?.estimated_on || record?.estimated_landing_time),
    actual: isoOrNull(record?.actual_on || record?.actual_landing_time),
  };

  const inboundFlightNumber = normalizeFlightCode(
    record?.inbound_ident_iata || record?.inbound_ident || record?.inbound_fa_flight_id
  );
  const inboundOrigin = normalizeAirportCode(
    record?.inbound_origin_iata ||
      record?.inbound_origin?.code_iata ||
      record?.inbound_origin ||
      record?.inbound_origin_airport
  );

  const inboundFlight = inboundFlightNumber || inboundOrigin
    ? {
        flightNumber: inboundFlightNumber || null,
        originAirportIata: inboundOrigin || null,
        estimatedArrival: isoOrNull(
          record?.inbound_estimated_in ||
            record?.inbound_estimated_arrival_time ||
            record?.inbound_scheduled_in
        ),
        status: record?.inbound_status ? normalizeStatus(record?.inbound_status) : null,
      }
    : null;

  const airlineCode =
    record?.operator_iata ||
    record?.airline_iata ||
    parseAirlineCode(record?.ident_iata || record?.actual_ident_iata) ||
    parseAirlineCode(flightNumber) ||
    null;

  return {
    airlineCode,
    providerFlightId: String(record?.fa_flight_id || "").trim() || null,
    flightNumber,
    departureAirportIata: originIata,
    arrivalAirportIata: destinationIata,
    departureTimes,
    takeoffTimes,
    landingTimes,
    arrivalTimes,
    status: normalizeStatus(record?.status || record?.flight_status),
    terminal:
      record?.terminal_origin ||
      record?.departure_terminal ||
      record?.terminal ||
      null,
    gate:
      record?.gate_origin ||
      record?.departure_gate ||
      record?.gate ||
      null,
    delayMinutes: calculateDelayMinutes(departureTimes),
    inboundFlight,
    recentHistory: [],
    alerts: null,
    progressPercent: finiteNumberOrNull(record?.progress_percent ?? record?.progress),
    livePosition: normalizeLivePositionFromFlightAware(record),
    trackPoints: [],
    provider: "flightaware",
    lastUpdated: isoOrNull(record?.last_position?.date || record?.updated || record?.filed_time) || new Date().toISOString(),
  };
}

function mergeTrackedFlightTimes(previousTimes, nextTimes) {
  const previous = previousTimes && typeof previousTimes === "object" ? previousTimes : {};
  const next = nextTimes && typeof nextTimes === "object" ? nextTimes : {};

  return {
    scheduled: next.scheduled || previous.scheduled || null,
    estimated: next.estimated || previous.estimated || null,
    actual: next.actual || previous.actual || null,
  };
}

function normalizeFirehoseAirportCode(value, fallback = null) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) {
    return fallback || null;
  }

  return raw.length === 3 ? raw : fallback || null;
}

function isoFromFirehoseValue(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  return isoOrNull(value);
}

function firehoseStatusFromMessage(message, previousStatus = "scheduled") {
  const type = firehoseMessageType(message);

  if (type === "cancellation") return "cancelled";
  if (type === "arrival" || type === "onblock") return "landed";
  if (type === "departure" || type === "offblock") return "departed";
  if (type === "position") {
    return String(message?.air_ground || "").trim().toUpperCase() === "G" ? "departed" : "enroute";
  }

  const statusCode = String(message?.status || "").trim().toUpperCase();
  if (statusCode === "X") return "cancelled";
  if (statusCode === "A") return "enroute";
  if (statusCode === "F") return "scheduled";

  if (message?.actual_in || message?.actual_on || message?.aat) {
    return "landed";
  }
  if (message?.actual_out || message?.actual_off || message?.adt) {
    return "departed";
  }

  return normalizeStatus(previousStatus);
}

function progressPercentFromNormalizedTimes(normalized, referenceMs = Date.now()) {
  if (isTerminalFlightStatus(normalized?.status)) {
    return 100;
  }

  const departureMs = new Date(
    normalized?.takeoffTimes?.actual ||
      normalized?.departureTimes?.actual ||
      normalized?.departureTimes?.estimated ||
      normalized?.departureTimes?.scheduled ||
      ""
  ).getTime();
  const arrivalMs = new Date(
    normalized?.arrivalTimes?.estimated ||
      normalized?.arrivalTimes?.scheduled ||
      normalized?.arrivalTimes?.actual ||
      ""
  ).getTime();

  if (!Number.isFinite(departureMs) || !Number.isFinite(arrivalMs) || arrivalMs <= departureMs) {
    return normalized?.status === "enroute" ? normalized?.progressPercent ?? null : null;
  }

  const progress = ((referenceMs - departureMs) / (arrivalMs - departureMs)) * 100;
  return Math.max(0, Math.min(progress, 100));
}

function pseudoFlightAwareRecordFromFirehoseMessage(message, previousNormalized) {
  const type = firehoseMessageType(message);
  const fallbackDepartureIata = normalizeAirportCode(previousNormalized?.departureAirportIata);
  const fallbackArrivalIata = normalizeAirportCode(previousNormalized?.arrivalAirportIata);
  const departureActual =
    message?.actual_out ||
    message?.adt ||
    (type === "departure" || type === "offblock" ? message?.clock : null);
  const arrivalActual =
    message?.actual_in ||
    message?.actual_on ||
    message?.aat ||
    (type === "arrival" || type === "onblock" ? message?.clock : null);

  const livePosition =
    type === "position"
      ? {
          latitude: message?.lat,
          longitude: message?.lon,
          heading: message?.heading_true || message?.heading || message?.heading_magnetic,
          groundspeed: message?.gs,
          altitude: message?.alt,
          date: message?.clock || message?.pitr,
        }
      : null;

  return {
    ident: message?.ident,
    ident_iata: message?.ident,
    fa_flight_id: firehoseMessageProviderFlightId(message),
    origin_iata: normalizeFirehoseAirportCode(message?.orig, fallbackDepartureIata),
    destination_iata: normalizeFirehoseAirportCode(message?.dest, fallbackArrivalIata),
    scheduled_out: message?.scheduled_out || message?.fdt || message?.scheduled_departure_time,
    estimated_out: message?.estimated_out || message?.edt || message?.estimated_departure_time,
    actual_out: departureActual,
    scheduled_off: message?.scheduled_off || message?.scheduled_takeoff_time,
    estimated_off: message?.estimated_off || message?.predicted_off || message?.estimated_takeoff_time,
    actual_off: message?.actual_off || null,
    scheduled_in: message?.scheduled_in || message?.scheduled_arrival_time,
    estimated_in: message?.estimated_in || message?.eta || message?.estimated_arrival_time,
    actual_in: arrivalActual,
    scheduled_on: message?.scheduled_on || message?.scheduled_landing_time,
    estimated_on: message?.estimated_on || message?.predicted_on || message?.estimated_landing_time,
    actual_on: message?.actual_on || null,
    status: firehoseStatusFromMessage(message, previousNormalized?.status),
    terminal_origin:
      message?.actual_departure_terminal ||
      message?.estimated_departure_terminal ||
      message?.scheduled_departure_terminal ||
      null,
    gate_origin:
      message?.actual_departure_gate ||
      message?.estimated_departure_gate ||
      message?.scheduled_departure_gate ||
      null,
    terminal:
      message?.actual_departure_terminal ||
      message?.estimated_departure_terminal ||
      message?.scheduled_departure_terminal ||
      message?.actual_arrival_terminal ||
      message?.estimated_arrival_terminal ||
      message?.scheduled_arrival_terminal ||
      null,
    gate:
      message?.actual_departure_gate ||
      message?.estimated_departure_gate ||
      message?.scheduled_departure_gate ||
      message?.actual_arrival_gate ||
      message?.estimated_arrival_gate ||
      message?.scheduled_arrival_gate ||
      null,
    progress_percent: previousNormalized?.progressPercent ?? null,
    last_position: livePosition,
    updated: isoFromFirehoseValue(message?.clock || message?.pitr),
    filed_time: isoFromFirehoseValue(message?.fdt || message?.scheduled_out),
  };
}

function normalizedFromFirehoseMessage(previousNormalized, message) {
  const pseudoRecord = pseudoFlightAwareRecordFromFirehoseMessage(message, previousNormalized);
  const firehoseNormalized = normalizeRecordFromFlightAware(pseudoRecord);
  const mergedDepartureTimes = mergeTrackedFlightTimes(
    previousNormalized?.departureTimes,
    firehoseNormalized?.departureTimes
  );
  const mergedTakeoffTimes = mergeTrackedFlightTimes(
    previousNormalized?.takeoffTimes,
    firehoseNormalized?.takeoffTimes
  );
  const mergedLandingTimes = mergeTrackedFlightTimes(
    previousNormalized?.landingTimes,
    firehoseNormalized?.landingTimes
  );
  const mergedArrivalTimes = mergeTrackedFlightTimes(
    previousNormalized?.arrivalTimes,
    firehoseNormalized?.arrivalTimes
  );
  const nextStatus = firehoseStatusFromMessage(message, previousNormalized?.status);
  const nextReferenceMs = firehoseMessageTimestampMs(message) || Date.now();
  const nextLivePosition =
    firehoseMessageType(message) === "position"
      ? firehoseNormalized.livePosition || previousNormalized?.livePosition || null
      : isTerminalFlightStatus(nextStatus)
        ? null
        : previousNormalized?.livePosition || null;
  const nextTrackPoints = mergeTrackPoints(
    previousNormalized?.trackPoints,
    firehoseNormalized?.trackPoints,
    nextLivePosition
  );

  const nextNormalized = reconcileOperationalStatus({
    ...previousNormalized,
    ...firehoseNormalized,
    departureAirportIata:
      firehoseNormalized.departureAirportIata ||
      previousNormalized?.departureAirportIata ||
      null,
    arrivalAirportIata:
      firehoseNormalized.arrivalAirportIata ||
      previousNormalized?.arrivalAirportIata ||
      null,
    departureTimes: mergedDepartureTimes,
    takeoffTimes: mergedTakeoffTimes,
    landingTimes: mergedLandingTimes,
    arrivalTimes: mergedArrivalTimes,
    status: nextStatus,
    terminal: firehoseNormalized.terminal || previousNormalized?.terminal || null,
    gate: firehoseNormalized.gate || previousNormalized?.gate || null,
    delayMinutes: calculateDelayMinutes(mergedDepartureTimes),
    inboundFlight: previousNormalized?.inboundFlight || null,
    recentHistory: previousNormalized?.recentHistory || [],
    livePosition: nextLivePosition,
    trackPoints: nextTrackPoints,
    provider: "flightaware",
    lastUpdated:
      nextLivePosition?.recordedAt ||
      firehoseNormalized.lastUpdated ||
      previousNormalized?.lastUpdated ||
      new Date().toISOString(),
  });

  nextNormalized.progressPercent = progressPercentFromNormalizedTimes(nextNormalized, nextReferenceMs);
  nextNormalized.alerts = deriveAlertFlags(previousNormalized, nextNormalized);

  return nextNormalized;
}

function makeProviderQueryKey(providerName, query) {
  return JSON.stringify({
    providerName,
    flightNumber: normalizeFlightCode(query.flightNumber),
    date: query.date || "",
    departureIata: (query.departureIata || "").toUpperCase(),
    arrivalIata: (query.arrivalIata || "").toUpperCase(),
    historical: query.historical === true,
    preferSchedules: query.preferSchedules === true,
  });
}

function dedupeFlightAwareRecords(records) {
  const seen = new Set();
  const deduped = [];

  for (const record of Array.isArray(records) ? records : []) {
    const normalized = normalizeRecordFromFlightAware(record);
    const key = [
      record?.fa_flight_id || normalized.flightNumber || "",
      normalized.departureAirportIata || "",
      normalized.arrivalAirportIata || "",
      normalized.departureTimes?.scheduled || normalized.departureTimes?.estimated || "",
    ].join("|");

    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(record);
  }

  return deduped;
}

function extractFlightAwareSearchRows(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  if (Array.isArray(payload.flights)) {
    return dedupeFlightAwareRecords(payload.flights);
  }

  const bucketNames = [
    "arrivals",
    "departures",
    "enroute",
    "scheduled",
    "scheduled_arrivals",
    "scheduled_departures",
    "data",
    "results",
  ];

  const rows = [];
  for (const bucketName of bucketNames) {
    if (Array.isArray(payload[bucketName])) {
      rows.push(...payload[bucketName]);
    }
  }

  return dedupeFlightAwareRecords(rows);
}

function flightAwareMatchableFlightCodes(record, normalizer) {
  const normalized = normalizer(record);
  const candidates = [
    normalized?.flightNumber,
    record?.ident_iata,
    record?.actual_ident_iata,
    record?.ident,
    record?.actual_ident,
    record?.flight_number,
  ];

  return Array.from(
    new Set(
      candidates
        .map((value) => normalizeFlightCode(value))
        .filter(Boolean)
    )
  );
}

function flightAwareDateWindow(queryDate) {
  const date = new Date(`${String(queryDate || "").slice(0, 10)}T12:00:00Z`);
  return Number.isFinite(date.getTime()) ? date.getTime() : null;
}

function normalizedQueryDateString(queryDate) {
  const normalizedDate = String(queryDate || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalizedDate) ? normalizedDate : null;
}

function currentUTCDateString(referenceTimeMs = Date.now()) {
  const reference = new Date(referenceTimeMs);
  return Number.isFinite(reference.getTime()) ? reference.toISOString().slice(0, 10) : null;
}

function isFutureFlightAwareQueryDate(queryDate, referenceTimeMs = Date.now()) {
  const normalizedDate = normalizedQueryDateString(queryDate);
  const currentDate = currentUTCDateString(referenceTimeMs);
  if (!normalizedDate || !currentDate) {
    return false;
  }

  return normalizedDate > currentDate;
}

function flightAwareHistoryBounds(queryDate) {
  const normalizedDate = normalizedQueryDateString(queryDate);
  if (!normalizedDate) {
    return null;
  }

  const startDate = new Date(`${normalizedDate}T00:00:00Z`);
  if (!Number.isFinite(startDate.getTime())) {
    return null;
  }

  const endDate = new Date(startDate);
  endDate.setUTCDate(endDate.getUTCDate() + 1);

  return {
    start: normalizedDate,
    end: endDate.toISOString().slice(0, 10),
  };
}

function shouldUseHistoricalFlightAwareSearch(query) {
  return query?.historical === true;
}

function shouldPrioritizeFlightAwareSchedules(query, referenceTimeMs = Date.now()) {
  return (
    query?.preferSchedules === true ||
    isFutureFlightAwareQueryDate(query?.date, referenceTimeMs) ||
    shouldPreferFlightAwareSchedules(query?.date, referenceTimeMs)
  );
}

function shouldPreferFlightAwareSchedules(queryDate, referenceTimeMs = Date.now()) {
  const targetMs = flightAwareDateWindow(queryDate);
  if (!Number.isFinite(targetMs)) {
    return false;
  }

  return Math.abs(targetMs - referenceTimeMs) > FLIGHTAWARE_SCHEDULE_WINDOW_MS;
}

function flightAwareScheduleBounds(queryDate) {
  const normalizedDate = String(queryDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) {
    return null;
  }

  return {
    start: `${normalizedDate}T00:00:00Z`,
    end: `${normalizedDate}T23:59:59Z`,
  };
}

function flightAwareScheduleQueryItems(query) {
  const params = new URLSearchParams();

  if (query.flightNumber) {
    const normalizedFlightNumber = normalizeFlightCode(query.flightNumber);
    const parts = normalizedFlightNumber.match(/^([A-Z0-9]{2,3}?)([0-9]{1,4}[A-Z]?)$/);
    if (parts) {
      params.set("airline", parts[1]);
      params.set("flight_number", parts[2]);
    }
  }

  if (query.departureIata) {
    params.set("origin", query.departureIata.toUpperCase());
  }

  if (query.arrivalIata) {
    params.set("destination", query.arrivalIata.toUpperCase());
  }

  return params;
}

async function fetchFlightAwareOperationalFlights(query) {
  const ident = normalizeFlightCode(query.flightNumber);
  const params = new URLSearchParams({ max_pages: "1" });

  if (query.date) {
    params.set("start", `${query.date}T00:00:00Z`);
    params.set("end", `${query.date}T23:59:59Z`);
  }

  let url;
  if (ident) {
    url = `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(ident)}?${params.toString()}`;
  } else if (query.departureIata && query.arrivalIata) {
    url =
      `${FLIGHTAWARE_BASE_URL}/airports/${encodeURIComponent(query.departureIata.toUpperCase())}` +
      `/flights/to/${encodeURIComponent(query.arrivalIata.toUpperCase())}?${params.toString()}`;
  } else {
    return [];
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  });

  if (response.status === 400 || response.status === 404) {
    return [];
  }

  if (!response.ok) {
    throw new Error(`Provider error (${response.status})`);
  }

  const payload = await response.json();
  return extractFlightAwareSearchRows(payload);
}

async function fetchFlightAwareScheduleFlights(query) {
  const bounds = flightAwareScheduleBounds(query.date);
  if (!bounds) {
    return [];
  }

  const params = flightAwareScheduleQueryItems(query);
  const queryString = params.toString();
  const url =
    `${FLIGHTAWARE_BASE_URL}/schedules/${encodeURIComponent(bounds.start)}` +
    `/${encodeURIComponent(bounds.end)}${queryString ? `?${queryString}` : ""}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  });

  if (response.status === 400 || response.status === 404) {
    return [];
  }

  if (!response.ok) {
    throw new Error(`Provider error (${response.status})`);
  }

  const payload = await response.json();
  return extractFlightAwareSearchRows(payload);
}

async function fetchFlightAwareHistoricalFlights(query) {
  const ident = normalizeFlightCode(query.flightNumber);
  const bounds = flightAwareHistoryBounds(query.date);
  if (!ident || !bounds) {
    return [];
  }

  const params = new URLSearchParams({
    ident_type: "designator",
    start: bounds.start,
    end: bounds.end,
    max_pages: "1",
  });

  const url =
    `${FLIGHTAWARE_BASE_URL}/history/flights/${encodeURIComponent(ident)}` +
    `?${params.toString()}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  });

  if (response.status === 400 || response.status === 404) {
    return [];
  }

  if (!response.ok) {
    throw new Error(`Provider error (${response.status})`);
  }

  const payload = await response.json();
  return extractFlightAwareSearchRows(payload);
}

async function fetchFlightAwareHistoricalRouteFlights(query) {
  const bounds = flightAwareHistoryBounds(query.date);
  const departureIata = normalizeAirportCode(query.departureIata);
  const arrivalIata = normalizeAirportCode(query.arrivalIata);
  if (!bounds || !departureIata || !arrivalIata) {
    return [];
  }

  const params = new URLSearchParams({
    start: bounds.start,
    end: bounds.end,
    max_pages: "1",
  });

  const url =
    `${FLIGHTAWARE_BASE_URL}/history/airports/${encodeURIComponent(departureIata)}` +
    `/flights/to/${encodeURIComponent(arrivalIata)}?${params.toString()}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  });

  if (response.status === 400 || response.status === 404) {
    return [];
  }

  if (!response.ok) {
    throw new Error(`Provider error (${response.status})`);
  }

  const payload = await response.json();
  return extractFlightAwareSearchRows(payload);
}

let providerAuthHealthCache = null;
let providerAuthHealthPromise = null;

function healthBuildInfo() {
  return {
    ...BUILD_INFO,
    gitCommitShort: BUILD_INFO.gitCommitSha ? BUILD_INFO.gitCommitSha.slice(0, 7) : null,
  };
}

function classifyFlightAwareAuthProbeResult({
  statusCode = null,
  checkedAt = new Date().toISOString(),
  error = null,
} = {}) {
  const normalizedError = error?.message || error || null;
  const base = {
    provider: "flightaware",
    endpoint: "schedules",
    checkedAt,
    ok: null,
    state: "unknown",
    statusCode,
    detail: null,
  };

  if (!PROVIDER_CALLS_ENABLED) {
    return {
      ...base,
      ok: null,
      state: "skipped",
      detail: "Provider calls are disabled.",
    };
  }

  if (!FLIGHTAWARE_API_KEY) {
    return {
      ...base,
      ok: false,
      state: "missing_api_key",
      detail: "FLIGHTAWARE_API_KEY is not configured.",
    };
  }

  if (normalizedError) {
    const lowered = String(normalizedError).toLowerCase();
    return {
      ...base,
      ok: null,
      state: lowered.includes("abort") || lowered.includes("timeout") ? "timeout" : "unreachable",
      detail: String(normalizedError),
    };
  }

  if (statusCode === 401 || statusCode === 403) {
    return {
      ...base,
      ok: false,
      state: "invalid_credentials",
      detail: "FlightAware rejected the configured credentials.",
    };
  }

  if (statusCode === 429) {
    return {
      ...base,
      ok: true,
      state: "rate_limited",
      detail: "FlightAware accepted the credentials but rate limited the probe.",
    };
  }

  if (statusCode >= 200 && statusCode < 500) {
    return {
      ...base,
      ok: true,
      state: "ok",
      detail: "FlightAware accepted the schedules probe.",
    };
  }

  if (statusCode >= 500) {
    return {
      ...base,
      ok: null,
      state: "upstream_error",
      detail: "FlightAware returned a server error for the schedules probe.",
    };
  }

  return base;
}

async function probeFlightAwareAuthHealth() {
  const checkedAt = new Date().toISOString();
  const bounds = flightAwareScheduleBounds(new Date().toISOString().slice(0, 10));
  if (!bounds) {
    return classifyFlightAwareAuthProbeResult({
      checkedAt,
      error: "Unable to build FlightAware schedule health-check bounds.",
    });
  }

  const params = new URLSearchParams([
    ["ident", "__RUNWY_HEALTHCHECK__"],
    ["max_pages", "1"],
  ]);
  const url =
    `${FLIGHTAWARE_BASE_URL}/schedules/${encodeURIComponent(bounds.start)}` +
    `/${encodeURIComponent(bounds.end)}?${params.toString()}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 4_000);

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "x-apikey": FLIGHTAWARE_API_KEY,
        Accept: "application/json",
      },
      signal: controller.signal,
    });

    return classifyFlightAwareAuthProbeResult({
      statusCode: response.status,
      checkedAt,
    });
  } catch (error) {
    return classifyFlightAwareAuthProbeResult({
      checkedAt,
      error,
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function getProviderAuthHealth() {
  if (FLIGHT_DATA_PROVIDER !== "flightaware") {
    return {
      provider: FLIGHT_DATA_PROVIDER,
      endpoint: null,
      checkedAt: new Date().toISOString(),
      ok: null,
      state: PROVIDER_CALLS_ENABLED ? "not_implemented" : "skipped",
      statusCode: null,
      detail: PROVIDER_CALLS_ENABLED
        ? `No auth probe is implemented for provider ${FLIGHT_DATA_PROVIDER}.`
        : "Provider calls are disabled.",
    };
  }

  const now = Date.now();
  if (
    providerAuthHealthCache &&
    now - providerAuthHealthCache.cachedAtMs < HEALTH_PROVIDER_AUTH_CACHE_TTL_MS
  ) {
    return {
      ...providerAuthHealthCache.result,
      cached: true,
      cacheTtlMs: HEALTH_PROVIDER_AUTH_CACHE_TTL_MS,
    };
  }

  if (!providerAuthHealthPromise) {
    providerAuthHealthPromise = (async () => {
      const result = await probeFlightAwareAuthHealth();
      providerAuthHealthCache = {
        cachedAtMs: Date.now(),
        result,
      };
      return result;
    })().finally(() => {
      providerAuthHealthPromise = null;
    });
  }

  const result = await providerAuthHealthPromise;
  return {
    ...result,
    cached: false,
    cacheTtlMs: HEALTH_PROVIDER_AUTH_CACHE_TTL_MS,
  };
}

function makeProviderPositionKey(providerName, providerFlightId) {
  return JSON.stringify({
    providerName,
    providerFlightId: String(providerFlightId || "").trim(),
    kind: "live-position",
  });
}

function makeProviderTrackKey(providerName, providerFlightId) {
  return JSON.stringify({
    providerName,
    providerFlightId: String(providerFlightId || "").trim(),
    kind: "flight-track",
  });
}

async function withProviderRequestDedup(cacheKey, loader) {
  const existingRequest = providerInFlightRequests.get(cacheKey);
  if (existingRequest) {
    return existingRequest;
  }

  const request = (async () => {
    try {
      return await loader();
    } finally {
      providerInFlightRequests.delete(cacheKey);
    }
  })();

  providerInFlightRequests.set(cacheKey, request);
  return request;
}

function toEpochMillisOrZero(value) {
  const epochMs = new Date(value || 0).getTime();
  return Number.isFinite(epochMs) ? epochMs : 0;
}

function isTrackableLiveStatus(status) {
  return ["boarding", "departed", "enroute", "delayed"].includes(String(status || "").toLowerCase());
}

function isTerminalFlightStatus(status) {
  return ["landed", "cancelled", "diverted"].includes(String(status || "").toLowerCase());
}

function normalizeFlightAwareTrackPoint(record) {
  if (!record || typeof record !== "object") return null;

  const coordinates = Array.isArray(record?.geometry?.coordinates)
    ? record.geometry.coordinates
    : Array.isArray(record?.coordinates)
      ? record.coordinates
      : null;

  const properties = record?.properties && typeof record.properties === "object"
    ? record.properties
    : {};

  return normalizeLivePosition({
    latitude: record.latitude ?? record.lat ?? properties.latitude ?? properties.lat ?? coordinates?.[1],
    longitude:
      record.longitude ??
      record.lon ??
      record.lng ??
      properties.longitude ??
      properties.lon ??
      properties.lng ??
      coordinates?.[0],
    headingDegrees:
      record.heading ??
      record.track ??
      record.direction ??
      properties.heading ??
      properties.track ??
      properties.direction,
    groundSpeedKnots:
      record.groundspeed ??
      record.ground_speed ??
      record.speed ??
      properties.groundspeed ??
      properties.ground_speed ??
      properties.speed,
    altitudeFeet:
      record.altitude ??
      record.reported_altitude ??
      properties.altitude ??
      properties.reported_altitude,
    recordedAt:
      record.timestamp ??
      record.recorded_at ??
      record.date ??
      record.observed ??
      properties.timestamp ??
      properties.recorded_at ??
      properties.date,
  });
}

function flightAwareTrackCandidatesFromPayload(payload) {
  if (!payload || typeof payload !== "object") return [];

  const candidates = [];

  if (Array.isArray(payload.positions)) candidates.push(...payload.positions);
  if (Array.isArray(payload.track)) candidates.push(...payload.track);
  if (payload.position && typeof payload.position === "object") candidates.push(payload.position);
  if (payload.last_position && typeof payload.last_position === "object") candidates.push(payload.last_position);
  if (Array.isArray(payload.features)) candidates.push(...payload.features);
  if (payload.geometry && payload.properties) candidates.push(payload);

  return candidates;
}

function latestTrackPoint(trackPoints) {
  const normalizedPoints = normalizeTrackPoints(trackPoints);
  return normalizedPoints.length > 0 ? normalizedPoints[normalizedPoints.length - 1] : null;
}

function flightAwareTrackSeedMetadata({ providerFlightId, source, fetchedTrackPoints, livePosition }) {
  const latestPoint = latestTrackPoint(fetchedTrackPoints) || livePosition || null;

  return {
    flightawareTrackTrail: {
      providerFlightId: String(providerFlightId || "").trim() || null,
      source: String(source || "").trim() || "unknown",
      requestedAt: new Date().toISOString(),
      fetchedPointCount: Array.isArray(fetchedTrackPoints) ? fetchedTrackPoints.length : 0,
      latestPointRecordedAt: latestPoint?.recordedAt || null,
    },
  };
}

function shouldSeedFlightAwareTrackTrail({
  normalized,
  providerName,
  providerFlightId,
  metadata,
}) {
  if (providerName !== "flightaware") {
    return false;
  }

  const normalizedProviderFlightId = String(providerFlightId || "").trim();
  if (!normalizedProviderFlightId || isTerminalFlightStatus(normalized?.status)) {
    return false;
  }

  const looksAirborneOrLive = Boolean(
    normalized?.livePosition ||
      normalized?.takeoffTimes?.actual ||
      normalized?.departureTimes?.actual ||
      ["departed", "enroute"].includes(String(normalized?.status || "").toLowerCase())
  );
  if (!looksAirborneOrLive) {
    return false;
  }

  if (Array.isArray(normalized?.trackPoints) && normalized.trackPoints.length > 1) {
    return false;
  }

  const priorSeed = metadata?.flightawareTrackTrail;
  if (
    String(priorSeed?.providerFlightId || "").trim() === normalizedProviderFlightId &&
    String(priorSeed?.requestedAt || "").trim()
  ) {
    const priorSeedRequestedAtMs = new Date(priorSeed.requestedAt).getTime();
    if (
      Number.isFinite(priorSeedRequestedAtMs) &&
      Date.now() - priorSeedRequestedAtMs < FLIGHTAWARE_POSITION_CACHE_TTL_MS
    ) {
      return false;
    }
  }

  return true;
}

function mergeFlightAwareTrackTrailIntoNormalized(normalized, { trackPoints, livePosition } = {}) {
  const mergedTrackPoints = mergeTrackPoints(normalized?.trackPoints, trackPoints, livePosition || null);
  const latestMergedTrackPoint = latestTrackPoint(mergedTrackPoints);
  const liveCandidates = [normalized?.livePosition, livePosition, latestMergedTrackPoint]
    .filter(Boolean)
    .sort((left, right) => toEpochMillisOrZero(right?.recordedAt) - toEpochMillisOrZero(left?.recordedAt));
  const mergedLivePosition = liveCandidates[0] || null;

  return reconcileOperationalStatus({
    ...normalized,
    livePosition: mergedLivePosition,
    trackPoints: mergedTrackPoints,
    lastUpdated:
      mergedLivePosition?.recordedAt ||
      latestMergedTrackPoint?.recordedAt ||
      normalized?.lastUpdated ||
      new Date().toISOString(),
  });
}

async function fetchFlightAwareTrackTrail(providerFlightId) {
  if (!PROVIDER_CALLS_ENABLED) {
    return { trackPoints: [], livePosition: null };
  }

  const normalizedFlightId = String(providerFlightId || "").trim();
  if (!normalizedFlightId) {
    return { trackPoints: [], livePosition: null };
  }

  const cacheKey = makeProviderTrackKey("flightaware", normalizedFlightId);
  const now = Date.now();
  const cached = providerCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(cacheKey, async () => {
    const response = await fetch(
      `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(normalizedFlightId)}/track`,
      {
        method: "GET",
        headers: {
          "x-apikey": FLIGHTAWARE_API_KEY,
          Accept: "application/json",
        },
      }
    );

    if ([400, 401, 403, 404].includes(response.status)) {
      const emptyData = { trackPoints: [], livePosition: null };
      providerCache.set(cacheKey, {
        data: emptyData,
        expiresAt: now + FLIGHTAWARE_POSITION_CACHE_TTL_MS,
      });
      enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
      return emptyData;
    }

    if (!response.ok) {
      throw new Error(`Provider track error (${response.status})`);
    }

    let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      payload = null;
    }

    const trackPoints = compactTrackPoints(
      flightAwareTrackCandidatesFromPayload(payload)
        .map(normalizeFlightAwareTrackPoint)
        .filter(Boolean)
    );
    const data = {
      trackPoints,
      livePosition: latestTrackPoint(trackPoints),
    };

    providerCache.set(cacheKey, {
      data,
      expiresAt: now + FLIGHTAWARE_POSITION_CACHE_TTL_MS,
    });
    enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
    return data;
  });
}

async function fetchFlightAwareLivePosition(providerFlightId) {
  if (!PROVIDER_CALLS_ENABLED) {
    return null;
  }

  const normalizedFlightId = String(providerFlightId || "").trim();
  if (!normalizedFlightId) return null;

  const cacheKey = makeProviderPositionKey("flightaware", normalizedFlightId);
  const now = Date.now();
  const cached = providerCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(cacheKey, async () => {
    const endpointPaths = [
      `/flights/${encodeURIComponent(normalizedFlightId)}/position`,
      `/flights/${encodeURIComponent(normalizedFlightId)}/track`,
    ];

    if (FLIGHTAWARE_ENABLE_MAP_FALLBACK) {
      endpointPaths.push(`/flights/${encodeURIComponent(normalizedFlightId)}/map`);
    }

    for (const path of endpointPaths) {
      const response = await fetch(`${FLIGHTAWARE_BASE_URL}${path}`, {
        method: "GET",
        headers: {
          "x-apikey": FLIGHTAWARE_API_KEY,
          Accept: "application/json",
        },
      });

      if ([400, 401, 403, 404].includes(response.status)) {
        continue;
      }

      if (!response.ok) {
        throw new Error(`Provider position error (${response.status})`);
      }

      let payload = null;
      try {
        payload = await response.json();
      } catch (_error) {
        continue;
      }

      const latestPosition =
        flightAwareTrackCandidatesFromPayload(payload)
          .map(normalizeFlightAwareTrackPoint)
          .filter(Boolean)
          .sort((left, right) => toEpochMillisOrZero(right.recordedAt) - toEpochMillisOrZero(left.recordedAt))[0] ||
        null;

      if (latestPosition) {
        providerCache.set(cacheKey, {
          data: latestPosition,
          expiresAt: now + FLIGHTAWARE_POSITION_CACHE_TTL_MS,
        });
        enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
        return latestPosition;
      }
    }

    providerCache.set(cacheKey, {
      data: null,
      expiresAt: now + FLIGHTAWARE_POSITION_CACHE_TTL_MS,
    });
    enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
    return null;
  });
}

async function maybeBuildFlightAwareTrackTrailSeed({
  trackedRecord = null,
  normalized,
  providerName,
  rawRecord,
  source,
}) {
  const providerFlightId = String(
    providerFlightIdentifier(rawRecord, providerName) || trackedRecord?.providerFlightId || ""
  ).trim();

  if (
    !shouldSeedFlightAwareTrackTrail({
      normalized,
      providerName,
      providerFlightId,
      metadata: trackedRecord?.metadata,
    })
  ) {
    return { normalized, metadataPatch: null };
  }

  try {
    const trackTrail = await fetchFlightAwareTrackTrail(providerFlightId);
    return {
      normalized: mergeFlightAwareTrackTrailIntoNormalized(normalized, trackTrail),
      metadataPatch: flightAwareTrackSeedMetadata({
        providerFlightId,
        source,
        fetchedTrackPoints: trackTrail.trackPoints,
        livePosition: trackTrail.livePosition,
      }),
    };
  } catch (error) {
    console.warn(
      `FlightAware track trail lookup failed for ${providerFlightId}: ${error?.message || String(error)}`
    );
    return { normalized, metadataPatch: null };
  }
}

function sortRecordsByDepartureDesc(records, normalizer) {
  return [...records].sort((a, b) => {
    const normalizedA = normalizer(a);
    const normalizedB = normalizer(b);

    const depA = normalizedA?.departureTimes?.scheduled || normalizedA?.departureTimes?.estimated;
    const depB = normalizedB?.departureTimes?.scheduled || normalizedB?.departureTimes?.estimated;

    const timeA = depA ? new Date(depA).getTime() : 0;
    const timeB = depB ? new Date(depB).getTime() : 0;
    return timeB - timeA;
  });
}

function scoreCandidate(record, query, normalizer) {
  let score = 0;
  const normalized = normalizer(record);
  const wantedFlight = normalizeFlightCode(query.flightNumber);

  const candidateFlightCodes = flightAwareMatchableFlightCodes(record, normalizer);
  if (wantedFlight && candidateFlightCodes.includes(wantedFlight)) score += 6;

  if (
    normalized.departureAirportIata &&
    query.departureIata &&
    normalized.departureAirportIata.toUpperCase() === query.departureIata.toUpperCase()
  ) score += 2;

  if (
    normalized.arrivalAirportIata &&
    query.arrivalIata &&
    normalized.arrivalAirportIata.toUpperCase() === query.arrivalIata.toUpperCase()
  ) score += 2;

  const depDate = normalized.departureTimes?.scheduled?.slice(0, 10);
  const arrDate = normalized.arrivalTimes?.scheduled?.slice(0, 10);
  if (query.date && (depDate === query.date || arrDate === query.date)) score += 2;

  if (normalized.status === "enroute" || normalized.status === "boarding") score += 1;
  return score;
}

function departureTimeForRecord(record, normalizer) {
  const normalized = normalizer(record);
  return (
    normalized?.departureTimes?.estimated ||
    normalized?.departureTimes?.scheduled ||
    normalized?.departureTimes?.actual ||
    ""
  );
}

function sortSearchRecords(records, query, normalizer) {
  return [...records].sort((a, b) => {
    const scoreDelta = scoreCandidate(b, query, normalizer) - scoreCandidate(a, query, normalizer);
    if (scoreDelta !== 0) {
      return scoreDelta;
    }

    const departureA = new Date(departureTimeForRecord(a, normalizer) || 0).getTime();
    const departureB = new Date(departureTimeForRecord(b, normalizer) || 0).getTime();
    return departureA - departureB;
  });
}

function bestMatch(records, query, normalizer) {
  if (!records.length) return null;
  return sortSearchRecords(records, query, normalizer)[0];
}

function deriveRecentHistory(records, selectedRecord, normalizer) {
  if (!records.length) return [];

  const selectedNorm = selectedRecord ? normalizer(selectedRecord) : null;
  return sortRecordsByDepartureDesc(records, normalizer)
    .map(normalizer)
    .filter((item) => item.flightNumber)
    .filter((item) => {
      if (!selectedNorm) return true;
      const selectedScheduled = selectedNorm.departureTimes?.scheduled || selectedNorm.departureTimes?.estimated;
      const candidateScheduled = item.departureTimes?.scheduled || item.departureTimes?.estimated;
      return !(item.flightNumber === selectedNorm.flightNumber && selectedScheduled === candidateScheduled);
    })
    .slice(0, 5)
    .map((item) => ({
      flightNumber: item.flightNumber,
      departureAirportIata: item.departureAirportIata,
      arrivalAirportIata: item.arrivalAirportIata,
      departureTime: item.departureTimes?.actual || item.departureTimes?.estimated || item.departureTimes?.scheduled || null,
      arrivalTime: item.arrivalTimes?.actual || item.arrivalTimes?.estimated || item.arrivalTimes?.scheduled || null,
      status: item.status,
    }));
}

function deriveAlertFlags(previousNormalized, nextNormalized) {
  if (!previousNormalized || !nextNormalized) {
    return {
      statusChanged: false,
      delayedNow: false,
      cancelledNow: false,
      gateChangedNow: false,
      previousStatus: previousNormalized?.status || null,
      currentStatus: nextNormalized?.status || null,
    };
  }

  const previousDelay = Number(previousNormalized.delayMinutes || 0);
  const nextDelay = Number(nextNormalized.delayMinutes || 0);
  const previousGate = `${previousNormalized.gate || ""}`.trim();
  const nextGate = `${nextNormalized.gate || ""}`.trim();

  return {
    statusChanged: previousNormalized.status !== nextNormalized.status,
    delayedNow:
      nextNormalized.status === "delayed" &&
      (previousNormalized.status !== "delayed" || nextDelay > previousDelay),
    cancelledNow:
      nextNormalized.status === "cancelled" && previousNormalized.status !== "cancelled",
    gateChangedNow:
      Boolean(nextGate) &&
      previousGate !== nextGate,
    previousStatus: previousNormalized.status || null,
    currentStatus: nextNormalized.status || null,
  };
}

async function fetchAviationstackFlights(query) {
  if (!PROVIDER_CALLS_ENABLED) {
    return [];
  }

  const key = makeProviderQueryKey("aviationstack", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(key, async () => {
    const params = new URLSearchParams({
      access_key: AVIATIONSTACK_KEY,
      limit: "25",
    });

    const flightCode = normalizeFlightCode(query.flightNumber);
    if (flightCode) params.set("flight_iata", flightCode);
    if (query.date) params.set("flight_date", query.date);
    if (query.departureIata) params.set("dep_iata", query.departureIata.toUpperCase());
    if (query.arrivalIata) params.set("arr_iata", query.arrivalIata.toUpperCase());

    const url = `${AVIATIONSTACK_BASE_URL}/flights?${params.toString()}`;
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      throw new Error(`Provider error (${response.status})`);
    }

    const payload = await response.json();
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    if (providerCache.has(key)) {
      providerCache.delete(key);
    }
    providerCache.set(key, {
      data: rows,
      expiresAt: now + CACHE_TTL_MS,
    });
    enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);

    return rows;
  });
}

async function fetchFlightAwareFlights(query) {
  if (!PROVIDER_CALLS_ENABLED) {
    return [];
  }

  const key = makeProviderQueryKey("flightaware", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(key, async () => {
    let rows = [];

    if (shouldUseHistoricalFlightAwareSearch(query)) {
      rows = query.flightNumber
        ? await fetchFlightAwareHistoricalFlights(query)
        : await fetchFlightAwareHistoricalRouteFlights(query);
    } else {
      const prioritizeSchedules = shouldPrioritizeFlightAwareSchedules(query);
      const fetchers = prioritizeSchedules
        ? [fetchFlightAwareScheduleFlights, fetchFlightAwareOperationalFlights]
        : [fetchFlightAwareOperationalFlights, fetchFlightAwareScheduleFlights];

      if (prioritizeSchedules) {
        const mergedRows = [];
        for (const fetcher of fetchers) {
          const nextRows = await fetcher(query);
          if (nextRows.length > 0) {
            mergedRows.push(...nextRows);
          }
        }
        rows = dedupeFlightAwareRecords(mergedRows);
      } else {
        for (const fetcher of fetchers) {
          rows = await fetcher(query);
          if (rows.length > 0) {
            break;
          }
        }
      }
    }

    if (providerCache.has(key)) {
      providerCache.delete(key);
    }
    providerCache.set(key, {
      data: rows,
      expiresAt: now + CACHE_TTL_MS,
    });
    enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);

    return rows;
  });
}

function providerAdapter(preferredProvider = FLIGHT_DATA_PROVIDER) {
  if (preferredProvider === "flightaware") {
    return {
      name: "flightaware",
      fetchFlights: fetchFlightAwareFlights,
      normalizeRecord: normalizeRecordFromFlightAware,
    };
  }

  return {
    name: "aviationstack",
    fetchFlights: fetchAviationstackFlights,
    normalizeRecord: normalizeRecordFromAviationstack,
  };
}

function normalizeWithContext(record, records, query, normalizer, previousNormalized = null) {
  const normalized = reconcileOperationalStatus(normalizer(record));
  const recentHistory = deriveRecentHistory(records, record, normalizer);
  const alerts = deriveAlertFlags(previousNormalized, normalized);

  return reconcileOperationalStatus(mergeRealtimeTelemetry(previousNormalized, {
    ...normalized,
    recentHistory,
    alerts,
    provider: FLIGHT_DATA_PROVIDER,
  }));
}

const trackingStore = createTrackingStore({
  pool,
  memoryTrackedFlights,
  memoryPushDevices,
  maxMemoryTrackedFlights: MAX_MEMORY_TRACKED_FLIGHTS,
  maxMemoryPushDevices: MAX_MEMORY_PUSH_DEVICES,
  defaultPollerBatchSize: POLLER_BATCH_SIZE,
  maxActiveTrackingSessionsPerUser: MAX_ACTIVE_TRACKING_SESSIONS_PER_USER,
  providerName: FLIGHT_DATA_PROVIDER,
  normalizeFlightCode,
  normalizeAirportCode,
  parseAirlineCode,
  displayFlightCode,
  enforceMapSizeLimit,
});

const {
  createOrReuseTrackingSession,
  disablePushToken,
  disablePushTokensForDevice,
  ensureDatabaseSchema,
  fetchTrackingSessionStatusSummary,
  fetchAccessibleTrackingRow,
  fetchTrackingRowByID,
  listFirehoseTrackedRows,
  listDueTrackingRows,
  listTrackedFlightsByProviderFlightId,
  listTrackedFlightsByFlightNumber,
  markTrackingRowErrored,
  mergeTrackingSessionMetadata,
  persistTrackingSnapshot,
  providerFlightIdentifier,
  upsertPushDevice,
  upsertTrackedFlightRecord,
  usesDatabase,
} = trackingStore;

async function enrichNormalizedWithLivePosition(normalized, providerName, rawRecord) {
  if (providerName !== "flightaware" || !isTrackableLiveStatus(normalized?.status)) {
    return normalized;
  }

  const providerFlightId = providerFlightIdentifier(rawRecord, providerName);
  if (!providerFlightId) {
    return normalized;
  }

  try {
    const livePosition = await fetchFlightAwareLivePosition(providerFlightId);
    if (!livePosition) {
      return normalized;
    }

    const trackPoints = mergeTrackPoints(normalized?.trackPoints, null, livePosition);

    return {
      ...reconcileOperationalStatus({
        ...normalized,
        livePosition,
        trackPoints,
      }),
    };
  } catch (error) {
    console.warn(
      `FlightAware live position lookup failed for ${providerFlightId}: ${error?.message || String(error)}`
    );
    return normalized;
  }
}

function addDaysToISODate(dateString, dayOffset) {
  const date = new Date(`${String(dateString || "").slice(0, 10)}T00:00:00Z`);
  if (!Number.isFinite(date.getTime())) {
    return null;
  }

  date.setUTCDate(date.getUTCDate() + dayOffset);
  return date.toISOString().slice(0, 10);
}

function flightAwareAlertContextForTrackedRecord(trackedRecord) {
  if (!trackedRecord || String(trackedRecord.provider || "").toLowerCase() !== "flightaware") {
    return null;
  }

  const status = String(trackedRecord.normalized?.status || "").toLowerCase();
  if (["landed", "cancelled", "diverted"].includes(status)) {
    return null;
  }

  const flightNumber = normalizeFlightCode(
    trackedRecord.query?.flightNumber || trackedRecord.normalized?.flightNumber
  );
  const departureIata =
    normalizeAirportCode(trackedRecord.query?.departureIata || trackedRecord.normalized?.departureAirportIata) || null;
  const arrivalIata =
    normalizeAirportCode(trackedRecord.query?.arrivalIata || trackedRecord.normalized?.arrivalAirportIata) || null;
  const startDate =
    String(
      trackedRecord.query?.date ||
        trackedRecord.normalized?.departureTimes?.scheduled?.slice(0, 10) ||
        trackedRecord.normalized?.departureTimes?.estimated?.slice(0, 10) ||
        ""
    ).slice(0, 10) || null;

  if (!flightNumber || !startDate) {
    return null;
  }

  const endDate = addDaysToISODate(startDate, 2);
  if (!endDate) {
    return null;
  }

  return {
    flightNumber,
    departureIata,
    arrivalIata,
    startDate,
    endDate,
  };
}

function flightAwareAlertFingerprint(context) {
  return crypto.createHash("sha256").update(JSON.stringify(context)).digest("hex");
}

function flightAwareAlertIDFromPayload(payload) {
  const candidates = [
    payload?.alert_id,
    payload?.alertId,
    payload?.id,
    payload?.alert?.alert_id,
    payload?.alert?.alertId,
    payload?.alert?.id,
    payload?.alerts?.[0]?.alert_id,
    payload?.alerts?.[0]?.alertId,
    payload?.alerts?.[0]?.id,
  ];

  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim();
    if (normalized) {
      return normalized;
    }
  }

  return null;
}

async function createFlightAwareAlert({ targetUrl, context }) {
  const payload = {
    ident_iata: context.flightNumber,
    start: context.startDate,
    end: context.endDate,
    impending_departure: [...FLIGHTAWARE_AUTO_ALERT_IMPENDING_DEPARTURE_MINUTES],
    impending_arrival: [...FLIGHTAWARE_AUTO_ALERT_IMPENDING_ARRIVAL_MINUTES],
    events: FLIGHTAWARE_AUTO_ALERT_EVENTS,
    target_url: targetUrl,
  };

  if (context.departureIata) {
    payload.origin_iata = context.departureIata;
  }
  if (context.arrivalIata) {
    payload.destination_iata = context.arrivalIata;
  }

  const response = await fetch(`${FLIGHTAWARE_BASE_URL}/alerts`, {
    method: "POST",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`FlightAware alert create failed (${response.status}): ${responseText.slice(0, 200)}`);
  }

  let responsePayload = null;
  try {
    responsePayload = responseText ? JSON.parse(responseText) : null;
  } catch (_error) {
    responsePayload = null;
  }

  return {
    alertId: flightAwareAlertIDFromPayload(responsePayload),
  };
}

async function ensureFlightAwareAlertForTrackedSession(req, trackedRecord) {
  if (!usesDatabase() || !PROVIDER_CALLS_ENABLED) {
    return;
  }

  const targetUrl = flightAwareWebhookTargetURL(req);
  if (!targetUrl) {
    return;
  }

  const context = flightAwareAlertContextForTrackedRecord(trackedRecord);
  if (!context) {
    return;
  }

  const fingerprint = flightAwareAlertFingerprint(context);
  const existing = trackedRecord.metadata?.flightawareAlert;
  if (existing?.fingerprint === fingerprint && (existing?.alertId || existing?.createdAt)) {
    return;
  }

  const baseMetadata = {
    autoCreated: true,
    provider: "flightaware",
    fingerprint,
    startDate: context.startDate,
    endDate: context.endDate,
    flightNumber: context.flightNumber,
    departureIata: context.departureIata,
    arrivalIata: context.arrivalIata,
    lastAttemptAt: new Date().toISOString(),
  };

  try {
    const createdAlert = await createFlightAwareAlert({ targetUrl, context });
    await mergeTrackingSessionMetadata(trackedRecord.flightId, {
      flightawareAlert: {
        ...baseMetadata,
        alertId: createdAlert.alertId,
        createdAt: new Date().toISOString(),
        lastError: null,
      },
    });
  } catch (error) {
    console.warn(
      `FlightAware alert create failed for ${trackedRecord.flightId}: ${error?.message || String(error)}`
    );
    await mergeTrackingSessionMetadata(trackedRecord.flightId, {
      flightawareAlert: {
        ...baseMetadata,
        alertId: existing?.alertId || null,
        createdAt: existing?.createdAt || null,
        lastError: String(error?.message || error || "").slice(0, 256),
      },
    });
  }
}

function apnsPrivateKeyMaterial() {
  if (APNS_PRIVATE_KEY_BASE64) {
    return Buffer.from(APNS_PRIVATE_KEY_BASE64, "base64").toString("utf8");
  }

  if (APNS_PRIVATE_KEY) {
    return APNS_PRIVATE_KEY.replace(/\\n/g, "\n");
  }

  return "";
}

function isApnsConfigured() {
  return Boolean(APNS_KEY_ID && APNS_TEAM_ID && APNS_BUNDLE_ID && apnsPrivateKeyMaterial());
}

function base64UrlEncode(input) {
  const buffer = Buffer.isBuffer(input) ? input : Buffer.from(input);
  return buffer
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function apnsAuthToken() {
  const now = Math.floor(Date.now() / 1000);
  if (apnsTokenCache.token && apnsTokenCache.expiresAt > now + 60) {
    return apnsTokenCache.token;
  }

  const header = base64UrlEncode(
    JSON.stringify({
      alg: "ES256",
      kid: APNS_KEY_ID,
    })
  );

  const payload = base64UrlEncode(
    JSON.stringify({
      iss: APNS_TEAM_ID,
      iat: now,
    })
  );

  const signingInput = `${header}.${payload}`;
  const signature = crypto.sign("sha256", Buffer.from(signingInput), apnsPrivateKeyMaterial());
  const jwt = `${signingInput}.${base64UrlEncode(signature)}`;

  apnsTokenCache.token = jwt;
  apnsTokenCache.expiresAt = now + 50 * 60;
  return jwt;
}

function apnsHost() {
  return APNS_USE_SANDBOX ? "api.sandbox.push.apple.com" : "api.push.apple.com";
}

function displayFlightCode(normalized) {
  const number = normalizeFlightCode(normalized.flightNumber);
  const airline = normalizeFlightCode(normalized.airlineCode);

  if (airline && number && !number.startsWith(airline)) {
    return `${airline}${number}`;
  }

  return number || "Flight";
}

function notificationPayloadFor(normalized, flightId) {
  const alerts = normalized?.alerts;
  if (!alerts) return null;

  const route = `${normalized?.departureAirportIata || "---"} → ${normalized?.arrivalAirportIata || "---"}`;
  const code = displayFlightCode(normalized);

  if (alerts.cancelledNow) {
    return {
      aps: {
        alert: {
          title: "Flight Cancelled",
          body: `${code} (${route}) has been cancelled.`,
        },
        sound: "default",
      },
      runwy: {
        type: "flight_cancelled",
        flightId,
        status: normalized.status || null,
        route,
      },
    };
  }

  if (alerts.delayedNow) {
    const delay = Number(normalized?.delayMinutes || 0);
    const delayText = delay > 0 ? ` by ${delay}m` : "";

    return {
      aps: {
        alert: {
          title: "Flight Delayed",
          body: `${code} (${route}) is delayed${delayText}.`,
        },
        sound: "default",
      },
      runwy: {
        type: "flight_delayed",
        flightId,
        status: normalized.status || null,
        delayMinutes: delay,
        route,
      },
    };
  }

  if (alerts.gateChangedNow) {
    const gate = `${normalized?.gate || ""}`.trim();
    const gateText = gate ? ` to gate ${gate}` : "";

    return {
      aps: {
        alert: {
          title: "Gate Changed",
          body: `${code} (${route}) moved${gateText}.`,
        },
        sound: "default",
      },
      runwy: {
        type: "flight_gate_change",
        flightId,
        status: normalized.status || null,
        gate: gate || null,
        route,
      },
    };
  }

  return null;
}

function notificationEventFor(normalized, flightId) {
  const payload = notificationPayloadFor(normalized, flightId);
  const title = payload?.aps?.alert?.title;
  const body = payload?.aps?.alert?.body;
  const type = payload?.runwy?.type;

  if (!payload || !title || !body || !type) {
    return null;
  }

  return {
    type,
    title,
    body,
    payload,
  };
}

function ownerNotificationPreferenceConditionForEventType(eventType) {
  switch (eventType) {
    case "flight_delayed":
      return "coalesce((uf.alert_settings_json ->> 'delayUpdates')::boolean, true) = true";
    case "flight_gate_change":
      return "coalesce((uf.alert_settings_json ->> 'gateChange')::boolean, true) = true";
    case "flight_departed":
    case "flight_arrived":
      return "coalesce((uf.alert_settings_json ->> 'takeoffLanding')::boolean, true) = true";
    case "flight_baggage_claim":
      return "coalesce((uf.alert_settings_json ->> 'baggageClaim')::boolean, true) = true";
    default:
      return "true";
  }
}

function circleNotificationPreferenceConditionForEventType(eventType) {
  switch (eventType) {
    case "flight_delayed":
      return "fp.notify_delay = true";
    case "flight_gate_change":
      return "fp.notify_gate_change = true";
    case "flight_departed":
      return "fp.notify_departure = true";
    case "flight_arrived":
      return "fp.notify_arrival = true";
    default:
      return "true";
  }
}

async function listNotificationRecipientsForFlight(flightId, eventType) {
  if (!usesDatabase()) return [];

  const ownerCondition = ownerNotificationPreferenceConditionForEventType(eventType);
  const circleCondition = circleNotificationPreferenceConditionForEventType(eventType);
  const result = await pool.query(
    `
    with base as (
      select ts.id as tracking_session_id, ts.owner_user_id
      from public.tracking_sessions ts
      where ts.id = $1::uuid
    ),
    owner_recipient as (
      select
        base.owner_user_id as user_id,
        null::uuid as friend_relationship_id
      from base
      left join public.user_flights uf
        on uf.user_id = base.owner_user_id
       and uf.tracking_session_id = base.tracking_session_id
       and uf.deleted_at is null
      where coalesce(uf.notifications_enabled, true) = true
        and ${ownerCondition}
    ),
    circle_recipients as (
      select
        fp.viewer_user_id as user_id,
        fp.relationship_id as friend_relationship_id
      from base
      join public.friend_permissions fp
        on fp.owner_user_id = base.owner_user_id
      join public.friend_relationships fr
        on fr.id = fp.relationship_id
      where fr.relationship_status = 'active'
        and fp.can_view_live = true
        and fp.can_receive_alerts = true
        and ${circleCondition}
    ),
    recipients as (
      select * from owner_recipient
      union
      select * from circle_recipients
    )
    select
      recipients.user_id::text as user_id,
      recipients.friend_relationship_id::text as friend_relationship_id,
      pd.apns_token
    from recipients
    left join public.push_devices pd
      on pd.user_id = recipients.user_id
     and pd.push_enabled = true
    `,
    [flightId]
  );

  return result.rows.map((row) => ({
    userId: row.user_id,
    friendRelationshipId: row.friend_relationship_id || null,
    apnsToken: row.apns_token || null,
  }));
}

async function sendApnsNotification(apnsToken, payload) {
  if (!isApnsConfigured()) {
    return { skipped: true };
  }

  const response = await fetch(`https://${apnsHost()}/3/device/${apnsToken}`, {
    method: "POST",
    headers: {
      authorization: `bearer ${apnsAuthToken()}`,
      "apns-topic": APNS_BUNDLE_ID,
      "apns-push-type": "alert",
      "apns-priority": "10",
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (response.status === 200) {
    return { ok: true };
  }

  let reason = "Unknown";
  try {
    const body = await response.json();
    reason = body?.reason || reason;
  } catch (_error) {
    reason = `HTTP_${response.status}`;
  }

  if (["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"].includes(reason)) {
    await disablePushToken(apnsToken);
  }

  return { ok: false, status: response.status, reason };
}

async function dispatchFlightStatusNotifications(flightId, normalized) {
  const event = notificationEventFor(normalized, flightId);
  if (!event) return;

  const recipients = await listNotificationRecipientsForFlight(flightId, event.type);

  if (usesDatabase() && recipients.length) {
    const uniqueRecipients = Array.from(
      new Map(
        recipients.map((recipient) => [
          `${recipient.userId}|${recipient.friendRelationshipId || ""}`,
          {
            user_id: recipient.userId,
            friend_relationship_id: recipient.friendRelationshipId || "",
          },
        ])
      ).values()
    );

    await pool.query(
      `
      insert into public.notifications (
        user_id,
        tracking_session_id,
        friend_relationship_id,
        notification_type,
        delivery_channel,
        delivery_status,
        title,
        body,
        payload_json,
        scheduled_for
      )
      select
        recipients.user_id::uuid,
        $1::uuid,
        nullif(recipients.friend_relationship_id, '')::uuid,
        $2,
        'push',
        $3,
        $4,
        $5,
        $6::jsonb,
        now()
      from jsonb_to_recordset($7::jsonb) as recipients(
        user_id text,
        friend_relationship_id text
      )
      `,
      [
        flightId,
        event.type,
        isApnsConfigured() ? "queued" : "pending",
        event.title,
        event.body,
        JSON.stringify(event.payload),
        JSON.stringify(uniqueRecipients),
      ]
    );
  }

  const tokens = Array.from(
    new Set(
      recipients
        .map((recipient) => recipient.apnsToken)
        .filter(Boolean)
    )
  );
  if (!tokens.length || !isApnsConfigured()) return;

  const results = await Promise.all(tokens.map((token) => sendApnsNotification(token, event.payload)));

  if (usesDatabase()) {
    const deliveryStatus = results.some((result) => result?.ok) ? "sent" : "failed";
    await pool.query(
      `
      update public.notifications
      set
        delivery_status = $2,
        sent_at = case when $2 = 'sent' then coalesce(sent_at, now()) else sent_at end,
        updated_at = now()
      where tracking_session_id = $1::uuid
        and notification_type = $3
        and delivery_status = 'queued'
        and created_at >= now() - interval '15 minutes'
      `,
      [flightId, deliveryStatus, event.type]
    );
  }
}

async function findReusableTrackedRecordForUser({
  userId,
  providerFlightId,
  query,
}) {
  const normalizedUserId = String(userId || "").trim();
  if (!normalizedUserId || !query?.flightNumber) {
    return null;
  }

  let candidates = [];

  if (providerFlightId) {
    candidates = await listTrackedFlightsByProviderFlightId(providerFlightId, {
      statuses: ["pending", "active"],
    });
  }

  if (!candidates.length) {
    candidates = await listTrackedFlightsByFlightNumber(query.flightNumber, {
      statuses: ["pending", "active"],
      startDate: query.date,
      endDate: query.date,
      departureIata: query.departureIata,
      arrivalIata: query.arrivalIata,
    });
  }

  return candidates.find((candidate) => String(candidate?.ownerUserId || "").trim() === normalizedUserId) || null;
}

async function refreshTrackedFlightRecord(trackedRecord, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return trackedRecord;
  }

  const { includeLivePosition = false } = options;
  const provider = providerAdapter(trackedRecord.provider || FLIGHT_DATA_PROVIDER);
  const records = await provider.fetchFlights(trackedRecord.query);
  const selected = bestMatch(records, trackedRecord.query, provider.normalizeRecord);

  if (!selected) {
    return trackedRecord;
  }

  let normalized = normalizeWithContext(
    selected,
    records,
    trackedRecord.query,
    provider.normalizeRecord,
    trackedRecord.normalized
  );

  let trailSeedMetadataPatch = null;
  const seededTrail = await maybeBuildFlightAwareTrackTrailSeed({
    trackedRecord,
    normalized,
    providerName: provider.name,
    rawRecord: selected,
    source: "tracked_refresh",
  });
  normalized = seededTrail.normalized;
  trailSeedMetadataPatch = seededTrail.metadataPatch;

  if (
    !trailSeedMetadataPatch &&
    (
      includeLivePosition ||
      (!normalized.livePosition && (!Array.isArray(normalized.trackPoints) || normalized.trackPoints.length === 0))
    )
  ) {
    normalized = await enrichNormalizedWithLivePosition(normalized, provider.name, selected);
  }
  normalized.lastUpdated = normalized.livePosition?.recordedAt || normalized.lastUpdated || new Date().toISOString();

  if (usesDatabase()) {
    await persistTrackingSnapshot({
      flightId: trackedRecord.flightId,
      userId: trackedRecord.ownerUserId,
      query: trackedRecord.query,
      normalized,
      provider: provider.name,
      providerFlightId: providerFlightIdentifier(selected, provider.name),
      rawProviderPayload: selected,
    });
    if (trailSeedMetadataPatch) {
      await mergeTrackingSessionMetadata(trackedRecord.flightId, trailSeedMetadataPatch);
    }
  } else {
    const lastUpdated = new Date().toISOString();
    await upsertTrackedFlightRecord({
      flightId: trackedRecord.flightId,
      query: trackedRecord.query,
      normalized,
      provider: provider.name,
      lastUpdated,
    });
  }

  if (normalized.alerts?.cancelledNow || normalized.alerts?.delayedNow || normalized.alerts?.gateChangedNow) {
    await dispatchFlightStatusNotifications(trackedRecord.flightId, normalized);
  }

  if (usesDatabase()) {
    return fetchTrackingRowByID(trackedRecord.flightId);
  }

  return {
    ...trackedRecord,
    normalized,
    provider: provider.name,
    lastUpdated: normalized.lastUpdated,
  };
}

function extractWebhookEvents(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.events)) return body.events;
  if (Array.isArray(body?.alerts)) return body.alerts;
  if (Array.isArray(body?.flights)) return body.flights;
  if (body && typeof body === "object") return [body];
  return [];
}

function flightNumberFromWebhookEvent(event) {
  return normalizeFlightCode(
    event?.ident_iata ||
      event?.ident ||
      event?.flightNumber ||
      event?.flight_number ||
      event?.flight?.ident_iata ||
      event?.flight?.ident ||
      event?.flight?.flight_number
  );
}

function providerFlightIdFromWebhookEvent(event) {
  const value =
    event?.fa_flight_id ||
    event?.faFlightId ||
    event?.faFlightID ||
    event?.flight_id ||
    event?.flightId ||
    event?.flight?.fa_flight_id ||
    event?.flight?.faFlightId ||
    event?.flight?.faFlightID ||
    event?.flight?.flight_id ||
    event?.flight?.flightId;

  const normalized = String(value || "").trim();
  return normalized || null;
}

function airportCodeFromWebhookValue(value) {
  if (!value) return null;
  if (typeof value === "string") return normalizeAirportCode(value);
  if (typeof value !== "object") return null;

  return normalizeAirportCode(
    value.code_iata ||
      value.codeIata ||
      value.iata ||
      value.airport_code ||
      value.airportCode ||
      value.code
  );
}

function departureAirportFromWebhookEvent(event) {
  return airportCodeFromWebhookValue(
    event?.origin ||
      event?.departure ||
      event?.departure_airport ||
      event?.departureAirport ||
      event?.origin_airport ||
      event?.originAirport ||
      event?.flight?.origin ||
      event?.flight?.departure ||
      event?.flight?.departure_airport ||
      event?.flight?.departureAirport ||
      event?.flight?.origin_airport ||
      event?.flight?.originAirport ||
      event?.origin_iata ||
      event?.departure_iata ||
      event?.flight?.origin_iata ||
      event?.flight?.departure_iata
  );
}

function arrivalAirportFromWebhookEvent(event) {
  return airportCodeFromWebhookValue(
    event?.destination ||
      event?.arrival ||
      event?.arrival_airport ||
      event?.arrivalAirport ||
      event?.destination_airport ||
      event?.destinationAirport ||
      event?.flight?.destination ||
      event?.flight?.arrival ||
      event?.flight?.arrival_airport ||
      event?.flight?.arrivalAirport ||
      event?.flight?.destination_airport ||
      event?.flight?.destinationAirport ||
      event?.destination_iata ||
      event?.arrival_iata ||
      event?.flight?.destination_iata ||
      event?.flight?.arrival_iata
  );
}

function timestampMsFromWebhookEvent(event) {
  const candidates = [
    event?.actual_out,
    event?.actualOff,
    event?.actual_off,
    event?.estimated_out,
    event?.estimatedOut,
    event?.estimated_off,
    event?.scheduled_out,
    event?.scheduledOut,
    event?.scheduled_off,
    event?.actual_in,
    event?.actualIn,
    event?.actual_on,
    event?.estimated_in,
    event?.estimatedIn,
    event?.estimated_on,
    event?.scheduled_in,
    event?.scheduledIn,
    event?.scheduled_on,
    event?.timestamp,
    event?.occurred_at,
    event?.occurredAt,
    event?.event_time,
    event?.eventTime,
    event?.flight?.actual_out,
    event?.flight?.estimated_out,
    event?.flight?.scheduled_out,
    event?.flight?.actual_in,
    event?.flight?.estimated_in,
    event?.flight?.scheduled_in,
  ];

  for (const candidate of candidates) {
    const timestamp = new Date(candidate || "").getTime();
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }

  return null;
}

function travelDateWindowFromWebhookEvent(event) {
  const timestampMs = timestampMsFromWebhookEvent(event);
  if (!Number.isFinite(timestampMs)) {
    return { startDate: null, endDate: null };
  }

  const startDate = new Date(timestampMs - 36 * 60 * 60_000).toISOString().slice(0, 10);
  const endDate = new Date(timestampMs + 36 * 60 * 60_000).toISOString().slice(0, 10);
  return { startDate, endDate };
}

function webhookStatusFromEvent(event) {
  return normalizeStatus(
    event?.status ||
      event?.flight_status ||
      event?.state ||
      event?.event_status ||
      event?.eventStatus ||
      event?.type ||
      event?.runwy?.status ||
      event?.flight?.status ||
      event?.flight?.flight_status
  );
}

function isTrackedRecordRefreshDue(trackedRecord, nowMs = Date.now()) {
  if (!trackedRecord || isTerminalFlightStatus(trackedRecord.normalized?.status)) {
    return false;
  }

  if (
    shouldSeedFlightAwareTrackTrail({
      normalized: trackedRecord.normalized,
      providerName: String(trackedRecord.provider || FLIGHT_DATA_PROVIDER).toLowerCase(),
      providerFlightId: trackedRecord.providerFlightId,
      metadata: trackedRecord.metadata,
    })
  ) {
    return true;
  }

  const nextPollAt = new Date(trackedRecord.nextPollAfter || "").getTime();
  return Number.isFinite(nextPollAt) && nextPollAt <= nowMs;
}

function shouldRefreshTrackedRecordFromWebhook(trackedRecord, event, nowMs = Date.now()) {
  if (!trackedRecord || isTerminalFlightStatus(trackedRecord.normalized?.status)) {
    return false;
  }

  const trackedStatus = String(trackedRecord.normalized?.status || "").toLowerCase();
  const incomingStatus = webhookStatusFromEvent(event);
  if ((trackedStatus === "departed" || trackedStatus === "enroute") && !isTerminalFlightStatus(incomingStatus)) {
    return false;
  }

  if (isTerminalFlightStatus(incomingStatus)) {
    return true;
  }

  const lastUpdatedAt = new Date(trackedRecord.lastUpdated).getTime();
  if (Number.isFinite(lastUpdatedAt) && nowMs - lastUpdatedAt < WEBHOOK_REFRESH_MIN_INTERVAL_MS) {
    return false;
  }

  return true;
}

function firehoseTravelDateMatchesTrackedRecord(trackedRecord, timestampMs) {
  if (!Number.isFinite(timestampMs)) {
    return true;
  }

  const travelDate = String(trackedRecord?.query?.date || "").slice(0, 10);
  if (!travelDate) {
    return true;
  }

  const travelMiddayMs = new Date(`${travelDate}T12:00:00Z`).getTime();
  if (!Number.isFinite(travelMiddayMs)) {
    return true;
  }

  return Math.abs(travelMiddayMs - timestampMs) <= 36 * 60 * 60_000;
}

function firehoseMessageMatchesTrackedRecord(message, trackedRecord) {
  if (!trackedRecord) {
    return false;
  }

  const providerFlightId = firehoseMessageProviderFlightId(message);
  if (providerFlightId && trackedRecord.providerFlightId === providerFlightId) {
    return true;
  }

  const messageFlightNumbers = Array.from(
    new Set(
      [message?.ident_iata, message?.ident, message?.flight_number]
        .map((value) => normalizeFlightCode(value))
        .filter(Boolean)
    )
  );
  const trackedFlightNumbers = Array.from(
    new Set(
      [
        trackedRecord.query?.flightNumber,
        trackedRecord.normalized?.flightNumber,
        String(trackedRecord.providerFlightId || "").split("-")[0],
      ]
        .map((value) => normalizeFlightCode(value))
        .filter(Boolean)
    )
  );

  if (
    messageFlightNumbers.length === 0 ||
    trackedFlightNumbers.length === 0 ||
    !messageFlightNumbers.some((flightNumber) => trackedFlightNumbers.includes(flightNumber))
  ) {
    return false;
  }

  const timestampMs = firehoseMessageTimestampMs(message);
  if (!firehoseTravelDateMatchesTrackedRecord(trackedRecord, timestampMs)) {
    return false;
  }

  const departureIata = normalizeFirehoseAirportCode(
    message?.orig,
    normalizeAirportCode(trackedRecord.query?.departureIata)
  );
  const arrivalIata = normalizeFirehoseAirportCode(
    message?.dest,
    normalizeAirportCode(trackedRecord.query?.arrivalIata)
  );

  if (
    departureIata &&
    normalizeAirportCode(trackedRecord.query?.departureIata) &&
    departureIata !== normalizeAirportCode(trackedRecord.query?.departureIata)
  ) {
    return false;
  }

  if (
    arrivalIata &&
    normalizeAirportCode(trackedRecord.query?.arrivalIata) &&
    arrivalIata !== normalizeAirportCode(trackedRecord.query?.arrivalIata)
  ) {
    return false;
  }

  return true;
}

function isFirehoseEligibleTrackedRecord(trackedRecord, nowMs = Date.now()) {
  if (!trackedRecord) {
    return false;
  }

  if (String(trackedRecord.provider || "").toLowerCase() !== "flightaware") {
    return false;
  }

  const status = String(trackedRecord.normalized?.status || "").toLowerCase();
  if (isTerminalFlightStatus(status)) {
    return false;
  }

  if (["boarding", "delayed", "departed", "enroute"].includes(status)) {
    return true;
  }

  const departureMs =
    new Date(
      trackedRecord.normalized?.departureTimes?.estimated ||
        trackedRecord.normalized?.departureTimes?.scheduled ||
        trackedRecord.normalized?.departureTimes?.actual ||
        ""
    ).getTime();

  if (Number.isFinite(departureMs)) {
    return departureMs - nowMs <= FIREHOSE_TRACK_LOOKAHEAD_MS;
  }

  const travelDate = String(trackedRecord.query?.date || "").slice(0, 10);
  if (!travelDate) {
    return false;
  }

  const travelDateStartMs = new Date(`${travelDate}T00:00:00Z`).getTime();
  const travelDateEndMs = new Date(`${travelDate}T23:59:59Z`).getTime();
  return Number.isFinite(travelDateStartMs) && Number.isFinite(travelDateEndMs)
    ? travelDateStartMs - FIREHOSE_TRACK_LOOKAHEAD_MS <= nowMs &&
        travelDateEndMs + FIREHOSE_POST_ARRIVAL_BUFFER_MS >= nowMs
    : false;
}

async function listFirehoseEligibleTrackingRows() {
  if (!usesDatabase()) {
    return [];
  }

  const trackedRows = await listFirehoseTrackedRows();
  return trackedRows.filter((trackedRecord) => isFirehoseEligibleTrackedRecord(trackedRecord));
}

async function applyFirehoseMessageToTrackedRecord(trackedRecord, message) {
  if (!trackedRecord || !usesDatabase()) {
    return trackedRecord;
  }

  const previousNormalized = trackedRecord.normalized;
  const normalized = normalizedFromFirehoseMessage(previousNormalized, message);
  const providerFlightId = firehoseMessageProviderFlightId(message) || trackedRecord.providerFlightId || null;

  await persistTrackingSnapshot({
    flightId: trackedRecord.flightId,
    userId: trackedRecord.ownerUserId,
    query: trackedRecord.query,
    normalized,
    provider: "flightaware",
    providerFlightId,
    rawProviderPayload: message,
  });

  if (normalized.alerts?.cancelledNow || normalized.alerts?.delayedNow || normalized.alerts?.gateChangedNow) {
    await dispatchFlightStatusNotifications(trackedRecord.flightId, normalized);
  }

  return fetchTrackingRowByID(trackedRecord.flightId);
}

async function processFirehoseMessage(message, trackedRowsById) {
  const trackedRows = Array.from(trackedRowsById.values());
  const matchedRows = trackedRows.filter((trackedRecord) =>
    firehoseMessageMatchesTrackedRecord(message, trackedRecord)
  );

  for (const trackedRecord of matchedRows) {
    const updated = await applyFirehoseMessageToTrackedRecord(trackedRecord, message);
    if (updated) {
      trackedRowsById.set(updated.flightId, updated);
    }
  }
}

const trackingPollerRuntime = createTrackingPollerRuntime({
  isPollerEnabled: ENABLE_TRACKING_POLLER,
  usesDatabase,
  ensureDatabaseSchema,
  listDueTrackingRows,
  refreshTrackedFlightRecord,
  markTrackingRowErrored,
  pollerIntervalMs: POLLER_INTERVAL_MS,
  pollerBatchSize: POLLER_BATCH_SIZE,
  logPollerSummary: TRACKING_POLLER_LOG_SUMMARY,
  providerName: FLIGHT_DATA_PROVIDER,
});

const {
  isPollerRunning,
  runTrackingPollerCycle,
  startTrackingPoller,
  startTrackingPollerWorker,
} = trackingPollerRuntime;

const firehoseRuntime = createFirehoseRuntime({
  firehoseEnabled: ENABLE_FIREHOSE_WORKER,
  firehoseHost: FIREHOSE_HOST,
  firehosePort: FIREHOSE_PORT,
  firehoseVersion: FIREHOSE_VERSION,
  firehoseUsername: FIREHOSE_USERNAME,
  firehosePassword: FIREHOSE_PASSWORD,
  firehoseUserAgent: FIREHOSE_USER_AGENT,
  firehoseKeepaliveSeconds: FIREHOSE_KEEPALIVE_SECONDS,
  firehoseEvents: FIREHOSE_EVENTS,
  firehoseMinSecondsBetweenAirborne: FIREHOSE_MIN_SECONDS_BETWEEN_AIRBORNE,
  firehoseTrackedSetRefreshMs: FIREHOSE_TRACKED_SET_REFRESH_MS,
  firehoseReconnectDelayMs: FIREHOSE_RECONNECT_DELAY_MS,
  firehoseBackfillMaxHours: FIREHOSE_BACKFILL_MAX_HOURS,
  firehoseBackfillPredepartureMinutes: FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
  firehoseBackfillMinTrackPoints: FIREHOSE_BACKFILL_MIN_TRACK_POINTS,
  usesDatabase,
  ensureDatabaseSchema,
  listFirehoseTrackedRows: listFirehoseEligibleTrackingRows,
  processFirehoseMessage,
  providerName: FLIGHT_DATA_PROVIDER,
});

const {
  isFirehoseConfigured,
  isFirehoseRunning,
  startFirehoseWorker,
} = firehoseRuntime;

app.get("/health", async (_req, res) => {
  let trackingSummary = null;
  let providerAuth = null;

  if (usesDatabase()) {
    try {
      trackingSummary = await fetchTrackingSessionStatusSummary();
    } catch (error) {
      trackingSummary = {
        error: error?.message || String(error),
      };
    }
  }

  try {
    providerAuth = await getProviderAuthHealth();
  } catch (error) {
    providerAuth = {
      provider: FLIGHT_DATA_PROVIDER,
      endpoint: FLIGHT_DATA_PROVIDER === "flightaware" ? "schedules" : null,
      checkedAt: new Date().toISOString(),
      ok: null,
      state: "health_probe_failed",
      statusCode: null,
      detail: error?.message || String(error),
      cached: false,
      cacheTtlMs: HEALTH_PROVIDER_AUTH_CACHE_TTL_MS,
    };
  }

  res.json({
    ok: true,
    build: healthBuildInfo(),
    provider: FLIGHT_DATA_PROVIDER,
    providerCallsEnabled: PROVIDER_CALLS_ENABLED,
    nodeEnv: NODE_ENV,
    persistence: usesDatabase() ? "supabase-postgres" : "memory",
    apnsConfigured: isApnsConfigured(),
    pollerEnabled: isPollerRunning(),
    firehoseConfigured: isFirehoseConfigured(),
    firehoseEnabled: isFirehoseRunning(),
    providerAuth,
    trackingSummary,
    safeguards: {
      maxActiveTrackingSessionsPerUser:
        Number.isFinite(MAX_ACTIVE_TRACKING_SESSIONS_PER_USER) && MAX_ACTIVE_TRACKING_SESSIONS_PER_USER < Number.MAX_SAFE_INTEGER
          ? MAX_ACTIVE_TRACKING_SESSIONS_PER_USER
          : null,
      pollerSummaryLogging: TRACKING_POLLER_LOG_SUMMARY,
      mapFallbackEnabled: FLIGHTAWARE_ENABLE_MAP_FALLBACK,
      webhookRefreshMinIntervalMs: WEBHOOK_REFRESH_MIN_INTERVAL_MS,
      providerCallsEnabled: PROVIDER_CALLS_ENABLED,
    disableProviderCalls: DISABLE_PROVIDER_CALLS,
    firehoseTrackLookaheadMs: FIREHOSE_TRACK_LOOKAHEAD_MS,
    firehoseTrackedSetRefreshMs: FIREHOSE_TRACKED_SET_REFRESH_MS,
    firehoseBackfillMaxHours: FIREHOSE_BACKFILL_MAX_HOURS,
    firehoseBackfillPredepartureMinutes: FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
    firehoseBackfillMinTrackPoints: FIREHOSE_BACKFILL_MIN_TRACK_POINTS,
  },
});
});

app.get("/v1/airports", (req, res) => {
  try {
    const catalog = getAirportCatalog();
    const etag = catalog.version ? `"${catalog.version}"` : null;

    if (etag && req.get("If-None-Match") === etag) {
      return res.status(304).end();
    }

    res.set("Cache-Control", "public, max-age=86400, stale-while-revalidate=604800");
    if (etag) {
      res.set("ETag", etag);
    }

    return res.type("application/json").send(catalog.body);
  } catch (error) {
    console.error("Airport catalog unavailable", error?.message || String(error));
    return res.status(500).json({ error: "Airport catalog unavailable" });
  }
});

app.post("/v1/devices/push-token", async (req, res) => {
  const deviceId = normalizedHeaderDeviceID(req);
  const userId = String(req.auth?.userId || "").trim() || null;
  const validated = validatePushTokenPayload(req.body);

  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }

  if (!deviceId) {
    return res.status(400).json({ error: "X-Device-Id header is required" });
  }

  if (validated.error) {
    return res.status(400).json({ error: validated.error });
  }

  try {
    await upsertPushDevice({
      apnsToken: validated.value.token,
      deviceId,
      userId,
      platform: validated.value.platform,
    });

    return res.json({ ok: true });
  } catch (_error) {
    return res.status(500).json({ error: "Unable to store push token" });
  }
});

app.post("/v1/devices/push-token/remove", async (req, res) => {
  const deviceId = normalizedHeaderDeviceID(req);
  const userId = String(req.auth?.userId || "").trim() || null;
  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }
  if (!deviceId) {
    return res.status(400).json({ error: "X-Device-Id header is required" });
  }

  try {
    await disablePushTokensForDevice(deviceId, userId);
    return res.json({ ok: true });
  } catch (_error) {
    return res.status(500).json({ error: "Unable to disable push token" });
  }
});

app.post("/v1/track", async (req, res) => {
  const validated = validateTrackPayload(req.body);
  if (validated.error) {
    return res.status(400).json({ error: validated.error });
  }

  const userId = String(req.auth?.userId || "").trim() || null;
  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }

  if (!usesDatabase()) {
    return res.status(503).json({ error: "Tracking persistence is not configured" });
  }

  if (!PROVIDER_CALLS_ENABLED) {
    return res.status(503).json({ error: "Provider calls are temporarily disabled" });
  }

  try {
    const query = validated.value;
    const provider = providerAdapter();
    const records = await provider.fetchFlights(query);
    const selected = bestMatch(records, query, provider.normalizeRecord);
    if (!selected) {
      return res.status(404).json({ error: "No matching flight found" });
    }

    const providerFlightId = providerFlightIdentifier(selected, provider.name);
    const reusableTracked = await findReusableTrackedRecordForUser({
      userId,
      providerFlightId,
      query,
    });

    let normalized = normalizeWithContext(
      selected,
      records,
      query,
      provider.normalizeRecord,
      reusableTracked?.normalized || null
    );
    if (reusableTracked?.normalized) {
      normalized = mergeFlightAwareTrackTrailIntoNormalized(normalized, {
        trackPoints: reusableTracked.normalized.trackPoints,
        livePosition: reusableTracked.normalized.livePosition,
      });
    }
    const trackTrailSeedCandidate = shouldSeedFlightAwareTrackTrail({
      normalized,
      providerName: provider.name,
      providerFlightId,
      metadata: reusableTracked?.metadata || null,
    });
    if (!trackTrailSeedCandidate) {
      normalized = await enrichNormalizedWithLivePosition(normalized, provider.name, selected);
    }
    normalized.lastUpdated = normalized.livePosition?.recordedAt || normalized.lastUpdated || new Date().toISOString();

    let tracked = await createOrReuseTrackingSession({
      query,
      normalized,
      rawProviderPayload: selected,
      userId,
      provider: provider.name,
      createdSource: "manual_track",
    });

    if (!tracked) {
      return res.status(500).json({ error: "Unable to create tracking session" });
    }

    if (trackTrailSeedCandidate) {
      const seededTrail = await maybeBuildFlightAwareTrackTrailSeed({
        trackedRecord: tracked,
        normalized: tracked.normalized,
        providerName: provider.name,
        rawRecord: selected,
        source: "manual_track",
      });

      let trackedNormalized = seededTrail.normalized;
      if (
        !seededTrail.metadataPatch &&
        !trackedNormalized.livePosition &&
        (!Array.isArray(trackedNormalized.trackPoints) || trackedNormalized.trackPoints.length === 0)
      ) {
        trackedNormalized = await enrichNormalizedWithLivePosition(trackedNormalized, provider.name, selected);
        trackedNormalized.lastUpdated =
          trackedNormalized.livePosition?.recordedAt ||
          trackedNormalized.lastUpdated ||
          new Date().toISOString();
      }

      if (
        seededTrail.metadataPatch ||
        trackedNormalized.livePosition ||
        (Array.isArray(trackedNormalized.trackPoints) && trackedNormalized.trackPoints.length > 0)
      ) {
        await persistTrackingSnapshot({
          flightId: tracked.flightId,
          userId: tracked.ownerUserId,
          query: tracked.query,
          normalized: trackedNormalized,
          provider: tracked.provider,
          providerFlightId: providerFlightIdentifier(selected, provider.name) || tracked.providerFlightId || providerFlightId,
          rawProviderPayload: selected,
        });
        if (seededTrail.metadataPatch) {
          await mergeTrackingSessionMetadata(tracked.flightId, seededTrail.metadataPatch);
        }
        tracked = await fetchTrackingRowByID(tracked.flightId);
      }
    }

    ensureFlightAwareAlertForTrackedSession(req, tracked).catch((error) => {
      console.warn(
        `FlightAware alert ensure failed for ${tracked.flightId}: ${error?.message || String(error)}`
      );
    });

    return res.json({
      flightId: tracked.flightId,
      normalized: tracked.normalized,
    });
  } catch (_error) {
    if (_error?.code === "TRACKING_LIMIT_REACHED") {
      return res.status(429).json({ error: _error.message });
    }
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.get("/v1/flights/:flightId", async (req, res) => {
  const flightId = req.params.flightId;
  const userId = String(req.auth?.userId || "").trim() || null;

  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }

  try {
    const tracked = await fetchAccessibleTrackingRow(flightId, userId);
    if (!tracked) {
      return res.status(404).json({ error: "Unknown flightId" });
    }

    const shouldRefresh = isTrackedRecordRefreshDue(tracked);
    const current = shouldRefresh ? await refreshTrackedFlightRecord(tracked) : tracked;

    return res.json({
      flightId,
      normalized: current.normalized,
      lastUpdated: current.lastUpdated,
    });
  } catch (error) {
    console.error("Failed to load tracked flight details", {
      flightId,
      userId,
      error: error?.message || String(error),
    });
    return res.status(500).json({ error: "Failed to load flight details" });
  }
});

async function buildSearchCandidates(query) {
  const provider = providerAdapter();
  const records = await provider.fetchFlights(query);
  const topRecords = sortSearchRecords(records, query, provider.normalizeRecord).slice(0, 30);

  const normalized = await Promise.all(
    topRecords.map(async (record, index) => {
      let candidate = normalizeWithContext(record, records, query, provider.normalizeRecord, null);
      if (query?.historical !== true && index < SEARCH_LIVE_ENRICH_LIMIT) {
        candidate = await enrichNormalizedWithLivePosition(candidate, provider.name, record);
      }
      candidate.lastUpdated = candidate.livePosition?.recordedAt || candidate.lastUpdated || new Date().toISOString();
      return candidate;
    })
  );

  return normalized;
}

app.get("/v1/search", async (req, res) => {
  const validated = validateSearchQuery(req.query);
  if (validated.error) {
    return res.status(400).json({ error: validated.error });
  }

  const { flightNumber, date, departureIata, arrivalIata, historical, preferSchedules } = validated.value;

  if (!PROVIDER_CALLS_ENABLED) {
    return res.json({ candidates: [], providerDisabled: true });
  }

  try {
    const query = { flightNumber, date, departureIata, arrivalIata, historical, preferSchedules };
    const normalized = await buildSearchCandidates(query);
    return res.json({ candidates: normalized });
  } catch (error) {
    console.error("Search provider fetch failed", {
      provider: FLIGHT_DATA_PROVIDER,
      flightNumber,
      date,
      departureIata,
      arrivalIata,
      historical,
      preferSchedules,
      error: error?.message || String(error),
    });
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.get("/v1/search/route", async (req, res) => {
  const validated = validateRouteSearchQuery(req.query);
  if (validated.error) {
    return res.status(400).json({ error: validated.error });
  }

  const { date, departureIata, arrivalIata, historical, preferSchedules } = validated.value;

  if (!PROVIDER_CALLS_ENABLED) {
    return res.json({ candidates: [], providerDisabled: true });
  }

  try {
    const query = { date, departureIata, arrivalIata, historical, preferSchedules };
    const normalized = await buildSearchCandidates(query);
    return res.json({ candidates: normalized });
  } catch (error) {
    console.error("Route search provider fetch failed", {
      provider: FLIGHT_DATA_PROVIDER,
      date,
      departureIata,
      arrivalIata,
      historical,
      preferSchedules,
      error: error?.message || String(error),
    });
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.get("/v1/providers/flightaware/flights/:providerFlightId/track", async (req, res) => {
  if (!PROVIDER_CALLS_ENABLED) {
    return res.status(503).json({ error: "Provider calls are temporarily disabled" });
  }

  const providerFlightId = String(req.params?.providerFlightId || "").trim();
  if (!providerFlightId) {
    return res.status(400).json({ error: "Missing provider flight id" });
  }

  try {
    const trackTrail = await fetchFlightAwareTrackTrail(providerFlightId);
    return res.json({
      providerFlightId,
      trackPoints: Array.isArray(trackTrail.trackPoints) ? trackTrail.trackPoints : [],
      livePosition: trackTrail.livePosition || null,
    });
  } catch (error) {
    console.error("FlightAware track fetch failed", {
      providerFlightId,
      error: error?.message || String(error),
    });
    return res.status(502).json({ error: "Failed to fetch provider track" });
  }
});

app.post("/v1/webhooks/flightaware", async (req, res) => {
  if (!WEBHOOK_SHARED_SECRET) {
    return res.status(503).json({ error: "Webhook secret is not configured" });
  }

  const incomingSecret = webhookSecretFromRequest(req);
  if (!timingSafeEqualText(incomingSecret, WEBHOOK_SHARED_SECRET)) {
    return res.status(401).json({ error: "Unauthorized webhook" });
  }

  if (!PROVIDER_CALLS_ENABLED) {
    const events = extractWebhookEvents(req.body);
    return res.json({
      ok: true,
      providerCallsEnabled: false,
      receivedEvents: events.length,
      matchedFlights: 0,
      refreshedFlights: 0,
      throttledFlights: 0,
    });
  }

  const events = extractWebhookEvents(req.body);

  let matchedFlights = 0;
  let refreshedFlights = 0;
  let throttledFlights = 0;

  for (const event of events) {
    const flightNumber = flightNumberFromWebhookEvent(event);
    const providerFlightId = providerFlightIdFromWebhookEvent(event);
    const departureIata = departureAirportFromWebhookEvent(event);
    const arrivalIata = arrivalAirportFromWebhookEvent(event);
    const travelDateWindow = travelDateWindowFromWebhookEvent(event);

    let candidates = [];

    if (providerFlightId) {
      candidates = await listTrackedFlightsByProviderFlightId(providerFlightId, {
        statuses: ["pending", "active"],
      });
    }

    if (!candidates.length && flightNumber) {
      candidates = await listTrackedFlightsByFlightNumber(flightNumber, {
        statuses: ["pending", "active"],
        startDate: travelDateWindow.startDate,
        endDate: travelDateWindow.endDate,
        departureIata,
        arrivalIata,
      });
    }

    if (!candidates.length) {
      continue;
    }

    matchedFlights += candidates.length;

    for (const tracked of candidates) {
      if (!shouldRefreshTrackedRecordFromWebhook(tracked, event)) {
        throttledFlights += 1;
        continue;
      }

      try {
        await refreshTrackedFlightRecord(tracked);
        refreshedFlights += 1;
      } catch (_error) {
        // Ignore single-flight failures; continue processing webhook batch.
      }
    }
  }

  return res.json({
    ok: true,
    receivedEvents: events.length,
    matchedFlights,
    refreshedFlights,
    throttledFlights,
  });
});

async function startApiServer() {
  if (usesDatabase()) {
    await ensureDatabaseSchema();
  }

  startTrackingPoller({ keepProcessAlive: false });

  return app.listen(PORT, () => {
    console.log(
      `Flight proxy running on port ${PORT} provider=${FLIGHT_DATA_PROVIDER} persistence=${usesDatabase() ? "supabase-postgres" : "memory"} poller=${isPollerRunning() ? "on" : "off"}`
    );
  });
}

if (require.main === module) {
  startApiServer().catch((error) => {
    console.error("Failed to start flight proxy", error);
    process.exit(1);
  });
}

module.exports = {
  app,
  ensureDatabaseSchema,
  isFirehoseRunning,
  runTrackingPollerCycle,
  startApiServer,
  startFirehoseWorker,
  startTrackingPoller,
  startTrackingPollerWorker,
  usesDatabase,
  __test__: {
    classifyFlightAwareAuthProbeResult,
    extractFlightAwareSearchRows,
    flightAwareHistoryBounds,
    healthBuildInfo,
    isFutureFlightAwareQueryDate,
    normalizeRecordFromFlightAware,
    scoreCandidate,
    shouldPreferFlightAwareSchedules,
  },
};
