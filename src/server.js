require("dotenv").config();

const crypto = require("node:crypto");
const http2 = require("node:http2");
const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");
const { version: PACKAGE_VERSION = "0.0.0" } = require("../package.json");
const { getAirportCatalog } = require("./airport-catalog");
const {
  validatePushTokenPayload,
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
const { createProviderAdapter: createSharedProviderAdapter } = require("./shared-flight/provider-adapter");
const {
  createMemorySharedFlightRepository,
  createPostgresSharedFlightRepository,
} = require("./shared-flight/repository");
const { createApnsSender: createSharedApnsSender } = require("./shared-flight/notifications");
const { createSharedFlightService } = require("./shared-flight/service");
const { mountSharedFlightRoutes } = require("./shared-flight/routes");
const {
  deriveFlightLifecyclePhase,
  displayStatusForPhase,
} = require("./shared-flight/state");

const PORT = Number(process.env.PORT || 8787);
const FLIGHT_DATA_PROVIDER = (process.env.FLIGHT_DATA_PROVIDER || "aviationstack").toLowerCase();
const RUNWY_NOTIFICATION_SOUND = "RunwyNotification.caf";

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
const SHARED_FLIGHT_STREAMING_ENABLED =
  String(process.env.SHARED_FLIGHT_STREAMING_ENABLED || process.env.ENABLE_FIREHOSE_WORKER || "false").toLowerCase() === "true";
const SHARED_FLIGHT_TRACK_BRIDGE_ENABLED =
  String(process.env.SHARED_FLIGHT_TRACK_BRIDGE_ENABLED || "true").toLowerCase() !== "false";
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
const DATABASE_SSL_REJECT_UNAUTHORIZED =
  String(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED || (IS_PRODUCTION ? "true" : "false")).toLowerCase() !== "false";

const CACHE_TTL_MS = toPositiveNumber(process.env.CACHE_TTL_MS, 5 * 60_000);
const FLIGHTAWARE_SCHEDULE_MAX_PAGES = Math.min(
  10,
  Math.max(1, Math.round(toPositiveNumber(process.env.FLIGHTAWARE_SCHEDULE_MAX_PAGES, 1)))
);
const FLIGHTAWARE_POSITION_CACHE_TTL_MS = toPositiveNumber(
  process.env.FLIGHTAWARE_POSITION_CACHE_TTL_MS,
  5 * 60_000
);
const FLIGHTAWARE_DAILY_FLIGHT_CALL_LIMIT = toPositiveNumber(
  process.env.FLIGHTAWARE_DAILY_FLIGHT_CALL_LIMIT,
  500
);
const FLIGHTAWARE_DAILY_SEARCH_RESERVE = toPositiveNumber(
  process.env.FLIGHTAWARE_DAILY_SEARCH_RESERVE,
  100
);
const FLIGHTAWARE_SCHEDULE_WINDOW_MS = 48 * 60 * 60_000;
// This bucket covers every authenticated /v1 request, including background
// lifecycle, Circle, device-token, and cached status traffic. Keep enough
// headroom that those requests cannot block an intentional flight search.
const RATE_LIMIT_PER_MINUTE = toPositiveNumber(process.env.RATE_LIMIT_PER_MINUTE, 300);
const SEARCH_RATE_LIMIT_PER_MINUTE = toPositiveNumber(
  process.env.SEARCH_RATE_LIMIT_PER_MINUTE,
  60
);
const WEBHOOK_SHARED_SECRET = (process.env.FLIGHTAWARE_WEBHOOK_SECRET || process.env.WEBHOOK_SHARED_SECRET || "").trim();

const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim().replace(/\/+$/, "");
const SUPABASE_ANON_KEY = (process.env.SUPABASE_ANON_KEY || "").trim();
const SUPABASE_JWT_SECRET = process.env.SUPABASE_JWT_SECRET || "";
const ALLOW_INSECURE_NO_AUTH = String(process.env.ALLOW_INSECURE_NO_AUTH || "false").toLowerCase() === "true";
const AUTH_CACHE_TTL_MS = toPositiveNumber(process.env.AUTH_CACHE_TTL_MS, 5 * 60_000);
const MAX_AUTH_CACHE_ENTRIES = toPositiveNumber(process.env.MAX_AUTH_CACHE_ENTRIES, 5_000);
const POLLER_INTERVAL_MS = toPositiveNumber(process.env.POLLER_INTERVAL_MS, 2 * 60_000);
const POLLER_BATCH_SIZE = toPositiveNumber(process.env.POLLER_BATCH_SIZE, 25);
const FINAL_TRAVEL_ROUTE_CAPTURE_ENABLED =
  String(process.env.FINAL_TRAVEL_ROUTE_CAPTURE_ENABLED || "true").toLowerCase() !== "false";
const FINAL_TRAVEL_ROUTE_CAPTURE_BEFORE_ARRIVAL_MS = toPositiveNumber(
  process.env.FINAL_TRAVEL_ROUTE_CAPTURE_BEFORE_ARRIVAL_MINUTES,
  90
) * 60_000;
const FINAL_TRAVEL_ROUTE_CAPTURE_AFTER_ARRIVAL_MS = toPositiveNumber(
  process.env.FINAL_TRAVEL_ROUTE_CAPTURE_AFTER_ARRIVAL_HOURS,
  12
) * 60 * 60_000;
const FINAL_TRAVEL_ROUTE_CAPTURE_RETRY_MS = toPositiveNumber(
  process.env.FINAL_TRAVEL_ROUTE_CAPTURE_RETRY_MINUTES,
  180
) * 60_000;
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
  IS_PRODUCTION ? 5 : 20
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
const TEST_NOTIFICATION_RATE_LIMIT_PER_MINUTE = toPositiveNumber(
  process.env.TEST_NOTIFICATION_RATE_LIMIT_PER_MINUTE,
  2
);
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
  out: true,
  off: true,
  on: true,
  in: true,
  hold_start: false,
  hold_end: false,
});
const FLIGHTAWARE_AUTO_ALERT_IMPENDING_DEPARTURE_MINUTES = Object.freeze([120, 60, 15]);
const FLIGHTAWARE_AUTO_ALERT_IMPENDING_ARRIVAL_MINUTES = Object.freeze([30]);
const FLIGHTAWARE_ALERT_CONFIGURATION_CHANGED_AT = "2026-08-28T11:07:22.000Z";
let flightAwareAlertEndpointReadyURL = null;
let flightAwareAlertEndpointPromise = null;

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

if (IS_PRODUCTION && ALLOW_INSECURE_NO_AUTH) {
  console.error("ALLOW_INSECURE_NO_AUTH cannot be enabled when NODE_ENV=production.");
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

function isTestNotificationRequest(req) {
  const path = String(req.originalUrl || req.url || "").split("?", 1)[0];
  return path === "/v1/devices/test-notification";
}

function isFlightSearchRequest(req) {
  const path = String(req.originalUrl || req.url || "").split("?", 1)[0];
  return path === "/v1/search" || path === "/v1/search/route";
}

function authenticatedRequestKey(req, namespace) {
  const userID = String(req.auth?.userId || "").trim();
  if (userID) {
    return `${namespace}:user:${userID.slice(0, 128)}`;
  }

  const ip = req.ip || req.socket?.remoteAddress || "unknown";
  const deviceID = normalizedHeaderDeviceID(req);
  if (deviceID) {
    return `${namespace}:ip:${ip}|device:${deviceID.slice(0, 128)}`;
  }
  return `${namespace}:ip:${ip}`;
}

const limiter = rateLimit({
  windowMs: 60_000,
  max: RATE_LIMIT_PER_MINUTE,
  standardHeaders: true,
  legacyHeaders: false,
  // Test pushes have a dedicated limiter below. Counting them here as well
  // lets unrelated foreground/background API traffic disable the diagnostic.
  skip: (req) => isTestNotificationRequest(req) || isFlightSearchRequest(req),
  keyGenerator: (req) => authenticatedRequestKey(req, "api"),
});

const searchLimiter = rateLimit({
  windowMs: 60_000,
  max: SEARCH_RATE_LIMIT_PER_MINUTE,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => authenticatedRequestKey(req, "flight-search"),
});

const testNotificationLimiter = rateLimit({
  windowMs: 60_000,
  max: TEST_NOTIFICATION_RATE_LIMIT_PER_MINUTE,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const userID = String(req.auth?.userId || req.ip || "unknown").slice(0, 128);
    const deviceID = normalizedHeaderDeviceID(req) || "unknown-device";
    return `test-push:${userID}:${deviceID.slice(0, 128)}`;
  },
  handler: (req, res) => {
    const resetAt = req.rateLimit?.resetTime?.getTime?.() || Date.now() + 60_000;
    const retryAfterSeconds = Math.max(1, Math.ceil((resetAt - Date.now()) / 1_000));
    return res.status(429).json({
      error: `Please wait ${retryAfterSeconds}s before scheduling another test notification.`,
      retryAfterSeconds,
    });
  },
});

app.use("/v1", authenticateRequest);
app.use("/v1", limiter);
// Search has its own bucket so unrelated lifecycle and background requests
// cannot prevent a user from deliberately looking up a flight.
app.use("/v1/search", searchLimiter);

const providerCache = new Map();
const providerInFlightRequests = new Map();
const memoryTrackedFlights = new Map();
const memoryPushDevices = new Map();

function postgresSSLConfig() {
  if (!DATABASE_SSL) return undefined;
  return { rejectUnauthorized: DATABASE_SSL_REJECT_UNAUTHORIZED };
}

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: postgresSSLConfig(),
    })
  : null;

function flightAwareDailyBudgetLimitForEndpoint(endpoint) {
  const isSearchRequest = ["operational", "schedules", "historical"].includes(String(endpoint || ""));
  return FLIGHTAWARE_DAILY_FLIGHT_CALL_LIMIT +
    (isSearchRequest ? FLIGHTAWARE_DAILY_SEARCH_RESERVE : 0);
}

async function flightAwareFlightFetch(url, options, { endpoint, units = 1 } = {}) {
  const usageEndpoint = `aeroapi:flight:${String(endpoint || "unknown")}`;
  const estimatedUnits = Math.max(1, Math.round(Number(units) || 1));
  const isSearchRequest = ["operational", "schedules", "historical"].includes(String(endpoint || ""));
  const effectiveLimit = flightAwareDailyBudgetLimitForEndpoint(endpoint);

  if (pool) {
    const usage = await pool.query(
      `select coalesce(sum(coalesce(cost_estimate, 1)), 0)::int as units
       from public.api_usage_logs
       where provider = 'flightaware'
         and endpoint like 'aeroapi:flight:%'
         and created_at >= date_trunc('day', now() at time zone 'UTC') at time zone 'UTC'`
    );
    const usedUnits = Number(usage.rows[0]?.units || 0);
    if (usedUnits + estimatedUnits > effectiveLimit) {
      const error = new Error(
        `FlightAware daily ${isSearchRequest ? "search reserve" : "Flight-call"} budget exhausted (${usedUnits}/${effectiveLimit})`
      );
      error.code = "FLIGHTAWARE_DAILY_BUDGET_EXHAUSTED";
      error.statusCode = 429;
      throw error;
    }
  }

  const startedAt = Date.now();
  let response = null;
  let requestError = null;
  try {
    response = await fetch(url, options);
    return response;
  } catch (error) {
    requestError = error;
    throw error;
  } finally {
    if (pool) {
      pool.query(
        `insert into public.api_usage_logs
           (provider, endpoint, status_code, response_time_ms, cache_status, cost_estimate, error)
         values ('flightaware', $1, $2, $3, 'outbound', $4, $5)`,
        [
          usageEndpoint,
          response?.status || null,
          Date.now() - startedAt,
          estimatedUnits,
          requestError?.message || null,
        ]
      ).catch((error) => {
        console.warn("Failed to record FlightAware outbound usage", error?.message || String(error));
      });
    }
  }
}

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
  const authorization = String(req.get("Authorization") || "").trim();
  const bearer = authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim();
  if (bearer) {
    return bearer;
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
  if (!req || typeof req.get !== "function") return "";
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

  // Use the unified handler so one provider callback updates both the shared
  // flight projection and the owner-specific tracking/notification pipeline.
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

function normalizeAircraftType(input) {
  const value = String(input || "").toUpperCase().trim();
  if (!value || value === "UNKNOWN" || value === "N/A" || value === "NA") return null;
  return value.replace(/\s+/g, "");
}

function normalizeStatus(rawStatus) {
  const value = String(rawStatus || "").toLowerCase().trim();
  if (!value) return "scheduled";
  if (value.includes("cancel")) return "cancelled";
  if (value.includes("divert")) return "diverted";
  if (value.includes("onblock") || value.includes("inblock") || value.includes("arrived_at_gate")) return "arrived_at_gate";
  if (value.includes("land")) return "landed";
  if (value.includes("taxi in") || value.includes("taxi_in")) return "taxi_in";
  if (value.includes("takeoff roll") || value.includes("takeoff_roll")) return "takeoff_roll";
  if (value.includes("taxi") || value.includes("offblock")) return "taxiing";
  if (value.includes("board")) return "boarding";
  if (value.includes("delay")) return "delayed";
  if (
    value.includes("active") ||
    value.includes("airborne") ||
    value.includes("en route") ||
    value.includes("en-route") ||
    value.includes("enroute")
  ) return "enroute";
  if (value.includes("depart") || value.includes("off")) return "departed";
  return "scheduled";
}

function reconcileOperationalStatus(normalized) {
  if (!normalized || typeof normalized !== "object") {
    return normalized;
  }

  const liveAltitudeFeet = Number(normalized.livePosition?.altitudeFeet);
  const liveGroundSpeedKnots = Number(normalized.livePosition?.groundSpeedKnots);
  const airGround = String(
    normalized.livePosition?.airGround || normalized.livePosition?.air_ground || ""
  ).toUpperCase();
  const groundTelemetry = normalized.livePosition && (
    airGround === "G" ||
    (Number.isFinite(liveAltitudeFeet) && liveAltitudeFeet <= 150 &&
      Number.isFinite(liveGroundSpeedKnots) && liveGroundSpeedKnots <= 60)
  );
  const airborneTelemetry = normalized.livePosition && (
    airGround === "A" ||
    (Number.isFinite(liveAltitudeFeet) && liveAltitudeFeet > 300) ||
    (Number.isFinite(liveAltitudeFeet) && liveAltitudeFeet > 150 &&
      Number.isFinite(liveGroundSpeedKnots) && liveGroundSpeedKnots > 80)
  );
  const livePositionRecordedAtMs = new Date(
    normalized.livePosition?.recordedAt || normalized.lastUpdated || 0
  ).getTime();
  const freshAirborneTelemetry = airborneTelemetry &&
    Number.isFinite(livePositionRecordedAtMs) &&
    Math.abs(Date.now() - livePositionRecordedAtMs) <= 5 * 60_000;

  const actualArrivalValue = normalized.arrivalTimes?.actual;
  const actualArrivalMs = new Date(actualArrivalValue || 0).getTime();
  const credibleActualArrival = Boolean(actualArrivalValue) && Number.isFinite(actualArrivalMs) && actualArrivalMs <= Date.now() + 2 * 60_000;
  if (!freshAirborneTelemetry && credibleActualArrival) {
    return {
      ...normalized,
      status: "arrived_at_gate",
    };
  }

  const landingActual = normalized.landingTimes?.actual;
  const actualLandingMs = new Date(landingActual || 0).getTime();
  const credibleActualLanding = Boolean(landingActual) && Number.isFinite(actualLandingMs) && actualLandingMs <= Date.now() + 2 * 60_000;
  if (!freshAirborneTelemetry && credibleActualLanding) {
    return {
      ...normalized,
      status: "landed",
    };
  }

  if (groundTelemetry) {
    return {
      ...normalized,
      status: ["boarding", "taxiing", "taxi_out", "takeoff_roll", "taxi_in"].includes(normalized.status)
        ? normalized.status
        : "taxiing",
    };
  }

  const airborneSignal = airborneTelemetry || normalized.takeoffTimes?.actual;

  if (airborneSignal) {
    return {
      ...normalized,
      status: "enroute",
    };
  }

  const departedSignal = normalized.departureTimes?.actual || normalized.takeoffTimes?.actual;
  if (departedSignal && ["cancelled", "scheduled", "boarding", "delayed"].includes(normalized.status)) {
    return {
      ...normalized,
      status: normalized.takeoffTimes?.actual ? "airborne" : "taxiing",
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

  // Prefer an IATA two-character carrier when the remainder is a valid flight
  // number. A greedy 2–3 character prefix turns AI2015 into AI2 + 015 and 6E609
  // into 6E6 + 09, creating a different daily flight instance on refresh.
  const iataMatch = code.match(/^([A-Z0-9]{2})(\d{1,4}[A-Z]?)$/);
  if (iataMatch) return iataMatch[1];

  const icaoMatch = code.match(/^([A-Z]{3})(\d{1,4}[A-Z]?)$/);
  return icaoMatch ? icaoMatch[1] : null;
}

function calculateDelayMinutes(departureTimes) {
  const actualOrEstimate = departureTimes?.actual || departureTimes?.estimated;
  if (!departureTimes?.scheduled || !actualOrEstimate) return null;
  const scheduled = new Date(departureTimes.scheduled).getTime();
  const displayed = new Date(actualOrEstimate).getTime();
  if (!Number.isFinite(scheduled) || !Number.isFinite(displayed)) return null;
  return Math.max(0, Math.round((displayed - scheduled) / 60_000));
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
    altitudeFeet: flightAwareAltitudeFeet(lastPosition, record),
    recordedAt:
      lastPosition.date ??
      record?.updated ??
      record?.filed_time,
  });
}

function flightAwareAltitudeFeet(...records) {
  for (const record of records) {
    const explicitFeet = finiteNumberOrNull(
      record?.altitude_feet ?? record?.altitudeFeet ?? record?.reported_altitude_feet
    );
    if (explicitFeet !== null) return explicitFeet;
    const hundredsOfFeet = finiteNumberOrNull(record?.altitude ?? record?.reported_altitude);
    if (hundredsOfFeet !== null) return hundredsOfFeet * 100;
  }
  return null;
}

function firstNonBlank(...values) {
  for (const value of values) {
    if (value === null || value === undefined) {
      continue;
    }
    const text = String(value).trim();
    if (text) {
      return text;
    }
  }
  return null;
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
    aircraftType: normalizeAircraftType(
      record?.aircraft?.iata ||
        record?.aircraft?.icao ||
        record?.aircraft?.type ||
        record?.aircraft_type ||
        record?.aircraftType ||
        record?.equipment
    ),
    status: normalizeStatus(record?.flight_status),
    departureTerminal: firstNonBlank(departure.terminal, departure.terminal_name),
    departureGate: firstNonBlank(departure.gate, departure.gate_name),
    arrivalTerminal: firstNonBlank(arrival.terminal, arrival.terminal_name),
    arrivalGate: firstNonBlank(arrival.gate, arrival.gate_name),
    terminal: firstNonBlank(departure.terminal, departure.terminal_name),
    gate: firstNonBlank(departure.gate, departure.gate_name),
    baggageClaim: firstNonBlank(
      arrival.baggage,
      arrival.baggage_claim,
      arrival.baggage_belt
    ),
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
    record?.inbound_ident_iata || record?.inbound_ident
  );
  const inboundProviderFlightId = String(record?.inbound_fa_flight_id || "").trim() || null;
  const inboundOrigin = normalizeAirportCode(
    record?.inbound_origin_iata ||
      record?.inbound_origin?.code_iata ||
      record?.inbound_origin ||
      record?.inbound_origin_airport
  );

  const inboundFlight = inboundFlightNumber || inboundOrigin || inboundProviderFlightId
    ? {
        flightNumber: inboundFlightNumber || null,
        providerFlightId: inboundProviderFlightId,
        originAirportIata: inboundOrigin || null,
        destinationAirportIata: originIata || null,
        estimatedArrival: isoOrNull(
          record?.inbound_estimated_in ||
            record?.inbound_estimated_arrival_time ||
            record?.inbound_scheduled_in
        ),
        estimatedDeparture: isoOrNull(
          record?.inbound_estimated_out ||
            record?.inbound_estimated_off ||
            record?.inbound_scheduled_out
        ),
        actualDeparture: isoOrNull(record?.inbound_actual_out || record?.inbound_actual_off),
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
    arrivalTimezone:
      firstNonBlank(
        record?.destination?.timezone,
        record?.destination_timezone,
        record?.arrival_timezone,
        record?.timezone_destination
      ) || null,
    departureTimes,
    takeoffTimes,
    landingTimes,
    arrivalTimes,
    aircraftType: normalizeAircraftType(
      record?.aircraft_type ||
        record?.aircraftType ||
        record?.aircraft_type_iata ||
        record?.aircraft_type_icao ||
        record?.equipment ||
        record?.equipment_type ||
        record?.aircraft?.type ||
        record?.aircraft?.iata ||
        record?.aircraft?.icao
    ),
    status: normalizeStatus(record?.status || record?.flight_status),
    departureTerminal: firstNonBlank(
      record?.origin?.terminal,
      record?.terminal_origin,
      record?.terminalOrigin,
      record?.origin_terminal,
      record?.departure_terminal,
      record?.departureTerminal,
      record?.actual_departure_terminal,
      record?.actualDepartureTerminal,
      record?.estimated_departure_terminal,
      record?.estimatedDepartureTerminal,
      record?.scheduled_departure_terminal,
      record?.scheduledDepartureTerminal,
      record?.terminalOut,
      record?.terminal
    ),
    departureGate: firstNonBlank(
      record?.origin?.gate,
      record?.gate_origin,
      record?.gateOrigin,
      record?.origin_gate,
      record?.departure_gate,
      record?.departureGate,
      record?.actual_departure_gate,
      record?.actualDepartureGate,
      record?.estimated_departure_gate,
      record?.estimatedDepartureGate,
      record?.scheduled_departure_gate,
      record?.scheduledDepartureGate,
      record?.terminal_gate_origin,
      record?.gateOut,
      record?.gate
    ),
    arrivalTerminal: firstNonBlank(
      record?.destination?.terminal,
      record?.terminal_destination,
      record?.terminalDestination,
      record?.destination_terminal,
      record?.arrival_terminal,
      record?.arrivalTerminal,
      record?.actual_arrival_terminal,
      record?.actualArrivalTerminal,
      record?.estimated_arrival_terminal,
      record?.estimatedArrivalTerminal,
      record?.scheduled_arrival_terminal,
      record?.scheduledArrivalTerminal,
      record?.terminalIn
    ),
    arrivalGate: firstNonBlank(
      record?.destination?.gate,
      record?.gate_destination,
      record?.gateDestination,
      record?.destination_gate,
      record?.arrival_gate,
      record?.arrivalGate,
      record?.actual_arrival_gate,
      record?.actualArrivalGate,
      record?.estimated_arrival_gate,
      record?.estimatedArrivalGate,
      record?.scheduled_arrival_gate,
      record?.scheduledArrivalGate,
      record?.terminal_gate_destination,
      record?.gateIn
    ),
    terminal: firstNonBlank(
      record?.origin?.terminal,
      record?.terminal_origin,
      record?.terminalOrigin,
      record?.departure_terminal,
      record?.departureTerminal,
      record?.terminalOut,
      record?.terminal
    ),
    gate: firstNonBlank(
      record?.origin?.gate,
      record?.gate_origin,
      record?.gateOrigin,
      record?.departure_gate,
      record?.departureGate,
      record?.terminal_gate_origin,
      record?.gateOut,
      record?.gate
    ),
    baggageClaim:
      record?.baggage_claim ||
      record?.baggage_belt ||
      record?.arrival_baggage_claim ||
      record?.estimated_arrival_baggage_claim ||
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
  if (type === "onblock") return "arrived_at_gate";
  if (type === "arrival") return "landed";
  if (type === "offblock") return "taxiing";
  if (type === "departure") return "airborne";
  if (type === "position") {
    const airGround = String(message?.air_ground || "").trim().toUpperCase();
    if (airGround !== "G") return "enroute";
    if (["landed", "taxi_in", "arrived_at_gate"].includes(String(previousStatus || "").toLowerCase())) {
      return "taxi_in";
    }
    return "taxiing";
  }

  const statusCode = String(message?.status || "").trim().toUpperCase();
  if (statusCode === "X") return "cancelled";
  if (statusCode === "A") return "enroute";
  if (statusCode === "F") return "scheduled";

  if (message?.actual_in) {
    return "arrived_at_gate";
  }
  if (message?.actual_on || message?.aat) {
    return "landed";
  }
  if (message?.actual_off || message?.adt) {
    return "airborne";
  }
  if (message?.actual_out) {
    return "taxiing";
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
      null,
    gate:
      message?.actual_departure_gate ||
      message?.estimated_departure_gate ||
      message?.scheduled_departure_gate ||
      null,
    terminal_destination:
      message?.actual_arrival_terminal ||
      message?.estimated_arrival_terminal ||
      message?.scheduled_arrival_terminal ||
      null,
    gate_destination:
      message?.actual_arrival_gate ||
      message?.estimated_arrival_gate ||
      message?.scheduled_arrival_gate ||
      null,
    baggage_claim:
      message?.baggage_claim ||
      message?.baggage_belt ||
      message?.arrival_baggage_claim ||
      message?.estimated_arrival_baggage_claim ||
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
    arrivalTimezone:
      firehoseNormalized.arrivalTimezone ||
      previousNormalized?.arrivalTimezone ||
      null,
    departureTimes: mergedDepartureTimes,
    takeoffTimes: mergedTakeoffTimes,
    landingTimes: mergedLandingTimes,
    arrivalTimes: mergedArrivalTimes,
    status: nextStatus,
    terminal: firehoseNormalized.terminal || previousNormalized?.terminal || null,
    gate: firehoseNormalized.gate || previousNormalized?.gate || null,
    departureTerminal: firehoseNormalized.departureTerminal || previousNormalized?.departureTerminal || null,
    departureGate: firehoseNormalized.departureGate || previousNormalized?.departureGate || null,
    arrivalTerminal: firehoseNormalized.arrivalTerminal || previousNormalized?.arrivalTerminal || null,
    arrivalGate: firehoseNormalized.arrivalGate || previousNormalized?.arrivalGate || null,
    baggageClaim: firehoseNormalized.baggageClaim || previousNormalized?.baggageClaim || null,
    delayMinutes: calculateDelayMinutes(mergedDepartureTimes),
    inboundFlight: firehoseNormalized.inboundFlight || previousNormalized?.inboundFlight || null,
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
    timezoneOffsetMinutes: normalizedTimezoneOffsetMinutes(query.timezoneOffsetMinutes),
  });
}

function flightNumberSuffix(input) {
  return normalizeFlightCode(input).match(/(\d+[A-Z]?)$/)?.[1] || null;
}

function dedupeFlightAwareRecords(records, query = {}) {
  const seen = new Set();
  const deduped = [];
  const requestedFlightNumber = normalizeFlightCode(query?.flightNumber);
  const requestedSuffix = flightNumberSuffix(requestedFlightNumber);

  for (const record of Array.isArray(records) ? records : []) {
    const normalized = normalizeRecordFromFlightAware(record);
    const normalizedFlightNumber = normalizeFlightCode(normalized.flightNumber);
    // FlightAware can return the same occurrence once as an IATA marketing
    // flight (DL2307) and once as its ICAO/operator identity (DAL2307). When
    // both share the requested numeric suffix, treat the requested code as the
    // canonical identity; route + departure time still preserve legitimate
    // multiple same-day occurrences.
    const occurrenceFlightNumber = requestedFlightNumber && requestedSuffix &&
      flightNumberSuffix(normalizedFlightNumber) === requestedSuffix
      ? requestedFlightNumber
      : normalizedFlightNumber;
    const scheduledDeparture =
      normalized.departureTimes?.scheduled || normalized.departureTimes?.estimated || "";
    const occurrenceKey = [
      occurrenceFlightNumber,
      normalized.departureAirportIata || "",
      normalized.arrivalAirportIata || "",
      scheduledDeparture,
    ].join("|");
    const hasOccurrenceIdentity = Boolean(occurrenceFlightNumber && scheduledDeparture);
    const key = hasOccurrenceIdentity
      ? occurrenceKey
      : String(record?.fa_flight_id || occurrenceKey);

    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(record);
  }

  return deduped;
}

function flattenFlightAwareSearchRecords(records) {
  return (Array.isArray(records) ? records : []).flatMap((record) => {
    if (Array.isArray(record?.segments)) {
      return record.segments;
    }
    return [record];
  });
}

function extractFlightAwareSearchRows(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  if (Array.isArray(payload.flights)) {
    return dedupeFlightAwareRecords(flattenFlightAwareSearchRecords(payload.flights));
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
      rows.push(...flattenFlightAwareSearchRecords(payload[bucketName]));
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
    ...(Array.isArray(record?.codeshares_iata) ? record.codeshares_iata : []),
    ...(Array.isArray(record?.codeshares) ? record.codeshares : []),
  ];

  return Array.from(
    new Set(
      candidates
        .map((value) => normalizeFlightCode(value))
        .filter(Boolean)
    )
  );
}

function flightAwareRecordMatchesRequestedFlight(record, query) {
  const wantedFlight = normalizeFlightCode(query?.flightNumber);
  if (!wantedFlight) {
    return false;
  }

  return flightAwareMatchableFlightCodes(record, normalizeRecordFromFlightAware)
    .includes(wantedFlight);
}

function applyRequestedFlightIdentity(normalized, record, query) {
  const wantedFlight = normalizeFlightCode(query?.flightNumber);
  if (
    !wantedFlight ||
    !flightAwareRecordMatchesRequestedFlight(record, query)
  ) {
    return normalized;
  }

  return {
    ...normalized,
    airlineCode: parseAirlineCode(wantedFlight) || normalized?.airlineCode || null,
    flightNumber: wantedFlight,
  };
}

function normalizedQueryDateString(queryDate) {
  const normalizedDate = String(queryDate || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalizedDate) ? normalizedDate : null;
}

function normalizedTimezoneOffsetMinutes(rawOffsetMinutes) {
  const numericValue = Number(rawOffsetMinutes);
  if (!Number.isFinite(numericValue)) {
    return 0;
  }

  const roundedValue = Math.trunc(numericValue);
  return Math.max(-840, Math.min(840, roundedValue));
}

function utcDateStringFromMs(referenceTimeMs) {
  const reference = new Date(referenceTimeMs);
  return Number.isFinite(reference.getTime()) ? reference.toISOString().slice(0, 10) : null;
}

function currentDateStringForTimezone(referenceTimeMs = Date.now(), timezoneOffsetMinutes = 0) {
  const shiftedReferenceTimeMs =
    referenceTimeMs + normalizedTimezoneOffsetMinutes(timezoneOffsetMinutes) * 60 * 1000;
  return utcDateStringFromMs(shiftedReferenceTimeMs);
}

function flightAwareLocalDayInterval(queryDate, timezoneOffsetMinutes = 0) {
  const normalizedDate = normalizedQueryDateString(queryDate);
  if (!normalizedDate) {
    return null;
  }

  const [year, month, day] = normalizedDate.split("-").map((part) => Number.parseInt(part, 10));
  if (![year, month, day].every(Number.isFinite)) {
    return null;
  }

  const offsetMs = normalizedTimezoneOffsetMinutes(timezoneOffsetMinutes) * 60 * 1000;
  const startMs = Date.UTC(year, month - 1, day, 0, 0, 0) - offsetMs;
  if (!Number.isFinite(startMs)) {
    return null;
  }

  return {
    startMs,
    endMs: startMs + 24 * 60 * 60 * 1000,
  };
}

function secondPrecisionISOString(referenceTimeMs) {
  const reference = new Date(referenceTimeMs);
  if (!Number.isFinite(reference.getTime())) {
    return null;
  }

  return reference.toISOString().replace(".000Z", "Z");
}

function nextUTCDateString(referenceTimeMs) {
  const dateString = utcDateStringFromMs(referenceTimeMs);
  if (!dateString) {
    return null;
  }

  const date = new Date(`${dateString}T00:00:00Z`);
  if (!Number.isFinite(date.getTime())) {
    return null;
  }

  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

function flightAwareDateWindow(queryDate, timezoneOffsetMinutes = 0) {
  const interval = flightAwareLocalDayInterval(queryDate, timezoneOffsetMinutes);
  return interval ? interval.startMs + 12 * 60 * 60 * 1000 : null;
}

function isFutureFlightAwareQueryDate(queryDate, referenceTimeMs = Date.now(), timezoneOffsetMinutes = 0) {
  const normalizedDate = normalizedQueryDateString(queryDate);
  const currentDate = currentDateStringForTimezone(referenceTimeMs, timezoneOffsetMinutes);
  if (!normalizedDate || !currentDate) {
    return false;
  }

  return normalizedDate > currentDate;
}

function flightAwareHistoryBounds(queryDate, timezoneOffsetMinutes = 0) {
  const interval = flightAwareLocalDayInterval(queryDate, timezoneOffsetMinutes);
  if (!interval) {
    return null;
  }

  const start = utcDateStringFromMs(interval.startMs);
  const end = nextUTCDateString(interval.endMs - 1);
  if (!start || !end) {
    return null;
  }

  return {
    start,
    end,
  };
}

function shouldUseHistoricalFlightAwareSearch(query) {
  return query?.historical === true;
}

function shouldPrioritizeFlightAwareSchedules(query, referenceTimeMs = Date.now()) {
  const timezoneOffsetMinutes = query?.timezoneOffsetMinutes;
  return (
    query?.preferSchedules === true ||
    isFutureFlightAwareQueryDate(query?.date, referenceTimeMs, timezoneOffsetMinutes) ||
    shouldPreferFlightAwareSchedules(query?.date, referenceTimeMs, timezoneOffsetMinutes)
  );
}

function shouldPreferFlightAwareSchedules(queryDate, referenceTimeMs = Date.now(), timezoneOffsetMinutes = 0) {
  const targetMs = flightAwareDateWindow(queryDate, timezoneOffsetMinutes);
  if (!Number.isFinite(targetMs)) {
    return false;
  }

  return Math.abs(targetMs - referenceTimeMs) > FLIGHTAWARE_SCHEDULE_WINDOW_MS;
}

function flightAwareOperationalBounds(queryDate, timezoneOffsetMinutes = 0) {
  const interval = flightAwareLocalDayInterval(queryDate, timezoneOffsetMinutes);
  if (!interval) {
    return null;
  }

  return {
    start: secondPrecisionISOString(interval.startMs),
    end: secondPrecisionISOString(interval.endMs - 1000),
  };
}

// A flight number is reused every operating day. Before the origin airport is
// known, a date selected on the phone cannot safely be converted with the
// phone's timezone: the requested origin-local day can begin up to 14 hours
// before UTC and end up to 12 hours after UTC. Fetch that complete occurrence
// window in one provider request, then let normalized origin/status data select
// the correct instance.
function flightAwareOccurrenceBounds(query) {
  const normalizedDate = normalizedQueryDateString(query?.date);
  const isUnscopedFlightNumberSearch = Boolean(query?.flightNumber) && !query?.departureIata;
  if (!normalizedDate || !isUnscopedFlightNumberSearch) {
    return flightAwareOperationalBounds(query?.date, query?.timezoneOffsetMinutes);
  }

  const selectedDayStartMs = Date.parse(`${normalizedDate}T00:00:00Z`);
  if (!Number.isFinite(selectedDayStartMs)) {
    return null;
  }

  return {
    start: secondPrecisionISOString(selectedDayStartMs - 14 * 60 * 60 * 1000),
    end: secondPrecisionISOString(selectedDayStartMs + 36 * 60 * 60 * 1000 - 1000),
  };
}

function flightAwareScheduleBounds(queryDate, timezoneOffsetMinutes = 0) {
  return flightAwareOperationalBounds(queryDate, timezoneOffsetMinutes);
}

function flightAwareScheduleQueryItems(query, { includeAirline = true } = {}) {
  // AeroAPI's schedule response is paginated at up to 15 records per result set.
  // Runwy searches by a specific airline and flight number, so one page normally
  // preserves multiple same-day occurrences without reserving unnecessary cost.
  const params = new URLSearchParams({
    max_pages: String(FLIGHTAWARE_SCHEDULE_MAX_PAGES),
  });

  if (query.flightNumber) {
    const normalizedFlightNumber = normalizeFlightCode(query.flightNumber);
    const parts = normalizedFlightNumber.match(/^([A-Z0-9]{2,3}?)([0-9]{1,4}[A-Z]?)$/);
    if (parts) {
      if (includeAirline) {
        params.set("airline", parts[1]);
      }
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
  if (!ident) {
    return [];
  }

  const params = new URLSearchParams({ max_pages: "1" });
  const bounds = flightAwareOccurrenceBounds(query);

  if (bounds) {
    params.set("start", bounds.start);
    params.set("end", bounds.end);
  }

  const url = `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(ident)}?${params.toString()}`;

  const response = await flightAwareFlightFetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  }, { endpoint: "operational" });

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
  const bounds = flightAwareOccurrenceBounds(query);
  if (!bounds) {
    return [];
  }

  const fetchSchedulePage = async (includeAirline) => {
    const params = flightAwareScheduleQueryItems(query, { includeAirline });
    const queryString = params.toString();
    const url =
      `${FLIGHTAWARE_BASE_URL}/schedules/${encodeURIComponent(bounds.start)}` +
      `/${encodeURIComponent(bounds.end)}${queryString ? `?${queryString}` : ""}`;

    const response = await flightAwareFlightFetch(url, {
      method: "GET",
      headers: {
        "x-apikey": FLIGHTAWARE_API_KEY,
        Accept: "application/json",
      },
    }, { endpoint: "schedules", units: FLIGHTAWARE_SCHEDULE_MAX_PAGES });

    if (response.status === 400 || response.status === 404) {
      return [];
    }

    if (!response.ok) {
      throw new Error(`Provider error (${response.status})`);
    }

    const payload = await response.json();
    return extractFlightAwareSearchRows(payload).filter((record) =>
      flightAwareRecordMatchesRequestedFlight(record, query)
    );
  };

  const directMatches = await fetchSchedulePage(true);
  if (directMatches.length > 0) {
    return directMatches;
  }

  // Regional services can be indexed only under the operating carrier. A
  // second, still single-page lookup by flight number exposes codeshare fields
  // such as AA5091 on JIA5091. Strict matching discards unrelated airlines that
  // happen to reuse the same numeric flight number.
  return fetchSchedulePage(false);
}

async function fetchFlightAwareHistoricalFlights(query) {
  const ident = normalizeFlightCode(query.flightNumber);
  const bounds = flightAwareHistoryBounds(query.date, query.timezoneOffsetMinutes);
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

  const response = await flightAwareFlightFetch(url, {
    method: "GET",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  }, { endpoint: "history" });

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
    altitudeFeet: flightAwareAltitudeFeet(record, properties),
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

async function fetchFlightAwareTrackTrail(providerFlightId, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return { trackPoints: [], livePosition: null };
  }

  const normalizedFlightId = String(providerFlightId || "").trim();
  if (!normalizedFlightId) {
    return { trackPoints: [], livePosition: null };
  }

  const cacheKey = makeProviderTrackKey("flightaware", normalizedFlightId);
  const now = Date.now();
  const forceRefresh = options.forceRefresh === true;
  const cached = providerCache.get(cacheKey);
  if (!forceRefresh && cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(cacheKey, async () => {
    const response = await flightAwareFlightFetch(
      `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(normalizedFlightId)}/track`,
      {
        method: "GET",
        headers: {
          "x-apikey": FLIGHTAWARE_API_KEY,
          Accept: "application/json",
        },
      },
      { endpoint: "track" }
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

function coalesceFlightAwareTrackTrail(trackTrail, livePosition = null) {
  const normalizedTrackPoints = Array.isArray(trackTrail?.trackPoints) ? trackTrail.trackPoints : [];
  const normalizedLivePosition = trackTrail?.livePosition || livePosition || null;

  return {
    trackPoints: normalizedTrackPoints,
    livePosition: normalizedLivePosition,
  };
}

let airportCoordinatesByCode = null;

function airportCoordinateForCode(code) {
  const normalizedCode = normalizeAirportCode(code);
  if (!normalizedCode) return null;

  if (!airportCoordinatesByCode) {
    try {
      const catalog = getAirportCatalog();
      airportCoordinatesByCode = new Map(
        catalog.airports
          .map((airport) => [
            normalizeAirportCode(airport?.code),
            normalizeCoordinatePoint(airport?.coordinate),
          ])
          .filter(([airportCode, coordinate]) => airportCode && coordinate)
      );
    } catch (_error) {
      airportCoordinatesByCode = new Map();
    }
  }

  return airportCoordinatesByCode.get(normalizedCode) || null;
}

function normalizeCoordinatePoint(point) {
  const latitude = Number(point?.latitude);
  const longitude = Number(point?.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null;
  }
  return { latitude, longitude };
}

function appendCoordinateIfUseful(polyline, coordinate) {
  const normalized = normalizeCoordinatePoint(coordinate);
  if (!normalized) return;

  const last = polyline[polyline.length - 1];
  if (last && distanceBetweenCoordinatesMeters(last, normalized) < 250) {
    return;
  }
  polyline.push(normalized);
}

function routePolylineFromTrackTrail({ originIata, destinationIata, trackTrail }) {
  const polyline = [];
  appendCoordinateIfUseful(polyline, airportCoordinateForCode(originIata));

  const trackPoints = compactTrackPoints([
    ...(Array.isArray(trackTrail?.trackPoints) ? trackTrail.trackPoints : []),
    trackTrail?.livePosition || null,
  ]);
  for (const point of trackPoints) {
    appendCoordinateIfUseful(polyline, point);
  }

  appendCoordinateIfUseful(polyline, airportCoordinateForCode(destinationIata));
  return polyline.length >= 3 ? polyline : [];
}

async function fetchFlightAwareTrackTrailWithLiveFallback(providerFlightId, options = {}) {
  const trackTrail = await fetchFlightAwareTrackTrail(providerFlightId, options);
  if ((trackTrail?.trackPoints || []).length > 0 || trackTrail?.livePosition) {
    return coalesceFlightAwareTrackTrail(trackTrail);
  }

  const livePosition = await fetchFlightAwareLivePosition(providerFlightId, options);
  return coalesceFlightAwareTrackTrail(trackTrail, livePosition);
}

async function fetchFlightAwareLivePosition(providerFlightId, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return null;
  }

  const normalizedFlightId = String(providerFlightId || "").trim();
  if (!normalizedFlightId) return null;

  const cacheKey = makeProviderPositionKey("flightaware", normalizedFlightId);
  const now = Date.now();
  const forceRefresh = options.forceRefresh === true;
  const cached = providerCache.get(cacheKey);
  if (!forceRefresh && cached && cached.expiresAt > now) {
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
      const response = await flightAwareFlightFetch(`${FLIGHTAWARE_BASE_URL}${path}`, {
        method: "GET",
        headers: {
          "x-apikey": FLIGHTAWARE_API_KEY,
          Accept: "application/json",
        },
      }, { endpoint: path.endsWith("/track") ? "track_fallback" : path.endsWith("/map") ? "map_fallback" : "position" });

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
    const trackTrail = await fetchFlightAwareTrackTrailWithLiveFallback(providerFlightId);
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

  if (
    [
      "boarding",
      "taxiing",
      "taxi_out",
      "takeoff_roll",
      "departed",
      "airborne",
      "enroute",
      "taxi_in",
    ].includes(normalized.status)
  ) score += 8;
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
  const recentEventWindowMs = 30 * 60 * 1000;
  const toMs = (value) => {
    const instant = value ? new Date(value).getTime() : NaN;
    return Number.isFinite(instant) ? instant : null;
  };
  const isRecent = (value) => {
    const instant = toMs(value);
    if (instant == null) return false;
    return Math.abs(Date.now() - instant) <= recentEventWindowMs;
  };
  const normalizedInboundStatus = (value) =>
    String(value?.inboundFlight?.status || "").toLowerCase() || null;

  if (!previousNormalized || !nextNormalized) {
    const currentStatus = nextNormalized?.status || null;
    const currentInAir = ["departed", "airborne", "enroute"].includes(currentStatus);
    const recentDeparture =
      isRecent(nextNormalized?.takeoffTimes?.actual) ||
      isRecent(nextNormalized?.departureTimes?.actual) ||
      isRecent(nextNormalized?.takeoffTimes?.estimated);
    const recentArrival =
      isRecent(nextNormalized?.landingTimes?.actual) ||
      isRecent(nextNormalized?.arrivalTimes?.actual);

    return {
      statusChanged: false,
      delayedNow: false,
      cancelledNow: false,
      gateChangedNow: false,
      departedNow: currentInAir && recentDeparture,
      arrivedNow: ["landed", "arrived", "arrived_at_gate"].includes(currentStatus) && recentArrival,
      taxiingNow: ["taxiing", "taxi_out", "taxi_in"].includes(currentStatus),
      takeoffNow: currentStatus === "takeoff_roll",
      baggageBeltAssignedNow: Boolean(nextNormalized?.baggageClaim || nextNormalized?.baggageBelt),
      inboundArrivedNow: false,
      previousStatus: previousNormalized?.status || null,
      currentStatus,
    };
  }

  const previousDelay = Number(previousNormalized.delayMinutes || 0);
  const nextDelay = Number(nextNormalized.delayMinutes || 0);
  const previousGate = `${previousNormalized.gate || ""}`.trim();
  const nextGate = `${nextNormalized.gate || ""}`.trim();
  const previousStatus = previousNormalized.status || null;
  const currentStatus = nextNormalized.status || null;
  const previousInAir = ["departed", "airborne", "enroute"].includes(previousStatus);
  const currentInAir = ["departed", "airborne", "enroute"].includes(currentStatus);
  const previousTaxi = ["taxiing", "taxi_out", "taxi_in"].includes(previousStatus);
  const currentTaxi = ["taxiing", "taxi_out", "taxi_in"].includes(currentStatus);
  const previousBaggage = `${previousNormalized.baggageClaim || previousNormalized.baggageBelt || ""}`.trim();
  const nextBaggage = `${nextNormalized.baggageClaim || nextNormalized.baggageBelt || ""}`.trim();
  const previousInboundStatus = normalizedInboundStatus(previousNormalized);
  const currentInboundStatus = normalizedInboundStatus(nextNormalized);
  const delayIncreased = nextDelay > previousDelay;
  const hasPositiveDelay = nextDelay > 0;
  const delayEligibleStatuses = !["cancelled", "landed"].includes(currentStatus);

  return {
    statusChanged: previousStatus !== currentStatus,
    delayedNow:
      delayEligibleStatuses &&
      hasPositiveDelay &&
      (
        (currentStatus === "delayed" &&
          (previousStatus !== "delayed" || delayIncreased)) ||
        (currentStatus !== "delayed" && delayIncreased)
      ),
    cancelledNow:
      currentStatus === "cancelled" && previousStatus !== "cancelled",
    gateChangedNow:
      Boolean(nextGate) &&
      previousGate !== nextGate,
    departedNow:
      currentInAir &&
      !previousInAir &&
      previousStatus !== "landed",
    arrivedNow:
      ["landed", "arrived", "arrived_at_gate"].includes(currentStatus) &&
      !["landed", "arrived", "arrived_at_gate"].includes(previousStatus),
    taxiingNow:
      currentTaxi &&
      !previousTaxi,
    takeoffNow:
      currentStatus === "takeoff_roll" &&
      previousStatus !== "takeoff_roll",
    baggageBeltAssignedNow:
      Boolean(nextBaggage) &&
      previousBaggage !== nextBaggage,
    inboundArrivedNow:
      currentInboundStatus === "landed" &&
      previousInboundStatus !== "landed" &&
      !["cancelled", "landed"].includes(currentStatus),
    previousStatus,
    currentStatus,
  };
}

function departureTimeForNotification(normalized) {
  const candidates = [
    normalized?.departureTimes?.estimated,
    normalized?.departureTimes?.scheduled,
    normalized?.departureTimes?.actual,
  ];

  for (const candidate of candidates) {
    const instant = candidate ? new Date(candidate).getTime() : NaN;
    if (Number.isFinite(instant)) {
      return instant;
    }
  }

  return null;
}

function formatMinutesForNotification(totalMinutes) {
  const normalizedMinutes = Math.max(1, Math.round(Number(totalMinutes) || 0));
  const hours = Math.floor(normalizedMinutes / 60);
  const minutes = normalizedMinutes % 60;

  if (hours > 0 && minutes > 0) {
    return `${hours}h ${minutes}m`;
  }

  if (hours > 0) {
    return `${hours}h`;
  }

  return `${minutes}m`;
}

function departureCountdownTextForNotification(normalized, referenceMs = Date.now()) {
  const departureTimeMs = departureTimeForNotification(normalized);
  if (!Number.isFinite(departureTimeMs) || departureTimeMs <= referenceMs) {
    return null;
  }

  return `Departure is in ${formatMinutesForNotification((departureTimeMs - referenceMs) / 60000)}.`;
}

async function fetchAviationstackFlights(query, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return [];
  }

  if (!normalizeFlightCode(query.flightNumber)) {
    return [];
  }

  const key = makeProviderQueryKey("aviationstack", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (!options.forceRefresh && cached && cached.expiresAt > now) {
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

async function fetchFlightAwareSearchSources(fetchers, query, { mergeAll = false } = {}) {
  const rows = [];
  const errors = [];
  let completedSources = 0;

  for (const fetcher of fetchers) {
    try {
      const nextRows = await fetcher(query);
      completedSources += 1;
      if (nextRows.length > 0) {
        rows.push(...nextRows);
        if (!mergeAll) {
          break;
        }
      }
    } catch (error) {
      errors.push(error);
      console.warn("FlightAware search source failed; trying available fallback", {
        source: fetcher.name || "unknown",
        error: error?.message || String(error),
      });
    }
  }

  if (completedSources === 0 && errors.length > 0) {
    throw errors[0];
  }

  return dedupeFlightAwareRecords(rows, query);
}

async function fetchFlightAwareFlights(query, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return [];
  }

  // Airport-pair discovery is intentionally unsupported. Provider search must
  // always be anchored to a specific airline flight number.
  if (!normalizeFlightCode(query.flightNumber)) {
    return [];
  }

  const key = makeProviderQueryKey("flightaware", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (!options.forceRefresh && cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(key, async () => {
    let rows = [];

    if (shouldUseHistoricalFlightAwareSearch(query)) {
      rows = await fetchFlightAwareHistoricalFlights(query);
    } else {
      const prioritizeSchedules = shouldPrioritizeFlightAwareSchedules(query);
      const fetchers = prioritizeSchedules
        ? [fetchFlightAwareScheduleFlights, fetchFlightAwareOperationalFlights]
        : [fetchFlightAwareOperationalFlights, fetchFlightAwareScheduleFlights];

      rows = await fetchFlightAwareSearchSources(fetchers, query, {
        mergeAll: prioritizeSchedules,
      });
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

async function fetchFlightAwareFlightByProviderId(providerFlightId, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return null;
  }

  const normalizedFlightId = String(providerFlightId || "").trim();
  if (!normalizedFlightId) {
    return null;
  }

  const cacheKey = JSON.stringify({
    providerName: "flightaware",
    kind: "flight-instance",
    providerFlightId: normalizedFlightId,
  });
  const now = Date.now();
  const cached = providerCache.get(cacheKey);
  if (!options.forceRefresh && cached && cached.expiresAt > now) {
    return cached.data;
  }

  return withProviderRequestDedup(cacheKey, async () => {
    const response = await flightAwareFlightFetch(
      `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(normalizedFlightId)}`,
      {
        method: "GET",
        headers: {
          "x-apikey": FLIGHTAWARE_API_KEY,
          Accept: "application/json",
        },
      },
      { endpoint: "flight_instance" }
    );

    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`Provider flight-instance error (${response.status})`);
    }

    const payload = await response.json();
    const candidates = extractFlightAwareSearchRows(payload);
    const exact = candidates.find(
      (record) => String(record?.fa_flight_id || "").trim() === normalizedFlightId
    );
    const data = exact || (String(payload?.fa_flight_id || "").trim() === normalizedFlightId ? payload : null);

    providerCache.set(cacheKey, {
      data,
      expiresAt: now + CACHE_TTL_MS,
    });
    enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
    return data;
  });
}

function providerAdapter(preferredProvider = FLIGHT_DATA_PROVIDER) {
  if (preferredProvider === "flightaware") {
    return {
      name: "flightaware",
      fetchFlights: fetchFlightAwareFlights,
      fetchFlightByProviderId: fetchFlightAwareFlightByProviderId,
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
  let normalized = reconcileOperationalStatus(normalizer(record));
  if (normalizer === normalizeRecordFromFlightAware) {
    normalized = applyRequestedFlightIdentity(normalized, record, query);
  }
  const recentHistory = deriveRecentHistory(records, record, normalizer);
  const alerts = deriveAlertFlags(previousNormalized, normalized);

  return reconcileOperationalStatus(mergeRealtimeTelemetry(previousNormalized, {
    ...normalized,
    recentHistory,
    alerts,
    provider: FLIGHT_DATA_PROVIDER,
  }));
}

const sharedFlightRepository = pool
  ? createPostgresSharedFlightRepository(pool)
  : createMemorySharedFlightRepository();

const sharedFlightService = createSharedFlightService({
  repository: sharedFlightRepository,
  streamingEnabled: SHARED_FLIGHT_STREAMING_ENABLED,
  liveActivities: {
    sendFlightState: (flight) => sendLiveActivityStateForFlight(flight),
  },
  apns: createSharedApnsSender({
    send: async ({ token, payload, environment }) => sendApnsNotification(token, payload, environment),
  }),
  provider: createSharedProviderAdapter({
    providerName: FLIGHT_DATA_PROVIDER,
    fetchFlights: (query, options) => providerAdapter().fetchFlights(query, options),
    fetchByProviderId: FLIGHT_DATA_PROVIDER === "flightaware"
      ? (providerFlightId, options) => fetchFlightAwareFlightByProviderId(providerFlightId, options)
      : null,
    normalizeRecord: (record) => providerAdapter().normalizeRecord(record),
    normalizeSelected: (record, records, query) =>
      normalizeWithContext(record, records, query, providerAdapter().normalizeRecord, null),
    enrichNormalized: (normalized, record, _query, _params, options = {}) =>
      options.skipLivePosition
        ? normalized
        : enrichNormalizedWithLivePosition(normalized, providerAdapter().name, record, options),
    selectRecord: (records, query, normalizer) => bestMatch(records, query, normalizer),
    ensureFlightAlert: FLIGHT_DATA_PROVIDER === "flightaware"
      ? (flight, options) => ensureFlightAwareAlertForSharedFlight(flight, options)
      : null,
    ensureInboundFlightAlert: FLIGHT_DATA_PROVIDER === "flightaware"
      ? (flight, inboundFlight, options) => ensureFlightAwareAlertForInboundFlight(flight, inboundFlight, options)
      : null,
    alertConfigurationChangedAt: FLIGHTAWARE_ALERT_CONFIGURATION_CHANGED_AT,
  }),
});

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
  buildArchivedRoutePolyline: (normalized) =>
    routePolylineFromTrackTrail({
      originIata: normalized?.departureAirportIata,
      destinationIata: normalized?.arrivalAirportIata,
      trackTrail: {
        trackPoints: normalized?.trackPoints || [],
        livePosition: null,
      },
    }),
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

async function enrichNormalizedWithLivePosition(normalized, providerName, rawRecord, options = {}) {
  if (providerName !== "flightaware" || !isTrackableLiveStatus(normalized?.status)) {
    return normalized;
  }

  const providerFlightId = providerFlightIdentifier(rawRecord, providerName);
  if (!providerFlightId) {
    return normalized;
  }

  try {
    const livePosition = await fetchFlightAwareLivePosition(providerFlightId, options);
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

function isoDateOnly(value) {
  if (!value) return null;
  if (typeof value === "string") {
    const normalized = value.trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(normalized)) return normalized.slice(0, 10);
  }
  const date = value instanceof Date ? value : new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString().slice(0, 10) : null;
}

function flightAwareAlertContextForTrackedRecord(trackedRecord) {
  if (!trackedRecord || String(trackedRecord.provider || "").toLowerCase() !== "flightaware") {
    return null;
  }

  const status = String(trackedRecord.normalized?.status || "").toLowerCase();
  if (["landed", "arrived", "arrived_at_gate", "cancelled", "diverted"].includes(status)) {
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
  const departureTime =
    trackedRecord.normalized?.departureTimes?.estimated ||
    trackedRecord.normalized?.departureTimes?.scheduled ||
    trackedRecord.normalized?.departureTimes?.actual ||
    null;
  const timezoneOffsetMinutes = normalizedTimezoneOffsetMinutes(
    trackedRecord.query?.timezoneOffsetMinutes
  );

  if (!flightNumber || !startDate) {
    return null;
  }

  const endDate = addDaysToISODate(startDate, 2);
  if (!endDate) {
    return null;
  }

  return {
    flightNumber,
    providerFlightId: trackedRecord.providerFlightId || null,
    status,
    departureIata,
    arrivalIata,
    startDate,
    endDate,
    departureTime,
    timezoneOffsetMinutes,
  };
}

function flightAwareAlertContextForSharedFlight(flight) {
  if (!flight || String(flight.provider || "").toLowerCase() !== "flightaware") return null;
  const status = String(flight.status || "").toLowerCase();
  if (["landed", "arrived", "arrived_at_gate", "cancelled", "diverted"].includes(status)) return null;
  // PostgreSQL timestamp columns are Date objects at runtime. Prefer the
  // provider timestamp and normalize it without String(Date), which produces
  // values such as "Fri Aug 28" and silently prevents alert registration.
  const startDate = isoDateOnly(
    flight.scheduled_departure_at ||
    flight.estimated_departure_at ||
    flight.departure_date
  );
  const endDate = addDaysToISODate(startDate, 2);
  const flightNumber = normalizeFlightCode(`${flight.airline_code || ""}${flight.flight_number || ""}`);
  if (!flightNumber || !startDate || !endDate) return null;
  return {
    flightNumber,
    providerFlightId: flight.provider_flight_id || null,
    status,
    departureIata: normalizeAirportCode(flight.origin_airport) || null,
    arrivalIata: normalizeAirportCode(flight.destination_airport) || null,
    startDate,
    endDate,
    departureTime: flight.actual_departure_at || flight.estimated_departure_at || flight.scheduled_departure_at || null,
    timezoneOffsetMinutes: null,
  };
}

async function ensureFlightAwareAlertForSharedFlight(flight) {
  const targetUrl = flightAwareWebhookTargetURL(null);
  if (!targetUrl) {
    throw new Error("FlightAware webhook URL is unavailable; configure WEBHOOK_SHARED_SECRET and WEBHOOK_PUBLIC_BASE_URL");
  }
  const context = flightAwareAlertContextForSharedFlight(flight);
  if (!context) return null;
  const disposition = flightAwareAlertCreationDisposition(context);
  if (!disposition.eligible) return null;
  const creationContext = { ...context, windowStrategy: disposition.windowStrategy };
  const created = flight.provider_alert_id
    ? await updateFlightAwareAlert({ alertId: flight.provider_alert_id, targetUrl, context: creationContext })
    : await createFlightAwareAlert({ targetUrl, context: creationContext });
  return {
    providerAlertId: created.alertId,
    status: "active",
    createdAt: new Date().toISOString(),
    expiresAt: `${context.endDate}T23:59:59.999Z`,
    refreshPriority: "minimal",
  };
}

async function ensureFlightAwareAlertForInboundFlight(_flight, inboundFlight) {
  const targetUrl = flightAwareWebhookTargetURL(null);
  if (!targetUrl) {
    throw new Error("FlightAware webhook URL is unavailable; configure WEBHOOK_SHARED_SECRET and WEBHOOK_PUBLIC_BASE_URL");
  }
  const providerFlightId = String(inboundFlight?.providerFlightId || "").trim();
  if (!providerFlightId) return null;
  const context = {
    providerFlightId,
    flightNumber: inboundFlight.flightNumber || providerFlightId.split("-")[0],
    departureIata: normalizeAirportCode(inboundFlight.originAirportIata) || null,
    arrivalIata: normalizeAirportCode(inboundFlight.destinationAirportIata) || null,
    windowStrategy: "open",
    impendingDepartureMinutes: [],
    impendingArrivalMinutes: [],
    events: {
      arrival: false,
      cancelled: true,
      departure: false,
      diverted: true,
      filed: false,
      out: false,
      off: true,
      on: false,
      in: false,
      hold_start: false,
      hold_end: false,
    },
  };
  const created = inboundFlight.providerAlertId
    ? await updateFlightAwareAlert({ alertId: inboundFlight.providerAlertId, targetUrl, context })
    : await createFlightAwareAlert({ targetUrl, context });
  return {
    providerAlertId: created.alertId,
    status: "active",
    createdAt: new Date().toISOString(),
  };
}

async function updateFlightAwareAlert({ alertId, targetUrl, context }) {
  await ensureFlightAwareAlertEndpoint(targetUrl);
  const payload = buildFlightAwareAlertPayload({ targetUrl, context });
  const response = await fetch(`${FLIGHTAWARE_BASE_URL}/alerts/${encodeURIComponent(alertId)}`, {
    method: "PUT",
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`FlightAware alert update failed (${response.status}): ${responseText.slice(0, 200)}`);
  }
  return { alertId };
}

function flightAwareAlertFingerprint(context) {
  return crypto.createHash("sha256").update(JSON.stringify(context)).digest("hex");
}

function flightAwareAlertCreationDisposition(context, nowISO = new Date().toISOString()) {
  const startDate = String(context?.startDate || "").slice(0, 10);
  const referenceTimeMs = new Date(nowISO || "").getTime();
  const currentDate = currentDateStringForTimezone(
    referenceTimeMs,
    context?.timezoneOffsetMinutes
  );

  if (!startDate || !currentDate || !Number.isFinite(referenceTimeMs)) {
    return {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "bounded",
    };
  }

  const departureTimeMs = new Date(context?.departureTime || "").getTime();
  const status = String(context?.status || "").toLowerCase();
  const flightHasStarted =
    ["departed", "airborne", "enroute", "approaching", "taxi_in"].includes(status) ||
    (Number.isFinite(departureTimeMs) && departureTimeMs <= referenceTimeMs);

  // AeroAPI rejects a bounded alert whose start date is before the current
  // day. An exact, open alert remains useful for a flight that is already
  // underway (including overnight flights whose departure date has rolled
  // into the previous local day).
  if (flightHasStarted) {
    return {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "open",
    };
  }

  if (startDate < currentDate) {
    return {
      eligible: false,
      reason: "start_date_in_past",
      detail: `Skipping FlightAware alert auto-create because start date ${startDate} is before current local date ${currentDate}.`,
      windowStrategy: "bounded",
    };
  }

  if (startDate === currentDate) {
    return {
      eligible: true,
      reason: null,
      detail: null,
      windowStrategy: "open",
    };
  }

  return {
    eligible: true,
    reason: null,
    detail: null,
    windowStrategy: "bounded",
  };
}

function flightAwareAlertIDFromPayload(payload) {
  if (typeof payload === "string" || typeof payload === "number") {
    const scalar = String(payload).trim();
    return scalar || null;
  }

  if (Array.isArray(payload)) {
    for (const item of payload) {
      const alertId = flightAwareAlertIDFromPayload(item);
      if (alertId) return alertId;
    }
    return null;
  }

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

function flightAwareAlertIDFromLocation(location) {
  const value = String(location || "").trim();
  if (!value) return null;

  const match = value.match(/(?:^|\/)alerts\/([^/?#]+)(?:[?#].*)?$/i);
  return match ? decodeURIComponent(match[1]).trim() || null : null;
}

function buildFlightAwareAlertPayload({ targetUrl, context }) {
  const payload = {
    // A fa_flight_id identifies one concrete flight instance and prevents an
    // open alert from following the same recurring flight number tomorrow.
    ident: context.providerFlightId || context.flightNumber,
    impending_departure: [...(context.impendingDepartureMinutes ?? FLIGHTAWARE_AUTO_ALERT_IMPENDING_DEPARTURE_MINUTES)],
    impending_arrival: [...(context.impendingArrivalMinutes ?? FLIGHTAWARE_AUTO_ALERT_IMPENDING_ARRIVAL_MINUTES)],
    events: context.events || FLIGHTAWARE_AUTO_ALERT_EVENTS,
    target_url: targetUrl,
  };

  if (context.windowStrategy !== "open") {
    payload.start = context.startDate;
    payload.end = context.endDate;
  }

  if (context.departureIata) {
    payload.origin = context.departureIata;
  }
  if (context.arrivalIata) {
    payload.destination = context.arrivalIata;
  }

  return payload;
}

function flightAwareAlertRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.alerts)) return payload.alerts;
  if (payload && typeof payload === "object") return [payload];
  return [];
}

function normalizeAlertAirport(value) {
  return normalizeAirportCode(
    typeof value === "object"
      ? value?.code_iata || value?.iata || value?.code || value?.airport_code
      : value
  );
}

function matchingFlightAwareAlertID(payload, context, targetUrl) {
  const expectedIdents = new Set(
    [
      context?.providerFlightId,
      String(context?.providerFlightId || "").split("-")[0],
      context?.flightNumber,
    ]
      .map((value) => String(value || "").trim().toUpperCase())
      .filter(Boolean)
  );

  for (const alert of flightAwareAlertRows(payload)) {
    const ident = String(
      alert?.ident || alert?.ident_iata || alert?.ident_icao || ""
    ).trim().toUpperCase();
    if (expectedIdents.size > 0 && !expectedIdents.has(ident)) continue;

    const origin = normalizeAlertAirport(
      alert?.origin_iata || alert?.origin || alert?.origin_icao
    );
    const destination = normalizeAlertAirport(
      alert?.destination_iata || alert?.destination || alert?.destination_icao
    );
    if (context?.departureIata && origin && origin !== context.departureIata) continue;
    if (context?.arrivalIata && destination && destination !== context.arrivalIata) continue;
    if (alert?.target_url && String(alert.target_url) !== String(targetUrl)) continue;

    const alertId = flightAwareAlertIDFromPayload(alert);
    if (alertId) return alertId;
  }

  return null;
}

async function verifyFlightAwareAlert({ targetUrl, context }) {
  const response = await fetch(`${FLIGHTAWARE_BASE_URL}/alerts`, {
    headers: {
      "x-apikey": FLIGHTAWARE_API_KEY,
      Accept: "application/json",
    },
  });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`FlightAware alert verification failed (${response.status}): ${responseText.slice(0, 200)}`);
  }

  let responsePayload = null;
  try {
    responsePayload = responseText ? JSON.parse(responseText) : null;
  } catch (_error) {
    responsePayload = null;
  }
  return matchingFlightAwareAlertID(responsePayload, context, targetUrl);
}

async function createFlightAwareAlert({ targetUrl, context }) {
  await ensureFlightAwareAlertEndpoint(targetUrl);
  const payload = buildFlightAwareAlertPayload({ targetUrl, context });

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

  // AeroAPI returns 201 with an empty body and exposes the new alert ID in
  // `Location: /alerts/{id}`. Capture it before falling back to the eventually
  // consistent alert list; otherwise a successful alert is recorded as failed.
  const responseAlertId =
    flightAwareAlertIDFromPayload(responsePayload) ||
    flightAwareAlertIDFromLocation(response.headers.get("location"));
  const alertId =
    responseAlertId ||
    await verifyFlightAwareAlert({ targetUrl, context });
  if (!alertId) {
    throw new Error("FlightAware accepted the alert but Runwy could not verify its alert id");
  }

  return { alertId };
}

async function ensureFlightAwareAlertEndpoint(targetUrl) {
  if (flightAwareAlertEndpointReadyURL === targetUrl) return;
  if (flightAwareAlertEndpointPromise) {
    await flightAwareAlertEndpointPromise;
    if (flightAwareAlertEndpointReadyURL === targetUrl) return;
  }

  flightAwareAlertEndpointPromise = (async () => {
    const response = await fetch(`${FLIGHTAWARE_BASE_URL}/alerts/endpoint`, {
      method: "PUT",
      headers: {
        "x-apikey": FLIGHTAWARE_API_KEY,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ url: targetUrl }),
    });
    const responseText = await response.text();
    if (!response.ok) {
      throw new Error(`FlightAware alert endpoint registration failed (${response.status}): ${responseText.slice(0, 200)}`);
    }
    flightAwareAlertEndpointReadyURL = targetUrl;
  })();

  try {
    await flightAwareAlertEndpointPromise;
  } finally {
    flightAwareAlertEndpointPromise = null;
  }
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

  const creationDisposition = flightAwareAlertCreationDisposition(context);
  const creationContext = {
    ...context,
    windowStrategy: creationDisposition.windowStrategy,
  };
  const fingerprint = flightAwareAlertFingerprint(creationContext);
  const existing = trackedRecord.metadata?.flightawareAlert;
  if (
    existing?.fingerprint === fingerprint &&
    existing?.alertId
  ) {
    return;
  }

  const baseMetadata = {
    autoCreated: true,
    provider: "flightaware",
    fingerprint,
    windowStrategy: creationDisposition.windowStrategy,
    startDate: context.startDate,
    endDate: context.endDate,
    flightNumber: context.flightNumber,
    departureIata: context.departureIata,
    arrivalIata: context.arrivalIata,
    lastAttemptAt: new Date().toISOString(),
  };

  if (!creationDisposition.eligible) {
    await mergeTrackingSessionMetadata(trackedRecord.flightId, {
      flightawareAlert: {
        ...baseMetadata,
        alertId: null,
        createdAt: null,
        lastError: null,
        skipReason: creationDisposition.reason,
        skipDetail: creationDisposition.detail,
        skippedAt: new Date().toISOString(),
      },
    });
    return;
  }

  try {
    const createdAlert = await createFlightAwareAlert({ targetUrl, context: creationContext });
    await mergeTrackingSessionMetadata(trackedRecord.flightId, {
      flightawareAlert: {
        ...baseMetadata,
        alertId: createdAlert.alertId,
        createdAt: new Date().toISOString(),
        lastError: null,
        skipReason: null,
        skipDetail: null,
        skippedAt: null,
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
        skipReason: null,
        skipDetail: null,
        skippedAt: null,
      },
    });
  }
}

function apnsPrivateKeyMaterial() {
  if (APNS_PRIVATE_KEY) {
    return APNS_PRIVATE_KEY.replace(/\\n/g, "\n");
  }

  if (APNS_PRIVATE_KEY_BASE64) {
    return Buffer.from(APNS_PRIVATE_KEY_BASE64, "base64").toString("utf8");
  }

  return "";
}

function apnsPrivateKeySource() {
  if (APNS_PRIVATE_KEY) return "inline";
  if (APNS_PRIVATE_KEY_BASE64) return "base64";
  return null;
}

function apnsPrivateKeysConflict() {
  if (!APNS_PRIVATE_KEY || !APNS_PRIVATE_KEY_BASE64) return false;
  try {
    const inlineKey = crypto.createPublicKey(crypto.createPrivateKey(APNS_PRIVATE_KEY.replace(/\\n/g, "\n")))
      .export({ type: "spki", format: "der" });
    const base64Key = crypto.createPublicKey(
      crypto.createPrivateKey(Buffer.from(APNS_PRIVATE_KEY_BASE64, "base64").toString("utf8"))
    ).export({ type: "spki", format: "der" });
    return !crypto.timingSafeEqual(
      crypto.createHash("sha256").update(inlineKey).digest(),
      crypto.createHash("sha256").update(base64Key).digest()
    );
  } catch (_error) {
    return true;
  }
}

function apnsConfigStatus() {
  const privateKeySource = apnsPrivateKeySource();
  const privateKeyMaterial = apnsPrivateKeyMaterial();
  const missingEnv = [];

  if (!APNS_KEY_ID) missingEnv.push("APNS_KEY_ID");
  if (!APNS_TEAM_ID) missingEnv.push("APNS_TEAM_ID");
  if (!APNS_BUNDLE_ID) missingEnv.push("APNS_BUNDLE_ID");
  if (!privateKeySource) {
    missingEnv.push("APNS_PRIVATE_KEY or APNS_PRIVATE_KEY_BASE64");
  } else if (!privateKeyMaterial) {
    missingEnv.push(`${privateKeySource} private key material`);
  }
  if (apnsPrivateKeysConflict()) {
    missingEnv.push("APNS_PRIVATE_KEY conflicts with APNS_PRIVATE_KEY_BASE64");
  }

  return {
    configured: missingEnv.length === 0,
    bundleId: APNS_BUNDLE_ID || null,
    sandbox: APNS_USE_SANDBOX,
    host: apnsHost(),
    hasKeyId: Boolean(APNS_KEY_ID),
    hasTeamId: Boolean(APNS_TEAM_ID),
    hasBundleId: Boolean(APNS_BUNDLE_ID),
    privateKeySource,
    hasPrivateKeyMaterial: Boolean(privateKeyMaterial),
    missingEnv,
  };
}

function isApnsConfigured() {
  return apnsConfigStatus().configured;
}

function base64UrlEncode(input) {
  const buffer = Buffer.isBuffer(input) ? input : Buffer.from(input);
  return buffer
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function signApnsJwtInput(signingInput, privateKey) {
  // ES256 JWT signatures are the fixed-width 64-byte r || s representation.
  // Node defaults to ASN.1/DER for ECDSA, which APNs rejects as an invalid
  // provider token even though the underlying .p8 key is valid.
  return crypto.sign("sha256", Buffer.from(signingInput), {
    key: privateKey,
    dsaEncoding: "ieee-p1363",
  });
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
  const signature = signApnsJwtInput(signingInput, apnsPrivateKeyMaterial());
  const jwt = `${signingInput}.${base64UrlEncode(signature)}`;

  apnsTokenCache.token = jwt;
  apnsTokenCache.expiresAt = now + 50 * 60;
  return jwt;
}

function apnsHost(environment = null) {
  const normalizedEnvironment = String(environment || "").trim().toLowerCase();
  if (normalizedEnvironment === "sandbox") return "api.sandbox.push.apple.com";
  if (normalizedEnvironment === "production") return "api.push.apple.com";
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

function readableFlightCode(normalized) {
  const airline = normalizeFlightCode(normalized?.airlineCode);
  const combined = displayFlightCode(normalized);
  if (!airline || combined === "Flight") return combined;

  const suffix = combined.startsWith(airline)
    ? combined.slice(airline.length)
    : normalizeFlightCode(normalized?.flightNumber);
  return suffix ? `${airline} ${suffix}` : airline;
}

function firstNameForNotification(value) {
  return String(value || "").trim().split(/\s+/)[0] || null;
}

function routeCitiesForNotification(normalized) {
  const departureCode = normalizeAirportCode(normalized?.departureAirportIata);
  const arrivalCode = normalizeAirportCode(normalized?.arrivalAirportIata);
  const departure = normalized?.departureCity || airportForNotification(departureCode)?.city || departureCode;
  const arrival = normalized?.arrivalCity || airportForNotification(arrivalCode)?.city || arrivalCode;
  return departure && arrival ? `${departure} to ${arrival}` : null;
}

function flightSubjectForNotification(normalized, context = {}, ownerPrefix = "Flight") {
  const code = readableFlightCode(normalized);
  const travelerName = firstNameForNotification(context.travelerName);
  if (context.isOwner === false && travelerName) {
    return `${travelerName}'s flight ${code}`;
  }
  return `${ownerPrefix} ${code}`;
}

function airportForNotification(iata) {
  const code = normalizeAirportCode(iata);
  if (!code) return null;

  try {
    return getAirportCatalog().airports.find((airport) => airport.code === code) || null;
  } catch (_error) {
    return null;
  }
}

function weatherEmojiForNotification(conditionCode) {
  const condition = String(conditionCode || "").toLowerCase();
  if (condition.includes("thunder")) return "⛈️";
  if (condition.includes("snow") || condition.includes("sleet")) return "🌨️";
  if (condition.includes("rain") || condition.includes("drizzle")) return "🌧️";
  if (condition.includes("fog") || condition.includes("haze") || condition.includes("mist")) return "🌫️";
  if (condition.includes("partly") || condition.includes("mostly")) return "🌤️";
  if (condition.includes("cloud") || condition.includes("overcast")) return "☁️";
  if (condition.includes("clear") || condition.includes("sun")) return "☀️";
  return "🌤️";
}

function arrivalWeatherTitleSuffix(normalized) {
  const weather = normalized?.weatherInsight;
  const temperatureC = Number(weather?.temperatureC);
  if (!weather?.available || !Number.isFinite(temperatureC)) {
    return "";
  }

  return ` ${weatherEmojiForNotification(weather.conditionCode)} ${Math.round(temperatureC)}°`;
}

function positiveDurationMinutes(start, end, maximumMinutes = 120) {
  const startMs = start ? new Date(start).getTime() : NaN;
  const endMs = end ? new Date(end).getTime() : NaN;
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return null;
  }

  const minutes = Math.round((endMs - startMs) / 60_000);
  return minutes > 0 && minutes <= maximumMinutes ? minutes : null;
}

function arrivalLocalTimeForNotification(normalized) {
  const arrival =
    normalized?.arrivalTimes?.actual ||
    normalized?.arrivalTimes?.estimated ||
    normalized?.arrivalTimes?.scheduled;
  const timeZone = String(normalized?.arrivalTimezone || "").trim();
  if (!arrival || !timeZone) return null;

  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(arrival));
  } catch (_error) {
    return null;
  }
}

function arrivalScheduleVarianceText(normalized) {
  const scheduledMs = new Date(normalized?.arrivalTimes?.scheduled || "").getTime();
  const currentMs = new Date(
    normalized?.arrivalTimes?.actual ||
      normalized?.arrivalTimes?.estimated ||
      ""
  ).getTime();
  if (!Number.isFinite(scheduledMs) || !Number.isFinite(currentMs)) {
    return null;
  }

  const minutes = Math.round((currentMs - scheduledMs) / 60_000);
  if (Math.abs(minutes) < 2) return "on time";
  return `${Math.abs(minutes)}m ${minutes < 0 ? "early" : "late"}`;
}

function ordinalNumber(value) {
  const number = Math.max(1, Math.round(Number(value) || 0));
  const mod100 = number % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${number}th`;
  if (number % 10 === 1) return `${number}st`;
  if (number % 10 === 2) return `${number}nd`;
  if (number % 10 === 3) return `${number}rd`;
  return `${number}th`;
}

function arrivalWelcomePayload(normalized, flightId, context = {}) {
  const airportCode = normalizeAirportCode(normalized?.arrivalAirportIata) || "your destination";
  const airport = context.airport || airportForNotification(airportCode);
  const city = airport?.city || normalized?.arrivalCity || airportCode;
  const title = `✈️ Welcome to ${city}!${arrivalWeatherTitleSuffix(normalized)}`;
  const terminal = String(normalized?.arrivalTerminal || "").trim();
  const gate = String(normalized?.arrivalGate || "").trim();
  const locationParts = [
    airportCode,
    terminal ? `Terminal ${terminal}` : null,
    gate ? `Gate ${gate}` : null,
  ].filter(Boolean);
  const localTime = arrivalLocalTimeForNotification(normalized);
  const variance = arrivalScheduleVarianceText(normalized);
  const taxiMinutes = positiveDurationMinutes(
    normalized?.landingTimes?.actual,
    normalized?.arrivalTimes?.actual || normalized?.arrivalTimes?.estimated
  );

  const sentences = [];
  if (taxiMinutes) {
    sentences.push(`Taxiing for ${taxiMinutes}m.`);
  }

  let arrivalSentence = locationParts.length > 0
    ? `Arriving at ${locationParts.join(" • ")}`
    : "Arriving at the gate";
  if (localTime) {
    arrivalSentence += ` at ${localTime} local time`;
  }
  if (variance) {
    arrivalSentence += ` (${variance})`;
  }
  sentences.push(`${arrivalSentence}.`);

  if (Number.isFinite(Number(context.visitOrdinal)) && Number(context.visitOrdinal) > 0) {
    sentences.push(`This is your ${ordinalNumber(context.visitOrdinal)} time here.`);
  }

  return {
    aps: {
      alert: {
        title,
        body: sentences.join(" "),
      },
      sound: RUNWY_NOTIFICATION_SOUND,
      "thread-id": `runwy.flight.${flightId}`,
      "interruption-level": "active",
    },
    flight_instance_id: flightId,
    deep_link: `runwy://flights/${flightId}`,
    runwy: {
      type: "flight_arrived",
      flightId,
      status: normalized.status || null,
      route: `${normalized?.departureAirportIata || "---"} → ${normalized?.arrivalAirportIata || "---"}`,
      destinationIata: normalizeAirportCode(normalized?.arrivalAirportIata),
      visitOrdinal: Number(context.visitOrdinal) || null,
    },
  };
}

function trackedArrivalPayload(normalized, flightId, context = {}) {
  const code = readableFlightCode(normalized);
  const route = routeCitiesForNotification(normalized);
  const localTime = arrivalLocalTimeForNotification(normalized);
  const travelerName = firstNameForNotification(context.travelerName);
  const subject = context.isOwner === false && travelerName
    ? `${travelerName}'s flight ${code}`
    : `Flight ${code}`;

  let body = `${subject}${route ? `, ${route},` : ""} that you were tracking has landed`;
  if (localTime) {
    body += ` at ${localTime} local time`;
  }
  body += ".";

  return {
    aps: {
      alert: {
        title: "✈️ Tracked Flight Landed",
        body,
      },
      sound: RUNWY_NOTIFICATION_SOUND,
      "thread-id": `runwy.flight.${flightId}`,
      "interruption-level": "active",
    },
    flight_instance_id: flightId,
    deep_link: `runwy://flights/${flightId}`,
    runwy: {
      type: "flight_arrived",
      flightId,
      status: normalized.status || null,
      route: `${normalized?.departureAirportIata || "---"} → ${normalized?.arrivalAirportIata || "---"}`,
      destinationIata: normalizeAirportCode(normalized?.arrivalAirportIata),
      trackingOnly: true,
    },
  };
}

function notificationPayloadFor(normalized, flightId, context = {}) {
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
        sound: RUNWY_NOTIFICATION_SOUND,
      },
      runwy: {
        type: "flight_cancelled",
        flightId,
        status: normalized.status || null,
        route,
      },
    };
  }

  if (alerts.arrivedNow) {
    return context.isTraveler === false
      ? trackedArrivalPayload(normalized, flightId, context)
      : arrivalWelcomePayload(normalized, flightId, context);
  }

  if (alerts.departedNow) {
    const subject = flightSubjectForNotification(normalized, context);
    const routeDescription = routeCitiesForNotification(normalized);
    return {
      aps: {
        alert: {
          title: "✈️ Flight Took Off",
          body: `${subject}${routeDescription ? `, ${routeDescription},` : ""} is now in the air.`,
        },
        sound: RUNWY_NOTIFICATION_SOUND,
      },
      runwy: {
        type: "flight_departed",
        flightId,
        status: normalized.status || null,
        route,
      },
    };
  }

  if (alerts.takeoffNow) {
    const subject = flightSubjectForNotification(normalized, context);
    const routeDescription = routeCitiesForNotification(normalized);
    return {
      aps: {
        alert: {
          title: "✈️ Taking Off",
          body: `${subject}${routeDescription ? `, ${routeDescription},` : ""} is about to take off.`,
        },
        sound: RUNWY_NOTIFICATION_SOUND,
      },
      runwy: {
        type: "flight_takeoff_roll",
        flightId,
        status: normalized.status || null,
        route,
      },
    };
  }

  if (alerts.taxiingNow) {
    return {
      aps: {
        alert: {
          title: "Taxiing",
          body: `${code} (${route}) is taxiing.`,
        },
        sound: RUNWY_NOTIFICATION_SOUND,
      },
      runwy: {
        type: "flight_taxiing",
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
        sound: RUNWY_NOTIFICATION_SOUND,
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
        sound: RUNWY_NOTIFICATION_SOUND,
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

  if (alerts.baggageBeltAssignedNow) {
    const belt = `${normalized?.baggageClaim || normalized?.baggageBelt || ""}`.trim();
    if (belt) {
      const previousNotifiedBelt = `${context.previousNotifiedBaggageBelt || ""}`.trim();
      const isReassignment = Boolean(previousNotifiedBelt) && previousNotifiedBelt !== belt;
      const flightCode = readableFlightCode(normalized);
      const routeDescription = routeCitiesForNotification(normalized);
      const travelerName = firstNameForNotification(context.travelerName);
      const luggageOwner = context.isOwner === false && travelerName
        ? `${travelerName}'s luggage`
        : "Your luggage";
      const flightDescription = [
        flightCode !== "Flight" ? `flight ${flightCode}` : null,
        routeDescription,
      ].filter(Boolean).join(", ");
      return {
        aps: {
          alert: {
            title: isReassignment ? "🧳 Baggage Claim Changed" : "🧳 Baggage Claim Assigned",
            body: isReassignment
              ? `${luggageOwner}${flightDescription ? ` for ${flightDescription}` : ""} changed from belt ${previousNotifiedBelt} to belt ${belt}.`
              : `${luggageOwner}${flightDescription ? ` for ${flightDescription}` : ""} will be on belt ${belt}.`,
          },
          sound: RUNWY_NOTIFICATION_SOUND,
          "thread-id": `runwy.flight.${flightId}`,
          "interruption-level": "active",
        },
        flight_instance_id: flightId,
        deep_link: `runwy://flights/${flightId}`,
        runwy: {
          type: "flight_baggage_claim",
          flightId,
          status: normalized.status || null,
          baggageBelt: belt,
          previousBaggageBelt: isReassignment ? previousNotifiedBelt : null,
          route,
        },
      };
    }
  }

  if (alerts.inboundArrivedNow) {
    const departureAirport = normalized?.departureAirportIata || "the departure airport";
    const inboundFlightCode = String(normalized?.inboundFlight?.flightNumber || "").trim();
    const inboundText = inboundFlightCode
      ? `Inbound ${inboundFlightCode} for ${code}`
      : `Your inbound aircraft for ${code}`;
    const departureCountdown = departureCountdownTextForNotification(normalized);

    return {
      aps: {
        alert: {
          title: "Inbound Aircraft Landed",
          body: `${inboundText} has landed at ${departureAirport}.${departureCountdown ? ` ${departureCountdown}` : ""}`,
        },
        sound: RUNWY_NOTIFICATION_SOUND,
      },
      runwy: {
        type: "flight_inbound_arrived",
        flightId,
        status: normalized.status || null,
        route,
        departureAirportIata: normalized?.departureAirportIata || null,
        inboundFlightNumber: inboundFlightCode || null,
      },
    };
  }

  return null;
}

function notificationEventFor(normalized, flightId, context = {}) {
  const payload = notificationPayloadFor(normalized, flightId, context);
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

function notificationEventsFor(normalized, flightId, context = {}) {
  const alerts = normalized?.alerts || {};
  const activeFlags = [
    "cancelledNow",
    "arrivedNow",
    "departedNow",
    "takeoffNow",
    "taxiingNow",
    "delayedNow",
    "gateChangedNow",
    "inboundArrivedNow",
  ].filter((flag) => alerts[flag] === true);

  if (shouldOfferReliableBaggageNotification(normalized)) {
    activeFlags.push("baggageBeltAssignedNow");
  }

  return activeFlags
    .filter((flag) => !(alerts.arrivedNow && ["taxiingNow", "gateChangedNow"].includes(flag)))
    .map((flag) =>
      notificationEventFor(
        {
          ...normalized,
          alerts: { [flag]: true },
        },
        flightId,
        context
      )
    )
    .filter(Boolean);
}

function baggageBeltForNotification(normalized) {
  return `${normalized?.baggageClaim || normalized?.baggageBelt || ""}`.trim();
}

function shouldOfferReliableBaggageNotification(normalized) {
  if (!baggageBeltForNotification(normalized)) return false;

  const status = String(normalized?.status || "").trim().toLowerCase();
  return ["taxi_in", "landed", "arrived", "arrived_at_gate"].includes(status);
}

function ownerNotificationPreferenceConditionForEventType(eventType) {
  switch (eventType) {
    case "flight_delayed":
      return "coalesce((uf.alert_settings_json ->> 'delayUpdates')::boolean, true) = true";
    case "flight_gate_change":
      return "coalesce((uf.alert_settings_json ->> 'gateChange')::boolean, true) = true";
    case "flight_departed":
    case "flight_arrived":
    case "flight_takeoff_roll":
    case "flight_taxiing":
    case "flight_inbound_arrived":
    case "flight_inbound_departed":
    case "flight_inbound_cancelled":
    case "flight_inbound_diverted":
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
    case "flight_takeoff_roll":
    case "flight_taxiing":
    case "flight_inbound_arrived":
    case "flight_inbound_departed":
    case "flight_inbound_cancelled":
    case "flight_inbound_diverted":
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
      select
        ts.id as tracking_session_id,
        ts.owner_user_id,
        p.display_name as owner_display_name
      from public.tracking_sessions ts
      left join public.profiles p
        on p.user_id = ts.owner_user_id
      where ts.id = $1::uuid
    ),
    owner_recipient as (
      select
        base.owner_user_id as user_id,
        null::uuid as friend_relationship_id,
        base.owner_display_name,
        coalesce(uf.source_type, 'tracked') <> 'tracked' as is_traveler
      from base
      join public.user_flights uf
        on uf.user_id = base.owner_user_id
       and uf.tracking_session_id = base.tracking_session_id
       and uf.deleted_at is null
       and coalesce(uf.lifecycle_state, '') <> 'deleted'
      where coalesce(uf.notifications_enabled, true) = true
        and ${ownerCondition}
    ),
    circle_recipients as (
      select
        fp.viewer_user_id as user_id,
        fp.relationship_id as friend_relationship_id,
        base.owner_display_name,
        false as is_traveler
      from base
      join public.friend_permissions fp
        on fp.owner_user_id = base.owner_user_id
      join public.friend_relationships fr
        on fr.id = fp.relationship_id
      where fr.relationship_status = 'active'
        and exists (
          select 1
          from public.user_flights uf
          where uf.user_id = base.owner_user_id
            and uf.tracking_session_id = base.tracking_session_id
            and uf.deleted_at is null
            and coalesce(uf.lifecycle_state, '') <> 'deleted'
        )
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
      recipients.owner_display_name,
      recipients.is_traveler,
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
    ownerDisplayName: row.owner_display_name || null,
    isTraveler: row.is_traveler === true,
    apnsToken: row.apns_token || null,
  }));
}

async function hasActiveNotificationSubscription(flightId) {
  if (!usesDatabase()) return true;

  const result = await pool.query(
    `
    select exists (
      select 1
      from public.user_flights
      where tracking_session_id = $1::uuid
        and deleted_at is null
        and coalesce(lifecycle_state, '') <> 'deleted'
    ) as active
    `,
    [flightId]
  );
  return result.rows[0]?.active === true;
}

async function sendApnsNotification(apnsToken, payload, environment = null) {
  if (!isApnsConfigured()) {
    console.warn("Skipping APNs delivery because APNs is not fully configured", apnsConfigStatus());
    return { skipped: true };
  }

  const response = await sendApnsHttp2Request(apnsToken, payload, environment);

  if (response.status === 200) return { ok: true, apnsId: response.apnsId || null };

  const reason = response.reason || `HTTP_${response.status}`;

  if (["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"].includes(reason)) {
    await disablePushToken(apnsToken);
  }

  console.warn("APNs delivery failed", {
    apnsHost: apnsHost(environment),
    bundleId: APNS_BUNDLE_ID,
    sandbox: apnsHost(environment).includes("sandbox"),
    status: response.status,
    reason,
  });

  return { ok: false, status: response.status, reason };
}

function testPushNotificationPayload() {
  return {
    aps: {
      alert: {
        title: "Runwy Test Notification",
        body: "Closed-app notifications are working.",
      },
      sound: RUNWY_NOTIFICATION_SOUND,
    },
    runwy: {
      type: "push_test",
    },
  };
}

async function deliverTestPushJob(job) {
  const userId = String(job?.data?.userId || "").trim();
  const deviceId = String(job?.data?.deviceId || "").trim();
  if (!userId || !deviceId || !usesDatabase()) {
    return { sent: 0, failed: 0, skipped: true };
  }

  const result = await pool.query(
    `
    select
      pd.apns_token,
      coalesce(
        dt.environment,
        case when $3::boolean then 'sandbox' else 'production' end
      ) as environment
    from public.push_devices pd
    left join public.device_tokens dt
      on dt.user_id = pd.user_id
     and dt.device_token = pd.apns_token
     and dt.is_active = true
    where pd.user_id = $1::uuid
      and pd.device_id = $2
      and pd.push_enabled = true
    `,
    [userId, deviceId, APNS_USE_SANDBOX]
  );

  const deliveries = await Promise.all(
    result.rows.map(async (row) => {
      const delivery = await sendApnsNotification(
        row.apns_token,
        testPushNotificationPayload(),
        row.environment
      );

      if (
        delivery?.ok !== true &&
        ["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"].includes(delivery?.reason)
      ) {
        await sharedFlightRepository.disableDeviceToken(row.apns_token);
      }

      return {
        ok: delivery?.ok === true,
        status: delivery?.status || null,
        reason: delivery?.reason || null,
        environment: row.environment,
      };
    })
  );

  const summary = {
    sent: deliveries.filter((delivery) => delivery.ok).length,
    failed: deliveries.filter((delivery) => !delivery.ok).length,
    results: deliveries,
  };
  console.log("Runwy test push delivery completed", { userId, deviceId, ...summary });
  return summary;
}

sharedFlightService.queue.process("testPushJob", deliverTestPushJob);

function sendApnsHttp2Request(apnsToken, payload, environment = null, options = {}) {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`https://${apnsHost(environment)}`);
    let settled = false;

    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      client.close();
      fn(value);
    };

    client.on("error", (error) => {
      finish(reject, error);
    });
    client.setTimeout(10_000, () => {
      finish(reject, new Error("APNs request timed out"));
    });

    const request = client.request({
      ":method": "POST",
      ":path": `/3/device/${apnsToken}`,
      authorization: `bearer ${apnsAuthToken()}`,
      "apns-topic": options.topic || APNS_BUNDLE_ID,
      "apns-push-type": options.pushType || "alert",
      "apns-priority": String(options.priority || 10),
      "content-type": "application/json",
    });

    let status = 0;
    let apnsId = null;
    let body = "";

    request.setEncoding("utf8");
    request.on("response", (headers) => {
      status = Number(headers[":status"] || 0);
      apnsId = headers["apns-id"] || null;
    });
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      let reason = null;
      if (body) {
        try {
          reason = JSON.parse(body)?.reason || null;
        } catch (_error) {
          reason = body.slice(0, 160);
        }
      }
      finish(resolve, { status, apnsId, reason });
    });
    request.on("error", (error) => {
      finish(reject, error);
    });
    request.setTimeout(10_000, () => {
      finish(reject, new Error("APNs request timed out"));
    });

    request.end(JSON.stringify(payload));
  });
}

function liveActivityDate(value) {
  const unixSeconds = new Date(value || 0).getTime() / 1000;
  // ActivityKit uses Codable's default Date representation for content-state.
  return Number.isFinite(unixSeconds) ? unixSeconds - 978307200 : null;
}

function liveActivityPhase(flight) {
  const canonical = deriveFlightLifecyclePhase(flight).phase;
  const mapping = {
    scheduled: "scheduled",
    delayed: "delayed",
    boarding: "boarding",
    taxiing: "taxiOut",
    taxi_out: "taxiOut",
    takeoff_roll: "taxiOut",
    departed: "departed",
    airborne: "cruise",
    enroute: "cruise",
    approaching: "descent",
    landed: "landed",
    taxi_in: "taxiIn",
    arrived: "arrivedAtGate",
    arrived_at_gate: "arrivedAtGate",
    cancelled: "cancelled",
    diverted: "diverted",
  };
  return mapping[String(canonical || flight.status || "scheduled").toLowerCase()] || "scheduled";
}

function liveActivityContentState(flight) {
  const normalized = flight.normalized_data || {};
  const phase = liveActivityPhase(flight);
  const departureScheduled = flight.scheduled_departure_at;
  const departureEstimated = flight.actual_departure_at || flight.estimated_departure_at || departureScheduled;
  const arrivalScheduled = flight.scheduled_arrival_at;
  const arrivalEstimated = flight.actual_arrival_at || flight.estimated_arrival_at || arrivalScheduled;
  const delayMinutes = (actualOrEstimate, scheduled) => {
    const difference = new Date(actualOrEstimate || 0).getTime() - new Date(scheduled || 0).getTime();
    return Number.isFinite(difference) ? Math.round(difference / 60_000) : null;
  };
  const departureDelay = delayMinutes(departureEstimated, departureScheduled);
  const arrivalDelay = delayMinutes(arrivalEstimated, arrivalScheduled);
  const semantic = (minutes) => Number(minutes) > 0 ? "negative" : Number(minutes) < 0 ? "positive" : "neutral";
  const progressByPhase = {
    scheduled: 0.02, delayed: 0.02, boarding: 0.12, taxiOut: 0.20,
    departed: 0.20, climb: 0.30, cruise: 0.50, descent: 0.75,
    landed: 0.80, taxiIn: 0.90, arrivedAtGate: 1, completed: 1,
    cancelled: 0.02, diverted: 0.50,
  };
  const delayText = (minutes) => Number(minutes) > 0 ? `${minutes}m late` : Number(minutes) < 0 ? `${Math.abs(minutes)}m early` : "On time";
  const baggage = normalized.baggageBelt || flight.baggage_belt || null;

  return {
    departureScheduled: liveActivityDate(departureScheduled),
    departureEstimated: liveActivityDate(departureEstimated),
    arrivalScheduled: liveActivityDate(arrivalScheduled),
    arrivalEstimated: liveActivityDate(arrivalEstimated),
    gateArrivalEstimated: ["arrivedAtGate", "cancelled", "diverted"].includes(phase) ? null : liveActivityDate(arrivalEstimated),
    departureTerminal: normalized.departureTerminal || flight.terminal || null,
    departureGate: normalized.departureGate || flight.gate || null,
    arrivalTerminal: normalized.arrivalTerminal || null,
    arrivalGate: normalized.arrivalGate || null,
    baggageClaim: baggage,
    departureDelayMinutes: departureDelay,
    arrivalDelayMinutes: arrivalDelay,
    lastUpdated: liveActivityDate(flight.last_fetched_at || flight.updated_at || new Date()),
    progress: progressByPhase[phase] ?? 0.02,
    phase,
    leftStatusText: delayText(departureDelay),
    rightStatusText: delayText(arrivalDelay),
    bannerTitle: phase === "arrivedAtGate" ? "Arrived" : phase === "landed" ? "Landed" : null,
    bannerSubtitle: baggage ? `Baggage belt ${baggage}` : null,
    bannerStyle: ["cancelled", "diverted"].includes(phase) ? "critical" : phase === "arrivedAtGate" ? "success" : null,
    showBottomBanner: ["arrivedAtGate", "cancelled", "diverted"].includes(phase),
    statusAccentLeft: semantic(departureDelay),
    statusAccentRight: semantic(arrivalDelay),
    baggageAvailable: Boolean(baggage),
    boardingCodeAvailable: false,
  };
}

async function sendLiveActivityStateForFlight(flight) {
  if (!usesDatabase() || !flight?.id || !isApnsConfigured()) return { sent: 0, skipped: true };
  const tokenResult = await pool.query(
    `select id, push_token, environment
     from public.live_activity_tokens
     where flight_instance_id = $1::uuid and is_active = true`,
    [flight.id]
  );
  const phase = liveActivityPhase(flight);
  const shouldEnd = ["arrivedAtGate", "cancelled", "diverted"].includes(phase);
  const payload = {
    aps: {
      timestamp: Math.floor(Date.now() / 1000),
      event: shouldEnd ? "end" : "update",
      "content-state": liveActivityContentState(flight),
      ...(shouldEnd ? { "dismissal-date": Math.floor(Date.now() / 1000) + 45 * 60 } : {}),
    },
  };
  let sent = 0;
  for (const token of tokenResult.rows) {
    const response = await sendApnsHttp2Request(token.push_token, payload, token.environment, {
      topic: `${APNS_BUNDLE_ID}.push-type.liveactivity`,
      pushType: "liveactivity",
      priority: 10,
    });
    const ok = response.status === 200;
    if (ok) sent += 1;
    await pool.query(
      `update public.live_activity_tokens
       set is_active = case when $2 then is_active else false end,
           last_sent_at = case when $2 then now() else last_sent_at end,
           last_error = $3,
           updated_at = now()
       where id = $1`,
      [token.id, ok, ok ? null : (response.reason || `HTTP_${response.status}`)]
    );
  }
  return { sent, attempted: tokenResult.rowCount, phase };
}

function notificationDedupeKey(flightId, event) {
  if (event.type === "flight_arrived") {
    return `arrival-welcome:${flightId}`;
  }
  if (event.type === "flight_baggage_claim") {
    const belt = String(event.payload?.runwy?.baggageBelt || "").trim().toLowerCase();
    return `baggage:${flightId}:${belt || "assigned"}`;
  }

  const eventVersion =
    event.payload?.runwy?.gate ||
    event.payload?.runwy?.delayMinutes ||
    event.payload?.runwy?.status ||
    event.body;
  return `${event.type}:${flightId}:${String(eventVersion || "event").trim().toLowerCase()}`;
}

async function arrivalVisitOrdinalForUser(userId, flightId, destinationIata) {
  if (!usesDatabase() || !userId || !destinationIata) return null;

  const result = await pool.query(
    `
    select count(distinct uf.id)::int as visits
    from public.user_flights uf
    where uf.user_id = $1::uuid
      and upper(coalesce(uf.destination_iata, '')) = upper($2)
      and uf.source_type = 'travelled_archive'
      and uf.deleted_at is null
      and uf.tracking_session_id is distinct from $3::uuid
      and (
        uf.lifecycle_state in ('landed', 'archived')
        or uf.actual_arrival is not null
      )
    `,
    [userId, destinationIata, flightId]
  );

  return Number(result.rows[0]?.visits || 0) + 1;
}

function groupedNotificationRecipients(recipients) {
  const groups = new Map();
  for (const recipient of recipients) {
    const key = `${recipient.userId}|${recipient.friendRelationshipId || ""}`;
    const current = groups.get(key) || {
      userId: recipient.userId,
      friendRelationshipId: recipient.friendRelationshipId || null,
      ownerDisplayName: recipient.ownerDisplayName || null,
      isTraveler: recipient.isTraveler === true,
      tokens: [],
    };
    if (recipient.apnsToken) {
      current.tokens.push(recipient.apnsToken);
    }
    groups.set(key, current);
  }

  return Array.from(groups.values()).map((group) => ({
    ...group,
    tokens: Array.from(new Set(group.tokens)),
  }));
}

async function createNotificationRecord({
  recipient,
  flightId,
  event,
  dedupeKey,
  deliveryStatus,
}) {
  if (!usesDatabase()) return { id: null, created: true };

  const result = await pool.query(
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
      dedupe_key,
      scheduled_for
    )
    values (
      $1::uuid,
      $2::uuid,
      $3::uuid,
      $4,
      'push',
      $5,
      $6,
      $7,
      $8::jsonb,
      $9,
      now()
    )
    on conflict (user_id, dedupe_key)
      where dedupe_key is not null
      do nothing
    returning id::text
    `,
    [
      recipient.userId,
      flightId,
      recipient.friendRelationshipId,
      event.type,
      deliveryStatus,
      event.title,
      event.body,
      JSON.stringify(event.payload),
      dedupeKey,
    ]
  );

  return {
    id: result.rows[0]?.id || null,
    created: result.rowCount > 0,
  };
}

async function updateNotificationRecordDelivery(id, deliveryStatus) {
  if (!usesDatabase() || !id) return;
  await pool.query(
    `
    update public.notifications
    set
      delivery_status = $2,
      sent_at = case when $2 = 'sent' then coalesce(sent_at, now()) else sent_at end,
      updated_at = now()
    where id = $1::uuid
    `,
    [id, deliveryStatus]
  );
}

async function lastNotifiedBaggageBeltForRecipient(userId, flightId) {
  if (!usesDatabase() || !userId || !flightId) return null;

  const result = await pool.query(
    `
    select payload_json #>> '{runwy,baggageBelt}' as baggage_belt
    from public.notifications
    where user_id = $1::uuid
      and tracking_session_id = $2::uuid
      and notification_type = 'flight_baggage_claim'
      and delivery_status in ('queued', 'sent')
    order by created_at desc
    limit 1
    `,
    [userId, flightId]
  );

  return String(result.rows[0]?.baggage_belt || "").trim() || null;
}

async function enrichNormalizedForNotification(flightId, normalized) {
  if (normalized?.weatherInsight?.available || !sharedFlightService) {
    return normalized;
  }

  try {
    const tracked = await fetchTrackingRowByID(flightId);
    const sharedFlightInstanceId = tracked?.metadata?.sharedFlightInstanceId;
    if (!sharedFlightInstanceId) return normalized;

    const sharedFlight = await sharedFlightService.flightWithWeatherInsight(
      sharedFlightInstanceId,
      { userId: tracked.ownerUserId, cacheStatus: "arrival_notification" }
    );
    if (!sharedFlight?.weatherInsight) return normalized;

    return {
      ...normalized,
      weatherInsight: sharedFlight.weatherInsight,
    };
  } catch (error) {
    console.warn("Arrival notification weather enrichment failed", {
      flightId,
      error: error?.message || String(error),
    });
    return normalized;
  }
}

async function dispatchFlightStatusNotifications(flightId, normalized) {
  const notificationNormalized = await enrichNormalizedForNotification(flightId, normalized);
  const eventTypes = notificationEventsFor(notificationNormalized, flightId).map((event) => event.type);
  if (!eventTypes.length) return;

  const recipientsByType = new Map();
  for (const eventType of eventTypes) {
    recipientsByType.set(
      eventType,
      groupedNotificationRecipients(
        await listNotificationRecipientsForFlight(flightId, eventType)
      )
    );
  }

  for (const eventType of eventTypes) {
    const recipients = recipientsByType.get(eventType) || [];
    if (!recipients.length) {
      console.warn("Skipping flight notification because no recipients were resolved", {
        flightId,
        eventType,
      });
      continue;
    }

    for (const recipient of recipients) {
      // Recipient resolution and APNs delivery are separate operations. Recheck
      // the durable subscription so a swipe-delete that lands between them wins.
      if (!(await hasActiveNotificationSubscription(flightId))) break;
      const isOwner = !recipient.friendRelationshipId;
      const visitOrdinal =
        eventType === "flight_arrived" && isOwner && recipient.isTraveler
          ? await arrivalVisitOrdinalForUser(
              recipient.userId,
              flightId,
              normalizeAirportCode(notificationNormalized?.arrivalAirportIata)
            )
          : null;
      const previousNotifiedBaggageBelt =
        eventType === "flight_baggage_claim"
          ? await lastNotifiedBaggageBeltForRecipient(recipient.userId, flightId)
          : null;
      const event = notificationEventsFor(notificationNormalized, flightId, {
        visitOrdinal,
        isOwner,
        isTraveler: recipient.isTraveler,
        travelerName: recipient.ownerDisplayName,
        previousNotifiedBaggageBelt,
      })
        .find((candidate) => candidate.type === eventType);
      if (!event) continue;

      const hasDeliverableToken = recipient.tokens.length > 0 && isApnsConfigured();
      const record = await createNotificationRecord({
        recipient,
        flightId,
        event,
        dedupeKey: notificationDedupeKey(flightId, event),
        deliveryStatus: hasDeliverableToken ? "queued" : "pending",
      });
      if (!record.created) continue;

      if (!recipient.tokens.length) {
        console.warn("Skipping APNs delivery because no enabled device tokens were found", {
          flightId,
          eventType,
          userId: recipient.userId,
        });
        continue;
      }
      if (!isApnsConfigured()) continue;

      if (!(await hasActiveNotificationSubscription(flightId))) {
        await updateNotificationRecordDelivery(record.id, "failed");
        continue;
      }

      const results = await Promise.all(
        recipient.tokens.map((token) => sendApnsNotification(token, event.payload))
      );
      const deliveryStatus = results.some((result) => result?.ok) ? "sent" : "failed";
      await updateNotificationRecordDelivery(record.id, deliveryStatus);
    }
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

function trackedProviderRefreshOptions(options = {}) {
  return { forceRefresh: options.forceProviderRefresh === true };
}

async function refreshTrackedFlightRecord(trackedRecord, options = {}) {
  if (!PROVIDER_CALLS_ENABLED) {
    return trackedRecord;
  }

  const { includeLivePosition = false, forceProviderRefresh = false } = options;
  const providerRefreshOptions = trackedProviderRefreshOptions({ forceProviderRefresh });
  const provider = providerAdapter(trackedRecord.provider || FLIGHT_DATA_PROVIDER);
  const exactProviderRecord = trackedRecord.providerFlightId && provider.fetchFlightByProviderId
    ? await provider.fetchFlightByProviderId(trackedRecord.providerFlightId, providerRefreshOptions)
    : null;
  const records = exactProviderRecord
    ? [exactProviderRecord]
    : await provider.fetchFlights(trackedRecord.query, providerRefreshOptions);
  const selected = exactProviderRecord || bestMatch(records, trackedRecord.query, provider.normalizeRecord);

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
    normalized = await enrichNormalizedWithLivePosition(normalized, provider.name, selected, {
      ...providerRefreshOptions,
    });
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

  if (
    trackedRecord.metadata?.providerRefreshOwner !== "shared_flight_instance" &&
    (
      normalized.alerts?.cancelledNow ||
      normalized.alerts?.departedNow ||
      normalized.alerts?.arrivedNow ||
      normalized.alerts?.taxiingNow ||
      normalized.alerts?.takeoffNow ||
      normalized.alerts?.delayedNow ||
      normalized.alerts?.gateChangedNow ||
      normalized.alerts?.baggageBeltAssignedNow ||
      normalized.alerts?.inboundArrivedNow ||
      shouldOfferReliableBaggageNotification(normalized)
    )
  ) {
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
  const rawEventType = String(
    event?.event_code ||
      event?.event ||
      event?.alert_type ||
      event?.alertType ||
      event?.type ||
      ""
  ).trim().toLowerCase();

  if (["in", "inblock", "onblock", "arrived_at_gate"].includes(rawEventType)) {
    return "arrived_at_gate";
  }
  if (["on", "landing", "landed", "arrival", "arrived"].includes(rawEventType)) {
    return "landed";
  }
  if (["off", "takeoff", "departure", "departed", "airborne"].includes(rawEventType)) {
    return "departed";
  }
  if (["out", "offblock", "taxi", "taxiing"].includes(rawEventType)) {
    return "taxiing";
  }
  if (rawEventType.includes("cancel")) return "cancelled";
  if (rawEventType.includes("divert")) return "diverted";

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

  if (trackedRecord.metadata?.providerRefreshOwner === "shared_flight_instance") {
    return false;
  }

  if (!normalizeAircraftType(trackedRecord.normalized?.aircraftType)) {
    return true;
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
  if (
    incomingStatus &&
    incomingStatus !== "scheduled" &&
    incomingStatus !== trackedStatus
  ) {
    return true;
  }
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
  if (["cancelled", "diverted"].includes(status)) {
    return false;
  }

  if (
    ["landed", "arrived", "arrived_at_gate", "taxi_in"].includes(status) &&
    trackedRecord.sessionStatus === "active"
  ) {
    return true;
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

  if (
    trackedRecord.metadata?.providerRefreshOwner !== "shared_flight_instance" &&
    (
      normalized.alerts?.cancelledNow ||
      normalized.alerts?.departedNow ||
      normalized.alerts?.arrivedNow ||
      normalized.alerts?.taxiingNow ||
      normalized.alerts?.takeoffNow ||
      normalized.alerts?.delayedNow ||
      normalized.alerts?.gateChangedNow ||
      normalized.alerts?.baggageBeltAssignedNow ||
      shouldOfferReliableBaggageNotification(normalized)
    )
  ) {
    await dispatchFlightStatusNotifications(trackedRecord.flightId, normalized);
  }

  return fetchTrackingRowByID(trackedRecord.flightId);
}

function sharedNormalizedFromFirehoseNormalized(normalized, message) {
  if (!normalized) return null;
  const flightNumber = normalizeFlightCode(normalized.flightNumber || firehoseMessageFlightNumber(message));
  const airlineCode = normalized.airlineCode || parseAirlineCode(flightNumber) || null;
  const numericFlightNumber = airlineCode && flightNumber.startsWith(airlineCode)
    ? flightNumber.slice(airlineCode.length)
    : flightNumber.replace(/^[A-Z]+/, "");
  return {
    providerFlightId: firehoseMessageProviderFlightId(message) || normalized.providerFlightId || null,
    airlineCode,
    flightNumber: numericFlightNumber || flightNumber,
    origin: normalized.departureAirportIata || null,
    destination: normalized.arrivalAirportIata || null,
    arrivalTimezone: normalized.arrivalTimezone || null,
    status: normalized.status || "unknown",
    statusDetail: normalized.statusDetail || null,
    scheduledDepartureAt: normalized.departureTimes?.scheduled || null,
    scheduledArrivalAt: normalized.arrivalTimes?.scheduled || null,
    estimatedDepartureAt: normalized.departureTimes?.estimated || normalized.takeoffTimes?.estimated || null,
    estimatedArrivalAt: normalized.arrivalTimes?.estimated || normalized.landingTimes?.estimated || null,
    actualDepartureAt: normalized.departureTimes?.actual || normalized.takeoffTimes?.actual || null,
    actualArrivalAt: normalized.arrivalTimes?.actual || normalized.landingTimes?.actual || null,
    gate: normalized.gate || null,
    terminal: normalized.terminal || null,
    departureTerminal: normalized.departureTerminal || normalized.terminal || null,
    departureGate: normalized.departureGate || normalized.gate || null,
    arrivalTerminal: normalized.arrivalTerminal || null,
    arrivalGate: normalized.arrivalGate || null,
    baggageBelt: normalized.baggageClaim || normalized.baggageBelt || null,
    position: {
      lat: normalized.livePosition?.latitude ?? null,
      lon: normalized.livePosition?.longitude ?? null,
      altitude: normalized.livePosition?.altitudeFeet ?? null,
      groundSpeed: normalized.livePosition?.groundSpeedKnots ?? null,
      heading: normalized.livePosition?.headingDegrees ?? null,
    },
    provider: "flightaware",
    liveDataSource: "streaming",
    streamingStatus: "active",
    dataConfidence: "high",
    rawProviderResponse: message,
  };
}

async function applyFirehoseMessageToSharedFlights(message) {
  if (!sharedFlightService?.repository?.listStreamUpdateTargets) return;
  const providerFlightId = firehoseMessageProviderFlightId(message) || null;
  const flightNumber = normalizeFlightCode(firehoseMessageFlightNumber(message));
  const timestampMs = firehoseMessageTimestampMs(message);
  const departureDate = Number.isFinite(timestampMs) ? new Date(timestampMs).toISOString().slice(0, 10) : null;
  const targets = await sharedFlightService.repository.listStreamUpdateTargets({
    providerFlightId,
    flightNumber,
    departureDate,
  });

  for (const target of targets) {
    const previousNormalized = trackedPayloadFromSharedFlight({
      flightInstanceId: target.id,
      flightKey: target.flight_key,
      providerFlightId: target.provider_flight_id,
      airlineCode: target.airline_code,
      flightNumber: target.flight_number,
      origin: target.origin_airport,
      destination: target.destination_airport,
      arrivalTimezone: target.normalized_data?.arrivalTimezone || null,
      status: target.status,
      scheduledDepartureAt: target.scheduled_departure_at,
      scheduledArrivalAt: target.scheduled_arrival_at,
      estimatedDepartureAt: target.estimated_departure_at,
      estimatedArrivalAt: target.estimated_arrival_at,
      actualDepartureAt: target.actual_departure_at,
      actualArrivalAt: target.actual_arrival_at,
      gate: target.gate,
      terminal: target.terminal,
      baggageBelt: target.baggage_belt,
      position: {
        lat: target.position_lat,
        lon: target.position_lon,
        altitude: target.altitude,
        groundSpeed: target.ground_speed,
        heading: target.heading,
      },
      provider: target.provider,
      lastUpdatedAt: target.last_fetched_at || target.updated_at,
    });
    const nextTrackedNormalized = normalizedFromFirehoseMessage(previousNormalized, message);
    const sharedNormalized = sharedNormalizedFromFirehoseNormalized(nextTrackedNormalized, message);
    if (sharedNormalized) {
      await sharedFlightService.applyStreamedFlightUpdate(target.id, sharedNormalized, {
        eventTime: Number.isFinite(timestampMs) ? new Date(timestampMs).toISOString() : new Date().toISOString(),
      });
    }
  }
}

async function processFirehoseMessage(message, trackedRowsById) {
  const trackedRows = Array.from(trackedRowsById.values());
  const matchedRows = trackedRows.filter((trackedRecord) =>
    firehoseMessageMatchesTrackedRecord(message, trackedRecord)
  );

  await applyFirehoseMessageToSharedFlights(message);

  for (const trackedRecord of matchedRows) {
    const updated = await applyFirehoseMessageToTrackedRecord(trackedRecord, message);
    if (updated) {
      trackedRowsById.set(updated.flightId, updated);
    }
  }
}

async function claimDueFinalTravelRouteRows(limit = POLLER_BATCH_SIZE) {
  if (!usesDatabase() || !FINAL_TRAVEL_ROUTE_CAPTURE_ENABLED || FLIGHT_DATA_PROVIDER !== "flightaware") {
    return [];
  }

  const result = await pool.query(
    `
    with due as (
      select id
      from public.user_flights
      where deleted_at is null
        and tracking_session_id is null
        and coalesce(source_type, '') <> 'tracked'
        and coalesce(lifecycle_state, '') in ('active', 'landed', 'archived')
        and lower(coalesce(provider_name, $5)) = 'flightaware'
        and nullif(trim(coalesce(provider_flight_id, '')), '') is not null
        and coalesce(estimated_arrival, scheduled_arrival, actual_arrival) is not null
        and coalesce(estimated_arrival, scheduled_arrival, actual_arrival)
          between now() - ($2::double precision * interval '1 millisecond')
              and now() + ($3::double precision * interval '1 millisecond')
        and (
          route_polyline is null
          or jsonb_typeof(route_polyline) <> 'array'
          or jsonb_array_length(route_polyline) < 3
        )
        and coalesce(final_route_capture_status, 'pending') in ('pending', 'failed', 'in_progress')
        and (
          final_route_capture_next_attempt_at is null
          or final_route_capture_next_attempt_at <= now()
        )
      order by coalesce(estimated_arrival, scheduled_arrival, actual_arrival) asc
      limit $1
      for update skip locked
    )
    update public.user_flights uf
    set
      final_route_capture_status = 'in_progress',
      final_route_capture_attempted_at = now(),
      final_route_capture_next_attempt_at = now() + ($4::double precision * interval '1 millisecond'),
      final_route_capture_error = null,
      updated_at = now()
    from due
    where uf.id = due.id
    returning uf.*
    `,
    [
      Math.max(1, Number(limit) || POLLER_BATCH_SIZE),
      FINAL_TRAVEL_ROUTE_CAPTURE_AFTER_ARRIVAL_MS,
      FINAL_TRAVEL_ROUTE_CAPTURE_BEFORE_ARRIVAL_MS,
      FINAL_TRAVEL_ROUTE_CAPTURE_RETRY_MS,
      FLIGHT_DATA_PROVIDER,
    ]
  );

  return result.rows;
}

async function completeFinalTravelRouteRows(rows, routePolyline, status = "captured") {
  if (!rows.length) return 0;
  const result = await pool.query(
    `
    update public.user_flights
    set
      route_polyline = case
        when $2::jsonb is null then route_polyline
        else $2::jsonb
      end,
      final_route_capture_status = $3,
      final_route_capture_completed_at = now(),
      final_route_capture_next_attempt_at = null,
      final_route_capture_error = null,
      updated_at = now()
    where id = any($1::uuid[])
    returning id
    `,
    [rows.map((row) => row.id), routePolyline ? JSON.stringify(routePolyline) : null, status]
  );
  return result.rowCount || 0;
}

async function failFinalTravelRouteRows(rows, error) {
  if (!rows.length) return 0;
  const result = await pool.query(
    `
    update public.user_flights
    set
      final_route_capture_status = 'failed',
      final_route_capture_error = left($2, 500),
      final_route_capture_next_attempt_at = now() + ($3::double precision * interval '1 millisecond'),
      updated_at = now()
    where id = any($1::uuid[])
    returning id
    `,
    [rows.map((row) => row.id), error?.message || String(error), FINAL_TRAVEL_ROUTE_CAPTURE_RETRY_MS]
  );
  return result.rowCount || 0;
}

function groupFinalRouteRowsByProviderFlightId(rows) {
  const groups = new Map();
  for (const row of rows) {
    const providerFlightId = String(row.provider_flight_id || "").trim();
    if (!providerFlightId) continue;
    if (!groups.has(providerFlightId)) {
      groups.set(providerFlightId, []);
    }
    groups.get(providerFlightId).push(row);
  }
  return groups;
}

function shouldRetryMissingFinalRoute(groupRows) {
  const latestArrivalMs = groupRows
    .map((row) => new Date(row.actual_arrival || row.estimated_arrival || row.scheduled_arrival || "").getTime())
    .filter(Number.isFinite)
    .sort((left, right) => right - left)[0];
  if (!Number.isFinite(latestArrivalMs)) return true;
  return Date.now() - latestArrivalMs < FINAL_TRAVEL_ROUTE_CAPTURE_AFTER_ARRIVAL_MS;
}

async function captureFinalTravelRoutes(limit = POLLER_BATCH_SIZE) {
  const rows = await claimDueFinalTravelRouteRows(limit);
  if (!rows.length) {
    return { claimed: 0, captured: 0, noTrack: 0, failed: 0, providerCalls: 0 };
  }

  const groups = groupFinalRouteRowsByProviderFlightId(rows);
  let captured = 0;
  let noTrack = 0;
  let failed = 0;
  let providerCalls = 0;

  for (const [providerFlightId, groupRows] of groups.entries()) {
    try {
      providerCalls += 1;
      const trackTrail = await fetchFlightAwareTrackTrailWithLiveFallback(providerFlightId);
      const representative = groupRows[0] || {};
      const routePolyline = routePolylineFromTrackTrail({
        originIata: representative.origin_iata,
        destinationIata: representative.destination_iata,
        trackTrail,
      });

      if (routePolyline.length >= 3) {
        captured += await completeFinalTravelRouteRows(groupRows, routePolyline, "captured");
      } else if (shouldRetryMissingFinalRoute(groupRows)) {
        failed += await failFinalTravelRouteRows(groupRows, new Error("No usable provider track yet"));
      } else {
        noTrack += await completeFinalTravelRouteRows(groupRows, null, "no_track");
      }
    } catch (error) {
      failed += await failFinalTravelRouteRows(groupRows, error);
    }
  }

  return { claimed: rows.length, captured, noTrack, failed, providerCalls };
}

const trackingPollerRuntime = createTrackingPollerRuntime({
  isPollerEnabled: ENABLE_TRACKING_POLLER,
  usesDatabase,
  ensureDatabaseSchema,
  listDueTrackingRows,
  refreshTrackedFlightRecord,
  markTrackingRowErrored,
  captureFinalTravelRoutes,
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

function backgroundTrackingMode() {
  if (isFirehoseRunning()) {
    return "firehose";
  }

  if (isPollerRunning()) {
    return "poller";
  }

  return "on_demand_only";
}

function detailedHealthSafeguards() {
  return {
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
  };
}

async function buildDetailedHealth() {
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

  return {
    ok: true,
    build: healthBuildInfo(),
    provider: FLIGHT_DATA_PROVIDER,
    providerCallsEnabled: PROVIDER_CALLS_ENABLED,
    nodeEnv: NODE_ENV,
    persistence: usesDatabase() ? "supabase-postgres" : "memory",
    apnsConfigured: isApnsConfigured(),
    apns: apnsConfigStatus(),
    pollerEnabled: isPollerRunning(),
    firehoseConfigured: isFirehoseConfigured(),
    firehoseEnabled: isFirehoseRunning(),
    backgroundTrackingMode: backgroundTrackingMode(),
    providerAuth,
    trackingSummary,
    safeguards: detailedHealthSafeguards(),
  };
}

app.get("/health", async (_req, res) => {
  res.json({
    ok: true,
    build: healthBuildInfo(),
    nodeEnv: NODE_ENV,
    persistence: usesDatabase() ? "supabase-postgres" : "memory",
  });
});

app.get("/v1/health/details", async (_req, res) => {
  res.json(await buildDetailedHealth());
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
    if (sharedFlightService?.upsertDeviceToken) {
      await sharedFlightService.upsertDeviceToken(userId, {
        deviceToken: validated.value.token,
        platform: validated.value.platform,
        environment: validated.value.environment || (APNS_USE_SANDBOX ? "sandbox" : "production"),
      });
    }

    return res.json({ ok: true });
  } catch (_error) {
    return res.status(500).json({ error: "Unable to store push token" });
  }
});

app.post("/v1/live-activities/token", async (req, res) => {
  const userId = String(req.auth?.userId || "").trim();
  const token = String(req.body?.token || "").trim().toLowerCase();
  const activityId = String(req.body?.activityId || "").trim().slice(0, 256);
  const localFlightId = String(req.body?.localFlightId || "").trim().slice(0, 128) || null;
  const trackingId = String(req.body?.trackingId || "").trim();
  const environment = String(req.body?.environment || "").trim().toLowerCase();

  if (!userId) return res.status(401).json({ error: "Sign in is required" });
  if (!usesDatabase()) return res.status(503).json({ error: "Live Activity persistence is not configured" });
  if (!/^[a-f0-9]{32,512}$/.test(token) || !activityId || !isUUID(trackingId)) {
    return res.status(400).json({ error: "Invalid Live Activity token payload" });
  }
  if (!["sandbox", "production"].includes(environment)) {
    return res.status(400).json({ error: "Invalid APNs environment" });
  }

  try {
    const link = await pool.query(
      `select uf.flight_instance_id
       from public.user_flights uf
       where uf.user_id = $1::uuid
         and uf.tracking_session_id = $2::uuid
         and uf.flight_instance_id is not null
         and uf.deleted_at is null
         and coalesce(uf.lifecycle_state, '') <> 'deleted'
       order by uf.updated_at desc
       limit 1`,
      [userId, trackingId]
    );
    const flightInstanceId = link.rows[0]?.flight_instance_id;
    if (!flightInstanceId) {
      return res.status(404).json({ error: "No active shared flight is linked to this activity" });
    }

    await pool.query(
      `insert into public.live_activity_tokens (
         user_id, flight_instance_id, tracking_session_id, activity_id,
         local_flight_id, push_token, environment, is_active
       ) values ($1,$2,$3,$4,$5,$6,$7,true)
       on conflict (user_id, activity_id) do update set
         flight_instance_id = excluded.flight_instance_id,
         tracking_session_id = excluded.tracking_session_id,
         local_flight_id = excluded.local_flight_id,
         push_token = excluded.push_token,
         environment = excluded.environment,
         is_active = true,
         last_error = null,
         updated_at = now()`,
      [userId, flightInstanceId, trackingId, activityId, localFlightId, token, environment]
    );
    const flight = await sharedFlightRepository.findFlightById(flightInstanceId);
    if (flight) await sendLiveActivityStateForFlight(flight);
    return res.json({ ok: true, flightInstanceId });
  } catch (error) {
    console.error("Unable to store Live Activity token", error?.message || String(error));
    return res.status(500).json({ error: "Unable to store Live Activity token" });
  }
});

app.post("/v1/devices/test-notification", testNotificationLimiter, async (req, res) => {
  const userId = String(req.auth?.userId || "").trim() || null;
  const deviceId = normalizedHeaderDeviceID(req);

  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }
  if (!deviceId) {
    return res.status(400).json({ error: "X-Device-Id header is required" });
  }
  if (!usesDatabase()) {
    return res.status(503).json({ error: "Push notification persistence is not configured" });
  }
  if (!isApnsConfigured()) {
    return res.status(503).json({
      error: "APNs is not configured",
      apns: apnsConfigStatus(),
    });
  }

  try {
    const tokenResult = await pool.query(
      `
      select 1
      from public.push_devices
      where user_id = $1::uuid
        and device_id = $2
        and push_enabled = true
      limit 1
      `,
      [userId, deviceId]
    );

    if (!tokenResult.rowCount) {
      return res.status(404).json({
        error: "No active push token is registered for this device. Reopen Runwy and try again.",
      });
    }

    const deliveryInSeconds = 15;
    const job = await sharedFlightService.queue.add(
      "testPushJob",
      { userId, deviceId },
      {
        delayMs: deliveryInSeconds * 1_000,
        dedupe: true,
        dedupeKey: `test-push:${userId}:${deviceId}`,
      }
    );

    return res.status(202).json({
      queued: true,
      deduped: job?.deduped === true,
      deliveryInSeconds,
    });
  } catch (error) {
    console.error("Unable to schedule Runwy test notification", {
      userId,
      deviceId,
      error: error?.message || String(error),
    });
    return res.status(500).json({ error: "Unable to schedule test notification" });
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
    if (usesDatabase()) {
      await pool.query(
        `
        update public.device_tokens
        set is_active = false, updated_at = now()
        where user_id = $1::uuid
          and device_token in (
            select apns_token
            from public.push_devices
            where device_id = $2
              and user_id = $1::uuid
          )
        `,
        [userId, deviceId]
      );
    }
    return res.json({ ok: true });
  } catch (_error) {
    return res.status(500).json({ error: "Unable to disable push token" });
  }
});

function sharedTrackInputFromQuery(query) {
  const flightCode = normalizeFlightCode(query.flightNumber);
  const airline = parseAirlineCode(flightCode);
  const number = airline ? flightCode.slice(airline.length) : flightCode.replace(/^[A-Z]+/, "");
  if (!airline || !number) return null;
  return {
    airline,
    number,
    date: query.date,
    origin: query.departureIata || undefined,
    destination: query.arrivalIata || undefined,
    // The shared flight instance is the sole provider-refresh and notification
    // owner for bridged sessions. The legacy tracking projection stays paused.
    notificationEnabled: true,
  };
}

function trackedPayloadFromSharedFlight(flight) {
  const lifecycle = flight.computedPhase
    ? {
        phase: flight.computedPhase,
        confidence: flight.phaseConfidence || "backend_computed",
        reason: flight.phaseReason || "shared_flight_response",
      }
    : deriveFlightLifecyclePhase(flight);
  const providerStatus = String(flight.providerStatus || flight.status || "scheduled").toLowerCase();
  const status = displayStatusForPhase(lifecycle.phase, providerStatus);
  const scheduledDeparture = flight.departureTimes?.scheduled || flight.scheduledDepartureAt || flight.estimatedDepartureAt || null;
  const estimatedDeparture = flight.departureTimes?.estimated || flight.estimatedDepartureAt || flight.scheduledDepartureAt || null;
  const actualDeparture = flight.departureTimes?.actual || flight.actualDepartureAt || null;
  const scheduledTakeoff = flight.takeoffTimes?.scheduled || scheduledDeparture;
  const estimatedTakeoff = flight.takeoffTimes?.estimated || estimatedDeparture;
  const actualTakeoff = flight.takeoffTimes?.actual || null;
  const scheduledArrival = flight.arrivalTimes?.scheduled || flight.scheduledArrivalAt || flight.estimatedArrivalAt || null;
  const estimatedArrival = flight.arrivalTimes?.estimated || flight.estimatedArrivalAt || flight.scheduledArrivalAt || null;
  const actualArrival = flight.arrivalTimes?.actual || flight.actualArrivalAt || null;
  const scheduledLanding = flight.landingTimes?.scheduled || scheduledArrival;
  const estimatedLanding = flight.landingTimes?.estimated || estimatedArrival;
  const actualLanding = flight.landingTimes?.actual || null;
  const livePosition =
    flight.position?.lat != null && flight.position?.lon != null
      ? {
          latitude: flight.position.lat,
          longitude: flight.position.lon,
          headingDegrees: flight.position.heading ?? null,
          groundSpeedKnots: flight.position.groundSpeed ?? null,
          altitudeFeet: flight.position.altitude ?? null,
          recordedAt: flight.lastUpdatedAt || new Date().toISOString(),
        }
      : null;

  return {
    airlineCode: flight.airlineCode || null,
    providerFlightId: flight.providerFlightId || null,
    flightNumber: flight.airlineCode && !String(flight.flightNumber || "").startsWith(flight.airlineCode)
      ? `${flight.airlineCode}${flight.flightNumber}`
      : String(flight.flightNumber || ""),
    departureAirportIata: flight.origin || null,
    arrivalAirportIata: flight.destination || null,
    arrivalTimezone: flight.arrivalTimezone || null,
    departureTimes: {
      scheduled: scheduledDeparture,
      estimated: estimatedDeparture,
      actual: actualDeparture,
    },
    takeoffTimes: {
      scheduled: scheduledTakeoff,
      estimated: estimatedTakeoff,
      actual: actualTakeoff,
    },
    landingTimes: {
      scheduled: scheduledLanding,
      estimated: estimatedLanding,
      actual: actualLanding,
    },
    arrivalTimes: {
      scheduled: scheduledArrival,
      estimated: estimatedArrival,
      actual: actualArrival,
    },
    status,
    providerStatus,
    computedPhase: lifecycle.phase,
    phaseConfidence: lifecycle.confidence,
    phaseReason: lifecycle.reason,
    statusDetail: flight.statusDetail || null,
    terminal: flight.terminal || null,
    gate: flight.gate || null,
    departureTerminal: flight.departureTerminal || flight.terminal || null,
    departureGate: flight.departureGate || flight.gate || null,
    arrivalTerminal: flight.arrivalTerminal || null,
    arrivalGate: flight.arrivalGate || null,
    baggageBelt: flight.baggageBelt || null,
    baggageClaim: flight.baggageBelt || null,
    weatherInsight: flight.weatherInsight || null,
    delayMinutes: calculateDelayMinutes({ scheduled: scheduledDeparture, estimated: estimatedDeparture, actual: actualDeparture }),
    inboundFlight: null,
    recentHistory: [],
    alerts: null,
    progressPercent: null,
    livePosition,
    trackPoints: livePosition ? [livePosition] : [],
    provider: flight.provider || null,
    freshness: flight.freshness || null,
    source: flight.source || null,
    isRefreshing: flight.isRefreshing === true,
    dataConfidence: flight.dataConfidence || null,
    lastUpdated: flight.lastUpdatedAt || new Date().toISOString(),
  };
}

async function createTrackingSessionFromSharedFlight({ sharedFlight, query, userId, providerName }) {
  const normalized = trackedPayloadFromSharedFlight(sharedFlight);
  const tracked = await createOrReuseTrackingSession({
    query,
    normalized,
    rawProviderPayload: {
      source: "shared_flight_instance",
      flightInstanceId: sharedFlight.flightInstanceId,
      flightKey: sharedFlight.flightKey,
    },
    userId,
    provider: providerName || sharedFlight.provider || FLIGHT_DATA_PROVIDER,
    createdSource: "shared_manual_track",
  });
  if (tracked?.flightId && usesDatabase()) {
    await pool.query(
      `
      update public.tracking_sessions
      set
        metadata_json = coalesce(metadata_json, '{}'::jsonb) || jsonb_build_object(
          'sharedFlightInstanceId', $2::text,
          'sharedFlightKey', $3::text,
          'providerRefreshOwner', 'shared_flight_instance'
        ),
        session_status = 'paused',
        next_poll_after = null,
        polling_stopped_reason = 'shared_flight_instance_owns_provider_refresh',
        updated_at = now()
      where id = $1::uuid
      `,
      [tracked.flightId, sharedFlight.flightInstanceId, sharedFlight.flightKey]
    );
    return fetchTrackingRowByID(tracked.flightId);
  }
  return tracked;
}

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
    if (SHARED_FLIGHT_TRACK_BRIDGE_ENABLED && sharedFlightService) {
      const sharedInput = sharedTrackInputFromQuery(query);
      if (sharedInput) {
        try {
          const shared = await sharedFlightService.saveUserFlight(userId, sharedInput);
          if (shared?.flight?.flightInstanceId && shared.flight.freshness !== "pending") {
            const tracked = await createTrackingSessionFromSharedFlight({
              sharedFlight: shared.flight,
              query,
              userId,
              providerName: shared.flight.provider,
            });
            if (tracked) {
              await sharedFlightService.ensureLiveSource(shared.flight.flightInstanceId, "manual_track_bridge");
              return res.json({
                flightId: tracked.flightId,
                normalized: tracked.normalized,
              });
            }
          }
        } catch (error) {
          console.warn("Shared flight track bridge failed; falling back to provider track", {
            error: error?.message || String(error),
            details: error?.details || null,
            flightNumber: query.flightNumber,
            date: query.date,
          });
        }
      }
    }

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

mountSharedFlightRoutes(app, sharedFlightService);

app.get("/v1/flights/:flightId", async (req, res) => {
  const flightId = req.params.flightId;
  const userId = String(req.auth?.userId || "").trim() || null;
  const forceDetailRefresh = String(req.query?.refresh || "").toLowerCase() === "detail";

  if (!userId) {
    return res.status(401).json({ error: "Sign in is required" });
  }

  try {
    const tracked = await fetchAccessibleTrackingRow(flightId, userId);
    if (!tracked) {
      const sharedRows = sharedFlightService ? await sharedFlightService.listUserFlights(userId) : [];
      const shared = sharedRows.find((item) => item.flight?.flightInstanceId === flightId);
      if (!shared?.flight) {
        return res.status(404).json({ error: "Unknown flightId" });
      }
      if (forceDetailRefresh) {
        await sharedFlightService.refreshFlightJob({
          data: {
            flight_key: shared.flight.flightKey,
            flight_instance_id: flightId,
            reason: "detail_open",
          },
        });
      }
      const weatherAwareFlight = await sharedFlightService.flightWithWeatherInsight(flightId, { userId, cacheStatus: "detail_view" }) || shared.flight;
      return res.json({
        flightId,
        normalized: trackedPayloadFromSharedFlight(weatherAwareFlight),
        lastUpdated: weatherAwareFlight.lastUpdatedAt || shared.flight.lastUpdatedAt || new Date().toISOString(),
      });
    }

    if (tracked.metadata?.providerRefreshOwner === "shared_flight_instance" && tracked.metadata?.sharedFlightInstanceId) {
      const sharedRows = sharedFlightService ? await sharedFlightService.listUserFlights(userId) : [];
      const shared = sharedRows.find((item) => item.flight?.flightInstanceId === tracked.metadata.sharedFlightInstanceId);
      if (shared?.flight) {
        if (forceDetailRefresh) {
          await sharedFlightService.refreshFlightJob({
            data: {
              flight_key: shared.flight.flightKey,
              flight_instance_id: tracked.metadata.sharedFlightInstanceId,
              reason: "detail_open",
            },
          });
        }
        const weatherAwareFlight = await sharedFlightService.flightWithWeatherInsight(tracked.metadata.sharedFlightInstanceId, { userId, cacheStatus: "detail_view" }) || shared.flight;
        return res.json({
          flightId,
          normalized: trackedPayloadFromSharedFlight(weatherAwareFlight),
          lastUpdated: weatherAwareFlight.lastUpdatedAt || shared.flight.lastUpdatedAt || new Date().toISOString(),
        });
      }
    }

    const shouldRefresh = forceDetailRefresh || isTrackedRecordRefreshDue(tracked);
    const current = shouldRefresh
      ? await refreshTrackedFlightRecord(tracked, { includeLivePosition: forceDetailRefresh })
      : tracked;

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
  if (!normalizeFlightCode(query?.flightNumber)) {
    return [];
  }

  const provider = providerAdapter();
  const records = await provider.fetchFlights(query);
  const eligibleRecords = records;
  const candidateLimit = 30;
  const topRecords = sortSearchRecords(eligibleRecords, query, provider.normalizeRecord).slice(0, candidateLimit);

  const normalized = await Promise.all(
    topRecords.map(async (record, index) => {
      let candidate = normalizeWithContext(record, eligibleRecords, query, provider.normalizeRecord, null);
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

  const {
    flightNumber,
    date,
    departureIata,
    arrivalIata,
    historical,
    preferSchedules,
    timezoneOffsetMinutes,
  } = validated.value;

  if (!PROVIDER_CALLS_ENABLED) {
    return res.json({ candidates: [], providerDisabled: true });
  }

  try {
    const query = {
      flightNumber,
      date,
      departureIata,
      arrivalIata,
      historical,
      preferSchedules,
      timezoneOffsetMinutes,
    };
    if (SHARED_FLIGHT_TRACK_BRIDGE_ENABLED && !historical && sharedFlightService) {
      const sharedInput = sharedTrackInputFromQuery(query);
      if (sharedInput && departureIata && arrivalIata) {
        try {
          const sharedFlight = await sharedFlightService.searchFlight(sharedInput, {
            userId: String(req.auth?.userId || "").trim() || null,
          });
          if (sharedFlight?.flightInstanceId && sharedFlight.freshness !== "pending") {
            return res.json({ candidates: [trackedPayloadFromSharedFlight(sharedFlight)] });
          }
        } catch (error) {
          console.warn("Shared flight search bridge failed; falling back to provider search", {
            error: error?.message || String(error),
            flightNumber,
            date,
          });
        }
      }
    }
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
      timezoneOffsetMinutes,
      error: error?.message || String(error),
    });
    const statusCode = error?.code === "FLIGHTAWARE_DAILY_BUDGET_EXHAUSTED"
      ? 429
      : error?.statusCode || 502;
    return res.status(statusCode).json({
      error: statusCode === 429
        ? "Live search capacity reached. Please retry shortly."
        : "Failed to fetch provider data",
    });
  }
});

app.get("/v1/search/route", async (req, res) => {
  return res.status(410).json({
    error: "Airport-to-airport search has been removed. Search by airline and flight number.",
  });
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
    // Old clients used `refresh=detail` every two minutes. Honoring that as a
    // cache bypass multiplied one open map into thousands of paid AeroAPI calls.
    // Only an internal explicit `refresh=force` may bypass the shared cache.
    const forceDetailRefresh = String(req.query?.refresh || "").toLowerCase() === "force";
    const trackTrail = await fetchFlightAwareTrackTrailWithLiveFallback(providerFlightId, {
      forceRefresh: forceDetailRefresh,
    });
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

async function handleUnifiedFlightAwareWebhook(req, res) {
  if (!WEBHOOK_SHARED_SECRET) {
    return res.status(503).json({ error: "Webhook secret is not configured" });
  }

  const incomingSecret = webhookSecretFromRequest(req);
  if (!timingSafeEqualText(incomingSecret, WEBHOOK_SHARED_SECRET)) {
    return res.status(401).json({ error: "Unauthorized webhook" });
  }
  if (!req.body || typeof req.body !== "object") {
    return res.status(400).json({ error: "Malformed FlightAware alert payload" });
  }

  let sharedAlertResult = null;
  if (sharedFlightService?.processFlightAwareAlertWebhook && req.body && typeof req.body === "object") {
    try {
      sharedAlertResult = await sharedFlightService.processFlightAwareAlertWebhook(req.body);
    } catch (error) {
      console.warn("Shared FlightAware alert processing failed", error?.message || String(error));
    }
  }

  if (!PROVIDER_CALLS_ENABLED) {
    const events = extractWebhookEvents(req.body);
    return res.json({
      ok: true,
      providerCallsEnabled: false,
      sharedAlertResult,
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
    sharedAlertResult,
    receivedEvents: events.length,
    matchedFlights,
    refreshedFlights,
    throttledFlights,
  });
}

// Keep the previous callback URLs live so alerts created before the unified
// endpoint migration still reach the owner-specific notification pipeline.
app.post("/webhooks/flightaware/alerts", handleUnifiedFlightAwareWebhook);
app.post("/v1/webhooks/flightaware/alerts", handleUnifiedFlightAwareWebhook);
app.post("/v1/webhooks/flightaware", handleUnifiedFlightAwareWebhook);

async function startApiServer() {
  if (apnsPrivateKeysConflict()) {
    throw new Error("Conflicting APNs private keys are configured; remove or synchronize the duplicate credential");
  }
  if (usesDatabase()) {
    await ensureDatabaseSchema();
  }

  startTrackingPoller({ keepProcessAlive: false });

  const server = app.listen(PORT, () => {
    console.log(
      `Flight proxy running on port ${PORT} provider=${FLIGHT_DATA_PROVIDER} persistence=${usesDatabase() ? "supabase-postgres" : "memory"} poller=${isPollerRunning() ? "on" : "off"} backgroundTracking=${backgroundTrackingMode()} apnsConfigured=${isApnsConfigured() ? "yes" : "no"} apnsHost=${apnsHost()} apnsTopic=${APNS_BUNDLE_ID || "missing"}`
    );
  });

  let lifecycleRecoveryRunning = false;
  const runLifecycleRecovery = async (reason) => {
    if (lifecycleRecoveryRunning) return;
    lifecycleRecoveryRunning = true;
    try {
      const recovered = await sharedFlightService.recoverLifecycleCatchups(reason);
      console.log(`Lifecycle recovery checked=${recovered.checked} scheduled=${recovered.scheduled} fanoutRecovered=${recovered.recoveredFanout || 0}`);
    } catch (error) {
      console.warn(`Lifecycle recovery failed: ${error?.message || String(error)}`);
    } finally {
      lifecycleRecoveryRunning = false;
    }
  };
  setImmediate(() => runLifecycleRecovery("api_startup"));
  const lifecycleRecoveryTimer = setInterval(() => runLifecycleRecovery("periodic_recovery"), 5 * 60_000);
  if (typeof lifecycleRecoveryTimer.unref === "function") lifecycleRecoveryTimer.unref();

  return server;
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
    coalesceFlightAwareTrackTrail,
    buildFlightAwareAlertPayload,
    circleNotificationPreferenceConditionForEventType,
    classifyFlightAwareAuthProbeResult,
    deriveAlertFlags,
    dedupeFlightAwareRecords,
    extractFlightAwareSearchRows,
    fetchFlightAwareFlights,
    fetchFlightAwareOperationalFlights,
    fetchFlightAwareScheduleFlights,
    flightAwareAlertCreationDisposition,
    flightAwareAlertIDFromPayload,
    flightAwareAlertIDFromLocation,
    flightAwareAlertContextForSharedFlight,
    flightAwareWebhookTargetURL,
    flightAwareOccurrenceBounds,
    flightAwareOperationalBounds,
    flightAwareHistoryBounds,
    flightAwareDailyBudgetLimitForEndpoint,
    flightAwareScheduleQueryItems,
    flightAwareRecordMatchesRequestedFlight,
    fetchFlightAwareSearchSources,
    healthBuildInfo,
    isFutureFlightAwareQueryDate,
    liveActivityContentState,
    liveActivityPhase,
    normalizeRecordFromFlightAware,
    normalizeWithContext,
    normalizeRecordFromAviationstack,
    normalizedTimezoneOffsetMinutes,
    notificationDedupeKey,
    notificationEventsFor,
    notificationPayloadFor,
    ordinalNumber,
    shouldRefreshTrackedRecordFromWebhook,
    testPushNotificationPayload,
    trackedProviderRefreshOptions,
    updateFlightAwareAlert,
    webhookStatusFromEvent,
    ownerNotificationPreferenceConditionForEventType,
    reconcileOperationalStatus,
    scoreCandidate,
    signApnsJwtInput,
    sharedFlightService,
    shouldPreferFlightAwareSchedules,
  },
};
