require("dotenv").config();

const crypto = require("node:crypto");
const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");
const { getAirportCatalog } = require("./airport-catalog");
const { validatePushTokenPayload, validateSearchQuery, validateTrackPayload } = require("./request-schemas");
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

const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_SSL = String(process.env.DATABASE_SSL || "false").toLowerCase() === "true";
const NODE_ENV = String(process.env.NODE_ENV || "development").toLowerCase();
const IS_PRODUCTION = NODE_ENV === "production";

const CACHE_TTL_MS = toPositiveNumber(process.env.CACHE_TTL_MS, 5 * 60_000);
const FLIGHTAWARE_POSITION_CACHE_TTL_MS = toPositiveNumber(
  process.env.FLIGHTAWARE_POSITION_CACHE_TTL_MS,
  5 * 60_000
);
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
const SEARCH_LIVE_ENRICH_LIMIT = toPositiveNumber(process.env.SEARCH_LIVE_ENRICH_LIMIT, 1);
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

const MAX_PROVIDER_CACHE_ENTRIES = toPositiveNumber(process.env.MAX_PROVIDER_CACHE_ENTRIES, 2_000);
const MAX_MEMORY_TRACKED_FLIGHTS = toPositiveNumber(process.env.MAX_MEMORY_TRACKED_FLIGHTS, 10_000);
const MAX_MEMORY_PUSH_DEVICES = toPositiveNumber(process.env.MAX_MEMORY_PUSH_DEVICES, 25_000);

const APNS_KEY_ID = process.env.APNS_KEY_ID || "";
const APNS_TEAM_ID = process.env.APNS_TEAM_ID || "";
const APNS_BUNDLE_ID = process.env.APNS_BUNDLE_ID || "";
const APNS_PRIVATE_KEY = process.env.APNS_PRIVATE_KEY || "";
const APNS_PRIVATE_KEY_BASE64 = process.env.APNS_PRIVATE_KEY_BASE64 || "";
const APNS_USE_SANDBOX = String(process.env.APNS_USE_SANDBOX || "true").toLowerCase() === "true";

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
    provider: "aviationstack",
    lastUpdated: isoOrNull(live.updated) || new Date().toISOString(),
  };
}

function normalizeRecordFromFlightAware(record) {
  const flightNumber = normalizeFlightCode(
    record?.ident_iata || record?.ident || record?.fa_flight_id || record?.flight_number
  );

  const originIata = normalizeAirportCode(
    record?.origin?.code_iata ||
      record?.origin_iata ||
      record?.origin?.code ||
      record?.origin?.airport_code
  );

  const destinationIata = normalizeAirportCode(
    record?.destination?.code_iata ||
      record?.destination_iata ||
      record?.destination?.code ||
      record?.destination?.airport_code
  );

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
    parseAirlineCode(flightNumber) ||
    null;

  return {
    airlineCode,
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
    provider: "flightaware",
    lastUpdated: isoOrNull(record?.last_position?.date || record?.updated || record?.filed_time) || new Date().toISOString(),
  };
}

function makeProviderQueryKey(providerName, query) {
  return JSON.stringify({
    providerName,
    flightNumber: normalizeFlightCode(query.flightNumber),
    date: query.date || "",
    departureIata: (query.departureIata || "").toUpperCase(),
    arrivalIata: (query.arrivalIata || "").toUpperCase(),
  });
}

function makeProviderPositionKey(providerName, providerFlightId) {
  return JSON.stringify({
    providerName,
    providerFlightId: String(providerFlightId || "").trim(),
    kind: "live-position",
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

  if (normalized.flightNumber === wantedFlight) score += 6;

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

function bestMatch(records, query, normalizer) {
  if (!records.length) return null;
  return [...records].sort((a, b) => scoreCandidate(b, query, normalizer) - scoreCandidate(a, query, normalizer))[0];
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
      previousStatus: previousNormalized?.status || null,
      currentStatus: nextNormalized?.status || null,
    };
  }

  const previousDelay = Number(previousNormalized.delayMinutes || 0);
  const nextDelay = Number(nextNormalized.delayMinutes || 0);

  return {
    statusChanged: previousNormalized.status !== nextNormalized.status,
    delayedNow:
      nextNormalized.status === "delayed" &&
      (previousNormalized.status !== "delayed" || nextDelay > previousDelay),
    cancelledNow:
      nextNormalized.status === "cancelled" && previousNormalized.status !== "cancelled",
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
    const ident = normalizeFlightCode(query.flightNumber);
    const params = new URLSearchParams({ max_pages: "1" });

    if (query.date) {
      params.set("start", `${query.date}T00:00:00Z`);
      params.set("end", `${query.date}T23:59:59Z`);
    }

    const url = `${FLIGHTAWARE_BASE_URL}/flights/${encodeURIComponent(ident)}?${params.toString()}`;
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "x-apikey": FLIGHTAWARE_API_KEY,
        Accept: "application/json",
      },
    });

    // FlightAware can return 400/404 for unknown identifiers or no data windows.
    // Treat these as "no candidates" so app search shows empty results instead of hard errors.
    if (response.status === 400 || response.status === 404) {
      if (providerCache.has(key)) {
        providerCache.delete(key);
      }
      providerCache.set(key, {
        data: [],
        expiresAt: now + CACHE_TTL_MS,
      });
      enforceMapSizeLimit(providerCache, MAX_PROVIDER_CACHE_ENTRIES);
      return [];
    }

    if (!response.ok) {
      throw new Error(`Provider error (${response.status})`);
    }

    const payload = await response.json();
    const rows = Array.isArray(payload?.flights) ? payload.flights : [];
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
  const normalized = normalizer(record);
  const recentHistory = deriveRecentHistory(records, record, normalizer);
  const alerts = deriveAlertFlags(previousNormalized, normalized);

  return {
    ...normalized,
    recentHistory,
    alerts,
    provider: FLIGHT_DATA_PROVIDER,
  };
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
  listDueTrackingRows,
  listTrackedFlightsByProviderFlightId,
  listPushTokensForFlight,
  listTrackedFlightsByFlightNumber,
  markTrackingRowErrored,
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

    return {
      ...normalized,
      livePosition,
    };
  } catch (error) {
    console.warn(
      `FlightAware live position lookup failed for ${providerFlightId}: ${error?.message || String(error)}`
    );
    return normalized;
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

  if (usesDatabase()) {
    await pool.query(
      `
      with recipients as (
        select ts.owner_user_id as user_id
        from public.tracking_sessions ts
        where ts.id = $1::uuid

        union

        select fw.watcher_user_id as user_id
        from public.flight_watchers fw
        where fw.tracking_session_id = $1::uuid
          and fw.watch_state = 'approved'
          and fw.can_receive_notifications = true
      )
      insert into public.notifications (
        user_id,
        tracking_session_id,
        notification_type,
        delivery_channel,
        delivery_status,
        title,
        body,
        payload_json,
        scheduled_for
      )
      select
        recipients.user_id,
        $1::uuid,
        $2,
        'push',
        $3,
        $4,
        $5,
        $6::jsonb,
        now()
      from recipients
      `,
      [flightId, event.type, isApnsConfigured() ? "queued" : "pending", event.title, event.body, JSON.stringify(event.payload)]
    );
  }

  const tokens = await listPushTokensForFlight(flightId);
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

  if (includeLivePosition) {
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

  if (normalized.alerts?.cancelledNow || normalized.alerts?.delayedNow) {
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

app.get("/health", async (_req, res) => {
  let trackingSummary = null;

  if (usesDatabase()) {
    try {
      trackingSummary = await fetchTrackingSessionStatusSummary();
    } catch (error) {
      trackingSummary = {
        error: error?.message || String(error),
      };
    }
  }

  res.json({
    ok: true,
    provider: FLIGHT_DATA_PROVIDER,
    providerCallsEnabled: PROVIDER_CALLS_ENABLED,
    nodeEnv: NODE_ENV,
    persistence: usesDatabase() ? "supabase-postgres" : "memory",
    apnsConfigured: isApnsConfigured(),
    pollerEnabled: isPollerRunning(),
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

    let normalized = normalizeWithContext(selected, records, query, provider.normalizeRecord, null);
    normalized = await enrichNormalizedWithLivePosition(normalized, provider.name, selected);
    normalized.lastUpdated = normalized.livePosition?.recordedAt || normalized.lastUpdated || new Date().toISOString();

    const tracked = await createOrReuseTrackingSession({
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

  const tracked = await fetchAccessibleTrackingRow(flightId, userId);
  if (!tracked) {
    return res.status(404).json({ error: "Unknown flightId" });
  }

  try {
    const shouldRefresh = isTrackedRecordRefreshDue(tracked);
    const current = shouldRefresh ? await refreshTrackedFlightRecord(tracked) : tracked;

    return res.json({
      flightId,
      normalized: current.normalized,
      lastUpdated: current.lastUpdated,
    });
  } catch (_error) {
    return res.status(200).json({
      flightId,
      normalized: tracked.normalized,
      lastUpdated: tracked.lastUpdated,
    });
  }
});

app.get("/v1/search", async (req, res) => {
  const validated = validateSearchQuery(req.query);
  if (validated.error) {
    return res.status(400).json({ error: validated.error });
  }

  const { flightNumber, date, departureIata, arrivalIata } = validated.value;

  if (!PROVIDER_CALLS_ENABLED) {
    return res.json({ candidates: [], providerDisabled: true });
  }

  try {
    const query = { flightNumber, date, departureIata, arrivalIata };
    const provider = providerAdapter();
    const records = await provider.fetchFlights(query);
    const topRecords = records
      .sort((a, b) => scoreCandidate(b, query, provider.normalizeRecord) - scoreCandidate(a, query, provider.normalizeRecord))
      .slice(0, 10);

    const normalized = await Promise.all(
      topRecords.map(async (record, index) => {
        let candidate = normalizeWithContext(record, records, query, provider.normalizeRecord, null);
        if (index < SEARCH_LIVE_ENRICH_LIMIT) {
          candidate = await enrichNormalizedWithLivePosition(candidate, provider.name, record);
        }
        candidate.lastUpdated = candidate.livePosition?.recordedAt || candidate.lastUpdated || new Date().toISOString();
        return candidate;
      })
    );

    return res.json({ candidates: normalized });
  } catch (error) {
    console.error("Search provider fetch failed", {
      provider: FLIGHT_DATA_PROVIDER,
      flightNumber,
      date,
      departureIata,
      arrivalIata,
      error: error?.message || String(error),
    });
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.post("/v1/webhooks/flightaware", async (req, res) => {
  if (!WEBHOOK_SHARED_SECRET) {
    return res.status(503).json({ error: "Webhook secret is not configured" });
  }

  const incomingSecret = req.get("X-Runwy-Webhook-Secret") || "";
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
  runTrackingPollerCycle,
  startApiServer,
  startTrackingPoller,
  startTrackingPollerWorker,
  usesDatabase,
};
