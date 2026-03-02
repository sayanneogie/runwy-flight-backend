require("dotenv").config();

const crypto = require("node:crypto");
const express = require("express");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");

const PORT = Number(process.env.PORT || 8787);
const FLIGHT_DATA_PROVIDER = (process.env.FLIGHT_DATA_PROVIDER || "aviationstack").toLowerCase();

const AVIATIONSTACK_KEY = process.env.AVIATIONSTACK_KEY;
const AVIATIONSTACK_BASE_URL =
  process.env.AVIATIONSTACK_BASE_URL || "http://api.aviationstack.com/v1";

const FLIGHTAWARE_API_KEY = process.env.FLIGHTAWARE_API_KEY;
const FLIGHTAWARE_BASE_URL =
  process.env.FLIGHTAWARE_BASE_URL || "https://aeroapi.flightaware.com/aeroapi";

const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_SSL = String(process.env.DATABASE_SSL || "false").toLowerCase() === "true";

const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 90_000);
const RATE_LIMIT_PER_MINUTE = Number(process.env.RATE_LIMIT_PER_MINUTE || 60);
const WEBHOOK_SHARED_SECRET = process.env.WEBHOOK_SHARED_SECRET || "";

const APNS_KEY_ID = process.env.APNS_KEY_ID || "";
const APNS_TEAM_ID = process.env.APNS_TEAM_ID || "";
const APNS_BUNDLE_ID = process.env.APNS_BUNDLE_ID || "";
const APNS_PRIVATE_KEY = process.env.APNS_PRIVATE_KEY || "";
const APNS_PRIVATE_KEY_BASE64 = process.env.APNS_PRIVATE_KEY_BASE64 || "";
const APNS_USE_SANDBOX = String(process.env.APNS_USE_SANDBOX || "true").toLowerCase() === "true";

if (FLIGHT_DATA_PROVIDER === "aviationstack" && !AVIATIONSTACK_KEY) {
  console.error("Missing AVIATIONSTACK_KEY environment variable.");
  process.exit(1);
}

if (FLIGHT_DATA_PROVIDER === "flightaware" && !FLIGHTAWARE_API_KEY) {
  console.error("Missing FLIGHTAWARE_API_KEY environment variable.");
  process.exit(1);
}

if (!["aviationstack", "flightaware"].includes(FLIGHT_DATA_PROVIDER)) {
  console.error(`Unsupported FLIGHT_DATA_PROVIDER: ${FLIGHT_DATA_PROVIDER}`);
  process.exit(1);
}

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "100kb" }));

const limiter = rateLimit({
  windowMs: 60_000,
  max: RATE_LIMIT_PER_MINUTE,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const deviceID = req.get("X-Device-Id");
    if (deviceID && deviceID.trim().length > 0) {
      return `device:${deviceID.trim().slice(0, 128)}`;
    }
    return `ip:${req.ip || "unknown"}`;
  },
});

app.use("/v1", limiter);

const providerCache = new Map();
const memoryTrackedFlights = new Map();
const memoryFlightSubscriptions = new Map();
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
}, 60_000).unref();

const apnsTokenCache = {
  token: null,
  expiresAt: 0,
};

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

  const arrivalTimes = {
    scheduled: isoOrNull(arrival.scheduled),
    estimated: isoOrNull(arrival.estimated),
    actual: isoOrNull(arrival.actual),
  };

  return {
    airlineCode,
    flightNumber: normalizeFlightCode(flightNumberRaw),
    departureAirportIata: normalizeAirportCode(departure.iata),
    arrivalAirportIata: normalizeAirportCode(arrival.iata),
    departureTimes,
    arrivalTimes,
    status: normalizeStatus(record?.flight_status),
    terminal: departure.terminal || arrival.terminal || null,
    gate: departure.gate || arrival.gate || null,
    delayMinutes: calculateDelayMinutes(departureTimes),
    inboundFlight: null,
    recentHistory: [],
    alerts: null,
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

  const arrivalTimes = {
    scheduled: isoOrNull(record?.scheduled_in || record?.scheduled_arrival_time),
    estimated: isoOrNull(record?.estimated_in || record?.estimated_arrival_time),
    actual: isoOrNull(record?.actual_in || record?.actual_arrival_time),
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
  const key = makeProviderQueryKey("aviationstack", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

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
  providerCache.set(key, {
    data: rows,
    expiresAt: now + CACHE_TTL_MS,
  });

  return rows;
}

async function fetchFlightAwareFlights(query) {
  const key = makeProviderQueryKey("flightaware", query);
  const now = Date.now();
  const cached = providerCache.get(key);
  if (cached && cached.expiresAt > now) {
    return cached.data;
  }

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
    providerCache.set(key, {
      data: [],
      expiresAt: now + CACHE_TTL_MS,
    });
    return [];
  }

  if (!response.ok) {
    throw new Error(`Provider error (${response.status})`);
  }

  const payload = await response.json();
  const rows = Array.isArray(payload?.flights) ? payload.flights : [];
  providerCache.set(key, {
    data: rows,
    expiresAt: now + CACHE_TTL_MS,
  });

  return rows;
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

function validateTrackPayload(body) {
  const flightNumber = normalizeFlightCode(body?.flightNumber);
  const date = String(body?.date || "");
  const departureIata = body?.departureIata ? String(body.departureIata).toUpperCase() : undefined;
  const arrivalIata = body?.arrivalIata ? String(body.arrivalIata).toUpperCase() : undefined;

  if (!flightNumber.match(/^[A-Z0-9]{3,8}$/)) return { error: "Invalid flightNumber" };
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) return { error: "Invalid date (expected YYYY-MM-DD)" };
  if (departureIata && !departureIata.match(/^[A-Z]{3}$/)) return { error: "Invalid departureIata" };
  if (arrivalIata && !arrivalIata.match(/^[A-Z]{3}$/)) return { error: "Invalid arrivalIata" };

  return {
    value: {
      flightNumber,
      date,
      departureIata,
      arrivalIata,
    },
  };
}

function mapTrackedFlightRow(row) {
  const rawDate = row.last_updated;
  const lastUpdated = rawDate instanceof Date ? rawDate.toISOString() : new Date(rawDate).toISOString();

  return {
    flightId: row.flight_id,
    query: row.query,
    normalized: row.normalized,
    provider: row.provider,
    lastUpdated,
  };
}

function usesDatabase() {
  return Boolean(pool);
}

async function ensureDatabaseSchema() {
  if (!usesDatabase()) return;

  await pool.query(`
    create table if not exists runwy_tracked_flights (
      flight_id text primary key,
      query jsonb not null,
      normalized jsonb not null,
      provider text not null,
      last_updated timestamptz not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  `);

  await pool.query(`
    create table if not exists runwy_flight_subscriptions (
      flight_id text not null references runwy_tracked_flights(flight_id) on delete cascade,
      device_id text not null,
      created_at timestamptz not null default now(),
      primary key (flight_id, device_id)
    );
  `);

  await pool.query(`
    create table if not exists runwy_push_devices (
      apns_token text primary key,
      device_id text not null,
      user_id text,
      platform text not null default 'ios',
      push_enabled boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  `);

  await pool.query(`
    create index if not exists runwy_push_devices_device_idx
    on runwy_push_devices(device_id);
  `);

  await pool.query(`
    create index if not exists runwy_tracked_flights_flight_number_idx
    on runwy_tracked_flights((upper(query->>'flightNumber')));
  `);
}

async function upsertTrackedFlightRecord({ flightId, query, normalized, provider, lastUpdated }) {
  if (usesDatabase()) {
    await pool.query(
      `
      insert into runwy_tracked_flights (flight_id, query, normalized, provider, last_updated)
      values ($1, $2::jsonb, $3::jsonb, $4, $5::timestamptz)
      on conflict (flight_id) do update
      set
        query = excluded.query,
        normalized = excluded.normalized,
        provider = excluded.provider,
        last_updated = excluded.last_updated,
        updated_at = now()
      `,
      [flightId, JSON.stringify(query), JSON.stringify(normalized), provider, lastUpdated]
    );
    return;
  }

  memoryTrackedFlights.set(flightId, { flightId, query, normalized, provider, lastUpdated });
}

async function getTrackedFlightRecord(flightId) {
  if (usesDatabase()) {
    const result = await pool.query(
      `
      select flight_id, query, normalized, provider, last_updated
      from runwy_tracked_flights
      where flight_id = $1
      limit 1
      `,
      [flightId]
    );

    if (!result.rows.length) return null;
    return mapTrackedFlightRow(result.rows[0]);
  }

  return memoryTrackedFlights.get(flightId) || null;
}

async function listTrackedFlightsByFlightNumber(flightNumber) {
  if (usesDatabase()) {
    const result = await pool.query(
      `
      select flight_id, query, normalized, provider, last_updated
      from runwy_tracked_flights
      where upper(query->>'flightNumber') = upper($1)
      `,
      [normalizeFlightCode(flightNumber)]
    );

    return result.rows.map(mapTrackedFlightRow);
  }

  return [...memoryTrackedFlights.values()].filter((item) =>
    normalizeFlightCode(item.query?.flightNumber) === normalizeFlightCode(flightNumber)
  );
}

async function addFlightSubscription(flightId, deviceId) {
  if (!deviceId) return;

  if (usesDatabase()) {
    await pool.query(
      `
      insert into runwy_flight_subscriptions (flight_id, device_id)
      values ($1, $2)
      on conflict do nothing
      `,
      [flightId, deviceId]
    );
    return;
  }

  const current = memoryFlightSubscriptions.get(flightId) || new Set();
  current.add(deviceId);
  memoryFlightSubscriptions.set(flightId, current);
}

async function upsertPushDevice({ apnsToken, deviceId, userId, platform = "ios" }) {
  if (usesDatabase()) {
    await pool.query(
      `
      insert into runwy_push_devices (apns_token, device_id, user_id, platform, push_enabled, updated_at)
      values ($1, $2, $3, $4, true, now())
      on conflict (apns_token) do update
      set
        device_id = excluded.device_id,
        user_id = excluded.user_id,
        platform = excluded.platform,
        push_enabled = true,
        updated_at = now()
      `,
      [apnsToken, deviceId, userId || null, platform]
    );

    return;
  }

  memoryPushDevices.set(apnsToken, {
    apnsToken,
    deviceId,
    userId: userId || null,
    platform,
    pushEnabled: true,
    updatedAt: new Date().toISOString(),
  });
}

async function disablePushTokensForDevice(deviceId) {
  if (!deviceId) return;

  if (usesDatabase()) {
    await pool.query(
      `
      update runwy_push_devices
      set push_enabled = false, updated_at = now()
      where device_id = $1
      `,
      [deviceId]
    );
    return;
  }

  for (const [token, info] of memoryPushDevices.entries()) {
    if (info.deviceId === deviceId) {
      memoryPushDevices.set(token, {
        ...info,
        pushEnabled: false,
        updatedAt: new Date().toISOString(),
      });
    }
  }
}

async function disablePushToken(apnsToken) {
  if (!apnsToken) return;

  if (usesDatabase()) {
    await pool.query(
      `
      update runwy_push_devices
      set push_enabled = false, updated_at = now()
      where apns_token = $1
      `,
      [apnsToken]
    );
    return;
  }

  const info = memoryPushDevices.get(apnsToken);
  if (!info) return;
  memoryPushDevices.set(apnsToken, {
    ...info,
    pushEnabled: false,
    updatedAt: new Date().toISOString(),
  });
}

async function listPushTokensForFlight(flightId) {
  if (usesDatabase()) {
    const result = await pool.query(
      `
      select pd.apns_token
      from runwy_flight_subscriptions fs
      join runwy_push_devices pd on pd.device_id = fs.device_id
      where fs.flight_id = $1
        and pd.push_enabled = true
      `,
      [flightId]
    );

    return result.rows.map((row) => row.apns_token);
  }

  const subscriptions = memoryFlightSubscriptions.get(flightId);
  if (!subscriptions || subscriptions.size === 0) return [];

  const allowed = new Set(subscriptions);
  return [...memoryPushDevices.values()]
    .filter((device) => device.pushEnabled && allowed.has(device.deviceId))
    .map((device) => device.apnsToken);
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
  const payload = notificationPayloadFor(normalized, flightId);
  if (!payload) return;

  const tokens = await listPushTokensForFlight(flightId);
  if (!tokens.length) return;

  await Promise.all(tokens.map((token) => sendApnsNotification(token, payload)));
}

async function refreshTrackedFlightRecord(trackedRecord) {
  const provider = providerAdapter(trackedRecord.provider || FLIGHT_DATA_PROVIDER);
  const records = await provider.fetchFlights(trackedRecord.query);
  const selected = bestMatch(records, trackedRecord.query, provider.normalizeRecord);

  if (!selected) {
    return trackedRecord;
  }

  const normalized = normalizeWithContext(
    selected,
    records,
    trackedRecord.query,
    provider.normalizeRecord,
    trackedRecord.normalized
  );

  const lastUpdated = new Date().toISOString();
  await upsertTrackedFlightRecord({
    flightId: trackedRecord.flightId,
    query: trackedRecord.query,
    normalized,
    provider: provider.name,
    lastUpdated,
  });

  if (normalized.alerts?.cancelledNow || normalized.alerts?.delayedNow) {
    await dispatchFlightStatusNotifications(trackedRecord.flightId, normalized);
  }

  return {
    ...trackedRecord,
    normalized,
    provider: provider.name,
    lastUpdated,
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

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    provider: FLIGHT_DATA_PROVIDER,
    persistence: usesDatabase() ? "postgres" : "memory",
    apnsConfigured: isApnsConfigured(),
  });
});

app.post("/v1/devices/push-token", async (req, res) => {
  const deviceId = req.get("X-Device-Id")?.trim();
  const token = String(req.body?.token || "").trim();
  const platform = String(req.body?.platform || "ios").toLowerCase();
  const userId = req.body?.userId ? String(req.body.userId) : null;

  if (!deviceId) {
    return res.status(400).json({ error: "X-Device-Id header is required" });
  }

  if (!/^[A-Fa-f0-9]{64,512}$/.test(token)) {
    return res.status(400).json({ error: "Invalid APNs token" });
  }

  try {
    await upsertPushDevice({
      apnsToken: token.toLowerCase(),
      deviceId,
      userId,
      platform,
    });

    return res.json({ ok: true });
  } catch (_error) {
    return res.status(500).json({ error: "Unable to store push token" });
  }
});

app.post("/v1/devices/push-token/remove", async (req, res) => {
  const deviceId = req.get("X-Device-Id")?.trim();
  if (!deviceId) {
    return res.status(400).json({ error: "X-Device-Id header is required" });
  }

  try {
    await disablePushTokensForDevice(deviceId);
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

  try {
    const query = validated.value;
    const provider = providerAdapter();
    const records = await provider.fetchFlights(query);
    const selected = bestMatch(records, query, provider.normalizeRecord);
    if (!selected) {
      return res.status(404).json({ error: "No matching flight found" });
    }

    const normalized = normalizeWithContext(selected, records, query, provider.normalizeRecord, null);
    const flightId = crypto.randomUUID();
    const lastUpdated = new Date().toISOString();

    await upsertTrackedFlightRecord({
      flightId,
      query,
      normalized,
      provider: provider.name,
      lastUpdated,
    });

    const deviceId = req.get("X-Device-Id")?.trim();
    if (deviceId) {
      await addFlightSubscription(flightId, deviceId);
    }

    return res.json({
      flightId,
      normalized,
    });
  } catch (_error) {
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.get("/v1/flights/:flightId", async (req, res) => {
  const flightId = req.params.flightId;
  const tracked = await getTrackedFlightRecord(flightId);
  if (!tracked) {
    return res.status(404).json({ error: "Unknown flightId" });
  }

  try {
    const refreshed = await refreshTrackedFlightRecord(tracked);

    return res.json({
      flightId,
      normalized: refreshed.normalized,
      lastUpdated: refreshed.lastUpdated,
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
  const flightNumber = normalizeFlightCode(req.query.flightNumber || "");
  const date = String(req.query.date || "");
  const departureIata = req.query.dep ? String(req.query.dep).toUpperCase() : undefined;
  const arrivalIata = req.query.arr ? String(req.query.arr).toUpperCase() : undefined;

  if (!flightNumber) {
    return res.status(400).json({ error: "flightNumber is required" });
  }
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    return res.status(400).json({ error: "date is required (YYYY-MM-DD)" });
  }

  try {
    const query = { flightNumber, date, departureIata, arrivalIata };
    const provider = providerAdapter();
    const records = await provider.fetchFlights(query);
    const normalized = records
      .sort((a, b) => scoreCandidate(b, query, provider.normalizeRecord) - scoreCandidate(a, query, provider.normalizeRecord))
      .slice(0, 10)
      .map((record) => normalizeWithContext(record, records, query, provider.normalizeRecord, null));

    return res.json({ candidates: normalized });
  } catch (_error) {
    return res.status(502).json({ error: "Failed to fetch provider data" });
  }
});

app.post("/v1/webhooks/flightaware", async (req, res) => {
  if (WEBHOOK_SHARED_SECRET) {
    const incomingSecret = req.get("X-Runwy-Webhook-Secret") || "";
    if (incomingSecret !== WEBHOOK_SHARED_SECRET) {
      return res.status(401).json({ error: "Unauthorized webhook" });
    }
  }

  const events = extractWebhookEvents(req.body);

  let matchedFlights = 0;
  let refreshedFlights = 0;

  for (const event of events) {
    const flightNumber = flightNumberFromWebhookEvent(event);
    if (!flightNumber) {
      continue;
    }

    const candidates = await listTrackedFlightsByFlightNumber(flightNumber);
    if (!candidates.length) {
      continue;
    }

    matchedFlights += candidates.length;

    for (const tracked of candidates) {
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
  });
});

async function start() {
  if (usesDatabase()) {
    await ensureDatabaseSchema();
  }

  app.listen(PORT, () => {
    console.log(
      `Flight proxy running on port ${PORT} provider=${FLIGHT_DATA_PROVIDER} persistence=${usesDatabase() ? "postgres" : "memory"}`
    );
  });
}

start().catch((error) => {
  console.error("Failed to start flight proxy", error);
  process.exit(1);
});
