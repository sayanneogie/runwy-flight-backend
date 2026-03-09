#!/usr/bin/env node

const path = require("path");
const dotenv = require("dotenv");
const { Client } = require("pg");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const REQUIRED_TABLES = [
  "profiles",
  "user_settings",
  "tracking_sessions",
  "user_flights",
  "flight_watchers",
  "live_snapshots",
  "notifications",
  "push_devices",
];

function boolText(value) {
  return value ? "yes" : "no";
}

function redact(value) {
  const text = String(value || "");
  if (!text) return "<missing>";
  if (text.length <= 8) return "<set>";
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

async function checkDatabase(results) {
  const connectionString = process.env.DATABASE_URL || "";
  if (!connectionString) {
    results.push({
      area: "database",
      ok: false,
      detail: "DATABASE_URL is missing",
    });
    return;
  }

  const ssl =
    String(process.env.DATABASE_SSL || "").toLowerCase() === "true"
      ? { rejectUnauthorized: false }
      : undefined;

  const client = new Client({ connectionString, ssl });

  try {
    await client.connect();

    const tableResult = await client.query(
      `
        select table_name
        from information_schema.tables
        where table_schema = 'public'
          and table_name = any($1::text[])
      `,
      [REQUIRED_TABLES]
    );

    const present = new Set(tableResult.rows.map((row) => row.table_name));
    const missing = REQUIRED_TABLES.filter((name) => !present.has(name));

    results.push({
      area: "database",
      ok: missing.length === 0,
      detail:
        missing.length === 0
          ? "connected and all live-tracking tables are present"
          : `connected but missing tables: ${missing.join(", ")}`,
    });
  } catch (error) {
    results.push({
      area: "database",
      ok: false,
      detail: `connection failed: ${error.message}`,
    });
  } finally {
    try {
      await client.end();
    } catch {}
  }
}

function checkAuthEnv(results) {
  const hasJWTSecret = Boolean(String(process.env.SUPABASE_JWT_SECRET || "").trim());
  const hasURL = Boolean(String(process.env.SUPABASE_URL || "").trim());
  const hasAnonKey = Boolean(String(process.env.SUPABASE_ANON_KEY || "").trim());

  const ok = hasJWTSecret || (hasURL && hasAnonKey);
  results.push({
    area: "auth-env",
    ok,
    detail: ok
      ? `jwt_secret=${boolText(hasJWTSecret)} supabase_url=${boolText(hasURL)} anon_key=${boolText(hasAnonKey)}`
      : "set SUPABASE_JWT_SECRET or both SUPABASE_URL and SUPABASE_ANON_KEY",
  });
}

async function checkFlightAware(results) {
  const apiKey = String(process.env.FLIGHTAWARE_API_KEY || "").trim();
  const baseURL = String(process.env.FLIGHTAWARE_BASE_URL || "https://aeroapi.flightaware.com/aeroapi").trim();

  if (!apiKey) {
    results.push({
      area: "provider-flightaware",
      ok: false,
      detail: "FLIGHTAWARE_API_KEY is missing",
    });
    return;
  }

  const url = `${baseURL}/flights/AI203?max_pages=1&start=2026-02-22T00:00:00Z&end=2026-02-22T23:59:59Z`;

  try {
    const response = await fetch(url, {
      headers: {
        "x-apikey": apiKey,
        Accept: "application/json",
      },
    });
    const body = await response.text();

    results.push({
      area: "provider-flightaware",
      ok: response.status !== 401,
      detail:
        response.status === 401
          ? `invalid key (${redact(apiKey)}): ${body.slice(0, 160)}`
          : `auth accepted with status ${response.status}`,
    });
  } catch (error) {
    results.push({
      area: "provider-flightaware",
      ok: false,
      detail: `request failed: ${error.message}`,
    });
  }
}

async function checkAviationstack(results) {
  const apiKey = String(process.env.AVIATIONSTACK_KEY || "").trim();
  const baseURL = String(process.env.AVIATIONSTACK_BASE_URL || "https://api.aviationstack.com/v1").trim();

  if (!apiKey) {
    results.push({
      area: "provider-aviationstack",
      ok: false,
      detail: "AVIATIONSTACK_KEY is missing",
    });
    return;
  }

  const url = `${baseURL}/flights?access_key=${encodeURIComponent(apiKey)}&limit=1&flight_iata=DL404&flight_date=2026-03-08&dep_iata=LAX`;

  try {
    const response = await fetch(url);
    const body = await response.text();

    results.push({
      area: "provider-aviationstack",
      ok: response.status !== 401,
      detail:
        response.status === 401
          ? `invalid key (${redact(apiKey)}): ${body.slice(0, 160)}`
          : `auth accepted with status ${response.status}`,
    });
  } catch (error) {
    results.push({
      area: "provider-aviationstack",
      ok: false,
      detail: `request failed: ${error.message}`,
    });
  }
}

async function main() {
  const results = [];

  checkAuthEnv(results);
  await checkDatabase(results);
  await checkFlightAware(results);
  await checkAviationstack(results);

  let failures = 0;
  for (const result of results) {
    const marker = result.ok ? "OK" : "FAIL";
    if (!result.ok) failures += 1;
    console.log(`[${marker}] ${result.area}: ${result.detail}`);
  }

  process.exitCode = failures > 0 ? 1 : 0;
}

main().catch((error) => {
  console.error(`[FAIL] doctor: ${error.message}`);
  process.exit(1);
});
