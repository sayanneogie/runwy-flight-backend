"use strict";

require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("pg");

async function main() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL) === "true" ? {
      rejectUnauthorized: String(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED || "true") !== "false",
    } : undefined,
    connectionTimeoutMillis: 10000,
    statement_timeout: 30000,
  });
  try {
    await client.connect();
    if (process.argv.includes("--apply")) {
      await client.query(fs.readFileSync(path.join(__dirname, "../supabase/migrations/20260915_request_abuse_guardrails.sql"), "utf8"));
      await client.query("notify pgrst, 'reload schema'");
    }
    const { rows } = await client.query(`select
      to_regprocedure('public.runwy_consume_rate_limit(text,text,integer,integer)') is not null as rate_limits_ready,
      to_regprocedure('public.runwy_reserve_flightaware_budget(text,integer,integer)') is not null as provider_budget_ready,
      not has_schema_privilege('anon', 'runwy_security', 'USAGE') as anonymous_access_blocked,
      not has_schema_privilege('authenticated', 'runwy_security', 'USAGE') as client_access_blocked`);
    console.log(JSON.stringify(rows[0]));
    if (!Object.values(rows[0]).every(Boolean)) throw new Error("Request protection migration verification failed");
  } finally { await client.end(); }
}

main().catch(error => { console.error("Request protection migration failed:", error.message); process.exitCode = 1; });
