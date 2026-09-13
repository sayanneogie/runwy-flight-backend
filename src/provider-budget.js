"use strict";

// Reserve before issuing a paid request. The short transaction serializes each
// budget bucket, without holding a database connection during the HTTP request.
async function reserveProviderBudget(pool, { endpoint, tracked, limit, units, path, reason }) {
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query("select pg_advisory_xact_lock(hashtext($1))", [`runwy:flightaware:${tracked ? 'tracked' : 'general'}`]);
    const result = await client.query(`select coalesce(sum(coalesce(cost_estimate, 1)), 0)::int as units
      from public.api_usage_logs where provider='flightaware' and endpoint like 'aeroapi:flight:%'
        and (($1::boolean and endpoint='aeroapi:flight:tracked_flight')
          or (not $1::boolean and endpoint<>'aeroapi:flight:tracked_flight'))
        and created_at >= date_trunc('day', clock_timestamp() at time zone 'UTC') at time zone 'UTC'`, [tracked]);
    const used = Number(result.rows[0]?.units || 0);
    if (used + units > limit) {
      const error = new Error(`FlightAware daily ${tracked ? 'tracked-flight' : 'general'} budget exhausted (${used}/${limit})`);
      error.code = "FLIGHTAWARE_DAILY_BUDGET_EXHAUSTED";
      error.statusCode = 429;
      throw error;
    }
    const reservation = await client.query(`insert into public.api_usage_logs
      (provider,endpoint,cache_status,cost_estimate,provider_path,request_reason,created_at)
      values ('flightaware',$1,'reserved',$2,$3,$4,clock_timestamp()) returning id`, [endpoint, units, path, reason]);
    await client.query("commit");
    return reservation.rows[0].id;
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally { client.release(); }
}
module.exports = { reserveProviderBudget };
