"use strict";
async function reserveFlightWork(query, userId, input, { tracking = false, limit = 20 } = {}) {
  const number = String(input.flightNumber || `${input.airline || ''}${input.number || ''}`).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const { rows } = await query('select public.runwy_reserve_flight_work($1,$2,$3::date,$4,$5,$6,$7) as token',
    [userId, number, input.date, input.origin || input.departureIata || null, input.destination || input.arrivalIata || null, tracking, limit]);
  return rows[0].token;
}
async function releaseFlightWork(query, userId, token) {
  await query('delete from runwy_security.flight_work_reservations where user_id=$1 and token=$2', [userId, token]);
}
module.exports = { reserveFlightWork, releaseFlightWork };
