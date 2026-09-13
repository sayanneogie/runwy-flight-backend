"use strict";

// Only RevenueCat's server response is evidence of membership. Never accept
// isPro, entitlement names, or purchase dates supplied by an app request.
function paidEntitlement(payload, now = Date.now()) {
  const entitlement = payload?.subscriber?.entitlements?.first_class;
  if (!entitlement || !entitlement.product_identifier || !entitlement.purchase_date) return false;
  if (entitlement.expires_date === null) return true;
  const expires = Math.max(Date.parse(entitlement.expires_date) || 0,
    Date.parse(entitlement.grace_period_expires_date) || 0);
  return expires > now;
}

function createPaidAccess({ apiKey, query, fetchImpl = global.fetch, now = Date.now }) {
  const cache = new Map();
  const pending = new Map();
  async function membership(userId) {
    if (!userId || !apiKey) return { paid: false, verified: false };
    // Swift UUID.uuidString is uppercase; PostgreSQL UUID output is lowercase.
    // RevenueCat IDs are case-sensitive, so mirror the app's logIn identity.
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(userId)) userId = userId.toUpperCase();
    const prior = cache.get(userId);
    if (prior && prior.until > now()) return prior;
    if (pending.has(userId)) return pending.get(userId);
    const request = (async () => {
      try {
        const response = await fetchImpl(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(userId)}`, {
          headers: { Authorization: `Bearer ${apiKey}`, Accept: "application/json" },
          signal: AbortSignal.timeout(8000),
        });
        if (!response.ok) throw new Error("Membership verification unavailable");
        const payload = await response.json();
        if (!payload?.subscriber?.entitlements) throw new Error("Invalid membership response");
        const paid = paidEntitlement(payload, now());
        const entitlement = payload.subscriber.entitlements.first_class;
        const expiration = entitlement?.expires_date === null ? Infinity : Math.max(
          Date.parse(entitlement?.expires_date) || 0, Date.parse(entitlement?.grace_period_expires_date) || 0);
        const result = { paid, verified: true, until: Math.min(now() + 5 * 60_000, paid ? expiration : Infinity) };
        cache.set(userId, result);
        if (cache.size > 10000) cache.delete(cache.keys().next().value);
        return result;
      } catch (_) {
        // An outage is not evidence of expiration: don't delete subscriptions.
        // New paid work is withheld until verification succeeds.
        const result = { paid: false, verified: false, until: now() + 30_000 };
        cache.set(userId, result);
        return result;
      }
    })().finally(() => pending.delete(userId));
    pending.set(userId, request);
    return request;
  }
  async function flight(providerFlightId) {
    if (!providerFlightId || !query) return { paid: false, verified: false };
    const result = await query(`select distinct uf.user_id
      from public.user_flights uf
      left join public.tracking_sessions ts on ts.id = uf.tracking_session_id
      left join public.flight_instances fi on fi.id = uf.flight_instance_id
        or fi.id::text = ts.metadata_json->>'sharedFlightInstanceId'
      where uf.deleted_at is null and coalesce(uf.lifecycle_state, '') <> 'deleted'
        and (fi.provider_flight_id = $1
          or fi.normalized_data->'inboundFlight'->>'providerFlightId' = $1
          or ts.provider_flight_id = $1)`, [providerFlightId]);
    let verified = true;
    for (const row of result.rows) {
      const access = await membership(row.user_id);
      if (access.paid) return access;
      verified = verified && access.verified;
    }
    return { paid: false, verified };
  }
  async function tokenAllowed(token) {
    if (!query) return false;
    const result = await query(`select user_id from public.device_tokens where device_token = $1 and is_active = true
      union select user_id from public.push_devices where apns_token = $1 and push_enabled = true`, [token]);
    for (const row of result.rows) if ((await membership(row.user_id)).paid) return true;
    return false;
  }
  async function delivery(deliveryId) {
    if (!query) return { paid: false, verified: false };
    const { rows } = await query(`select nd.user_id, uf.user_id as owner_user_id
      from public.notification_deliveries nd left join public.user_flights uf on uf.id = nd.user_flight_id
      where nd.id = $1`, [deliveryId]);
    if (!rows[0]) return { paid: false, verified: true };
    const recipient = await membership(rows[0].user_id);
    if (!recipient.paid) return recipient;
    return rows[0].owner_user_id ? membership(rows[0].owner_user_id) : recipient;
  }
  return { membership, flight, tokenAllowed, delivery };
}

function withoutLiveTelemetry(value) {
  if (Array.isArray(value)) return value.map(withoutLiveTelemetry);
  if (!value || typeof value !== "object" || value instanceof Date) return value;
  const hidden = new Set(["livePosition", "position", "trackPoints", "position_lat", "position_lon", "rawProviderResponse", "rawProviderPayload"]);
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key,
    hidden.has(key) ? (key === "trackPoints" ? [] : null) : withoutLiveTelemetry(item)]));
}

module.exports = { createPaidAccess, paidEntitlement, withoutLiveTelemetry };
