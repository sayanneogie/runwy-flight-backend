"use strict";

const { createFlightCache } = require("./cache");
const {
  compareFlightState,
  getFlightFreshnessTTL,
  isFinalStatus,
  isProviderAlertActive,
  isStreamingActive,
  mapNormalizedToDb,
  normalizeSearchParams,
  reconcileDiversionContext,
  rowToFlightResponse,
  validateProviderFlight,
} = require("./state");
const { createSharedFlightQueue } = require("./queue");
const { createApnsSender, notificationPayload } = require("./notifications");
const { createFlightWeatherService, weatherEventFromInsight, weatherTargetForFlight } = require("./weather");
const {
  extractFlightAwareAlertEvents,
  flightUpdateFromAlert,
  generateFlightAwareAlertDedupeKey,
  normalizeFlightAwareAlert,
  targetMatchesAlert,
} = require("../flightaware-alerts");

const FETCH_LOCK_MS = 90_000;
const REFRESH_LOCK_MS = 90_000;
const ACTIVE_VIEWER_TTL_SECONDS = 90;
const DEPARTURE_CATCHUP_AFTER_MS = 2 * 60_000;
const DEPARTURE_CATCHUP_FINAL_AFTER_MS = 8 * 60_000;
const MISSED_DEPARTURE_RECOVERY_MIN_STALE_MS = 10 * 60_000;
const MISSED_DEPARTURE_RECOVERY_WINDOW_MS = 2 * 60 * 60_000;
const ARRIVAL_CATCHUP_AFTER_MS = 6 * 60_000;
const PREFLIGHT_REMINDER_BEFORE_MS = 5 * 60 * 60_000;
const INBOUND_MONITOR_WINDOW_MS = 3 * 60 * 60_000;
const DEFAULT_API_ACTIVE_POLL_MS = 2 * 60_000;
const DEFAULT_API_PREDEPARTURE_POLL_MS = 5 * 60_000;
const DEFAULT_API_PREDEPARTURE_WINDOW_MS = 45 * 60_000;
const DEFAULT_API_POST_ARRIVAL_POLL_MS = 15 * 60_000;
const DEFAULT_API_POST_ARRIVAL_WINDOW_MS = 6 * 60 * 60_000;
const MAX_API_POLL_SCHEDULE_AHEAD_MS = 72 * 60 * 60_000;
const DEPARTURE_DETAIL_CHECKPOINTS = [
  { stage: "t-2h", offsetMs: -2 * 60 * 60_000 },
  { stage: "t-30m", offsetMs: -30 * 60_000 },
];
const INBOUND_MONITOR_CHECKPOINTS = [
  { stage: "t-3h", offsetMs: -3 * 60 * 60_000 },
  { stage: "t-60m", offsetMs: -60 * 60_000 },
];
const ARRIVAL_DETAIL_CHECKPOINTS = [
  { stage: "t-60m", offsetMs: -60 * 60_000 },
  { stage: "t-30m", offsetMs: -30 * 60_000 },
  { stage: "post-5m", offsetMs: 5 * 60_000 },
  { stage: "post-15m", offsetMs: 15 * 60_000 },
  { stage: "post-30m", offsetMs: 30 * 60_000 },
];

function normalizedBreadcrumb(point) {
  const latitude = Number(point?.latitude ?? point?.lat);
  const longitude = Number(point?.longitude ?? point?.lon);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return {
    latitude,
    longitude,
    headingDegrees: point?.headingDegrees ?? point?.heading ?? null,
    groundSpeedKnots: point?.groundSpeedKnots ?? point?.groundSpeed ?? null,
    altitudeFeet: point?.altitudeFeet ?? point?.altitude ?? null,
    recordedAt: point?.recordedAt || null,
  };
}

function accumulatedBreadcrumbs(previous, incoming) {
  const candidates = [
    ...(Array.isArray(previous?.trackPoints) ? previous.trackPoints : []),
    previous?.position,
    previous?.livePosition,
    ...(Array.isArray(incoming?.trackPoints) ? incoming.trackPoints : []),
    incoming?.position,
    incoming?.livePosition,
  ].map(normalizedBreadcrumb).filter(Boolean);

  candidates.sort((left, right) => {
    const leftMs = Date.parse(left.recordedAt || "");
    const rightMs = Date.parse(right.recordedAt || "");
    return (Number.isFinite(leftMs) ? leftMs : 0) - (Number.isFinite(rightMs) ? rightMs : 0);
  });

  const result = [];
  for (const point of candidates) {
    const last = result[result.length - 1];
    if (
      last &&
      Math.abs(last.latitude - point.latitude) < 0.0001 &&
      Math.abs(last.longitude - point.longitude) < 0.0001
    ) {
      result[result.length - 1] = point;
    } else {
      result.push(point);
    }
  }
  return result.slice(-2_000);
}

function preserveKnownOperationalFields(normalized, row) {
  const previous = row?.normalized_data || {};
  const preferred = (next, fallback) => {
    if (next === null || next === undefined || String(next).trim() === "") {
      return fallback ?? null;
    }
    return next;
  };

  const mergeInboundFlight = (nextInbound, previousInbound) => {
    if (!nextInbound) return previousInbound || null;
    if (!previousInbound) return nextInbound;

    const normalizedIdentity = (value) => String(value || "").trim().toUpperCase();
    const nextProviderID = normalizedIdentity(nextInbound.providerFlightId);
    const previousProviderID = normalizedIdentity(previousInbound.providerFlightId);
    const nextFlightNumber = normalizedIdentity(nextInbound.flightNumber);
    const previousFlightNumber = normalizedIdentity(previousInbound.flightNumber);
    const assignmentChanged =
      (nextProviderID && previousProviderID && nextProviderID !== previousProviderID) ||
      (!nextProviderID && !previousProviderID && nextFlightNumber && previousFlightNumber && nextFlightNumber !== previousFlightNumber);

    if (assignmentChanged) return nextInbound;

    const merged = { ...previousInbound, ...nextInbound };
    for (const key of [
      "providerFlightId",
      "flightNumber",
      "originAirportIata",
      "destinationAirportIata",
      "estimatedArrival",
      "estimatedDeparture",
      "actualDeparture",
      "status",
      "providerAlertId",
      "providerAlertStatus",
      "providerAlertCreatedAt",
      "detailsLookupAttemptedAt",
    ]) {
      merged[key] = preferred(nextInbound[key], previousInbound[key]);
    }
    return merged;
  };

  const trackPoints = accumulatedBreadcrumbs(previous, normalized);
  return {
    ...normalized,
    trackPoints,
    gate: preferred(normalized?.gate, row?.gate ?? previous.gate),
    terminal: preferred(normalized?.terminal, row?.terminal ?? previous.terminal),
    departureGate: preferred(
      normalized?.departureGate,
      previous.departureGate ?? row?.gate
    ),
    departureTerminal: preferred(
      normalized?.departureTerminal,
      previous.departureTerminal ?? row?.terminal
    ),
    arrivalGate: preferred(normalized?.arrivalGate, previous.arrivalGate),
    arrivalTerminal: preferred(normalized?.arrivalTerminal, previous.arrivalTerminal),
    baggageBelt: preferred(
      normalized?.baggageBelt,
      row?.baggage_belt ?? previous.baggageBelt
    ),
    inboundFlight: mergeInboundFlight(normalized?.inboundFlight, previous.inboundFlight),
  };
}

function isOlderStreamEvent(currentEventAt, incomingEventAt) {
  const currentMs = new Date(currentEventAt || 0).getTime();
  const incomingMs = new Date(incomingEventAt || 0).getTime();
  return Number.isFinite(currentMs) && currentMs > 0 &&
    Number.isFinite(incomingMs) && incomingMs > 0 && incomingMs < currentMs;
}

// Shared flight-state design: client requests only touch Runwy-owned state.
// Provider calls are guarded by Redis-compatible locks, normalized, validated,
// snapshotted, diffed into shared events, then fanned out to private user links.
function createSharedFlightService({
  repository,
  provider,
  cache = createFlightCache(),
  queue = createSharedFlightQueue(),
  apns = createApnsSender(),
  liveActivities = null,
  stateProjection = null,
  weather = null,
  streamingEnabled = false,
  apiPollingEnabled = !streamingEnabled,
  apiActivePollMs = DEFAULT_API_ACTIVE_POLL_MS,
  apiPredeparturePollMs = DEFAULT_API_PREDEPARTURE_POLL_MS,
  apiPredepartureWindowMs = DEFAULT_API_PREDEPARTURE_WINDOW_MS,
  apiPostArrivalPollMs = DEFAULT_API_POST_ARRIVAL_POLL_MS,
  apiPostArrivalWindowMs = DEFAULT_API_POST_ARRIVAL_WINDOW_MS,
  wait = sleep,
} = {}) {
  const weatherService = weather || createFlightWeatherService({ cache, repository });
  const apiPollPolicy = {
    activePollMs: positiveMilliseconds(apiActivePollMs, DEFAULT_API_ACTIVE_POLL_MS),
    predeparturePollMs: positiveMilliseconds(apiPredeparturePollMs, DEFAULT_API_PREDEPARTURE_POLL_MS),
    predepartureWindowMs: positiveMilliseconds(apiPredepartureWindowMs, DEFAULT_API_PREDEPARTURE_WINDOW_MS),
    postArrivalPollMs: positiveMilliseconds(apiPostArrivalPollMs, DEFAULT_API_POST_ARRIVAL_POLL_MS),
    postArrivalWindowMs: positiveMilliseconds(apiPostArrivalWindowMs, DEFAULT_API_POST_ARRIVAL_WINDOW_MS),
  };

  async function acquireProviderCallLock(lockKey, ttlMs) {
    const localToken = await cache.acquireLock(lockKey, ttlMs);
    if (!localToken) return null;
    if (typeof repository.acquireProviderRequestLease !== "function") {
      return { lockKey, localToken, distributedToken: null };
    }

    try {
      const distributedToken = await repository.acquireProviderRequestLease(`provider:${lockKey}`, ttlMs);
      if (!distributedToken) {
        await cache.releaseLock(lockKey, localToken);
        return null;
      }
      return { lockKey, localToken, distributedToken };
    } catch (error) {
      await cache.releaseLock(lockKey, localToken);
      throw error;
    }
  }

  async function releaseProviderCallLock(lock) {
    if (!lock) return;
    try {
      if (lock.distributedToken && typeof repository.releaseProviderRequestLease === "function") {
        await repository.releaseProviderRequestLease(`provider:${lock.lockKey}`, lock.distributedToken);
      }
    } finally {
      await cache.releaseLock(lock.lockKey, lock.localToken);
    }
  }

  async function searchFlight(input, context = {}) {
    const params = normalizeSearchParams(input);
    const cacheKey = `flight:${params.flightKey}`;
    const cached = await cache.getJSON(cacheKey);
    if (cached) return { ...cached, source: "redis", freshness: cached.freshness || "fresh", isRefreshing: false };

    const existing = await repository.findFlightByKeyOrAlias(params.flightKey);
    if (existing) {
      const fresh = existing.fresh_until && new Date(existing.fresh_until).getTime() > Date.now();
      const deferPolling = shouldDeferProviderPolling(existing);
      const row = !fresh && deferPolling
        ? await repository.updateFlight(await extendFreshnessWithoutProviderCall(existing, "webhook_predeparture_defer"))
        : existing;
      const response = rowToFlightResponse(row, {
        source: "postgres",
        freshness: fresh || deferPolling ? "fresh" : "stale",
        isRefreshing: !fresh && !deferPolling,
      });
      await cache.setJSON(`flight:${row.flight_key}`, { ...response, source: "redis" }, await freshnessTTL(row));
      if (!fresh && !deferPolling) await enqueueRefresh(row, "stale_search");
      return response;
    }

    const lockKey = `fetch_lock:${params.flightKey}`;
    const lock = await acquireProviderCallLock(lockKey, FETCH_LOCK_MS);
    if (!lock) {
      await wait(150);
      const afterWait = (await cache.getJSON(cacheKey)) || rowToFlightResponse(await repository.findFlightByKeyOrAlias(params.flightKey), { source: "postgres", freshness: "fresh" });
      if (afterWait) return afterWait;
      return { status: "pending", message: "Flight lookup is in progress", flightKey: params.flightKey, freshness: "pending", isRefreshing: true };
    }

    const startedAt = Date.now();
    try {
      const normalized = await provider.fetchFlightByNumber(params);
      if (!normalized) {
        await repository.logApiUsage({ provider: provider.name, endpoint: "fetchFlightByNumber", flight_key: params.flightKey, user_id: context.userId, response_time_ms: Date.now() - startedAt, cache_status: "miss", error: "no_match" });
        const error = new Error("Unable to confidently match this flight.");
        error.statusCode = 404;
        error.details = { reason: "no_match", flightKey: params.flightKey };
        throw error;
      }
      const validation = validateProviderFlight(normalized, params);
      normalized.dataConfidence = validation.confidence;
      if (!validation.ok) {
        await repository.logApiUsage({ provider: provider.name, endpoint: "fetchFlightByNumber", flight_key: params.flightKey, user_id: context.userId, response_time_ms: Date.now() - startedAt, cache_status: "miss", error: validation.problems.join(",") });
        const error = new Error("Unable to confidently match this flight.");
        error.statusCode = 422;
        error.details = {
          reason: "validation_failed",
          flightKey: params.flightKey,
          problems: validation.problems,
          confidence: validation.confidence,
        };
        throw error;
      }
      const ttl = getFlightFreshnessTTL(normalized);
      const freshUntil = new Date(Date.now() + ttl * 1000).toISOString();
      const saved = await repository.upsertFlightFromNormalized(normalized, params, freshUntil);
      await repository.insertSnapshot(saved);
      const response = rowToFlightResponse(saved, { source: "provider", freshness: "fresh" });
      await cache.setJSON(`flight:${saved.flight_key}`, { ...response, source: "redis" }, ttl);
      await repository.logApiUsage({ provider: provider.name, endpoint: "fetchFlightByNumber", flight_key: saved.flight_key, user_id: context.userId, response_time_ms: Date.now() - startedAt, cache_status: "miss", status_code: 200 });
      return response;
    } finally {
      await releaseProviderCallLock(lock);
    }
  }

  async function enqueueRefresh(row, reason, options = {}) {
    const lockToken = await cache.acquireLock(`refresh_lock:${row.flight_key}`, REFRESH_LOCK_MS);
    if (!lockToken) {
      await repository.logApiUsage({ provider: row.provider || provider.name, endpoint: "refreshFlightJob", flight_key: row.flight_key, cache_status: "refresh_lock_busy", error: "refresh lock already held" });
      return null;
    }
    await cache.releaseLock(`refresh_lock:${row.flight_key}`, lockToken);
    return queue.add("refreshFlightJob", { flight_key: row.flight_key, flight_instance_id: row.id, reason }, { dedupe: true, dedupeKey: `refresh:${row.id}`, runImmediately: options.runImmediately });
  }

  async function enqueueStateConsumers(flight) {
    const version = flight.last_stream_event_at || flight.last_fetched_at || flight.updated_at || Date.now();
    if (stateProjection?.syncFlightState) {
      try {
        // Persist the canonical app snapshot before APNs can announce data the
        // app has not stored yet. Queue a retry only when the synchronous bridge
        // is temporarily unavailable.
        await stateProjection.syncFlightState(flight);
      } catch (_error) {
        await queue.add("legacyStateProjectionJob", { flight_instance_id: flight.id }, {
          dedupe: true,
          dedupeKey: `legacy-state:${flight.id}:${version}`,
        });
      }
    }
    if (liveActivities?.sendFlightState) {
      await queue.add("liveActivityUpdateJob", { flight_instance_id: flight.id }, {
        dedupe: true,
        dedupeKey: `live-activity:${flight.id}:${version}`,
      });
    }
  }

  async function refreshFlightJob(job) {
    const row = job.data.flight_instance_id
      ? await repository.findFlightById(job.data.flight_instance_id)
      : await repository.findFlightByKeyOrAlias(job.data.flight_key);
    if (!row || (row.is_final && job.data.reason !== "forced" && !isArrivalDetailsRefreshReason(job.data.reason))) return null;
    const lockKey = `refresh_lock:${row.flight_key}`;
    const lock = await acquireProviderCallLock(lockKey, REFRESH_LOCK_MS);
    if (!lock) return null;
    const startedAt = Date.now();
    try {
      const params = { airline: row.airline_code, number: row.flight_number, date: dateOnly(row.departure_date), origin: row.origin_airport || "UNKNOWN", destination: row.destination_airport || "UNKNOWN", flightKey: row.flight_key };
      const reason = String(job.data.reason || "");
      const usesTrackedFlightReserve =
        isOperationallyOverdueWithoutTakeoff(row) ||
        hasOperationalDepartureEvidence({
          status: row.status,
          actualDepartureAt: row.actual_departure_at,
          position: {
            lat: row.position_lat,
            altitude: row.altitude,
            groundSpeed: row.ground_speed,
          },
        });
      const providerOptions = {
        forceRefresh: reason === "forced" || reason.startsWith("provider_alert_position"),
        skipLivePosition: isOperationalDetailsRefreshReason(job.data.reason),
        ...(usesTrackedFlightReserve ? { budgetEndpoint: "tracked_flight" } : {}),
      };
      let providerNormalized = row.provider_flight_id && provider.supportsProviderId && provider.fetchFlightByProviderId
        ? await provider.fetchFlightByProviderId(row.provider_flight_id, providerOptions)
        : null;

      // FlightAware can publish the schedule and the eventual operating flight
      // under different provider IDs (for example 6E481 later operating with
      // callsign IGO23EC). Once the saved schedule is overdue, an exact lookup
      // can remain permanently scheduled even though a number/route lookup has
      // an airborne occurrence. Re-resolve only in that narrow state, and only
      // adopt a replacement that has operational departure evidence and still
      // passes the original flight identity validation below.
      const shouldResolveOperationalReplacement =
        !providerNormalized ||
        isOperationallyPastArrivalWithoutFinalState(row) ||
        (
          isOperationallyOverdueWithoutTakeoff(row) &&
          !hasOperationalDepartureEvidence(providerNormalized)
        );
      if (shouldResolveOperationalReplacement) {
        const resolvedByNumber = await provider.fetchFlightByNumber(params, {
          ...providerOptions,
          forceRefresh: true,
        });
        if (
          resolvedByNumber &&
          (
            !providerNormalized ||
            hasOperationalDepartureEvidence(resolvedByNumber)
          )
        ) {
          providerNormalized = resolvedByNumber;
        }
      }
      if (!providerNormalized) {
        await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: row.flight_key, response_time_ms: Date.now() - startedAt, error: "no_match" });
        return row;
      }
      const normalized = reconcileDiversionContext(
        preserveKnownOperationalFields(providerNormalized, row),
        params,
        row
      );
      const validation = validateProviderFlight(normalized, params, row);
      normalized.dataConfidence = validation.confidence;
      if (!validation.ok) {
        await repository.markSuspicious(row.id, validation.problems.join(","));
        await queue.add("revalidateSuspiciousFlightJob", { flight_instance_id: row.id, flight_key: row.flight_key }, { dedupe: true, dedupeKey: `revalidate:${row.id}`, runImmediately: false });
        return row;
      }
      const activeViewerCount = await getActiveViewerCount(row.id);
      const ttl = getFlightFreshnessTTL({ ...normalized, activeViewerCount }, Date.now(), Math.random, { activeViewerCount });
      const nextDb = {
        ...row,
        ...mapNormalizedToDb(normalized, params),
        id: row.id,
        provider_alert_id: row.provider_alert_id,
        provider_alert_status: row.provider_alert_status,
        provider_alert_created_at: row.provider_alert_created_at,
        provider_alert_expires_at: row.provider_alert_expires_at,
        last_webhook_received_at: row.last_webhook_received_at,
        fresh_until: new Date(Date.now() + ttl * 1000).toISOString(),
      };
      const events = compareFlightState(row, nextDb);
      const suspicious = events.find((event) => event.event_type === "PROVIDER_DATA_SUSPICIOUS");
      if (suspicious) {
        await repository.markSuspicious(row.id, suspicious.summary);
        await queue.add("revalidateSuspiciousFlightJob", { flight_instance_id: row.id, flight_key: row.flight_key }, { dedupe: true, dedupeKey: `revalidate:${row.id}`, runImmediately: false });
        return row;
      }
      const committed = repository.commitCanonicalState
        ? await repository.commitCanonicalState(nextDb, events, provider.name)
        : null;
      const saved = committed?.flight || await repository.updateFlight(nextDb);
      if (!committed && repository.insertSnapshot) {
        await repository.insertSnapshot(saved);
      }
      const savedEvents = committed
        ? committed.events
        : await repository.insertEvents(saved.id, events, provider.name);
      if (committed && committed.applied === false) {
        return saved;
      }
      await cache.setJSON(`flight:${saved.flight_key}`, rowToFlightResponse(saved, { source: "redis", freshness: "fresh" }), ttl);
      // Mirror the canonical update into the app-readable snapshot before a
      // notification for that same update can be delivered.
      await enqueueStateConsumers(saved);
      for (const event of savedEvents.filter((item) => item.notification_required)) {
        await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
      }
      // State, timing and position can change without creating a lifecycle event.
      // Keep every consumer on the canonical row instead of only fanning out when
      // compareFlightState happens to emit a notification event.
      await ensureInboundFlightMonitoring(saved.id, job.data.reason || "refresh");
      await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: saved.flight_key, response_time_ms: Date.now() - startedAt, status_code: 200 });
      return saved;
    } catch (error) {
      await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: row.flight_key, response_time_ms: Date.now() - startedAt, error: error?.message || String(error) });
      // A failed refresh of the outbound flight must not starve the separately
      // identified inbound aircraft. Inbound resolution has its own bounded
      // provider budget and is still useful when the main refresh is throttled.
      await ensureInboundFlightMonitoring(row.id, `${job.data.reason || "refresh"}_fallback`);
      throw error;
    } finally {
      await releaseProviderCallLock(lock);
    }
  }

  async function fanoutNotificationJob(job) {
    const data = await repository.getEventWithFlight(job.data.flight_event_id);
    if (!data?.event || !data?.flight) return { sent: 0 };
    const targets = await repository.listNotificationTargets(data.flight.id, data.event.event_severity, data.event.event_type);
    const isArrival = ["LANDED", "ARRIVED"].includes(data.event.event_type);
    const weatherInsight = isArrival
      ? await weatherService.insightForFlight(data.flight, { cacheStatus: "arrival_notification" })
      : null;
    let sent = 0;
    for (const target of targets) {
      const ownerUserId = target.userFlight.owner_user_id || target.userFlight.user_id;
      if (repository.isUserFlightNotificationActive) {
        const active = await repository.isUserFlightNotificationActive(ownerUserId, target.userFlight.id);
        if (!active) continue;
      }
      const delivery = await repository.createNotificationDelivery(
        target.userFlight.user_id,
        data.flight.id,
        data.event.id,
        "apns",
        target.userFlight.id || null
      );
      if (!delivery.created || !delivery.row) continue;
      const notificationContext = {
        isCircle: target.isCircle === true,
        isTraveler: target.isTraveler !== false,
        ownerDisplayName: target.ownerDisplayName || null,
        recipientDisplayName: target.recipientDisplayName || null,
        temperatureUnit: target.temperatureUnit || "celsius",
        weatherInsight: weatherInsight?.available ? weatherInsight : null,
        visitOrdinal: isArrival && target.isCircle !== true && target.isTraveler !== false && repository.arrivalVisitOrdinalForUser
          ? await repository.arrivalVisitOrdinalForUser(
            ownerUserId,
            data.flight.destination_airport,
            target.userFlight.id || null
          )
          : null,
      };
      try {
        if (repository.createAppNotification) {
          const payload = notificationPayload(data.flight, data.event, notificationContext);
          payload.user_flight_id = target.userFlight.id || null;
          payload.owner_user_id = target.userFlight.owner_user_id || target.userFlight.user_id || null;
          await repository.createAppNotification({
            userId: target.userFlight.user_id,
            flightInstanceId: data.flight.id,
            flightEventId: data.event.id,
            notificationType: sharedNotificationType(data.event.event_type),
            title: payload.aps.alert.title,
            body: payload.aps.alert.body,
            payload,
            deliveryStatus: "queued",
          });
        }
        if (repository.isUserFlightNotificationActive) {
          const active = await repository.isUserFlightNotificationActive(ownerUserId, target.userFlight.id);
          if (!active) {
            await repository.updateNotificationDelivery(delivery.row.id, {
              status: "failed",
              error: "user_flight_deleted_before_delivery",
            });
            continue;
          }
        }
        const results = [];
        for (const token of target.tokens) {
          const result = await apns.sendFlightEvent({
            token,
            flight: data.flight,
            event: data.event,
            context: notificationContext,
          });
          results.push({ token, result });
          if (isInvalidApnsTokenResult(result) && repository.disableDeviceToken) {
            await repository.disableDeviceToken(token.device_token || token.apnsToken);
          }
        }
        if (results.length === 0) {
          throw new Error("no_active_push_tokens");
        }
        // One account can retain tokens from both Xcode (sandbox) and
        // TestFlight/App Store (production). A stale or misconfigured token in
        // one environment must not turn a successful delivery to another
        // active device into a total failure.
        if (!results.some(({ result }) => result?.ok === true)) {
          throw new Error(results.find(({ result }) => result?.reason || result?.error)?.result?.reason || "APNs delivery failed");
        }
        if (repository.updateAppNotificationDeliveryStatus) {
          await repository.updateAppNotificationDeliveryStatus({
            userId: target.userFlight.user_id,
            flightInstanceId: data.flight.id,
            flightEventId: data.event.id,
            notificationType: sharedNotificationType(data.event.event_type),
            deliveryStatus: "sent",
            sentAt: new Date().toISOString(),
          });
        }
        await repository.updateNotificationDelivery(delivery.row.id, { status: "sent", sent_at: new Date().toISOString() });
        sent += 1;
      } catch (error) {
        if (repository.updateAppNotificationDeliveryStatus) {
          await repository.updateAppNotificationDeliveryStatus({
            userId: target.userFlight.user_id,
            flightInstanceId: data.flight.id,
            flightEventId: data.event.id,
            notificationType: sharedNotificationType(data.event.event_type),
            deliveryStatus: "failed",
          });
        }
        await repository.updateNotificationDelivery(delivery.row.id, { status: "failed", error: error?.message || String(error) });
      }
    }
    return { sent };
  }

  async function revalidateSuspiciousFlightJob(job) {
    await wait(250);
    const row = await repository.findFlightByKeyOrAlias(job.data.flight_key);
    if (!row) return null;
    return refreshFlightJob({ data: { flight_key: row.flight_key, flight_instance_id: row.id, reason: "forced" } });
  }

  async function saveUserFlight(userId, input) {
    const flight = await searchFlight(input, { userId });
    if (!flight.flightInstanceId) return { flight, userFlight: null };
    const userFlight = await repository.upsertUserFlight(userId, flight.flightInstanceId, input);
    await ensureLiveSource(flight.flightInstanceId, "user_saved");
    await scheduleLifecycleCatchups(flight.flightInstanceId, "user_saved");
    await scheduleApiPoll(flight.flightInstanceId, "user_saved");
    await scheduleWeatherInsight(flight.flightInstanceId, "user_saved");
    const updatedRow = await repository.findFlightById(flight.flightInstanceId);
    const updatedFlight = updatedRow
      ? rowToFlightResponse(updatedRow, {
          source: "postgres",
          freshness: updatedRow.fresh_until && new Date(updatedRow.fresh_until).getTime() > Date.now()
            ? "fresh"
            : "stale",
        })
      : flight;
    return { flight: updatedFlight, userFlight };
  }

  async function ensureUserFlightLiveCoverage(userId, input = {}) {
    const ids = Array.isArray(input.ids)
      ? input.ids.map((id) => String(id || "").trim()).filter(Boolean).slice(0, 50)
      : [];
    if (!ids.length || typeof repository.listUserFlightsByIds !== "function") {
      return { checked: 0, covered: 0, failed: [] };
    }

    const rows = await repository.listUserFlightsByIds(userId, ids);
    const failed = [];
    let covered = 0;

    for (const row of rows) {
      try {
        const flightInstanceId = row.flight_instance_id || await resolveFlightInstanceForUserFlight(userId, row);
        if (!flightInstanceId) {
          failed.push({ id: row.id, reason: "unresolved" });
          continue;
        }

        await ensureLiveSource(flightInstanceId, "user_flight_coverage");
        await scheduleLifecycleCatchups(flightInstanceId, "user_flight_coverage");
        await scheduleApiPoll(flightInstanceId, "user_flight_coverage");
        await scheduleWeatherInsight(flightInstanceId, "user_flight_coverage");
        covered += 1;
      } catch (error) {
        failed.push({ id: row.id, reason: error?.message || "failed" });
      }
    }

    return { checked: rows.length, covered, failed };
  }

  async function resolveFlightInstanceForUserFlight(userId, row) {
    const params = searchInputFromUserFlight(row);
    if (!params) return null;
    const flight = await searchFlight(params, { userId });
    if (!flight.flightInstanceId) return null;

    if (typeof repository.linkUserFlightToInstance === "function") {
      await repository.linkUserFlightToInstance(userId, row.id, flight.flightInstanceId, {
        notificationEnabled: row.notification_enabled ?? row.notifications_enabled ?? true,
        alertPreferences: row.alert_preferences || sharedAlertPreferencesFromLegacySettings(row.alert_settings_json),
      });
    }

    return flight.flightInstanceId;
  }

  async function deleteUserFlight(userId, id) {
    const existingRows = typeof repository.listUserFlightsByIds === "function"
      ? await repository.listUserFlightsByIds(userId, [id])
      : [];
    const existing = existingRows[0] || null;
    const deleted = await repository.deleteUserFlight(userId, id);
    if (deleted && typeof repository.removeNotificationArtifactsForUserFlight === "function") {
      await repository.removeNotificationArtifactsForUserFlight(userId, existing || deleted);
    }
    return deleted;
  }

  async function ensureLiveSource(flightInstanceId, reason) {
    let stream = null;
    if (streamingEnabled) {
      stream = await ensureStreamingRegistration(flightInstanceId, reason);
    }
    // A Firehose registration is a useful low-call live source, but it is not
    // proof that the worker is connected or that every lifecycle event will be
    // delivered. Keep one provider alert per shared flight as the inexpensive
    // delivery safety net; downstream event dedupe prevents double pushes.
    const alert = await ensureProviderAlert(flightInstanceId, reason);
    await ensureInboundFlightMonitoring(flightInstanceId, reason);
    return alert || stream;
  }

  async function ensureInboundFlightMonitoring(flightInstanceId, reason) {
    if (typeof provider.ensureInboundFlightAlert !== "function") return null;
    let flight = await repository.findFlightById(flightInstanceId);
    let inbound = flight?.normalized_data?.inboundFlight;
    const departureMs = new Date(flight?.estimated_departure_at || flight?.scheduled_departure_at || 0).getTime();
    const untilDepartureMs = departureMs - Date.now();
    const needsDetails = !inbound?.originAirportIata || !inbound?.estimatedArrival;
    if (
      !flight ||
      isFinalStatus(flight.status) ||
      !inbound?.providerFlightId ||
      (inbound.providerAlertStatus === "active" && !needsDetails) ||
      !Number.isFinite(departureMs) ||
      untilDepartureMs > INBOUND_MONITOR_WINDOW_MS ||
      untilDepartureMs < -30 * 60_000
    ) {
      return flight;
    }

    try {
      const lastLookupMs = new Date(inbound.detailsLookupAttemptedAt || 0).getTime();
      const mayRetryDetails = !Number.isFinite(lastLookupMs) || Date.now() - lastLookupMs >= 45 * 60_000;
      if (needsDetails && mayRetryDetails && provider.supportsProviderId && provider.fetchFlightByProviderId) {
        const resolved = await provider.fetchFlightByProviderId(inbound.providerFlightId, {
          skipLivePosition: true,
          budgetEndpoint: "inbound_flight_instance",
        });
        inbound = {
          ...inbound,
          flightNumber: resolved?.airlineCode && resolved?.flightNumber
            ? `${resolved.airlineCode}${resolved.flightNumber}`
            : inbound.flightNumber,
          originAirportIata: resolved?.origin || inbound.originAirportIata || null,
          destinationAirportIata: resolved?.destination || inbound.destinationAirportIata || flight.origin_airport || null,
          estimatedArrival: resolved?.estimatedArrivalAt || resolved?.scheduledArrivalAt || inbound.estimatedArrival || null,
          estimatedDeparture: resolved?.estimatedDepartureAt || resolved?.scheduledDepartureAt || inbound.estimatedDeparture || null,
          actualDeparture: resolved?.actualDepartureAt || inbound.actualDeparture || null,
          status: resolved?.status || inbound.status || null,
          detailsLookupAttemptedAt: new Date().toISOString(),
        };
        flight = await repository.updateFlight({
          ...flight,
          normalized_data: { ...(flight.normalized_data || {}), inboundFlight: inbound },
        });
        await repository.logApiUsage({
          provider: provider.name,
          endpoint: "fetchInboundFlightByProviderId",
          flight_key: flight.flight_key,
          cache_status: resolved ? "miss" : "no_match",
          status_code: resolved ? 200 : null,
        });
      }
      if (inbound.providerAlertStatus === "active") {
        await cache.setJSON(`flight:${flight.flight_key}`, rowToFlightResponse(flight, { source: "redis", freshness: "fresh" }), await freshnessTTL(flight));
        return flight;
      }
      const alert = await provider.ensureInboundFlightAlert(flight, inbound, { reason });
      if (!alert) return flight;
      const updated = {
        ...flight,
        normalized_data: {
          ...(flight.normalized_data || {}),
          inboundFlight: {
            ...inbound,
            providerAlertId: alert.providerAlertId || alert.id || null,
            providerAlertStatus: alert.status || "active",
            providerAlertCreatedAt: alert.createdAt || new Date().toISOString(),
          },
        },
      };
      const saved = await repository.updateFlight(updated);
      await cache.setJSON(`flight:${saved.flight_key}`, rowToFlightResponse(saved, { source: "redis", freshness: "fresh" }), await freshnessTTL(saved));
      return saved;
    } catch (error) {
      await repository.logApiUsage({
        provider: provider.name,
        endpoint: "ensureInboundFlightAlert",
        flight_key: flight.flight_key,
        cache_status: "inbound_alert_failed",
        error: error?.message || String(error),
      });
      return flight;
    }
  }

  async function ensureStreamingRegistration(flightInstanceId, reason) {
    const flight = await repository.findFlightById(flightInstanceId);
    if (!flight || isStreamingActive(flight) || isFinalStatus(flight.status)) return flight;
    try {
      const stream = typeof provider.ensureFlightStream === "function"
        ? await provider.ensureFlightStream(flight, { reason })
        : { status: "active", liveDataSource: "streaming", refreshPriority: "minimal" };
      const updated = await repository.updateStreamingState(flightInstanceId, {
        liveDataSource: "streaming",
        status: stream?.status || "active",
        registeredAt: stream?.registeredAt || new Date().toISOString(),
        refreshPriority: stream?.refreshPriority || "minimal",
      });
      if (updated?.streaming_status === "active") {
        const ttl = getFlightFreshnessTTL(updated);
        updated.fresh_until = new Date(Date.now() + ttl * 1000).toISOString();
        return repository.updateFlight(updated);
      }
      return updated;
    } catch (error) {
      await repository.logApiUsage({
        provider: provider.name,
        endpoint: "ensureFlightStream",
        flight_key: flight.flight_key,
        cache_status: "stream_registration_failed",
        error: error?.message || String(error),
      });
      return repository.updateStreamingState(flightInstanceId, { status: "failed", liveDataSource: "on_demand" });
    }
  }

  async function ensureProviderAlert(flightInstanceId, reason) {
    if (typeof provider.ensureFlightAlert !== "function") return null;
    const flight = await repository.findFlightById(flightInstanceId);
    if (!flight || (!needsProviderAlertConfigurationUpgrade(flight, provider) && flight.provider_alert_status === "active") || isFinalStatus(flight.status)) return flight;
    try {
      const alert = await provider.ensureFlightAlert(flight, { reason });
      if (!alert) return flight;
      const updated = await repository.updateProviderAlert(flightInstanceId, alert);
      if (updated?.provider_alert_status === "active") {
        const ttl = getFlightFreshnessTTL(updated);
        updated.fresh_until = new Date(Date.now() + ttl * 1000).toISOString();
        return repository.updateFlight(updated);
      }
      return updated;
    } catch (error) {
      await repository.logApiUsage({
        provider: provider.name,
        endpoint: "ensureFlightAlert",
        flight_key: flight.flight_key,
        cache_status: "provider_alert_failed",
        error: error?.message || String(error),
      });
      return repository.updateProviderAlert(flightInstanceId, { status: "failed" });
    }
  }

  async function applyStreamedFlightUpdate(flightInstanceId, normalized, options = {}) {
    const row = await repository.findFlightById(flightInstanceId);
    if (!row || !normalized) return null;
    const incomingEventAt = options.eventTime || new Date().toISOString();
    if (isOlderStreamEvent(row.last_stream_event_at, incomingEventAt)) {
      return row;
    }
    const liveDataSource = options.liveDataSource || "streaming";
    const streamingStatus = options.streamingStatus || (liveDataSource === "streaming" ? "active" : row.streaming_status || "disabled");
    const params = {
      airline: row.airline_code,
      number: row.flight_number,
      date: dateOnly(row.departure_date),
      origin: row.origin_airport || "UNKNOWN",
      destination: row.destination_airport || "UNKNOWN",
      flightKey: row.flight_key,
      liveDataSource,
      streamingStatus,
      existingRow: row,
    };
    normalized = reconcileDiversionContext(normalized, params, row);
    const validation = validateProviderFlight(normalized, params, row);
    normalized.dataConfidence = validation.confidence;
    if (!validation.ok) {
      await repository.markSuspicious(row.id, validation.problems.join(","));
      await queue.add("revalidateSuspiciousFlightJob", { flight_instance_id: row.id, flight_key: row.flight_key }, { dedupe: true, dedupeKey: `revalidate:${row.id}`, runImmediately: false });
      return row;
    }
    const ttl = getFlightFreshnessTTL({ ...normalized, liveDataSource, streamingStatus });
    const nextDb = {
      ...row,
      ...mapNormalizedToDb(normalized, params),
      id: row.id,
      live_data_source: liveDataSource,
      streaming_status: streamingStatus,
      stream_registered_at: row.stream_registered_at || new Date().toISOString(),
      last_stream_event_at: incomingEventAt,
      provider_alert_id: row.provider_alert_id,
      provider_alert_status: row.provider_alert_status,
      provider_alert_created_at: row.provider_alert_created_at,
      provider_alert_expires_at: row.provider_alert_expires_at,
      fresh_until: new Date(Date.now() + ttl * 1000).toISOString(),
    };
    const events = compareFlightState(row, nextDb);
    const suspicious = events.find((event) => event.event_type === "PROVIDER_DATA_SUSPICIOUS");
    if (suspicious) {
      await repository.markSuspicious(row.id, suspicious.summary);
      await queue.add("revalidateSuspiciousFlightJob", { flight_instance_id: row.id, flight_key: row.flight_key }, { dedupe: true, dedupeKey: `revalidate:${row.id}`, runImmediately: false });
      return row;
    }
    const committed = repository.commitCanonicalState
      ? await repository.commitCanonicalState(nextDb, events, provider.name)
      : null;
    const saved = committed?.flight || await repository.updateFlight(nextDb);
    // Another worker may have committed a newer stream message after our
    // initial read. The repository rejects the stale write atomically; do not
    // generate events or projections from the rejected candidate.
    if (isOlderStreamEvent(saved?.last_stream_event_at, incomingEventAt)) {
      return saved;
    }
    if (committed && committed.applied === false) {
      return saved;
    }
    await repository.updateStreamingState(saved.id, {
      status: streamingStatus,
      liveDataSource,
      lastStreamEventAt: nextDb.last_stream_event_at,
      refreshPriority: "minimal",
    });
    if (!committed && repository.insertSnapshot) {
      await repository.insertSnapshot(saved);
    }
    const savedEvents = committed
      ? committed.events
      : await repository.insertEvents(saved.id, events, provider.name);
    await cache.setJSON(`flight:${saved.flight_key}`, rowToFlightResponse(saved, { source: "redis", freshness: "fresh" }), ttl);
    await enqueueStateConsumers(saved);
    for (const event of savedEvents.filter((item) => item.notification_required)) {
      await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
    }
    await scheduleApiPoll(saved.id, options.source || "provider_update", saved);
    return saved;
  }

  async function legacyStateProjectionJob(job) {
    if (!stateProjection?.syncFlightState) return { synced: 0, skipped: true };
    const flight = await repository.findFlightById(job.data.flight_instance_id);
    if (!flight) return { synced: 0, skipped: true };
    return stateProjection.syncFlightState(flight);
  }

  async function liveActivityUpdateJob(job) {
    if (!liveActivities?.sendFlightState) return { sent: 0, skipped: true };
    const flight = await repository.findFlightById(job.data.flight_instance_id);
    if (!flight) return { sent: 0, skipped: true };
    return liveActivities.sendFlightState(flight);
  }

  async function processFlightAwareAlertWebhook(payload) {
    const rawEvents = extractFlightAwareAlertEvents(payload);
    let matchedFlights = 0;
    let appliedEvents = 0;
    let duplicateEvents = 0;
    let unknownEvents = 0;

    for (const rawEvent of rawEvents) {
      const alert = normalizeFlightAwareAlert(rawEvent);
      const dedupeKey = generateFlightAwareAlertDedupeKey(alert, rawEvent);
      const logInput = {
        flight_instance_id: null,
        flight_key: alert.flight_key || "UNKNOWN",
        fa_flight_id: alert.fa_flight_id,
        ident: alert.ident,
        event_type: alert.event_type,
        event_status: alert.event_status,
        event_time: alert.event_time,
        source: "flightaware",
        raw_payload: rawEvent,
        normalized_payload: alert,
        dedupe_key: dedupeKey,
      };

      const inserted = repository.insertFlightEventLog
        ? await repository.insertFlightEventLog(logInput)
        : { created: true };
      if (!inserted.created) {
        duplicateEvents += 1;
        continue;
      }
      if (alert.event_type === "unknown_flight_event") {
        unknownEvents += 1;
        continue;
      }

      const targets = repository.listStreamUpdateTargets
        ? await repository.listStreamUpdateTargets({
          providerFlightId: alert.fa_flight_id,
          flightNumber: alert.ident,
          departureDate: alert.departureDate,
        })
        : [];
      const exactTargets = targets.filter((target) => targetMatchesAlert(target, alert));
      matchedFlights += exactTargets.length;

      for (const target of exactTargets) {
        if (repository.insertFlightEventLog && inserted.row?.id) {
          await repository.insertFlightEventLog({
            ...logInput,
            flight_instance_id: target.id,
            dedupe_key: `${dedupeKey}:target:${target.id}`,
          });
        }
        if (alert.event_type === "flight_departure_soon") {
          const recent = await repository.findRecentEventByType?.(target.id, "TRIP_STARTING", 24 * 60 * 60_000);
          if (recent) continue;
          const [event] = await repository.insertEvents(target.id, [{
            event_type: "TRIP_STARTING",
            event_severity: "medium",
            old_value: null,
            new_value: { minutesUntilDeparture: alert.minutes_until_departure },
            summary: alert.human_readable_summary,
            notification_required: true,
            confidence: "high",
            provider_event_time: alert.event_time,
          }], "flightaware");
          if (event?.notification_required) {
            await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
          }
          appliedEvents += 1;
          continue;
        }
        if (alert.event_type === "flight_arrived" && !alert.actual_in) {
          await queue.add("refreshFlightJob", {
            flight_instance_id: target.id,
            reason: "forced",
            trigger: "unconfirmed_arrival_alert",
          }, {
            dedupe: true,
            dedupeKey: `arrival-verify:${target.id}:${alert.event_time || "unknown"}`,
            runImmediately: true,
          });
          continue;
        }
        const update = flightUpdateFromAlert(target, alert);
        const saved = await applyStreamedFlightUpdate(target.id, update, {
          eventTime: alert.event_time || new Date().toISOString(),
          liveDataSource: "provider_alert",
          streamingStatus: target.streaming_status || "disabled",
        });
        if (saved) {
          appliedEvents += 1;
          await scheduleArrivalDetailRefreshes(saved.id, "provider_alert");
          // OUT/OFF alerts are authoritative lifecycle signals but do not contain
          // coordinates. Seed the track immediately after wheels-off so the map
          // does not remain a scheduled straight line until a later app refresh.
          if (alert.event_type === "flight_departed") {
            await queue.add("refreshFlightJob", {
              flight_instance_id: saved.id,
              reason: "provider_alert_position_seed",
              trigger: "takeoff_alert",
            }, {
              dedupe: true,
              dedupeKey: `takeoff-position:${saved.id}:${alert.event_time || "unknown"}`,
              runImmediately: true,
            });
          }
        }
      }

      const inboundTargets = repository.listInboundUpdateTargets
        ? await repository.listInboundUpdateTargets({
          providerFlightId: alert.fa_flight_id,
          flightNumber: alert.ident,
        })
        : [];
      matchedFlights += inboundTargets.length;
      for (const parent of inboundTargets) {
        const inbound = parent.normalized_data?.inboundFlight || {};
        if (alert.fa_flight_id && inbound.providerFlightId && String(alert.fa_flight_id) !== String(inbound.providerFlightId)) {
          continue;
        }
        const eventType = alert.event_type === "flight_departed"
          ? "INBOUND_DEPARTED"
          : alert.event_type === "flight_cancelled"
            ? "INBOUND_CANCELLED"
            : alert.event_type === "flight_diverted"
              ? "INBOUND_DIVERTED"
              : null;
        if (!eventType) continue;
        const recent = await repository.findRecentEventByType?.(parent.id, eventType, 12 * 60 * 60_000);
        if (recent) continue;

        const nextInbound = {
          ...inbound,
          status: alert.event_type === "flight_departed"
            ? "airborne"
            : alert.event_type === "flight_cancelled" ? "cancelled" : "diverted",
          actualDeparture: alert.actual_out || inbound.actualDeparture || null,
          estimatedArrival: alert.estimated_in || inbound.estimatedArrival || null,
        };
        const updatedParent = await repository.updateFlight({
          ...parent,
          normalized_data: { ...(parent.normalized_data || {}), inboundFlight: nextInbound },
        });
        const [event] = await repository.insertEvents(parent.id, [{
          event_type: eventType,
          event_severity: eventType === "INBOUND_DEPARTED" ? "medium" : "high",
          old_value: { inboundFlight: inbound },
          new_value: { inboundFlight: nextInbound },
          summary: alert.human_readable_summary,
          notification_required: true,
          confidence: "high",
          provider_event_time: alert.event_time,
        }], "flightaware");
        await cache.setJSON(`flight:${updatedParent.flight_key}`, rowToFlightResponse(updatedParent, { source: "redis", freshness: "fresh" }), await freshnessTTL(updatedParent));
        if (event?.notification_required) {
          await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
        }
        appliedEvents += 1;
      }
    }

    return {
      ok: true,
      receivedEvents: rawEvents.length,
      matchedFlights,
      appliedEvents,
      duplicateEvents,
      unknownEvents,
    };
  }

  async function registerActiveViewer(userId, flightInstanceId) {
    const flight = await repository.findFlightById(flightInstanceId);
    if (!flight) return null;
    await cache.redis.set(`active_watchers:${flightInstanceId}:${userId}`, "1", { ex: ACTIVE_VIEWER_TTL_SECONDS });
    const activeViewerCount = await getActiveViewerCount(flightInstanceId);
    const ttl = getFlightFreshnessTTL(flight, Date.now(), Math.random, { activeViewerCount });
    if ((!flight.fresh_until || new Date(flight.fresh_until).getTime() <= Date.now()) && !shouldDeferProviderPolling(flight, Date.now(), { allowActiveViewerRefresh: true })) {
      await enqueueRefresh(flight, "active_viewer");
    }
    return {
      flightInstanceId,
      activeViewerTtlSeconds: ACTIVE_VIEWER_TTL_SECONDS,
      recommendedRefreshTtlSeconds: ttl,
    };
  }

  async function getActiveViewerCount(flightInstanceId) {
    const values = cache.redis.__values;
    if (!values || typeof values.entries !== "function") return 0;
    const prefix = `active_watchers:${flightInstanceId}:`;
    const now = Date.now();
    let count = 0;
    for (const [key, record] of values.entries()) {
      if (record?.expiresAt && record.expiresAt <= now) {
        values.delete(key);
        continue;
      }
      if (String(key).startsWith(prefix)) count += 1;
    }
    return count;
  }

  async function listUserFlights(userId) {
    const rows = await repository.listUserFlights(userId);
    return rows.map(({ userFlight, flight }) => ({ userFlight, flight: rowToFlightResponse(flight, { source: "postgres", freshness: new Date(flight.fresh_until).getTime() > Date.now() ? "fresh" : "stale" }) }));
  }

  async function getFlightWeatherInsight(flightInstanceId, options = {}) {
    const row = await repository.findFlightById(flightInstanceId);
    if (!row) return null;
    const insight = await weatherService.insightForFlight(row, options);
    return insight;
  }

  async function flightWithWeatherInsight(flightInstanceId, options = {}) {
    let row = await repository.findFlightById(flightInstanceId);
    if (!row) return null;
    if (isOperationallyOverdueWithoutTakeoff(row)) {
      const throttleKey = `detail_overdue_refresh:${row.id}`;
      const shouldRefresh = await cache.redis.set(throttleKey, "1", { nx: true, ex: 120 });
      if (shouldRefresh === "OK") {
        row = await refreshFlightJob({
          data: {
            flight_key: row.flight_key,
            flight_instance_id: row.id,
            reason: options.reason || "active_detail_overdue_departure",
          },
        }) || row;
      }
    }
    const response = rowToFlightResponse(row, {
      source: options.source || "postgres",
      freshness: row.fresh_until && new Date(row.fresh_until).getTime() > Date.now() ? "fresh" : "stale",
      isRefreshing: false,
    });
    response.weatherInsight = await getFlightWeatherInsight(flightInstanceId, options);
    return response;
  }

  async function scheduleWeatherInsight(flightInstanceId, reason) {
    const row = await repository.findFlightById(flightInstanceId);
    const target = weatherTargetForFlight(row);
    if (!row || !target) return null;
    const departureMs = new Date(row.estimated_departure_at || row.scheduled_departure_at || 0).getTime();
    const preferredMs = departureMs - 4.5 * 60 * 60_000;
    const delayMs = Math.max(0, preferredMs - Date.now());
    return queue.add("weatherInsightJob", { flight_instance_id: flightInstanceId, reason }, {
      dedupe: true,
      dedupeKey: `weather:${flightInstanceId}:${new Date(departureMs).toISOString().slice(0, 13)}`,
      delayMs,
    });
  }

  async function scheduleLifecycleCatchups(flightInstanceId, reason) {
    const row = await repository.findFlightById(flightInstanceId);
    if (!row || isEffectivelyFinal(row)) return [];
    const scheduled = [];
    const nowMs = Date.now();
    const departureMs = new Date(row.estimated_departure_at || row.scheduled_departure_at || 0).getTime();
    if (Number.isFinite(departureMs)) {
      if (!row.actual_departure_at && departureMs > nowMs) {
        scheduled.push(await queue.add("preflightReminderJob", {
          flight_instance_id: flightInstanceId,
          reason,
        }, {
          dedupe: true,
          dedupeKey: `preflight-reminder:t-5h:${flightInstanceId}`,
          delayMs: Math.max(0, departureMs - PREFLIGHT_REMINDER_BEFORE_MS - nowMs),
        }));
      }
      // A stream is the primary live source, but it is not a delivery guarantee.
      // Keep these bounded catch-ups as a safety net so one missed departure or
      // arrival event cannot leave a flight permanently stuck as scheduled.
      if (nowMs < departureMs + DEPARTURE_CATCHUP_AFTER_MS) {
        scheduled.push(await queue.add("departureCatchupJob", { flight_instance_id: flightInstanceId, reason, stage: "first" }, {
          dedupe: true,
          dedupeKey: `departure-catchup:first:${flightInstanceId}`,
          delayMs: departureMs + DEPARTURE_CATCHUP_AFTER_MS - nowMs,
        }));
      }
      if (nowMs < departureMs + DEPARTURE_CATCHUP_FINAL_AFTER_MS) {
        scheduled.push(await queue.add("departureCatchupJob", { flight_instance_id: flightInstanceId, reason, stage: "final" }, {
          dedupe: true,
          dedupeKey: `departure-catchup:final:${flightInstanceId}`,
          delayMs: departureMs + DEPARTURE_CATCHUP_FINAL_AFTER_MS - nowMs,
        }));
      }
      for (const checkpoint of INBOUND_MONITOR_CHECKPOINTS) {
        const scheduledAtMs = departureMs + checkpoint.offsetMs;
        if (scheduledAtMs <= nowMs) continue;
        scheduled.push(await queue.add("inboundMonitoringJob", {
          flight_instance_id: flightInstanceId,
          reason,
          stage: checkpoint.stage,
        }, {
          dedupe: true,
          dedupeKey: `inbound-monitor:${checkpoint.stage}:${flightInstanceId}`,
          delayMs: scheduledAtMs - nowMs,
        }));
      }
      scheduled.push(...await scheduleDepartureDetailRefreshes(flightInstanceId, reason, row));
    }

    const arrivalMs = new Date(row.estimated_arrival_at || row.scheduled_arrival_at || 0).getTime();
    if (Number.isFinite(arrivalMs) && nowMs < arrivalMs + ARRIVAL_CATCHUP_AFTER_MS) {
      scheduled.push(await queue.add("arrivalCatchupJob", { flight_instance_id: flightInstanceId, reason }, {
        dedupe: true,
        dedupeKey: `arrival-catchup:${flightInstanceId}`,
        delayMs: arrivalMs + ARRIVAL_CATCHUP_AFTER_MS - nowMs,
      }));
      scheduled.push(...await scheduleArrivalDetailRefreshes(flightInstanceId, reason, row));
    }
    return scheduled;
  }

  async function scheduleApiPoll(flightInstanceId, reason, existingRow = null) {
    if (!apiPollingEnabled || streamingEnabled) return null;
    const row = existingRow || await repository.findFlightById(flightInstanceId);
    if (!row || isEffectivelyFinal(row)) return null;
    if (typeof repository.hasActiveUserFlights === "function" && !await repository.hasActiveUserFlights(flightInstanceId)) {
      return null;
    }

    const delayMs = apiPollDelayMs(row, Date.now(), apiPollPolicy);
    if (!Number.isFinite(delayMs)) return null;
    return queue.add("apiFlightPollJob", {
      flight_instance_id: flightInstanceId,
      reason,
    }, {
      dedupe: true,
      dedupeKey: `api-poll:${flightInstanceId}`,
      delayMs,
    });
  }

  async function apiFlightPollJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isEffectivelyFinal(row)) return row;
    if (typeof repository.hasActiveUserFlights === "function" && !await repository.hasActiveUserFlights(row.id)) {
      return row;
    }

    let refreshed = row;
    try {
      refreshed = await refreshFlightJob({
        data: {
          flight_key: row.flight_key,
          flight_instance_id: row.id,
          reason: `api_poll_${job.data.reason || "scheduled"}`,
        },
      }) || row;
      return refreshed;
    } finally {
      // The queue holds this flight's dedupe key until the handler returns.
      // Defer recurrence one event-loop turn so the next bounded timer can be
      // registered without allowing overlapping provider calls.
      const nextFlightId = refreshed?.id || row.id;
      const continuation = setImmediate(() => {
        scheduleApiPoll(nextFlightId, "continuation").catch((error) => {
          console.warn("Unable to schedule shared API flight poll", {
            flightInstanceId: nextFlightId,
            error: error?.message || String(error),
          });
        });
      });
      if (typeof continuation.unref === "function") continuation.unref();
    }
  }

  async function preflightReminderJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status) || row.actual_departure_at) return null;

    const departureAt = row.estimated_departure_at || row.scheduled_departure_at;
    const departureMs = new Date(departureAt || 0).getTime();
    const minutesUntilDeparture = Math.round((departureMs - Date.now()) / 60_000);
    if (!Number.isFinite(departureMs) || minutesUntilDeparture <= 0 || minutesUntilDeparture > 330) {
      return null;
    }

    const recent = await repository.findRecentEventByType?.(row.id, "TRIP_STARTING", 24 * 60 * 60_000);
    if (recent) return recent;

    const weatherInsight = await getFlightWeatherInsight(row.id, { cacheStatus: "preflight_reminder" });
    const embeddedWeather = weatherInsight?.available ? {
      airportCode: weatherInsight.airportCode || row.origin_airport || null,
      conditionCode: weatherInsight.conditionCode || null,
      temperatureC: weatherInsight.temperatureC ?? null,
      forecastTime: weatherInsight.forecastTime || departureAt,
    } : null;

    const [event] = await repository.insertEvents(row.id, [{
      event_type: "TRIP_STARTING",
      event_severity: "low",
      old_value: null,
      new_value: {
        minutesUntilDeparture,
        scheduledDepartureAt: row.scheduled_departure_at || departureAt,
        estimatedDepartureAt: row.estimated_departure_at || null,
        weatherInsight: embeddedWeather,
      },
      summary: "Flight scheduled to depart in about five hours",
      notification_required: true,
      confidence: "high",
      provider_event_time: new Date().toISOString(),
    }], "runwy");

    if (event?.notification_required) {
      await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, {
        dedupe: true,
        dedupeKey: `fanout:${event.id}`,
      });
    }
    return event || null;
  }

  async function recoverLifecycleCatchups(reason = "lifecycle_recovery") {
    if (typeof repository.listLifecycleRecoveryCandidates !== "function") {
      return { checked: 0, scheduled: 0 };
    }
    const rows = await repository.listLifecycleRecoveryCandidates();
    let scheduled = 0;
    for (const row of rows) {
      const jobs = await scheduleLifecycleCatchups(row.id, reason);
      scheduled += jobs.filter((job) => !job?.deduped).length;
      const apiPoll = await scheduleApiPoll(row.id, reason, row);
      if (apiPoll && !apiPoll.deduped) scheduled += 1;
      // Timers are intentionally in-process and disappear during a deploy.
      // If both bounded departure checks elapsed while this process was down,
      // recover with one shared refresh. last_fetched_at limits retries during
      // a long taxi or provider outage, independent of the number of users.
      if (shouldRecoverMissedDeparture(row)) {
        const recovery = await queue.add("departureCatchupJob", {
          flight_instance_id: row.id,
          reason,
          stage: "restart_recovery",
        }, {
          dedupe: true,
          dedupeKey: `departure-catchup:restart-recovery:${row.id}`,
        });
        if (!recovery?.deduped) scheduled += 1;
      }
      if (!isProviderAlertActive(row) || needsProviderAlertConfigurationUpgrade(row, provider)) {
        await ensureLiveSource(row.id, `${reason}_alert_repair`);
      }
    }
    const recoveredFanout = await recoverPendingNotificationFanout();
    return { checked: rows.length, scheduled, recoveredFanout };
  }

  async function recoverPendingNotificationFanout() {
    if (typeof repository.listPendingNotificationEventIds !== "function") return 0;
    const eventIds = await repository.listPendingNotificationEventIds();
    let recovered = 0;
    for (const flightEventId of eventIds) {
      const result = await fanoutNotificationJob({ data: { flight_event_id: flightEventId } });
      if (result?.sent > 0) recovered += result.sent;
    }
    return recovered;
  }

  async function scheduleArrivalDetailRefreshes(flightInstanceId, reason, existingRow = null) {
    const row = existingRow || await repository.findFlightById(flightInstanceId);
    // The stream is authoritative for movement, but its position/status events
    // often omit gate, terminal, and baggage details. Keep the low-frequency
    // full-provider checkpoints active so those operational fields can fill in.
    if (!row || !hasIncompleteArrivalDetails(row)) return [];
    const arrivalMs = new Date(row.estimated_arrival_at || row.scheduled_arrival_at || 0).getTime();
    if (!Number.isFinite(arrivalMs)) return [];

    const nowMs = Date.now();
    const scheduled = [];
    for (const checkpoint of ARRIVAL_DETAIL_CHECKPOINTS) {
      if (!needsArrivalDetailsForStage(row, checkpoint.stage)) continue;
      const scheduledAtMs = arrivalMs + checkpoint.offsetMs;
      if (scheduledAtMs <= nowMs) continue;
      scheduled.push(await queue.add("arrivalDetailRefreshJob", {
        flight_instance_id: flightInstanceId,
        reason,
        stage: checkpoint.stage,
      }, {
        dedupe: true,
        dedupeKey: `arrival-detail:${checkpoint.stage}:${flightInstanceId}`,
        delayMs: Math.max(0, scheduledAtMs - nowMs),
      }));
    }
    return scheduled;
  }

  async function scheduleDepartureDetailRefreshes(flightInstanceId, reason, existingRow = null) {
    const row = existingRow || await repository.findFlightById(flightInstanceId);
    // Alert and streaming payloads are optimized for state changes and may not
    // include terminal or gate assignments. Use a bounded set of full-provider
    // refreshes as departure approaches so early null values do not remain TBA.
    if (!row || !hasIncompleteDepartureDetails(row)) return [];
    const departureMs = new Date(row.estimated_departure_at || row.scheduled_departure_at || 0).getTime();
    if (!Number.isFinite(departureMs)) return [];

    const nowMs = Date.now();
    const scheduled = [];
    for (const checkpoint of DEPARTURE_DETAIL_CHECKPOINTS) {
      const scheduledAtMs = departureMs + checkpoint.offsetMs;
      if (scheduledAtMs <= nowMs) continue;
      scheduled.push(await queue.add("departureDetailRefreshJob", {
        flight_instance_id: flightInstanceId,
        reason,
        stage: checkpoint.stage,
      }, {
        dedupe: true,
        dedupeKey: `departure-detail:${checkpoint.stage}:${flightInstanceId}`,
        delayMs: Math.max(0, scheduledAtMs - nowMs),
      }));
    }
    return scheduled;
  }

  async function departureCatchupJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status) || !isOperationallyOverdueWithoutTakeoff(row)) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: `departure_catchup_${job.data.stage || "first"}`,
      },
    });
  }

  async function inboundMonitoringJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status) || row.normalized_data?.inboundFlight?.providerAlertStatus === "active") return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: `inbound_monitoring_${job.data.stage || "scheduled"}`,
      },
    });
  }

  async function departureDetailRefreshJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || !shouldRefreshDepartureDetails(row)) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: `departure_details_${job.data.stage || "scheduled"}`,
      },
    });
  }

  async function arrivalCatchupJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status)) return row;
    const status = String(row.status || "").toLowerCase();
    const shouldCheckArrival =
      ["departed", "airborne", "enroute", "taxi_in"].includes(status) ||
      Boolean(row.actual_departure_at) ||
      isOperationallyPastArrivalWithoutFinalState(row);
    if (!shouldCheckArrival) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: "arrival_catchup",
      },
    });
  }

  async function arrivalDetailRefreshJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || !shouldRefreshArrivalDetails(row, Date.now(), job.data.stage)) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: `arrival_details_${job.data.stage || "scheduled"}`,
      },
    });
  }

  async function weatherInsightJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status)) return null;
    const insight = await getFlightWeatherInsight(row.id, { cacheStatus: "scheduled" });
    const event = weatherEventFromInsight(insight);
    if (!event) return insight;
    const recentPreflight = await repository.findRecentEventByType?.(row.id, "TRIP_STARTING", 6 * 60 * 60_000);
    if (recentPreflight?.new_value?.weatherInsight) return insight;
    const recent = await repository.findRecentEventByType?.(row.id, "WEATHER_ADVISORY", 6 * 60 * 60_000);
    if (recent) return insight;
    const [savedEvent] = await repository.insertEvents(row.id, [event], insight.provider || "weatherkit");
    if (savedEvent?.notification_required) {
      await queue.add("fanoutNotificationJob", { flight_event_id: savedEvent.id }, { dedupe: true, dedupeKey: `fanout:${savedEvent.id}` });
    }
    return insight;
  }

  queue.process("refreshFlightJob", refreshFlightJob);
  queue.process("fanoutNotificationJob", fanoutNotificationJob);
  queue.process("liveActivityUpdateJob", liveActivityUpdateJob);
  queue.process("legacyStateProjectionJob", legacyStateProjectionJob);
  queue.process("revalidateSuspiciousFlightJob", revalidateSuspiciousFlightJob);
  queue.process("weatherInsightJob", weatherInsightJob);
  queue.process("departureDetailRefreshJob", departureDetailRefreshJob);
  queue.process("departureCatchupJob", departureCatchupJob);
  queue.process("inboundMonitoringJob", inboundMonitoringJob);
  queue.process("arrivalCatchupJob", arrivalCatchupJob);
  queue.process("arrivalDetailRefreshJob", arrivalDetailRefreshJob);
  queue.process("preflightReminderJob", preflightReminderJob);
  queue.process("apiFlightPollJob", apiFlightPollJob);

  return {
    searchFlight,
    saveUserFlight,
    ensureUserFlightLiveCoverage,
    deleteUserFlight,
    listUserFlights,
    updateUserFlight: repository.updateUserFlight,
    upsertDeviceToken: repository.upsertDeviceToken,
    registerActiveViewer,
    getFlightWeatherInsight,
    flightWithWeatherInsight,
    scheduleWeatherInsight,
    scheduleLifecycleCatchups,
    scheduleApiPoll,
    apiFlightPollJob,
    preflightReminderJob,
    recoverLifecycleCatchups,
    recoverPendingNotificationFanout,
    ensureLiveSource,
    ensureInboundFlightMonitoring,
    ensureStreamingRegistration,
    ensureProviderAlert,
    applyStreamedFlightUpdate,
    processFlightAwareAlertWebhook,
    enqueueRefresh,
    refreshFlightJob,
    fanoutNotificationJob,
    revalidateSuspiciousFlightJob,
    weatherInsightJob,
    departureDetailRefreshJob,
    departureCatchupJob,
    inboundMonitoringJob,
    arrivalCatchupJob,
    arrivalDetailRefreshJob,
    scheduleDepartureDetailRefreshes,
    scheduleArrivalDetailRefreshes,
    queue,
    cache,
    repository,
  };
}

async function freshnessTTL(row) {
  return getFlightFreshnessTTL(row);
}

async function extendFreshnessWithoutProviderCall(row, reason) {
  const ttl = getFlightFreshnessTTL(row);
  return {
    ...row,
    fresh_until: new Date(Date.now() + ttl * 1000).toISOString(),
    last_poll_reason: reason,
  };
}

function shouldDeferProviderPolling(row, nowMs = Date.now(), options = {}) {
  if ((!isProviderAlertActive(row, nowMs) && !isStreamingActive(row)) || row.needs_revalidation || isFinalStatus(row.status)) return false;
  if (options.allowActiveViewerRefresh && isOperationallyOverdueWithoutTakeoff(row, nowMs)) return false;
  const confidence = String(row.data_confidence || "").toLowerCase();
  if (confidence === "low" || confidence === "suspicious") return false;
  if (options.allowActiveViewerRefresh && isAirborne(row.status)) return false;
  return true;
}

function positiveMilliseconds(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function apiPollDelayMs(row, nowMs = Date.now(), policy = {}) {
  if (!row || isFinalStatus(row.status)) return null;

  const activePollMs = positiveMilliseconds(policy.activePollMs, DEFAULT_API_ACTIVE_POLL_MS);
  const predeparturePollMs = positiveMilliseconds(policy.predeparturePollMs, DEFAULT_API_PREDEPARTURE_POLL_MS);
  const predepartureWindowMs = positiveMilliseconds(policy.predepartureWindowMs, DEFAULT_API_PREDEPARTURE_WINDOW_MS);
  const postArrivalPollMs = positiveMilliseconds(policy.postArrivalPollMs, DEFAULT_API_POST_ARRIVAL_POLL_MS);
  const postArrivalWindowMs = positiveMilliseconds(policy.postArrivalWindowMs, DEFAULT_API_POST_ARRIVAL_WINDOW_MS);
  const status = String(row.status || "").trim().toLowerCase();
  const activeStatuses = new Set([
    "taxiing",
    "taxi_out",
    "takeoff_roll",
    "departed",
    "airborne",
    "enroute",
    "climb",
    "cruise",
    "descent",
    "approaching",
    "taxi_in",
    "diverted",
  ]);
  const arrivalMs = Date.parse(row.estimated_arrival_at || row.scheduled_arrival_at || "");
  const departureMs = Date.parse(row.estimated_departure_at || row.scheduled_departure_at || "");
  const hasDeparted = Boolean(row.actual_departure_at) || activeStatuses.has(status);

  if (hasDeparted) {
    if (Number.isFinite(arrivalMs) && nowMs > arrivalMs + 45 * 60_000) {
      return nowMs <= arrivalMs + postArrivalWindowMs ? postArrivalPollMs : null;
    }
    return activePollMs;
  }

  if (!Number.isFinite(departureMs)) return null;
  const untilDepartureMs = departureMs - nowMs;
  if (untilDepartureMs > predepartureWindowMs) {
    const delayMs = untilDepartureMs - predepartureWindowMs;
    return delayMs <= MAX_API_POLL_SCHEDULE_AHEAD_MS
      ? Math.max(activePollMs, delayMs)
      : null;
  }
  if (untilDepartureMs >= 0) return predeparturePollMs;

  // Poll quickly through a plausible taxi/delay window, then stop. Provider
  // alerts and the periodic recovery pass can restart the loop if new evidence
  // arrives, without allowing a stale scheduled row to spend indefinitely.
  return nowMs <= departureMs + MISSED_DEPARTURE_RECOVERY_WINDOW_MS ? activePollMs : null;
}

function isOperationallyOverdueWithoutTakeoff(row, nowMs = Date.now()) {
  const status = String(row?.status || "").toLowerCase();
  if (["departed", "airborne", "enroute", "taxi_in", "landed", "arrived", "arrived_at_gate", "cancelled", "diverted"].includes(status)) {
    return false;
  }
  const normalized = row?.normalized_data || {};
  const actualTakeoff = normalized?.takeoffTimes?.actual || normalized?.actualTakeoffAt || null;
  const hasAirbornePosition =
    row?.position_lat != null ||
    normalized?.position?.lat != null ||
    normalized?.livePosition?.latitude != null;
  if (actualTakeoff || hasAirbornePosition) return false;
  const departureMs = new Date(row?.estimated_departure_at || row?.scheduled_departure_at || "").getTime();
  return Number.isFinite(departureMs) && nowMs - departureMs >= 2 * 60_000;
}

function hasOperationalDepartureEvidence(normalized) {
  const status = String(normalized?.status || "").toLowerCase();
  if ([
    "taxiing",
    "taxi_out",
    "takeoff_roll",
    "departed",
    "airborne",
    "enroute",
    "climb",
    "cruise",
    "descent",
    "approaching",
    "taxi_in",
    "landed",
    "arrived",
    "arrived_at_gate",
  ].includes(status)) {
    return true;
  }
  if (normalized?.actualDepartureAt || normalized?.departureTimes?.actual || normalized?.takeoffTimes?.actual) {
    return true;
  }

  const position = normalized?.position || normalized?.livePosition || {};
  const altitude = Number(position.altitude ?? position.altitudeFeet);
  const groundSpeed = Number(position.groundSpeed ?? position.groundSpeedKnots);
  return (Number.isFinite(altitude) && altitude > 300) ||
    (Number.isFinite(altitude) && altitude > 150 && Number.isFinite(groundSpeed) && groundSpeed > 80);
}

function shouldRecoverMissedDeparture(row, nowMs = Date.now()) {
  if (!isOperationallyOverdueWithoutTakeoff(row, nowMs)) return false;
  const departureMs = new Date(row?.estimated_departure_at || row?.scheduled_departure_at || "").getTime();
  if (!Number.isFinite(departureMs) || nowMs > departureMs + MISSED_DEPARTURE_RECOVERY_WINDOW_MS) return false;
  const lastFetchedMs = new Date(row?.last_fetched_at || row?.updated_at || "").getTime();
  return !Number.isFinite(lastFetchedMs) || nowMs - lastFetchedMs >= MISSED_DEPARTURE_RECOVERY_MIN_STALE_MS;
}

function isEffectivelyFinal(row) {
  // Status reconciliation already validates actual-arrival evidence against
  // contradictory live telemetry. An isolated timestamp must not suppress the
  // recovery jobs that can correct a premature provider update.
  return isFinalStatus(row?.status);
}

function isOperationallyPastArrivalWithoutFinalState(row, nowMs = Date.now()) {
  if (isFinalStatus(row?.status)) return false;
  const arrivalMs = new Date(row?.estimated_arrival_at || row?.scheduled_arrival_at || "").getTime();
  return Number.isFinite(arrivalMs) && nowMs >= arrivalMs + ARRIVAL_CATCHUP_AFTER_MS;
}

function isAirborne(status) {
  return ["airborne", "enroute", "departed"].includes(String(status || "").toLowerCase());
}

function shouldRefreshDepartureDetails(row, nowMs = Date.now()) {
  const status = String(row?.status || "").toLowerCase();
  if (["departed", "airborne", "enroute", "taxi_in", "landed", "arrived", "arrived_at_gate", "cancelled", "diverted"].includes(status)) {
    return false;
  }
  if (row?.actual_departure_at) return false;

  const departureMs = new Date(row?.estimated_departure_at || row?.scheduled_departure_at || "").getTime();
  if (!Number.isFinite(departureMs)) return false;
  if (nowMs < departureMs - 4.25 * 60 * 60_000) return false;
  if (nowMs > departureMs + 10 * 60_000) return false;
  return hasIncompleteDepartureDetails(row);
}

function hasIncompleteDepartureDetails(row) {
  const normalized = row?.normalized_data || {};
  const departureGate = row?.gate || normalized.departureGate || normalized.departure_gate || normalized.gateOrigin || normalized.gate || null;
  const departureTerminal = row?.terminal || normalized.departureTerminal || normalized.departure_terminal || normalized.terminalOrigin || normalized.terminal || null;
  return !departureGate || !departureTerminal;
}

function shouldRefreshArrivalDetails(row, nowMs = Date.now(), stage = "scheduled") {
  const status = String(row?.status || "").toLowerCase();
  if (status === "cancelled") return false;

  const arrivalMs = new Date(row?.estimated_arrival_at || row?.scheduled_arrival_at || "").getTime();
  if (!Number.isFinite(arrivalMs)) return false;
  if (nowMs < arrivalMs - 4.25 * 60 * 60_000) return false;
  if (nowMs > arrivalMs + 90 * 60_000) return false;
  return needsArrivalDetailsForStage(row, stage);
}

function hasIncompleteArrivalDetails(row) {
  return hasIncompleteArrivalGateDetails(row) || !arrivalBaggageBelt(row);
}

function hasIncompleteArrivalGateDetails(row) {
  const normalized = row.normalized_data || {};
  const arrivalGate = normalized.arrivalGate || normalized.arrival_gate || normalized.gateDestination || null;
  const arrivalTerminal = normalized.arrivalTerminal || normalized.arrival_terminal || normalized.terminalDestination || null;
  return !arrivalGate || !arrivalTerminal;
}

function arrivalBaggageBelt(row) {
  const normalized = row.normalized_data || {};
  return row.baggage_belt || normalized.baggageBelt || normalized.baggageClaim || normalized.baggage_belt || null;
}

function needsArrivalDetailsForStage(row, stage) {
  return String(stage || "").startsWith("post-")
    ? hasIncompleteArrivalDetails(row)
    : hasIncompleteArrivalGateDetails(row);
}

function isArrivalDetailsRefreshReason(reason) {
  return String(reason || "").startsWith("arrival_details_");
}

function needsProviderAlertConfigurationUpgrade(row, provider) {
  if (row?.provider_alert_status !== "active" || !row?.provider_alert_id) return false;
  const changedAt = Date.parse(provider?.alertConfigurationChangedAt || "");
  if (!Number.isFinite(changedAt)) return false;
  const configuredAt = Date.parse(row.provider_alert_created_at || "");
  return !Number.isFinite(configuredAt) || configuredAt < changedAt;
}

function isOperationalDetailsRefreshReason(reason) {
  const value = String(reason || "");
  return value.startsWith("arrival_details_") || value.startsWith("departure_details_") || value.startsWith("inbound_monitoring_");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isInvalidApnsTokenResult(result) {
  const reason = String(result?.reason || result?.error || "").trim();
  return ["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"].includes(reason);
}

function sharedNotificationType(eventType) {
  switch (String(eventType || "").toUpperCase()) {
    case "DELAYED":
    case "RESCHEDULED":
      return "flight_delayed";
    case "CANCELLED":
      return "flight_cancelled";
    case "DIVERTED":
      return "flight_diverted";
    case "AIRCRAFT_CHANGED":
      return "flight_aircraft_changed";
    case "GATE_CHANGED":
      return "flight_gate_change";
    case "TAXIING":
    case "TAXI_IN":
      return "flight_taxiing";
    case "TAKEOFF_ROLL":
      return "flight_takeoff_roll";
    case "TRIP_STARTING":
      return "flight_trip_starting";
    case "INBOUND_DEPARTED":
      return "flight_inbound_departed";
    case "INBOUND_CANCELLED":
      return "flight_inbound_cancelled";
    case "INBOUND_DIVERTED":
      return "flight_inbound_diverted";
    case "DEPARTED":
    case "AIRBORNE":
      return "flight_departed";
    case "LANDED":
    case "ARRIVED":
    case "ARRIVED_AT_GATE":
      return "flight_arrived";
    case "BAGGAGE_BELT_ASSIGNED":
      return "flight_baggage_claim";
    case "WEATHER_ADVISORY":
      return "flight_weather_advisory";
    default:
      return "flight_status";
  }
}

function dateOnly(value) {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value || "").slice(0, 10);
}

function searchInputFromUserFlight(row = {}) {
  const flightNumber = String(row.display_flight_number || "").trim().toUpperCase();
  const match = flightNumber.replace(/\s+/g, "").match(/^([A-Z0-9]{2})(\d{1,4}[A-Z]?)$/);
  if (!match) return null;

  const date = dateOnly(row.scheduled_departure || row.estimated_departure || row.actual_departure);
  if (!date) return null;

  return {
    airline: match[1],
    number: match[2],
    date,
    origin: row.origin_iata || row.origin_airport || row.departure_iata,
    destination: row.destination_iata || row.destination_airport || row.arrival_iata,
    notificationEnabled: row.notification_enabled ?? row.notifications_enabled ?? true,
    alertPreferences: row.alert_preferences || sharedAlertPreferencesFromLegacySettings(row.alert_settings_json),
    sourceType: row.source_type || "user_flight_coverage",
    lifecycleState: row.lifecycle_state || null,
  };
}

function sharedAlertPreferencesFromLegacySettings(settings = {}) {
  const enabled = settings && typeof settings === "object" ? settings : {};
  return {
    low: Boolean(enabled.boardingTime || enabled.takeoffLanding || enabled.baggageClaim),
    medium: enabled.gateChange !== false || enabled.delayUpdates !== false || enabled.takeoffLanding !== false,
    high: enabled.delayUpdates !== false || enabled.takeoffLanding !== false,
    critical: true,
  };
}

module.exports = { createSharedFlightService, preserveKnownOperationalFields, isOlderStreamEvent };
