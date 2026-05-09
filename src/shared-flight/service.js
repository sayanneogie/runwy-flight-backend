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
  rowToFlightResponse,
  validateProviderFlight,
} = require("./state");
const { createSharedFlightQueue } = require("./queue");
const { createApnsSender } = require("./notifications");
const { createFlightWeatherService, weatherEventFromInsight, weatherTargetForFlight } = require("./weather");
const {
  extractFlightAwareAlertEvents,
  flightUpdateFromAlert,
  generateFlightAwareAlertDedupeKey,
  normalizeFlightAwareAlert,
  targetMatchesAlert,
} = require("../flightaware-alerts");

const FETCH_LOCK_MS = 8_000;
const REFRESH_LOCK_MS = 20_000;
const ACTIVE_VIEWER_TTL_SECONDS = 90;
const DEPARTURE_CATCHUP_AFTER_MS = 2 * 60_000;
const DEPARTURE_CATCHUP_FINAL_AFTER_MS = 17 * 60_000;
const ARRIVAL_CATCHUP_AFTER_MS = 6 * 60_000;

// Shared flight-state design: client requests only touch Runwy-owned state.
// Provider calls are guarded by Redis-compatible locks, normalized, validated,
// snapshotted, diffed into shared events, then fanned out to private user links.
function createSharedFlightService({ repository, provider, cache = createFlightCache(), queue = createSharedFlightQueue(), apns = createApnsSender(), weather = null, streamingEnabled = false, wait = sleep } = {}) {
  const weatherService = weather || createFlightWeatherService({ cache, repository });

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
    const token = await cache.acquireLock(lockKey, FETCH_LOCK_MS);
    if (!token) {
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
        throw error;
      }
      const validation = validateProviderFlight(normalized, params);
      normalized.dataConfidence = validation.confidence;
      if (!validation.ok) {
        await repository.logApiUsage({ provider: provider.name, endpoint: "fetchFlightByNumber", flight_key: params.flightKey, user_id: context.userId, response_time_ms: Date.now() - startedAt, cache_status: "miss", error: validation.problems.join(",") });
        const error = new Error("Unable to confidently match this flight.");
        error.statusCode = 422;
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
      await cache.releaseLock(lockKey, token);
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

  async function refreshFlightJob(job) {
    const row = job.data.flight_instance_id
      ? await repository.findFlightById(job.data.flight_instance_id)
      : await repository.findFlightByKeyOrAlias(job.data.flight_key);
    if (!row || (row.is_final && job.data.reason !== "forced")) return null;
    const token = await cache.acquireLock(`refresh_lock:${row.flight_key}`, REFRESH_LOCK_MS);
    if (!token) return null;
    const startedAt = Date.now();
    try {
      const params = { airline: row.airline_code, number: row.flight_number, date: dateOnly(row.departure_date), origin: row.origin_airport || "UNKNOWN", destination: row.destination_airport || "UNKNOWN", flightKey: row.flight_key };
      const normalized = row.provider_flight_id && provider.supportsProviderId && provider.fetchFlightByProviderId
        ? await provider.fetchFlightByProviderId(row.provider_flight_id)
        : await provider.fetchFlightByNumber(params);
      if (!normalized) {
        await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: row.flight_key, response_time_ms: Date.now() - startedAt, error: "no_match" });
        return row;
      }
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
      const saved = await repository.updateFlight(nextDb);
      await repository.insertSnapshot(saved);
      const savedEvents = await repository.insertEvents(saved.id, events, provider.name);
      await cache.setJSON(`flight:${saved.flight_key}`, rowToFlightResponse(saved, { source: "redis", freshness: "fresh" }), ttl);
      for (const event of savedEvents.filter((item) => item.notification_required)) {
        await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
      }
      await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: saved.flight_key, response_time_ms: Date.now() - startedAt, status_code: 200 });
      return saved;
    } catch (error) {
      await repository.logApiUsage({ provider: provider.name, endpoint: "refreshFlightJob", flight_key: row.flight_key, response_time_ms: Date.now() - startedAt, error: error?.message || String(error) });
      throw error;
    } finally {
      await cache.releaseLock(`refresh_lock:${row.flight_key}`, token);
    }
  }

  async function fanoutNotificationJob(job) {
    const data = await repository.getEventWithFlight(job.data.flight_event_id);
    if (!data?.event || !data?.flight) return { sent: 0 };
    const targets = await repository.listNotificationTargets(data.flight.id, data.event.event_severity);
    let sent = 0;
    for (const target of targets) {
      const delivery = await repository.createNotificationDelivery(target.userFlight.user_id, data.flight.id, data.event.id, "apns");
      if (!delivery.created || !delivery.row) continue;
      try {
        const results = [];
        for (const token of target.tokens) {
          const result = await apns.sendFlightEvent({ token, flight: data.flight, event: data.event });
          results.push({ token, result });
          if (isInvalidApnsTokenResult(result) && repository.disableDeviceToken) {
            await repository.disableDeviceToken(token.device_token || token.apnsToken);
          }
        }
        if (results.some(({ result }) => result?.ok === false)) {
          throw new Error(results.find(({ result }) => result?.reason || result?.error)?.result?.reason || "APNs delivery failed");
        }
        await repository.updateNotificationDelivery(delivery.row.id, { status: "sent", sent_at: new Date().toISOString() });
        sent += 1;
      } catch (error) {
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
    await ensureLiveSource(flight.flightInstanceId, "user_saved");
    await scheduleLifecycleCatchups(flight.flightInstanceId, "user_saved");
    await scheduleWeatherInsight(flight.flightInstanceId, "user_saved");
    const userFlight = await repository.upsertUserFlight(userId, flight.flightInstanceId, input);
    return { flight, userFlight };
  }

  async function ensureLiveSource(flightInstanceId, reason) {
    if (streamingEnabled) {
      const stream = await ensureStreamingRegistration(flightInstanceId, reason);
      if (stream?.streaming_status === "active") return stream;
    }
    return ensureProviderAlert(flightInstanceId, reason);
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
    if (!flight || flight.provider_alert_status === "active" || isFinalStatus(flight.status)) return flight;
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
    };
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
      last_stream_event_at: options.eventTime || new Date().toISOString(),
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
    const saved = await repository.updateFlight(nextDb);
    await repository.updateStreamingState(saved.id, {
      status: streamingStatus,
      liveDataSource,
      lastStreamEventAt: nextDb.last_stream_event_at,
      refreshPriority: "minimal",
    });
    await repository.insertSnapshot(saved);
    const savedEvents = await repository.insertEvents(saved.id, events, provider.name);
    await cache.setJSON(`flight:${saved.flight_key}`, rowToFlightResponse(saved, { source: "redis", freshness: "fresh" }), ttl);
    for (const event of savedEvents.filter((item) => item.notification_required)) {
      await queue.add("fanoutNotificationJob", { flight_event_id: event.id }, { dedupe: true, dedupeKey: `fanout:${event.id}` });
    }
    return saved;
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
        const update = flightUpdateFromAlert(target, alert);
        const saved = await applyStreamedFlightUpdate(target.id, update, {
          eventTime: alert.event_time || new Date().toISOString(),
          liveDataSource: "provider_alert",
          streamingStatus: target.streaming_status || "disabled",
        });
        if (saved) appliedEvents += 1;
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
    if (!row || isFinalStatus(row.status) || isStreamingActive(row)) return [];
    const scheduled = [];
    const departureMs = new Date(row.estimated_departure_at || row.scheduled_departure_at || 0).getTime();
    if (Number.isFinite(departureMs)) {
      scheduled.push(await queue.add("departureCatchupJob", { flight_instance_id: flightInstanceId, reason, stage: "first" }, {
        dedupe: true,
        dedupeKey: `departure-catchup:first:${flightInstanceId}`,
        delayMs: Math.max(0, departureMs + DEPARTURE_CATCHUP_AFTER_MS - Date.now()),
      }));
      scheduled.push(await queue.add("departureCatchupJob", { flight_instance_id: flightInstanceId, reason, stage: "final" }, {
        dedupe: true,
        dedupeKey: `departure-catchup:final:${flightInstanceId}`,
        delayMs: Math.max(0, departureMs + DEPARTURE_CATCHUP_FINAL_AFTER_MS - Date.now()),
      }));
    }

    const arrivalMs = new Date(row.estimated_arrival_at || row.scheduled_arrival_at || 0).getTime();
    if (Number.isFinite(arrivalMs)) {
      scheduled.push(await queue.add("arrivalCatchupJob", { flight_instance_id: flightInstanceId, reason }, {
        dedupe: true,
        dedupeKey: `arrival-catchup:${flightInstanceId}`,
        delayMs: Math.max(0, arrivalMs + ARRIVAL_CATCHUP_AFTER_MS - Date.now()),
      }));
    }
    return scheduled;
  }

  async function departureCatchupJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status) || isStreamingActive(row) || !isOperationallyOverdueWithoutTakeoff(row)) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: `departure_catchup_${job.data.stage || "first"}`,
      },
    });
  }

  async function arrivalCatchupJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status) || isStreamingActive(row)) return row;
    const status = String(row.status || "").toLowerCase();
    const shouldCheckArrival = ["departed", "airborne", "enroute", "taxi_in"].includes(status) || Boolean(row.actual_departure_at);
    if (!shouldCheckArrival) return row;
    return refreshFlightJob({
      data: {
        flight_key: row.flight_key,
        flight_instance_id: row.id,
        reason: "arrival_catchup",
      },
    });
  }

  async function weatherInsightJob(job) {
    const row = await repository.findFlightById(job.data.flight_instance_id);
    if (!row || isFinalStatus(row.status)) return null;
    const insight = await getFlightWeatherInsight(row.id, { cacheStatus: "scheduled" });
    const event = weatherEventFromInsight(insight);
    if (!event) return insight;
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
  queue.process("revalidateSuspiciousFlightJob", revalidateSuspiciousFlightJob);
  queue.process("weatherInsightJob", weatherInsightJob);
  queue.process("departureCatchupJob", departureCatchupJob);
  queue.process("arrivalCatchupJob", arrivalCatchupJob);

  return {
    searchFlight,
    saveUserFlight,
    listUserFlights,
    updateUserFlight: repository.updateUserFlight,
    upsertDeviceToken: repository.upsertDeviceToken,
    registerActiveViewer,
    getFlightWeatherInsight,
    flightWithWeatherInsight,
    scheduleWeatherInsight,
    scheduleLifecycleCatchups,
    ensureLiveSource,
    ensureStreamingRegistration,
    ensureProviderAlert,
    applyStreamedFlightUpdate,
    processFlightAwareAlertWebhook,
    enqueueRefresh,
    refreshFlightJob,
    fanoutNotificationJob,
    revalidateSuspiciousFlightJob,
    weatherInsightJob,
    departureCatchupJob,
    arrivalCatchupJob,
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

function isOperationallyOverdueWithoutTakeoff(row, nowMs = Date.now()) {
  const status = String(row?.status || "").toLowerCase();
  if (["departed", "airborne", "enroute", "taxi_in", "landed", "arrived", "arrived_at_gate", "cancelled", "diverted"].includes(status)) {
    return false;
  }
  if (row?.actual_departure_at) return false;
  const departureMs = new Date(row?.estimated_departure_at || row?.scheduled_departure_at || "").getTime();
  return Number.isFinite(departureMs) && nowMs - departureMs >= 2 * 60_000;
}

function isAirborne(status) {
  return ["airborne", "enroute", "departed"].includes(String(status || "").toLowerCase());
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isInvalidApnsTokenResult(result) {
  const reason = String(result?.reason || result?.error || "").trim();
  return ["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"].includes(reason);
}

function dateOnly(value) {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value || "").slice(0, 10);
}

module.exports = { createSharedFlightService };
