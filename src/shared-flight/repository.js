"use strict";

const { mapNormalizedToDb } = require("./state");

const DEFAULT_ALERT_PREFERENCES = Object.freeze({
  low: true,
  medium: true,
  high: true,
  critical: true,
});

function createMemorySharedFlightRepository() {
  const flights = new Map();
  const aliases = new Map();
  const userFlights = new Map();
  const events = new Map();
  const flightEventLogs = new Map();
  const appNotifications = new Map();
  const deliveries = new Map();
  const deviceTokens = new Map();
  const apiLogs = [];
  let idCounter = 0;
  const nextId = () => `00000000-0000-4000-8000-${String(++idCounter).padStart(12, "0")}`;

  return {
    async findFlightByKeyOrAlias(flightKey) {
      const aliasId = aliases.get(flightKey);
      if (aliasId) return [...flights.values()].find((row) => row.id === aliasId) || null;
      return flights.get(flightKey) || null;
    },
    async findFlightById(id) {
      return [...flights.values()].find((row) => row.id === id) || null;
    },
    async listStreamUpdateTargets({ providerFlightId, flightNumber, departureDate }) {
      return [...flights.values()].filter((row) => {
        if (providerFlightId && row.provider_flight_id === providerFlightId) return true;
        return (
          flightNumber &&
          row.airline_code + row.flight_number === String(flightNumber).toUpperCase() &&
          (!departureDate || String(row.departure_date || "").slice(0, 10) === departureDate)
        );
      });
    },
    async listLifecycleRecoveryCandidates() {
      const now = Date.now();
      const earliest = now - 24 * 60 * 60_000;
      const latest = now + 72 * 60 * 60_000;
      return [...flights.values()].filter((row) => {
        if (row.is_final) return false;
        const departure = Date.parse(row.estimated_departure_at || row.scheduled_departure_at || "");
        const arrival = Date.parse(row.estimated_arrival_at || row.scheduled_arrival_at || "");
        return (Number.isFinite(departure) && departure >= earliest && departure <= latest) ||
          (Number.isFinite(arrival) && arrival >= earliest && arrival <= latest);
      });
    },
    async upsertFlightFromNormalized(normalized, params, freshUntil) {
      const row = { ...mapNormalizedToDb(normalized, params), fresh_until: freshUntil };
      const existing = flights.get(row.flight_key);
      const saved = { ...(existing || {}), ...row, id: existing?.id || nextId(), updated_at: new Date().toISOString(), created_at: existing?.created_at || new Date().toISOString() };
      saved.provider_alert_status = saved.provider_alert_status || "unavailable";
      saved.live_data_source = saved.live_data_source || "on_demand";
      saved.streaming_status = saved.streaming_status || "disabled";
      saved.refresh_priority = saved.refresh_priority || "normal";
      flights.set(saved.flight_key, saved);
      if (params.flightKey && params.flightKey !== saved.flight_key) aliases.set(params.flightKey, saved.id);
      return saved;
    },
    async updateFlight(row) {
      const saved = { ...row, updated_at: new Date().toISOString() };
      flights.set(saved.flight_key, saved);
      return saved;
    },
    async updateProviderAlert(flightInstanceId, alert) {
      const row = [...flights.values()].find((item) => item.id === flightInstanceId);
      if (!row) return null;
      row.provider_alert_id = alert.providerAlertId || alert.id || null;
      row.provider_alert_status = alert.status || "unavailable";
      row.provider_alert_created_at = alert.createdAt || new Date().toISOString();
      row.provider_alert_expires_at = alert.expiresAt || null;
      row.refresh_priority = alert.refreshPriority || row.refresh_priority || "normal";
      row.updated_at = new Date().toISOString();
      return row;
    },
    async updateStreamingState(flightInstanceId, stream) {
      const row = [...flights.values()].find((item) => item.id === flightInstanceId);
      if (!row) return null;
      row.live_data_source = stream.liveDataSource || (stream.status === "active" ? "streaming" : row.live_data_source || "on_demand");
      row.streaming_status = stream.status || "disabled";
      row.stream_registered_at = stream.registeredAt || new Date().toISOString();
      row.last_stream_event_at = stream.lastStreamEventAt || row.last_stream_event_at || null;
      row.refresh_priority = stream.refreshPriority || row.refresh_priority || "minimal";
      row.updated_at = new Date().toISOString();
      return row;
    },
    async insertSnapshot(_row) {},
    async insertEvents(flightInstanceId, eventRows, provider) {
      return eventRows.map((event) => {
        const saved = { id: nextId(), flight_instance_id: flightInstanceId, provider, created_at: new Date().toISOString(), ...event };
        events.set(saved.id, saved);
        return saved;
      });
    },
    async insertFlightEventLog(entry) {
      if (flightEventLogs.has(entry.dedupe_key)) {
        return { row: flightEventLogs.get(entry.dedupe_key), created: false };
      }
      const row = { id: nextId(), source: "flightaware", created_at: new Date().toISOString(), ...entry };
      flightEventLogs.set(row.dedupe_key, row);
      return { row, created: true };
    },
    async findRecentEventByType(flightInstanceId, eventType, withinMs) {
      const cutoff = Date.now() - withinMs;
      return [...events.values()].find((event) =>
        event.flight_instance_id === flightInstanceId &&
        event.event_type === eventType &&
        Date.parse(event.created_at) >= cutoff
      ) || null;
    },
    async markSuspicious(flightInstanceId, reason) {
      const row = [...flights.values()].find((item) => item.id === flightInstanceId);
      if (row) {
        row.needs_revalidation = true;
        row.data_confidence = "suspicious";
      }
      const event = { id: nextId(), flight_instance_id: flightInstanceId, event_type: "PROVIDER_DATA_SUSPICIOUS", event_severity: "high", summary: reason, notification_required: false, created_at: new Date().toISOString() };
      events.set(event.id, event);
      return event;
    },
    async upsertUserFlight(userId, flightInstanceId, input = {}) {
      const key = `${userId}:${flightInstanceId}`;
      const flight = [...flights.values()].find((row) => row.id === flightInstanceId) || {};
      const displayFlightNumber = [
        flight.airline_code,
        flight.flight_number,
      ].filter(Boolean).join(" ").trim() || input.flightNumber || input.number || "Flight";
      const scheduledDeparture =
        flight.scheduled_departure_at ||
        flight.estimated_departure_at ||
        flight.actual_departure_at ||
        input.scheduledDeparture ||
        input.date ||
        new Date().toISOString();
      const saved = {
        id: userFlights.get(key)?.id || nextId(),
        user_id: userId,
        flight_instance_id: flightInstanceId,
        notification_enabled: input.notificationEnabled ?? true,
        alert_preferences: input.alertPreferences || DEFAULT_ALERT_PREFERENCES,
        user_label: input.userLabel || null,
        visibility: input.visibility || "private",
        source_type: input.sourceType || "manual_search",
        lifecycle_state: input.lifecycleState || "upcoming",
        display_flight_number: displayFlightNumber,
        marketing_airline_code: flight.airline_code || input.airline || null,
        operating_airline_code: flight.airline_code || input.airline || null,
        origin_iata: flight.origin_airport || input.origin || "UNKNOWN",
        destination_iata: flight.destination_airport || input.destination || "UNKNOWN",
        scheduled_departure: scheduledDeparture,
        scheduled_arrival: flight.scheduled_arrival_at || null,
        estimated_departure: flight.estimated_departure_at || null,
        estimated_arrival: flight.estimated_arrival_at || null,
        actual_departure: flight.actual_departure_at || null,
        actual_arrival: flight.actual_arrival_at || null,
        departure_terminal: flight.terminal || null,
        departure_gate: flight.gate || null,
        status: flight.status || null,
        provider_name: flight.provider || null,
        provider_flight_id: flight.provider_flight_id || null,
        notifications_enabled: input.notificationEnabled ?? true,
        alert_settings_json: input.alertPreferences || DEFAULT_ALERT_PREFERENCES,
        added_at: userFlights.get(key)?.added_at || new Date().toISOString(),
      };
      userFlights.set(key, saved);
      return saved;
    },
    async listUserFlights(userId) {
      return [...userFlights.values()]
        .filter((row) => row.user_id === userId && !row.deleted_at && row.lifecycle_state !== "deleted")
        .map((userFlight) => ({ userFlight, flight: [...flights.values()].find((flight) => flight.id === userFlight.flight_instance_id) || null }))
        .filter((item) => item.flight);
    },
    async updateUserFlight(userId, id, patch) {
      const entry = [...userFlights.entries()].find(([, row]) => row.user_id === userId && row.id === id);
      if (!entry) return null;
      const [key, row] = entry;
      const saved = { ...row, ...patch };
      userFlights.set(key, saved);
      return saved;
    },
    async listUserFlightsByIds(userId, ids) {
      const wanted = new Set((ids || []).map((id) => String(id)));
      return [...userFlights.values()]
        .filter((row) =>
          row.user_id === userId &&
          wanted.has(String(row.id)) &&
          !row.deleted_at &&
          row.lifecycle_state !== "deleted"
        );
    },
    async linkUserFlightToInstance(userId, id, flightInstanceId, patch = {}) {
      const entry = [...userFlights.entries()].find(([, row]) => row.user_id === userId && row.id === id);
      if (!entry) return null;
      const [key, row] = entry;
      const flight = [...flights.values()].find((item) => item.id === flightInstanceId) || {};
      const saved = {
        ...row,
        flight_instance_id: flightInstanceId,
        notification_enabled: patch.notificationEnabled ?? row.notification_enabled ?? row.notifications_enabled ?? true,
        notifications_enabled: patch.notificationEnabled ?? row.notifications_enabled ?? row.notification_enabled ?? true,
        alert_preferences: patch.alertPreferences || row.alert_preferences || DEFAULT_ALERT_PREFERENCES,
        status: flight.status || row.status || null,
        provider_name: flight.provider || row.provider_name || null,
        provider_flight_id: flight.provider_flight_id || row.provider_flight_id || null,
        updated_at: new Date().toISOString(),
      };
      userFlights.set(key, saved);
      return saved;
    },
    async deleteUserFlight(userId, id) {
      const entry = [...userFlights.entries()].find(([, row]) => row.user_id === userId && row.id === id);
      if (!entry) return null;
      const [key, row] = entry;
      const saved = {
        ...row,
        notification_enabled: false,
        notifications_enabled: false,
        lifecycle_state: "deleted",
        deleted_at: row.deleted_at || new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      userFlights.set(key, saved);
      return saved;
    },
    async upsertDeviceToken(userId, input) {
      const key = `${userId}:${input.deviceToken}`;
      const saved = { id: deviceTokens.get(key)?.id || nextId(), user_id: userId, device_token: input.deviceToken, platform: input.platform || "ios", environment: input.environment, is_active: true, updated_at: new Date().toISOString() };
      deviceTokens.set(key, saved);
      return saved;
    },
    async disableDeviceToken(deviceToken) {
      for (const [key, token] of deviceTokens.entries()) {
        if (token.device_token === deviceToken) {
          deviceTokens.set(key, { ...token, is_active: false, updated_at: new Date().toISOString() });
        }
      }
    },
    async getEventWithFlight(eventId) {
      const event = events.get(eventId);
      if (!event) return null;
      const flight = [...flights.values()].find((row) => row.id === event.flight_instance_id);
      return { event, flight };
    },
    async listNotificationTargets(flightInstanceId, severity, _eventType) {
      return [...userFlights.values()]
        .filter((row) =>
          row.flight_instance_id === flightInstanceId &&
          row.notification_enabled !== false &&
          row.alert_preferences?.[severity] !== false &&
          (_eventType !== "TRIP_STARTING" || row.source_type !== "tracked")
        )
        .map((userFlight) => ({
          userFlight,
          isCircle: false,
          isTraveler: userFlight.source_type !== "tracked",
          ownerDisplayName: null,
          tokens: [...deviceTokens.values()].filter((token) => token.user_id === userFlight.user_id && token.is_active),
        }));
    },
    async createNotificationDelivery(userId, flightInstanceId, eventId, channel = "apns") {
      const key = `${userId}:${eventId}:${channel}`;
      if (deliveries.has(key)) return { row: deliveries.get(key), created: false };
      const row = { id: nextId(), user_id: userId, flight_instance_id: flightInstanceId, flight_event_id: eventId, channel, status: "pending", created_at: new Date().toISOString() };
      deliveries.set(key, row);
      return { row, created: true };
    },
    async createAppNotification(input) {
      const key = `${input.userId}:${input.flightEventId}:${input.notificationType}`;
      const existing = appNotifications.get(key);
      if (existing) return { row: existing, created: false };
      const row = {
        id: nextId(),
        user_id: input.userId,
        flight_instance_id: input.flightInstanceId,
        flight_event_id: input.flightEventId,
        notification_type: input.notificationType,
        delivery_channel: "push",
        delivery_status: input.deliveryStatus || "queued",
        title: input.title,
        body: input.body,
        payload_json: input.payload || {},
        scheduled_for: new Date().toISOString(),
        created_at: new Date().toISOString(),
      };
      appNotifications.set(key, row);
      return { row, created: true };
    },
    async updateAppNotificationDeliveryStatus(input) {
      const key = `${input.userId}:${input.flightEventId}:${input.notificationType}`;
      const row = appNotifications.get(key);
      if (!row) return null;
      const updated = {
        ...row,
        delivery_status: input.deliveryStatus || row.delivery_status,
        sent_at: input.sentAt || row.sent_at || null,
        updated_at: new Date().toISOString(),
      };
      appNotifications.set(key, updated);
      return updated;
    },
    async updateNotificationDelivery(id, patch) {
      for (const [key, row] of deliveries.entries()) {
        if (row.id === id) deliveries.set(key, { ...row, ...patch });
      }
    },
    async logApiUsage(entry) {
      apiLogs.push({ id: nextId(), created_at: new Date().toISOString(), ...entry });
    },
    __memory: { flights, aliases, userFlights, events, flightEventLogs, appNotifications, deliveries, deviceTokens, apiLogs },
  };
}

function createPostgresSharedFlightRepository(pool) {
  const one = (result) => result.rows[0] || null;
  return {
    async findFlightByKeyOrAlias(flightKey) {
      return one(await pool.query(
        `
        select fi.*
        from public.flight_instances fi
        where fi.flight_key = $1
        union all
        select fi.*
        from public.flight_instance_aliases fia
        join public.flight_instances fi on fi.id = fia.flight_instance_id
        where fia.alias_key = $1
        limit 1
        `,
        [flightKey]
      ));
    },
    async findFlightById(id) {
      return one(await pool.query(`select * from public.flight_instances where id = $1`, [id]));
    },
    async listStreamUpdateTargets({ providerFlightId, flightNumber, departureDate }) {
      const normalizedFlightNumber = String(flightNumber || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
      const result = await pool.query(
        `select *
         from public.flight_instances
         where ($1::text is not null and provider_flight_id = $1)
            or (
              $2::text <> ''
              and (airline_code || flight_number) = $2
              and ($3::date is null or departure_date = $3::date)
            )
         order by last_stream_event_at desc nulls last, updated_at desc
         limit 20`,
        [providerFlightId || null, normalizedFlightNumber, departureDate || null]
      );
      return result.rows;
    },
    async listLifecycleRecoveryCandidates() {
      const result = await pool.query(
        `select distinct fi.*
         from public.flight_instances fi
         join public.user_flights uf on uf.flight_instance_id = fi.id
         where fi.is_final = false
           and uf.deleted_at is null
           and coalesce(uf.lifecycle_state, '') <> 'deleted'
           and (
             coalesce(fi.estimated_departure_at, fi.scheduled_departure_at)
               between now() - interval '24 hours' and now() + interval '72 hours'
             or coalesce(fi.estimated_arrival_at, fi.scheduled_arrival_at)
               between now() - interval '24 hours' and now() + interval '72 hours'
           )
         order by fi.updated_at asc
         limit 1000`
      );
      return result.rows;
    },
    async upsertFlightFromNormalized(normalized, params, freshUntil) {
      const row = { ...mapNormalizedToDb(normalized, params), fresh_until: freshUntil };
      const saved = one(await pool.query(
        `
        insert into public.flight_instances (
          flight_key, provider_flight_id, airline_code, flight_number, departure_date,
          origin_airport, destination_airport, scheduled_departure_at, scheduled_arrival_at,
          estimated_departure_at, estimated_arrival_at, actual_departure_at, actual_arrival_at,
          status, status_detail, gate, terminal, baggage_belt, position_lat, position_lon,
          altitude, ground_speed, heading, provider, data_confidence, normalized_data,
          raw_provider_response, last_fetched_at, fresh_until, needs_revalidation, is_final
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31
        )
        on conflict (flight_key) do update set
          provider_flight_id = excluded.provider_flight_id,
          scheduled_departure_at = excluded.scheduled_departure_at,
          scheduled_arrival_at = excluded.scheduled_arrival_at,
          estimated_departure_at = excluded.estimated_departure_at,
          estimated_arrival_at = excluded.estimated_arrival_at,
          actual_departure_at = excluded.actual_departure_at,
          actual_arrival_at = excluded.actual_arrival_at,
          status = excluded.status,
          status_detail = excluded.status_detail,
          gate = excluded.gate,
          terminal = excluded.terminal,
          baggage_belt = excluded.baggage_belt,
          position_lat = excluded.position_lat,
          position_lon = excluded.position_lon,
          altitude = excluded.altitude,
          ground_speed = excluded.ground_speed,
          heading = excluded.heading,
          provider = excluded.provider,
          data_confidence = excluded.data_confidence,
          normalized_data = excluded.normalized_data,
          raw_provider_response = excluded.raw_provider_response,
          last_fetched_at = excluded.last_fetched_at,
          fresh_until = excluded.fresh_until,
          needs_revalidation = excluded.needs_revalidation,
          is_final = excluded.is_final,
          updated_at = now()
        returning *
        `,
        [
          row.flight_key, row.provider_flight_id, row.airline_code, row.flight_number, row.departure_date,
          row.origin_airport, row.destination_airport, row.scheduled_departure_at, row.scheduled_arrival_at,
          row.estimated_departure_at, row.estimated_arrival_at, row.actual_departure_at, row.actual_arrival_at,
          row.status, row.status_detail, row.gate, row.terminal, row.baggage_belt, row.position_lat, row.position_lon,
          row.altitude, row.ground_speed, row.heading, row.provider, row.data_confidence, row.normalized_data,
          row.raw_provider_response, row.last_fetched_at, row.fresh_until, row.needs_revalidation, row.is_final,
        ]
      ));
      if (params.flightKey && params.flightKey !== saved.flight_key) {
        await pool.query(
          `insert into public.flight_instance_aliases (alias_key, flight_instance_id)
           values ($1, $2) on conflict (alias_key) do update set flight_instance_id = excluded.flight_instance_id`,
          [params.flightKey, saved.id]
        );
      }
      return saved;
    },
    async updateFlight(row) {
      return one(await pool.query(
        `
        update public.flight_instances set
          provider_flight_id=$2, scheduled_departure_at=$3, scheduled_arrival_at=$4,
          estimated_departure_at=$5, estimated_arrival_at=$6, actual_departure_at=$7,
          actual_arrival_at=$8, status=$9, status_detail=$10, gate=$11, terminal=$12,
          baggage_belt=$13, position_lat=$14, position_lon=$15, altitude=$16, ground_speed=$17,
          heading=$18, provider=$19, data_confidence=$20, normalized_data=$21,
          raw_provider_response=$22, last_fetched_at=$23, fresh_until=$24,
          needs_revalidation=$25, is_final=$26, updated_at=now()
        where id=$1
        returning *
        `,
        [
          row.id, row.provider_flight_id, row.scheduled_departure_at, row.scheduled_arrival_at,
          row.estimated_departure_at, row.estimated_arrival_at, row.actual_departure_at, row.actual_arrival_at,
          row.status, row.status_detail, row.gate, row.terminal, row.baggage_belt, row.position_lat,
          row.position_lon, row.altitude, row.ground_speed, row.heading, row.provider, row.data_confidence,
          row.normalized_data, row.raw_provider_response, row.last_fetched_at, row.fresh_until,
          row.needs_revalidation, row.is_final,
        ]
      ));
    },
    async updateProviderAlert(flightInstanceId, alert) {
      return one(await pool.query(
        `update public.flight_instances set
          provider_alert_id = $2,
          provider_alert_status = $3,
          provider_alert_created_at = coalesce($4, now()),
          provider_alert_expires_at = $5,
          refresh_priority = coalesce($6, refresh_priority),
          updated_at = now()
         where id = $1
         returning *`,
        [
          flightInstanceId,
          alert.providerAlertId || alert.id || null,
          alert.status || "unavailable",
          alert.createdAt || null,
          alert.expiresAt || null,
          alert.refreshPriority || null,
        ]
      ));
    },
    async updateStreamingState(flightInstanceId, stream) {
      return one(await pool.query(
        `update public.flight_instances set
          live_data_source = $2,
          streaming_status = $3,
          stream_registered_at = coalesce($4, now()),
          last_stream_event_at = coalesce($5, last_stream_event_at),
          refresh_priority = coalesce($6, refresh_priority),
          updated_at = now()
         where id = $1
         returning *`,
        [
          flightInstanceId,
          stream.liveDataSource || (stream.status === "active" ? "streaming" : "on_demand"),
          stream.status || "disabled",
          stream.registeredAt || null,
          stream.lastStreamEventAt || null,
          stream.refreshPriority || null,
        ]
      ));
    },
    async insertSnapshot(row) {
      await pool.query(
        `insert into public.flight_snapshots (
          flight_instance_id, status, estimated_departure_at, estimated_arrival_at,
          actual_departure_at, actual_arrival_at, gate, terminal, baggage_belt,
          position_lat, position_lon, altitude, ground_speed, heading, raw_provider_response, normalized_data
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
        [row.id, row.status, row.estimated_departure_at, row.estimated_arrival_at, row.actual_departure_at, row.actual_arrival_at, row.gate, row.terminal, row.baggage_belt, row.position_lat, row.position_lon, row.altitude, row.ground_speed, row.heading, row.raw_provider_response, row.normalized_data]
      );
    },
    async insertEvents(flightInstanceId, eventRows, provider) {
      const saved = [];
      for (const event of eventRows) {
        saved.push(one(await pool.query(
          `insert into public.flight_events (
            flight_instance_id, event_type, event_severity, old_value, new_value,
            summary, provider, provider_event_time, confidence, notification_required
          ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) returning *`,
          [flightInstanceId, event.event_type, event.event_severity, event.old_value || null, event.new_value || null, event.summary || null, provider || null, event.provider_event_time || null, event.confidence || "medium", event.notification_required === true]
        )));
      }
      return saved;
    },
    async insertFlightEventLog(entry) {
      const row = one(await pool.query(
        `insert into public.flight_event_logs (
           flight_instance_id, flight_key, fa_flight_id, ident, event_type,
           event_status, event_time, source, raw_payload, normalized_payload, dedupe_key
         )
         values ($1,$2,$3,$4,$5,$6,$7,coalesce($8, 'flightaware'),$9::jsonb,$10::jsonb,$11)
         on conflict (dedupe_key) do nothing
         returning *`,
        [
          entry.flight_instance_id || null,
          entry.flight_key,
          entry.fa_flight_id || null,
          entry.ident || null,
          entry.event_type,
          entry.event_status || null,
          entry.event_time || null,
          entry.source || "flightaware",
          JSON.stringify(entry.raw_payload || {}),
          JSON.stringify(entry.normalized_payload || {}),
          entry.dedupe_key,
        ]
      ));
      return { row, created: Boolean(row) };
    },
    async findRecentEventByType(flightInstanceId, eventType, withinMs) {
      return one(await pool.query(
        `select *
         from public.flight_events
         where flight_instance_id = $1
           and event_type = $2
           and created_at >= now() - ($3::int * interval '1 millisecond')
         order by created_at desc
         limit 1`,
        [flightInstanceId, eventType, Math.max(0, Math.round(withinMs || 0))]
      ));
    },
    async markSuspicious(flightInstanceId, reason) {
      await pool.query(`update public.flight_instances set needs_revalidation = true, data_confidence = 'suspicious', updated_at = now() where id = $1`, [flightInstanceId]);
      return one(await pool.query(
        `insert into public.flight_events (flight_instance_id, event_type, event_severity, summary, confidence)
         values ($1, 'PROVIDER_DATA_SUSPICIOUS', 'high', $2, 'suspicious') returning *`,
        [flightInstanceId, reason]
      ));
    },
    async upsertUserFlight(userId, flightInstanceId, input = {}) {
      return one(await pool.query(
        `insert into public.user_flights (
           user_id,
           flight_instance_id,
           notification_enabled,
           alert_preferences,
           user_label,
           visibility,
           added_at,
           source_type,
           lifecycle_state,
           display_flight_number,
           marketing_airline_code,
           operating_airline_code,
           origin_iata,
           destination_iata,
           scheduled_departure,
           scheduled_arrival,
           estimated_departure,
           estimated_arrival,
           actual_departure,
           actual_arrival,
           departure_terminal,
           departure_gate,
           status,
           provider_name,
           provider_flight_id,
           notifications_enabled,
           alert_settings_json
         )
         select
           $1,
           fi.id,
           $3,
           $4,
           $5,
           $6,
           now(),
           coalesce($7, 'manual_search'),
           coalesce(
             $8,
             case
               when lower(coalesce(fi.status, '')) in ('airborne', 'enroute', 'departed', 'taxiing', 'taxi_out', 'takeoff_roll') then 'active'
               when lower(coalesce(fi.status, '')) in ('landed', 'arrived', 'arrived_at_gate') then 'landed'
               else 'upcoming'
             end
           ),
           coalesce(nullif(trim(concat_ws(' ', fi.airline_code, fi.flight_number)), ''), $11, 'Flight'),
           fi.airline_code,
           fi.airline_code,
           coalesce(fi.origin_airport, $9, 'UNKNOWN'),
           coalesce(fi.destination_airport, $10, 'UNKNOWN'),
           coalesce(fi.scheduled_departure_at, fi.estimated_departure_at, fi.actual_departure_at, $12::timestamptz, now()),
           fi.scheduled_arrival_at,
           fi.estimated_departure_at,
           fi.estimated_arrival_at,
           fi.actual_departure_at,
           fi.actual_arrival_at,
           fi.terminal,
           fi.gate,
           fi.status,
           fi.provider,
           fi.provider_flight_id,
           $3,
           $4
         from public.flight_instances fi
         where fi.id = $2
         on conflict (user_id, flight_instance_id) where flight_instance_id is not null do update set
           notification_enabled = excluded.notification_enabled,
           alert_preferences = excluded.alert_preferences,
           user_label = excluded.user_label,
           visibility = excluded.visibility,
           source_type = excluded.source_type,
           lifecycle_state = excluded.lifecycle_state,
           display_flight_number = excluded.display_flight_number,
           marketing_airline_code = excluded.marketing_airline_code,
           operating_airline_code = excluded.operating_airline_code,
           origin_iata = excluded.origin_iata,
           destination_iata = excluded.destination_iata,
           scheduled_departure = excluded.scheduled_departure,
           scheduled_arrival = excluded.scheduled_arrival,
           estimated_departure = excluded.estimated_departure,
           estimated_arrival = excluded.estimated_arrival,
           actual_departure = excluded.actual_departure,
           actual_arrival = excluded.actual_arrival,
           departure_terminal = excluded.departure_terminal,
           departure_gate = excluded.departure_gate,
           status = excluded.status,
           provider_name = excluded.provider_name,
           provider_flight_id = excluded.provider_flight_id,
           notifications_enabled = excluded.notifications_enabled,
           alert_settings_json = excluded.alert_settings_json,
           updated_at = now()
         returning *`,
        [
          userId,
          flightInstanceId,
          input.notificationEnabled ?? true,
          input.alertPreferences || DEFAULT_ALERT_PREFERENCES,
          input.userLabel || null,
          input.visibility || "private",
          input.sourceType || null,
          input.lifecycleState || null,
          input.origin || null,
          input.destination || null,
          [input.airline, input.number || input.flightNumber].filter(Boolean).join(" ").trim() || null,
          input.scheduledDeparture || input.date || null,
        ]
      ));
    },
    async listUserFlights(userId) {
      const result = await pool.query(
        `select uf as user_flight, fi as flight
         from public.user_flights uf
         join public.flight_instances fi on fi.id = uf.flight_instance_id
         where uf.user_id = $1
           and uf.deleted_at is null
           and coalesce(uf.lifecycle_state, '') <> 'deleted'
         order by uf.added_at desc`,
        [userId]
      );
      return result.rows.map((row) => ({ userFlight: row.user_flight, flight: row.flight }));
    },
    async updateUserFlight(userId, id, patch) {
      return one(await pool.query(
        `update public.user_flights set
          notification_enabled = coalesce($3, notification_enabled),
          alert_preferences = coalesce($4, alert_preferences),
          user_label = coalesce($5, user_label),
          visibility = coalesce($6, visibility),
          updated_at = now()
         where user_id = $1 and id = $2
         returning *`,
        [userId, id, patch.notification_enabled, patch.alert_preferences, patch.user_label, patch.visibility]
      ));
    },
    async listUserFlightsByIds(userId, ids) {
      const result = await pool.query(
        `select *
         from public.user_flights
         where user_id = $1
           and id = any($2::uuid[])
           and deleted_at is null
           and coalesce(lifecycle_state, '') <> 'deleted'`,
        [userId, ids || []]
      );
      return result.rows;
    },
    async linkUserFlightToInstance(userId, id, flightInstanceId, patch = {}) {
      return one(await pool.query(
        `update public.user_flights uf set
          flight_instance_id = $3,
          notification_enabled = coalesce($4, uf.notification_enabled, uf.notifications_enabled, true),
          notifications_enabled = coalesce($4, uf.notifications_enabled, uf.notification_enabled, true),
          alert_preferences = coalesce($5, uf.alert_preferences, $6::jsonb),
          status = coalesce(fi.status, uf.status),
          provider_name = coalesce(fi.provider, uf.provider_name),
          provider_flight_id = coalesce(fi.provider_flight_id, uf.provider_flight_id),
          updated_at = now()
         from public.flight_instances fi
         where uf.user_id = $1
           and uf.id = $2
           and fi.id = $3
         returning uf.*`,
        [
          userId,
          id,
          flightInstanceId,
          patch.notificationEnabled ?? null,
          patch.alertPreferences || null,
          JSON.stringify(DEFAULT_ALERT_PREFERENCES),
        ]
      ));
    },
    async deleteUserFlight(userId, id) {
      return one(await pool.query(
        `update public.user_flights set
          notification_enabled = false,
          notifications_enabled = false,
          lifecycle_state = 'deleted',
          deleted_at = coalesce(deleted_at, now()),
          updated_at = now()
         where user_id = $1 and id = $2
           and deleted_at is null
           and coalesce(lifecycle_state, '') <> 'deleted'
         returning *`,
        [userId, id]
      ));
    },
    async upsertDeviceToken(userId, input) {
      return one(await pool.query(
        `insert into public.device_tokens (user_id, device_token, platform, environment, is_active, updated_at)
         values ($1, $2, $3, $4, true, now())
         on conflict (user_id, device_token) do update set
           platform = excluded.platform,
           environment = excluded.environment,
           is_active = true,
           updated_at = now()
         returning *`,
        [userId, input.deviceToken, input.platform || "ios", input.environment]
      ));
    },
    async disableDeviceToken(deviceToken) {
      await pool.query(`update public.device_tokens set is_active = false, updated_at = now() where device_token = $1`, [deviceToken]);
    },
    async getEventWithFlight(eventId) {
      const row = one(await pool.query(
        `select fe as event, fi as flight from public.flight_events fe join public.flight_instances fi on fi.id = fe.flight_instance_id where fe.id = $1`,
        [eventId]
      ));
      return row;
    },
    async listNotificationTargets(flightInstanceId, severity, eventType) {
      const circleCondition = circleNotificationPreferenceConditionForEventType(eventType);
      const tripStartingCondition = eventType === "TRIP_STARTING"
        ? "and coalesce(uf.source_type, 'tracked') <> 'tracked'"
        : "";
      const result = await pool.query(
        `with owner_targets as (
           select
             uf as user_flight,
             uf.user_id as recipient_user_id,
             false as is_circle,
             p.display_name as owner_display_name,
             coalesce(uf.source_type, 'tracked') <> 'tracked' as is_traveler
           from public.user_flights uf
           left join public.profiles p on p.user_id = uf.user_id
           where uf.flight_instance_id = $1
             and uf.notification_enabled = true
             and uf.deleted_at is null
             and coalesce(uf.lifecycle_state, '') <> 'deleted'
             and coalesce((uf.alert_preferences ->> $2)::boolean, false) = true
             ${tripStartingCondition}
         ),
         circle_targets as (
           select
             uf as user_flight,
             fp.viewer_user_id as recipient_user_id,
             true as is_circle,
             p.display_name as owner_display_name,
             false as is_traveler
           from public.user_flights uf
           left join public.profiles p on p.user_id = uf.user_id
           join public.friend_permissions fp
             on fp.owner_user_id = uf.user_id
           join public.friend_relationships fr
             on fr.id = fp.relationship_id
           where uf.flight_instance_id = $1
             and uf.deleted_at is null
             and coalesce(uf.lifecycle_state, '') <> 'deleted'
             and fr.relationship_status = 'active'
             and fp.can_view_live = true
             and fp.can_receive_alerts = true
             ${tripStartingCondition}
             and (
               fp.share_scope = 'all_flights'
               or (fp.share_scope = 'future_flights' and coalesce(uf.lifecycle_state, '') in ('upcoming', 'active'))
             )
             and ${circleCondition}
         ),
         targets as (
           select * from owner_targets
           union
           select * from circle_targets
         )
         select
           user_flight,
           recipient_user_id,
           is_circle,
           owner_display_name,
           is_traveler,
           coalesce(jsonb_agg(dt) filter (where dt.id is not null), '[]'::jsonb) as tokens
         from targets
         left join public.device_tokens dt
           on dt.user_id = targets.recipient_user_id
          and dt.is_active = true
         group by user_flight, recipient_user_id, is_circle, owner_display_name, is_traveler`,
        [flightInstanceId, severity]
      );
      return result.rows.map((row) => ({
        userFlight: { ...row.user_flight, user_id: row.recipient_user_id || row.user_flight?.user_id },
        isCircle: row.is_circle === true,
        isTraveler: row.is_traveler === true,
        ownerDisplayName: row.owner_display_name || null,
        tokens: row.tokens || [],
      }));
    },
    async createNotificationDelivery(userId, flightInstanceId, eventId, channel = "apns") {
      const row = one(await pool.query(
        `insert into public.notification_deliveries (user_id, flight_instance_id, flight_event_id, channel)
         values ($1, $2, $3, $4)
         on conflict (user_id, flight_event_id, channel) do nothing
         returning *`,
        [userId, flightInstanceId, eventId, channel]
      ));
      return { row, created: Boolean(row) };
    },
    async createAppNotification(input) {
      const existing = one(await pool.query(
        `select *
         from public.notifications
         where user_id = $1::uuid
           and notification_type = $2
           and payload_json ->> 'flight_event_id' = $3
           and created_at >= now() - interval '24 hours'
         order by created_at desc
         limit 1`,
        [input.userId, input.notificationType, input.flightEventId]
      ));
      if (existing) return { row: existing, created: false };

      const trackingSession = one(await pool.query(
        `select tracking_session_id
         from public.user_flights
         where user_id = $1::uuid
           and flight_instance_id = $2::uuid
           and deleted_at is null
         order by updated_at desc nulls last, added_at desc nulls last
         limit 1`,
        [input.userId, input.flightInstanceId]
      ));

      const payload = {
        ...(input.payload || {}),
        flight_event_id: input.flightEventId,
        flight_instance_id: input.flightInstanceId,
      };

      const row = one(await pool.query(
        `insert into public.notifications (
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
         values ($1::uuid,$2::uuid,$3,'push',$4,$5,$6,$7::jsonb,now())
         returning *`,
        [
          input.userId,
          trackingSession?.tracking_session_id || null,
          input.notificationType,
          input.deliveryStatus || "queued",
          input.title,
          input.body,
          JSON.stringify(payload),
        ]
      ));
      return { row, created: Boolean(row) };
    },
    async updateAppNotificationDeliveryStatus(input) {
      return one(await pool.query(
        `update public.notifications
         set
           delivery_status = $4,
           sent_at = case when $4 = 'sent' then coalesce(sent_at, $5::timestamptz, now()) else sent_at end,
           updated_at = now()
         where user_id = $1::uuid
           and notification_type = $2
           and payload_json ->> 'flight_event_id' = $3
           and created_at >= now() - interval '24 hours'
         returning *`,
        [
          input.userId,
          input.notificationType,
          input.flightEventId,
          input.deliveryStatus,
          input.sentAt || null,
        ]
      ));
    },
    async updateNotificationDelivery(id, patch) {
      await pool.query(
        `update public.notification_deliveries set status = coalesce($2, status), sent_at = coalesce($3, sent_at), error = coalesce($4, error) where id = $1`,
        [id, patch.status, patch.sent_at, patch.error]
      );
    },
    async logApiUsage(entry) {
      await pool.query(
        `insert into public.api_usage_logs (provider, endpoint, flight_key, user_id, status_code, response_time_ms, cache_status, cost_estimate, error)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [entry.provider, entry.endpoint, entry.flight_key || null, entry.user_id || null, entry.status_code || null, entry.response_time_ms || null, entry.cache_status || null, entry.cost_estimate || null, entry.error || null]
      );
    },
  };
}

function circleNotificationPreferenceConditionForEventType(eventType) {
  switch (String(eventType || "").toUpperCase()) {
    case "DELAYED":
    case "RESCHEDULED":
      return "fp.notify_delay = true";
    case "GATE_CHANGED":
      return "fp.notify_gate_change = true";
    case "DEPARTED":
    case "AIRBORNE":
    case "TAXIING":
    case "TAKEOFF_ROLL":
    case "TRIP_STARTING":
    case "INBOUND_ARRIVED":
      return "fp.notify_departure = true";
    case "LANDED":
    case "ARRIVED":
    case "ARRIVED_AT_GATE":
    case "TAXI_IN":
      return "fp.notify_arrival = true";
    default:
      return "true";
  }
}

module.exports = { createMemorySharedFlightRepository, createPostgresSharedFlightRepository };
