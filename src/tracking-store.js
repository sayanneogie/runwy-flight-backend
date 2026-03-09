function createTrackingStore({
  pool,
  memoryTrackedFlights,
  memoryPushDevices,
  maxMemoryTrackedFlights,
  maxMemoryPushDevices,
  defaultPollerBatchSize,
  providerName,
  normalizeFlightCode,
  normalizeAirportCode,
  parseAirlineCode,
  displayFlightCode,
  enforceMapSizeLimit,
}) {
  function usesDatabase() {
    return Boolean(pool);
  }

  async function ensureDatabaseSchema() {
    if (!usesDatabase()) return;

    await pool.query("select 1 from public.tracking_sessions limit 1");
    await pool.query("select 1 from public.user_flights limit 1");
    await pool.query("select 1 from public.live_snapshots limit 1");
    await pool.query("select 1 from public.notifications limit 1");
    await pool.query("select 1 from public.push_devices limit 1");
  }

  async function upsertTrackedFlightRecord({ flightId, query, normalized, provider, lastUpdated }) {
    if (memoryTrackedFlights.has(flightId)) {
      memoryTrackedFlights.delete(flightId);
    }

    memoryTrackedFlights.set(flightId, { flightId, query, normalized, provider, lastUpdated });
    enforceMapSizeLimit(memoryTrackedFlights, maxMemoryTrackedFlights);
  }

  async function listTrackedFlightsByFlightNumber(flightNumber) {
    if (usesDatabase()) {
      const result = await pool.query(
        `
        select
          ts.*,
          ls.provider as snapshot_provider,
          ls.provider_flight_id as snapshot_provider_flight_id,
          ls.canonical_snapshot_json,
          ls.provider_last_updated_at,
          ls.updated_at as snapshot_updated_at
        from public.tracking_sessions ts
        left join public.live_snapshots ls
          on ls.tracking_session_id = ts.id
        where upper(ts.flight_number) = upper($1)
          and ts.session_status in ('pending', 'active', 'paused')
        `,
        [normalizeFlightCode(flightNumber)]
      );

      return result.rows.map(mapTrackingRow).filter(Boolean);
    }

    return [...memoryTrackedFlights.values()].filter((item) =>
      normalizeFlightCode(item.query?.flightNumber) === normalizeFlightCode(flightNumber)
    );
  }

  async function upsertPushDevice({ apnsToken, deviceId, userId, platform = "ios" }) {
    if (usesDatabase()) {
      if (!userId) {
        throw new Error("Authenticated user is required to register push devices");
      }

      await pool.query(
        `
        insert into public.push_devices (apns_token, user_id, device_id, platform, push_enabled, updated_at)
        values ($1, $2::uuid, $3, $4, true, now())
        on conflict (apns_token) do update
        set
          user_id = excluded.user_id,
          device_id = excluded.device_id,
          platform = excluded.platform,
          push_enabled = true,
          updated_at = now()
        `,
        [apnsToken, userId, deviceId, platform]
      );

      return;
    }

    if (memoryPushDevices.has(apnsToken)) {
      memoryPushDevices.delete(apnsToken);
    }

    memoryPushDevices.set(apnsToken, {
      apnsToken,
      deviceId,
      userId: userId || null,
      platform,
      pushEnabled: true,
      updatedAt: new Date().toISOString(),
    });
    enforceMapSizeLimit(memoryPushDevices, maxMemoryPushDevices);
  }

  async function disablePushTokensForDevice(deviceId, userId) {
    if (!deviceId || !userId) return;

    if (usesDatabase()) {
      await pool.query(
        `
        update public.push_devices
        set push_enabled = false, updated_at = now()
        where device_id = $1
          and user_id = $2::uuid
        `,
        [deviceId, userId]
      );
      return;
    }

    for (const [token, info] of memoryPushDevices.entries()) {
      if (info.deviceId === deviceId) {
        memoryPushDevices.delete(token);
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
        update public.push_devices
        set push_enabled = false, updated_at = now()
        where apns_token = $1
        `,
        [apnsToken]
      );
      return;
    }

    const info = memoryPushDevices.get(apnsToken);
    if (!info) return;
    memoryPushDevices.delete(apnsToken);
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
        select distinct pd.apns_token
        from public.push_devices pd
        join recipients r
          on r.user_id = pd.user_id
        where pd.push_enabled = true
        `,
        [flightId]
      );

      return result.rows.map((row) => row.apns_token);
    }

    return [];
  }

  function toISOString(value) {
    if (!value) return null;
    const date = value instanceof Date ? value : new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  function providerFlightIdentifier(record, currentProviderName) {
    if (!record || typeof record !== "object") return null;

    if (currentProviderName === "flightaware") {
      return (
        String(
          record.fa_flight_id ||
            record.faFlightId ||
            record.ident_iata ||
            record.ident ||
            record.flight_number ||
            ""
        ).trim() || null
      );
    }

    return (
      String(
        record.flight?.iata ||
          record.flight?.icao ||
          record.flight_iata ||
          record.flight_icao ||
          record.flight_number ||
          ""
      ).trim() || null
    );
  }

  function queryForTrackingRow(row) {
    const metadata = row?.metadata_json && typeof row.metadata_json === "object" ? row.metadata_json : {};
    const query = metadata.query && typeof metadata.query === "object" ? metadata.query : {};
    const travelDate =
      toISOString(query.date || row.travel_date)?.slice(0, 10) || String(row.travel_date || "").slice(0, 10);

    return {
      flightNumber: normalizeFlightCode(query.flightNumber || row.flight_number),
      date: travelDate,
      departureIata: normalizeAirportCode(query.departureIata || row.origin_iata) || undefined,
      arrivalIata: normalizeAirportCode(query.arrivalIata || row.destination_iata) || undefined,
    };
  }

  function trackedStatusFromSessionStatus(sessionStatus) {
    switch (String(sessionStatus || "").toLowerCase()) {
      case "cancelled":
        return "cancelled";
      case "completed":
        return "landed";
      case "paused":
      case "errored":
      case "pending":
      case "active":
      default:
        return "scheduled";
    }
  }

  function fallbackNormalizedForTrackingRow(row) {
    return {
      airlineCode: row.airline_code || parseAirlineCode(row.flight_number) || null,
      flightNumber: normalizeFlightCode(row.flight_number),
      departureAirportIata: normalizeAirportCode(row.origin_iata),
      arrivalAirportIata: normalizeAirportCode(row.destination_iata),
      departureTimes: {
        scheduled: null,
        estimated: null,
        actual: null,
      },
      arrivalTimes: {
        scheduled: null,
        estimated: null,
        actual: null,
      },
      status: trackedStatusFromSessionStatus(row.session_status),
      terminal: null,
      gate: null,
      delayMinutes: null,
      inboundFlight: null,
      recentHistory: [],
      alerts: null,
      provider: row.provider || providerName,
      lastUpdated: toISOString(row.last_snapshot_at || row.updated_at) || new Date().toISOString(),
    };
  }

  function normalizedForTrackingRow(row) {
    const canonical = row?.canonical_snapshot_json;
    if (canonical && typeof canonical === "object") {
      return {
        ...canonical,
        provider: canonical.provider || row.snapshot_provider || row.provider || providerName,
        lastUpdated:
          canonical.lastUpdated ||
          toISOString(row.provider_last_updated_at || row.snapshot_updated_at || row.last_snapshot_at || row.updated_at) ||
          new Date().toISOString(),
      };
    }

    return fallbackNormalizedForTrackingRow(row);
  }

  function mapTrackingRow(row) {
    if (!row) return null;
    const normalized = normalizedForTrackingRow(row);
    const lastUpdated = normalized.lastUpdated || toISOString(row.snapshot_updated_at || row.updated_at) || new Date().toISOString();

    return {
      flightId: String(row.id),
      ownerUserId: String(row.owner_user_id),
      query: queryForTrackingRow(row),
      normalized,
      provider: row.provider || normalized.provider || providerName,
      providerFlightId: row.provider_flight_id || row.snapshot_provider_flight_id || null,
      lastUpdated,
    };
  }

  function nextPollAfterForNormalized(normalized, now = new Date()) {
    const status = String(normalized?.status || "").toLowerCase();
    if (["landed", "cancelled", "diverted"].includes(status)) {
      return null;
    }

    if (["boarding", "departed", "enroute", "delayed"].includes(status)) {
      return new Date(now.getTime() + 2 * 60_000).toISOString();
    }

    const departureISO =
      normalized?.departureTimes?.estimated ||
      normalized?.departureTimes?.scheduled ||
      normalized?.departureTimes?.actual;
    const departureMs = departureISO ? new Date(departureISO).getTime() : NaN;

    if (Number.isFinite(departureMs)) {
      const secondsUntilDeparture = Math.round((departureMs - now.getTime()) / 1000);
      if (secondsUntilDeparture <= 2 * 60 * 60) {
        return new Date(now.getTime() + 3 * 60_000).toISOString();
      }
      if (secondsUntilDeparture <= 12 * 60 * 60) {
        return new Date(now.getTime() + 10 * 60_000).toISOString();
      }
    }

    return new Date(now.getTime() + 30 * 60_000).toISOString();
  }

  async function findReusableTrackingSession({
    userId,
    provider,
    providerFlightId,
    flightNumber,
    travelDate,
    departureIata,
    arrivalIata,
  }) {
    if (!usesDatabase()) return null;

    const result = await pool.query(
      `
      select id
      from public.tracking_sessions
      where owner_user_id = $1::uuid
        and session_status in ('pending', 'active', 'paused')
        and (
          ($2::text is not null and provider = $3 and provider_flight_id = $2)
          or (
            $2::text is null
            and provider = $3
            and flight_number = $4
            and travel_date = $5::date
            and coalesce(origin_iata, '') = coalesce($6, '')
            and coalesce(destination_iata, '') = coalesce($7, '')
          )
        )
      order by created_at desc
      limit 1
      `,
      [
        userId,
        providerFlightId,
        provider,
        normalizeFlightCode(flightNumber),
        travelDate,
        departureIata || null,
        arrivalIata || null,
      ]
    );

    return result.rows[0]?.id || null;
  }

  async function fetchTrackingRowByID(flightId) {
    if (!usesDatabase()) return null;

    const result = await pool.query(
      `
      select
        ts.*,
        ls.provider as snapshot_provider,
        ls.provider_flight_id as snapshot_provider_flight_id,
        ls.snapshot_status,
        ls.terminal,
        ls.gate,
        ls.baggage_claim,
        ls.delay_minutes,
        ls.departure_times_json,
        ls.arrival_times_json,
        ls.alerts_json,
        ls.metrics_json,
        ls.canonical_snapshot_json,
        ls.raw_provider_payload_json,
        ls.provider_last_updated_at,
        ls.updated_at as snapshot_updated_at
      from public.tracking_sessions ts
      left join public.live_snapshots ls
        on ls.tracking_session_id = ts.id
      where ts.id = $1::uuid
      limit 1
      `,
      [flightId]
    );

    return mapTrackingRow(result.rows[0]);
  }

  async function fetchAccessibleTrackingRow(flightId, userId) {
    if (!usesDatabase()) return null;

    const result = await pool.query(
      `
      select
        ts.*,
        ls.provider as snapshot_provider,
        ls.provider_flight_id as snapshot_provider_flight_id,
        ls.snapshot_status,
        ls.terminal,
        ls.gate,
        ls.baggage_claim,
        ls.delay_minutes,
        ls.departure_times_json,
        ls.arrival_times_json,
        ls.alerts_json,
        ls.metrics_json,
        ls.canonical_snapshot_json,
        ls.raw_provider_payload_json,
        ls.provider_last_updated_at,
        ls.updated_at as snapshot_updated_at
      from public.tracking_sessions ts
      left join public.live_snapshots ls
        on ls.tracking_session_id = ts.id
      where ts.id = $1::uuid
        and (
          ts.owner_user_id = $2::uuid
          or exists (
            select 1
            from public.flight_watchers fw
            where fw.tracking_session_id = ts.id
              and fw.watcher_user_id = $2::uuid
              and fw.watch_state = 'approved'
          )
        )
      limit 1
      `,
      [flightId, userId]
    );

    return mapTrackingRow(result.rows[0]);
  }

  async function persistTrackingSnapshot({
    flightId,
    userId,
    query,
    normalized,
    provider,
    providerFlightId,
    rawProviderPayload,
    createdSource = "manual_track",
  }) {
    if (!usesDatabase()) {
      throw new Error("Tracking persistence requires DATABASE_URL pointing at Supabase Postgres.");
    }

    const nextPollAfter = nextPollAfterForNormalized(normalized);
    const displayCode = displayFlightCode(normalized);
    const scheduledDeparture =
      normalized?.departureTimes?.scheduled ||
      normalized?.departureTimes?.estimated ||
      normalized?.departureTimes?.actual ||
      null;
    const scheduledArrival =
      normalized?.arrivalTimes?.scheduled ||
      normalized?.arrivalTimes?.estimated ||
      normalized?.arrivalTimes?.actual ||
      null;

    await pool.query(
      `
      update public.tracking_sessions
      set
        provider = $2,
        provider_flight_id = coalesce($3, provider_flight_id),
        flight_number = $4,
        airline_code = $5,
        origin_iata = $6,
        destination_iata = $7,
        travel_date = $8::date,
        metadata_json = jsonb_build_object('query', $9::jsonb),
        next_poll_after = $10::timestamptz,
        updated_at = now()
      where id = $1::uuid
      `,
      [
        flightId,
        provider,
        providerFlightId,
        normalizeFlightCode(normalized.flightNumber || query.flightNumber),
        normalized.airlineCode || null,
        normalizeAirportCode(normalized.departureAirportIata || query.departureIata) || null,
        normalizeAirportCode(normalized.arrivalAirportIata || query.arrivalIata) || null,
        query.date,
        JSON.stringify(query),
        nextPollAfter,
      ]
    );

    await pool.query(
      `
      insert into public.user_flights (
        user_id,
        tracking_session_id,
        display_flight_number,
        airline_name,
        origin_iata,
        destination_iata,
        scheduled_departure,
        scheduled_arrival,
        added_source
      )
      values ($1::uuid, $2::uuid, $3, null, $4, $5, $6::timestamptz, $7::timestamptz, $8)
      on conflict (user_id, tracking_session_id)
      do update set
        display_flight_number = excluded.display_flight_number,
        origin_iata = excluded.origin_iata,
        destination_iata = excluded.destination_iata,
        scheduled_departure = excluded.scheduled_departure,
        scheduled_arrival = excluded.scheduled_arrival,
        updated_at = now()
      `,
      [
        userId,
        flightId,
        displayCode,
        normalizeAirportCode(normalized.departureAirportIata),
        normalizeAirportCode(normalized.arrivalAirportIata),
        scheduledDeparture,
        scheduledArrival,
        createdSource,
      ]
    );

    await pool.query(
      `
      insert into public.live_snapshots (
        tracking_session_id,
        provider,
        provider_flight_id,
        flight_number,
        airline_code,
        departure_airport_iata,
        arrival_airport_iata,
        snapshot_status,
        terminal,
        gate,
        baggage_claim,
        delay_minutes,
        departure_times_json,
        arrival_times_json,
        alerts_json,
        metrics_json,
        canonical_snapshot_json,
        raw_provider_payload_json,
        provider_last_updated_at
      )
      values (
        $1::uuid,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13::jsonb,
        $14::jsonb,
        $15::jsonb,
        $16::jsonb,
        $17::jsonb,
        $18::jsonb,
        $19::timestamptz
      )
      on conflict (tracking_session_id)
      do update set
        provider = excluded.provider,
        provider_flight_id = excluded.provider_flight_id,
        flight_number = excluded.flight_number,
        airline_code = excluded.airline_code,
        departure_airport_iata = excluded.departure_airport_iata,
        arrival_airport_iata = excluded.arrival_airport_iata,
        snapshot_status = excluded.snapshot_status,
        terminal = excluded.terminal,
        gate = excluded.gate,
        baggage_claim = excluded.baggage_claim,
        delay_minutes = excluded.delay_minutes,
        departure_times_json = excluded.departure_times_json,
        arrival_times_json = excluded.arrival_times_json,
        alerts_json = excluded.alerts_json,
        metrics_json = excluded.metrics_json,
        canonical_snapshot_json = excluded.canonical_snapshot_json,
        raw_provider_payload_json = excluded.raw_provider_payload_json,
        provider_last_updated_at = excluded.provider_last_updated_at,
        updated_at = now()
      `,
      [
        flightId,
        provider,
        providerFlightId,
        normalizeFlightCode(normalized.flightNumber || query.flightNumber),
        normalized.airlineCode || null,
        normalizeAirportCode(normalized.departureAirportIata || query.departureIata),
        normalizeAirportCode(normalized.arrivalAirportIata || query.arrivalIata),
        normalized.status || "scheduled",
        normalized.terminal || null,
        normalized.gate || null,
        normalized.baggageClaim || null,
        Number.isFinite(Number(normalized.delayMinutes)) ? Number(normalized.delayMinutes) : null,
        JSON.stringify(normalized.departureTimes || {}),
        JSON.stringify(normalized.arrivalTimes || {}),
        JSON.stringify(normalized.alerts || {}),
        JSON.stringify(normalized.metrics || {}),
        JSON.stringify(normalized),
        JSON.stringify(rawProviderPayload || {}),
        normalized.lastUpdated || new Date().toISOString(),
      ]
    );
  }

  async function createOrReuseTrackingSession({
    userId,
    query,
    normalized,
    provider,
    rawProviderPayload,
    createdSource = "manual_track",
  }) {
    if (!usesDatabase()) {
      throw new Error("Tracking persistence requires DATABASE_URL pointing at Supabase Postgres.");
    }

    const providerFlightId = providerFlightIdentifier(rawProviderPayload, provider);
    let flightId = await findReusableTrackingSession({
      userId,
      provider,
      providerFlightId,
      flightNumber: query.flightNumber,
      travelDate: query.date,
      departureIata: query.departureIata,
      arrivalIata: query.arrivalIata,
    });

    if (!flightId) {
      const inserted = await pool.query(
        `
        insert into public.tracking_sessions (
          owner_user_id,
          provider,
          provider_flight_id,
          flight_number,
          airline_code,
          origin_iata,
          destination_iata,
          travel_date,
          session_status,
          created_source,
          metadata_json
        )
        values (
          $1::uuid,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8::date,
          'pending',
          $9,
          jsonb_build_object('query', $10::jsonb)
        )
        returning id
        `,
        [
          userId,
          provider,
          providerFlightId,
          normalizeFlightCode(normalized.flightNumber || query.flightNumber),
          normalized.airlineCode || null,
          normalizeAirportCode(normalized.departureAirportIata || query.departureIata),
          normalizeAirportCode(normalized.arrivalAirportIata || query.arrivalIata),
          query.date,
          createdSource,
          JSON.stringify(query),
        ]
      );
      flightId = inserted.rows[0]?.id;
    }

    if (!flightId) {
      throw new Error("Failed to create tracking session");
    }

    await persistTrackingSnapshot({
      flightId,
      userId,
      query,
      normalized,
      provider,
      providerFlightId,
      rawProviderPayload,
      createdSource,
    });

    return fetchTrackingRowByID(flightId);
  }

  async function listDueTrackingRows(limit = defaultPollerBatchSize) {
    if (!usesDatabase()) return [];

    const result = await pool.query(
      `
      select
        ts.*,
        ls.provider as snapshot_provider,
        ls.provider_flight_id as snapshot_provider_flight_id,
        ls.canonical_snapshot_json,
        ls.provider_last_updated_at,
        ls.updated_at as snapshot_updated_at
      from public.tracking_sessions ts
      left join public.live_snapshots ls
        on ls.tracking_session_id = ts.id
      where ts.session_status in ('pending', 'active', 'paused', 'errored')
        and (ts.next_poll_after is null or ts.next_poll_after <= now())
      order by coalesce(ts.next_poll_after, ts.created_at) asc
      limit $1
      `,
      [limit]
    );

    return result.rows.map(mapTrackingRow).filter(Boolean);
  }

  async function markTrackingRowErrored(flightId, reason) {
    if (!usesDatabase()) return;

    await pool.query(
      `
      update public.tracking_sessions
      set
        session_status = 'errored',
        next_poll_after = now() + interval '10 minutes',
        polling_stopped_reason = left(coalesce($2, 'provider_error'), 256),
        updated_at = now()
      where id = $1::uuid
      `,
      [flightId, reason || null]
    );
  }

  return {
    createOrReuseTrackingSession,
    disablePushToken,
    disablePushTokensForDevice,
    ensureDatabaseSchema,
    fetchAccessibleTrackingRow,
    fetchTrackingRowByID,
    listDueTrackingRows,
    listPushTokensForFlight,
    listTrackedFlightsByFlightNumber,
    markTrackingRowErrored,
    persistTrackingSnapshot,
    providerFlightIdentifier,
    upsertPushDevice,
    upsertTrackedFlightRecord,
    usesDatabase,
  };
}

module.exports = {
  createTrackingStore,
};
