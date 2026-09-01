"use strict";

const FINAL_STATUSES = new Set(["landed", "arrived", "arrived_at_gate", "cancelled"]);
const AIRBORNE_STATUSES = new Set(["airborne", "enroute", "departed"]);
const TAXI_STATUSES = new Set(["taxiing", "taxi_out", "takeoff_roll", "taxi_in"]);
const APPROACH_WINDOW_MINUTES = 45;
const LIVE_POSITION_CONTRADICTION_WINDOW_MS = 5 * 60_000;
const ACTUAL_EVENT_CLOCK_SKEW_MS = 2 * 60_000;

function normalizeAirline(input) {
  return String(input || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function normalizeFlightNumber(input) {
  return String(input || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "").replace(/^[A-Z]+/, "") || String(input || "").trim();
}

function normalizeAirport(input) {
  const value = String(input || "").trim().toUpperCase();
  return value ? value.slice(0, 3) : "UNKNOWN";
}

function normalizeDate(input) {
  const value = String(input || "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  return value;
}

function buildFlightKey({ airline, airlineCode, number, flightNumber, date, departureDate, origin, destination }) {
  const airlinePart = normalizeAirline(airline || airlineCode);
  const numberPart = normalizeFlightNumber(number || flightNumber);
  const datePart = normalizeDate(date || departureDate);
  const originPart = normalizeAirport(origin);
  const destinationPart = normalizeAirport(destination);
  if (!airlinePart || !numberPart || !datePart) {
    throw new Error("Missing airline, flight number, or date");
  }
  return `${airlinePart}-${numberPart}-${datePart}-${originPart}-${destinationPart}`;
}

function normalizeSearchParams(input) {
  const airline = normalizeAirline(input.airline || input.airlineCode);
  const number = normalizeFlightNumber(input.number || input.flightNumber);
  const date = normalizeDate(input.date || input.departureDate);
  const origin = input.origin || input.departureIata ? normalizeAirport(input.origin || input.departureIata) : "UNKNOWN";
  const destination = input.destination || input.arrivalIata ? normalizeAirport(input.destination || input.arrivalIata) : "UNKNOWN";
  if (!airline || !number || !date) {
    const error = new Error("airline, number, and date are required");
    error.statusCode = 400;
    throw error;
  }
  return { airline, number, date, origin, destination, flightKey: buildFlightKey({ airline, number, date, origin, destination }) };
}

function toIso(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function minutesBetween(a, b) {
  const left = a ? new Date(a).getTime() : NaN;
  const right = b ? new Date(b).getTime() : NaN;
  if (!Number.isFinite(left) || !Number.isFinite(right)) return null;
  return Math.round((right - left) / 60000);
}

function isFinalStatus(status) {
  return FINAL_STATUSES.has(String(status || "").toLowerCase());
}

function reconcileDiversionContext(normalized, requested = {}, existingRow = null) {
  if (!normalized || typeof normalized !== "object") return normalized;

  const existingNormalized = existingRow?.normalized_data || existingRow?.normalizedData || {};
  const bookedDestination = normalizeAirport(
    existingNormalized.originalDestination ||
    existingNormalized.originalDestinationAirportIata ||
    existingRow?.destination_airport ||
    requested.destination ||
    normalized.originalDestination ||
    normalized.originalDestinationAirportIata ||
    normalized.destination
  );
  const incomingDestination = normalizeAirport(
    normalized.diversionAirport ||
    normalized.diversionAirportIata ||
    normalized.destination
  );
  const previousDiversion = normalizeAirport(
    existingNormalized.diversionAirport || existingNormalized.diversionAirportIata
  );
  const providerSaysDiverted = String(
    normalized.providerStatus || normalized.status || normalized.statusDetail || ""
  ).toLowerCase().includes("divert");
  const destinationChangedAfterDeparture = Boolean(existingRow) &&
    bookedDestination !== "UNKNOWN" &&
    incomingDestination !== "UNKNOWN" &&
    incomingDestination !== bookedDestination &&
    Boolean(
      existingRow.actual_departure_at ||
      normalized.actualDepartureAt ||
      normalized.departureTimes?.actual ||
      AIRBORNE_STATUSES.has(String(existingRow.status || normalized.status || "").toLowerCase())
    );
  const hasDiversion = previousDiversion !== "UNKNOWN" || providerSaysDiverted || destinationChangedAfterDeparture;

  if (!hasDiversion) return normalized;

  const diversionAirport = incomingDestination !== bookedDestination
    ? incomingDestination
    : previousDiversion;
  if (bookedDestination === "UNKNOWN" || diversionAirport === "UNKNOWN") return normalized;

  return {
    ...normalized,
    destination: bookedDestination,
    originalDestination: bookedDestination,
    diversionAirport,
    isDiverted: true,
  };
}

function timestampMs(value) {
  if (!value) return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function bestTime(record, names) {
  for (const name of names) {
    const ms = timestampMs(record?.[name]);
    if (ms != null) return ms;
  }
  return null;
}

function freshAirborneTelemetry(flight, nowMs = Date.now()) {
  const normalized = flight?.normalized_data || flight?.normalizedData || {};
  const position = normalized?.position || normalized?.livePosition || flight?.position || flight?.livePosition || {};
  const recordedAt = timestampMs(
    position?.recordedAt ||
    position?.recorded_at ||
    normalized?.lastUpdated ||
    flight?.position_recorded_at ||
    flight?.positionRecordedAt
  );
  if (recordedAt == null || Math.abs(nowMs - recordedAt) > LIVE_POSITION_CONTRADICTION_WINDOW_MS) {
    return false;
  }

  const hasPosition =
    flight?.position_lat != null ||
    position?.lat != null ||
    position?.latitude != null;
  if (!hasPosition) return false;

  const altitudeFeet = Number(flight?.altitude ?? position?.altitudeFeet ?? position?.altitude);
  const groundSpeedKnots = Number(flight?.ground_speed ?? position?.groundSpeedKnots ?? position?.groundSpeed);
  const airGround = String(position?.airGround || position?.air_ground || "").toUpperCase();
  return airGround === "A" ||
    (Number.isFinite(altitudeFeet) && altitudeFeet > 300) ||
    (Number.isFinite(altitudeFeet) && altitudeFeet > 150 &&
      Number.isFinite(groundSpeedKnots) && groundSpeedKnots > 80);
}

function phaseFromProviderStatus(status) {
  const value = String(status || "").toLowerCase();
  if (["cancelled", "canceled"].includes(value)) return "cancelled";
  if (value === "diverted") return "diverted";
  if (value === "arrived_at_gate") return "arrived_at_gate";
  if (["landed", "arrived"].includes(value)) return value;
  if (value === "taxi_in") return "taxi_in";
  if (["airborne", "enroute", "departed"].includes(value)) return "airborne";
  if (["takeoff_roll", "taxi_out", "taxiing", "boarding", "delayed"].includes(value)) return value;
  if (["scheduled", "unknown"].includes(value)) return value;
  return null;
}

function displayStatusForPhase(phase, fallbackStatus = "scheduled") {
  switch (String(phase || "").toLowerCase()) {
    case "airborne":
    case "approaching":
      return "enroute";
    case "taxi_out":
    case "takeoff_roll":
    case "boarding":
    case "taxi_in":
    case "arrived_at_gate":
    case "landed":
    case "arrived":
    case "cancelled":
    case "diverted":
    case "delayed":
    case "scheduled":
      return String(phase).toLowerCase();
    default:
      return String(fallbackStatus || "scheduled").toLowerCase();
  }
}

function deriveFlightLifecyclePhase(flight, nowMs = Date.now()) {
  const providerPhase = phaseFromProviderStatus(flight?.status);
  const actualDeparture = bestTime(flight, ["actual_departure_at", "actualDepartureAt"]);
  const actualTakeoff = timestampMs(
    flight?.normalized_data?.takeoffTimes?.actual ||
    flight?.normalizedData?.takeoffTimes?.actual ||
    flight?.takeoffTimes?.actual ||
    flight?.actualTakeoffAt
  );
  const actualArrival = bestTime(flight, ["actual_arrival_at", "actualArrivalAt"]);
  const credibleActualArrival = actualArrival != null && actualArrival <= nowMs + ACTUAL_EVENT_CLOCK_SKEW_MS;
  const scheduledDeparture = bestTime(flight, ["scheduled_departure_at", "scheduledDepartureAt"]);
  const estimatedDeparture = bestTime(flight, ["estimated_departure_at", "estimatedDepartureAt"]);
  const scheduledArrival = bestTime(flight, ["scheduled_arrival_at", "scheduledArrivalAt"]);
  const estimatedArrival = bestTime(flight, ["estimated_arrival_at", "estimatedArrivalAt"]);
  const departure = actualDeparture || estimatedDeparture || scheduledDeparture;
  const arrival = actualArrival || estimatedArrival || scheduledArrival;
  const hasLivePosition =
    flight?.position_lat != null ||
    flight?.position?.lat != null ||
    flight?.normalized_data?.position?.lat != null ||
    flight?.normalizedData?.position?.lat != null ||
    flight?.livePosition?.latitude != null;
  const liveAltitudeFeet = Number(
    flight?.altitude ??
    flight?.position?.altitude ??
    flight?.normalized_data?.livePosition?.altitudeFeet ??
    flight?.normalizedData?.livePosition?.altitudeFeet ??
    flight?.livePosition?.altitudeFeet
  );
  const liveGroundSpeedKnots = Number(
    flight?.ground_speed ??
    flight?.position?.groundSpeed ??
    flight?.normalized_data?.livePosition?.groundSpeedKnots ??
    flight?.normalizedData?.livePosition?.groundSpeedKnots ??
    flight?.livePosition?.groundSpeedKnots
  );
  const hasGroundTelemetry = hasLivePosition &&
    Number.isFinite(liveAltitudeFeet) &&
    Number.isFinite(liveGroundSpeedKnots) &&
    liveAltitudeFeet <= 150 &&
    liveGroundSpeedKnots <= 60;
  const hasAirbornePosition = hasLivePosition && (
    (Number.isFinite(liveAltitudeFeet) && liveAltitudeFeet > 300) ||
    (Number.isFinite(liveAltitudeFeet) && liveAltitudeFeet > 150 &&
      Number.isFinite(liveGroundSpeedKnots) && liveGroundSpeedKnots > 80)
  );

  if (providerPhase === "cancelled") {
    return { phase: providerPhase, confidence: "provider_confirmed", reason: `provider_status_${providerPhase}` };
  }
  if (freshAirborneTelemetry(flight, nowMs) && (
    credibleActualArrival ||
    ["landed", "arrived", "arrived_at_gate", "taxi_in"].includes(providerPhase)
  )) {
    const minutesUntilArrival = arrival != null ? (arrival - nowMs) / 60000 : null;
    return {
      phase: minutesUntilArrival != null && minutesUntilArrival <= APPROACH_WINDOW_MINUTES && minutesUntilArrival >= -10 ? "approaching" : "airborne",
      confidence: "position_confirmed",
      reason: "fresh_airborne_position_vetoed_terminal_state",
    };
  }
  if (credibleActualArrival) {
    return { phase: providerPhase === "arrived_at_gate" ? "arrived_at_gate" : "landed", confidence: "timestamp_confirmed", reason: "actual_arrival_present" };
  }
  if (["landed", "arrived", "arrived_at_gate"].includes(providerPhase)) {
    return { phase: providerPhase, confidence: "provider_confirmed", reason: `provider_status_${providerPhase}` };
  }
  if (providerPhase === "taxi_in") {
    return { phase: "taxi_in", confidence: "provider_confirmed", reason: "provider_status_taxi_in" };
  }
  if (["takeoff_roll", "taxi_out", "taxiing", "boarding"].includes(providerPhase)) {
    return { phase: providerPhase === "taxiing" ? "taxi_out" : providerPhase, confidence: "provider_confirmed", reason: `provider_status_${providerPhase}` };
  }
  if (hasGroundTelemetry) {
    return { phase: "taxi_out", confidence: "position_confirmed", reason: "ground_telemetry_present" };
  }
  if (["airborne", "enroute"].includes(String(flight?.status || "").toLowerCase()) || actualTakeoff || hasAirbornePosition) {
    const minutesUntilArrival = arrival != null ? (arrival - nowMs) / 60000 : null;
    return {
      phase: minutesUntilArrival != null && minutesUntilArrival <= APPROACH_WINDOW_MINUTES && minutesUntilArrival >= -10 ? "approaching" : "airborne",
      confidence: hasAirbornePosition ? "position_confirmed" : actualTakeoff ? "timestamp_confirmed" : "provider_confirmed",
      reason: hasAirbornePosition ? "airborne_position_present" : actualTakeoff ? "actual_takeoff_present" : "provider_airborne_status",
    };
  }
  if (providerPhase === "diverted") {
    return { phase: "diverted", confidence: "provider_confirmed", reason: "provider_status_diverted_without_movement_evidence" };
  }
  // FlightAware's actual OUT timestamp means the aircraft left the gate. It is
  // not the OFF timestamp and must never, by itself, be shown as airborne.
  if (actualDeparture) {
    return { phase: "taxi_out", confidence: "timestamp_confirmed", reason: "actual_gate_departure_present" };
  }

  if (providerPhase === "delayed") {
    return { phase: "delayed", confidence: "provider_confirmed", reason: "provider_status_delayed" };
  }
  return { phase: providerPhase || "scheduled", confidence: "schedule_only", reason: "no_confirmed_movement" };
}

function getFlightFreshnessTTL(flightInstance, nowMs = Date.now(), random = Math.random, options = {}) {
  const lifecycle = deriveFlightLifecyclePhase(flightInstance, nowMs);
  const status = displayStatusForPhase(lifecycle.phase, flightInstance?.status || "").toLowerCase();
  const confidence = String(flightInstance?.data_confidence || flightInstance?.dataConfidence || "").toLowerCase();
  const activeViewerCount = Number(options.activeViewerCount || flightInstance?.active_viewer_count || flightInstance?.activeViewerCount || 0);
  const alertActive = isProviderAlertActive(flightInstance, nowMs);
  const streamingActive = isStreamingActive(flightInstance);
  const departure = new Date(
    flightInstance?.estimated_departure_at ||
      flightInstance?.estimatedDepartureAt ||
      flightInstance?.scheduled_departure_at ||
      flightInstance?.scheduledDepartureAt ||
      `${flightInstance?.departure_date || flightInstance?.departureDate || ""}T00:00:00Z`
  ).getTime();
  const hoursUntilDeparture = Number.isFinite(departure) ? (departure - nowMs) / 36e5 : null;
  let min = 5 * 60;
  let max = 15 * 60;

  if (isFinalStatus(status) || flightInstance?.is_final) {
    min = 12 * 60 * 60;
    max = 24 * 60 * 60;
  } else if (AIRBORNE_STATUSES.has(status)) {
    if (activeViewerCount > 0) {
      min = 30;
      max = 60;
    } else if (streamingActive || alertActive) {
      min = 30 * 60;
      max = 60 * 60;
    } else {
      min = 60;
      max = 2 * 60;
    }
  } else if (TAXI_STATUSES.has(status)) {
    if (activeViewerCount > 0) {
      min = 30;
      max = 60;
    } else if (streamingActive || alertActive) {
      min = 10 * 60;
      max = 20 * 60;
    } else {
      min = 60;
      max = 3 * 60;
    }
  } else if (streamingActive || alertActive) {
    if (hoursUntilDeparture != null && hoursUntilDeparture > 0) {
      min = Math.max(30 * 60, Math.round(hoursUntilDeparture * 60 * 60));
      max = min;
    } else if (hoursUntilDeparture != null && hoursUntilDeparture > 3) {
      min = 60 * 60;
      max = 3 * 60 * 60;
    } else if (hoursUntilDeparture != null && hoursUntilDeparture >= 0) {
      min = 15 * 60;
      max = 30 * 60;
    } else {
      min = 30 * 60;
      max = 2 * 60 * 60;
    }
  } else {
    if (hoursUntilDeparture != null && hoursUntilDeparture > 24 * 7) {
      min = 18 * 60 * 60;
      max = 24 * 60 * 60;
    } else if (hoursUntilDeparture != null && hoursUntilDeparture > 24) {
      min = 6 * 60 * 60;
      max = 12 * 60 * 60;
    } else if (hoursUntilDeparture != null && hoursUntilDeparture >= 0 && hoursUntilDeparture <= 3) {
      min = 5 * 60;
      max = 15 * 60;
    } else {
      min = 15 * 60;
      max = 30 * 60;
    }
  }

  if (confidence === "low" || confidence === "suspicious" || flightInstance?.needs_revalidation) {
    min = 60;
    max = 5 * 60;
  }

  return Math.max(10, Math.round(min + random() * (max - min)));
}

function isProviderAlertActive(flightInstance, nowMs = Date.now()) {
  const status = String(flightInstance?.provider_alert_status || flightInstance?.providerAlertStatus || "").toLowerCase();
  if (status !== "active") return false;
  const expiresAt = flightInstance?.provider_alert_expires_at || flightInstance?.providerAlertExpiresAt;
  if (!expiresAt) return true;
  const expiresAtMs = new Date(expiresAt).getTime();
  return Number.isFinite(expiresAtMs) && expiresAtMs > nowMs;
}

function isStreamingActive(flightInstance) {
  return (
    String(flightInstance?.live_data_source || flightInstance?.liveDataSource || "").toLowerCase() === "streaming" &&
    String(flightInstance?.streaming_status || flightInstance?.streamingStatus || "").toLowerCase() === "active"
  );
}

function mapNormalizedToDb(normalized, params = {}) {
  normalized = reconcileDiversionContext(normalized, params, params.existingRow || null);
  const origin = normalizeAirport(normalized.origin || params.origin);
  const destination = normalizeAirport(normalized.originalDestination || normalized.destination || params.destination);
  const status = statusReconciledWithActualTimes(normalized);
  return {
    flight_key: buildFlightKey({
      airline: normalized.airlineCode || params.airline,
      number: normalized.flightNumber || params.number,
      date: params.date || normalized.departureDate || toIso(normalized.scheduledDepartureAt)?.slice(0, 10),
      origin,
      destination,
    }),
    provider_flight_id: normalized.providerFlightId || null,
    airline_code: normalizeAirline(normalized.airlineCode || params.airline),
    flight_number: normalizeFlightNumber(normalized.flightNumber || params.number),
    departure_date: params.date || normalized.departureDate || toIso(normalized.scheduledDepartureAt)?.slice(0, 10),
    origin_airport: origin === "UNKNOWN" ? null : origin,
    destination_airport: destination === "UNKNOWN" ? null : destination,
    scheduled_departure_at: toIso(normalized.scheduledDepartureAt),
    scheduled_arrival_at: toIso(normalized.scheduledArrivalAt),
    estimated_departure_at: toIso(normalized.estimatedDepartureAt),
    estimated_arrival_at: toIso(normalized.estimatedArrivalAt),
    actual_departure_at: toIso(normalized.actualDepartureAt),
    actual_arrival_at: toIso(normalized.actualArrivalAt),
    status,
    status_detail: normalized.statusDetail || null,
    gate: normalized.departureGate || normalized.gate || null,
    terminal: normalized.departureTerminal || normalized.terminal || null,
    baggage_belt: normalized.baggageBelt || null,
    position_lat: normalized.position?.lat ?? null,
    position_lon: normalized.position?.lon ?? null,
    altitude: normalized.position?.altitude ?? null,
    ground_speed: normalized.position?.groundSpeed ?? null,
    heading: normalized.position?.heading ?? null,
    provider: normalized.provider || null,
    provider_alert_status: normalized.providerAlertStatus || params.providerAlertStatus || "unavailable",
    live_data_source: normalized.liveDataSource || params.liveDataSource || "on_demand",
    streaming_status: normalized.streamingStatus || params.streamingStatus || "disabled",
    data_confidence: normalized.dataConfidence || "medium",
    normalized_data: { ...normalized, status },
    raw_provider_response: normalized.rawProviderResponse || null,
    last_fetched_at: new Date().toISOString(),
    needs_revalidation: normalized.dataConfidence === "suspicious",
    is_final: isFinalStatus(status),
  };
}

function statusReconciledWithActualTimes(normalized) {
  const status = String(normalized?.status || "unknown").toLowerCase();
  if (freshAirborneTelemetry(normalized)) {
    return "enroute";
  }
  const actualArrivalMs = timestampMs(normalized?.actualArrivalAt);
  if (actualArrivalMs != null && actualArrivalMs <= Date.now() + ACTUAL_EVENT_CLOCK_SKEW_MS) {
    return status === "landed" ? "landed" : "arrived_at_gate";
  }
  if (
    normalized?.actualDepartureAt &&
    ["unknown", "scheduled", "boarding", "delayed"].includes(status)
  ) {
    return "taxiing";
  }
  return status;
}

function rowToFlightResponse(row, { source = "postgres", freshness = "fresh", isRefreshing = false } = {}) {
  if (!row) return null;
  const lifecycle = deriveFlightLifecyclePhase(row);
  const normalized = row.normalized_data || {};
  return {
    flightKey: row.flight_key,
    flightInstanceId: row.id,
    stateRevision: Number(row.state_revision || 0),
    providerFlightId: row.provider_flight_id,
    airlineCode: row.airline_code,
    flightNumber: row.flight_number,
    origin: row.origin_airport,
    destination: row.destination_airport,
    originalDestination: normalized.originalDestination || row.destination_airport,
    diversionAirport: normalized.diversionAirport || null,
    isDiverted: normalized.isDiverted === true || Boolean(normalized.diversionAirport),
    status: displayStatusForPhase(lifecycle.phase, row.status),
    providerStatus: row.status,
    computedPhase: lifecycle.phase,
    phaseConfidence: lifecycle.confidence,
    phaseReason: lifecycle.reason,
    statusDetail: row.status_detail,
    scheduledDepartureAt: toIso(row.scheduled_departure_at),
    scheduledArrivalAt: toIso(row.scheduled_arrival_at),
    estimatedDepartureAt: toIso(row.estimated_departure_at),
    estimatedArrivalAt: toIso(row.estimated_arrival_at),
    actualDepartureAt: toIso(row.actual_departure_at),
    actualArrivalAt: toIso(row.actual_arrival_at),
    departureTimes: normalized.departureTimes || {
      scheduled: toIso(row.scheduled_departure_at),
      estimated: toIso(row.estimated_departure_at),
      actual: toIso(row.actual_departure_at),
    },
    takeoffTimes: normalized.takeoffTimes || null,
    landingTimes: normalized.landingTimes || null,
    arrivalTimes: normalized.arrivalTimes || {
      scheduled: toIso(row.scheduled_arrival_at),
      estimated: toIso(row.estimated_arrival_at),
      actual: toIso(row.actual_arrival_at),
    },
    gate: row.gate,
    terminal: row.terminal,
    departureGate: normalized.departureGate || row.gate,
    departureTerminal: normalized.departureTerminal || row.terminal,
    arrivalGate: normalized.arrivalGate || null,
    arrivalTerminal: normalized.arrivalTerminal || null,
    arrivalTimezone: normalized.arrivalTimezone || null,
    inboundFlight: normalized.inboundFlight || null,
    baggageBelt: row.baggage_belt,
    trackPoints: Array.isArray(normalized.trackPoints) ? normalized.trackPoints : [],
    aircraftType: normalized.aircraftType || null,
    aircraftRegistration: normalized.aircraftRegistration || null,
    position: {
      lat: row.position_lat,
      lon: row.position_lon,
      altitude: row.altitude,
      groundSpeed: row.ground_speed,
      heading: row.heading,
    },
    lastUpdatedAt: toIso(row.last_fetched_at || row.updated_at),
    freshUntil: toIso(row.fresh_until),
    freshness,
    source,
    isRefreshing,
    dataConfidence: row.data_confidence || "unknown",
    provider: row.provider || null,
    providerAlertStatus: row.provider_alert_status || "unavailable",
    liveDataSource: row.live_data_source || "on_demand",
    streamingStatus: row.streaming_status || "disabled",
  };
}

function validateProviderFlight(normalized, requested, existingRow = null) {
  normalized = reconcileDiversionContext(normalized, requested, existingRow);
  const problems = [];
  const airline = normalizeAirline(normalized.airlineCode);
  const number = normalizeFlightNumber(normalized.flightNumber);
  const requestedAirline = normalizeAirline(requested.airline);
  const requestedNumber = normalizeFlightNumber(requested.number);
  if (airline !== requestedAirline) problems.push("airline_mismatch");
  if (number !== requestedNumber) problems.push("flight_number_mismatch");
  if (!normalized.providerFlightId && !normalized.scheduledDepartureAt) problems.push("weak_identifiers");
  if (normalized.rawProviderResponse?.error || normalized.rawProviderResponse?.errors) problems.push("provider_error_payload");

  const scheduledDate = toIso(normalized.scheduledDepartureAt)?.slice(0, 10);
  if (scheduledDate) {
    const dayDelta = Math.abs((Date.parse(`${scheduledDate}T00:00:00Z`) - Date.parse(`${requested.date}T00:00:00Z`)) / 864e5);
    if (dayDelta > 1) problems.push("departure_date_mismatch");
  }
  if (requested.origin !== "UNKNOWN" && normalizeAirport(normalized.origin) !== requested.origin) problems.push("origin_mismatch");
  if (requested.destination !== "UNKNOWN" && normalizeAirport(normalized.destination) !== requested.destination && normalized.isDiverted !== true) problems.push("destination_mismatch");
  if (normalized.scheduledArrivalAt && normalized.scheduledDepartureAt && Date.parse(normalized.scheduledArrivalAt) < Date.parse(normalized.scheduledDepartureAt)) {
    problems.push("arrival_before_departure");
  }

  const suspicious = problems.some((problem) =>
    ["airline_mismatch", "flight_number_mismatch", "departure_date_mismatch", "provider_error_payload", "arrival_before_departure"].includes(problem)
  );
  const downgraded = problems.some((problem) => ["origin_mismatch", "destination_mismatch", "weak_identifiers"].includes(problem));
  return {
    ok: !suspicious,
    confidence: suspicious ? "suspicious" : downgraded ? "low" : normalized.dataConfidence || (existingRow ? "medium" : "high"),
    problems,
  };
}

function compareFlightState(oldState, newState, nowMs = Date.now()) {
  const events = [];
  const oldStatus = String(oldState?.status || "").toLowerCase();
  const newStatus = String(newState?.status || "").toLowerCase();
  const confidence = newState?.data_confidence || newState?.dataConfidence || "medium";
  const departureAt = new Date(newState?.scheduled_departure_at || newState?.scheduledDepartureAt || 0).getTime();
  const within24h = Number.isFinite(departureAt) && departureAt - nowMs <= 24 * 60 * 60_000;
  const reliableBaggageWindow = isReliableBaggageNotificationWindow(newState, nowMs);
  const push = (event_type, event_severity, old_value, new_value, summary, notification_required = false) => {
    events.push({ event_type, event_severity, old_value, new_value, summary, notification_required, confidence });
  };

  const oldNormalized = oldState?.normalized_data || oldState?.normalizedData || oldState || {};
  const newNormalized = newState?.normalized_data || newState?.normalizedData || newState || {};
  const oldDiversion = normalizeAirport(oldNormalized.diversionAirport || oldNormalized.diversionAirportIata);
  const newDiversion = normalizeAirport(newNormalized.diversionAirport || newNormalized.diversionAirportIata);
  const originalDestination = normalizeAirport(
    newNormalized.originalDestination || newNormalized.originalDestinationAirportIata || oldState?.destination_airport
  );

  if (!oldState) {
    push("SCHEDULED", "low", null, { status: newStatus }, "Flight tracking started", false);
    return events;
  }

  const suspicious =
    (oldStatus === "landed" && ["airborne", "enroute", "departed"].includes(newStatus)) ||
    (oldStatus === "cancelled" && newStatus === "scheduled" && confidence !== "high") ||
    (oldState.actual_departure_at && !newState.actual_departure_at) ||
    (oldState.origin_airport && newState.origin_airport && oldState.origin_airport !== newState.origin_airport && newStatus !== "diverted") ||
    (oldState.destination_airport && newState.destination_airport && oldState.destination_airport !== newState.destination_airport && newStatus !== "diverted") ||
    (newState.estimated_arrival_at && newState.estimated_departure_at && Date.parse(newState.estimated_arrival_at) < Date.parse(newState.estimated_departure_at));

  if (suspicious || confidence === "suspicious") {
    push("PROVIDER_DATA_SUSPICIOUS", "high", oldState, newState, "Provider returned a suspicious flight state transition", false);
    return events;
  }

  let emittedAirborneEvent = false;
  if (newStatus !== oldStatus) {
    if (newStatus === "cancelled") push("CANCELLED", "critical", { status: oldStatus }, { status: newStatus }, "Flight has been cancelled", true);
    else if (newStatus === "diverted") push("DIVERTED", "critical", { status: oldStatus }, { status: newStatus, originalDestination, diversionAirport: newDiversion === "UNKNOWN" ? null : newDiversion }, newDiversion !== "UNKNOWN" ? `Flight diverted to ${newDiversion}` : "Flight has been diverted", true);
    else if (["taxiing", "taxi_out"].includes(newStatus)) push("TAXIING", "medium", { status: oldStatus }, { status: newStatus }, "Flight is taxiing", true);
    else if (newStatus === "takeoff_roll") push("TAKEOFF_ROLL", "high", { status: oldStatus }, { status: newStatus }, "Flight is about to take off", true);
    else if (newStatus === "taxi_in") push("TAXI_IN", "low", { status: oldStatus }, { status: newStatus }, "Flight is taxiing to the gate", true);
    else if (newStatus === "arrived_at_gate") push("ARRIVED_AT_GATE", "medium", { status: oldStatus }, { status: newStatus }, "Flight has arrived at the gate", true);
    else if (newStatus === "departed") push("DEPARTED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has departed", true);
    else if (["airborne", "enroute"].includes(newStatus)) {
      push("AIRBORNE", "medium", { status: oldStatus }, { status: newStatus }, "Flight is airborne", true);
      emittedAirborneEvent = true;
    }
    else if (newStatus === "landed") push("LANDED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has landed", true);
    else if (newStatus === "arrived") push("ARRIVED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has arrived", true);
  }


  if (newDiversion !== "UNKNOWN" && newDiversion !== oldDiversion && newStatus !== "diverted") {
    push(
      "DIVERTED",
      "critical",
      { originalDestination, diversionAirport: oldDiversion === "UNKNOWN" ? null : oldDiversion },
      { originalDestination, diversionAirport: newDiversion },
      `Flight diverted to ${newDiversion}`,
      true
    );
  }

  const oldAircraftType = String(oldNormalized.aircraftType || "").trim().toUpperCase();
  const newAircraftType = String(newNormalized.aircraftType || "").trim().toUpperCase();
  const oldRegistration = String(oldNormalized.aircraftRegistration || "").trim().toUpperCase();
  const newRegistration = String(newNormalized.aircraftRegistration || "").trim().toUpperCase();
  if ((newAircraftType && oldAircraftType && newAircraftType !== oldAircraftType) ||
      (newRegistration && oldRegistration && newRegistration !== oldRegistration)) {
    push(
      "AIRCRAFT_CHANGED",
      "medium",
      { aircraftType: oldAircraftType || null, aircraftRegistration: oldRegistration || null },
      { aircraftType: newAircraftType || null, aircraftRegistration: newRegistration || null },
      `Aircraft changed${newAircraftType ? ` to ${newAircraftType}` : ""}`,
      true
    );
  }

  // A stale/time-inferred row may already say `enroute` before the aircraft
  // actually takes off. Emit the real takeoff event when positive telemetry or
  // the actual OFF timestamp first appears, even if the status string is unchanged.
  if (!emittedAirborneEvent && !hasPositiveTakeoffEvidence(oldState) && hasPositiveTakeoffEvidence(newState)) {
    push(
      "AIRBORNE",
      "medium",
      { status: oldStatus, takeoffConfirmed: false },
      { status: newStatus, takeoffConfirmed: true },
      "Flight is airborne",
      true
    );
  }

  const delayMinutes = minutesBetween(oldState.estimated_departure_at || oldState.scheduled_departure_at, newState.estimated_departure_at || newState.scheduled_departure_at);
  if (delayMinutes != null && delayMinutes >= 15) {
    push("DELAYED", delayMinutes >= 60 ? "high" : "medium", { estimatedDepartureAt: oldState.estimated_departure_at }, { estimatedDepartureAt: newState.estimated_departure_at }, `Departure delayed by ${delayMinutes} minutes`, true);
  } else if (delayMinutes != null && Math.abs(delayMinutes) >= 5) {
    push("RESCHEDULED", "medium", { estimatedDepartureAt: oldState.estimated_departure_at }, { estimatedDepartureAt: newState.estimated_departure_at }, "Flight schedule changed", Math.abs(delayMinutes) >= 15);
  }

  if ((oldState.gate || null) !== (newState.gate || null) && newState.gate) {
    push("GATE_CHANGED", "medium", { gate: oldState.gate || null }, { gate: newState.gate }, `Gate changed from ${oldState.gate || "unknown"} to ${newState.gate}`, within24h);
  }
  if ((oldState.terminal || null) !== (newState.terminal || null) && newState.terminal) {
    push("TERMINAL_CHANGED", "medium", { terminal: oldState.terminal || null }, { terminal: newState.terminal }, `Terminal changed from ${oldState.terminal || "unknown"} to ${newState.terminal}`, within24h);
  }
  const oldBaggageBelt = String(oldState.baggage_belt || "").trim();
  const newBaggageBelt = String(newState.baggage_belt || "").trim();
  const enteredTerminalArrivalState =
    ["taxi_in", "landed", "arrived", "arrived_at_gate"].includes(newStatus) &&
    !["taxi_in", "landed", "arrived", "arrived_at_gate"].includes(oldStatus);

  if (newBaggageBelt && oldBaggageBelt !== newBaggageBelt) {
    const changed = Boolean(oldBaggageBelt);
    push(
      "BAGGAGE_BELT_ASSIGNED",
      "low",
      { baggageBelt: oldBaggageBelt || null },
      { baggageBelt: newBaggageBelt },
      changed
        ? `Baggage belt changed from ${oldBaggageBelt} to ${newBaggageBelt}`
        : `Baggage belt assigned: ${newBaggageBelt}`,
      reliableBaggageWindow
    );
  } else if (newBaggageBelt && enteredTerminalArrivalState) {
    push(
      "BAGGAGE_BELT_ASSIGNED",
      "low",
      { baggageBelt: null },
      { baggageBelt: newBaggageBelt },
      `Baggage belt assigned: ${newBaggageBelt}`,
      true
    );
  }

  return events;
}

function hasPositiveTakeoffEvidence(state) {
  const normalized = state?.normalized_data || state?.normalizedData || state || {};
  const actualTakeoff =
    normalized?.takeoffTimes?.actual ||
    state?.takeoffTimes?.actual ||
    state?.actual_takeoff_at ||
    state?.actualTakeoffAt;
  if (actualTakeoff) return true;

  const position = normalized?.livePosition || state?.livePosition || state?.position || {};
  const altitudeFeet = Number(state?.altitude ?? position?.altitudeFeet ?? position?.altitude);
  const groundSpeedKnots = Number(state?.ground_speed ?? position?.groundSpeedKnots ?? position?.groundSpeed);
  const airGround = String(position?.airGround || position?.air_ground || "").toUpperCase();

  if (airGround === "A") return true;
  if (Number.isFinite(altitudeFeet) && altitudeFeet > 300) return true;
  return Number.isFinite(altitudeFeet) && altitudeFeet > 150 &&
    Number.isFinite(groundSpeedKnots) && groundSpeedKnots > 80;
}

function isReliableBaggageNotificationWindow(flight) {
  const status = String(flight?.status || "").trim().toLowerCase();
  return ["taxi_in", "landed", "arrived", "arrived_at_gate"].includes(status);
}

module.exports = {
  buildFlightKey,
  compareFlightState,
  deriveFlightLifecyclePhase,
  displayStatusForPhase,
  getFlightFreshnessTTL,
  isProviderAlertActive,
  isFinalStatus,
  mapNormalizedToDb,
  reconcileDiversionContext,
  isStreamingActive,
  normalizeSearchParams,
  rowToFlightResponse,
  validateProviderFlight,
};
