"use strict";

const FINAL_STATUSES = new Set(["landed", "arrived", "arrived_at_gate", "cancelled"]);
const AIRBORNE_STATUSES = new Set(["airborne", "enroute", "departed"]);
const TAXI_STATUSES = new Set(["taxiing", "taxi_out", "takeoff_roll", "taxi_in"]);

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

function getFlightFreshnessTTL(flightInstance, nowMs = Date.now(), random = Math.random, options = {}) {
  const status = String(flightInstance?.status || "").toLowerCase();
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
  const origin = normalizeAirport(normalized.origin || params.origin);
  const destination = normalizeAirport(normalized.destination || params.destination);
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
    status: String(normalized.status || "unknown").toLowerCase(),
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
    normalized_data: normalized,
    raw_provider_response: normalized.rawProviderResponse || null,
    last_fetched_at: new Date().toISOString(),
    needs_revalidation: normalized.dataConfidence === "suspicious",
    is_final: isFinalStatus(normalized.status),
  };
}

function rowToFlightResponse(row, { source = "postgres", freshness = "fresh", isRefreshing = false } = {}) {
  if (!row) return null;
  return {
    flightKey: row.flight_key,
    flightInstanceId: row.id,
    providerFlightId: row.provider_flight_id,
    airlineCode: row.airline_code,
    flightNumber: row.flight_number,
    origin: row.origin_airport,
    destination: row.destination_airport,
    status: row.status,
    statusDetail: row.status_detail,
    scheduledDepartureAt: toIso(row.scheduled_departure_at),
    scheduledArrivalAt: toIso(row.scheduled_arrival_at),
    estimatedDepartureAt: toIso(row.estimated_departure_at),
    estimatedArrivalAt: toIso(row.estimated_arrival_at),
    actualDepartureAt: toIso(row.actual_departure_at),
    actualArrivalAt: toIso(row.actual_arrival_at),
    gate: row.gate,
    terminal: row.terminal,
    departureGate: row.normalized_data?.departureGate || row.gate,
    departureTerminal: row.normalized_data?.departureTerminal || row.terminal,
    arrivalGate: row.normalized_data?.arrivalGate || null,
    arrivalTerminal: row.normalized_data?.arrivalTerminal || null,
    baggageBelt: row.baggage_belt,
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
  if (requested.destination !== "UNKNOWN" && normalizeAirport(normalized.destination) !== requested.destination) problems.push("destination_mismatch");
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
  const push = (event_type, event_severity, old_value, new_value, summary, notification_required = false) => {
    events.push({ event_type, event_severity, old_value, new_value, summary, notification_required, confidence });
  };

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

  if (newStatus !== oldStatus) {
    if (newStatus === "cancelled") push("CANCELLED", "critical", { status: oldStatus }, { status: newStatus }, "Flight has been cancelled", true);
    else if (newStatus === "diverted") push("DIVERTED", "critical", { status: oldStatus }, { status: newStatus }, "Flight has been diverted", true);
    else if (["taxiing", "taxi_out"].includes(newStatus)) push("TAXIING", "medium", { status: oldStatus }, { status: newStatus }, "Flight is taxiing", true);
    else if (newStatus === "takeoff_roll") push("TAKEOFF_ROLL", "high", { status: oldStatus }, { status: newStatus }, "Flight is about to take off", true);
    else if (newStatus === "taxi_in") push("TAXI_IN", "low", { status: oldStatus }, { status: newStatus }, "Flight is taxiing to the gate", true);
    else if (newStatus === "arrived_at_gate") push("ARRIVED_AT_GATE", "medium", { status: oldStatus }, { status: newStatus }, "Flight has arrived at the gate", true);
    else if (newStatus === "departed") push("DEPARTED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has departed", true);
    else if (["airborne", "enroute"].includes(newStatus)) push("AIRBORNE", "medium", { status: oldStatus }, { status: newStatus }, "Flight is airborne", true);
    else if (newStatus === "landed") push("LANDED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has landed", true);
    else if (newStatus === "arrived") push("ARRIVED", "medium", { status: oldStatus }, { status: newStatus }, "Flight has arrived", true);
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
  if (!oldState.baggage_belt && newState.baggage_belt) {
    push("BAGGAGE_BELT_ASSIGNED", "low", { baggageBelt: null }, { baggageBelt: newState.baggage_belt }, `Baggage belt assigned: ${newState.baggage_belt}`, true);
  }

  return events;
}

module.exports = {
  buildFlightKey,
  compareFlightState,
  getFlightFreshnessTTL,
  isProviderAlertActive,
  isFinalStatus,
  mapNormalizedToDb,
  isStreamingActive,
  normalizeSearchParams,
  rowToFlightResponse,
  validateProviderFlight,
};
