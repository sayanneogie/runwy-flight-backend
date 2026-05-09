"use strict";

const crypto = require("node:crypto");

function extractFlightAwareAlertEvents(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.events)) return body.events;
  if (Array.isArray(body?.alerts)) return body.alerts;
  if (Array.isArray(body?.flights)) return body.flights;
  if (body && typeof body === "object") return [body];
  return [];
}

function normalizeFlightAwareAlert(rawPayload) {
  const event = rawPayload?.flight && typeof rawPayload.flight === "object"
    ? { ...rawPayload.flight, ...rawPayload }
    : rawPayload || {};

  const ident = cleanIdent(
    firstPresent(
      event.ident_iata,
      event.ident,
      event.flightNumber,
      event.flight_number,
      event.fa_ident,
      event.flight?.ident_iata,
      event.flight?.ident
    )
  );
  const parsed = parseIdent(ident);
  const origin = airportCode(firstPresent(event.origin, event.departure, event.origin_airport, event.departure_airport, event.origin_iata, event.departure_iata));
  const destination = airportCode(firstPresent(event.destination, event.arrival, event.destination_airport, event.arrival_airport, event.destination_iata, event.arrival_iata));
  const scheduledOut = iso(firstPresent(event.scheduled_out, event.scheduledOff, event.scheduled_off, event.scheduled_departure_at));
  const estimatedOut = iso(firstPresent(event.estimated_out, event.estimatedOff, event.estimated_off, event.estimated_departure_at));
  const actualOut = iso(firstPresent(event.actual_out, event.actualOff, event.actual_off, event.actual_departure_at));
  const scheduledIn = iso(firstPresent(event.scheduled_in, event.scheduledOn, event.scheduled_in, event.scheduled_arrival_at));
  const estimatedIn = iso(firstPresent(event.estimated_in, event.estimatedOn, event.estimated_in, event.estimated_arrival_at));
  const actualIn = iso(firstPresent(event.actual_in, event.actualOn, event.actual_in, event.actual_arrival_at));
  const eventTime = iso(firstPresent(event.event_time, event.eventTime, event.timestamp, event.occurred_at, event.occurredAt, actualOut, actualIn, estimatedOut, estimatedIn));
  const departureDate = dateOnly(firstPresent(event.departure_date, event.flight_date, event.date, scheduledOut, estimatedOut, actualOut, eventTime));
  const rawType = String(firstPresent(event.event, event.type, event.alert_type, event.alertType, event.status, event.event_status, event.eventStatus) || "").toLowerCase();
  const eventType = normalizeEventType(rawType, event);
  const flightKey = parsed.airlineCode && parsed.flightNumber && departureDate
    ? `${parsed.airlineCode}-${parsed.flightNumber}-${departureDate}-${origin || "UNKNOWN"}-${destination || "UNKNOWN"}`
    : null;
  const delayMinutes = numeric(firstPresent(event.delay_minutes, event.delayMinutes, event.departure_delay, event.arrival_delay));
  const gateOrigin = text(firstPresent(event.gate_origin, event.departure_gate, event.gate, event.gateOut, event.terminal_gate_origin));
  const gateDestination = text(firstPresent(event.gate_destination, event.arrival_gate, event.gateIn, event.arrivalGate));
  const terminalOrigin = text(firstPresent(event.terminal_origin, event.departure_terminal, event.terminal, event.terminalOut));
  const terminalDestination = text(firstPresent(event.terminal_destination, event.arrival_terminal, event.terminalIn, event.arrivalTerminal));

  return {
    event_type: eventType,
    event_status: text(firstPresent(event.status, event.event_status, event.eventStatus)),
    flight_key: flightKey,
    fa_flight_id: text(firstPresent(event.fa_flight_id, event.faFlightId, event.faFlightID, event.flight_id, event.flightId)),
    ident,
    airlineCode: parsed.airlineCode,
    flightNumber: parsed.flightNumber,
    origin,
    destination,
    departureDate,
    scheduled_out: scheduledOut,
    estimated_out: estimatedOut,
    actual_out: actualOut,
    scheduled_in: scheduledIn,
    estimated_in: estimatedIn,
    actual_in: actualIn,
    gate_origin: gateOrigin,
    gate_destination: gateDestination,
    terminal_origin: terminalOrigin,
    terminal_destination: terminalDestination,
    baggage_belt: text(firstPresent(event.baggage_belt, event.baggage_claim, event.bag_claim, event.baggageClaim)),
    delay_minutes: delayMinutes,
    event_time: eventTime,
    human_readable_summary: summaryForEvent(eventType, ident, { origin, destination, delayMinutes, gateOrigin, gateDestination }),
  };
}

function flightUpdateFromAlert(row, alert) {
  const status = statusForAlert(alert.event_type, row.status);
  return {
    providerFlightId: alert.fa_flight_id || row.provider_flight_id || null,
    airlineCode: row.airline_code || alert.airlineCode,
    flightNumber: row.flight_number || alert.flightNumber,
    origin: row.origin_airport || alert.origin,
    destination: row.destination_airport || alert.destination,
    status,
    statusDetail: alert.human_readable_summary,
    scheduledDepartureAt: alert.scheduled_out || row.scheduled_departure_at,
    scheduledArrivalAt: alert.scheduled_in || row.scheduled_arrival_at,
    estimatedDepartureAt: alert.estimated_out || row.estimated_departure_at,
    estimatedArrivalAt: alert.estimated_in || row.estimated_arrival_at,
    actualDepartureAt: alert.actual_out || row.actual_departure_at,
    actualArrivalAt: alert.actual_in || row.actual_arrival_at,
    departureGate: alert.gate_origin || row.normalized_data?.departureGate || row.gate,
    departureTerminal: alert.terminal_origin || row.normalized_data?.departureTerminal || row.terminal,
    arrivalGate: alert.gate_destination || row.normalized_data?.arrivalGate || null,
    arrivalTerminal: alert.terminal_destination || row.normalized_data?.arrivalTerminal || null,
    gate: alert.gate_origin || row.gate,
    terminal: alert.terminal_origin || row.terminal,
    baggageBelt: alert.baggage_belt || row.baggage_belt,
    position: {
      lat: row.position_lat,
      lon: row.position_lon,
      altitude: row.altitude,
      groundSpeed: row.ground_speed,
      heading: row.heading,
    },
    provider: "flightaware",
    dataConfidence: "high",
    rawProviderResponse: { flightAwareAlert: alert },
  };
}

function generateFlightAwareAlertDedupeKey(alert, rawPayload) {
  const identity = alert.fa_flight_id || alert.flight_key || alert.ident || "unknown-flight";
  const eventTime = alert.event_time || alert.actual_out || alert.actual_in || alert.estimated_out || alert.estimated_in || "unknown-time";
  const importantFields = {
    event_type: alert.event_type,
    event_status: alert.event_status,
    actual_out: alert.actual_out,
    actual_in: alert.actual_in,
    estimated_out: alert.estimated_out,
    estimated_in: alert.estimated_in,
    gate_origin: alert.gate_origin,
    gate_destination: alert.gate_destination,
    baggage_belt: alert.baggage_belt,
    delay_minutes: alert.delay_minutes,
    raw_hint: rawPayload?.id || rawPayload?.alert_id || rawPayload?.sequence || null,
  };
  const hash = crypto.createHash("sha256").update(stableStringify(importantFields)).digest("hex").slice(0, 16);
  return `flightaware:${identity}:${alert.event_type}:${eventTime}:${hash}`;
}

function targetMatchesAlert(row, alert) {
  if (!row || !alert) return false;
  if (alert.departureDate && String(row.departure_date || "").slice(0, 10) !== alert.departureDate) return false;
  if (alert.origin && row.origin_airport && row.origin_airport !== alert.origin) return false;
  if (alert.destination && row.destination_airport && row.destination_airport !== alert.destination) return false;
  return true;
}

function normalizeEventType(rawType, event) {
  const joined = `${rawType} ${event?.summary || ""} ${event?.description || ""}`.toLowerCase();
  if (joined.includes("cancel")) return "flight_cancelled";
  if (joined.includes("divert")) return "flight_diverted";
  if (joined.includes("hold")) return "flight_hold";
  if (joined.includes("airport") && joined.includes("delay")) return "airport_delay";
  if (joined.includes("gate")) return "gate_changed";
  if (joined.includes("schedule") || joined.includes("time_change") || joined.includes("reschedule")) return "schedule_changed";
  if (joined.includes("delay")) return "flight_delayed";
  if (joined.includes("takeoff") || joined.includes("off") || joined.includes("depart")) return "flight_departed";
  if (joined.includes("arriv") || joined.includes("land") || joined.includes(" in")) return "flight_arrived";
  if (joined.includes("taxi")) return "flight_taxiing";
  return "unknown_flight_event";
}

function statusForAlert(eventType, fallback) {
  if (eventType === "flight_cancelled") return "cancelled";
  if (eventType === "flight_diverted") return "diverted";
  if (eventType === "flight_departed") return "airborne";
  if (eventType === "flight_arrived") return "landed";
  if (eventType === "flight_taxiing") return "taxiing";
  return fallback || "scheduled";
}

function summaryForEvent(eventType, ident, details) {
  const code = ident || "Flight";
  if (eventType === "flight_departed") return `${code} has taken off.`;
  if (eventType === "flight_arrived") return `${code} has landed.`;
  if (eventType === "flight_cancelled") return `${code} was cancelled.`;
  if (eventType === "flight_delayed") return `${code} is delayed${details.delayMinutes ? ` by ${details.delayMinutes} minutes` : ""}.`;
  if (eventType === "gate_changed") return `${code} gate changed${details.gateOrigin ? ` to ${details.gateOrigin}` : ""}.`;
  if (eventType === "flight_diverted") return `${code} was diverted.`;
  return `${code} status changed.`;
}

function parseIdent(ident) {
  const value = cleanIdent(ident);
  const match = value.match(/^([A-Z]{2,3})(\d+[A-Z]?)$/);
  return {
    airlineCode: match?.[1] || null,
    flightNumber: match?.[2] || null,
  };
}

function airportCode(value) {
  const raw = typeof value === "object" && value !== null
    ? firstPresent(value.code_iata, value.iata, value.code, value.airport_code, value.airportCode)
    : value;
  const normalized = String(raw || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
  return normalized ? normalized.slice(0, 3) : null;
}

function cleanIdent(value) {
  return String(value || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function text(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function numeric(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function iso(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function dateOnly(value) {
  const normalized = iso(value) || String(value || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized.slice(0, 10)) ? normalized.slice(0, 10) : null;
}

function firstPresent(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function stableStringify(value) {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

module.exports = {
  extractFlightAwareAlertEvents,
  flightUpdateFromAlert,
  generateFlightAwareAlertDedupeKey,
  normalizeFlightAwareAlert,
  targetMatchesAlert,
};
