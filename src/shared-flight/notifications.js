"use strict";

const { getAirportCatalog } = require("../airport-catalog");

function createApnsSender({ send } = {}) {
  return {
    async sendFlightEvent({ token, flight, event, context = {} }) {
      if (!send) return { ok: true, skipped: true };
      const payload = notificationPayload(flight, event, context);
      return send({ token: token.device_token || token.apnsToken, payload, environment: token.environment });
    },
  };
}

function readableFlightCode(flight) {
  const airline = String(flight.airline_code || "").trim().toUpperCase();
  const number = String(flight.flight_number || "").trim().toUpperCase();
  return `${airline}${airline && number ? " " : ""}${number}` || "Flight";
}

function firstName(value) {
  return String(value || "").trim().split(/\s+/)[0] || null;
}

function airportDetails(iata) {
  const code = String(iata || "").trim().toUpperCase();
  if (!code) return null;
  try {
    return getAirportCatalog().airports.find((airport) => airport.code === code) || null;
  } catch (_error) {
    return null;
  }
}

function routeDescription(flight) {
  const originCode = String(flight.origin_airport || "").trim().toUpperCase();
  const destinationCode = String(flight.destination_airport || "").trim().toUpperCase();
  const origin = airportDetails(originCode)?.city || originCode;
  const destination = airportDetails(destinationCode)?.city || destinationCode;
  return origin && destination ? `${origin} to ${destination}` : null;
}

function departureTime(flight, event) {
  const value =
    event?.new_value?.scheduledDepartureAt ||
    event?.new_value?.estimatedDepartureAt ||
    flight.estimated_departure_at ||
    flight.scheduled_departure_at;
  if (!value) return null;
  const timeZone = airportDetails(flight.origin_airport)?.timeZoneIdentifier;
  try {
    return new Intl.DateTimeFormat("en-US", {
      ...(timeZone ? { timeZone } : {}),
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(value));
  } catch (_error) {
    return null;
  }
}

function greetingForFlight(flight, event) {
  const value =
    event?.provider_event_time ||
    event?.created_at ||
    new Date().toISOString();
  const timeZone = airportDetails(flight.origin_airport)?.timeZoneIdentifier;
  let hour = new Date(value).getHours();
  try {
    if (timeZone) {
      hour = Number(new Intl.DateTimeFormat("en-US", {
        timeZone,
        hour: "numeric",
        hourCycle: "h23",
      }).format(new Date(value)));
    }
  } catch (_error) {
    // Fall back to the server-local hour when an airport timezone is unavailable.
  }
  if (hour >= 5 && hour < 12) return "Good morning ⛅️";
  if (hour >= 12 && hour < 17) return "Good afternoon ⛅️";
  return "Good evening ✨";
}

function notificationSubject(flight, context = {}) {
  const code = readableFlightCode(flight);
  const traveler = firstName(context.ownerDisplayName);
  return context.isCircle && traveler ? `${traveler}'s flight ${code}` : `Flight ${code}`;
}

function notificationTitle(flight, event, context = {}) {
  const code = `${flight.airline_code || ""}${flight.flight_number || ""}` || "Flight";
  if (event.event_type === "DELAYED") return "Flight Delayed";
  if (event.event_type === "CANCELLED") return "Flight Cancelled";
  if (event.event_type === "GATE_CHANGED") return "Gate Changed";
  if (event.event_type === "TAXIING") return "Taxiing";
  if (event.event_type === "TAKEOFF_ROLL") return "✈️ Taking Off";
  if (event.event_type === "TRIP_STARTING") {
    const traveler = firstName(context.ownerDisplayName);
    return context.isCircle && traveler
      ? `✈️ ${traveler} has a flight today`
      : greetingForFlight(flight, event);
  }
  if (event.event_type === "TAXI_IN") return "Taxiing In";
  if (event.event_type === "ARRIVED_AT_GATE") return "Arrived at Gate";
  if (event.event_type === "BAGGAGE_BELT_ASSIGNED") {
    return event.old_value?.baggageBelt ? "🧳 Baggage Belt Changed" : "🧳 Baggage Belt Assigned";
  }
  if (event.event_type === "WEATHER_ADVISORY") return "Weather Update";
  if (event.event_type === "LANDED" || event.event_type === "ARRIVED") return "✈️ Flight Landed";
  if (event.event_type === "AIRBORNE" || event.event_type === "DEPARTED") return "✈️ Flight Took Off";
  return code;
}

function notificationBody(flight, event, context = {}) {
  const code = readableFlightCode(flight);
  const subject = notificationSubject(flight, context);
  const route = routeDescription(flight);
  if (event.event_type === "DELAYED") return `${code} is delayed.`;
  if (event.event_type === "CANCELLED") return `${code} has been cancelled.`;
  if (event.event_type === "GATE_CHANGED") return `${code} gate changed from ${event.old_value?.gate || "unknown"} to ${event.new_value?.gate}.`;
  if (event.event_type === "TAXIING") return `${subject} is taxiing.`;
  if (event.event_type === "TAKEOFF_ROLL") return `${subject}${route ? `, ${route},` : ""} is about to take off.`;
  if (event.event_type === "TRIP_STARTING") {
    const time = departureTime(flight, event);
    const traveler = firstName(context.ownerDisplayName);
    const lead = context.isCircle && traveler
      ? `${traveler} has a flight scheduled for today.`
      : "You have a flight today.";
    return `${lead} ${code}${route ? `, ${route},` : ""} is scheduled to depart${time ? ` at ${time} local time` : " soon"}.`;
  }
  if (event.event_type === "TAXI_IN") return `${code} is taxiing to the gate.`;
  if (event.event_type === "ARRIVED_AT_GATE") return `${code} has arrived at the gate.`;
  if (event.event_type === "BAGGAGE_BELT_ASSIGNED") {
    const previousBelt = event.old_value?.baggageBelt;
    const nextBelt = event.new_value?.baggageBelt;
    const traveler = firstName(context.ownerDisplayName);
    const luggageOwner = context.isCircle && traveler ? `${traveler}'s luggage` : "Your luggage";
    return previousBelt
      ? `${luggageOwner} for flight ${code} changed from belt ${previousBelt} to belt ${nextBelt}.`
      : `${luggageOwner} for flight ${code} will be on belt ${nextBelt}.`;
  }
  if (event.event_type === "WEATHER_ADVISORY") return event.summary || `${code} weather update is available.`;
  if (event.event_type === "LANDED" || event.event_type === "ARRIVED") return `${subject}${route ? `, ${route},` : ""} has landed.`;
  if (event.event_type === "AIRBORNE" || event.event_type === "DEPARTED") return `${subject}${route ? `, ${route},` : ""} is now in the air.`;
  return event.summary || `${subject} status changed.`;
}

function notificationPayload(flight, event, context = {}) {
  return {
    aps: {
      alert: {
        title: notificationTitle(flight, event, context),
        body: notificationBody(flight, event, context),
      },
      sound: "default",
    },
    flight_instance_id: flight.id,
    flight_event_id: event.id,
    event_type: event.event_type,
    deep_link: `runwy://flights/${flight.id}`,
  };
}

module.exports = {
  createApnsSender,
  notificationBody,
  notificationPayload,
  notificationTitle,
};
