"use strict";

const { getAirportCatalog } = require("../airport-catalog");
const RUNWY_NOTIFICATION_SOUND = "RunwyNotification.caf";

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

function arrivalTime(flight) {
  const value = flight.estimated_arrival_at || flight.scheduled_arrival_at;
  if (!value) return null;
  const timeZone = airportDetails(flight.destination_airport)?.timeZoneIdentifier;
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

function weatherEmoji(conditionCode) {
  const condition = String(conditionCode || "").toLowerCase();
  if (condition.includes("thunder")) return "⛈️";
  if (condition.includes("snow") || condition.includes("blizzard")) return "❄️";
  if (condition.includes("rain") || condition.includes("drizzle")) return "🌧️";
  if (condition.includes("fog") || condition.includes("haze")) return "🌫️";
  if (condition.includes("cloud") || condition.includes("overcast")) return "☁️";
  if (condition.includes("clear") || condition.includes("sun")) return "☀️";
  return "⛅️";
}

function preflightWeather(event) {
  return event?.new_value?.weatherInsight || event?.new_value?.weather || null;
}

function arrivalWeather(flight, context = {}) {
  return context.weatherInsight || flight?.normalized_data?.weatherInsight || null;
}

function arrivalField(flight, normalizedKey, rowKey) {
  const normalized = flight?.normalized_data || {};
  return normalized[normalizedKey] ?? flight?.[rowKey] ?? null;
}

function arrivalLocalTime(flight) {
  const value =
    arrivalField(flight, "arrivalTimes", "arrival_times")?.actual ||
    flight.actual_arrival_at ||
    arrivalField(flight, "arrivalTimes", "arrival_times")?.estimated ||
    flight.estimated_arrival_at ||
    flight.scheduled_arrival_at;
  if (!value) return null;
  const timeZone = flight?.normalized_data?.arrivalTimezone || airportDetails(flight.destination_airport)?.timeZoneIdentifier;
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

function arrivalVariance(flight) {
  const scheduled = new Date(flight.scheduled_arrival_at || flight?.normalized_data?.arrivalTimes?.scheduled || "").getTime();
  const actual = new Date(flight.actual_arrival_at || flight.estimated_arrival_at || flight?.normalized_data?.arrivalTimes?.actual || "").getTime();
  if (!Number.isFinite(scheduled) || !Number.isFinite(actual)) return null;
  const minutes = Math.round((actual - scheduled) / 60_000);
  if (Math.abs(minutes) < 2) return "on time";
  return `${Math.abs(minutes)}m ${minutes < 0 ? "early" : "late"}`;
}

function arrivalTaxiMinutes(flight) {
  const landing = new Date(flight?.normalized_data?.landingTimes?.actual || "").getTime();
  const gate = new Date(flight.actual_arrival_at || flight.estimated_arrival_at || "").getTime();
  if (!Number.isFinite(landing) || !Number.isFinite(gate) || gate <= landing) return null;
  const minutes = Math.round((gate - landing) / 60_000);
  return minutes > 0 && minutes <= 120 ? minutes : null;
}

function ordinalNumber(value) {
  const number = Math.max(1, Math.round(Number(value) || 0));
  const mod100 = number % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${number}th`;
  if (number % 10 === 1) return `${number}st`;
  if (number % 10 === 2) return `${number}nd`;
  if (number % 10 === 3) return `${number}rd`;
  return `${number}th`;
}

function arrivalTitle(flight, context = {}) {
  const destination = String(flight.destination_airport || "").trim().toUpperCase();
  const city = airportDetails(destination)?.city || flight?.normalized_data?.arrivalCity || destination || "your destination";
  const weather = arrivalWeather(flight, context);
  const temperatureC = Number(weather?.temperatureC);
  const unit = String(context.temperatureUnit || "celsius").toLowerCase();
  let weatherSuffix = "";
  if (weather?.available !== false && Number.isFinite(temperatureC)) {
    const temperature = unit === "fahrenheit" || unit === "f"
      ? Math.round((temperatureC * 9) / 5 + 32)
      : Math.round(temperatureC);
    weatherSuffix = ` ${weatherEmoji(weather.conditionCode)} ${temperature}°`;
  }
  return `✈️ Welcome to ${city}!${weatherSuffix}`;
}

function arrivalBody(flight, context = {}) {
  const destination = String(flight.destination_airport || "---").trim().toUpperCase();
  const terminal = String(flight?.normalized_data?.arrivalTerminal || flight.arrival_terminal || "").trim();
  const gate = String(flight?.normalized_data?.arrivalGate || flight.arrival_gate || "").trim();
  const location = [destination, terminal ? `Terminal ${terminal}` : null, gate ? `Gate ${gate}` : null].filter(Boolean).join(" • ");
  const localTime = arrivalLocalTime(flight);
  const variance = arrivalVariance(flight);
  const taxiMinutes = arrivalTaxiMinutes(flight);
  const sentences = [];
  if (taxiMinutes) sentences.push(`Taxiing for ${taxiMinutes}m.`);
  let arrival = `Arriving at ${location}`;
  if (localTime) arrival += ` at ${localTime} local time`;
  if (variance) arrival += ` (${variance})`;
  sentences.push(`${arrival}.`);
  if (Number(context.visitOrdinal) > 0) {
    sentences.push(`This is your ${ordinalNumber(context.visitOrdinal)} time here.`);
  }
  return sentences.join(" ");
}

function temperatureText(event, context = {}) {
  const temperatureC = Number(preflightWeather(event)?.temperatureC);
  if (!Number.isFinite(temperatureC)) return null;
  const unit = String(context.temperatureUnit || "celsius").toLowerCase();
  if (unit === "fahrenheit" || unit === "f") {
    return `${Math.round((temperatureC * 9) / 5 + 32)}°F`;
  }
  return `${Math.round(temperatureC)}°C`;
}

function preflightTitle(flight, event, context = {}) {
  const origin = String(flight.origin_airport || "---").trim().toUpperCase();
  const destination = String(flight.destination_airport || "---").trim().toUpperCase();
  const temperature = temperatureText(event, context);
  const condition = preflightWeather(event)?.conditionCode;
  return `${origin} ✈️ ${destination}${temperature ? ` · ${temperature} ${weatherEmoji(condition)}` : ""}`;
}

function airportDetailLine(flight, event) {
  const origin = String(flight.origin_airport || "---").trim().toUpperCase();
  const destination = String(flight.destination_airport || "---").trim().toUpperCase();
  const terminal = String(flight.departure_terminal || flight.terminal || "").trim();
  const gate = String(flight.departure_gate || flight.gate || "").trim();
  const departure = departureTime(flight, event);
  const arrival = arrivalTime(flight);
  const departureDetails = [terminal ? `Terminal ${terminal}` : null, gate ? `Gate ${gate}` : null]
    .filter(Boolean)
    .join(" · ");
  const departureSuffix = [departureDetails, departure ? `at ${departure}` : null].filter(Boolean).join(" ");
  return `↗ ${origin}${departureSuffix ? ` ${departureSuffix}` : ""}\n↘ ${destination}${arrival ? ` at ${arrival}` : ""}`;
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
  if (hour >= 5 && hour < 12) return "Good morning";
  if (hour >= 12 && hour < 17) return "Good afternoon";
  return "Good evening";
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
  if (event.event_type === "INBOUND_DEPARTED") return "✈️ Your Aircraft Is on the Way";
  if (event.event_type === "INBOUND_CANCELLED") return "Inbound Aircraft Cancelled";
  if (event.event_type === "INBOUND_DIVERTED") return "Inbound Aircraft Diverted";
  if (event.event_type === "TRIP_STARTING") {
    return preflightTitle(flight, event, context);
  }
  if (event.event_type === "TAXI_IN") return "Taxiing In";
  if (event.event_type === "ARRIVED_AT_GATE") return "Arrived at Gate";
  if (event.event_type === "BAGGAGE_BELT_ASSIGNED") {
    return event.old_value?.baggageBelt ? "🧳 Baggage Belt Changed" : "🧳 Baggage Belt Assigned";
  }
  if (event.event_type === "WEATHER_ADVISORY") return "Weather Update";
  if (event.event_type === "LANDED" || event.event_type === "ARRIVED") {
    return context.isTraveler && !context.isCircle ? arrivalTitle(flight, context) : "✈️ Flight Landed";
  }
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
  if (["INBOUND_DEPARTED", "INBOUND_CANCELLED", "INBOUND_DIVERTED"].includes(event.event_type)) {
    const inbound = event.new_value?.inboundFlight || flight?.normalized_data?.inboundFlight || {};
    const inboundCode = String(inbound.flightNumber || "Your inbound aircraft").trim();
    const origin = String(inbound.originAirportIata || "its previous airport").trim().toUpperCase();
    const destination = String(flight.origin_airport || inbound.destinationAirportIata || "your departure airport").trim().toUpperCase();
    if (event.event_type === "INBOUND_CANCELLED") return `${inboundCode}, the inbound aircraft for ${code}, was cancelled.`;
    if (event.event_type === "INBOUND_DIVERTED") return `${inboundCode}, the inbound aircraft for ${code}, was diverted.`;
    const arrivalMs = new Date(inbound.estimatedArrival || "").getTime();
    const minutes = Number.isFinite(arrivalMs) ? Math.max(0, Math.round((arrivalMs - Date.now()) / 60_000)) : null;
    const eta = minutes == null
      ? ""
      : minutes >= 60
        ? ` in ${Math.floor(minutes / 60)}h${minutes % 60 ? ` ${minutes % 60}m` : ""}`
        : ` in ${Math.max(1, minutes)}m`;
    return `${inboundCode} has taken off from ${origin} and is expected at ${destination}${eta}.`;
  }
  if (event.event_type === "TRIP_STARTING") {
    const time = departureTime(flight, event);
    const traveler = firstName(context.ownerDisplayName);
    const recipient = firstName(context.recipientDisplayName);
    if (context.isCircle && traveler) {
      const greeting = recipient ? `Hey ${recipient}, ` : "Hey! ";
      const journey = route || [flight.origin_airport, flight.destination_airport].filter(Boolean).join(" to ");
      return `${greeting}today ${traveler} has a flight${journey ? ` from ${journey}` : ""}, scheduled${time ? ` at ${time} local time` : " for today"}.\n${code} · ${String(flight.origin_airport || "---").toUpperCase()} → ${String(flight.destination_airport || "---").toUpperCase()}`;
    }
    const delayed = String(flight.status || "").toLowerCase().includes("delay") || Number(flight.delay_minutes) > 0;
    return `${greetingForFlight(flight, event)}! Your flight today is ${delayed ? "delayed" : "on time"}. ${code}\n${airportDetailLine(flight, event)}`;
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
  if (event.event_type === "LANDED" || event.event_type === "ARRIVED") {
    return context.isTraveler && !context.isCircle
      ? arrivalBody(flight, context)
      : `${subject}${route ? `, ${route},` : ""} has landed.`;
  }
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
      sound: RUNWY_NOTIFICATION_SOUND,
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
