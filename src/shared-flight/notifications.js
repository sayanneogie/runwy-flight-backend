"use strict";

function createApnsSender({ send } = {}) {
  return {
    async sendFlightEvent({ token, flight, event }) {
      if (!send) return { ok: true, skipped: true };
      const payload = {
        aps: {
          alert: {
            title: `${flight.airline_code}${flight.flight_number}`,
            body: notificationBody(flight, event),
          },
          sound: "default",
        },
        flight_instance_id: flight.id,
        flight_event_id: event.id,
        event_type: event.event_type,
        deep_link: `runwy://flights/${flight.id}`,
      };
      return send({ token: token.device_token || token.apnsToken, payload, environment: token.environment });
    },
  };
}

function notificationBody(flight, event) {
  const code = `${flight.airline_code}${flight.flight_number}`;
  if (event.event_type === "DELAYED") return `${code} is delayed.`;
  if (event.event_type === "CANCELLED") return `${code} has been cancelled.`;
  if (event.event_type === "GATE_CHANGED") return `${code} gate changed from ${event.old_value?.gate || "unknown"} to ${event.new_value?.gate}.`;
  if (event.event_type === "TAXIING") return `${code} is taxiing.`;
  if (event.event_type === "TAKEOFF_ROLL") return `${code} is about to take off.`;
  if (event.event_type === "TAXI_IN") return `${code} is taxiing to the gate.`;
  if (event.event_type === "ARRIVED_AT_GATE") return `${code} has arrived at the gate.`;
  if (event.event_type === "BAGGAGE_BELT_ASSIGNED") return `${code} bags are expected at belt ${event.new_value?.baggageBelt}.`;
  if (event.event_type === "WEATHER_ADVISORY") return event.summary || `${code} weather update is available.`;
  if (event.event_type === "LANDED" || event.event_type === "ARRIVED") return `${code} has landed in ${flight.destination_airport || "the destination"}.`;
  return event.summary || `${code} status changed.`;
}

module.exports = { createApnsSender, notificationBody };
