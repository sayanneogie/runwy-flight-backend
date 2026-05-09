"use strict";

function createProviderAdapter({ providerName, fetchFlights, fetchByProviderId, normalizeRecord, normalizeSelected, enrichNormalized, selectRecord }) {
  return {
    name: providerName,
    supportsProviderId: Boolean(fetchByProviderId),
    async fetchFlightByNumber(params) {
      const query = {
        flightNumber: `${params.airline}${params.number}`,
        date: params.date,
        departureIata: params.origin === "UNKNOWN" ? null : params.origin,
        arrivalIata: params.destination === "UNKNOWN" ? null : params.destination,
      };
      const records = await fetchFlights(query);
      const selected = selectRecord ? selectRecord(records, query, normalizeRecord) : records?.[0];
      if (!selected) return null;
      const normalized = normalizeSelected
        ? await normalizeSelected(selected, records, query, params)
        : normalizeRecord(selected);
      const enriched = enrichNormalized ? await enrichNormalized(normalized, selected, query, params) : normalized;
      return normalizeProviderRecord(selected, () => enriched, providerName, params);
    },
    async fetchFlightByProviderId(providerFlightId) {
      if (!fetchByProviderId) throw new Error("fetchFlightByProviderId is not configured");
      const record = await fetchByProviderId(providerFlightId);
      if (!record) return null;
      const normalized = normalizeRecord(record);
      const enriched = enrichNormalized ? await enrichNormalized(normalized, record, {}, {}) : normalized;
      return normalizeProviderRecord(record, () => enriched, providerName, {});
    },
  };
}

function normalizeProviderRecord(record, normalizeRecord, providerName, params) {
  const normalized = normalizeRecord(record);
  const airlineCode = normalized.airlineCode || String(params.airline || normalized.flightNumber || "").slice(0, 2).toUpperCase();
  const rawFlightNumber = String(normalized.flightNumber || `${params.airline || ""}${params.number || ""}`).toUpperCase();
  const flightNumber = rawFlightNumber.startsWith(airlineCode)
    ? rawFlightNumber.slice(airlineCode.length)
    : rawFlightNumber.replace(/^[A-Z]+/, "");
  return {
    flightKey: null,
    providerFlightId: normalized.providerFlightId || record?.fa_flight_id || record?.flight_id || null,
    airlineCode,
    flightNumber,
    origin: normalized.departureAirportIata || normalized.origin || params.origin || null,
    destination: normalized.arrivalAirportIata || normalized.destination || params.destination || null,
    status: normalized.status || "unknown",
    statusDetail: normalized.statusDetail || null,
    scheduledDepartureAt: normalized.departureTimes?.scheduled || normalized.scheduledDepartureAt || null,
    scheduledArrivalAt: normalized.arrivalTimes?.scheduled || normalized.scheduledArrivalAt || null,
    estimatedDepartureAt: normalized.departureTimes?.estimated || normalized.takeoffTimes?.estimated || normalized.estimatedDepartureAt || null,
    estimatedArrivalAt: normalized.arrivalTimes?.estimated || normalized.landingTimes?.estimated || normalized.estimatedArrivalAt || null,
    actualDepartureAt: normalized.departureTimes?.actual || normalized.takeoffTimes?.actual || normalized.actualDepartureAt || null,
    actualArrivalAt: normalized.arrivalTimes?.actual || normalized.landingTimes?.actual || normalized.actualArrivalAt || null,
    gate: normalized.departureGate || normalized.gate || null,
    terminal: normalized.departureTerminal || normalized.terminal || null,
    departureGate: normalized.departureGate || normalized.gate || null,
    departureTerminal: normalized.departureTerminal || normalized.terminal || null,
    arrivalGate: normalized.arrivalGate || null,
    arrivalTerminal: normalized.arrivalTerminal || null,
    baggageBelt: normalized.baggageBelt || normalized.baggageClaim || null,
    position: {
      lat: normalized.livePosition?.latitude ?? normalized.position?.lat ?? null,
      lon: normalized.livePosition?.longitude ?? normalized.position?.lon ?? null,
      altitude: normalized.livePosition?.altitudeFeet ?? normalized.position?.altitude ?? null,
      groundSpeed: normalized.livePosition?.groundSpeedKnots ?? normalized.livePosition?.groundspeedKnots ?? normalized.position?.groundSpeed ?? null,
      heading: normalized.livePosition?.headingDegrees ?? normalized.livePosition?.heading ?? normalized.position?.heading ?? null,
    },
    provider: providerName,
    dataConfidence: normalized.dataConfidence || "medium",
    rawProviderResponse: record,
  };
}

module.exports = { createProviderAdapter, normalizeProviderRecord };
