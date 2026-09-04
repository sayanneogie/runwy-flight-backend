"use strict";

function toEpochMillisOrZero(value) {
  const epochMs = new Date(value || "").getTime();
  return Number.isFinite(epochMs) ? epochMs : 0;
}

function normalizedProgressPercent(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  if (!Number.isFinite(number)) {
    return null;
  }

  return Math.max(0, Math.min(number, 100));
}

function isTerminalStatus(status) {
  return ["landed", "arrived", "arrived_at_gate", "cancelled"].includes(
    String(status || "").toLowerCase()
  );
}

function choosePreferredLivePosition(nextLivePosition, previousLivePosition) {
  if (!nextLivePosition) {
    return previousLivePosition || null;
  }

  if (!previousLivePosition) {
    return nextLivePosition;
  }

  const nextRecordedAtMs = toEpochMillisOrZero(nextLivePosition.recordedAt);
  const previousRecordedAtMs = toEpochMillisOrZero(previousLivePosition.recordedAt);

  if (previousRecordedAtMs > nextRecordedAtMs) {
    return previousLivePosition;
  }

  return nextLivePosition;
}

function normalizedTrackPoints(trackPoints) {
  if (!Array.isArray(trackPoints)) {
    return [];
  }

  return trackPoints
    .filter((point) =>
      point &&
      typeof point === "object" &&
      Number.isFinite(Number(point.latitude)) &&
      Number.isFinite(Number(point.longitude)) &&
      Math.abs(Number(point.latitude)) <= 90 &&
      Math.abs(Number(point.longitude)) <= 180 &&
      !(Math.abs(Number(point.latitude)) < 0.0001 && Math.abs(Number(point.longitude)) < 0.0001)
    )
    .sort(
      (left, right) =>
        toEpochMillisOrZero(left?.recordedAt) - toEpochMillisOrZero(right?.recordedAt)
    );
}

function choosePreferredTrackPoints(nextTrackPoints, previousTrackPoints) {
  const next = normalizedTrackPoints(nextTrackPoints);
  const previous = normalizedTrackPoints(previousTrackPoints);

  if (!next.length) {
    return previous.length ? previous : null;
  }

  if (!previous.length) {
    return next;
  }

  // Provider refreshes and Firehose batches are frequently incremental rather
  // than complete. Replacing the prior trail merely because a shorter batch has
  // a newer timestamp discards the already-flown path at exactly the moment a
  // flight lands. Treat the trail as append-only and reconcile overlapping
  // points instead.
  const merged = [...previous, ...next].sort(
    (left, right) =>
      toEpochMillisOrZero(left?.recordedAt) - toEpochMillisOrZero(right?.recordedAt)
  );
  const seen = new Set();
  const deduplicated = [];
  for (const point of merged) {
    const latitude = Number(point.latitude);
    const longitude = Number(point.longitude);
    const recordedAt = String(point.recordedAt || "");
    const key = `${recordedAt}|${latitude.toFixed(5)}|${longitude.toFixed(5)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduplicated.push(point);
  }
  return deduplicated;
}

function mergeInboundFlightTelemetry(nextInbound, previousInbound) {
  if (!nextInbound || typeof nextInbound !== "object") {
    return previousInbound || null;
  }
  if (!previousInbound || typeof previousInbound !== "object") {
    return nextInbound;
  }

  const normalizedIdentity = (value) => String(value || "").trim().toUpperCase();
  const nextProviderId = normalizedIdentity(nextInbound.providerFlightId);
  const previousProviderId = normalizedIdentity(previousInbound.providerFlightId);
  const nextFlightNumber = normalizedIdentity(nextInbound.flightNumber);
  const previousFlightNumber = normalizedIdentity(previousInbound.flightNumber);
  const assignmentChanged =
    (nextProviderId && previousProviderId && nextProviderId !== previousProviderId) ||
    (!nextProviderId && !previousProviderId && nextFlightNumber && previousFlightNumber &&
      nextFlightNumber !== previousFlightNumber);

  if (assignmentChanged) {
    return nextInbound;
  }

  return {
    ...previousInbound,
    ...nextInbound,
    flightNumber: nextInbound.flightNumber || previousInbound.flightNumber || null,
    providerFlightId: nextInbound.providerFlightId || previousInbound.providerFlightId || null,
    originAirportIata:
      nextInbound.originAirportIata || previousInbound.originAirportIata || null,
    destinationAirportIata:
      nextInbound.destinationAirportIata || previousInbound.destinationAirportIata || null,
    estimatedArrival:
      nextInbound.estimatedArrival || previousInbound.estimatedArrival || null,
    estimatedDeparture:
      nextInbound.estimatedDeparture || previousInbound.estimatedDeparture || null,
    actualDeparture: nextInbound.actualDeparture || previousInbound.actualDeparture || null,
    status: nextInbound.status || previousInbound.status || null,
    livePosition: choosePreferredLivePosition(
      nextInbound.livePosition,
      previousInbound.livePosition
    ),
    trackPoints: choosePreferredTrackPoints(
      nextInbound.trackPoints,
      previousInbound.trackPoints
    ),
  };
}

function mergeRealtimeTelemetry(previousNormalized, nextNormalized) {
  if (!nextNormalized || typeof nextNormalized !== "object") {
    return nextNormalized;
  }

  if (!previousNormalized || typeof previousNormalized !== "object") {
    return {
      ...nextNormalized,
      progressPercent: normalizedProgressPercent(nextNormalized.progressPercent),
    };
  }

  const nextStatus = String(nextNormalized.status || "").toLowerCase();
  const nextWithOperationalFallbacks = {
    ...nextNormalized,
    terminal: nextNormalized.terminal || previousNormalized.terminal || null,
    gate: nextNormalized.gate || previousNormalized.gate || null,
    departureTerminal: nextNormalized.departureTerminal || previousNormalized.departureTerminal || null,
    departureGate: nextNormalized.departureGate || previousNormalized.departureGate || null,
    arrivalTerminal: nextNormalized.arrivalTerminal || previousNormalized.arrivalTerminal || null,
    arrivalGate: nextNormalized.arrivalGate || previousNormalized.arrivalGate || null,
    baggageClaim: nextNormalized.baggageClaim || previousNormalized.baggageClaim || null,
    baggageBelt: nextNormalized.baggageBelt || previousNormalized.baggageBelt || null,
    // The provider can omit the inbound assignment from otherwise valid
    // operational snapshots. Keep the last known assignment so a background
    // poll cannot make the inbound-aircraft card and map route disappear.
    inboundFlight: mergeInboundFlightTelemetry(
      nextNormalized.inboundFlight,
      previousNormalized.inboundFlight
    ),
  };
  if (
    isTerminalStatus(nextStatus) ||
    nextNormalized.landingTimes?.actual ||
    nextNormalized.arrivalTimes?.actual
  ) {
    const trackPoints = choosePreferredTrackPoints(
      nextNormalized.trackPoints,
      previousNormalized.trackPoints
    );

    return {
      ...nextWithOperationalFallbacks,
      livePosition: null,
      trackPoints,
      progressPercent: normalizedProgressPercent(nextNormalized.progressPercent) ?? 100,
    };
  }

  const livePosition = choosePreferredLivePosition(
    nextNormalized.livePosition,
    previousNormalized.livePosition
  );
  const trackPoints = choosePreferredTrackPoints(
    nextNormalized.trackPoints,
    previousNormalized.trackPoints
  );

  const progressPercent =
    normalizedProgressPercent(nextNormalized.progressPercent) ??
    normalizedProgressPercent(previousNormalized.progressPercent);

  const lastUpdated = [nextNormalized.lastUpdated, livePosition?.recordedAt]
    .sort((left, right) => toEpochMillisOrZero(right) - toEpochMillisOrZero(left))[0] ||
    nextNormalized.lastUpdated ||
    previousNormalized.lastUpdated ||
    new Date().toISOString();

  return {
    ...nextWithOperationalFallbacks,
    livePosition,
    trackPoints,
    progressPercent,
    lastUpdated,
  };
}

module.exports = {
  mergeRealtimeTelemetry,
};
