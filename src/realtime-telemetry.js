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
      Number.isFinite(Number(point.longitude))
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

  const nextLastRecordedAt = toEpochMillisOrZero(next[next.length - 1]?.recordedAt);
  const previousLastRecordedAt = toEpochMillisOrZero(previous[previous.length - 1]?.recordedAt);

  if (nextLastRecordedAt >= previousLastRecordedAt || next.length >= previous.length) {
    return next;
  }

  return previous;
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
