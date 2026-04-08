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
  return ["landed", "cancelled", "diverted"].includes(String(status || "").toLowerCase());
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
      ...nextNormalized,
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
    ...nextNormalized,
    livePosition,
    trackPoints,
    progressPercent,
    lastUpdated,
  };
}

module.exports = {
  mergeRealtimeTelemetry,
};
