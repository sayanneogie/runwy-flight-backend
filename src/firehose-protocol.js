"use strict";

function quoteFirehoseValue(value) {
  return `"${String(value || "")
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')}"`;
}

function normalizeFirehoseValues(values, { uppercase = false } = {}) {
  const rawValues = Array.isArray(values)
    ? values
    : String(values || "")
        .split(/\s+/)
        .filter(Boolean);

  return Array.from(
    new Set(
      rawValues
        .map((value) => {
          const normalized = String(value || "").trim();
          return uppercase ? normalized.toUpperCase() : normalized;
        })
        .filter(Boolean)
    )
  );
}

function buildFirehoseInitCommand({
  timeMode = "live",
  version = "36.0",
  username,
  password,
  userAgent = "runwy-firehose",
  keepaliveSeconds = 60,
  events = [],
  idents = [],
  minSecondsBetweenAirborne = null,
}) {
  const normalizedTimeMode = String(timeMode || "").trim() || "live";
  const normalizedVersion = String(version || "").trim() || "36.0";
  const normalizedUsername = String(username || "").trim();
  const normalizedPassword = String(password || "").trim();
  const normalizedUserAgent = String(userAgent || "").trim() || "runwy-firehose";
  const normalizedKeepaliveSeconds = Number(keepaliveSeconds || 0);

  if (!normalizedUsername || !normalizedPassword) {
    throw new Error("Firehose username/password are required");
  }

  const normalizedEvents = normalizeFirehoseValues(events);
  const sanitizedEvents = normalizedEvents.filter((event) => {
    if (event !== "extendedFlightInfo" && event !== "flightplan") {
      return true;
    }

    return !normalizedEvents.includes("flifo");
  });

  const normalizedIdents = normalizeFirehoseValues(idents, { uppercase: true });

  const commandParts = [
    normalizedTimeMode,
    "version",
    normalizedVersion,
    "username",
    normalizedUsername,
    "password",
    normalizedPassword,
    "useragent",
    normalizedUserAgent,
    "keepalive",
    Number.isFinite(normalizedKeepaliveSeconds) && normalizedKeepaliveSeconds > 0
      ? String(Math.round(normalizedKeepaliveSeconds))
      : "60",
  ];

  if (sanitizedEvents.length > 0) {
    commandParts.push("events", quoteFirehoseValue(sanitizedEvents.join(" ")));
  }

  if (normalizedIdents.length > 0) {
    commandParts.push("idents", quoteFirehoseValue(normalizedIdents.join(" ")));
  }

  if (
    minSecondsBetweenAirborne !== null &&
    minSecondsBetweenAirborne !== undefined &&
    Number.isFinite(Number(minSecondsBetweenAirborne)) &&
    Number(minSecondsBetweenAirborne) >= 0
  ) {
    commandParts.push(
      "min_seconds_between_airborne",
      String(Math.round(Number(minSecondsBetweenAirborne)))
    );
  }

  return `${commandParts.join(" ")}\n`;
}

function parseFirehoseJSONLine(line) {
  const trimmed = String(line || "").trim();
  if (!trimmed) return null;

  try {
    const parsed = JSON.parse(trimmed);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function firehoseMessageType(message) {
  return String(message?.type || "").trim().toLowerCase();
}

function isFirehoseKeepaliveMessage(message) {
  return firehoseMessageType(message) === "keepalive";
}

function isFirehoseErrorMessage(message) {
  return firehoseMessageType(message) === "error";
}

function firehoseMessageProviderFlightId(message) {
  const value =
    message?.id ||
    message?.fa_flight_id ||
    message?.faFlightId ||
    message?.faFlightID;
  const normalized = String(value || "").trim();
  return normalized || null;
}

function firehoseMessageFlightNumber(message) {
  const value = message?.ident || message?.ident_iata || message?.flight_number;
  const normalized = String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "");
  return normalized || null;
}

function firehoseMessageTimestampMs(message) {
  const candidates = [
    message?.clock,
    message?.adt,
    message?.aat,
    message?.actual_out,
    message?.actual_in,
    message?.actual_off,
    message?.actual_on,
    message?.estimated_out,
    message?.estimated_in,
    message?.estimated_off,
    message?.estimated_on,
    message?.scheduled_out,
    message?.scheduled_in,
    message?.scheduled_off,
    message?.scheduled_on,
    message?.edt,
    message?.eta,
    message?.pitr,
  ];

  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined || candidate === "") continue;

    const asNumber = Number(candidate);
    if (Number.isFinite(asNumber)) {
      const epochMs = Math.abs(asNumber) < 1e12 ? asNumber * 1000 : asNumber;
      return epochMs;
    }

    const asDate = new Date(candidate).getTime();
    if (Number.isFinite(asDate)) {
      return asDate;
    }
  }

  return null;
}

module.exports = {
  buildFirehoseInitCommand,
  firehoseMessageFlightNumber,
  firehoseMessageProviderFlightId,
  firehoseMessageTimestampMs,
  firehoseMessageType,
  isFirehoseErrorMessage,
  isFirehoseKeepaliveMessage,
  parseFirehoseJSONLine,
};
