"use strict";

const tls = require("node:tls");
const readline = require("node:readline");
const { setTimeout: delay } = require("node:timers/promises");
const {
  buildFirehoseInitCommand,
  firehoseMessageFlightNumber,
  firehoseMessageProviderFlightId,
  isFirehoseErrorMessage,
  isFirehoseKeepaliveMessage,
  parseFirehoseJSONLine,
} = require("./firehose-protocol");

const DEFAULT_FIREHOSE_BACKFILL_MAX_HOURS = 8;
const DEFAULT_FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES = 15;
const DEFAULT_FIREHOSE_BACKFILL_MIN_TRACK_POINTS = 8;

function epochMs(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const asNumber = Number(value);
  if (Number.isFinite(asNumber)) {
    return Math.abs(asNumber) < 1e12 ? asNumber * 1000 : asNumber;
  }

  const asDate = new Date(value).getTime();
  return Number.isFinite(asDate) ? asDate : null;
}

function trackedRowTrackPointCount(row) {
  return Array.isArray(row?.normalized?.trackPoints) ? row.normalized.trackPoints.length : 0;
}

function trackedRowBackfillStartMs(
  row,
  {
    nowMs = Date.now(),
    maxBackfillHours = DEFAULT_FIREHOSE_BACKFILL_MAX_HOURS,
    preDepartureMinutes = DEFAULT_FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
    minTrackPoints = DEFAULT_FIREHOSE_BACKFILL_MIN_TRACK_POINTS,
  } = {}
) {
  if (!row) {
    return null;
  }

  const status = String(row.normalized?.status || "").toLowerCase();
  if (!["boarding", "delayed", "departed", "enroute"].includes(status)) {
    return null;
  }

  if (trackedRowTrackPointCount(row) >= minTrackPoints) {
    return null;
  }

  const maxBackfillMs = Math.max(1, Number(maxBackfillHours) || 0) * 60 * 60 * 1000;
  const preDepartureBufferMs = Math.max(0, Number(preDepartureMinutes) || 0) * 60 * 1000;
  const oldestAllowedMs = nowMs - maxBackfillMs;

  const departureMs =
    epochMs(row.normalized?.takeoffTimes?.actual) ??
    epochMs(row.normalized?.departureTimes?.actual) ??
    epochMs(row.normalized?.takeoffTimes?.estimated) ??
    epochMs(row.normalized?.departureTimes?.estimated) ??
    epochMs(row.normalized?.takeoffTimes?.scheduled) ??
    epochMs(row.normalized?.departureTimes?.scheduled);

  const fallbackReferenceMs =
    epochMs(row.normalized?.livePosition?.recordedAt) ??
    epochMs(row.normalized?.lastUpdated) ??
    nowMs;

  const desiredStartMs =
    departureMs !== null ? departureMs - preDepartureBufferMs : fallbackReferenceMs - 30 * 60 * 1000;
  const clampedStartMs = Math.max(oldestAllowedMs, Math.min(desiredStartMs, nowMs));

  return Number.isFinite(clampedStartMs) && clampedStartMs < nowMs ? clampedStartMs : null;
}

function resolveFirehoseTimeMode({
  lastGoodPitr = null,
  trackedRows = [],
  nowMs = Date.now(),
  maxBackfillHours = DEFAULT_FIREHOSE_BACKFILL_MAX_HOURS,
  preDepartureMinutes = DEFAULT_FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
  minTrackPoints = DEFAULT_FIREHOSE_BACKFILL_MIN_TRACK_POINTS,
} = {}) {
  const pitrCandidates = [
    epochMs(lastGoodPitr),
    ...Array.from(trackedRows || [])
      .map((row) =>
        trackedRowBackfillStartMs(row, {
          nowMs,
          maxBackfillHours,
          preDepartureMinutes,
          minTrackPoints,
        })
      )
      .filter(Number.isFinite),
  ].filter(Number.isFinite);

  if (!pitrCandidates.length) {
    return "live";
  }

  return `pitr ${Math.floor(Math.min(...pitrCandidates) / 1000)}`;
}

function createFirehoseRuntime({
  firehoseEnabled,
  firehoseHost,
  firehosePort,
  firehoseVersion,
  firehoseUsername,
  firehosePassword,
  firehoseUserAgent,
  firehoseKeepaliveSeconds,
  firehoseEvents,
  firehoseMinSecondsBetweenAirborne,
  firehoseTrackedSetRefreshMs,
  firehoseReconnectDelayMs,
  firehoseBackfillMaxHours = DEFAULT_FIREHOSE_BACKFILL_MAX_HOURS,
  firehoseBackfillPredepartureMinutes = DEFAULT_FIREHOSE_BACKFILL_PREDEPARTURE_MINUTES,
  firehoseBackfillMinTrackPoints = DEFAULT_FIREHOSE_BACKFILL_MIN_TRACK_POINTS,
  usesDatabase,
  ensureDatabaseSchema,
  listFirehoseTrackedRows,
  processFirehoseMessage,
  providerName,
}) {
  let workerPromise = null;
  let lastGoodPitr = null;

  function isFirehoseConfigured() {
    return Boolean(
      String(firehoseHost || "").trim() &&
        Number.isFinite(Number(firehosePort)) &&
        String(firehoseUsername || "").trim() &&
        String(firehosePassword || "").trim()
    );
  }

  function isFirehoseRunning() {
    return Boolean(workerPromise);
  }

  function trackedRowsSignature(trackedRows) {
    return Array.from(trackedRows || [])
      .map((row) =>
        [
          row.flightId,
          row.providerFlightId || "",
          row.query?.flightNumber || row.normalized?.flightNumber || "",
          row.query?.date || "",
          row.query?.departureIata || "",
          row.query?.arrivalIata || "",
          row.normalized?.status || "",
        ].join("|")
      )
      .sort()
      .join("\n");
  }

  function trackedFlightIdents(trackedRows) {
    return Array.from(
      new Set(
        Array.from(trackedRows || [])
          .flatMap((row) => {
            const providerIdent = String(row.providerFlightId || "")
              .trim()
              .split("-")[0];

            return [
              row.query?.flightNumber,
              row.normalized?.flightNumber,
              providerIdent,
            ];
          })
          .map((value) => String(value || "").trim().toUpperCase().replace(/\s+/g, ""))
          .filter(Boolean)
      )
    ).sort();
  }

  function firehoseSocketOptions() {
    return {
      host: firehoseHost,
      port: firehosePort,
      servername: firehoseHost,
      keepAlive: true,
      rejectUnauthorized: true,
    };
  }

  async function connectAndStream(trackedRows) {
    const idents = trackedFlightIdents(trackedRows);
    if (!idents.length) {
      return { lastPitr: lastGoodPitr };
    }

    const timeMode = resolveFirehoseTimeMode({
      lastGoodPitr,
      trackedRows,
      nowMs: Date.now(),
      maxBackfillHours: firehoseBackfillMaxHours,
      preDepartureMinutes: firehoseBackfillPredepartureMinutes,
      minTrackPoints: firehoseBackfillMinTrackPoints,
    });
    const initCommand = buildFirehoseInitCommand({
      timeMode,
      version: firehoseVersion,
      username: firehoseUsername,
      password: firehosePassword,
      userAgent: firehoseUserAgent,
      keepaliveSeconds: firehoseKeepaliveSeconds,
      events: firehoseEvents,
      idents,
      minSecondsBetweenAirborne: firehoseMinSecondsBetweenAirborne,
    });

    console.log(
      `FlightAware Firehose connecting mode=${timeMode} trackedRows=${trackedRows.length} idents=${idents.length} sample=${idents
        .slice(0, 8)
        .join(",")}`
    );

    const trackedRowsById = new Map(Array.from(trackedRows || []).map((row) => [row.flightId, row]));
    let currentSignature = trackedRowsSignature(trackedRowsById.values());
    let lastPitrForConnection = lastGoodPitr;
    let socket;
    let trackedSetRefreshTimer = null;

    try {
      socket = await new Promise((resolve, reject) => {
        const client = tls.connect(firehoseSocketOptions(), () => {
          resolve(client);
        });

        client.once("error", reject);
      });

      socket.write(initCommand);

      trackedSetRefreshTimer = setInterval(async () => {
        try {
          const nextTrackedRows = await listFirehoseTrackedRows();
          const nextSignature = trackedRowsSignature(nextTrackedRows);
          if (nextSignature !== currentSignature) {
            socket.destroy(new Error("tracked_set_changed"));
            return;
          }

          trackedRowsById.clear();
          for (const row of nextTrackedRows) {
            trackedRowsById.set(row.flightId, row);
          }
        } catch (_error) {
          socket.destroy(new Error("tracked_set_refresh_failed"));
        }
      }, firehoseTrackedSetRefreshMs);

      if (typeof trackedSetRefreshTimer.unref === "function") {
        trackedSetRefreshTimer.unref();
      }

      const input = readline.createInterface({
        input: socket,
        crlfDelay: Infinity,
      });

      try {
        for await (const line of input) {
          const message = parseFirehoseJSONLine(line);
          if (!message) {
            continue;
          }

          if (message?.pitr) {
            lastPitrForConnection = String(message.pitr).trim() || lastPitrForConnection;
          }

          if (isFirehoseKeepaliveMessage(message)) {
            continue;
          }

          if (isFirehoseErrorMessage(message)) {
            throw new Error(String(message.error_msg || message.reason || "Firehose error"));
          }

          const providerFlightId = firehoseMessageProviderFlightId(message);
          const flightNumber = firehoseMessageFlightNumber(message);
          if (!providerFlightId && !flightNumber) {
            continue;
          }

          try {
            await processFirehoseMessage(message, trackedRowsById);
          } catch (error) {
            console.warn(
              `Firehose message handling failed for ${providerFlightId || flightNumber || "unknown"}: ${
                error?.message || String(error)
              }`
            );
          }
        }
      } finally {
        input.close();
      }

      return { lastPitr: lastPitrForConnection };
    } finally {
      if (trackedSetRefreshTimer) {
        clearInterval(trackedSetRefreshTimer);
      }

      if (socket && !socket.destroyed) {
        socket.destroy();
      }
    }
  }

  async function runFirehoseWorkerLoop() {
    console.log(
      `FlightAware Firehose worker starting provider=${providerName} host=${firehoseHost}:${firehosePort}`
    );

    while (true) {
      const trackedRows = await listFirehoseTrackedRows();
      if (!trackedRows.length) {
        await delay(firehoseTrackedSetRefreshMs);
        continue;
      }

      try {
        const result = await connectAndStream(trackedRows);
        if (result?.lastPitr) {
          lastGoodPitr = result.lastPitr;
        }
      } catch (error) {
        const reason = String(error?.message || error || "");
        if (reason !== "tracked_set_changed" && reason !== "tracked_set_refresh_failed") {
          console.warn(`FlightAware Firehose stream disconnected: ${reason}`);
          await delay(firehoseReconnectDelayMs);
        }
      }
    }
  }

  async function startFirehoseWorker(options = {}) {
    const { force = true } = options;

    if ((!force && !firehoseEnabled) || workerPromise) {
      return workerPromise;
    }

    if (!usesDatabase()) {
      throw new Error("FlightAware Firehose worker requires DATABASE_URL-backed persistence.");
    }

    if (!isFirehoseConfigured()) {
      throw new Error(
        "FlightAware Firehose worker requires FIREHOSE_HOST, FIREHOSE_PORT, FIREHOSE_USERNAME, and FIREHOSE_API_KEY (or legacy FIREHOSE_PASSWORD)."
      );
    }

    await ensureDatabaseSchema();
    workerPromise = runFirehoseWorkerLoop().finally(() => {
      workerPromise = null;
    });
    return workerPromise;
  }

  return {
    isFirehoseConfigured,
    isFirehoseRunning,
    startFirehoseWorker,
  };
}

module.exports = {
  createFirehoseRuntime,
  resolveFirehoseTimeMode,
  trackedRowBackfillStartMs,
  trackedRowTrackPointCount,
};
