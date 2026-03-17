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
          .map((row) =>
            String(row.query?.flightNumber || row.normalized?.flightNumber || "")
              .trim()
              .toUpperCase()
              .replace(/\s+/g, "")
          )
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

    const timeMode = lastGoodPitr ? `pitr ${lastGoodPitr}` : "live";
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
      throw new Error("FlightAware Firehose worker requires FIREHOSE_HOST, FIREHOSE_PORT, FIREHOSE_USERNAME, and FIREHOSE_PASSWORD.");
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
};
