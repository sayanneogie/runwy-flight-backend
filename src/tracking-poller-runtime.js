function createTrackingPollerRuntime({
  isPollerEnabled,
  usesDatabase,
  ensureDatabaseSchema,
  listDueTrackingRows,
  refreshTrackedFlightRecord,
  markTrackingRowErrored,
  pollerIntervalMs,
  pollerBatchSize,
  providerName,
}) {
  let pollerTimer = null;
  let pollerInFlight = false;

  async function runTrackingPollerCycle() {
    if (!usesDatabase() || pollerInFlight) return;

    pollerInFlight = true;
    try {
      const trackedRows = await listDueTrackingRows(pollerBatchSize);

      for (const tracked of trackedRows) {
        try {
          await refreshTrackedFlightRecord(tracked);
        } catch (error) {
          await markTrackingRowErrored(tracked.flightId, error?.message || String(error));
        }
      }
    } finally {
      pollerInFlight = false;
    }
  }

  function startTrackingPoller(options = {}) {
    const { force = false, keepProcessAlive = false } = options;

    if ((!force && !isPollerEnabled) || !usesDatabase() || pollerTimer) {
      return false;
    }

    pollerTimer = setInterval(() => {
      runTrackingPollerCycle().catch((error) => {
        console.error("Tracking poller cycle failed", error);
      });
    }, pollerIntervalMs);

    if (!keepProcessAlive && typeof pollerTimer.unref === "function") {
      pollerTimer.unref();
    }

    runTrackingPollerCycle().catch((error) => {
      console.error("Initial tracking poller cycle failed", error);
    });

    return true;
  }

  async function startTrackingPollerWorker(options = {}) {
    const { force = true } = options;

    if (!usesDatabase()) {
      throw new Error("Tracking poller requires DATABASE_URL-backed persistence.");
    }

    await ensureDatabaseSchema();

    const started = startTrackingPoller({ force, keepProcessAlive: true });
    if (!started) {
      throw new Error("Tracking poller worker could not start.");
    }

    console.log(
      `Flight poller worker started provider=${providerName} persistence=supabase-postgres intervalMs=${pollerIntervalMs} batchSize=${pollerBatchSize}`
    );
  }

  function isPollerRunning() {
    return Boolean(pollerTimer);
  }

  return {
    isPollerRunning,
    runTrackingPollerCycle,
    startTrackingPoller,
    startTrackingPollerWorker,
  };
}

module.exports = {
  createTrackingPollerRuntime,
};
