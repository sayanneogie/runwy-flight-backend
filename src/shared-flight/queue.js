"use strict";

function createSharedFlightQueue({ onError = null, maxHistory = 1000 } = {}) {
  let nextJobId = 0;
  const historyLimit = Number.isFinite(maxHistory) ? Math.max(1, Math.floor(maxHistory)) : 1000;
  const jobs = [];
  const handlers = new Map();
  const dedupe = new Set();

  async function add(name, data, options = {}) {
    const dedupeKey = options.dedupeKey || `${name}:${data.flight_instance_id || data.flight_key || data.flight_event_id || JSON.stringify(data)}`;
    if (options.dedupe && dedupe.has(dedupeKey)) return { id: dedupeKey, deduped: true };
    dedupe.add(dedupeKey);
    const job = { id: `${name}:${++nextJobId}`, name, data, options };
    jobs.push(job);
    if (jobs.length > historyLimit) jobs.splice(0, jobs.length - historyLimit);
    if (options.runImmediately !== false && handlers.has(name)) {
      const run = async () => {
        try {
          await handlers.get(name)(job);
        } catch (error) {
          if (typeof onError === "function") {
            onError(error, job);
          } else {
            console.error("Shared flight background job failed", {
              jobName: name,
              jobId: job.id,
              error: error?.message || String(error),
            });
          }
        } finally {
          dedupe.delete(dedupeKey);
        }
      };
      if (Number.isFinite(options.delayMs) && options.delayMs > 0) {
        const timer = setTimeout(run, options.delayMs);
        if (typeof timer.unref === "function") timer.unref();
      } else {
        setImmediate(run);
      }
    }
    return job;
  }

  function process(name, handler) {
    handlers.set(name, handler);
  }

  return { add, process, jobs, handlers };
}

module.exports = { createSharedFlightQueue };
