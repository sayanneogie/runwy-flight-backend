"use strict";

function createSharedFlightQueue({ onError = null, maxPendingJobs = 10000 } = {}) {
  const jobs = [];
  const handlers = new Map();
  const dedupe = new Set();
  let sequence = 0;

  async function add(name, data, options = {}) {
    const dedupeKey = options.dedupeKey || `${name}:${data.flight_instance_id || data.flight_key || data.flight_event_id || JSON.stringify(data)}`;
    if (options.dedupe && dedupe.has(dedupeKey)) return { id: dedupeKey, deduped: true };
    if (jobs.length >= maxPendingJobs) throw new Error("Shared flight queue is full; retry through durable recovery");
    dedupe.add(dedupeKey);
    const job = { id: `${name}:${++sequence}`, name, data, options };
    jobs.push(job);
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
          const index = jobs.indexOf(job);
          if (index >= 0) jobs.splice(index, 1);
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
