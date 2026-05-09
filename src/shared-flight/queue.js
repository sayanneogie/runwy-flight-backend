"use strict";

function createSharedFlightQueue() {
  const jobs = [];
  const handlers = new Map();
  const dedupe = new Set();

  async function add(name, data, options = {}) {
    const dedupeKey = options.dedupeKey || `${name}:${data.flight_instance_id || data.flight_key || data.flight_event_id || JSON.stringify(data)}`;
    if (options.dedupe && dedupe.has(dedupeKey)) return { id: dedupeKey, deduped: true };
    dedupe.add(dedupeKey);
    const job = { id: `${name}:${jobs.length + 1}`, name, data, options };
    jobs.push(job);
    if (options.runImmediately !== false && handlers.has(name)) {
      const run = async () => {
        try {
          await handlers.get(name)(job);
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
