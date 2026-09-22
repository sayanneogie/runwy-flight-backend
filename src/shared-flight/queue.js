"use strict";

function createSharedFlightQueue({ onError = null, maxPendingJobs = 10000, concurrency = 8 } = {}) {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || !Number.isSafeInteger(maxPendingJobs) || maxPendingJobs < 1) throw new Error("Invalid queue capacity");
  const jobs = [], ready = [], handlers = new Map(), dedupe = new Set(), timers = new Set();
  let sequence = 0, active = 0, closed = false;
  function drain() {
    while (!closed && active < concurrency && ready.length) {
      const { job, key } = ready.shift();
      active++;
      Promise.resolve().then(() => handlers.get(job.name)(job)).catch(error => {
        if (onError) onError(error, job);
        else console.error("Shared flight background job failed", { jobName: job.name, jobId: job.id, error: error?.message });
      }).catch(() => {}).finally(() => {
        active--; dedupe.delete(key);
        const index = jobs.indexOf(job); if (index >= 0) jobs.splice(index, 1);
        drain();
      });
    }
  }
  async function add(name, data, options = {}) {
    const key = options.dedupeKey || `${name}:${data.flight_instance_id || data.flight_key || data.flight_event_id || JSON.stringify(data)}`;
    if (options.dedupe && dedupe.has(key)) return { id: key, deduped: true };
    if (closed || jobs.length >= maxPendingJobs) throw Object.assign(new Error("Shared flight queue is full; retry through durable recovery"), { statusCode: 503 });
    dedupe.add(key);
    const job = { id: `${name}:${++sequence}`, name, data, options }; jobs.push(job);
    if (options.runImmediately !== false && handlers.has(name)) {
      const enqueue = () => { ready.push({ job, key }); drain(); };
      if (Number.isFinite(options.delayMs) && options.delayMs > 0) {
        const timer = setTimeout(() => { timers.delete(timer); enqueue(); }, options.delayMs);
        timers.add(timer); timer.unref?.();
      } else setImmediate(() => { if (!closed) enqueue(); });
    }
    return job;
  }
  function close() {
    closed = true;
    for (const timer of timers) clearTimeout(timer);
    timers.clear(); ready.length = 0;
    // Running work keeps its slot until it really finishes; a timeout must not
    // free capacity while uncancelled provider/database work is still running.
  }
  return { add, process: (name, handler) => handlers.set(name, handler), jobs, handlers, close, get activeCount() { return active; } };
}
module.exports = { createSharedFlightQueue };
