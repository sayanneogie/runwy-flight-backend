"use strict";

function resolveEntrypoint({ firehoseEnabled = false, serviceName = "" } = {}) {
  if (firehoseEnabled || serviceName === "runwy-flight-firehose") {
    return { modulePath: "flight-firehose.js", label: "Firehose worker" };
  }

  if (serviceName === "runwy-flight-poller") {
    return { modulePath: "flight-poller.js", label: "tracking poller" };
  }

  return { modulePath: "server.js", label: "API server" };
}

function main() {
  const firehoseEnabled =
    String(process.env.ENABLE_FIREHOSE_WORKER || "false").toLowerCase() ===
    "true";
  const serviceName = process.env.RAILWAY_SERVICE_NAME || "";
  const { modulePath, label } = resolveEntrypoint({
    firehoseEnabled,
    serviceName,
  });

  console.log(`Starting ${label} via ${modulePath}`);
  require(`./${modulePath}`);
}

if (require.main === module) {
  main();
}

module.exports = {
  resolveEntrypoint,
};
