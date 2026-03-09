require("dotenv").config();

const { startTrackingPollerWorker } = require("./server");

startTrackingPollerWorker({ force: true }).catch((error) => {
  console.error("Failed to start flight poller worker", error);
  process.exit(1);
});
