require("dotenv").config();

const { startFirehoseWorker } = require("./server");

startFirehoseWorker({ force: true }).catch((error) => {
  console.error("Failed to start FlightAware Firehose worker", error);
  process.exit(1);
});
