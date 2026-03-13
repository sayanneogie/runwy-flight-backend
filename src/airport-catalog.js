const fs = require("node:fs");
const path = require("node:path");

const DATASET_PATH = path.join(__dirname, "..", "data", "airports.json");

let cachedCatalog = null;

function getAirportCatalog() {
  if (cachedCatalog) {
    return cachedCatalog;
  }

  const body = fs.readFileSync(DATASET_PATH, "utf8");
  const parsed = JSON.parse(body);

  if (!Array.isArray(parsed?.airports)) {
    throw new Error("Airport catalog is missing the airports array.");
  }

  const aliases =
    parsed?.aliases && typeof parsed.aliases === "object" && !Array.isArray(parsed.aliases)
      ? parsed.aliases
      : {};

  cachedCatalog = {
    version: typeof parsed?.version === "string" && parsed.version.trim() ? parsed.version.trim() : null,
    airports: parsed.airports,
    aliases,
    body,
  };

  return cachedCatalog;
}

module.exports = {
  getAirportCatalog,
};
