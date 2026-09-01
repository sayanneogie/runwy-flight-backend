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

function normalizedCityKey(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function airportCodesForCity(code) {
  const normalizedCode = String(code || "").trim().toUpperCase();
  if (!normalizedCode) return [];

  const airports = getAirportCatalog().airports;
  const destination = airports.find(
    (airport) => String(airport?.code || "").trim().toUpperCase() === normalizedCode
  );
  if (!destination) return [normalizedCode];

  const cityKey = normalizedCityKey(destination.city);
  const countryCode = String(destination.countryCode || "").trim().toUpperCase();
  if (!cityKey) return [normalizedCode];

  const matches = airports
    .filter((airport) =>
      normalizedCityKey(airport?.city) === cityKey &&
      String(airport?.countryCode || "").trim().toUpperCase() === countryCode
    )
    .map((airport) => String(airport.code || "").trim().toUpperCase())
    .filter(Boolean);

  return [...new Set([normalizedCode, ...matches])];
}

module.exports = {
  airportCodesForCity,
  getAirportCatalog,
};
