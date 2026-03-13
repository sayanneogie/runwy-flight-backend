#!/usr/bin/env node

const fs = require("node:fs/promises");
const path = require("node:path");

const AIRPORTS_CSV_URL =
  "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv";
const OUTPUT_PATH = path.join(__dirname, "..", "data", "airports.json");

const TYPE_PRIORITY = {
  large_airport: 60,
  medium_airport: 50,
  small_airport: 40,
  heliport: 30,
  seaplane_base: 20,
  balloonport: 10,
};

const ALIAS_OVERRIDES = {
  Andal: "RDP",
  Bangalore: "BLR",
  Bengaluru: "BLR",
  Bombay: "BOM",
  Calcutta: "CCU",
  Cochin: "COK",
  Durgapur: "RDP",
  Hindon: "HDO",
  "Kazi Nazrul Islam": "RDP",
  Kempegowda: "BLR",
  London: "LHR",
  Manila: "MNL",
  Manilla: "MNL",
  "Netaji Subhas Chandra Bose": "CCU",
  "New Delhi": "DEL",
  "Ninoy Aquino": "MNL",
  "Ninoy Aquino International Airport": "MNL",
  NYC: "JFK",
  Osaka: "KIX",
  Paris: "CDG",
  "Sao Paulo": "GRU",
  "São Paulo": "GRU",
  Seoul: "ICN",
  Tokyo: "HND",
};

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeIata(value) {
  const normalized = cleanText(value).toUpperCase();
  return /^[A-Z]{3}$/.test(normalized) ? normalized : null;
}

function foldAscii(value) {
  return cleanText(value)
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim();
}

function addTextVariant(variants, value) {
  const cleaned = cleanText(value);
  if (!cleaned || cleaned.length < 3) return;

  variants.add(cleaned);

  const folded = foldAscii(cleaned);
  if (folded && folded !== cleaned) {
    variants.add(folded);
  }
}

function cityAliasVariants(value) {
  const raw = cleanText(value);
  const variants = new Set();
  if (!raw) return variants;

  addTextVariant(variants, raw);

  const withoutParentheses = cleanText(raw.replace(/\s*\([^)]*\)/g, " "));
  if (withoutParentheses && withoutParentheses !== raw) {
    addTextVariant(variants, withoutParentheses);
  }

  const parentheticalSegments = Array.from(raw.matchAll(/\(([^)]+)\)/g), (match) => match[1]);
  for (const segment of parentheticalSegments) {
    addTextVariant(variants, segment);
  }

  for (const candidate of [raw, withoutParentheses]) {
    if (!candidate) continue;
    const slashSegments = candidate.split(/\s*\/\s*/g);
    if (slashSegments.length > 1) {
      for (const segment of slashSegments) {
        addTextVariant(variants, segment);
      }
    }

    const commaSegments = candidate.split(/\s*,\s*/g);
    if (commaSegments.length > 1) {
      for (const segment of commaSegments) {
        addTextVariant(variants, segment);
      }
    }
  }

  return variants;
}

function nameAliasVariants(value) {
  const variants = new Set();
  addTextVariant(variants, value);
  return variants;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (inQuotes) {
      if (char === '"') {
        if (next === '"') {
          cell += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(cell);
      cell = "";
      continue;
    }

    if (char === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    cell += char;
  }

  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }

  return rows;
}

function rowsToObjects(rows) {
  if (!rows.length) return [];
  const header = rows[0];

  return rows.slice(1).map((columns) => {
    const entry = {};
    for (let index = 0; index < header.length; index += 1) {
      entry[header[index]] = columns[index] || "";
    }
    return entry;
  });
}

function airportRank(record) {
  let score = TYPE_PRIORITY[record.type] || 0;

  if (record.scheduled_service === "yes") score += 100;
  if (cleanText(record.icao_code)) score += 5;
  if (cleanText(record.municipality)) score += 5;
  if (Number.isFinite(Number(record.latitude_deg)) && Number.isFinite(Number(record.longitude_deg))) score += 5;

  return score;
}

function makeAirport(record) {
  const code = normalizeIata(record.iata_code);
  if (!code || record.type === "closed") {
    return null;
  }

  const latitude = Number(record.latitude_deg);
  const longitude = Number(record.longitude_deg);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null;
  }

  const city = cleanText(record.municipality) || code;
  const name = cleanText(record.name) || city;
  const countryCode = cleanText(record.iso_country).toUpperCase();

  return {
    code,
    name,
    city,
    countryCode: /^[A-Z]{2}$/.test(countryCode) ? countryCode : "--",
    coordinate: {
      latitude,
      longitude,
    },
  };
}

function addAlias(aliasScores, alias, code, score) {
  const normalizedAlias = cleanText(alias);
  if (!normalizedAlias) return;

  const existing = aliasScores.get(normalizedAlias);
  if (!existing || score > existing.score || (score === existing.score && code < existing.code)) {
    aliasScores.set(normalizedAlias, { code, score });
  }
}

async function main() {
  const response = await fetch(AIRPORTS_CSV_URL, {
    headers: {
      Accept: "text/csv",
      "User-Agent": "runwy-airport-dataset-builder",
    },
  });

  if (!response.ok) {
    throw new Error(`Airport dataset download failed (${response.status}).`);
  }

  const csv = await response.text();
  const rows = rowsToObjects(parseCsv(csv));

  const selectedByCode = new Map();
  for (const record of rows) {
    const airport = makeAirport(record);
    if (!airport) continue;

    const rank = airportRank(record);
    const existing = selectedByCode.get(airport.code);
    if (!existing || rank > existing.rank || (rank === existing.rank && airport.name < existing.airport.name)) {
      selectedByCode.set(airport.code, { airport, rank });
    }
  }

  const aliasScores = new Map();
  const airports = Array.from(selectedByCode.values())
    .sort((left, right) => left.airport.code.localeCompare(right.airport.code))
    .map(({ airport, rank }) => {
      for (const alias of cityAliasVariants(airport.city)) {
        addAlias(aliasScores, alias, airport.code, rank);
      }

      for (const alias of nameAliasVariants(airport.name)) {
        addAlias(aliasScores, alias, airport.code, rank);
      }

      return airport;
    });

  const aliases = Object.fromEntries(
    Array.from(aliasScores.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([alias, entry]) => [alias, entry.code])
  );

  for (const [alias, code] of Object.entries(ALIAS_OVERRIDES)) {
    aliases[alias] = code;
  }

  const payload = {
    version: `ourairports-${new Date().toISOString().slice(0, 10)}`,
    source: "OurAirports",
    sourceURL: AIRPORTS_CSV_URL,
    airports,
    aliases,
  };

  await fs.mkdir(path.dirname(OUTPUT_PATH), { recursive: true });
  await fs.writeFile(OUTPUT_PATH, JSON.stringify(payload), "utf8");

  console.log(`Wrote ${airports.length} airports and ${Object.keys(aliases).length} aliases to ${OUTPUT_PATH}`);
}

main().catch((error) => {
  console.error(error?.message || String(error));
  process.exitCode = 1;
});
