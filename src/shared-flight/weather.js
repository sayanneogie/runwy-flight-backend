"use strict";

const crypto = require("node:crypto");
const { getAirportCatalog } = require("../airport-catalog");

const WEATHER_CACHE_TTL_SECONDS = 45 * 60;
const WEATHER_LOOKAHEAD_HOURS = 24;
const PREFLIGHT_ALERT_HOURS = 5;

function createWeatherKitClient({ env = process.env, fetchImpl = global.fetch } = {}) {
  const teamId = env.WEATHERKIT_TEAM_ID || env.APPLE_TEAM_ID;
  const serviceId = env.WEATHERKIT_SERVICE_ID;
  const keyId = env.WEATHERKIT_KEY_ID;
  const privateKey = normalizePrivateKey(env.WEATHERKIT_PRIVATE_KEY);
  const enabled = String(env.WEATHERKIT_ENABLED || "").toLowerCase() === "true";

  return {
    name: "weatherkit",
    isConfigured() {
      return Boolean(enabled && teamId && serviceId && keyId && privateKey && fetchImpl);
    },
    async fetchWeather({ latitude, longitude, forecastTime, language = "en_US" }) {
      if (!this.isConfigured()) {
        const error = new Error("WeatherKit is not configured");
        error.code = "WEATHERKIT_NOT_CONFIGURED";
        throw error;
      }
      const token = createWeatherKitToken({ teamId, serviceId, keyId, privateKey });
      const target = new Date(forecastTime || Date.now());
      const hourlyStart = target.toISOString();
      const hourlyEnd = new Date(target.getTime() + 60 * 60_000).toISOString();
      const url = new URL(`https://weatherkit.apple.com/api/v1/weather/${language}/${latitude}/${longitude}`);
      url.searchParams.set("dataSets", "currentWeather,hourlyForecast,weatherAlerts");
      url.searchParams.set("hourlyStart", hourlyStart);
      url.searchParams.set("hourlyEnd", hourlyEnd);
      const response = await fetchImpl(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!response.ok) {
        const error = new Error(`WeatherKit request failed (${response.status})`);
        error.statusCode = response.status;
        throw error;
      }
      return response.json();
    },
  };
}

function createFlightWeatherService({ cache, repository, weatherProvider = createWeatherKitClient(), now = () => Date.now() } = {}) {
  async function insightForFlight(row, options = {}) {
    const target = weatherTargetForFlight(row, now());
    if (!target) return unavailable("too_early", "Weather check will run closer to departure.");
    const airport = airportByCode(target.airportCode);
    if (!airport?.coordinate?.latitude || !airport?.coordinate?.longitude) {
      return unavailable("airport_coordinates_missing", "Weather is unavailable for this airport.");
    }
    const cacheKey = weatherCacheKey(target.airportCode, target.forecastTime);
    const cached = cache ? await cache.getJSON(cacheKey) : null;
    if (cached) return { ...cached, source: "redis" };
    if (!weatherProvider?.isConfigured?.()) {
      return unavailable("weatherkit_not_configured", "WeatherKit is not enabled on the backend.");
    }

    const startedAt = now();
    try {
      const raw = await weatherProvider.fetchWeather({
        latitude: airport.coordinate.latitude,
        longitude: airport.coordinate.longitude,
        forecastTime: target.forecastTime,
      });
      const insight = buildWeatherInsight({ raw, airport, target, row, nowMs: now() });
      if (cache) await cache.setJSON(cacheKey, insight, WEATHER_CACHE_TTL_SECONDS);
      await repository?.logApiUsage?.({
        provider: weatherProvider.name || "weatherkit",
        endpoint: "weatherInsight",
        flight_key: row.flight_key,
        response_time_ms: now() - startedAt,
        cache_status: options.cacheStatus || "miss",
        status_code: 200,
      });
      return insight;
    } catch (error) {
      await repository?.logApiUsage?.({
        provider: weatherProvider.name || "weatherkit",
        endpoint: "weatherInsight",
        flight_key: row.flight_key,
        response_time_ms: now() - startedAt,
        cache_status: "miss",
        status_code: error.statusCode || null,
        error: error?.message || String(error),
      });
      return unavailable("provider_unavailable", "Weather is temporarily unavailable.");
    }
  }

  return { insightForFlight };
}

function weatherTargetForFlight(row, nowMs = Date.now()) {
  const status = String(row?.status || "").toLowerCase();
  const scheduledDeparture = parseTime(row?.estimated_departure_at || row?.scheduled_departure_at);
  const scheduledArrival = parseTime(row?.estimated_arrival_at || row?.scheduled_arrival_at);
  const airborne = ["departed", "airborne", "enroute", "taxi_in", "landed", "arrived"].includes(status);
  const forecastTime = airborne ? scheduledArrival : scheduledDeparture;
  const airportCode = airborne ? row?.destination_airport : row?.origin_airport;
  const role = airborne ? "arrival" : "departure";
  if (!forecastTime || !airportCode) return null;
  const hoursUntil = (forecastTime.getTime() - nowMs) / 36e5;
  if (hoursUntil > WEATHER_LOOKAHEAD_HOURS) return null;
  return { airportCode, role, forecastTime: forecastTime.toISOString(), hoursUntil };
}

function buildWeatherInsight({ raw, airport, target, row, nowMs = Date.now() }) {
  const hour = nearestHourlyForecast(raw?.hourlyForecast?.hours, target.forecastTime) || raw?.currentWeather || {};
  const alerts = Array.isArray(raw?.weatherAlerts?.alerts) ? raw.weatherAlerts.alerts : [];
  const normalized = {
    conditionCode: hour.conditionCode || raw?.currentWeather?.conditionCode || null,
    temperatureC: numberOrNull(hour.temperature ?? raw?.currentWeather?.temperature),
    windSpeedKph: numberOrNull(hour.windSpeed ?? raw?.currentWeather?.windSpeed),
    precipitationChance: numberOrNull(hour.precipitationChance),
    visibilityMeters: numberOrNull(hour.visibility ?? raw?.currentWeather?.visibility),
    severeAlertCount: alerts.filter(isSevereWeatherAlert).length,
  };
  const assessment = assessWeather(normalized);
  const flightCode = `${row.airline_code || ""}${row.flight_number || ""}`.trim();
  const departureAt = parseTime(row.estimated_departure_at || row.scheduled_departure_at);
  const hoursUntilDeparture = departureAt ? (departureAt.getTime() - nowMs) / 36e5 : null;
  const flightOnTime = !String(row.status || "").toLowerCase().includes("delay");
  const notificationWindow = target.role === "departure" && hoursUntilDeparture != null && hoursUntilDeparture > 0 && hoursUntilDeparture <= PREFLIGHT_ALERT_HOURS;
  const notificationRequired = notificationWindow && (assessment.severity !== "low" || flightOnTime);

  return {
    available: true,
    source: "weatherkit",
    provider: "weatherkit",
    airportCode: airport.code,
    airportName: airport.name || null,
    airportRole: target.role,
    forecastTime: target.forecastTime,
    generatedAt: new Date(nowMs).toISOString(),
    expiresAt: new Date(nowMs + WEATHER_CACHE_TTL_SECONDS * 1000).toISOString(),
    title: target.role === "arrival" ? "Arrival Weather" : "Departure Weather",
    summary: weatherSummary({ airportCode: airport.code, role: target.role, assessment, flightCode, flightOnTime, row }),
    severity: assessment.severity,
    notificationRequired,
    conditionCode: normalized.conditionCode,
    temperatureC: normalized.temperatureC,
    windSpeedKph: normalized.windSpeedKph,
    precipitationChance: normalized.precipitationChance,
    visibilityMeters: normalized.visibilityMeters,
    severeAlertCount: normalized.severeAlertCount,
  };
}

function assessWeather(weather) {
  const condition = String(weather.conditionCode || "").toLowerCase();
  if (weather.severeAlertCount > 0 || condition.includes("thunderstorm") || condition.includes("blizzard")) {
    return { severity: "high" };
  }
  if ((weather.windSpeedKph ?? 0) >= 45 || (weather.precipitationChance ?? 0) >= 0.65 || (weather.visibilityMeters ?? Infinity) <= 3000) {
    return { severity: "high" };
  }
  if (condition.includes("rain") || condition.includes("snow") || condition.includes("fog") || (weather.windSpeedKph ?? 0) >= 28 || (weather.precipitationChance ?? 0) >= 0.35) {
    return { severity: "medium" };
  }
  return { severity: "low" };
}

function weatherSummary({ airportCode, role, assessment, flightCode, flightOnTime }) {
  const phase = role === "arrival" ? "arrival" : "departure";
  if (assessment.severity === "high") return `Weather near ${airportCode} may affect ${phase}. We will keep watching ${flightCode || "your flight"}.`;
  if (assessment.severity === "medium") return `Some weather risk near ${airportCode} around ${phase}.`;
  if (role === "departure" && flightOnTime) return `${flightCode || "Your flight"} is on time and weather at ${airportCode} looks favorable for departure.`;
  return `Conditions look favorable for ${phase} at ${airportCode}.`;
}

function weatherEventFromInsight(insight) {
  if (!insight?.available || !insight.notificationRequired) return null;
  return {
    event_type: "WEATHER_ADVISORY",
    event_severity: insight.severity,
    old_value: null,
    new_value: insight,
    summary: insight.summary,
    provider: insight.provider,
    provider_event_time: insight.generatedAt,
    confidence: "medium",
    notification_required: true,
  };
}

function airportByCode(code) {
  const normalized = String(code || "").trim().toUpperCase();
  if (!normalized) return null;
  const catalog = getAirportCatalog();
  const alias = catalog.aliases?.[normalized] || normalized;
  return catalog.airports.find((airport) => airport.code === alias) || null;
}

function weatherCacheKey(airportCode, forecastTime) {
  const hour = new Date(forecastTime).toISOString().slice(0, 13);
  return `weather:${airportCode}:${hour}`;
}

function nearestHourlyForecast(hours, forecastTime) {
  if (!Array.isArray(hours) || hours.length === 0) return null;
  const target = Date.parse(forecastTime);
  return hours.reduce((best, hour) => {
    const diff = Math.abs(Date.parse(hour.forecastStart || hour.startTime || 0) - target);
    if (!best || diff < best.diff) return { diff, hour };
    return best;
  }, null)?.hour || null;
}

function isSevereWeatherAlert(alert) {
  const severity = String(alert?.severity || "").toLowerCase();
  return ["severe", "extreme", "critical"].includes(severity);
}

function unavailable(reason, summary) {
  return { available: false, reason, summary, source: "none", generatedAt: new Date().toISOString() };
}

function parseTime(value) {
  const date = value ? new Date(value) : null;
  return date && Number.isFinite(date.getTime()) ? date : null;
}

function numberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function normalizePrivateKey(value) {
  return value ? String(value).replace(/\\n/g, "\n") : "";
}

function createWeatherKitToken({ teamId, serviceId, keyId, privateKey }) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "ES256", kid: keyId, id: `${teamId}.${serviceId}` };
  const claims = { iss: teamId, iat: now, exp: now + 55 * 60, sub: serviceId };
  const input = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(claims))}`;
  const signature = crypto.sign("sha256", Buffer.from(input), { key: privateKey, dsaEncoding: "ieee-p1363" });
  return `${input}.${base64url(signature)}`;
}

function base64url(input) {
  return Buffer.from(input).toString("base64url");
}

module.exports = {
  createFlightWeatherService,
  createWeatherKitClient,
  weatherEventFromInsight,
  weatherTargetForFlight,
  buildWeatherInsight,
};
