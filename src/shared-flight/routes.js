"use strict";

function mountSharedFlightRoutes(app, service) {
  app.get("/v1/flights/search", async (req, res) => {
    try {
      const flight = await service.searchFlight(req.query, { userId: req.auth?.userId || null });
      return res.json(flight);
    } catch (error) {
      return res.status(error.code === "PT429" ? 429 : error.code === "23514" ? 400 : (error.statusCode || 500)).json({ error: error.message || "Unable to search flight" });
    }
  });

  app.post("/v1/user-flights", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const result = await service.saveUserFlight(userId, req.body || {});
      return res.status(201).json(result);
    } catch (error) {
      return res.status(error.code === "PT429" ? 429 : error.code === "23514" ? 400 : (error.statusCode || 500)).json({ error: error.message || "Unable to save flight" });
    }
  });

  app.post("/v1/user-flights/ensure-live-coverage", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    const controller = new AbortController();
    const cancel = () => controller.abort();
    res.on('close', cancel);
    try {
      const result = await service.ensureUserFlightLiveCoverage(userId, req.body || {}, { signal: controller.signal });
      return res.json(result);
    } catch (error) {
      return res.status(error.code === "PT429" ? 429 : error.code === "23514" ? 400 : (error.statusCode || 500)).json({ error: error.message || "Unable to ensure live coverage" });
    } finally { res.off('close', cancel); }
  });

  app.put("/v1/user-flights/displayed", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const result = await service.reconcileDisplayedUserFlights(userId, req.body || {});
      return res.json(result);
    } catch (error) {
      return res.status(error.code === "PT429" ? 429 : error.code === "23514" ? 400 : (error.statusCode || 500)).json({
        error: error.message || "Unable to reconcile displayed flights",
      });
    }
  });

  app.get("/v1/user-flights", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const limit = Number(req.query.limit || 100), offset = Number(req.query.offset || 0);
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 || !Number.isSafeInteger(offset) || offset < 0 || offset > 10000)
        return res.status(400).json({ error: "Invalid page" });
      const flights = await service.listUserFlights(userId, { limit, offset });
      return res.json({ flights, nextOffset: flights.length === limit ? offset + limit : null });
    } catch (error) {
      return res.status(500).json({ error: "Unable to load saved flights" });
    }
  });

  app.patch("/v1/user-flights/:id", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    const body = req.body || {};
    const patch = {};
    if ("notification_enabled" in body) patch.notification_enabled = body.notification_enabled === true;
    if ("notificationEnabled" in body) patch.notification_enabled = body.notificationEnabled === true;
    if ("alert_preferences" in body) patch.alert_preferences = body.alert_preferences;
    if ("alertPreferences" in body) patch.alert_preferences = body.alertPreferences;
    if ("alert_settings_json" in body) patch.alert_settings_json = body.alert_settings_json;
    if ("alertSettings" in body) patch.alert_settings_json = body.alertSettings;
    if ("user_label" in body) patch.user_label = body.user_label;
    if ("userLabel" in body) patch.user_label = body.userLabel;
    if ("visibility" in body) patch.visibility = body.visibility;

    if ((patch.alert_preferences !== undefined && !validAlertPreferences(patch.alert_preferences, false)) ||
        (patch.alert_settings_json !== undefined && !validAlertPreferences(patch.alert_settings_json, true)) ||
        (patch.user_label != null && (typeof patch.user_label !== "string" || patch.user_label.length > 200))) {
      return res.status(400).json({ error: "Invalid alert preferences or flight label" });
    }

    try {
      const updated = await service.updateUserFlight(userId, req.params.id, patch);
      if (!updated) return res.status(404).json({ error: "Saved flight not found" });
      return res.json({ userFlight: updated });
    } catch (error) {
      return res.status(500).json({ error: "Unable to update saved flight" });
    }
  });

  app.delete("/v1/user-flights/:id", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });

    try {
      const deleted = await service.deleteUserFlight(userId, req.params.id);
      if (!deleted) return res.status(404).json({ error: "Saved flight not found" });
      return res.json({ userFlight: deleted });
    } catch (_error) {
      return res.status(500).json({ error: "Unable to delete saved flight" });
    }
  });

  app.post("/v1/device-tokens", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    const body = req.body || {};
    const deviceToken = String(body.deviceToken || body.device_token || "").trim().toLowerCase();
    const environment = String(body.environment || "").trim().toLowerCase();
    const deviceId = req.get("X-Device-ID") || body.deviceId || body.device_id;
    if (!/^[a-fA-F0-9]{64,512}$/.test(deviceToken) || typeof deviceId !== "string" || !deviceId.trim() || deviceId.length > 128 ||
        (body.platform && body.platform !== "ios") || !["sandbox", "production"].includes(environment)) {
      return res.status(400).json({ error: "A valid deviceToken, deviceId and environment are required" });
    }
    try {
      const token = await service.upsertDeviceToken(userId, {
        deviceToken,
        deviceId: deviceId.trim(),
        environment,
        platform: body.platform || "ios",
      });
      return res.status(201).json({ deviceToken: token });
    } catch (error) {
      return res.status(error.code === "PT429" ? 429 : 500).json({ error: "Unable to register device token" });
    }
  });

  app.post("/v1/flights/:flightInstanceId/active-viewer", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const result = await service.registerActiveViewer(userId, req.params.flightInstanceId);
      if (!result) return res.status(404).json({ error: "Flight not found" });
      return res.json(result);
    } catch (_error) {
      return res.status(500).json({ error: "Unable to register active viewer" });
    }
  });

  app.get("/v1/flights/:flightInstanceId/weather-insight", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const insight = await service.getFlightWeatherInsight(req.params.flightInstanceId, { userId, cacheStatus: "detail_view" });
      if (!insight) return res.status(404).json({ error: "Flight not found" });
      return res.json({ weatherInsight: insight });
    } catch (_error) {
      return res.status(500).json({ error: "Unable to load weather insight" });
    }
  });
}

function validAlertPreferences(value, detailed) {
  if (!value || typeof value !== "object" || Array.isArray(value) || JSON.stringify(value).length > 4096) return false;
  const keys = detailed ? ["gateChange", "delayUpdates", "boardingTime", "takeoffLanding", "baggageClaim", "inboundAircraft", "flightPlans", "enabled", "low", "medium", "high", "critical"] : ["low", "medium", "high", "critical"];
  return Object.entries(value).every(([key, item]) => {
    if (detailed && key === "quietHours") return item && typeof item === "object" && !Array.isArray(item) &&
      Object.entries(item).every(([name, number]) => ["startHour", "endHour", "startMinute", "endMinute"].includes(name) &&
        Number.isInteger(number) && number >= 0 && number <= (name.endsWith("Hour") ? 23 : 59));
    return keys.includes(key) && typeof item === "boolean";
  });
}
module.exports = { mountSharedFlightRoutes, validAlertPreferences };
