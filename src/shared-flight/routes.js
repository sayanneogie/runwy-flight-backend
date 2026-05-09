"use strict";

function mountSharedFlightRoutes(app, service) {
  app.get("/v1/flights/search", async (req, res) => {
    try {
      const flight = await service.searchFlight(req.query, { userId: req.auth?.userId || null });
      return res.json(flight);
    } catch (error) {
      return res.status(error.statusCode || 500).json({ error: error.message || "Unable to search flight" });
    }
  });

  app.post("/v1/user-flights", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      const result = await service.saveUserFlight(userId, req.body || {});
      return res.status(201).json(result);
    } catch (error) {
      return res.status(error.statusCode || 500).json({ error: error.message || "Unable to save flight" });
    }
  });

  app.get("/v1/user-flights", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    try {
      return res.json({ flights: await service.listUserFlights(userId) });
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
    if ("user_label" in body) patch.user_label = body.user_label;
    if ("userLabel" in body) patch.user_label = body.userLabel;
    if ("visibility" in body) patch.visibility = body.visibility;

    try {
      const updated = await service.updateUserFlight(userId, req.params.id, patch);
      if (!updated) return res.status(404).json({ error: "Saved flight not found" });
      return res.json({ userFlight: updated });
    } catch (error) {
      return res.status(500).json({ error: "Unable to update saved flight" });
    }
  });

  app.post("/v1/device-tokens", async (req, res) => {
    const userId = String(req.auth?.userId || "").trim();
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    const body = req.body || {};
    const deviceToken = String(body.deviceToken || body.device_token || "").trim();
    const environment = String(body.environment || "").trim().toLowerCase();
    if (!deviceToken || !["sandbox", "production"].includes(environment)) {
      return res.status(400).json({ error: "deviceToken and environment are required" });
    }
    try {
      const token = await service.upsertDeviceToken(userId, {
        deviceToken,
        environment,
        platform: body.platform || "ios",
      });
      return res.status(201).json({ deviceToken: token });
    } catch (error) {
      return res.status(500).json({ error: "Unable to register device token" });
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

module.exports = { mountSharedFlightRoutes };
