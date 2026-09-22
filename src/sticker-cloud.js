"use strict";

function stickerCloudAccessHandler({ query, paidAccess }) {
  return async (req, res) => {
    const userId = req.auth?.userId;
    if (!userId) return res.status(401).json({ error: "Sign in is required" });
    if (!query) return res.status(503).json({ error: "Cloud storage unavailable" });
    try {
      const access = await paidAccess.membership(userId);
      if (!access.paid || !access.verified || !Number.isFinite(access.until) || access.until <= Date.now()) {
        await query("delete from runwy_security.sticker_cloud_access where user_id=$1::uuid", [userId]);
        return res.status(access.verified ? 403 : 503).json({ error: access.verified ? "Sticker cloud sync requires First Class" : "Membership verification unavailable" });
      }
      // A cached membership result cannot extend a lease past its verification window.
      const { rows } = await query("select public.runwy_grant_sticker_cloud_access($1::uuid,$2::timestamptz) as expires_at",
        [userId,new Date(access.until).toISOString()]);
      return res.json({ allowed: true, expiresAt: rows[0].expires_at });
    } catch {
      return res.status(503).json({ error: "Unable to authorize sticker cloud sync" });
    }
  };
}
module.exports = { stickerCloudAccessHandler };
