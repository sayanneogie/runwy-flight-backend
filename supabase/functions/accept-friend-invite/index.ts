import { withRequestProtection, consumeEdgeQuota } from "../_shared/request-protection.ts";
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError, jsonResponse, requireJsonBody } from "../_shared/http.ts";
import {
  createAdminClient,
  fetchUserSummary,
  requireAuthenticatedUser,
  sha256,
} from "../_shared/supabase.ts";

type InviteTokenRequest = {
  token?: string;
};

serve((request) => withRequestProtection(request, "accept-friend-invite", async () => {
  const cors = handleCors(request);
  if (cors) {
    return cors;
  }

  try {
    if (request.method !== "POST") {
      throw new HttpError(405, "Method not allowed.");
    }

    const user = await requireAuthenticatedUser(request);
    await consumeEdgeQuota("edge:accept-friend-invite:user", user.id, 30);
    const body = await requireJsonBody<InviteTokenRequest>(request);
    const token = typeof body?.token === "string" ? body.token.trim() : "";
    if (!/^[a-f0-9]{64}$/i.test(token)) {
      throw new HttpError(400, "Invite token is required.");
    }

    const admin = createAdminClient();
    const tokenHash = await sha256(token);
    const { data: result, error } = await admin.rpc("runwy_accept_circle_invite", {
      p_token_hash: tokenHash, p_actor: user.id,
    });
    if (error) {
      throw new HttpError(error.code === "22023" ? 409 : 500,
        error.code === "22023" ? "This invite is no longer available." : "Unable to accept invite.");
    }

    const inviter = await fetchUserSummary(admin, result.inviter_user_id);

    return jsonResponse({
      relationship_created: result.relationship_created,
      member: {
        id: result.relationship_id,
        user_id: inviter.userID,
        display_name: inviter.displayName,
        picture_url: inviter.pictureURL,
        share_scope: result.share_scope,
        can_receive_alerts: true,
        upcoming_flight_count: 0,
        live_flight_count: 0,
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
}));
