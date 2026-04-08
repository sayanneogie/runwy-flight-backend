import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError, jsonResponse, requireJsonBody } from "../_shared/http.ts";
import { createAdminClient, fetchUserSummary, sha256 } from "../_shared/supabase.ts";

type InviteTokenRequest = {
  token?: string;
};

type InviteRow = {
  inviter_user_id: string;
  message: string | null;
  expires_at: string | null;
};

serve(async (request) => {
  const cors = handleCors(request);
  if (cors) {
    return cors;
  }

  try {
    if (request.method !== "POST") {
      throw new HttpError(405, "Method not allowed.");
    }

    const body = await requireJsonBody<InviteTokenRequest>(request);
    const token = body.token?.trim();
    if (!token) {
      throw new HttpError(400, "Invite token is required.");
    }

    const admin = createAdminClient();
    const tokenHash = await sha256(token);

    const previewResponse = await admin
      .from("friend_invites")
      .select("inviter_user_id, message, expires_at")
      .eq("token_hash", tokenHash)
      .eq("status", "pending")
      .gt("expires_at", new Date().toISOString())
      .maybeSingle();

    if (previewResponse.error) {
      throw new HttpError(500, previewResponse.error.message);
    }

    const data = previewResponse.data as InviteRow | null;
    if (!data) {
      throw new HttpError(404, "This Flight Circle invite is no longer available.");
    }

    const inviter = await fetchUserSummary(admin, data.inviter_user_id);

    return jsonResponse({
      inviter_user_id: inviter.userID,
      inviter_display_name: inviter.displayName,
      inviter_picture_url: inviter.pictureURL,
      message: data.message,
      expires_at: data.expires_at,
    });
  } catch (error) {
    return errorResponse(error);
  }
});
