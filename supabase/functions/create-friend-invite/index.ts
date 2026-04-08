import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError } from "../_shared/http.ts";
import {
  buildInviteURL,
  createAdminClient,
  makeInviteToken,
  requireAuthenticatedUser,
  sha256,
} from "../_shared/supabase.ts";

serve(async (request) => {
  const cors = handleCors(request);
  if (cors) {
    return cors;
  }

  try {
    if (request.method !== "POST") {
      throw new HttpError(405, "Method not allowed.");
    }

    const user = await requireAuthenticatedUser(request);
    const admin = createAdminClient();
    const token = makeInviteToken();
    const tokenHash = await sha256(token);
    const expiresAt = new Date(Date.now() + (7 * 24 * 60 * 60 * 1000)).toISOString();

    const { error } = await admin
      .from("friend_invites")
      .insert({
        inviter_user_id: user.id,
        token_hash: tokenHash,
        status: "pending",
        default_share_scope: "future_flights",
        expires_at: expiresAt,
      });

    if (error) {
      throw new HttpError(500, error.message);
    }

    return new Response(
      JSON.stringify({
        invite_url: buildInviteURL(token),
      }),
      {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        },
      },
    );
  } catch (error) {
    return errorResponse(error);
  }
});
