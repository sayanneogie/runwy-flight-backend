import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError, jsonResponse, requireJsonBody } from "../_shared/http.ts";
import {
  createAdminClient,
  fetchUserSummary,
  orderedRelationshipPair,
  requireAuthenticatedUser,
  sha256,
} from "../_shared/supabase.ts";

type InviteTokenRequest = {
  token?: string;
};

type InviteRow = {
  id: string;
  inviter_user_id: string;
  default_share_scope: "future_flights" | "all_flights" | "selected_flights";
  expires_at: string;
};

type RelationshipRow = {
  id: string;
  user_a: string;
  user_b: string;
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

    const user = await requireAuthenticatedUser(request);
    const body = await requireJsonBody<InviteTokenRequest>(request);
    const token = body.token?.trim();
    if (!token) {
      throw new HttpError(400, "Invite token is required.");
    }

    const admin = createAdminClient();
    const tokenHash = await sha256(token);
    const now = new Date().toISOString();

    const inviteResponse = await admin
      .from("friend_invites")
      .select("id, inviter_user_id, default_share_scope, expires_at")
      .eq("token_hash", tokenHash)
      .eq("status", "pending")
      .gt("expires_at", now)
      .maybeSingle();

    if (inviteResponse.error) {
      throw new HttpError(500, inviteResponse.error.message);
    }

    const invite = inviteResponse.data as InviteRow | null;
    if (!invite) {
      throw new HttpError(404, "This Flight Circle invite is no longer available.");
    }

    if (invite.inviter_user_id === user.id) {
      throw new HttpError(409, "You cannot accept your own Flight Circle invite.");
    }

    const [userA, userB] = orderedRelationshipPair(invite.inviter_user_id, user.id);
    const existingRelationship = await admin
      .from("friend_relationships")
      .select("id, user_a, user_b")
      .eq("user_a", userA)
      .eq("user_b", userB)
      .maybeSingle();

    if (existingRelationship.error) {
      throw new HttpError(500, existingRelationship.error.message);
    }

    let relationship = existingRelationship.data as RelationshipRow | null;
    let relationshipCreated = false;

    if (!relationship) {
      const insertedRelationship = await admin
        .from("friend_relationships")
        .insert({
          user_a: userA,
          user_b: userB,
          relationship_status: "active",
          created_by_user_id: user.id,
        })
        .select("id, user_a, user_b")
        .single();

      if (insertedRelationship.error) {
        throw new HttpError(500, insertedRelationship.error.message);
      }

      relationship = insertedRelationship.data as RelationshipRow;
      relationshipCreated = true;
    }

    const permissionRows = [
      {
        relationship_id: relationship.id,
        owner_user_id: invite.inviter_user_id,
        viewer_user_id: user.id,
        share_scope: invite.default_share_scope,
        can_view_live: true,
        can_view_history: false,
        can_receive_alerts: true,
      },
      {
        relationship_id: relationship.id,
        owner_user_id: user.id,
        viewer_user_id: invite.inviter_user_id,
        share_scope: "future_flights",
        can_view_live: true,
        can_view_history: false,
        can_receive_alerts: true,
      },
    ];

    const permissionsUpsert = await admin
      .from("friend_permissions")
      .upsert(permissionRows, { onConflict: "owner_user_id,viewer_user_id" });

    if (permissionsUpsert.error) {
      throw new HttpError(500, permissionsUpsert.error.message);
    }

    const inviteUpdate = await admin
      .from("friend_invites")
      .update({
        status: "accepted",
        accepted_by_user_id: user.id,
        accepted_at: now,
      })
      .eq("id", invite.id);

    if (inviteUpdate.error) {
      throw new HttpError(500, inviteUpdate.error.message);
    }

    const inviter = await fetchUserSummary(admin, invite.inviter_user_id);

    return jsonResponse({
      relationship_created: relationshipCreated,
      member: {
        id: relationship.id,
        user_id: inviter.userID,
        display_name: inviter.displayName,
        picture_url: inviter.pictureURL,
        share_scope: invite.default_share_scope,
        can_receive_alerts: true,
        upcoming_flight_count: 0,
        live_flight_count: 0,
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
});
