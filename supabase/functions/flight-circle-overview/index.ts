import { withRequestProtection, consumeEdgeQuota } from "../_shared/request-protection.ts";
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError, jsonResponse } from "../_shared/http.ts";
import { createAdminClient, requireAuthenticatedUser } from "../_shared/supabase.ts";

serve((request) => withRequestProtection(request, "flight-circle-overview", async () => {
  const cors = handleCors(request);
  if (cors) return cors;
  try {
    if (request.method !== "GET") throw new HttpError(405, "Method not allowed.");
    const user = await requireAuthenticatedUser(request);
    await consumeEdgeQuota("edge:flight-circle-overview:user", user.id, 60);
    const url = new URL(request.url);
    const offset = Number(url.searchParams.get("offset") ?? 0);
    const limit = Number(url.searchParams.get("limit") ?? 100);
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100000 ||
        !Number.isSafeInteger(limit) || limit < 1 || limit > 200) {
      throw new HttpError(400, "Invalid page.");
    }
    const { data, error } = await createAdminClient().rpc("runwy_circle_overview", {
      p_viewer: user.id, p_limit: limit, p_offset: offset,
    });
    if (error) throw new HttpError(500, "Unable to load Runwy Circle.");
    return jsonResponse(data);
  } catch (error) { return errorResponse(error); }
}));
