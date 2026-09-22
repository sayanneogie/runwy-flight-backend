import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createAdminClient, requireAuthenticatedUser } from "../_shared/supabase.ts";
import { errorResponse, HttpError, jsonResponse } from "../_shared/http.ts";
import { withRequestProtection, consumeEdgeQuota } from "../_shared/request-protection.ts";

serve((request) => withRequestProtection(request, "delete-account", async () => {
  try {
    if (request.method !== "POST") throw new HttpError(405, "Method not allowed.");
    const user = await requireAuthenticatedUser(request);
    await consumeEdgeQuota("edge:delete-account:user", user.id, 10);
    const admin = createAdminClient();
    // Each batch is durable and retryable. SQL stores progress and enumerates
    // ownership; the Storage API removes the actual objects and metadata.
    for (let batch = 0; batch < 20; batch++) {
      const { data, error } = await admin.rpc("runwy_account_deletion_batch", { p_actor: user.id });
      if (error) throw new HttpError(500, "Unable to prepare account deletion.");
      const objects = data as Array<{ bucket_id: string; name: string }>;
      if (!objects.length) {
        const { data: deleted, error: finishError } = await admin.rpc("runwy_finish_account_deletion", { p_actor: user.id });
        if (finishError || deleted !== true) throw new HttpError(500, "Account deletion could not finish. Please retry.");
        return jsonResponse({ deleted: true });
      }
      for (const bucket of ["profile-avatars", "ticket-stickers"]) {
        const paths = objects.filter((object) => object.bucket_id === bucket).map((object) => object.name);
        if (paths.length) {
          const { error: storageError } = await admin.storage.from(bucket).remove(paths);
          if (storageError) throw new HttpError(503, "File cleanup was interrupted. Retry to continue deleting your account.");
        }
      }
    }
    throw new HttpError(503, "Account cleanup is still in progress. Retry to continue.");
  } catch (error) { return errorResponse(error); }
}));
