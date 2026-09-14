import ipaddr from "npm:ipaddr.js@2.2.0";
import { createAdminClient, sha256 } from "./supabase.ts";
import { corsHeaders, errorResponse, HttpError } from "./http.ts";

const localCounters = new Map<string, { hits: number; resetAt: number }>();
let active = 0;
let nextSweep = 0;
let lastLog = 0;
let rejected = 0;

function clientNetwork(request: Request): string {
  // Supabase's hosted Cloudflare gateway supplies this header. Never trust a
  // caller-controlled device ID or the leftmost X-Forwarded-For entry.
  try {
    const address = ipaddr.process(request.headers.get("cf-connecting-ip") || "");
    if (address.kind() === "ipv4") return address.toString();
    const bytes = address.toByteArray();
    bytes.fill(0, 7);
    return `${ipaddr.fromByteArray(bytes).toNormalizedString()}/56`;
  } catch { return "unknown"; }
}

export async function consumeEdgeQuota(namespace: string, identity: string, limit: number): Promise<void> {
  const admin = createAdminClient();
  const { data, error } = await admin.rpc("runwy_consume_rate_limit", {
    p_namespace: namespace,
    p_key_hash: await sha256(identity),
    p_window_ms: 60000,
    p_limit: limit,
  }).abortSignal(AbortSignal.timeout(2000));
  if (error || !data?.[0]) throw new HttpError(503, "Service is busy. Please try again shortly.");
  if (Number(data[0].total_hits) > limit) throw new HttpError(429, "Too many requests. Please try again shortly.");
}

export async function withRequestProtection(request: Request, namespace: string,
  handler: () => Promise<Response>): Promise<Response> {
  let acquired = false;
  try {
    const now = Date.now();
    if (now >= nextSweep) {
      for (const [key, counter] of localCounters) if (counter.resetAt <= now) localCounters.delete(key);
      nextSweep = now + 60000;
    }
    const key = clientNetwork(request);
    let counter = localCounters.get(key);
    if (!counter || counter.resetAt <= now) {
      if (!counter && localCounters.size >= 1000) throw new HttpError(503, "Service is busy. Please try again shortly.");
      counter = { hits: 0, resetAt: now + 60000 };
      localCounters.set(key, counter);
    }
    counter.hits = Math.min(121, counter.hits + 1);
    if (counter.hits > 120) throw new HttpError(429, "Too many requests. Please try again shortly.");
    if (active >= 16) throw new HttpError(503, "Service is busy. Please try again shortly.");
    active++;
    acquired = true;
    if (request.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
    // The global bucket also bounds work if a deployment has no trusted client IP.
    await consumeEdgeQuota(`edge:${namespace}:global`, "all", 600);
    await consumeEdgeQuota(`edge:${namespace}:ip`, key, 60);
    return await handler();
  } catch (error) {
    rejected++;
    if (Date.now() - lastLog >= 30000) {
      console.warn(JSON.stringify({ event: "edge_request_protection", namespace, rejected }));
      lastLog = Date.now();
      rejected = 0;
    }
    const response = errorResponse(error instanceof HttpError ? error : new HttpError(503, "Service is busy. Please try again shortly."));
    if ([429, 503].includes(response.status)) response.headers.set("Retry-After", "60");
    response.headers.set("Cache-Control", "no-store");
    return response;
  } finally { if (acquired) active--; }
}
