export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
} as const;

export class HttpError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function handleCors(request: Request): Response | null {
  if (request.method !== "OPTIONS") {
    return null;
  }

  return new Response("ok", { headers: corsHeaders });
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders,
      "Cache-Control": "no-store",
      "Content-Type": "application/json",
    },
  });
}

export function errorResponse(error: unknown): Response {
  if (error instanceof HttpError) {
    const response = jsonResponse(
      {
        error: error.message,
        message: error.message,
      },
      error.status,
    );
    if ([429, 503].includes(error.status)) response.headers.set("Retry-After", "60");
    return response;
  }

  const message = error instanceof Error ? error.message : "Unknown backend error";
  return jsonResponse(
    {
      error: message,
      message,
    },
    500,
  );
}

export async function requireJsonBody<T>(request: Request): Promise<T> {
  const maxBytes = 16 * 1024;
  if (Number(request.headers.get("content-length")) > maxBytes) {
    throw new HttpError(413, "Request body is too large.");
  }
  const reader = request.body?.getReader();
  if (!reader) throw new HttpError(400, "Request body must be valid JSON.");
  const chunks: Uint8Array[] = [];
  let total = 0;
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; void reader.cancel().catch(() => {}); }, 5000);
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (timedOut) throw new HttpError(408, "Request body timed out.");
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new HttpError(413, "Request body is too large.");
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    return JSON.parse(new TextDecoder().decode(bytes)) as T;
  } catch (error) {
    if (error instanceof HttpError) throw error;
    throw new HttpError(400, "Request body must be valid JSON.");
  } finally { clearTimeout(timer); reader.releaseLock(); }
}
