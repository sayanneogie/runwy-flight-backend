import { requireJsonBody, HttpError, errorResponse } from "./http.ts";

function assert(value: boolean, message: string) { if (!value) throw new Error(message); }

Deno.test("bounded JSON reader counts actual streamed bytes and rejects oversized bodies", async () => {
  const stream = new ReadableStream({ start(controller) {
    controller.enqueue(new TextEncoder().encode('{"value":"'));
    controller.enqueue(new Uint8Array(17000).fill(65));
    controller.close();
  } });
  try {
    await requireJsonBody(new Request("https://example.test", { method: "POST", body: stream }));
    throw new Error("Accepted an oversized body");
  } catch (error) { assert(error instanceof HttpError && error.status === 413, "Expected 413"); }
  const data = await requireJsonBody<{ value: string }>(new Request("https://example.test", {
    method: "POST", body: JSON.stringify({ value: "✈️" }),
  }));
  assert(data.value === "✈️", "Unicode must survive decoding");
});

Deno.test("malformed JSON produces a controlled response and throttling includes retry guidance", async () => {
  try {
    await requireJsonBody(new Request("https://example.test", { method: "POST", body: "{" }));
    throw new Error("Accepted invalid JSON");
  } catch (error) { assert(error instanceof HttpError && error.status === 400, "Expected 400"); }
  const response = errorResponse(new HttpError(429, "Too many requests"));
  assert(response.headers.get("Retry-After") === "60", "Missing retry header");
  assert(response.headers.get("Cache-Control") === "no-store", "Private responses must not be cached");
});
