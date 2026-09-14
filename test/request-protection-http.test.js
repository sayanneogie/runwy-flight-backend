const test = require("node:test");
const assert = require("node:assert/strict");
process.env.DATABASE_URL = "";
process.env.ALLOW_INSECURE_NO_AUTH = "false";
process.env.SUPABASE_URL = "https://test.supabase.co";
process.env.SUPABASE_ANON_KEY = "test";
process.env.SUPABASE_JWT_SECRET = "";
process.env.PROVIDER_CALLS_ENABLED = "false";
process.env.PREAUTH_IP_REQUESTS_PER_MINUTE = "2";
process.env.TRUST_PROXY_HOPS = "1";
process.env.WEBHOOK_SHARED_SECRET = "test-webhook-secret";
const { app } = require("../src/server");

test("production middleware blocks requests before auth and JSON work; webhook aliases share authentication", async () => {
  const server = app.listen(0, "127.0.0.1");
  const originalFetch = global.fetch;
  let authCalls = 0;
  global.fetch = async (url, options) => {
    if (String(url).startsWith(process.env.SUPABASE_URL)) {
      authCalls++;
      return new Response("{}", { status: 401 });
    }
    return originalFetch(url, options);
  };
  try {
    await new Promise(resolve => server.once("listening", resolve));
    const base = `http://127.0.0.1:${server.address().port}`;
    const token = `${Buffer.from('{"alg":"HS256"}').toString("base64url")}.${Buffer.from(JSON.stringify({ sub: "user", exp: Math.floor(Date.now()/1000)+600 })).toString("base64url")}.invalid`;
    const request = (path, ip, options = {}) => originalFetch(base + path, {
      ...options, headers: { "X-Forwarded-For": ip, ...options.headers },
    });
    for (let i = 0; i < 2; i++) {
      const result = await request("/v1/user-flights", "192.0.2.1", { headers: { Authorization: `Bearer ${token}`, "X-Device-Id": `device-${i}` } });
      assert.equal(result.status, 401);
    }
    const blocked = await request("/v1/user-flights", "192.0.2.1", { method: "POST", headers: { Authorization: `Bearer ${token}`, "X-Device-Id": "rotated", "Content-Type": "application/json" }, body: "invalid-json" });
    assert.equal(blocked.status, 429);
    assert.ok(blocked.headers.get("retry-after"));
    assert.equal(authCalls, 2);
    // The proxy appends the actual client as the rightmost hop; a supplied prefix does not reset it.
    assert.equal((await request("/v1/user-flights", "203.0.113.99, 192.0.2.1")).status, 429);
    // Liveness stays available without consuming shared quotas.
    assert.equal((await request("/health", "192.0.2.1")).status, 200);
    for (const [i, route] of ["/webhooks/flightaware/alerts", "/v1/webhooks/flightaware/alerts", "/v1/webhooks/flightaware"].entries()) {
      assert.equal((await request(route, `192.0.2.${10+i}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "invalid-json" })).status, 401);
      const valid = await request(route, `192.0.2.${10+i}`, { method: "POST", headers: { "X-Runwy-Webhook-Secret": "test-webhook-secret", "Content-Type": "application/json" }, body: "{}" });
      assert.notEqual(valid.status, 401, route);
    }
    const oversized = await request("/v1/user-flights", "192.0.2.50", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ value: "x".repeat(103000) }) });
    assert.equal(oversized.status, 413);
  } finally {
    global.fetch = originalFetch;
    await new Promise(resolve => server.close(resolve));
  }
});
