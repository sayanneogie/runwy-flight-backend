"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { validatePushTokenPayload } = require("../src/request-schemas");

test("push-token payload preserves the app APNs environment", () => {
  const token = "a".repeat(64);
  const parsed = validatePushTokenPayload({
    token,
    platform: "ios",
    environment: "sandbox",
  });

  assert.equal(parsed.error, undefined);
  assert.deepEqual(parsed.value, {
    token,
    platform: "ios",
    environment: "sandbox",
  });
});

test("push-token payload rejects unknown APNs environments", () => {
  const parsed = validatePushTokenPayload({
    token: "b".repeat(64),
    platform: "ios",
    environment: "staging",
  });

  assert.equal(parsed.error, "Invalid APNs environment");
});
