const test = require("node:test");
const assert = require("node:assert/strict");

const catalog = require("../data/airports.json");

test("every generated airport has a valid IANA timezone", () => {
  assert.ok(Array.isArray(catalog.airports));
  assert.ok(catalog.airports.length > 9000);

  for (const airport of catalog.airports) {
    assert.equal(typeof airport.timeZoneIdentifier, "string", `${airport.code} is missing a timezone`);
    assert.doesNotThrow(
      () => new Intl.DateTimeFormat("en", { timeZone: airport.timeZoneIdentifier }),
      `${airport.code} has an invalid timezone: ${airport.timeZoneIdentifier}`
    );
  }
});
