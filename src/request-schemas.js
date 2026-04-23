const { z } = require("zod");

const flightNumberSchema = z
  .string()
  .trim()
  .transform((value) => value.toUpperCase().replace(/\s+/g, ""))
  .refine((value) => /^[A-Z0-9]{3,8}$/.test(value), {
    message: "Invalid flightNumber",
  });

const dateSchema = z
  .string()
  .trim()
  .refine((value) => /^\d{4}-\d{2}-\d{2}$/.test(value), {
    message: "Invalid date (expected YYYY-MM-DD)",
  });

const optionalIataSchema = z.preprocess((value) => {
  if (value == null) return undefined;
  const normalized = String(value).trim().toUpperCase();
  return normalized.length === 0 ? undefined : normalized;
}, z.string().regex(/^[A-Z]{3}$/, "Invalid airport code").optional());

const optionalBooleanQuerySchema = z.preprocess((value) => {
  if (value == null) return undefined;
  if (typeof value === "boolean") return value;

  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return value;
}, z.boolean().optional());

const optionalTimezoneOffsetMinutesSchema = z.preprocess((value) => {
  if (value == null) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }

  const normalized = String(value).trim();
  if (!normalized) return undefined;

  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : value;
}, z.number().int().min(-840).max(840).optional());

const pushPlatformSchema = z
  .string()
  .trim()
  .toLowerCase()
  .refine((value) => /^[a-z0-9_-]{2,20}$/.test(value), {
    message: "Invalid platform",
  });

const pushTokenSchema = z
  .string()
  .trim()
  .refine((value) => /^[A-Fa-f0-9]{64,512}$/.test(value), {
    message: "Invalid APNs token",
  })
  .transform((value) => value.toLowerCase());

const trackPayloadSchema = z.object({
  flightNumber: flightNumberSchema,
  date: dateSchema,
  departureIata: optionalIataSchema,
  arrivalIata: optionalIataSchema,
  timezoneOffsetMinutes: optionalTimezoneOffsetMinutesSchema,
});

const searchQuerySchema = z.object({
  flightNumber: flightNumberSchema,
  date: dateSchema,
  dep: optionalIataSchema,
  arr: optionalIataSchema,
  historical: optionalBooleanQuerySchema,
  preferSchedules: optionalBooleanQuerySchema,
  timezoneOffsetMinutes: optionalTimezoneOffsetMinutesSchema,
});

const routeSearchQuerySchema = z.object({
  date: dateSchema,
  dep: optionalIataSchema,
  arr: optionalIataSchema,
  historical: optionalBooleanQuerySchema,
  preferSchedules: optionalBooleanQuerySchema,
  timezoneOffsetMinutes: optionalTimezoneOffsetMinutesSchema,
}).superRefine((value, ctx) => {
  if (!value.dep || !value.arr) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "Route search requires both dep and arr airport codes",
    });
  }
});

const pushTokenBodySchema = z.object({
  token: pushTokenSchema,
  platform: pushPlatformSchema.default("ios"),
  userId: z.any().optional(),
});

function parseSchema(schema, value) {
  const parsed = schema.safeParse(value);
  if (parsed.success) {
    return { value: parsed.data };
  }

  return {
    error: parsed.error.issues[0]?.message || "Invalid request",
  };
}

function validateTrackPayload(body) {
  return parseSchema(trackPayloadSchema, body);
}

function validateSearchQuery(query) {
  const parsed = parseSchema(searchQuerySchema, query);
  if (parsed.error) return parsed;

  return {
    value: {
      flightNumber: parsed.value.flightNumber,
      date: parsed.value.date,
      departureIata: parsed.value.dep,
      arrivalIata: parsed.value.arr,
      historical: parsed.value.historical === true,
      preferSchedules: parsed.value.preferSchedules === true,
      timezoneOffsetMinutes: parsed.value.timezoneOffsetMinutes,
    },
  };
}

function validateRouteSearchQuery(query) {
  const parsed = parseSchema(routeSearchQuerySchema, query);
  if (parsed.error) return parsed;

  return {
    value: {
      date: parsed.value.date,
      departureIata: parsed.value.dep,
      arrivalIata: parsed.value.arr,
      historical: parsed.value.historical === true,
      preferSchedules: parsed.value.preferSchedules === true,
      timezoneOffsetMinutes: parsed.value.timezoneOffsetMinutes,
    },
  };
}

function validatePushTokenPayload(body) {
  const parsed = parseSchema(pushTokenBodySchema, body);
  if (parsed.error) return parsed;

  return {
    value: {
      token: parsed.value.token,
      platform: parsed.value.platform,
    },
  };
}

module.exports = {
  validatePushTokenPayload,
  validateRouteSearchQuery,
  validateSearchQuery,
  validateTrackPayload,
};
