import { createClient, type SupabaseClient, type User } from "https://esm.sh/@supabase/supabase-js@2.45.4";
import { HttpError } from "./http.ts";

type JsonMap = Record<string, unknown>;

const supabaseURL = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseAnonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";
const supabaseServiceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const runwyAppBaseURL = Deno.env.get("RUNWY_APP_BASE_URL") ?? "https://runwy.app";

function requireEnv(name: string, value: string): string {
  if (!value.trim()) {
    throw new HttpError(500, `${name} is not configured.`);
  }
  return value;
}

export function createAdminClient(): SupabaseClient {
  return createClient(
    requireEnv("SUPABASE_URL", supabaseURL),
    requireEnv("SUPABASE_SERVICE_ROLE_KEY", supabaseServiceRoleKey),
    {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
      },
    },
  );
}

export function createRequestClient(authHeader: string): SupabaseClient {
  return createClient(
    requireEnv("SUPABASE_URL", supabaseURL),
    requireEnv("SUPABASE_ANON_KEY", supabaseAnonKey),
    {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
      },
      global: {
        headers: {
          Authorization: authHeader,
        },
      },
    },
  );
}

export async function requireAuthenticatedUser(request: Request): Promise<User> {
  const authHeader = request.headers.get("Authorization")?.trim();
  if (!authHeader) {
    throw new HttpError(401, "Authorization is required.");
  }

  const client = createRequestClient(authHeader);
  const { data, error } = await client.auth.getUser();
  if (error || !data.user) {
    throw new HttpError(401, "Authorization is invalid or expired.");
  }

  return data.user;
}

function metadataValue(metadata: JsonMap | undefined, key: string): string | null {
  const raw = metadata?.[key];
  if (typeof raw !== "string") {
    return null;
  }

  const trimmed = raw.trim();
  return trimmed.length == 0 ? null : trimmed;
}

function firstNonEmpty(...values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    const trimmed = value?.trim();
    if (trimmed) {
      return trimmed;
    }
  }

  return null;
}

export type CircleUserSummary = {
  userID: string;
  displayName: string;
  pictureURL: string | null;
};

export async function fetchUserSummary(admin: SupabaseClient, userID: string): Promise<CircleUserSummary> {
  const { data, error } = await admin.auth.admin.getUserById(userID);
  if (error || !data.user) {
    throw new HttpError(404, "Flight Circle user could not be found.");
  }

  const metadata = ((data.user.user_metadata ?? {}) as JsonMap);
  const displayName = firstNonEmpty(
    metadataValue(metadata, "full_name"),
    metadataValue(metadata, "name"),
    data.user.email?.split("@")[0],
    "Runwy Traveler",
  ) ?? "Runwy Traveler";

  return {
    userID,
    displayName,
    pictureURL: firstNonEmpty(
      metadataValue(metadata, "picture"),
      metadataValue(metadata, "avatar_url"),
    ),
  };
}

export async function sha256(value: string): Promise<string> {
  const encoded = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export function makeInviteToken(): string {
  return `${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
}

export function buildInviteURL(token: string): string {
  return new URL(`/circle/invite/${token}`, runwyAppBaseURL).toString();
}

export function orderedRelationshipPair(userA: string, userB: string): [string, string] {
  return userA.localeCompare(userB) <= 0 ? [userA, userB] : [userB, userA];
}
