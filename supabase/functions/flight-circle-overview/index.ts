import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { errorResponse, handleCors, HttpError, jsonResponse } from "../_shared/http.ts";
import {
  createAdminClient,
  fetchUserSummary,
  requireAuthenticatedUser,
} from "../_shared/supabase.ts";

type RelationshipRow = {
  id: string;
  user_a: string;
  user_b: string;
  relationship_status: "active" | "blocked" | "removed";
};

type PermissionRow = {
  relationship_id: string;
  owner_user_id: string;
  viewer_user_id: string;
  share_scope: "future_flights" | "all_flights" | "selected_flights";
  can_receive_alerts: boolean;
};

type UserFlightRow = {
  id: string;
  user_id: string;
  marketing_airline_name: string | null;
  operating_airline_name: string | null;
  display_flight_number: string | null;
  origin_iata: string | null;
  destination_iata: string | null;
  scheduled_departure: string | null;
  lifecycle_state: string | null;
  status: string | null;
  deleted_at: string | null;
};

serve(async (request) => {
  const cors = handleCors(request);
  if (cors) {
    return cors;
  }

  try {
    if (request.method !== "GET") {
      throw new HttpError(405, "Method not allowed.");
    }

    const user = await requireAuthenticatedUser(request);
    const admin = createAdminClient();

    const relationshipsResponse = await admin
      .from("friend_relationships")
      .select("id, user_a, user_b, relationship_status")
      .or(`user_a.eq.${user.id},user_b.eq.${user.id}`)
      .eq("relationship_status", "active");

    if (relationshipsResponse.error) {
      throw new HttpError(500, relationshipsResponse.error.message);
    }

    const relationships = (relationshipsResponse.data ?? []) as RelationshipRow[];
    if (!relationships.length) {
      return jsonResponse({
        members: [],
        shared_flights: [],
      });
    }

    const friendIDs = relationships.map((relationship) =>
      relationship.user_a === user.id ? relationship.user_b : relationship.user_a
    );

    const permissionsResponse = await admin
      .from("friend_permissions")
      .select("relationship_id, owner_user_id, viewer_user_id, share_scope, can_receive_alerts")
      .eq("viewer_user_id", user.id)
      .in("owner_user_id", friendIDs);

    if (permissionsResponse.error) {
      throw new HttpError(500, permissionsResponse.error.message);
    }

    const permissions = (permissionsResponse.data ?? []) as PermissionRow[];
    const permissionByOwner = new Map<string, PermissionRow>();
    for (const permission of permissions) {
      permissionByOwner.set(permission.owner_user_id, permission);
    }

    const flightResponse = await admin
      .from("user_flights")
      .select("id, user_id, marketing_airline_name, operating_airline_name, display_flight_number, origin_iata, destination_iata, scheduled_departure, lifecycle_state, status, deleted_at")
      .in("user_id", friendIDs)
      .gte("scheduled_departure", new Date(Date.now() - (12 * 60 * 60 * 1000)).toISOString())
      .is("deleted_at", null)
      .order("scheduled_departure", { ascending: true })
      .limit(60);

    if (flightResponse.error) {
      throw new HttpError(500, flightResponse.error.message);
    }

    const flights = (flightResponse.data ?? []) as UserFlightRow[];
    const now = Date.now();
    const sharedFlights: Array<Record<string, unknown>> = [];
    const memberSummaries: Array<Record<string, unknown>> = [];

    for (const relationship of relationships) {
      const friendID = relationship.user_a === user.id ? relationship.user_b : relationship.user_a;
      const permission = permissionByOwner.get(friendID);
      if (!permission) {
        continue;
      }

      const summary = await fetchUserSummary(admin, friendID);
      const friendFlights = flights.filter((flight) => {
        if (flight.user_id !== friendID) {
          return false;
        }

        if (flight.deleted_at) {
          return false;
        }

        if (permission.share_scope === "selected_flights") {
          return false;
        }

        if (permission.share_scope === "future_flights" &&
            ["landed", "archived", "deleted"].includes(String(flight.lifecycle_state ?? "").toLowerCase())) {
          return false;
        }

        return true;
      });

      const upcomingFlightCount = friendFlights.filter((flight) => {
        const departure = flight.scheduled_departure ? Date.parse(flight.scheduled_departure) : Number.NaN;
        return Number.isFinite(departure) && departure >= now;
      }).length;

      memberSummaries.push({
        id: relationship.id,
        user_id: summary.userID,
        display_name: summary.displayName,
        picture_url: summary.pictureURL,
        share_scope: permission.share_scope,
        can_receive_alerts: permission.can_receive_alerts,
        upcoming_flight_count: upcomingFlightCount,
        live_flight_count: 0,
      });

      for (const flight of friendFlights) {
        const routeTitle = [flight.origin_iata, flight.destination_iata]
          .map((value) => value?.trim())
          .filter(Boolean)
          .join(" to ");

        const departureDate = flight.scheduled_departure ? new Date(flight.scheduled_departure) : null;
        const departureSummary = departureDate
          ? `Departs ${departureDate.toLocaleString("en-US", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" })}`
          : "Departure time pending";

        sharedFlights.push({
          id: flight.id,
          owner_user_id: summary.userID,
          owner_display_name: summary.displayName,
          owner_picture_url: summary.pictureURL,
          airline_name: flight.marketing_airline_name ?? flight.operating_airline_name ?? "Shared flight",
          flight_number: flight.display_flight_number ?? "Flight",
          route_title: routeTitle || "Route pending",
          departure_summary: departureSummary,
          status_summary: flight.status ?? "Upcoming",
          is_live: ["active"].includes(String(flight.lifecycle_state ?? "").toLowerCase()),
        });
      }
    }

    return jsonResponse({
      members: memberSummaries,
      shared_flights: sharedFlights.slice(0, 20),
    });
  } catch (error) {
    return errorResponse(error);
  }
});
