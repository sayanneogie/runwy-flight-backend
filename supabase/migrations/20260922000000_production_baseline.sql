-- Production application schema baseline, 2026-09-22. SCHEMA ONLY.

-- Fresh databases only. Existing production is baselined by the verified adoption script.

SET check_function_bodies = false;

CREATE SCHEMA IF NOT EXISTS runwy_security;

REVOKE ALL ON SCHEMA runwy_security FROM public,anon,authenticated;

CREATE TABLE "public"."api_usage_logs" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "provider" text NOT NULL,
  "endpoint" text NOT NULL,
  "flight_key" text,
  "user_id" uuid,
  "status_code" integer,
  "response_time_ms" integer,
  "cache_status" text,
  "cost_estimate" numeric,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now(),
  "provider_path" text,
  "request_reason" text
);

CREATE TABLE "public"."device_tokens" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "device_token" text NOT NULL,
  "platform" text DEFAULT 'ios'::text NOT NULL,
  "environment" text NOT NULL,
  "is_active" boolean DEFAULT true,
  "created_at" timestamp with time zone DEFAULT now(),
  "updated_at" timestamp with time zone DEFAULT now()
);

CREATE TABLE "public"."entitlements" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "provider" text DEFAULT 'revenuecat'::text NOT NULL,
  "product_id" text,
  "entitlement_key" text NOT NULL,
  "is_active" boolean DEFAULT false NOT NULL,
  "expires_at" timestamp with time zone,
  "last_synced_at" timestamp with time zone DEFAULT now() NOT NULL,
  "raw_payload" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."flight_definitions" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "airline_code" text NOT NULL,
  "flight_number" text NOT NULL,
  "airline_name" text,
  "typical_origin_airport" text,
  "typical_destination_airport" text,
  "provider" text,
  "created_at" timestamp with time zone DEFAULT now(),
  "updated_at" timestamp with time zone DEFAULT now()
);

CREATE TABLE "public"."flight_event_logs" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "flight_instance_id" uuid,
  "flight_key" text NOT NULL,
  "fa_flight_id" text,
  "ident" text,
  "event_type" text NOT NULL,
  "event_status" text,
  "event_time" timestamp with time zone,
  "source" text DEFAULT 'flightaware'::text NOT NULL,
  "raw_payload" jsonb NOT NULL,
  "normalized_payload" jsonb NOT NULL,
  "dedupe_key" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now()
);

CREATE TABLE "public"."flight_events" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "flight_instance_id" uuid,
  "event_type" text NOT NULL,
  "event_severity" text DEFAULT 'low'::text NOT NULL,
  "old_value" jsonb,
  "new_value" jsonb,
  "summary" text,
  "provider" text,
  "provider_event_time" timestamp with time zone,
  "confidence" text DEFAULT 'medium'::text,
  "notification_required" boolean DEFAULT false,
  "created_at" timestamp with time zone DEFAULT now(),
  "state_revision" bigint DEFAULT 0 NOT NULL,
  "flight_snapshot" jsonb,
  "fanout_completed_at" timestamp with time zone
);

CREATE TABLE "public"."flight_instance_aliases" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "alias_key" text NOT NULL,
  "flight_instance_id" uuid,
  "created_at" timestamp with time zone DEFAULT now()
);

CREATE TABLE "public"."flight_instances" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "flight_key" text NOT NULL,
  "provider_flight_id" text,
  "airline_code" text NOT NULL,
  "flight_number" text NOT NULL,
  "departure_date" date NOT NULL,
  "origin_airport" text,
  "destination_airport" text,
  "scheduled_departure_at" timestamp with time zone,
  "scheduled_arrival_at" timestamp with time zone,
  "estimated_departure_at" timestamp with time zone,
  "estimated_arrival_at" timestamp with time zone,
  "actual_departure_at" timestamp with time zone,
  "actual_arrival_at" timestamp with time zone,
  "status" text DEFAULT 'unknown'::text NOT NULL,
  "status_detail" text,
  "gate" text,
  "terminal" text,
  "baggage_belt" text,
  "position_lat" double precision,
  "position_lon" double precision,
  "altitude" integer,
  "ground_speed" integer,
  "heading" integer,
  "provider" text,
  "provider_alert_id" text,
  "provider_alert_status" text DEFAULT 'unavailable'::text NOT NULL,
  "provider_alert_created_at" timestamp with time zone,
  "provider_alert_expires_at" timestamp with time zone,
  "last_webhook_received_at" timestamp with time zone,
  "live_data_source" text DEFAULT 'on_demand'::text NOT NULL,
  "streaming_status" text DEFAULT 'disabled'::text NOT NULL,
  "stream_registered_at" timestamp with time zone,
  "last_stream_event_at" timestamp with time zone,
  "last_poll_reason" text,
  "refresh_priority" text DEFAULT 'normal'::text NOT NULL,
  "data_confidence" text DEFAULT 'unknown'::text,
  "normalized_data" jsonb,
  "raw_provider_response" jsonb,
  "last_fetched_at" timestamp with time zone,
  "fresh_until" timestamp with time zone,
  "needs_revalidation" boolean DEFAULT false,
  "is_final" boolean DEFAULT false,
  "created_at" timestamp with time zone DEFAULT now(),
  "updated_at" timestamp with time zone DEFAULT now(),
  "state_revision" bigint DEFAULT 0 NOT NULL
);

CREATE TABLE "public"."flight_snapshots" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "flight_instance_id" uuid,
  "status" text,
  "estimated_departure_at" timestamp with time zone,
  "estimated_arrival_at" timestamp with time zone,
  "actual_departure_at" timestamp with time zone,
  "actual_arrival_at" timestamp with time zone,
  "gate" text,
  "terminal" text,
  "baggage_belt" text,
  "position_lat" double precision,
  "position_lon" double precision,
  "altitude" integer,
  "ground_speed" integer,
  "heading" integer,
  "raw_provider_response" jsonb,
  "normalized_data" jsonb,
  "created_at" timestamp with time zone DEFAULT now(),
  "state_revision" bigint DEFAULT 0 NOT NULL
);

CREATE TABLE "public"."friend_invites" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "inviter_user_id" uuid NOT NULL,
  "token_hash" text NOT NULL,
  "status" text DEFAULT 'pending'::text NOT NULL,
  "default_share_scope" text DEFAULT 'future_flights'::text NOT NULL,
  "message" text,
  "expires_at" timestamp with time zone DEFAULT (now() + '7 days'::interval) NOT NULL,
  "accepted_by_user_id" uuid,
  "accepted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."friend_permissions" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "relationship_id" uuid NOT NULL,
  "owner_user_id" uuid NOT NULL,
  "viewer_user_id" uuid NOT NULL,
  "share_scope" text DEFAULT 'future_flights'::text NOT NULL,
  "can_view_live" boolean DEFAULT true NOT NULL,
  "can_view_history" boolean DEFAULT false NOT NULL,
  "can_receive_alerts" boolean DEFAULT true NOT NULL,
  "notify_departure" boolean DEFAULT true NOT NULL,
  "notify_arrival" boolean DEFAULT true NOT NULL,
  "notify_delay" boolean DEFAULT true NOT NULL,
  "notify_gate_change" boolean DEFAULT true NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."friend_relationships" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_a" uuid NOT NULL,
  "user_b" uuid NOT NULL,
  "relationship_status" text DEFAULT 'active'::text NOT NULL,
  "created_by_user_id" uuid NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."live_activity_tokens" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "flight_instance_id" uuid NOT NULL,
  "tracking_session_id" uuid,
  "activity_id" text NOT NULL,
  "local_flight_id" text,
  "push_token" text NOT NULL,
  "environment" text NOT NULL,
  "is_active" boolean DEFAULT true NOT NULL,
  "last_sent_at" timestamp with time zone,
  "last_error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "last_content_phase" text
);

CREATE TABLE "public"."live_snapshots" (
  "tracking_session_id" uuid NOT NULL,
  "provider" text NOT NULL,
  "provider_flight_id" text,
  "flight_number" text,
  "airline_code" text,
  "departure_airport_iata" text,
  "arrival_airport_iata" text,
  "snapshot_status" text,
  "terminal" text,
  "gate" text,
  "baggage_claim" text,
  "delay_minutes" integer,
  "departure_times_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "arrival_times_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "alerts_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "metrics_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "canonical_snapshot_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "raw_provider_payload_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "provider_last_updated_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "canonical_revision" bigint DEFAULT 0 NOT NULL
);

CREATE TABLE "public"."notification_deliveries" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "flight_instance_id" uuid,
  "flight_event_id" uuid,
  "channel" text DEFAULT 'apns'::text NOT NULL,
  "status" text DEFAULT 'pending'::text NOT NULL,
  "sent_at" timestamp with time zone,
  "opened_at" timestamp with time zone,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now(),
  "user_flight_id" uuid,
  "dedupe_key" text,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."notification_delivery_tokens" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "notification_delivery_id" uuid NOT NULL,
  "device_token_id" uuid NOT NULL,
  "payload_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "status" text DEFAULT 'queued'::text NOT NULL,
  "attempt_count" integer DEFAULT 0 NOT NULL,
  "next_attempt_at" timestamp with time zone DEFAULT now() NOT NULL,
  "locked_until" timestamp with time zone,
  "last_attempt_at" timestamp with time zone,
  "accepted_at" timestamp with time zone,
  "apns_id" text,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."notifications" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "tracking_session_id" uuid,
  "friend_relationship_id" uuid,
  "notification_type" text NOT NULL,
  "delivery_channel" text DEFAULT 'push'::text NOT NULL,
  "delivery_status" text DEFAULT 'queued'::text NOT NULL,
  "title" text NOT NULL,
  "body" text NOT NULL,
  "payload_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "scheduled_for" timestamp with time zone,
  "sent_at" timestamp with time zone,
  "read_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "dedupe_key" text
);

CREATE TABLE "public"."profiles" (
  "user_id" uuid NOT NULL,
  "display_name" text DEFAULT 'Traveler'::text NOT NULL,
  "avatar_url" text,
  "email" text,
  "auth_provider" text,
  "onboarding_completed" boolean DEFAULT false NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."provider_request_leases" (
  "lock_key" text NOT NULL,
  "lease_token" uuid NOT NULL,
  "expires_at" timestamp with time zone NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."provider_response_cache" (
  "cache_key" text NOT NULL,
  "response" jsonb NOT NULL,
  "requested_at" timestamp with time zone NOT NULL,
  "expires_at" timestamp with time zone NOT NULL
);

CREATE TABLE "public"."push_devices" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "device_id" text,
  "apns_token" text NOT NULL,
  "platform" text DEFAULT 'ios'::text NOT NULL,
  "push_enabled" boolean DEFAULT true NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."ticket_souvenirs" (
  "user_id" uuid NOT NULL,
  "flight_id" uuid NOT NULL,
  "flight_payload" jsonb,
  "collected_at" timestamp with time zone,
  "locked_at" timestamp with time zone,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "deleted_at" timestamp with time zone
);

CREATE TABLE "public"."tracking_sessions" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "owner_user_id" uuid NOT NULL,
  "provider" text NOT NULL,
  "provider_flight_id" text,
  "flight_number" text NOT NULL,
  "airline_code" text,
  "origin_iata" text,
  "destination_iata" text,
  "travel_date" date,
  "session_status" text DEFAULT 'pending'::text NOT NULL,
  "created_source" text,
  "metadata_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "next_poll_after" timestamp with time zone,
  "last_snapshot_at" timestamp with time zone,
  "polling_stopped_reason" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."user_achievements" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "achievement_key" text NOT NULL,
  "achievement_type" text NOT NULL,
  "user_flight_id" uuid,
  "flight_instance_id" uuid,
  "title" text NOT NULL,
  "body" text NOT NULL,
  "symbol" text NOT NULL,
  "metadata_json" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "awarded_at" timestamp with time zone DEFAULT now() NOT NULL,
  "notified_at" timestamp with time zone,
  "celebrated_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE "public"."user_flights" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL,
  "source_type" text DEFAULT 'manual_search'::text NOT NULL,
  "lifecycle_state" text DEFAULT 'upcoming'::text NOT NULL,
  "tracking_session_id" uuid,
  "display_flight_number" text NOT NULL,
  "marketing_airline_code" text,
  "marketing_airline_name" text,
  "operating_airline_code" text,
  "operating_airline_name" text,
  "origin_iata" text NOT NULL,
  "destination_iata" text NOT NULL,
  "scheduled_departure" timestamp with time zone NOT NULL,
  "scheduled_arrival" timestamp with time zone,
  "estimated_departure" timestamp with time zone,
  "estimated_arrival" timestamp with time zone,
  "actual_departure" timestamp with time zone,
  "actual_arrival" timestamp with time zone,
  "departure_terminal" text,
  "departure_gate" text,
  "arrival_terminal" text,
  "arrival_gate" text,
  "baggage_claim" text,
  "aircraft_type" text,
  "status" text,
  "delay_minutes" integer,
  "distance_km" numeric,
  "flight_time_minutes" integer,
  "route_polyline" jsonb,
  "tracked_snapshot" jsonb,
  "calendar_source_text" text,
  "provider_name" text,
  "provider_flight_id" text,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "notifications_enabled" boolean DEFAULT true NOT NULL,
  "alert_settings_json" jsonb DEFAULT jsonb_build_object('gateChange', true, 'delayUpdates', true, 'boardingTime', true, 'takeoffLanding', false, 'baggageClaim', true, 'quietHours', jsonb_build_object('startHour', 22, 'startMinute', 0, 'endHour', 7, 'endMinute', 0)) NOT NULL,
  "flight_instance_id" uuid,
  "notification_enabled" boolean DEFAULT true,
  "alert_preferences" jsonb DEFAULT '{"low": false, "high": true, "medium": true, "critical": true}'::jsonb,
  "trip_id" uuid,
  "user_label" text,
  "visibility" text DEFAULT 'private'::text,
  "added_at" timestamp with time zone DEFAULT now(),
  "final_route_capture_status" text,
  "final_route_capture_attempted_at" timestamp with time zone,
  "final_route_capture_completed_at" timestamp with time zone,
  "final_route_capture_next_attempt_at" timestamp with time zone,
  "final_route_capture_error" text
);

CREATE TABLE "public"."user_settings" (
  "user_id" uuid NOT NULL,
  "preferred_theme" text DEFAULT 'system'::text NOT NULL,
  "distance_unit" text DEFAULT 'km'::text NOT NULL,
  "uses_24_hour_time" boolean DEFAULT false NOT NULL,
  "default_airport_code" text DEFAULT ''::text NOT NULL,
  "validate_with_provider_enabled" boolean DEFAULT true NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "temperature_unit" text DEFAULT 'celsius'::text NOT NULL
);

CREATE TABLE "runwy_security"."provider_daily_budgets" (
  "budget_day" date NOT NULL,
  "bucket" text NOT NULL,
  "reserved_units" integer NOT NULL
);

CREATE TABLE "runwy_security"."rate_limits" (
  "namespace" text NOT NULL,
  "key_hash" text NOT NULL,
  "hits" integer NOT NULL,
  "expires_at" timestamp with time zone NOT NULL
);

ALTER TABLE "public"."api_usage_logs" ADD CONSTRAINT "api_usage_logs_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."device_tokens" ADD CONSTRAINT "device_tokens_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."device_tokens" ADD CONSTRAINT "device_tokens_user_id_device_token_key" UNIQUE (user_id, device_token);

ALTER TABLE "public"."entitlements" ADD CONSTRAINT "entitlements_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."entitlements" ADD CONSTRAINT "entitlements_unique_user_entitlement" UNIQUE (user_id, entitlement_key);

ALTER TABLE "public"."flight_definitions" ADD CONSTRAINT "flight_definitions_airline_code_flight_number_key" UNIQUE (airline_code, flight_number);

ALTER TABLE "public"."flight_definitions" ADD CONSTRAINT "flight_definitions_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."flight_event_logs" ADD CONSTRAINT "flight_event_logs_dedupe_key_key" UNIQUE (dedupe_key);

ALTER TABLE "public"."flight_event_logs" ADD CONSTRAINT "flight_event_logs_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."flight_events" ADD CONSTRAINT "flight_events_confidence_check" CHECK (confidence = ANY (ARRAY['high'::text, 'medium'::text, 'low'::text, 'suspicious'::text]));

ALTER TABLE "public"."flight_events" ADD CONSTRAINT "flight_events_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."flight_events" ADD CONSTRAINT "flight_events_severity_check" CHECK (event_severity = ANY (ARRAY['low'::text, 'medium'::text, 'high'::text, 'critical'::text]));

ALTER TABLE "public"."flight_events" ADD CONSTRAINT "flight_events_type_check" CHECK (event_type = ANY (ARRAY['SCHEDULED'::text, 'DELAYED'::text, 'RESCHEDULED'::text, 'CANCELLED'::text, 'DEPARTED'::text, 'AIRBORNE'::text, 'LANDED'::text, 'ARRIVED'::text, 'TAXIING'::text, 'TAKEOFF_ROLL'::text, 'TAXI_IN'::text, 'ARRIVED_AT_GATE'::text, 'GATE_CHANGED'::text, 'TERMINAL_CHANGED'::text, 'BAGGAGE_BELT_ASSIGNED'::text, 'DIVERTED'::text, 'RETURNED_TO_GATE'::text, 'WEATHER_ADVISORY'::text, 'TRIP_STARTING'::text, 'AIRCRAFT_CHANGED'::text, 'UNKNOWN_CHANGE'::text, 'PROVIDER_DATA_SUSPICIOUS'::text]));

ALTER TABLE "public"."flight_instance_aliases" ADD CONSTRAINT "flight_instance_aliases_alias_key_key" UNIQUE (alias_key);

ALTER TABLE "public"."flight_instance_aliases" ADD CONSTRAINT "flight_instance_aliases_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_confidence_check" CHECK (data_confidence = ANY (ARRAY['unknown'::text, 'high'::text, 'medium'::text, 'low'::text, 'suspicious'::text]));

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_flight_key_key" UNIQUE (flight_key);

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_live_data_source_check" CHECK (live_data_source = ANY (ARRAY['on_demand'::text, 'provider_alert'::text, 'streaming'::text]));

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_provider_alert_status_check" CHECK (provider_alert_status = ANY (ARRAY['active'::text, 'unavailable'::text, 'failed'::text, 'expired'::text]));

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_refresh_priority_check" CHECK (refresh_priority = ANY (ARRAY['none'::text, 'minimal'::text, 'low'::text, 'normal'::text, 'high'::text, 'critical'::text]));

ALTER TABLE "public"."flight_instances" ADD CONSTRAINT "flight_instances_streaming_status_check" CHECK (streaming_status = ANY (ARRAY['disabled'::text, 'pending'::text, 'active'::text, 'failed'::text, 'expired'::text]));

ALTER TABLE "public"."flight_snapshots" ADD CONSTRAINT "flight_snapshots_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_default_share_scope_check" CHECK (default_share_scope = ANY (ARRAY['future_flights'::text, 'all_flights'::text, 'selected_flights'::text]));

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_status_check" CHECK (status = ANY (ARRAY['pending'::text, 'accepted'::text, 'revoked'::text, 'expired'::text]));

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_token_hash_key" UNIQUE (token_hash);

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_distinct_users" CHECK (owner_user_id <> viewer_user_id);

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_share_scope_check" CHECK (share_scope = ANY (ARRAY['future_flights'::text, 'all_flights'::text, 'selected_flights'::text]));

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_unique_direction" UNIQUE (owner_user_id, viewer_user_id);

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_distinct_users" CHECK (user_a <> user_b);

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_relationship_status_check" CHECK (relationship_status = ANY (ARRAY['active'::text, 'blocked'::text, 'removed'::text]));

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_unique_pair" UNIQUE (user_a, user_b);

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_environment_check" CHECK (environment = ANY (ARRAY['sandbox'::text, 'production'::text]));

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_user_id_activity_id_key" UNIQUE (user_id, activity_id);

ALTER TABLE "public"."live_snapshots" ADD CONSTRAINT "live_snapshots_pkey" PRIMARY KEY (tracking_session_id);

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_user_id_flight_event_id_channel_key" UNIQUE (user_id, flight_event_id, channel);

ALTER TABLE "public"."notification_delivery_tokens" ADD CONSTRAINT "notification_delivery_tokens_notification_delivery_id_devic_key" UNIQUE (notification_delivery_id, device_token_id);

ALTER TABLE "public"."notification_delivery_tokens" ADD CONSTRAINT "notification_delivery_tokens_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."notification_delivery_tokens" ADD CONSTRAINT "notification_delivery_tokens_status_check" CHECK (status = ANY (ARRAY['queued'::text, 'sending'::text, 'retry'::text, 'accepted'::text, 'permanent_failed'::text, 'uncertain'::text]));

ALTER TABLE "public"."notifications" ADD CONSTRAINT "notifications_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."profiles" ADD CONSTRAINT "profiles_pkey" PRIMARY KEY (user_id);

ALTER TABLE "public"."provider_request_leases" ADD CONSTRAINT "provider_request_leases_pkey" PRIMARY KEY (lock_key);

ALTER TABLE "public"."provider_response_cache" ADD CONSTRAINT "provider_response_cache_pkey" PRIMARY KEY (cache_key);

ALTER TABLE "public"."push_devices" ADD CONSTRAINT "push_devices_apns_token_key" UNIQUE (apns_token);

ALTER TABLE "public"."push_devices" ADD CONSTRAINT "push_devices_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."ticket_souvenirs" ADD CONSTRAINT "ticket_souvenirs_active_payload_check" CHECK (deleted_at IS NULL AND flight_payload IS NOT NULL AND collected_at IS NOT NULL OR deleted_at IS NOT NULL);

ALTER TABLE "public"."ticket_souvenirs" ADD CONSTRAINT "ticket_souvenirs_pkey" PRIMARY KEY (user_id, flight_id);

ALTER TABLE "public"."tracking_sessions" ADD CONSTRAINT "tracking_sessions_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."tracking_sessions" ADD CONSTRAINT "tracking_sessions_session_status_check" CHECK (session_status = ANY (ARRAY['pending'::text, 'active'::text, 'paused'::text, 'completed'::text, 'cancelled'::text, 'errored'::text]));

ALTER TABLE "public"."user_achievements" ADD CONSTRAINT "user_achievements_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."user_achievements" ADD CONSTRAINT "user_achievements_type_check" CHECK (achievement_type = ANY (ARRAY['new_country'::text, 'yearly_flights'::text, 'lifetime_distance'::text]));

ALTER TABLE "public"."user_achievements" ADD CONSTRAINT "user_achievements_user_key_unique" UNIQUE (user_id, achievement_key);

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_alert_settings_is_object" CHECK (jsonb_typeof(alert_settings_json) = 'object'::text);

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_final_route_capture_status_check" CHECK (final_route_capture_status IS NULL OR (final_route_capture_status = ANY (ARRAY['pending'::text, 'in_progress'::text, 'failed'::text, 'captured'::text, 'no_track'::text])));

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_lifecycle_state_check" CHECK (lifecycle_state = ANY (ARRAY['upcoming'::text, 'active'::text, 'landed'::text, 'archived'::text, 'deleted'::text]));

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_pkey" PRIMARY KEY (id);

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_source_type_check" CHECK (source_type = ANY (ARRAY['trip'::text, 'manual_search'::text, 'calendar_import'::text, 'tracked'::text, 'recovered'::text, 'manual_verified'::text, 'manual_recovery'::text, 'history_snapshot'::text, 'history_repair'::text, 'auto_archive'::text, 'travelled_archive'::text]));

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_user_tracking_unique" UNIQUE (user_id, tracking_session_id);

ALTER TABLE "public"."user_settings" ADD CONSTRAINT "user_settings_distance_unit_check" CHECK (distance_unit = ANY (ARRAY['km'::text, 'miles'::text]));

ALTER TABLE "public"."user_settings" ADD CONSTRAINT "user_settings_pkey" PRIMARY KEY (user_id);

ALTER TABLE "public"."user_settings" ADD CONSTRAINT "user_settings_preferred_theme_check" CHECK (preferred_theme = ANY (ARRAY['light'::text, 'dark'::text, 'system'::text]));

ALTER TABLE "public"."user_settings" ADD CONSTRAINT "user_settings_temperature_unit_check" CHECK (temperature_unit = ANY (ARRAY['celsius'::text, 'fahrenheit'::text]));

ALTER TABLE "runwy_security"."provider_daily_budgets" ADD CONSTRAINT "provider_daily_budgets_bucket_check" CHECK (bucket = ANY (ARRAY['general'::text, 'tracked'::text]));

ALTER TABLE "runwy_security"."provider_daily_budgets" ADD CONSTRAINT "provider_daily_budgets_pkey" PRIMARY KEY (budget_day, bucket);

ALTER TABLE "runwy_security"."provider_daily_budgets" ADD CONSTRAINT "provider_daily_budgets_reserved_units_check" CHECK (reserved_units >= 0);

ALTER TABLE "runwy_security"."rate_limits" ADD CONSTRAINT "rate_limits_pkey" PRIMARY KEY (namespace, key_hash);

ALTER TABLE "public"."device_tokens" ADD CONSTRAINT "device_tokens_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."entitlements" ADD CONSTRAINT "entitlements_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."flight_event_logs" ADD CONSTRAINT "flight_event_logs_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE SET NULL;

ALTER TABLE "public"."flight_events" ADD CONSTRAINT "flight_events_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."flight_instance_aliases" ADD CONSTRAINT "flight_instance_aliases_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."flight_snapshots" ADD CONSTRAINT "flight_snapshots_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_accepted_by_user_id_fkey" FOREIGN KEY (accepted_by_user_id) REFERENCES auth.users(id) ON DELETE SET NULL;

ALTER TABLE "public"."friend_invites" ADD CONSTRAINT "friend_invites_inviter_user_id_fkey" FOREIGN KEY (inviter_user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_owner_user_id_fkey" FOREIGN KEY (owner_user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_relationship_id_fkey" FOREIGN KEY (relationship_id) REFERENCES friend_relationships(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_permissions" ADD CONSTRAINT "friend_permissions_viewer_user_id_fkey" FOREIGN KEY (viewer_user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_created_by_user_id_fkey" FOREIGN KEY (created_by_user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_user_a_fkey" FOREIGN KEY (user_a) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."friend_relationships" ADD CONSTRAINT "friend_relationships_user_b_fkey" FOREIGN KEY (user_b) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_tracking_session_id_fkey" FOREIGN KEY (tracking_session_id) REFERENCES tracking_sessions(id) ON DELETE SET NULL;

ALTER TABLE "public"."live_activity_tokens" ADD CONSTRAINT "live_activity_tokens_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."live_snapshots" ADD CONSTRAINT "live_snapshots_tracking_session_id_fkey" FOREIGN KEY (tracking_session_id) REFERENCES tracking_sessions(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_flight_event_id_fkey" FOREIGN KEY (flight_event_id) REFERENCES flight_events(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_user_flight_id_fkey" FOREIGN KEY (user_flight_id) REFERENCES user_flights(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_deliveries" ADD CONSTRAINT "notification_deliveries_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_delivery_tokens" ADD CONSTRAINT "notification_delivery_tokens_device_token_id_fkey" FOREIGN KEY (device_token_id) REFERENCES device_tokens(id) ON DELETE CASCADE;

ALTER TABLE "public"."notification_delivery_tokens" ADD CONSTRAINT "notification_delivery_tokens_notification_delivery_id_fkey" FOREIGN KEY (notification_delivery_id) REFERENCES notification_deliveries(id) ON DELETE CASCADE;

ALTER TABLE "public"."notifications" ADD CONSTRAINT "notifications_friend_relationship_id_fkey" FOREIGN KEY (friend_relationship_id) REFERENCES friend_relationships(id) ON DELETE SET NULL;

ALTER TABLE "public"."notifications" ADD CONSTRAINT "notifications_tracking_session_id_fkey" FOREIGN KEY (tracking_session_id) REFERENCES tracking_sessions(id) ON DELETE SET NULL;

ALTER TABLE "public"."notifications" ADD CONSTRAINT "notifications_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."profiles" ADD CONSTRAINT "profiles_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."push_devices" ADD CONSTRAINT "push_devices_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."ticket_souvenirs" ADD CONSTRAINT "ticket_souvenirs_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."tracking_sessions" ADD CONSTRAINT "tracking_sessions_owner_user_id_fkey" FOREIGN KEY (owner_user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."user_achievements" ADD CONSTRAINT "user_achievements_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_flight_instance_id_fkey" FOREIGN KEY (flight_instance_id) REFERENCES flight_instances(id) ON DELETE CASCADE;

ALTER TABLE "public"."user_flights" ADD CONSTRAINT "user_flights_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

ALTER TABLE "public"."user_settings" ADD CONSTRAINT "user_settings_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

CREATE INDEX api_usage_logs_provider_created_idx ON public.api_usage_logs USING btree (provider, created_at DESC);

CREATE INDEX device_tokens_user_active_idx ON public.device_tokens USING btree (user_id, is_active);

CREATE INDEX entitlements_user_active_idx ON public.entitlements USING btree (user_id, is_active, entitlement_key);

CREATE INDEX flight_event_logs_fa_flight_id_idx ON public.flight_event_logs USING btree (fa_flight_id, created_at DESC) WHERE (fa_flight_id IS NOT NULL);

CREATE INDEX flight_event_logs_flight_instance_idx ON public.flight_event_logs USING btree (flight_instance_id, created_at DESC);

CREATE INDEX flight_event_logs_flight_key_idx ON public.flight_event_logs USING btree (flight_key, created_at DESC);

CREATE INDEX flight_events_instance_created_idx ON public.flight_events USING btree (flight_instance_id, created_at DESC);

CREATE INDEX flight_events_pending_fanout_idx ON public.flight_events USING btree (created_at) WHERE ((notification_required = true) AND (fanout_completed_at IS NULL));

CREATE INDEX flight_instances_lookup_idx ON public.flight_instances USING btree (airline_code, flight_number, departure_date, origin_airport, destination_airport);

CREATE INDEX flight_instances_provider_alert_idx ON public.flight_instances USING btree (provider, provider_alert_status, provider_alert_expires_at);

CREATE INDEX flight_instances_refresh_idx ON public.flight_instances USING btree (fresh_until, is_final, needs_revalidation, provider_alert_status, refresh_priority);

CREATE INDEX flight_instances_streaming_idx ON public.flight_instances USING btree (provider, live_data_source, streaming_status, last_stream_event_at);

CREATE INDEX idx_flight_instances_canonical_order ON public.flight_instances USING btree (id, state_revision, last_stream_event_at);

CREATE INDEX flight_snapshots_instance_created_idx ON public.flight_snapshots USING btree (flight_instance_id, created_at DESC);

CREATE INDEX friend_invites_inviter_idx ON public.friend_invites USING btree (inviter_user_id, status, expires_at DESC);

CREATE INDEX friend_permissions_viewer_idx ON public.friend_permissions USING btree (viewer_user_id, owner_user_id);

CREATE INDEX friend_relationships_user_a_idx ON public.friend_relationships USING btree (user_a, relationship_status);

CREATE INDEX friend_relationships_user_b_idx ON public.friend_relationships USING btree (user_b, relationship_status);

CREATE INDEX live_activity_tokens_flight_active_idx ON public.live_activity_tokens USING btree (flight_instance_id, is_active);

CREATE INDEX idx_live_snapshots_canonical_order ON public.live_snapshots USING btree (tracking_session_id, canonical_revision, provider_last_updated_at);

CREATE INDEX notification_deliveries_user_created_idx ON public.notification_deliveries USING btree (user_id, created_at DESC);

CREATE UNIQUE INDEX notification_deliveries_user_dedupe_key_channel_uidx ON public.notification_deliveries USING btree (user_id, dedupe_key, channel) WHERE (dedupe_key IS NOT NULL);

CREATE INDEX notification_deliveries_user_flight_idx ON public.notification_deliveries USING btree (user_flight_id) WHERE (user_flight_id IS NOT NULL);

CREATE INDEX notification_delivery_tokens_due_idx ON public.notification_delivery_tokens USING btree (next_attempt_at, created_at) WHERE (status = ANY (ARRAY['queued'::text, 'retry'::text]));

CREATE INDEX notification_delivery_tokens_stale_sending_idx ON public.notification_delivery_tokens USING btree (locked_until) WHERE (status = 'sending'::text);

CREATE INDEX notifications_tracking_idx ON public.notifications USING btree (tracking_session_id, created_at DESC);

CREATE INDEX notifications_user_created_idx ON public.notifications USING btree (user_id, created_at DESC);

CREATE UNIQUE INDEX notifications_user_dedupe_key_uidx ON public.notifications USING btree (user_id, dedupe_key) WHERE (dedupe_key IS NOT NULL);

CREATE INDEX profiles_email_idx ON public.profiles USING btree (email);

CREATE INDEX provider_request_leases_expires_at_idx ON public.provider_request_leases USING btree (expires_at);

CREATE INDEX provider_response_cache_expiry_idx ON public.provider_response_cache USING btree (expires_at);

CREATE INDEX push_devices_user_enabled_idx ON public.push_devices USING btree (user_id, push_enabled);

CREATE INDEX ticket_souvenirs_user_collected_idx ON public.ticket_souvenirs USING btree (user_id, collected_at DESC) WHERE (deleted_at IS NULL);

CREATE INDEX tracking_sessions_due_idx ON public.tracking_sessions USING btree (session_status, next_poll_after);

CREATE INDEX tracking_sessions_owner_status_idx ON public.tracking_sessions USING btree (owner_user_id, session_status, updated_at DESC);

CREATE INDEX tracking_sessions_provider_idx ON public.tracking_sessions USING btree (provider, provider_flight_id);

CREATE INDEX user_achievements_pending_idx ON public.user_achievements USING btree (user_id, awarded_at) WHERE (celebrated_at IS NULL);

CREATE INDEX user_flights_final_route_capture_due_idx ON public.user_flights USING btree (lifecycle_state, final_route_capture_status, final_route_capture_next_attempt_at, estimated_arrival) WHERE ((deleted_at IS NULL) AND (tracking_session_id IS NULL) AND (provider_flight_id IS NOT NULL));

CREATE UNIQUE INDEX user_flights_history_occurrence_unique ON public.user_flights USING btree (user_id, regexp_replace(upper(display_flight_number), '[^A-Z0-9]'::text, ''::text, 'g'::text), upper(TRIM(BOTH FROM origin_iata)), upper(TRIM(BOTH FROM destination_iata)), date_trunc('minute'::text, (scheduled_departure AT TIME ZONE 'UTC'::text))) WHERE ((lifecycle_state = 'archived'::text) AND (source_type <> ALL (ARRAY['trip'::text, 'tracked'::text])) AND (tracking_session_id IS NULL) AND (deleted_at IS NULL) AND (display_flight_number IS NOT NULL) AND (origin_iata IS NOT NULL) AND (destination_iata IS NOT NULL) AND (scheduled_departure IS NOT NULL));

CREATE INDEX user_flights_provider_idx ON public.user_flights USING btree (provider_name, provider_flight_id);

CREATE INDEX user_flights_user_deleted_idx ON public.user_flights USING btree (user_id, deleted_at);

CREATE UNIQUE INDEX user_flights_user_flight_instance_unique ON public.user_flights USING btree (user_id, flight_instance_id) WHERE (flight_instance_id IS NOT NULL);

CREATE INDEX user_flights_user_state_idx ON public.user_flights USING btree (user_id, lifecycle_state, scheduled_departure DESC);

CREATE INDEX rate_limits_expiry_idx ON runwy_security.rate_limits USING btree (expires_at);

CREATE OR REPLACE FUNCTION public.can_access_tracking_session(p_tracking_session_id uuid, p_user_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select exists (
    select 1
    from public.tracking_sessions ts
    where ts.id = p_tracking_session_id
      and (
        ts.owner_user_id = p_user_id
        or exists (
          select 1
          from public.flight_watchers fw
          where fw.tracking_session_id = ts.id
            and fw.watcher_user_id = p_user_id
            and fw.watch_state = 'approved'
        )
      )
  );
$function$
;

REVOKE ALL ON FUNCTION public.can_access_tracking_session(uuid,uuid) FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.can_access_tracking_session(uuid,uuid) TO anon;

GRANT EXECUTE ON FUNCTION public.can_access_tracking_session(uuid,uuid) TO authenticated;

GRANT EXECUTE ON FUNCTION public.can_access_tracking_session(uuid,uuid) TO service_role;

CREATE OR REPLACE FUNCTION public.cleanup_deleted_user_flight_notification_artifacts()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  deleted_flight public.user_flights%rowtype;
begin
  if tg_op = 'DELETE' then
    deleted_flight := old;
  else
    deleted_flight := new;
    if deleted_flight.deleted_at is null
       and coalesce(deleted_flight.lifecycle_state, '') <> 'deleted' then
      return new;
    end if;
  end if;

  delete from public.notifications n
   where n.payload_json ->> 'user_flight_id' = deleted_flight.id::text
      or (
        n.user_id = deleted_flight.user_id
        and (
          (deleted_flight.tracking_session_id is not null
            and (n.tracking_session_id = deleted_flight.tracking_session_id
              or n.payload_json ->> 'tracking_session_id' = deleted_flight.tracking_session_id::text))
          or (deleted_flight.flight_instance_id is not null
            and n.payload_json ->> 'flight_instance_id' = deleted_flight.flight_instance_id::text)
        )
      );

  delete from public.notification_deliveries d
   where d.user_flight_id = deleted_flight.id
      or (
        d.user_id = deleted_flight.user_id
        and deleted_flight.flight_instance_id is not null
        and d.flight_instance_id = deleted_flight.flight_instance_id
      );

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.cleanup_deleted_user_flight_notification_artifacts() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notification_artifacts() TO anon;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notification_artifacts() TO authenticated;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notification_artifacts() TO service_role;

CREATE OR REPLACE FUNCTION public.cleanup_deleted_user_flight_notifications()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  has_active_sibling boolean := false;
begin
  if new.deleted_at is null and coalesce(new.lifecycle_state, '') <> 'deleted' then
    return new;
  end if;

  if tg_op = 'UPDATE'
     and old.deleted_at is not null
     and coalesce(old.lifecycle_state, '') = 'deleted' then
    return new;
  end if;

  if new.display_flight_number is not null
     and new.origin_iata is not null
     and new.destination_iata is not null
     and new.scheduled_departure is not null then
    select exists (
      select 1
      from public.user_flights sibling
      where sibling.user_id = new.user_id
        and sibling.id <> new.id
        and sibling.deleted_at is null
        and coalesce(sibling.lifecycle_state, '') <> 'deleted'
        and regexp_replace(upper(coalesce(sibling.display_flight_number, '')), '[^A-Z0-9]', '', 'g')
          = regexp_replace(upper(new.display_flight_number), '[^A-Z0-9]', '', 'g')
        and upper(coalesce(sibling.origin_iata, '')) = upper(new.origin_iata)
        and upper(coalesce(sibling.destination_iata, '')) = upper(new.destination_iata)
        and abs(extract(epoch from (sibling.scheduled_departure - new.scheduled_departure))) <= 1800
    ) into has_active_sibling;
  end if;

  delete from public.notification_deliveries
  where user_flight_id = new.id
     or (
       not has_active_sibling
       and user_id = new.user_id
       and new.flight_instance_id is not null
       and flight_instance_id = new.flight_instance_id
     );

  delete from public.notifications
  where payload_json ->> 'user_flight_id' = new.id::text
     or payload_json ->> 'userFlightId' = new.id::text
     or (
       not has_active_sibling
       and user_id = new.user_id
       and (
         (new.tracking_session_id is not null and tracking_session_id = new.tracking_session_id)
         or (new.tracking_session_id is not null and payload_json ->> 'tracking_session_id' = new.tracking_session_id::text)
         or (new.flight_instance_id is not null and payload_json ->> 'flight_instance_id' = new.flight_instance_id::text)
       )
     );

  if new.tracking_session_id is not null
     and not exists (
       select 1 from public.user_flights uf
       where uf.tracking_session_id = new.tracking_session_id
         and uf.deleted_at is null
         and coalesce(uf.lifecycle_state, '') <> 'deleted'
     ) then
    update public.tracking_sessions
    set session_status = 'paused',
        next_poll_after = null,
        polling_stopped_reason = 'user_flight_deleted',
        updated_at = now()
    where id = new.tracking_session_id;
  end if;

  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.cleanup_deleted_user_flight_notifications() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notifications() TO anon;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notifications() TO authenticated;

GRANT EXECUTE ON FUNCTION public.cleanup_deleted_user_flight_notifications() TO service_role;

CREATE OR REPLACE FUNCTION public.delete_current_user_account()
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  requesting_user_id uuid := auth.uid();
  deleted_user boolean := false;
begin
  if requesting_user_id is null then
    raise exception 'Authentication is required to delete an account'
      using errcode = '42501';
  end if;

  -- This column intentionally has no foreign key because usage logs may outlive
  -- shared flight records. A deletion request must still remove its user link.
  delete from public.api_usage_logs
  where user_id = requesting_user_id;

  -- Accepted invitations use ON DELETE SET NULL. Remove them instead so no
  -- invitation record remains associated with the departing account.
  delete from public.friend_invites
  where accepted_by_user_id = requesting_user_id;

  delete from auth.users
  where id = requesting_user_id;

  deleted_user := found;
  if not deleted_user then
    raise exception 'Authenticated account no longer exists'
      using errcode = 'P0002';
  end if;

  return true;
end;
$function$
;

REVOKE ALL ON FUNCTION public.delete_current_user_account() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.delete_current_user_account() TO authenticated;

GRANT EXECUTE ON FUNCTION public.delete_current_user_account() TO service_role;

CREATE OR REPLACE FUNCTION public.handle_new_auth_user()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  insert into public.profiles (user_id, email, display_name, avatar_url)
  values (
    new.id,
    new.email,
    coalesce(
      nullif(trim(new.raw_user_meta_data ->> 'full_name'), ''),
      nullif(trim(new.raw_user_meta_data ->> 'name'), ''),
      nullif(split_part(coalesce(new.email, ''), '@', 1), ''),
      'Traveler'
    ),
    coalesce(
      nullif(trim(new.raw_user_meta_data ->> 'avatar_url'), ''),
      nullif(trim(new.raw_user_meta_data ->> 'picture'), '')
    )
  )
  on conflict (user_id) do nothing;

  insert into public.user_settings (user_id)
  values (new.id)
  on conflict (user_id) do nothing;

  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.handle_new_auth_user() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.handle_new_auth_user() TO anon;

GRANT EXECUTE ON FUNCTION public.handle_new_auth_user() TO authenticated;

GRANT EXECUTE ON FUNCTION public.handle_new_auth_user() TO service_role;

CREATE OR REPLACE FUNCTION public.increment_live_snapshot_version()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  new.version = coalesce(old.version, 0) + 1;
  new.updated_at = now();
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.increment_live_snapshot_version() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.increment_live_snapshot_version() TO anon;

GRANT EXECUTE ON FUNCTION public.increment_live_snapshot_version() TO authenticated;

GRANT EXECUTE ON FUNCTION public.increment_live_snapshot_version() TO service_role;

CREATE OR REPLACE FUNCTION public.is_tracking_session_owner(p_tracking_session_id uuid, p_user_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select exists (
    select 1
    from public.tracking_sessions ts
    where ts.id = p_tracking_session_id
      and ts.owner_user_id = p_user_id
  );
$function$
;

REVOKE ALL ON FUNCTION public.is_tracking_session_owner(uuid,uuid) FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.is_tracking_session_owner(uuid,uuid) TO anon;

GRANT EXECUTE ON FUNCTION public.is_tracking_session_owner(uuid,uuid) TO authenticated;

GRANT EXECUTE ON FUNCTION public.is_tracking_session_owner(uuid,uuid) TO service_role;

CREATE OR REPLACE FUNCTION public.prevent_stale_ticket_souvenir_write()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
begin
  if new.updated_at < old.updated_at then
    return old;
  end if;
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.prevent_stale_ticket_souvenir_write() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.prevent_stale_ticket_souvenir_write() TO anon;

GRANT EXECUTE ON FUNCTION public.prevent_stale_ticket_souvenir_write() TO authenticated;

GRANT EXECUTE ON FUNCTION public.prevent_stale_ticket_souvenir_write() TO service_role;

CREATE OR REPLACE FUNCTION public.reconcile_user_flight_history_occurrence()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if new.lifecycle_state = 'archived'
     and new.source_type not in ('trip', 'tracked')
     and new.tracking_session_id is null
     and new.deleted_at is null
     and new.display_flight_number is not null
     and new.origin_iata is not null
     and new.destination_iata is not null
     and new.scheduled_departure is not null then
    update public.user_flights as existing
    set lifecycle_state = 'deleted',
        deleted_at = now(),
        updated_at = now()
    where existing.id <> new.id
      and existing.user_id = new.user_id
      and existing.lifecycle_state = 'archived'
      and existing.source_type not in ('trip', 'tracked')
      and existing.tracking_session_id is null
      and existing.deleted_at is null
      and regexp_replace(upper(existing.display_flight_number), '[^A-Z0-9]', '', 'g')
        = regexp_replace(upper(new.display_flight_number), '[^A-Z0-9]', '', 'g')
      and upper(trim(existing.origin_iata)) = upper(trim(new.origin_iata))
      and upper(trim(existing.destination_iata)) = upper(trim(new.destination_iata))
      and date_trunc('minute', existing.scheduled_departure at time zone 'UTC')
        = date_trunc('minute', new.scheduled_departure at time zone 'UTC');
  end if;

  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.reconcile_user_flight_history_occurrence() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.reconcile_user_flight_history_occurrence() TO anon;

GRANT EXECUTE ON FUNCTION public.reconcile_user_flight_history_occurrence() TO authenticated;

GRANT EXECUTE ON FUNCTION public.reconcile_user_flight_history_occurrence() TO service_role;

CREATE OR REPLACE FUNCTION public.runwy_cleanup_rate_limits()
 RETURNS void
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
  delete from runwy_security.rate_limits where (namespace, key_hash) in (
    select namespace, key_hash from runwy_security.rate_limits
    where expires_at < now() - interval '5 minutes' order by expires_at limit 10000
  );
$function$
;

REVOKE ALL ON FUNCTION public.runwy_cleanup_rate_limits() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.runwy_cleanup_rate_limits() TO service_role;

CREATE OR REPLACE FUNCTION public.runwy_consume_rate_limit(p_namespace text, p_key_hash text, p_window_ms integer, p_limit integer)
 RETURNS TABLE(total_hits integer, reset_at timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  current_time_value timestamptz := clock_timestamp();
begin
  if length(p_namespace) > 80 or p_namespace is null or p_key_hash !~ '^[a-f0-9]{64}$'
     or p_key_hash is null or p_window_ms is null or p_window_ms not between 1000 and 3600000
     or p_limit is null or p_limit not between 1 and 1000000 then
    raise exception 'Invalid rate-limit parameters';
  end if;
  return query
    insert into runwy_security.rate_limits as counters(namespace, key_hash, hits, expires_at)
    values (p_namespace, p_key_hash, 1, current_time_value + p_window_ms * interval '1 millisecond')
    on conflict (namespace, key_hash) do update
    set hits = case when counters.expires_at <= current_time_value then 1
                    else least(counters.hits + 1, p_limit + 1) end,
        expires_at = case when counters.expires_at <= current_time_value
                     then current_time_value + p_window_ms * interval '1 millisecond'
                     else counters.expires_at end
    returning counters.hits, counters.expires_at;
end;
$function$
;

REVOKE ALL ON FUNCTION public.runwy_consume_rate_limit(text,text,integer,integer) FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.runwy_consume_rate_limit(text,text,integer,integer) TO service_role;

CREATE OR REPLACE FUNCTION public.runwy_reserve_flightaware_budget(p_bucket text, p_limit integer, p_units integer)
 RETURNS TABLE(allowed boolean, used_units integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  day_value date := (now() at time zone 'UTC')::date;
  current_units integer;
begin
  if p_bucket not in ('general', 'tracked') or p_bucket is null
     or p_limit is null or p_limit < 1 or p_units is null or p_units < 1 then
    raise exception 'Invalid provider budget parameters';
  end if;
  -- Seed the first reservation from existing usage so deployment does not reset today's budget.
  if not exists (select 1 from runwy_security.provider_daily_budgets where budget_day = day_value and bucket = p_bucket) then
  insert into runwy_security.provider_daily_budgets(budget_day, bucket, reserved_units)
  select day_value, p_bucket, coalesce(sum(coalesce(cost_estimate, 1)), 0)::integer
  from public.api_usage_logs
  where provider = 'flightaware' and endpoint like 'aeroapi:flight:%'
    and ((p_bucket = 'tracked' and endpoint = 'aeroapi:flight:tracked_flight')
      or (p_bucket = 'general' and endpoint <> 'aeroapi:flight:tracked_flight'))
    and created_at >= day_value::timestamp at time zone 'UTC'
  on conflict (budget_day, bucket) do nothing;
  end if;

  select reserved_units into current_units from runwy_security.provider_daily_budgets
  where budget_day = day_value and bucket = p_bucket for update;
  if p_units > p_limit - current_units then
    return query select false, current_units;
    return;
  end if;
  update runwy_security.provider_daily_budgets set reserved_units = reserved_units + p_units
  where budget_day = day_value and bucket = p_bucket;
  return query select true, current_units + p_units;
end;
$function$
;

REVOKE ALL ON FUNCTION public.runwy_reserve_flightaware_budget(text,integer,integer) FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.runwy_reserve_flightaware_budget(text,integer,integer) TO service_role;

CREATE OR REPLACE FUNCTION public.runwy_touch_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  new.updated_at = now();
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.runwy_touch_updated_at() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.runwy_touch_updated_at() TO anon;

GRANT EXECUTE ON FUNCTION public.runwy_touch_updated_at() TO authenticated;

GRANT EXECUTE ON FUNCTION public.runwy_touch_updated_at() TO service_role;

CREATE OR REPLACE FUNCTION public.set_updated_at_timestamp()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  new.updated_at = now();
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.set_updated_at_timestamp() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.set_updated_at_timestamp() TO anon;

GRANT EXECUTE ON FUNCTION public.set_updated_at_timestamp() TO authenticated;

GRANT EXECUTE ON FUNCTION public.set_updated_at_timestamp() TO service_role;

CREATE OR REPLACE FUNCTION public.sync_user_backup_after_past_flight_import(p_user_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_flights_json jsonb;
  v_total_count int;
  v_stats_json jsonb;
begin
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pf.id,
        'fa_flight_id', pf.fa_flight_id,
        'ident', pf.ident,
        'display_flight_number', pf.display_flight_number,
        'operator_name', pf.operator_name,
        'origin_iata', pf.origin_iata,
        'origin_icao', pf.origin_icao,
        'destination_iata', pf.destination_iata,
        'destination_icao', pf.destination_icao,
        'scheduled_out', pf.scheduled_out,
        'actual_out', pf.actual_out,
        'scheduled_in', pf.scheduled_in,
        'actual_in', pf.actual_in,
        'status', pf.status,
        'aircraft_type', pf.aircraft_type,
        'import_source', pf.import_source,
        'imported_at', pf.imported_at
      )
      order by coalesce(pf.actual_out, pf.scheduled_out, pf.imported_at) desc
    ),
    '[]'::jsonb
  )
  into v_flights_json
  from public.past_flights pf
  where pf.user_id = p_user_id;

  select count(*)
  into v_total_count
  from public.past_flights pf
  where pf.user_id = p_user_id;

  with yearly as (
    select
      extract(year from coalesce(actual_out, scheduled_out, imported_at))::int as yr,
      count(*)::int as cnt
    from public.past_flights
    where user_id = p_user_id
    group by 1
  ),
  top_airlines as (
    select
      coalesce(nullif(operator_name, ''), 'Unknown') as airline,
      count(*)::int as cnt
    from public.past_flights
    where user_id = p_user_id
    group by 1
    order by 2 desc, 1 asc
    limit 10
  ),
  top_airports as (
    select
      airport,
      count(*)::int as cnt
    from (
      select origin_iata as airport
      from public.past_flights
      where user_id = p_user_id and origin_iata is not null and origin_iata <> ''
      union all
      select destination_iata as airport
      from public.past_flights
      where user_id = p_user_id and destination_iata is not null and destination_iata <> ''
    ) airports
    group by airport
    order by 2 desc, 1 asc
    limit 12
  )
  select jsonb_build_object(
    'total_flights', v_total_count,
    'yearly_counts', coalesce((select jsonb_object_agg(yr::text, cnt) from yearly), '{}'::jsonb),
    'top_airlines', coalesce((select jsonb_agg(jsonb_build_object('name', airline, 'count', cnt)) from top_airlines), '[]'::jsonb),
    'top_airports', coalesce((select jsonb_agg(jsonb_build_object('code', airport, 'count', cnt)) from top_airports), '[]'::jsonb),
    'synced_at', now()
  )
  into v_stats_json;

  insert into public.user_backup_snapshots (user_id, backup_version, past_flights_json, stats_json)
  values (p_user_id, 1, v_flights_json, v_stats_json)
  on conflict (user_id)
  do update set
    backup_version = public.user_backup_snapshots.backup_version + 1,
    past_flights_json = excluded.past_flights_json,
    stats_json = excluded.stats_json,
    updated_at = now();
end;
$function$
;

REVOKE ALL ON FUNCTION public.sync_user_backup_after_past_flight_import(uuid) FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.sync_user_backup_after_past_flight_import(uuid) TO anon;

GRANT EXECUTE ON FUNCTION public.sync_user_backup_after_past_flight_import(uuid) TO authenticated;

GRANT EXECUTE ON FUNCTION public.sync_user_backup_after_past_flight_import(uuid) TO service_role;

CREATE OR REPLACE FUNCTION public.touch_tracking_session_from_snapshot()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  update public.tracking_sessions
  set
    last_snapshot_at = coalesce(new.provider_last_updated_at, new.updated_at, now()),
    last_polled_at = now(),
    next_poll_after = case
      when new.snapshot_status in ('landed', 'cancelled', 'diverted') then null
      else next_poll_after
    end,
    session_status = case
      when new.snapshot_status = 'cancelled' then 'cancelled'
      when new.snapshot_status in ('landed', 'diverted') then 'completed'
      when session_status in ('pending', 'paused') then 'active'
      else session_status
    end,
    completed_at = case
      when new.snapshot_status in ('landed', 'diverted') then coalesce(completed_at, now())
      else completed_at
    end,
    cancelled_at = case
      when new.snapshot_status = 'cancelled' then coalesce(cancelled_at, now())
      else cancelled_at
    end,
    polling_stopped_reason = case
      when new.snapshot_status in ('landed', 'cancelled', 'diverted')
        then coalesce(polling_stopped_reason, new.snapshot_status)
      else polling_stopped_reason
    end,
    updated_at = now()
  where id = new.tracking_session_id;

  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.touch_tracking_session_from_snapshot() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.touch_tracking_session_from_snapshot() TO anon;

GRANT EXECUTE ON FUNCTION public.touch_tracking_session_from_snapshot() TO authenticated;

GRANT EXECUTE ON FUNCTION public.touch_tracking_session_from_snapshot() TO service_role;

CREATE OR REPLACE FUNCTION public.touch_user_achievement_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
begin
  new.updated_at = now();
  return new;
end;
$function$
;

REVOKE ALL ON FUNCTION public.touch_user_achievement_updated_at() FROM public,anon,authenticated;

GRANT EXECUTE ON FUNCTION public.touch_user_achievement_updated_at() TO anon;

GRANT EXECUTE ON FUNCTION public.touch_user_achievement_updated_at() TO authenticated;

GRANT EXECUTE ON FUNCTION public.touch_user_achievement_updated_at() TO service_role;

ALTER TABLE "public"."api_usage_logs" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."api_usage_logs" FROM public,anon,authenticated;

ALTER TABLE "public"."device_tokens" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."device_tokens" FROM public,anon,authenticated;

ALTER TABLE "public"."entitlements" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."entitlements" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_definitions" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_definitions" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_event_logs" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_event_logs" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_events" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_events" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_instance_aliases" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_instance_aliases" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_instances" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_instances" FROM public,anon,authenticated;

ALTER TABLE "public"."flight_snapshots" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."flight_snapshots" FROM public,anon,authenticated;

ALTER TABLE "public"."friend_invites" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."friend_invites" FROM public,anon,authenticated;

ALTER TABLE "public"."friend_permissions" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."friend_permissions" FROM public,anon,authenticated;

ALTER TABLE "public"."friend_relationships" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."friend_relationships" FROM public,anon,authenticated;

ALTER TABLE "public"."live_activity_tokens" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."live_activity_tokens" FROM public,anon,authenticated;

ALTER TABLE "public"."live_snapshots" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."live_snapshots" FROM public,anon,authenticated;

ALTER TABLE "public"."notification_deliveries" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."notification_deliveries" FROM public,anon,authenticated;

ALTER TABLE "public"."notification_delivery_tokens" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."notification_delivery_tokens" FROM public,anon,authenticated;

ALTER TABLE "public"."notifications" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."notifications" FROM public,anon,authenticated;

ALTER TABLE "public"."profiles" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."profiles" FROM public,anon,authenticated;

ALTER TABLE "public"."provider_request_leases" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."provider_request_leases" FROM public,anon,authenticated;

ALTER TABLE "public"."provider_response_cache" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."provider_response_cache" FROM public,anon,authenticated;

ALTER TABLE "public"."push_devices" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."push_devices" FROM public,anon,authenticated;

ALTER TABLE "public"."ticket_souvenirs" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."ticket_souvenirs" FROM public,anon,authenticated;

ALTER TABLE "public"."tracking_sessions" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."tracking_sessions" FROM public,anon,authenticated;

ALTER TABLE "public"."user_achievements" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."user_achievements" FROM public,anon,authenticated;

ALTER TABLE "public"."user_flights" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."user_flights" FROM public,anon,authenticated;

ALTER TABLE "public"."user_settings" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "public"."user_settings" FROM public,anon,authenticated;

ALTER TABLE "runwy_security"."provider_daily_budgets" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "runwy_security"."provider_daily_budgets" FROM public,anon,authenticated;

ALTER TABLE "runwy_security"."rate_limits" ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON "runwy_security"."rate_limits" FROM public,anon,authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."api_usage_logs" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."device_tokens" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."device_tokens" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."device_tokens" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."entitlements" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."entitlements" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."entitlements" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_definitions" TO service_role;

GRANT SELECT,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_event_logs" TO anon;

GRANT SELECT,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_event_logs" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_event_logs" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_events" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_instance_aliases" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_instances" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."flight_snapshots" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_invites" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_invites" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_invites" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_permissions" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_permissions" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_permissions" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_relationships" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_relationships" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."friend_relationships" TO service_role;

GRANT SELECT,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_activity_tokens" TO anon;

GRANT SELECT,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_activity_tokens" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_activity_tokens" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_snapshots" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_snapshots" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."live_snapshots" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notification_deliveries" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notification_deliveries" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notification_deliveries" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notification_delivery_tokens" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notifications" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notifications" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."notifications" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."profiles" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."profiles" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."profiles" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."provider_request_leases" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."provider_response_cache" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."push_devices" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."push_devices" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."push_devices" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."ticket_souvenirs" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."ticket_souvenirs" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."ticket_souvenirs" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."tracking_sessions" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."tracking_sessions" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."tracking_sessions" TO service_role;

GRANT SELECT ON "public"."user_achievements" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_achievements" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_flights" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_flights" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_flights" TO service_role;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_settings" TO anon;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_settings" TO authenticated;

GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON "public"."user_settings" TO service_role;

GRANT UPDATE (celebrated_at) ON public.user_achievements TO authenticated;

CREATE POLICY "device_tokens_own_delete" ON public.device_tokens AS PERMISSIVE FOR DELETE TO public USING ((auth.uid() = user_id));

CREATE POLICY "device_tokens_own_insert" ON public.device_tokens AS PERMISSIVE FOR INSERT TO public WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "device_tokens_own_select" ON public.device_tokens AS PERMISSIVE FOR SELECT TO public USING ((auth.uid() = user_id));

CREATE POLICY "device_tokens_own_update" ON public.device_tokens AS PERMISSIVE FOR UPDATE TO public USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "entitlements_select_self" ON public.entitlements AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "flight_event_logs_no_client_select" ON public.flight_event_logs AS PERMISSIVE FOR SELECT TO public USING (false);

CREATE POLICY "friend_invites_select_own" ON public.friend_invites AS PERMISSIVE FOR SELECT TO authenticated USING (((auth.uid() = inviter_user_id) OR (auth.uid() = accepted_by_user_id)));

CREATE POLICY "friend_permissions_select_members" ON public.friend_permissions AS PERMISSIVE FOR SELECT TO authenticated USING (((auth.uid() = owner_user_id) OR (auth.uid() = viewer_user_id)));

CREATE POLICY "friend_permissions_update_owner" ON public.friend_permissions AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = owner_user_id)) WITH CHECK ((auth.uid() = owner_user_id));

CREATE POLICY "friend_relationships_select_members" ON public.friend_relationships AS PERMISSIVE FOR SELECT TO authenticated USING (((auth.uid() = user_a) OR (auth.uid() = user_b)));

CREATE POLICY "live_activity_tokens_own_select" ON public.live_activity_tokens AS PERMISSIVE FOR SELECT TO public USING ((auth.uid() = user_id));

CREATE POLICY "live_snapshots_select_visible" ON public.live_snapshots AS PERMISSIVE FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM tracking_sessions ts
  WHERE ((ts.id = live_snapshots.tracking_session_id) AND ((ts.owner_user_id = auth.uid()) OR (EXISTS ( SELECT 1
           FROM (friend_permissions fp
             JOIN friend_relationships fr ON ((fr.id = fp.relationship_id)))
          WHERE ((fp.owner_user_id = ts.owner_user_id) AND (fp.viewer_user_id = auth.uid()) AND (fr.relationship_status = 'active'::text) AND (fp.can_view_live = true)))))))));

CREATE POLICY "notification_deliveries_own_select" ON public.notification_deliveries AS PERMISSIVE FOR SELECT TO public USING ((auth.uid() = user_id));

CREATE POLICY "notifications_select_self" ON public.notifications AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "notifications_update_self" ON public.notifications AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "profiles_insert_self" ON public.profiles AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "profiles_select_self" ON public.profiles AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "profiles_update_self" ON public.profiles AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "push_devices_delete_self" ON public.push_devices AS PERMISSIVE FOR DELETE TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "push_devices_insert_self" ON public.push_devices AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "push_devices_select_self" ON public.push_devices AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "push_devices_update_self" ON public.push_devices AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "ticket_souvenirs_insert_owner" ON public.ticket_souvenirs AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((user_id = auth.uid()));

CREATE POLICY "ticket_souvenirs_select_owner" ON public.ticket_souvenirs AS PERMISSIVE FOR SELECT TO authenticated USING ((user_id = auth.uid()));

CREATE POLICY "ticket_souvenirs_update_owner" ON public.ticket_souvenirs AS PERMISSIVE FOR UPDATE TO authenticated USING ((user_id = auth.uid())) WITH CHECK ((user_id = auth.uid()));

CREATE POLICY "tracking_sessions_insert_owner" ON public.tracking_sessions AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((auth.uid() = owner_user_id));

CREATE POLICY "tracking_sessions_select_visible" ON public.tracking_sessions AS PERMISSIVE FOR SELECT TO authenticated USING (((auth.uid() = owner_user_id) OR (EXISTS ( SELECT 1
   FROM (friend_permissions fp
     JOIN friend_relationships fr ON ((fr.id = fp.relationship_id)))
  WHERE ((fp.owner_user_id = tracking_sessions.owner_user_id) AND (fp.viewer_user_id = auth.uid()) AND (fr.relationship_status = 'active'::text) AND (fp.can_view_live = true))))));

CREATE POLICY "tracking_sessions_update_owner" ON public.tracking_sessions AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = owner_user_id)) WITH CHECK ((auth.uid() = owner_user_id));

CREATE POLICY "user_achievements_select_owner" ON public.user_achievements AS PERMISSIVE FOR SELECT TO authenticated USING ((user_id = auth.uid()));

CREATE POLICY "user_achievements_update_owner" ON public.user_achievements AS PERMISSIVE FOR UPDATE TO authenticated USING ((user_id = auth.uid())) WITH CHECK ((user_id = auth.uid()));

CREATE POLICY "user_flights_delete_owner" ON public.user_flights AS PERMISSIVE FOR DELETE TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "user_flights_insert_owner" ON public.user_flights AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "user_flights_own_delete" ON public.user_flights AS PERMISSIVE FOR DELETE TO public USING ((auth.uid() = user_id));

CREATE POLICY "user_flights_own_insert" ON public.user_flights AS PERMISSIVE FOR INSERT TO public WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "user_flights_own_select" ON public.user_flights AS PERMISSIVE FOR SELECT TO public USING ((auth.uid() = user_id));

CREATE POLICY "user_flights_own_update" ON public.user_flights AS PERMISSIVE FOR UPDATE TO public USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "user_flights_select_owner" ON public.user_flights AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "user_flights_update_owner" ON public.user_flights AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "user_settings_insert_self" ON public.user_settings AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "user_settings_select_self" ON public.user_settings AS PERMISSIVE FOR SELECT TO authenticated USING ((auth.uid() = user_id));

CREATE POLICY "user_settings_update_self" ON public.user_settings AS PERMISSIVE FOR UPDATE TO authenticated USING ((auth.uid() = user_id)) WITH CHECK ((auth.uid() = user_id));

CREATE POLICY "flight_liveries_public_read" ON storage.objects AS PERMISSIVE FOR SELECT TO public USING ((bucket_id = 'flight-liveries'::text));

CREATE POLICY "profile_avatars_circle_read" ON storage.objects AS PERMISSIVE FOR SELECT TO authenticated USING (((bucket_id = 'profile-avatars'::text) AND (((storage.foldername(name))[1] = (auth.uid())::text) OR (EXISTS ( SELECT 1
   FROM friend_relationships relationship
  WHERE ((relationship.relationship_status = 'active'::text) AND (((relationship.user_a = auth.uid()) AND ((relationship.user_b)::text = (storage.foldername(objects.name))[1])) OR ((relationship.user_b = auth.uid()) AND ((relationship.user_a)::text = (storage.foldername(objects.name))[1])))))))));

CREATE POLICY "profile_avatars_owner_delete" ON storage.objects AS PERMISSIVE FOR DELETE TO authenticated USING (((bucket_id = 'profile-avatars'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "profile_avatars_owner_insert" ON storage.objects AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK (((bucket_id = 'profile-avatars'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "profile_avatars_owner_update" ON storage.objects AS PERMISSIVE FOR UPDATE TO authenticated USING (((bucket_id = 'profile-avatars'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text))) WITH CHECK (((bucket_id = 'profile-avatars'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "ticket_stickers_owner_delete" ON storage.objects AS PERMISSIVE FOR DELETE TO authenticated USING (((bucket_id = 'ticket-stickers'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "ticket_stickers_owner_insert" ON storage.objects AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK (((bucket_id = 'ticket-stickers'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "ticket_stickers_owner_select" ON storage.objects AS PERMISSIVE FOR SELECT TO authenticated USING (((bucket_id = 'ticket-stickers'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE POLICY "ticket_stickers_owner_update" ON storage.objects AS PERMISSIVE FOR UPDATE TO authenticated USING (((bucket_id = 'ticket-stickers'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text))) WITH CHECK (((bucket_id = 'ticket-stickers'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));

CREATE TRIGGER on_auth_user_created_runwy AFTER INSERT ON auth.users FOR EACH ROW EXECUTE FUNCTION handle_new_auth_user();

CREATE TRIGGER device_tokens_touch_updated_at BEFORE UPDATE ON device_tokens FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER entitlements_touch_updated_at BEFORE UPDATE ON entitlements FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER flight_definitions_touch_updated_at BEFORE UPDATE ON flight_definitions FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER flight_instances_touch_updated_at BEFORE UPDATE ON flight_instances FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER friend_invites_touch_updated_at BEFORE UPDATE ON friend_invites FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER friend_permissions_touch_updated_at BEFORE UPDATE ON friend_permissions FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER friend_relationships_touch_updated_at BEFORE UPDATE ON friend_relationships FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER live_snapshots_touch_updated_at BEFORE UPDATE ON live_snapshots FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER notifications_touch_updated_at BEFORE UPDATE ON notifications FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER profiles_touch_updated_at BEFORE UPDATE ON profiles FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER push_devices_touch_updated_at BEFORE UPDATE ON push_devices FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER ticket_souvenirs_reject_stale_update BEFORE UPDATE ON ticket_souvenirs FOR EACH ROW EXECUTE FUNCTION prevent_stale_ticket_souvenir_write();

CREATE TRIGGER tracking_sessions_touch_updated_at BEFORE UPDATE ON tracking_sessions FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER user_achievements_touch_updated_at BEFORE UPDATE ON user_achievements FOR EACH ROW EXECUTE FUNCTION touch_user_achievement_updated_at();

CREATE TRIGGER cleanup_deleted_user_flight_notifications AFTER INSERT OR UPDATE ON user_flights FOR EACH ROW EXECUTE FUNCTION cleanup_deleted_user_flight_notifications();

CREATE TRIGGER reconcile_user_flight_history_occurrence_trigger BEFORE INSERT OR UPDATE OF lifecycle_state, source_type, tracking_session_id, deleted_at, display_flight_number, origin_iata, destination_iata, scheduled_departure ON user_flights FOR EACH ROW EXECUTE FUNCTION reconcile_user_flight_history_occurrence();

CREATE TRIGGER user_flights_touch_updated_at BEFORE UPDATE ON user_flights FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

CREATE TRIGGER user_settings_touch_updated_at BEFORE UPDATE ON user_settings FOR EACH ROW EXECUTE FUNCTION runwy_touch_updated_at();

INSERT INTO storage.buckets(id,name,public,file_size_limit,allowed_mime_types) VALUES ('flight-liveries','flight-liveries',true,2097152,ARRAY['image/webp','image/png','image/jpeg']) ON CONFLICT (id) DO NOTHING;

INSERT INTO storage.buckets(id,name,public,file_size_limit,allowed_mime_types) VALUES ('profile-avatars','profile-avatars',false,2097152,ARRAY['image/jpeg','image/png']) ON CONFLICT (id) DO NOTHING;

INSERT INTO storage.buckets(id,name,public,file_size_limit,allowed_mime_types) VALUES ('ticket-stickers','ticket-stickers',false,1000000,ARRAY['image/png']) ON CONFLICT (id) DO NOTHING;

SET check_function_bodies = true;
