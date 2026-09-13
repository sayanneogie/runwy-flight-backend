# Paid tracking enforcement

The backend verifies RevenueCat `first_class` membership using `REVENUECAT_API_KEY` and API v1 customer info. The app's existing public app-specific key supports this read; it is configured as a Railway variable, never committed. Server membership decisions ignore client flags. UUID identities match Swift's uppercase `UUID.uuidString` used at RevenueCat login.

- Flight status/search/add remain available without membership.
- Position and track fetch functions check for an active paid tracker, including inbound references, before reading their caches or reaching FlightAware. An occurrence shared with a paid tracker keeps its paid data source.
- HTTP flight responses for free users omit live position/track data; the explicit track route returns an empty paid-required response without provider access. This does not migrate or erase previously downloaded device data or change existing direct Supabase access policies.
- Outbound and inbound provider alert creation/update require paid tracking. Startup and five-minute recovery delete stored free-only alert subscriptions, including legacy registrations. Unverifiable membership does not authorize deletion.
- APNs recipients, Live Activity recipients and queued notification recipients are checked. Circle fanout requires both paid owner and recipient. A paid user independently tracking the occurrence can still use their own subscription.
- Membership is cached for up to five minutes, capped by known expiry; failed verification is retried after 30 seconds. Failures withhold new premium work but do not classify the customer as expired. Purchase recognition and provider subscription reconciliation are bounded by these cache/recovery intervals.
- Subscription cancellation with a future expiry remains entitled; expired access is denied, lifetime access and explicit grace periods are supported.

No five-flight quota or billing plan was changed. No test notifications are sent by verification. Tests cover provider-enabled forced free telemetry, mixed flights, expiry, lifetime, grace, identity casing, nested response filtering and queued Circle delivery after owner expiry.
