# Paid sticker cloud storage

Flight cloud synchronization remains available to signed-in free users. Sticker creation, editing and images remain available locally on-device. First Class membership is required to back up or restore sticker PNGs and decorated-ticket souvenir metadata.

The app skips decorated-ticket synchronization for free users. Before a paid sync, it calls the authenticated `/v1/stickers/cloud-access` endpoint. The backend verifies the account through RevenueCat and issues a private database access record valid for at most five minutes, never later than the verified membership/cache expiry. Requests cannot supply trusted membership flags or extend a cached verification. Database and Storage restrictive RLS policies require this server-issued access in addition to the existing owner policies. Clients cannot create their own access records. Admin free-test mode revokes access immediately.

Expiration preserves local stickers and existing cloud backups; it does not delete user data. Reads and writes to sticker cloud storage stop when the short verification window expires. Upgrading or renewing permits reconciliation on the next sync. Flight records, flight history and profile avatars retain their existing free ownership-based synchronization. Account deletion still uses the privileged cleanup path and does not require membership.

Validation covers free flight writes, denial of free sticker uploads and souvenir writes, paid owner access, cross-account isolation even between paid users, expiry, retained backups, free avatars, forged grant attempts and untrusted HTTP membership fields. Existing membership tests cover purchase expiry, grace, lifetime and admin test mode. Swift parsing is checked for the edited client files; no full simulator build is required for this change.

Deploy the migration and backend together. The updated iOS sync flow is required for paid sticker backups to request verification; an old app build cannot create that access record itself. The iOS edits remain in the app workspace until an app build/release. No existing sticker assets or souvenirs are deleted by the migration.
