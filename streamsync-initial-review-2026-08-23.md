# StreamSync 2.0.1 — Initial Security and Reliability Review

**Review date:** 2026-08-23  
**Repository:** `KaoticGames/StreamSync`  
**Branch:** `main`  
**Commit:** `654e1b9f136c0998cff851727537e9499dc1760a`  
**Review mode:** Read-only source review with targeted Node reproductions  
**Working tree after review:** Clean

## Executive summary

StreamSync has a coherent local-first architecture: a small Rust/Tauri desktop shell embeds an Axum HTTP/WebSocket server, connects directly to Twitch/Kick after authorization, and serves deterministic localhost browser sources to OBS. The system-tray lifecycle, external user-data storage, atomic JSON writes, and separation between personal and delegated identities are solid foundations.

The review also found several material issues that should be addressed before scaling paid Early Access. The most urgent is that the localhost control plane is unauthenticated and deliberately permits arbitrary browser origins. A malicious webpage opened while StreamSync is running can potentially reach privileged routes and WebSockets on `127.0.0.1:4040`, modify account/configuration state, send chat or moderation commands, and abuse upload endpoints. Loopback binding prevents ordinary LAN access but does not protect localhost from the user's own browser.

The alert-tier defect was reproduced deterministically: string-valued thresholds fail open in the variation matcher, allowing an ineligible higher tier into the weighted selection pool. Several additional correctness and production-reliability issues were identified, including takeover cleanup races, missing alert queueing, a Kick event-name mismatch, incomplete StreamElements inheritance, and sequential WebSocket fan-out under a shared lock.

## Priority order

1. Authenticate and origin-protect the localhost control plane.
2. Make delegated revocation cleanup reliable and fail safely.
3. Fix and regression-test alert threshold normalization.
4. Add serialized alert queueing.
5. Correct Kick `bits`/`cheer` normalization and gift-alert duplication.
6. Correct StreamElements variation inheritance and placeholder parity.
7. Remove residual plaintext credentials and redesign backup secret handling.
8. Fix WebSocket fan-out, Twitch reconnect semantics, and single-instance startup.
9. Replace the bundled updater HMAC secret with public-key verification.
10. Add CI and release gates, then reconcile documentation.

---

## Critical finding

### 1. Unauthenticated localhost HTTP/WebSocket control plane accepts arbitrary origins

**Severity:** Critical  
**Status:** Confirmed by source inspection

**Evidence**

- Privileged and public routes share one unauthenticated router: `crates/stream-sync-core/src/routes.rs:43-104`.
- The router applies `CorsLayer::permissive()`: `routes.rs:105-113`.
- The WebSocket endpoint performs no authentication or `Origin` validation: `routes.rs:1736-1743`.
- Any connected WebSocket may submit `chat-send` messages to Twitch or Kick: `routes.rs:1762-1797`.
- Twitch chat handling recognizes moderation-capable slash commands: `crates/stream-sync-core/src/twitch.rs:1196-1209,1424-1453`.
- The server binds to `127.0.0.1`, which limits network exposure but remains reachable from local browser contexts: `crates/stream-sync-core/src/lib.rs:105-106`.
- A global 64 MiB body limit applies broadly, including privileged upload/config routes: `routes.rs:110-113`.

**Potential impact**

A malicious site visited while StreamSync is running could attempt to:

- Switch, remove, or disconnect Twitch/Kick connections.
- Replace or delete StreamElements session state.
- Modify or delete overlay configurations.
- Trigger test alerts.
- Send Twitch/Kick chat and moderation commands.
- Consume local memory/disk through repeated uploads.
- Subscribe to local WebSocket feed data.

**Recommended correction**

- Generate a cryptographically random per-installation control token.
- Require it on every privileged HTTP route and WebSocket upgrade.
- Validate exact trusted `Origin` values; do not use permissive CORS.
- Separate public OBS rendering/feed routes from privileged control routes.
- Keep browser-source routes read-only.
- Apply small endpoint-specific body limits; retain larger limits only for authenticated media upload.
- Reject WebSocket upgrades without an approved origin and valid token.
- Add browser-based regression tests proving an unrelated origin cannot mutate state or send chat.

---

## High-severity findings

### 2. Active takeover revocation cleanup can race against itself and the watcher fails open

**Severity:** High  
**Status:** Confirmed design risk; requires runtime regression test

**Evidence**

- Watch transport errors only log and retry after five seconds while existing platform clients continue: `crates/stream-sync-core/src/twitch.rs:1028-1037`.
- Token refresh fallback may sleep until two minutes before token expiration: `twitch.rs:861-875`.
- Revocation calls `end_delegated_session_after_key_invalid`: `twitch.rs:923-930,999-1001`.
- Cleanup calls `remove_delegated_session`, which aborts both delegated task handles: `twitch.rs:707-720,729-757`.
- If the watcher or refresher invokes cleanup, it can abort its own task before all subsequent awaits complete.

**Recommended correction**

- Do not let a delegated worker abort its own `JoinHandle` during cleanup.
- Move teardown into an independently spawned coordinator, or use cancellation tokens and await worker exits from outside those workers.
- Close Twitch IRC/EventSub and Kick ingestion before deleting authorization state.
- Mark the delegated session revoked locally before network-dependent cleanup.
- Add a short lease or periodic validation so a missed SSE push has a bounded authorization lifetime.
- Regression-test: active revocation, missed push, SSE reconnect, multiple StreamSync instances, expiration, restart, and fallback to personal credentials.

### 3. Alert thresholds fail open when stored as strings

**Severity:** High  
**Status:** Reproduced deterministically

**Evidence**

- `variationMatchesTrigger` uses `Number.isFinite(trigger.value)` without coercion and returns `true` when `exact`/`min` values are invalid: `events-variation-picker.js:65-97`.
- Pool narrowing also cannot rank string thresholds: `events-variation-picker.js:111-128`.
- Event profiles are accepted and persisted without schema/type validation: `crates/stream-sync-core/src/routes.rs:1341-1365`.
- Studio migration does not normalize generic trigger values: `overlay-server/events-studio.html:1525-1539`.
- Editing a variation converts the input through `Number(...)`: `events-studio.html:3033-3047`.

**Reproduction result**

A Node harness using numeric `100` and `250` thresholds selected only the eligible 100-bit variations. The same configuration with string values `"100"` and `"250"` allowed a 100-bit event to select the 250-bit variation.

**Recommended correction**

- Normalize finite numeric strings at load/migration and again at the selection boundary.
- Make `exact` and `min` with invalid thresholds fail closed, not match everything.
- Validate persisted profile schemas.
- Add deterministic table tests for numeric strings, numbers, nulls, malformed values, exact/min modes, duplicate thresholds, and `100 !→ 250`.

### 4. Alerts are not queued and can overwrite one another

**Severity:** High  
**Status:** Confirmed by source inspection

**Evidence**

- Each incoming event invokes `showAlert` without awaiting a serialized queue: `events-overlay.js:886-920`.
- `showAlert` immediately clears the current alert and replaces shared timer/media state: `events-overlay.js:789-879`.

**Impact**

Bursty follows, gifts, subscriptions, raids, or cheers can replace one another, race asynchronous media/font work, or cause an older call to resume over a newer alert.

**Recommended correction**

Implement a FIFO alert queue with one active alert, explicit completion, generation/cancellation IDs, and bounded queue behavior. Test rapid mixed event bursts and media playback delays.

### 5. Kick Kicks alerts emit an event key with no matching configuration

**Severity:** High  
**Status:** Confirmed by source inspection

**Evidence**

- Kick emits overlay `eventType: "bits"`: `crates/stream-sync-core/src/kick.rs:427-434,481-490`.
- Overlay profiles define `cheer`: `crates/stream-sync-core/src/config_types.rs:302-309`.
- The renderer performs exact event-key lookup: `events-overlay.js:327-341`.

**Recommended correction**

Normalize Kick monetary alerts to `cheer` for overlay rendering while retaining `kicks` as the dock label/type, or centralize event aliases. Add a test proving a Kick Kicks event resolves the configured alert.

### 6. StreamElements variations lose parent settings when the variation overrides only part of an alert

**Severity:** High  
**Status:** Confirmed by source inspection

**Evidence**

- Variations are mapped from only the variation's settings rather than parent settings plus overrides: `crates/stream-sync-core/src/streamelements.rs:658-687,833-855`.
- The mapper then supplies defaults/empty media for unspecified fields: `streamelements.rs:956-1036`.

**Impact**

A StreamElements variation that changes only one graphic can lose the parent's message, typography, duration, audio, or other presentation. This plausibly contributes to imported overlays requiring manual one-click correction.

**Recommended correction**

Deep-merge parent event settings with variation overrides before mapping, or emit explicit inheritance markers. Add fixture tests for graphic-only, audio-only, message-only, and duration-only overrides.

---

## Medium-severity findings

### 7. Revoked and rotated credentials remain in plaintext `.bak` files

- Delegated sessions persist the takeover key and Twitch/Kick access/refresh tokens: `crates/stream-sync-core/src/config_types.rs:359-389`.
- Atomic writes retain the previous file as `.bak`: `crates/stream-sync-core/src/storage.rs:341-363`.
- Removal deletes the current delegated file but not its `.bak`: `crates/stream-sync-core/src/app_state.rs:247-258`.
- No restrictive permissions or OS credential-store integration was found.

Use Windows Credential Manager/DPAPI or another OS-backed secret store. At minimum, securely remove current and backup secret files on revocation and avoid creating recoverable backups of credentials.

### 8. Exported backups contain reusable account credentials

- Backups include Twitch/Kick tokens, StreamElements JWT, `.env`, token directories, and logs: `crates/stream-sync-core/src/export.rs:23-24,40-88`.
- Delegated takeover files are correctly excluded, but other reusable secrets remain.

Default to a configuration/media-only backup. If credential export is retained, require explicit opt-in and strong authenticated encryption with clear warnings.

### 9. OAuth flows lack state/binding validation

- Twitch uses implicit grant without `state`: `routes.rs:1091-1102`.
- Callback token installation reaches the unauthenticated local API: `routes.rs:435-470`.
- Token validation does not verify the returned Twitch client ID, and persisted scopes may come from callback input rather than Twitch validation: `twitch.rs:497-528`.
- Kick similarly lacks a local state/nonce binding: `crates/stream-sync-core/src/kick.rs:60-66`; `routes.rs:474-505`.

Add PKCE/state where supported, bind callbacks to an initiated local login transaction, verify client/channel identity, and take authoritative scopes from platform validation.

### 10. One slow WebSocket client can stall every feed

- `FeedHub` holds the registry read lock while sequentially awaiting each socket send and ignores failures: `crates/stream-sync-core/src/broadcast.rs:39-65`.

Snapshot senders, release the map lock, send concurrently with timeouts, and prune failed clients.

### 11. Gifted Twitch subscriptions may generate duplicate/wrong alerts

- `channel.subscribe` emits `sub` without checking `is_gift`: `crates/stream-sync-core/src/twitch.rs:1590-1605`.
- `channel.subscription.gift` separately emits `gift`: `twitch.rs:1654-1681`.

Suppress the ordinary subscription alert when `is_gift` is true and test representative EventSub notification pairs.

### 12. Imported event types and placeholders do not fully match runtime capabilities

- Import mappings create targets such as `redeem`, while Twitch redemptions are dock-only and Studio omits `redeem`: `streamelements.rs:600-611`; `twitch.rs:1711-1736`; `events-studio.html:1588-1595`.
- Imported templates can generate `[tier]`, `[message]`, `[sender]`, `[items]`, and `[currency]`, while the renderer supports a smaller vocabulary: `streamelements.rs:1131-1158`; `events-overlay.js:77-91`.
- Mapping StreamElements tips to cheers can cause donation assets to fire for bits.

Centralize normalized event types and placeholder vocabulary, then assert that every importer output has both an ingestion producer and an editable/renderable target.

### 13. Second desktop launches can attach to the wrong localhost server

- There is no single-instance enforcement; each launch starts a server: `crates/stream-sync-desktop/src/lib.rs:42-52`.
- Bind failure is logged from a detached task: `crates/stream-sync-desktop/src/overlay.rs:55-65`.
- Health checks accept any success response and the window can still open after timeout: `overlay.rs:68-81`; `lib.rs:93-123`.

Enforce a single application instance and validate a service identity/version/instance nonce before opening the UI.

### 14. Updater HMAC secret is distributed to every client

- Release preparation copies `.env` into bundled resources: `scripts/prepare-release.js:9-22`; `crates/stream-sync-desktop/tauri.conf.json:30-32`.
- The client reads that extractable secret and signs an update-page URL: `crates/stream-sync-desktop/src/commands.rs:212-242`.
- The current feature opens a webpage rather than validating/downloading/installing an update.

A client-shipped shared secret cannot authenticate clients. Use public-key-signed update metadata/artifacts, with the signing private key retained only in release infrastructure.

---

## Lower-priority reliability and maintenance findings

- EventSub reconnect handling appears to ignore Twitch's supplied `reconnect_url` and attempts fresh subscription setup, risking gaps: `crates/stream-sync-core/src/twitch.rs:1771-1808`.
- Release diagnostics are weak: Windows console is suppressed and no file tracing appender was found.
- Backup ZIP creation reads media and constructs the archive in memory before save completion, which can exhaust resources for large libraries.
- Backup/restore portability is incomplete: export exists, but no restore path was found; some profile indexes live in WebView `localStorage` and are absent from the archive.
- Versions are duplicated across Cargo, npm, and Tauri configuration without an automated consistency gate.
- No CI workflow or automated installer smoke/release gate is present.
- Tauri CSP is disabled and global Tauri APIs are enabled: `crates/stream-sync-desktop/tauri.conf.json:11-17`. This increases the impact of any frontend XSS.
- Archived/compatibility paths are mostly labeled, but `boot.html` and `streamelements-auth-inject.js` appear orphaned in the current Tauri flow.
- Documentation contains obsolete `cd rust` instructions, a headless port contradiction, an obsolete `npm start` contract workflow, and an `nsis`/`nis` path typo.

## What is sound

- Clean separation between personal and delegated identities.
- Direct platform ingestion keeps Syndicate outside the ordinary live data path.
- Loopback-only server binding.
- Correct system-tray close/quit behavior.
- User data resides outside packaged resources.
- Atomic JSON writes and corruption recovery provide a useful persistence baseline.
- Delegated takeover files are intentionally excluded from exported backup ZIPs.
- OBS source URLs are deterministic under the configured port.
- Packaged UI assets are explicitly included and resolved.
- Current repository structure cleanly separates reusable core, desktop shell, and optional headless server.

## Verification performed

- Confirmed exact repository commit and clean working tree.
- Read and mapped all runtime entrypoints and primary integration boundaries.
- Ran `node --check` against every JavaScript file: all passed.
- Ran a deterministic Node harness reproducing the string-threshold variation defect.
- Rust tests, formatting, Clippy, and builds were not executed because `cargo` and `rustc` are not installed in this Linux environment. No toolchain was silently installed.

## Suggested first implementation slices

1. **Local control-plane boundary** — token/origin middleware, privileged/public route split, WebSocket protection, endpoint body limits, regression tests.
2. **Takeover revocation coordinator** — non-self-aborting teardown, fail-safe lease/revalidation, multi-instance tests, secret cleanup.
3. **Alert correctness** — threshold normalization/fail-closed behavior, Kick alias fix, gifted-sub suppression, table tests.
4. **Alert delivery** — FIFO queue and WebSocket fan-out redesign.
5. **StreamElements fidelity** — parent/variation merge, placeholder/event vocabulary, fixtures.
6. **Credential lifecycle** — OS-backed secrets, backup redesign, OAuth state/PKCE.
7. **Release integrity** — signed update model, CI, version consistency, installer smoke tests, diagnostics.

This ordering prioritizes account authority and live-broadcast correctness before documentation and general cleanup.