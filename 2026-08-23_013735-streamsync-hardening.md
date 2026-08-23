# StreamSync Hardening and Reliability Implementation Plan

> **For Cursor:** Implement this plan one phase at a time. Do not attempt the entire audit in one pass. Use strict test-first development: write a regression test, run it and confirm the expected failure, implement the smallest correction, run focused and full tests, then stop for review before the next phase.

**Goal:** Correct the material security and live-broadcast reliability issues identified in the StreamSync 2.0.1 audit without broad rewrites or unrelated cleanup.

**Architecture:** Preserve StreamSync's local-first Rust/Tauri/Axum architecture. Separate OBS-safe read-only rendering/feed behavior from privileged control behavior, centralize event normalization, and add narrow regression coverage around every changed boundary. Security changes must preserve OBS integration, Tauri operation, account switching, and takeover behavior.

**Tech stack:** Rust 2021, Tokio, Axum 0.7, Tower HTTP, Tauri 2, browser JavaScript, Node's built-in test runner, Twitch IRC/EventSub, Kick/Syndicate SSE.

**Source baseline:** `main` at `654e1b9f136c0998cff851727537e9499dc1760a`.

**Audit reference:** `/home/steve/reviews/streamsync-initial-review-2026-08-23.md` (copy or attach this audit in Cursor if Cursor cannot access that local path).

---

## Non-negotiable execution rules

1. Create a new branch from the reviewed baseline for each phase.
2. Do not combine security boundary changes with event/import fixes.
3. Add the failing regression test before production changes.
4. Run the focused test and confirm that it fails for the intended reason.
5. Implement only the behavior covered by the current phase.
6. Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and JavaScript tests before committing.
7. Preserve existing OBS URLs unless a phase explicitly introduces a migration strategy.
8. Never place OAuth tokens, takeover keys, update secrets, or control capabilities in logs, error strings, backup archives, Git history, or test fixtures that resemble real credentials.
9. Stop after each phase and present the diff, test output, residual risks, and manual verification checklist.
10. Do not automatically fix lower-priority audit findings while touching nearby files.

---

# Phase 1 — Protect the localhost control plane

**Objective:** Prevent unrelated browser origins from invoking privileged StreamSync HTTP/WebSocket behavior while preserving read-only OBS rendering.

## Design to implement

Classify routes into two groups:

### Read-only OBS/public-local routes

These may remain available on loopback without a bearer token, but must not mutate state or send platform actions:

- Static UI/overlay assets needed by OBS
- `/health`
- Read-only overlay configuration required to render a selected profile
- A server-to-client-only feed endpoint for rendered chat/events
- Font/media files required by configured overlays

### Privileged control routes

These require both an approved local origin and a per-installation control capability:

- Account/token installation and removal
- Takeover connection creation, selection, removal, and disconnect
- Twitch/Kick chat sending and moderation
- StreamElements session creation/deletion/import
- Configuration mutation/deletion
- Media/font upload
- Test-alert injection
- Backup/log operations exposed through HTTP, if any
- Any bidirectional WebSocket command

Do not treat CORS as authentication. Generate a high-entropy per-installation capability and keep it outside browser-readable static files. The Tauri window may receive it through a narrowly scoped native command or injected initialization data. OBS docks that need to send chat require an explicit generated control-capability URL; ordinary overlays must remain read-only.

## Likely files

- Modify: `crates/stream-sync-core/src/app_state.rs`
- Modify: `crates/stream-sync-core/src/routes.rs`
- Modify: `crates/stream-sync-core/src/lib.rs`
- Modify: `crates/stream-sync-desktop/src/commands.rs`
- Modify: `crates/stream-sync-desktop/src/lib.rs`
- Modify: `crates/stream-sync-desktop/permissions/stream-sync-commands.toml`
- Modify: `tauri-bridge.js`
- Modify: `connections-api.js`
- Modify: `overlay-server/chat-dock.html`
- Modify: overlay/feed clients only as required
- Create: focused Axum integration tests under `crates/stream-sync-core/tests/`

## Tasks

### Task 1.1: Inventory and classify every route

Create a test-visible route policy table or nested routers rather than scattered ad hoc checks. For every route in `build_router`, label it read-only or privileged. Fail the build/test if a newly added mutating route is not classified.

### Task 1.2: Add origin validation tests

Write failing tests proving:

- An unrelated `Origin: https://example.invalid` cannot call privileged routes.
- Approved StreamSync localhost origins can proceed to capability validation.
- Missing or malformed `Origin` is handled deliberately rather than accidentally.
- OBS read-only routes still load from the expected localhost origin.
- WebSocket upgrades from an unrelated origin are rejected.

Expected initial result: current code accepts these requests because `CorsLayer::permissive()` and the WebSocket upgrade lack validation.

### Task 1.3: Add per-installation capability validation tests

Write failing tests for privileged HTTP requests:

- Missing capability returns `401`.
- Wrong capability returns `401`.
- Correct capability reaches the handler.
- Capability values never appear in response bodies or logs.

Use constant-time comparison for capability validation.

### Task 1.4: Split read-only feed from control messages

Write failing WebSocket tests proving:

- A read-only OBS feed cannot submit `chat-send`.
- An authenticated control/dock connection can submit it.
- Authentication must complete before feed data or command processing where the route is privileged.
- Rejected sockets do not remain registered in `FeedHub`.

Prefer separate endpoints (for example, read-only feed versus authenticated control) over a single socket with ambiguous authority.

### Task 1.5: Replace permissive CORS

Remove `CorsLayer::permissive()`. Configure exact allowed origins and methods per router. Do not allow credentials or authorization headers from arbitrary origins.

### Task 1.6: Apply endpoint-specific body limits

Write failing tests proving normal JSON control endpoints reject oversized bodies well below 64 MiB. Keep a larger authenticated limit only on media upload, with an explicit maximum and disk-space/error handling.

### Task 1.7: Wire Tauri and OBS dock capability delivery

- The main Tauri window must receive the control capability without embedding it in static assets.
- Ordinary OBS overlays remain read-only.
- A chat dock capable of sending messages must use an explicitly generated privileged URL/capability.
- Do not place the capability in server access logs.
- If a URL fragment is used, authenticate the WebSocket with the fragment value after opening; do not grant any feed/command access before validation.

### Task 1.8: Full verification

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm test
```

Manual verification:

- Existing Twitch/Kick overlays load in OBS.
- Existing read-only browser sources display chat/events.
- Tauri can connect/disconnect accounts and edit profiles.
- An unrelated web page cannot POST to a privileged route or open a command-capable socket.
- Chat dock can send messages only when configured with the privileged URL.

Stop for review and commit only this phase.

---

# Phase 2 — Make takeover revocation reliable

**Objective:** Guarantee prompt delegated-session termination without self-cancellation races and provide bounded enforcement if the SSE push is missed.

## Likely files

- Modify: `crates/stream-sync-core/src/twitch.rs`
- Modify: `crates/stream-sync-core/src/kick.rs`
- Modify: `crates/stream-sync-core/src/syndicate_connection.rs`
- Modify: `crates/stream-sync-core/src/app_state.rs`
- Create/extend tests in `crates/stream-sync-core/tests/`

## Tasks

### Task 2.1: Reproduce the self-abort cleanup race

Write a Tokio test with controlled fake watcher/refresher tasks. Trigger revocation from the watcher and assert that all cleanup completes:

- delegated state removed
- Twitch IRC stopped
- Twitch EventSub stopped
- Kick delegated ingestion stopped
- active mode persisted
- personal fallback started when available

Expected initial result: cleanup can abort the task performing teardown.

### Task 2.2: Introduce one teardown coordinator

Move delegated teardown into a coordinator that is not one of the handles it cancels. Use cancellation tokens or an equivalent explicit shutdown signal. Make teardown idempotent so simultaneous expiration, refresh failure, and SSE revocation cannot corrupt state.

### Task 2.3: Define fail-safe watch behavior

Write tests for:

- SSE revocation event
- SSE `401`
- transient SSE transport failure
- repeated reconnect failure
- key expiration
- refresh reporting `revoked`, `expired`, or `invalid_key`

A missed SSE message must not permit indefinite delegated access. Implement a bounded lease/periodic revalidation interval and document the maximum revocation delay under push failure.

### Task 2.4: Test multiple local sessions conceptually

The Syndicate server implementation is outside this repository, but add client tests ensuring each StreamSync instance maintains an independent watcher and handles a revocation event identically. Record a required integration test against Syndicate proving one key revocation reaches all connected consumers.

### Task 2.5: Remove raw key from event-stream URLs

Change the events request from `?key=...` to an authorization header or a short-lived opaque watch-session ID. Ensure transport errors cannot print the raw takeover key.

### Task 2.6: Verify manual behavior

- Connect using takeover key.
- Confirm chat/event ingestion.
- Revoke while active.
- Confirm immediate stop.
- Confirm personal connection remains selectable.
- Disconnect network before revocation, restore it, and confirm bounded enforcement.
- Repeat with two StreamSync instances using the same key.
- Restart after revocation and confirm failure.

Stop for review and commit only this phase.

---

# Phase 3 — Correct alert selection and platform event normalization

**Objective:** Ensure an event can only select eligible alert variations and normalize platform-specific names before renderer lookup.

## Likely files

- Modify: `events-variation-picker.js`
- Modify: `overlay-server/events-studio.html`
- Modify: `crates/stream-sync-core/src/routes.rs`
- Modify: `crates/stream-sync-core/src/kick.rs`
- Modify: `crates/stream-sync-core/src/twitch.rs`
- Create: `tests/events-variation-picker.test.js` or equivalent Node built-in tests
- Modify: `package.json` test script to run both Rust and JS suites without hiding either exit code

## Tasks

### Task 3.1: Add the 100-bit regression test

Write deterministic tests proving:

- Numeric threshold `100` matches a 100-bit event.
- Numeric threshold `250` does not match a 100-bit event.
- String threshold `"100"` is normalized and matches.
- String threshold `"250"` is normalized and does not match.
- Invalid `exact`/`min` values fail closed.
- Weighted choice occurs only after eligibility filtering.
- Multiple variations at the same eligible threshold respect weights.

Confirm the string-threshold test initially fails by selecting the ineligible 250-bit variation under a controlled random value.

### Task 3.2: Normalize at migration and selection boundaries

Coerce finite numeric strings once during profile migration. Also defend at runtime selection so malformed imported/persisted data cannot fail open. Reject or ignore invalid rule values with a visible configuration warning.

### Task 3.3: Validate profile writes

Add server-side validation for event profiles rather than persisting arbitrary JSON opaquely. Preserve forward-compatible unknown presentation fields, but enforce types for event keys, variations, trigger mode/value/tier, chance, duration, and placement bounds.

### Task 3.4: Normalize Kick Kicks to the configured overlay event

Add a failing test showing a Kick `kicks` event resolves the `cheer` alert configuration. Emit normalized `cheer` for overlays while retaining a Kick-specific dock label/type.

### Task 3.5: Suppress duplicate gifted-sub alerts

Add Twitch EventSub fixture tests proving `channel.subscribe` with `is_gift: true` does not emit a normal `sub` overlay alert, while `channel.subscription.gift` emits exactly one `gift` alert.

Stop for review and commit only this phase.

---

# Phase 4 — Serialize live alert delivery

**Objective:** Display every eligible alert in a deterministic FIFO sequence without asynchronous overlap.

## Likely files

- Modify: `events-overlay.js`
- Add browser/Node tests for queue state and ordering

## Tasks

1. Write a failing test sending three alerts rapidly and asserting all three complete in order.
2. Introduce one alert queue and one active alert worker.
3. Make `showAlert` resolve only after entrance, display duration, exit, and cleanup complete.
4. Use a generation/cancellation ID to prevent stale async font/audio/media work from mutating a newer alert.
5. Define bounded behavior for pathological bursts: maximum queue length, drop policy if any, and logging/metrics.
6. Test audio failure, missing media, font timeout, zero/maximum duration, and WebSocket reconnect during an active alert.
7. Manually fire rapid follow/sub/gift/bits/raid tests into OBS and confirm none overwrite another.

Stop for review and commit only this phase.

---

# Phase 5 — Restore StreamElements import fidelity

**Objective:** Preserve parent alert settings when StreamElements variations override only selected fields, and ensure every imported event/token is renderable.

## Likely files

- Modify: `crates/stream-sync-core/src/streamelements.rs`
- Modify: `events-overlay.js`
- Modify: `overlay-server/events-studio.html`
- Extend fixtures under `crates/stream-sync-core/tests/se_mapper_fixtures/`

## Tasks

1. Add a fixture where a variation overrides only graphics; assert message, duration, audio, typography, and layout inherit from the parent.
2. Add equivalent fixtures for audio-only, message-only, and duration-only overrides.
3. Deep-merge parent settings with variation settings before mapping, or emit explicit StreamSync inheritance markers. Do not replace missing variation fields with unrelated defaults.
4. Centralize placeholder vocabulary shared by importer and renderer.
5. Add tests for every importer-produced placeholder: name, user, amount, months, reward, input/message, recipient, tier, sender, items, and currency—or explicitly warn and skip unsupported placeholders.
6. Inventory normalized event targets. Every imported event type must have an ingestion producer, Studio selector, and renderer configuration; otherwise warn/skip it rather than silently mapping it to a semantically different event.
7. Do not map tips/donations to Twitch cheers unless the product explicitly intends shared presentation.
8. Re-import representative StreamElements overlays and compare parent/variation rendering without manual one-click repairs.

Stop for review and commit only this phase.

---

# Phase 6 — Credential lifecycle, OAuth, and backups

**Objective:** Remove recoverable residual credentials, bind OAuth callbacks to initiated sessions, and make backups safe by default.

## Likely files

- Modify: `crates/stream-sync-core/src/storage.rs`
- Modify: `crates/stream-sync-core/src/app_state.rs`
- Modify: `crates/stream-sync-core/src/streamelements.rs`
- Modify: `crates/stream-sync-core/src/export.rs`
- Modify: `crates/stream-sync-core/src/twitch.rs`
- Modify: `crates/stream-sync-core/src/kick.rs`
- Modify: `crates/stream-sync-core/src/routes.rs`
- Add an OS credential-store abstraction in the desktop crate or a narrowly selected dependency

## Tasks

1. Write tests proving delegated removal deletes current and backup secret material.
2. Stop creating plaintext `.bak` copies of credential files.
3. Move Twitch/Kick/StreamElements/takeover credentials into Windows Credential Manager/DPAPI or an equivalent OS-backed store; keep non-secret metadata in JSON.
4. Add OAuth transaction state and callback binding. Use PKCE where supported.
5. Verify returned client/channel identity and take scopes from authoritative platform validation rather than callback input.
6. Change backup defaults to configuration/media only.
7. If credential export remains available, require explicit opt-in and authenticated encryption; never export silently.
8. Add restore tests before promising PC migration in the UI. Include profile indexes currently held in WebView storage or migrate them to server-owned persistence.

Stop for review and commit only this phase.

---

# Phase 7 — Runtime and release reliability

**Objective:** Remove feed stalls, ambiguous startup, and extractable updater trust while adding repeatable release gates.

## Tasks

### Task 7.1: Fix WebSocket fan-out

- Add tests with one blocked/failed client and one healthy client.
- Snapshot sender handles outside the client-map lock.
- Send concurrently with timeouts.
- Prune failed clients.
- Ensure register/unregister are not blocked by a slow send.

### Task 7.2: Follow Twitch EventSub reconnect semantics

- Add fixture tests for `session_reconnect`.
- Connect to Twitch's supplied `reconnect_url` rather than creating unrelated subscriptions immediately.
- Verify no event gap or duplicate subscription set during handoff.

### Task 7.3: Enforce a single desktop instance

- Add single-instance enforcement.
- Validate `/health` service identity, version, and an instance nonce before opening the Tauri window.
- Treat bind failure as a visible startup error rather than detached logging.

### Task 7.4: Add durable diagnostics

- Add a rotating file tracing appender with credential redaction.
- Correct the purge-log API/UI contract.
- Ensure release-mode startup and platform-connection failures are visible.

### Task 7.5: Replace updater shared-secret design

- Remove `STREAM_SYNC_UPDATE_SECRET` from client bundles.
- Use public-key-signed update metadata and artifacts.
- Keep the private signing key only in release infrastructure.
- If automatic update is not yet implemented, rename the UI action to accurately describe opening the download/update page.

### Task 7.6: Add CI and release gates

Add GitHub Actions for:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm test
```

Also add:

- Version consistency check across Cargo, npm, and Tauri configuration.
- Windows installer build gate.
- Signed artifact/provenance workflow when signing infrastructure is ready.
- Installer smoke checklist for Twitch auth, takeover, Kick connection, overlays, docks, tray lifecycle, and update behavior.

### Task 7.7: Reconcile documentation

Only after runtime behavior is final:

- Remove obsolete `cd rust` instructions.
- Correct headless default-port documentation.
- Replace obsolete `npm start` contract instructions.
- Correct `nis` to `nsis`.
- Document readonly mode's actual filesystem behavior.
- Classify or remove orphaned `boot.html` and `streamelements-auth-inject.js` paths after reference verification.

Stop for final review.

---

# Final acceptance criteria

The hardening program is complete only when:

- An unrelated webpage cannot invoke any privileged localhost HTTP or WebSocket operation.
- OBS read-only overlays continue working with stable URLs.
- Chat-sending docks require explicit delegated local authority.
- Revoking or expiring a takeover terminates active platform ingestion within a documented maximum interval, including push-channel failure.
- A 100-bit event can never select a 250-bit alert because of threshold type or weighting.
- Rapid alerts display in order without overwriting one another.
- Kick Kicks and Twitch gifts resolve exactly one correct alert.
- StreamElements partial variations preserve parent behavior.
- Revoked/rotated credentials do not remain in plaintext backups.
- Default backups do not contain reusable account credentials.
- Slow/dead OBS sources cannot stall healthy feeds.
- Second launches cannot attach to the wrong local process.
- Update authenticity does not depend on a client-shipped shared secret.
- Rust formatting, Clippy, Rust tests, JavaScript tests, Windows build, and manual OBS smoke checks all pass.

# Cursor handoff prompt

Use the following prompt with Cursor for each phase, changing only the phase number:

> Read `.hermes/plans/2026-08-23_013735-streamsync-hardening.md` and the attached initial audit. Implement **Phase N only** using strict test-first development. Before editing production code, inspect all cited files and write the smallest regression test that demonstrates the current defect. Run it and show the expected failure. Then implement the minimal bounded fix, run focused and full verification, and stop. Do not implement later phases, refactor unrelated code, change public behavior not required by Phase N, commit, or push without my approval. Return the changed-file list, diff summary, exact test output, remaining risks, and manual verification steps.
