# Voice local finalization (In-Sync processor)

StreamSync downloads sealed Opus journals from Syndicate, decodes locally, publishes WAV stems + `Syndicate Audio Recovery Report.txt`, and posts a digest-bound receipt.

## API (Syndicate Dashboard)

Bearer: existing `sdk_` host token (`connection_key_authorization`).

- `GET /api/stream-sync/voice/local-v1/deliveries/pending?limit=N`
- `POST /api/stream-sync/voice/local-v1/deliveries/:sessionId/claim`
- `POST /api/stream-sync/voice/local-v1/deliveries/:sessionId/lease/renew`
- `GET /api/stream-sync/voice/local-v1/deliveries/:sessionId/manifest`
- `GET /api/stream-sync/voice/local-v1/deliveries/:sessionId/segments/:segmentSeq` — `Range` required, `307` → R2 (credentials stripped on redirect)
- `POST /api/stream-sync/voice/local-v1/deliveries/:sessionId/receipt`
- `POST /api/stream-sync/voice/local-v1/deliveries/:sessionId/release`

## Local layout

Under the user-selected recording parent:

```text
.in-sync/<session-id>/
  manifest.json
  download-checkpoint.json
  journal/segments/*.synj
  output-staging/
```

Published stems still use the existing `syndicate-discord-voice/<guildId>/…` publication system when wired through the v2 orchestrator.

## Config

`discord-voice-config.json`:

- `local_processing_mode`: `manual` (default) or `automatic_after_seal`

## Opus decode

Enable native decode for production builds:

```bash
cargo build -p stream-sync-core --features local-opus-native
```

Requires system `libopus` (or Windows dev environment with Opus). Default builds use a stub decoder (silence/report path only) so CI/desktop check can compile without libopus.

## UI

Top-level **In-Sync** tab → Recordings functional panel (`views/in-sync.html`, `in-sync-recordings.js`). Overlay status includes `inSync` and `/api/in-sync/*` control routes.

## Protocol

`recordingManifest.key` is either legacy `…/manifest/recording.manifest.json` or content-addressed `…/manifest/recording/<64 lowercase hex>.manifest.json`; for the latter, the path digest must equal `recordingManifest.sha256`.

Shared manifest fixture digest: `fc0c603e00b6656fcbf4e921bf118b941dd1a9433d5e17b0cd24b5fe05740023` (`crates/stream-sync-core/tests/fixtures/local_finalization_manifest_v1.json`).
