# Voice delivery v2 — StreamSync host client (Phase 5)

## Protocol mode

- Persisted in `discord-voice-config.json` as `voice_delivery_protocol` (`v2` default on this branch).
- Set to `legacy` only for explicit rollback to five-minute chunk append.
- After connect (`/connect` redeem), protocol is pinned to **v2**; the worker does not fall back to legacy chunk URLs mid-session.

## Local layout

Under the user-selected **recording parent** (`recording_parent`):

- `syndicate-discord-voice/<guildId>/` — final parent for published sessions
- `.streamsync-stage-<token>/` — staging (per delivery, from ledger identity)
- `.streamsync-control/` — locks, ledgers, checkpoints (opaque per session UUID)

## API (Syndicate)

- `GET /api/stream-sync/voice/v2/deliveries/pending?limit=N` — sealed deliveries; client expects each item to include `manifest` (strict Syndicate finalized manifest) plus `manifestDigest` and `sessionId`.
- `GET /api/stream-sync/voice/v2/deliveries/:sessionId/stems/:stemId?offset=&length=` — requires `206`, `Content-Range`, `ETag` stem SHA, bounded 8 MiB ranges.
- `POST /api/stream-sync/voice/v2/deliveries/:sessionId/receipt` — after local `Published` ledger state.

Bearer: existing SDK host token (`connection_key_authorization`).

## Retry / recovery

- Retryable: network, `Retry-After`, 429/503/5xx.
- Terminal: manifest/identity/range/ETag mismatch → quarantine local delivery, queue continues.
- Crash restart: reopen `DeliverySessionGuard`, resume checkpoints/partials, recover `PublishIntent` / `Published`, retry receipt without re-download.

## Status JSON

`GET` overlay status includes `voiceDeliveryProtocol`, `v2Phase`, `v2LastPublishedPath`.

## Windows manual acceptance (Phase 6 gate — not run here)

```powershell
cd crates\stream-sync-desktop
npx tauri build --bundles nsis --ci --no-sign --config tauri.ci.conf.json
```

On a Windows host with NTFS recording folder: connect SDK key, seal a test session on Syndicate staging, confirm pending → download → publish path under `syndicate-discord-voice/<guildId>/`, receipt accepted, no `chunks/pending` traffic (proxy log).

## Linux dev

```bash
cargo test -p stream-sync-core voice_delivery::
cargo clippy -p stream-sync-core -- -D warnings
npm test
```
