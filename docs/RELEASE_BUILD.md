# Release build — GitHub Releases + signed updater

Stream Sync ships Windows x86_64 NSIS installers through **GitHub Releases**. CI must be green on `main` before tagging.

## Prerequisites

- Rust stable, Node.js 20, npm
- `.env` filled in (see `config/env.example`)
- WebView2 on Windows 10/11
- **Operator gate (G1):** `tauri signer generate` — store `TAURI_SIGNING_PRIVATE_KEY` in GitHub Actions secrets and paste the matching public key into `crates/stream-sync-desktop/tauri.conf.json` `plugins.updater.pubkey`

Until the signing secret and production pubkey are installed, release builds still produce the NSIS installer but updater artifacts (`.sig`, `latest.json`) are only uploaded when signing succeeds.

## Local build (unsigned smoke test)

```powershell
npm install
npm run build
```

`npm run build` runs `prepare-release` (`.env` → `config/bundled.env`) then `tauri build`.

**Output:** `target\release\bundle\nsis\Stream Sync_2.1.0_x64-setup.exe` (spacing per Tauri `productName`).

Copy bytes unchanged to the canonical release name `StreamSync-windows-x86_64-setup.exe` in CI.

## GitHub Release workflow

1. Bump `2.1.0` consistently (`Cargo.toml`, `package.json`, `tauri.conf.json`, UI).
2. Merge to `main` with green CI quality gates.
3. Tag `vX.Y.Z` on `main` (must match `tauri.conf.json`).
4. `release.yml` validates the tag, re-runs quality gates, builds NSIS, creates a **draft** Release with:
   - `StreamSync-windows-x86_64-setup.exe`
   - `SHA256SUMS.txt`
   - `StreamSync-windows-x86_64-setup.exe.sig` (when signing secret present)
   - `latest.json` (Phase 2+, when `.sig` exists)
5. Complete [INSTALLER_SMOKE.md](INSTALLER_SMOKE.md) on the draft asset.
6. Approve the `streamsync-release` environment to publish.

Stable human download URL:

`https://github.com/KaoticGames/StreamSync/releases/latest/download/StreamSync-windows-x86_64-setup.exe`

Updater manifest URL:

`https://github.com/KaoticGames/StreamSync/releases/latest/download/latest.json`

## Signing layers

| Mechanism | Purpose |
|-----------|---------|
| Tauri `.sig` | Authenticates downloaded updater artifact bytes before install. Does **not** sign the full `latest.json` manifest. |
| Authenticode (`WINDOWS_CERT_P12`) | Windows SmartScreen / publisher name — deferred Phase 3 |

## What gets bundled

| Item | Purpose |
|------|---------|
| `config/bundled.env` | Twitch Client ID, redirect, port |
| UI assets | `shell.html`, `overlay-server/`, `views/`, etc. |
| Updater config | `plugins.updater` endpoints + pubkey |

`%APPDATA%\Stream Sync\.env` still overrides bundled defaults.

## Legacy R2 flow

Manual Cloudflare R2 upload is retired for new releases. Keep read-only buckets for old bookmarks until optional mirror work in Phase 3.
