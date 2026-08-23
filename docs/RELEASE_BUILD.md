# Release build — NSIS installer

Local Windows installer for Stream Sync 2.0, same workflow as Electron: build on your machine, upload the NSIS `.exe` to Cloudflare R2.

## Prerequisites

- Rust stable, Node.js, npm
- `rust/.env` filled in (see `config/env.example`)
- WebView2 (usually already on Windows 10/11)
- Optional: Windows SDK `signtool` for code signing

## Build (unsigned — smoke test)

```powershell
cd g:\Stream_Sync_V1.0\rust
npm install
npm run build
```

`npm run build` runs `prepare-release` first (copies `.env` → `config/bundled.env`), then `tauri build`.

**Output:** `rust\target\release\bundle\nsis\` — `Stream Sync_2.0.1_x64-setup.exe` (name may vary).

Install that exe on a test machine and verify Twitch connect, overlays, and **Help → Check for updates**.

## Code signing (recommended for R2 downloads)

Uses the same Authenticode `.pfx` as electron-builder (`certs/kaotic-games.pfx`).

### Option A — Certificate in Windows store (Tauri default)

1. Import the PFX (PowerShell):

```powershell
$pwd = Read-Host "PFX password" -AsSecureString
Import-PfxCertificate -FilePath "g:\Stream_Sync_V1.0\certs\kaotic-games.pfx" `
  -CertStoreLocation Cert:\CurrentUser\My -Password $pwd
```

2. Copy the certificate **Thumbprint** from `certmgr.msc` → Personal → Certificates.

3. Set in [`tauri.conf.json`](../crates/stream-sync-desktop/tauri.conf.json) under `bundle.windows`:

```json
"certificateThumbprint": "YOUR_THUMBPRINT_NO_SPACES"
```

`digestAlgorithm` and `timestampUrl` are already set.

4. `npm run build` — Tauri signs the NSIS installer via `signtool`.

### Option B — Unsigned build

Leave `certificateThumbprint` unset. Fine for your own install testing; SmartScreen may warn on public downloads.

## What gets bundled

| Item | Purpose |
|------|---------|
| `config/bundled.env` | Twitch Client ID, redirect, port, `STREAM_SYNC_UPDATE_SECRET` |
| UI assets | `shell.html`, `overlay-server/`, `views/`, etc. |
| Version `2.0.1` | `tauri.conf.json` + Cargo workspace |

`%APPDATA%\Stream Sync\.env` still overrides bundled defaults for power users.

## Upload

Upload the signed (or test) setup `.exe` from `target\release\bundle\nis\` to your R2 bucket / download page.

No GitHub Actions required for this flow.
