# Stream Sync 2.1 — Rust workspace

**Release installer:** see [docs/RELEASE_BUILD.md](docs/RELEASE_BUILD.md) (`npm run build` → NSIS in `target/release/bundle/nsis/`).

This directory **is** the workspace (crates, UI, config). Run commands here. Do not `cd rust`.

| Crate | Role |
|-------|------|
| `stream-sync-desktop` | **Primary app** — Tauri window + in-process overlay |
| `stream-sync-core` | Overlay HTTP/WS/Twitch server library |
| `stream-sync-server` | Headless CLI (optional, no UI) |

## First-time setup

```powershell
Copy-Item config\env.example .env
# Edit .env — set TWITCH_CLIENT_ID, etc.
npm install
```

## Run the desktop app

```powershell
npm run dev
```

Or:

```powershell
cargo run -p stream-sync-desktop
```

- Config: **`.env`** in this directory
- User profiles/tokens: `%APPDATA%\Stream Sync\`
- UI files: served from this workspace root in dev, or from the Tauri resource bundle when packaged

See [docs/CONFIG.md](docs/CONFIG.md) and [docs/TAURI_DESKTOP.md](docs/TAURI_DESKTOP.md).

## Headless overlay only

Default listen port is **4040** (same as desktop / OBS URLs). Do not run this at the same time as the desktop app against the same user-data folder.

```powershell
cargo run -p stream-sync-server
```

For a second instance on **4041**, see [docs/AB_TESTING.md](docs/AB_TESTING.md).

## Build installer

```powershell
npm run build
```

Installers appear under `target/release/bundle/nsis/`.
