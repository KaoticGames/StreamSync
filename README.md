# Stream Sync 2.0 — Rust workspace

**Release installer:** see [docs/RELEASE_BUILD.md](docs/RELEASE_BUILD.md) (`npm run build` → NSIS in `target/release/bundle/nsis/`).

Standalone Rust/Tauri build. All UI assets, config, and crates live in this directory — no dependency on a parent repo folder.

| Crate | Role |
|-------|------|
| `stream-sync-desktop` | **Primary app** — Tauri window + in-process overlay |
| `stream-sync-core` | Overlay HTTP/WS/Twitch server library |
| `stream-sync-server` | Headless CLI (optional, no UI) |

## First-time setup

```powershell
cd rust
Copy-Item config\env.example .env
# Edit .env — set TWITCH_CLIENT_ID, etc.
npm install
```

## Run the desktop app

```powershell
cd rust
npm run dev
```

Or:

```powershell
cd rust
cargo run -p stream-sync-desktop
```

- Config: **`.env`** in this directory
- User profiles/tokens: `%APPDATA%\Stream Sync\`
- UI files: served from this workspace root in dev, or from the Tauri resource bundle when packaged

See [docs/CONFIG.md](docs/CONFIG.md) and [docs/TAURI_DESKTOP.md](docs/TAURI_DESKTOP.md).

## Headless overlay only

```powershell
cd rust
cargo run -p stream-sync-server
```

## Build installer

```powershell
cd rust
npm run build
```

Installers appear under `target/release/bundle/`.
