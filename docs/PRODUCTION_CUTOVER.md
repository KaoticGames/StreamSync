# Production — Stream Sync 2.0 (Rust / Tauri)

Stream Sync **2.0** ships from this workspace: Tauri desktop + `stream-sync-core` on port **4040**.

Legacy Node `overlay-server/server.js` is archived under `legacy/node-overlay/` for reference only.

## What ships

| Layer | Location |
|-------|----------|
| Desktop app | `stream-sync-desktop` (Tauri) |
| Overlay server | `stream-sync-core` (in-process, `:4040`) |
| UI assets | Workspace root (`shell.html`, `overlay-server/`, `views/`) |
| User configs | `%APPDATA%\Stream Sync\` (unchanged from V1 Electron) |

## Commands

| Command | Purpose |
|---------|---------|
| `npm run dev` | Tauri dev (from `rust/`) |
| `npm run build` | Windows NSIS installer (see [RELEASE_BUILD.md](RELEASE_BUILD.md)) |
| `cargo run -p stream-sync-server` | Headless overlay only |

## Installer output

After `npm run build`:

`rust/target/release/bundle/nsis/` — see [RELEASE_BUILD.md](RELEASE_BUILD.md) for signing and R2 upload.

## Parent broadcasting app

Embed `stream-sync-core` per [HOST_INTEGRATION.md](HOST_INTEGRATION.md).
