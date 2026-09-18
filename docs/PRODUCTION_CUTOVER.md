# Production — Stream Sync 2.1 (Rust / Tauri)

Stream Sync **2.1** ships from this workspace: Tauri desktop + `stream-sync-core` on port **4040**.

## What ships

| Layer | Location |
|-------|----------|
| Desktop app | `stream-sync-desktop` (Tauri) |
| Overlay server | `stream-sync-core` (in-process, `:4040`) |
| UI assets | Workspace root (`shell.html`, `overlay-server/`, `views/`) |
| User configs | `%APPDATA%\Stream Sync\` (unchanged from V1 Electron) |
| Updates | GitHub Releases (`latest.json` + signed NSIS) |

## Commands

Run from this directory. There is no `npm start`.

| Command | Purpose |
|---------|---------|
| `npm run dev` | Tauri dev |
| `npm run build` | Windows NSIS installer (see [RELEASE_BUILD.md](RELEASE_BUILD.md)) |
| `cargo run -p stream-sync-server` | Headless overlay only (default **4040**) |

## Release authority

| Concern | Source |
|---------|--------|
| Latest version | GitHub Release tag + `latest.json` |
| Installer download | `StreamSync-windows-x86_64-setup.exe` on the Release |
| Browser fallback | `https://syndicateai.net/update?app=stream-sync&v=<version>` |

Syndicate dashboard/API cutover to read GitHub directly is a separate repo change (Phase S) after the first signed `latest.json` is published.

## Installer output

After `npm run build`:

`target/release/bundle/nsis/` — see [RELEASE_BUILD.md](RELEASE_BUILD.md) for signing, tagging, and publish gates.

## Parent broadcasting app

Embed `stream-sync-core` per [HOST_INTEGRATION.md](HOST_INTEGRATION.md).
