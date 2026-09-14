# Configuration (Rust / Tauri desktop)

## Two roots (by design)

| Root | Path | Purpose |
|------|------|---------|
| **Workspace** | This directory | `.env`, crates, `Cargo.toml`, UI static files |
| **User data** | `%APPDATA%\Stream Sync\` | Profiles, Twitch tokens, fonts, imported media |

In development, the HTTP server serves UI from the workspace root (`shell.html`, `overlay-server/`, `views/`). In release builds, the same files come from the Tauri resource bundle.

## `.env`

```env
TWITCH_CLIENT_ID=your_app_client_id
TWITCH_REDIRECT_URI=http://localhost:4040/auth/twitch/callback
OVERLAY_PORT=4040
```

Template: [config/env.example](../config/env.example)

```powershell
Copy-Item config\env.example .env
# edit .env
```

Optional override (same keys): `%APPDATA%\Stream Sync\.env`

## Environment variables

| Variable | Meaning |
|----------|---------|
| `STREAMSYNC_RUST_ROOT` | Force workspace path (usually auto-detected) |
| `STREAMSYNC_UI_ROOT` | Force UI asset root (workspace or bundle) |
| `STREAMSYNC_USERDATA` | `%APPDATA%\Stream Sync` |
| `STREAMSYNC_REPO_ROOT` | Alias for `STREAMSYNC_UI_ROOT` (static files only) |
| `STREAMSYNC_READONLY` | `true` / `1` — see below |
| `OVERLAY_PORT` | Listen port. Default **4040**. |

## User data (not in the workspace)

Profiles, Twitch tokens, fonts, SE imports: `%APPDATA%\Stream Sync\`

Logs: `%APPDATA%\Stream Sync\logs\stream-sync-YYYY-MM-DD.log`

## Readonly mode

Set `STREAMSYNC_READONLY=true` (or `--readonly` on `stream-sync-server`).

What it actually does:

- Loads existing JSON configs if they are present.
- Does **not** create the userdata directory, fonts dir, media dir, or token files.
- Does **not** write JSON, tokens, or control-token files.
- Uses an ephemeral in-memory control token (`ssc_readonly_…`) instead of persisting `control-token.txt`.
- Refuses to load a delegated session if a replace-pending marker is on disk.
- Mutating HTTP routes still fail closed.

Use it to point a second process at live userdata without mutating it. Still do not run two **writable** servers on the same folder.

## Legacy Electron (frozen)

The V1 Electron app is no longer maintained. There is no `npm start`. Use `npm run dev` from this directory.
