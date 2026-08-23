# Configuration (Rust / Tauri desktop)

## Two roots (by design)

| Root | Path | Purpose |
|------|------|---------|
| **Workspace** | This directory (`rust/`) | `.env`, crates, `Cargo.toml`, UI static files |
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

## User data (not in the workspace)

Profiles, Twitch tokens, fonts, SE imports: `%APPDATA%\Stream Sync\`

## Legacy Electron (frozen)

The V1 Electron app at the old repo root is no longer maintained. Use `npm run dev` from this directory.
