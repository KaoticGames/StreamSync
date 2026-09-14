# A/B testing: headless server on alternate port

`stream-sync-server` defaults to port **4040**, same as the desktop app. For parity checks, run it on **4041** so it does not collide with Tauri.

## Ports

| Stack | Command | URL |
|-------|---------|-----|
| **Desktop (Tauri)** | `npm run dev` (this directory) | `http://localhost:4040` |
| **Headless (default)** | `cargo run -p stream-sync-server` | `http://localhost:4040` |
| **Headless A/B** | `OVERLAY_PORT=4041 cargo run -p stream-sync-server` | `http://localhost:4041` |

## Safety rule

**Never run two servers writing to the same `%APPDATA%\Stream Sync` folder at once.**

For Rust validation on a copy of real configs:

```powershell
$env:STREAMSYNC_READONLY = "true"
$env:OVERLAY_PORT = "4041"
cargo run -p stream-sync-server
```

Or point at a fixture directory:

```powershell
$env:STREAMSYNC_USERDATA = "C:\path\to\fixture-userdata"
$env:OVERLAY_PORT = "4041"
cargo run -p stream-sync-server
```

## Browser URLs (headless A/B)

| URL | Purpose |
|-----|---------|
| `http://localhost:4041/health` | Health check (`ok`, `overlay-server`, version, `instanceNonce`) |
| `http://localhost:4041/overlay-server/events-studio.html` | Events alert editor |
| `http://localhost:4041/dock/chat` | Chat dock |

## OBS A/B

Duplicate a browser source URL and change the port:

- Desktop: `http://localhost:4040/overlay/chat?profile=chat-default`
- Headless test: `http://localhost:4041/overlay/chat?profile=chat-default`

## Contract tests

Against a **running** overlay (desktop on 4040 or headless A/B on 4041):

```powershell
$env:CONTRACT_BASE_URL = "http://127.0.0.1:4041"
cargo test -p stream-sync-core --test contract -- --ignored
```

## Twitch OAuth (A/B on 4041)

Register both redirect URLs in the Twitch developer console when testing:

- `http://localhost:4040/auth/twitch/callback` (desktop)
- `http://localhost:4041/auth/twitch/callback` (headless A/B)

If `.env` sets `TWITCH_REDIRECT_URI` to port **4040**, the Rust server rewrites localhost redirects to match `OVERLAY_PORT`.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `STREAMSYNC_USERDATA` | `%APPDATA%\Stream Sync` |
| `STREAMSYNC_READONLY` | `true` / `1` — load JSON, block writes (see [CONFIG.md](CONFIG.md)) |
| `OVERLAY_PORT` | Listen port. Default **4040** for both desktop and `stream-sync-server`. Set `4041` only for A/B. |
| `STREAMSYNC_UI_ROOT` | Workspace root (auto-detected) |
| `TWITCH_CLIENT_ID` | From `.env` or userData `.env` |
