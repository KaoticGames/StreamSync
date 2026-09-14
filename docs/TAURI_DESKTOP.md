# Stream Sync — Tauri desktop (primary)

The shipping desktop app is **Rust + Tauri**. Commands run from this workspace root.

| Layer | Crate / path |
|-------|----------------|
| Overlay HTTP/WS/Twitch | `stream-sync-core` (in-process, port **4040**) |
| Desktop shell | `stream-sync-desktop` (Tauri window) |
| UI | `shell.html`, `views/`, config JS (workspace root or Tauri bundle) |

## Run (development)

```powershell
npm install
npm run dev
```

This runs `tauri dev`, which:

1. Starts `stream-sync-core` on `http://127.0.0.1:4040`
2. Opens a Tauri window to `http://127.0.0.1:4040/shell.html`
3. Injects desktop APIs via `tauri-bridge.js`

There is no `npm start`. Dev is `npm run dev`; installer is `npm run build`.

## User data (unchanged)

Configs and Twitch tokens still live in:

`%APPDATA%\Stream Sync\` (same as legacy Electron `productName`)

## OBS URLs (unchanged)

`http://localhost:4040/dock/chat`, `/overlay/events?profile=default`, etc.

## Build installer

```powershell
npm run build
```

Output: `target/release/bundle/nsis/`

## Headless overlay on :4041

See [AB_TESTING.md](AB_TESTING.md) — only needed for parity testing, not daily use. The headless CLI itself defaults to **4040**.

## Bundled UI files

| File | Status |
|------|--------|
| `shell.html` | Live — Tauri main window |
| `streamelements-auth-inject.js` | Live — injected into the StreamElements login webview |
| `boot.html` | Superseded Electron splash. Not opened by Tauri. Left in the tree for history; not a product path. |
