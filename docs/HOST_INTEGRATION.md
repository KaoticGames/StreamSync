# Host integration (parent broadcasting app)

A parent Rust broadcasting suite can depend on `stream-sync-core` as a path or workspace crate:

```toml
stream-sync-core = { path = "crates/stream-sync-core" }
```

## Startup (in-process)

```rust
use stream_sync_core::{OverlayConfig, OverlayServer, rust_workspace_root};
use std::path::PathBuf;

let repo_root = rust_workspace_root();
std::env::set_var("STREAMSYNC_UI_ROOT", &repo_root);

let config = OverlayConfig {
    port: 4040,
    repo_root,
    readonly: false,
    userdata_root: None,
    secret_store: None,
};
// Match existing Stream Sync userData on Windows when integrating for real users:
// std::env::set_var("STREAMSYNC_USERDATA", r"C:\Users\...\AppData\Roaming\Stream Sync");

tokio::spawn(async move {
    OverlayServer::new(config).run().await.expect("overlay server");
});
```

## Public API surface

- `OverlayConfig` — port, repo root, readonly flag, optional userdata root and secret store
- `OverlayServer::build_app()` — returns `(Router, Arc<AppState>, Arc<TwitchServices>)` for embedding in a larger Axum app
- `OverlayServer::run()` — binds the port **before** starting Twitch/Kick/Discord workers
- `rust_workspace_root()` / `resolve_ui_assets_root()` — locate UI assets in the workspace or bundle

Readonly (`STREAMSYNC_READONLY` / `OverlayConfig.readonly`): load JSON if present; do not create userdata dirs or write tokens/config. See [CONFIG.md](CONFIG.md).

## Deferred until parent app exists

- Native settings UI (replace `shell.html`)
- Shared update/signing pipeline
