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
};
// Match existing Stream Sync userData on Windows when integrating for real users:
// std::env::set_var("STREAMSYNC_USERDATA", r"C:\Users\...\AppData\Roaming\Stream Sync");

tokio::spawn(async move {
    OverlayServer::new(config).run().await.expect("overlay server");
});
```

## Public API surface

- `OverlayConfig` — port, repo root, readonly flag
- `OverlayServer::build_app()` — returns `(Router, Arc<AppState>, Arc<TwitchServices>)` for embedding in a larger Axum app
- `OverlayServer::run()` — standalone listener
- `rust_workspace_root()` / `resolve_ui_assets_root()` — locate UI assets in the workspace or bundle

## Deferred until parent app exists

- Native settings UI (replace `shell.html`)
- Unified tray / single-instance with broadcaster shell
- Shared update/signing pipeline
