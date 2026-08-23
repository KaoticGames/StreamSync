//! Example: how a parent broadcasting app embeds `stream-sync-core`.
//!
//! ```text
//! cargo run --example host_embed -p stream-sync-core
//! ```

use stream_sync_core::{OverlayConfig, OverlayServer, rust_workspace_root};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let repo = rust_workspace_root();
    std::env::set_var("STREAMSYNC_UI_ROOT", &repo);
    // Match Stream Sync userData on Windows when integrating for real users:
    // std::env::set_var("STREAMSYNC_USERDATA", r"C:\Users\...\AppData\Roaming\Stream Sync");

    let config = OverlayConfig {
        port: 4041,
        repo_root: repo,
        readonly: true,
    };

    OverlayServer::new(config).run().await
}
