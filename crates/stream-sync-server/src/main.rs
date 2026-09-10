//! Standalone CLI for the Rust overlay server (default port 4040).

use clap::Parser;
use stream_sync_core::{OverlayConfig, OverlayServer};

#[derive(Parser, Debug)]
#[command(name = "stream-sync-server")]
#[command(about = "Stream Sync Rust overlay server")]
struct Args {
    /// HTTP port (default 4040 — same as Electron/OBS URLs)
    #[arg(long, env = "OVERLAY_PORT", default_value = "4040")]
    port: u16,

    /// Path to workspace UI root (contains shell.html and overlay-server/)
    #[arg(long, env = "STREAMSYNC_REPO_ROOT")]
    repo_root: Option<std::path::PathBuf>,

    /// Read-only mode — load configs but do not write JSON
    #[arg(long, env = "STREAMSYNC_READONLY", default_value = "false")]
    readonly: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logs_dir = std::env::var("STREAMSYNC_USERDATA")
        .ok()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("logs");
    let _ = stream_sync_core::init_tracing(&logs_dir);

    let args = Args::parse();
    let mut config = OverlayConfig {
        port: args.port,
        readonly: args.readonly,
        ..OverlayConfig::default()
    };
    if let Some(root) = args.repo_root {
        config.repo_root = root;
    }

    OverlayServer::new(config).run().await
}
