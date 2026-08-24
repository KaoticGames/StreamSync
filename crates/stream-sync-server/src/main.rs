//! Standalone CLI for the Rust overlay server (default port 4040).

use clap::Parser;
use stream_sync_core::{OverlayConfig, OverlayServer};
use tracing_subscriber::EnvFilter;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("stream_sync_core=info".parse()?),
        )
        .init();

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
