//! Phase 2 — takeover revocation client guarantees.
//!
//! ## Required Syndicate integration (outside this repo)
//! Prove that one connection-key revocation fan-out reaches every connected StreamSync
//! consumer (SSE `revoked` and/or subsequent refresh `revoked`/`expired`/`invalid_key`).
//! Client-side coverage below only asserts each local instance keeps an independent
//! watcher/teardown gate and uses Authorization (not `?key=`) for the events URL.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stream_sync_core::{
    OverlayConfig, OverlayServer, TwitchServices, MAX_DELEGATED_REVOCATION_DELAY,
};

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);
static TEST_SETUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_userdata_dir() -> std::path::PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("streamsync-phase2-it-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

async fn build_instance(port: u16) -> (Arc<stream_sync_core::AppState>, Arc<TwitchServices>) {
    let _guard = TEST_SETUP_LOCK.lock().await;
    let userdata = test_userdata_dir();
    std::env::set_var("STREAMSYNC_USERDATA", userdata.display().to_string());
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf();
    let config = OverlayConfig {
        port,
        repo_root,
        readonly: false,
    };
    let (_router, state, twitch) = OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app");
    (state, twitch)
}

#[tokio::test]
async fn each_streamsync_instance_owns_independent_twitch_services() {
    let (_s1, t1) = build_instance(0).await;
    let (_s2, t2) = build_instance(0).await;
    assert!(!Arc::ptr_eq(&t1, &t2));
}

#[test]
fn events_url_helper_has_no_query_key() {
    let url = stream_sync_core::connection_key_events_url();
    assert!(url.contains("/api/stream-sync/connection-keys/events"));
    assert!(!url.contains("?key="));
}

#[test]
fn max_delegated_revocation_delay_is_five_minutes() {
    assert_eq!(
        MAX_DELEGATED_REVOCATION_DELAY,
        std::time::Duration::from_secs(300)
    );
}
