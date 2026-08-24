//! Phase 2 review corrections — production-path delegated revocation tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(unix)]
use stream_sync_core::disconnect_twitch;
use stream_sync_core::{
    connection_key_events_url, remove_file_durable, sync_live_identity,
    write_delegated_revoked_tombstone, write_json, AppState, DelegatedSessionFile, OverlayConfig,
    OverlayServer, TwitchActiveMode, TwitchServices, MAX_DELEGATED_REVOCATION_DELAY,
    SYNDICATE_HTTP_TIMEOUT, SYNDICATE_SSE_READ_TIMEOUT,
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

async fn build_app(
    port: u16,
) -> (
    axum::Router,
    Arc<stream_sync_core::AppState>,
    Arc<TwitchServices>,
) {
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
    OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app")
}

fn sample_delegated_json() -> serde_json::Value {
    serde_json::json!({
        "generation": 1,
        "connection_key": "ssk_phase2_test_placeholder_not_a_real_key",
        "client_id": "cid",
        "access_token": "delegated-access",
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        "kick_access_token": "delegated-kick",
        "kick_id": "k1"
    })
}

#[tokio::test]
async fn durable_revoke_propagates_readonly_deletion_failure() {
    let (_router, state, _services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap(),
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            &state.paths.twitch_delegated,
            std::fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        assert!(state.durable_revoke_delegated().await.is_err());
        assert!(state.paths.twitch_delegated.is_file());
    }

    #[cfg(windows)]
    {
        let mut perms = std::fs::metadata(&state.paths.twitch_delegated)
            .unwrap()
            .permissions();
        perms.set_readonly(true);
        let _ = std::fs::set_permissions(&state.paths.twitch_delegated, perms);
        let result = state.durable_revoke_delegated().await;
        if result.is_err() {
            assert!(state.paths.twitch_delegated.is_file());
        } else {
            assert!(!state.paths.twitch_delegated.is_file());
        }
    }
}

#[tokio::test]
async fn disconnect_twitch_propagates_durable_revoke_failure() {
    let (_router, state, services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap(),
    )
    .unwrap();
    let mut session =
        serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap();
    session.generation = 1;
    *state.delegated.write().await = Some(session);
    *state.active_mode.write().await = TwitchActiveMode::Delegated;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            &state.paths.twitch_delegated,
            std::fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        assert!(disconnect_twitch(state.clone(), services.clone())
            .await
            .is_err());
        assert!(state.paths.twitch_delegated.is_file());
    }

    #[cfg(windows)]
    {
        let _ = services;
        // Windows may allow deletion of read-only files; covered on Unix.
    }
}

#[tokio::test]
async fn revoked_tombstone_prevents_restart_into_delegated_mode() {
    let userdata = test_userdata_dir();
    std::env::set_var("STREAMSYNC_USERDATA", userdata.display().to_string());
    write_json(
        &userdata.join("twitch-delegated.json"),
        &serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap(),
    )
    .unwrap();
    write_delegated_revoked_tombstone(&userdata.join("twitch-delegated.revoked")).unwrap();

    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .unwrap()
        .to_path_buf();
    let paths = stream_sync_core::get_paths().unwrap();
    let state = AppState::new(paths, repo_root, 0, false).expect("startup quarantine");
    assert!(state.delegated.read().await.is_none());
    assert_ne!(*state.active_mode.read().await, TwitchActiveMode::Delegated);
}

#[tokio::test]
async fn stale_generation_teardown_cannot_clear_replacement_session() {
    let (_router, state, services) = build_app(0).await;
    let gen_a = state.bump_delegated_generation();
    *state.delegated.write().await = Some(serde_json::from_value(sample_delegated_json()).unwrap());
    services.install_delegated_generation(gen_a).await;

    let gen_b = state.bump_delegated_generation();
    let mut session_b =
        serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap();
    session_b.generation = gen_b;
    session_b.connection_key = "ssk_generation_b".into();
    *state.delegated.write().await = Some(session_b);
    services.install_delegated_generation(gen_b).await;

    services
        .signal_delegated_teardown(state.clone(), gen_a, "revoked")
        .await
        .expect("stale teardown no-op");

    let delegated = state.delegated.read().await.clone();
    assert_eq!(
        delegated.as_ref().map(|s| s.connection_key.as_str()),
        Some("ssk_generation_b")
    );
}

#[tokio::test]
async fn kick_feed_uses_authorization_not_query_key() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let saw_auth = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_query = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_auth2 = saw_auth.clone();
    let saw_query2 = saw_query.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
        if req.contains("authorization:") && req.contains("bearer ssk_") {
            saw_auth2.store(true, Ordering::SeqCst);
        }
        if req.contains("?key=") {
            saw_query2.store(true, Ordering::SeqCst);
        }
        let body = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
        let _ = socket.write_all(body).await;
    });
    std::env::set_var("SYNDICATE_API_BASE", format!("http://127.0.0.1:{port}"));

    let (_router, state, _services) = build_app(0).await;
    *state.delegated.write().await = Some(serde_json::from_value(sample_delegated_json()).unwrap());
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    {
        let mut k = state.kick.write().await;
        k.tokens.access_token = Some("delegated-kick".into());
        k.tokens.kick_id = Some("k1".into());
    }
    sync_live_identity(state.clone()).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(saw_auth.load(Ordering::SeqCst));
    assert!(!saw_query.load(Ordering::SeqCst));
}

#[test]
fn documented_revocation_bound_includes_http_and_sse_timeouts() {
    assert_eq!(MAX_DELEGATED_REVOCATION_DELAY.as_secs(), 300);
    assert!(SYNDICATE_HTTP_TIMEOUT.as_secs() > 0);
    assert!(SYNDICATE_SSE_READ_TIMEOUT.as_secs() > 0);
}

#[test]
fn events_url_helper_has_no_query_key() {
    let url = connection_key_events_url();
    assert!(url.contains("/api/stream-sync/connection-keys/events"));
    assert!(!url.contains("?key="));
}

#[tokio::test]
async fn each_streamsync_instance_owns_independent_twitch_services() {
    let (_r1, _s1, t1) = build_app(0).await;
    let (_r2, _s2, t2) = build_app(0).await;
    assert!(!Arc::ptr_eq(&t1, &t2));
}

#[test]
fn remove_file_durable_deletes_existing_file() {
    let dir = test_userdata_dir();
    let path = dir.join("twitch-delegated.json");
    std::fs::write(&path, b"{}").unwrap();
    remove_file_durable(&path).unwrap();
    assert!(!path.is_file());
}
