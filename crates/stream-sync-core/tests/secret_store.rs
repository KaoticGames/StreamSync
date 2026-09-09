//! Phase 6 Pass 3 — secret store adapters (placeholders only).

use std::sync::atomic::{AtomicU64, Ordering};
use stream_sync_core::{
    fs_secret_store, memory_secret_store, FailClosedSecretStore, KickTokenFile, OverlayConfig,
    OverlayServer, SeSession, SecretStore, TwitchTokenFile, FAIL_CLOSED_SECRET_STORE_MESSAGE,
    STREAMELEMENTS_JWT_KEY, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_CONNECTION_KEY,
    TWITCH_PERSONAL_ACCESS_KEY,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> std::path::PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "streamsync-secret-store-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf()
}

#[test]
fn memory_secret_store_roundtrip_and_delete() {
    let store = memory_secret_store();
    assert!(store.get("missing").unwrap().is_none());
    store.set("k", b"ssk_test_placeholder_mem").unwrap();
    assert_eq!(
        store.get("k").unwrap().as_deref(),
        Some(b"ssk_test_placeholder_mem".as_slice())
    );
    store.delete("k").unwrap();
    assert!(store.get("k").unwrap().is_none());
}

#[cfg(not(windows))]
#[test]
fn fail_closed_store_refuses_write_on_non_windows() {
    let dir = test_dir();
    let store = FailClosedSecretStore;
    let err = store
        .set("twitch.personal.access_token", b"ssk_test_placeholder")
        .unwrap_err();
    assert!(err.to_string().contains(FAIL_CLOSED_SECRET_STORE_MESSAGE));
    let leftover: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(leftover.is_empty(), "fail-closed must not create files");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn personal_twitch_save_does_not_serialize_tokens_to_json() {
    let userdata = test_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    *state.personal_tokens.write().await = TwitchTokenFile {
        access_token: Some("ssk_test_placeholder_twitch_access".into()),
        refresh_token: Some("ssk_test_placeholder_twitch_refresh".into()),
        login: Some("ssk_test_placeholder_login".into()),
        user_id: Some("0".into()),
        ..Default::default()
    };
    state.save_twitch_tokens().await.unwrap();
    let raw = std::fs::read_to_string(state.paths.twitch_tokens.clone()).unwrap();
    assert!(!raw.contains("ssk_test_placeholder_twitch_access"));
    assert!(!raw.contains("ssk_test_placeholder_twitch_refresh"));
    assert!(raw.contains("ssk_test_placeholder_login"));
    let store = fs_secret_store(&userdata);
    let got = store.get(TWITCH_PERSONAL_ACCESS_KEY).unwrap();
    assert_eq!(
        got.as_deref(),
        Some(b"ssk_test_placeholder_twitch_access".as_slice())
    );
    let restarted = OverlayServer::new(OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    })
    .build_app()
    .await
    .unwrap()
    .1;
    let reloaded = restarted.personal_tokens.read().await.clone();
    assert_eq!(
        reloaded.access_token.as_deref(),
        Some("ssk_test_placeholder_twitch_access")
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn personal_kick_save_does_not_serialize_tokens_to_json() {
    let userdata = test_dir();
    let (_router, state, _svc) = OverlayServer::new(OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    })
    .build_app()
    .await
    .unwrap();
    *state.personal_kick.write().await = KickTokenFile {
        access_token: Some("ssk_test_placeholder_kick_access".into()),
        refresh_token: Some("ssk_test_placeholder_kick_refresh".into()),
        feed_ticket: Some("ssk_test_placeholder_feed".into()),
        kick_id: Some("0".into()),
        login: Some("ssk_test_placeholder_kick_login".into()),
        ..Default::default()
    };
    state.save_kick_tokens().await.unwrap();
    let raw = std::fs::read_to_string(state.paths.kick_tokens.clone()).unwrap();
    assert!(!raw.contains("ssk_test_placeholder_kick_access"));
    assert!(!raw.contains("ssk_test_placeholder_kick_refresh"));
    assert!(!raw.contains("ssk_test_placeholder_feed"));
    let _ = std::fs::remove_dir_all(&userdata);
}

#[test]
fn streamelements_session_json_does_not_contain_jwt() {
    let userdata = test_dir();
    let paths = stream_sync_core::paths_for_root(&userdata, false).unwrap();
    let store = fs_secret_store(&userdata);
    let session = SeSession {
        jwt: "jwt-test-placeholder".into(),
        account_id: "0".into(),
        username: Some("se_user".into()),
        captured_at: None,
    };
    stream_sync_core::se_save_session(&paths, store.as_ref(), &session).unwrap();
    let raw = std::fs::read_to_string(userdata.join("streamelements-session.json")).unwrap();
    assert!(!raw.contains("jwt-test-placeholder"));
    assert!(raw.contains("\"accountId\""));
    let loaded = stream_sync_core::se_load_session(&paths, store.as_ref(), false)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.jwt, "jwt-test-placeholder");
    stream_sync_core::se_clear_session(&paths, store.as_ref()).unwrap();
    assert!(store.get(STREAMELEMENTS_JWT_KEY).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn delegated_persist_does_not_serialize_connection_key_or_tokens() {
    let userdata = test_dir();
    let (_router, state, _svc) = OverlayServer::new(OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    })
    .build_app()
    .await
    .unwrap();
    let session = stream_sync_core::DelegatedSessionFile {
        generation: 1,
        connection_key: "ssk_test_placeholder_conn".into(),
        client_id: "cid".into(),
        access_token: "ssk_test_placeholder_delegated_at".into(),
        channel_login: "takeover_chan".into(),
        channel_twitch_id: "999".into(),
        twitch_expires_at: "2099-01-01T00:00:00Z".into(),
        kick_access_token: Some("ssk_test_placeholder_delegated_kick_at".into()),
        kick_refresh_token: Some("ssk_test_placeholder_delegated_kick_rt".into()),
        ..Default::default()
    };
    state.persist_delegated_session(&session).unwrap();
    let raw = std::fs::read_to_string(&state.paths.twitch_delegated).unwrap();
    assert!(!raw.contains("ssk_test_placeholder_conn"));
    assert!(!raw.contains("ssk_test_placeholder_delegated_at"));
    assert!(!raw.contains("ssk_test_placeholder_delegated_kick_at"));
    assert!(!raw.contains("ssk_test_placeholder_delegated_kick_rt"));
    let store = fs_secret_store(&userdata);
    assert_eq!(
        store
            .get(TWITCH_DELEGATED_CONNECTION_KEY)
            .unwrap()
            .as_deref(),
        Some(b"ssk_test_placeholder_conn".as_slice())
    );
    state.durable_revoke_delegated().await.unwrap();
    assert!(store
        .get(TWITCH_DELEGATED_CONNECTION_KEY)
        .unwrap()
        .is_none());
    assert!(store
        .get(TWITCH_DELEGATED_ACCESS_TOKEN_KEY)
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}
