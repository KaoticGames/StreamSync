//! Delegated secret representation coherence — revision-bound bundles and fail-closed load.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stream_sync_core::{
    delegated_bundle_store_key, fs_secret_store, paths_for_root, write_json, AppState,
    DelegatedSessionFile, OverlayConfig, OverlayServer, TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
    TWITCH_DELEGATED_CONNECTION_KEY, TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY,
    TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
};

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn test_userdata_dir() -> std::path::PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "streamsync-delegated-secret-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf()
}

fn restart_app_at(userdata: &std::path::Path) -> Arc<AppState> {
    let paths = paths_for_root(userdata, false).expect("paths_for_root");
    AppState::new(paths, repo_root(), 0, false, fs_secret_store(userdata))
        .expect("AppState::new")
}

fn sample_metadata_only_json(generation: u64, secret_revision: u64) -> serde_json::Value {
    serde_json::json!({
        "generation": generation,
        "secret_revision": secret_revision,
        "client_id": "cid",
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": "2099-01-01T00:00:00Z"
    })
}

fn sample_inline_legacy_json(connection_key: &str, access_token: &str) -> serde_json::Value {
    serde_json::json!({
        "generation": 1,
        "connection_key": connection_key,
        "client_id": "cid",
        "access_token": access_token,
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": "2099-01-01T00:00:00Z"
    })
}

fn sample_session() -> DelegatedSessionFile {
    DelegatedSessionFile {
        generation: 1,
        connection_key: "ssk_test_placeholder_conn".into(),
        client_id: "cid".into(),
        access_token: "ssk_test_placeholder_at".into(),
        channel_login: "takeover_chan".into(),
        channel_twitch_id: "999".into(),
        twitch_expires_at: "2099-01-01T00:00:00Z".into(),
        ..Default::default()
    }
}

fn write_delegated_bundle(
    store: &dyn stream_sync_core::SecretStore,
    revision: u64,
    connection_key: &str,
    access_token: &str,
    kick_access: Option<&str>,
    kick_refresh: Option<&str>,
) {
    let bundle = serde_json::json!({
        "revision": revision,
        "connection_key": connection_key,
        "access_token": access_token,
        "kick_access_token": kick_access,
        "kick_refresh_token": kick_refresh,
    });
    store
        .set(
            &delegated_bundle_store_key(revision),
            serde_json::to_vec(&bundle).unwrap().as_slice(),
        )
        .unwrap();
}

fn legacy_store_snapshot(store: &dyn stream_sync_core::SecretStore) -> (bool, bool, bool, bool) {
    let conn = store.get(TWITCH_DELEGATED_CONNECTION_KEY).unwrap().is_some();
    let at = store.get(TWITCH_DELEGATED_ACCESS_TOKEN_KEY).unwrap().is_some();
    let kick_at = store
        .get(TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)
        .unwrap()
        .is_some();
    let kick_rt = store
        .get(TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)
        .unwrap()
        .is_some();
    (conn, at, kick_at, kick_rt)
}

fn metadata_secret_revision(path: &std::path::Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    v.get("secret_revision").and_then(|r| r.as_u64())
}

#[tokio::test]
async fn valid_both_inline_legacy_migrates_to_bound_bundle() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(
        &paths.twitch_delegated,
        &sample_inline_legacy_json(
            "ssk_test_placeholder_conn",
            "ssk_test_placeholder_at",
        ),
    )
    .unwrap();

    let state = restart_app_at(&userdata);
    let loaded = state.delegated.read().await;
    let session = loaded.as_ref().expect("both-inline legacy must activate");
    assert_eq!(session.connection_key, "ssk_test_placeholder_conn");
    assert_eq!(session.access_token, "ssk_test_placeholder_at");
    assert!(session.secret_revision >= 1);

    let store = fs_secret_store(&userdata);
    assert!(
        store
            .get(&delegated_bundle_store_key(session.secret_revision))
            .unwrap()
            .is_some(),
        "bundle must exist at bound revision"
    );
    let raw = std::fs::read_to_string(&paths.twitch_delegated).unwrap();
    assert!(!raw.contains("ssk_test_placeholder_conn"));
    assert!(!raw.contains("ssk_test_placeholder_at"));
    assert_eq!(
        metadata_secret_revision(&paths.twitch_delegated),
        Some(session.secret_revision)
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn partial_inline_connection_key_only_rejected_without_store_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    let store = fs_secret_store(&userdata);
    store
        .set(
            TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
            b"ssk_test_placeholder_orphan_at".as_slice(),
        )
        .unwrap();
    let before = legacy_store_snapshot(store.as_ref());
    write_json(
        &paths.twitch_delegated,
        &serde_json::json!({
            "generation": 1,
            "connection_key": "ssk_test_placeholder_conn_only",
            "client_id": "cid",
            "channel_login": "takeover_chan",
            "channel_twitch_id": "999",
            "twitch_expires_at": "2099-01-01T00:00:00Z"
        }),
    )
    .unwrap();

    let state = restart_app_at(&userdata);
    assert!(state.delegated.read().await.is_none());
    let after = legacy_store_snapshot(store.as_ref());
    assert_eq!(before, after, "partial inline must not mutate secret store");
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn partial_inline_access_token_only_rejected_without_store_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    let store = fs_secret_store(&userdata);
    store
        .set(
            TWITCH_DELEGATED_CONNECTION_KEY,
            b"ssk_test_placeholder_orphan_conn".as_slice(),
        )
        .unwrap();
    let before = legacy_store_snapshot(store.as_ref());
    write_json(
        &paths.twitch_delegated,
        &serde_json::json!({
            "generation": 1,
            "access_token": "ssk_test_placeholder_at_only",
            "client_id": "cid",
            "channel_login": "takeover_chan",
            "channel_twitch_id": "999",
            "twitch_expires_at": "2099-01-01T00:00:00Z"
        }),
    )
    .unwrap();

    let state = restart_app_at(&userdata);
    assert!(state.delegated.read().await.is_none());
    let after = legacy_store_snapshot(store.as_ref());
    assert_eq!(before, after, "partial inline must not mutate secret store");
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn metadata_only_with_matching_bound_bundle_loads() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 3)).unwrap();
    let store = fs_secret_store(&userdata);
    write_delegated_bundle(
        store.as_ref(),
        3,
        "ssk_test_placeholder_conn",
        "ssk_test_placeholder_at",
        None,
        None,
    );

    let state = restart_app_at(&userdata);
    let session = state.delegated.read().await.clone().expect("must load");
    assert_eq!(session.generation, 1);
    assert_eq!(session.secret_revision, 3);
    assert_eq!(session.connection_key, "ssk_test_placeholder_conn");
    assert_eq!(session.access_token, "ssk_test_placeholder_at");
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn missing_bound_bundle_fails_closed() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 5)).unwrap();

    let state = restart_app_at(&userdata);
    assert!(state.delegated.read().await.is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn revision_mismatch_fails_closed() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 2)).unwrap();
    let store = fs_secret_store(&userdata);
    write_delegated_bundle(
        store.as_ref(),
        1,
        "ssk_test_placeholder_conn",
        "ssk_test_placeholder_at",
        None,
        None,
    );

    let state = restart_app_at(&userdata);
    assert!(state.delegated.read().await.is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn crash_after_bundle_write_before_metadata_commit_fails_closed_or_stays_coherent() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1)).unwrap();
    let store = fs_secret_store(&userdata);
    write_delegated_bundle(
        store.as_ref(),
        1,
        "ssk_test_placeholder_rev1_conn",
        "ssk_test_placeholder_rev1_at",
        None,
        None,
    );
    // Simulated crash: new bundle written but metadata still references rev 1 while bundle is rev 2.
    write_delegated_bundle(
        store.as_ref(),
        2,
        "ssk_test_placeholder_rev2_conn",
        "ssk_test_placeholder_rev2_at",
        None,
        None,
    );

    let state = restart_app_at(&userdata);
    let session = state.delegated.read().await.clone();
    match session {
        None => {}
        Some(s) => {
            assert_eq!(s.secret_revision, 1);
            assert_eq!(s.connection_key, "ssk_test_placeholder_rev1_conn");
            assert_ne!(s.connection_key, "ssk_test_placeholder_rev2_conn");
        }
    }
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn refresh_same_generation_increments_secret_revision() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let session = sample_session();
    state.persist_delegated_session(&session).unwrap();
    let rev1 = metadata_secret_revision(&state.paths.twitch_delegated).expect("rev");

    let mut refreshed = session.clone();
    refreshed.access_token = "ssk_test_placeholder_refreshed_at".into();
    refreshed.generation = 1;
    state.persist_delegated_session(&refreshed).unwrap();
    let rev2 = metadata_secret_revision(&state.paths.twitch_delegated).expect("rev2");
    assert!(rev2 > rev1);

    let store = fs_secret_store(&userdata);
    assert!(
        store.get(&delegated_bundle_store_key(rev2)).unwrap().is_some(),
        "new bundle must exist"
    );

    let restarted = restart_app_at(&userdata);
    let loaded = restarted.delegated.read().await.clone().unwrap();
    assert_eq!(loaded.generation, 1);
    assert_eq!(loaded.secret_revision, rev2);
    assert_eq!(loaded.access_token, "ssk_test_placeholder_refreshed_at");
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn restart_after_committed_winner_hydrates_delegated() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let session = sample_session();
    state.persist_delegated_session(&session).unwrap();
    *state.delegated.write().await = Some(session);

    let restarted = restart_app_at(&userdata);
    let loaded = restarted.delegated.read().await.clone().expect("winner on disk");
    assert_eq!(loaded.generation, 1);
    assert_eq!(loaded.connection_key, "ssk_test_placeholder_conn");
    assert_eq!(loaded.access_token, "ssk_test_placeholder_at");
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn revocation_removes_bound_bundle_and_legacy_keys() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let session = sample_session();
    state.persist_delegated_session(&session).unwrap();
    let rev = metadata_secret_revision(&state.paths.twitch_delegated).expect("rev");
    let store = fs_secret_store(&userdata);
    store
        .set(TWITCH_DELEGATED_CONNECTION_KEY, b"ssk_test_placeholder_legacy".as_slice())
        .unwrap();

    state.durable_revoke_delegated().await.unwrap();
    assert!(store.get(&delegated_bundle_store_key(rev)).unwrap().is_none());
    assert!(store.get(TWITCH_DELEGATED_CONNECTION_KEY).unwrap().is_none());
    assert!(store.get(TWITCH_DELEGATED_ACCESS_TOKEN_KEY).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn unversioned_legacy_store_pair_migrates_when_metadata_has_no_revision() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(
        &paths.twitch_delegated,
        &sample_metadata_only_json(1, 0),
    )
    .unwrap();
    let store = fs_secret_store(&userdata);
    store
        .set(
            TWITCH_DELEGATED_CONNECTION_KEY,
            b"ssk_test_placeholder_legacy_conn".as_slice(),
        )
        .unwrap();
    store
        .set(
            TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
            b"ssk_test_placeholder_legacy_at".as_slice(),
        )
        .unwrap();

    let state = restart_app_at(&userdata);
    let session = state.delegated.read().await.clone().expect("migrate pair");
    assert_eq!(session.connection_key, "ssk_test_placeholder_legacy_conn");
    assert_eq!(session.access_token, "ssk_test_placeholder_legacy_at");
    assert!(session.secret_revision >= 1);
    assert!(store.get(TWITCH_DELEGATED_CONNECTION_KEY).unwrap().is_none());
    assert!(store.get(TWITCH_DELEGATED_ACCESS_TOKEN_KEY).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn unversioned_legacy_store_partial_pair_fails_closed_without_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(
        &paths.twitch_delegated,
        &sample_metadata_only_json(1, 0),
    )
    .unwrap();
    let store = fs_secret_store(&userdata);
    store
        .set(
            TWITCH_DELEGATED_CONNECTION_KEY,
            b"ssk_test_placeholder_partial_conn".as_slice(),
        )
        .unwrap();
    let before = legacy_store_snapshot(store.as_ref());

    let state = restart_app_at(&userdata);
    assert!(state.delegated.read().await.is_none());
    let after = legacy_store_snapshot(store.as_ref());
    assert_eq!(before, after);
    let _ = std::fs::remove_dir_all(&userdata);
}
