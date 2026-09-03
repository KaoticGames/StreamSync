//! StreamElements session file I/O (no live SE API).

use stream_sync_core::{
    paths_for_root, se_clear_session, se_load_session, se_save_session, SeSession,
};

#[test]
fn se_session_save_load_clear() {
    let dir = std::env::temp_dir().join(format!(
        "stream-sync-se-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");

    let paths = paths_for_root(&dir, false).expect("paths");
    let session = SeSession {
        jwt: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0In0.test".into(),
        account_id: "account123".into(),
        username: None,
        captured_at: Some("2026-01-01T00:00:00Z".into()),
    };
    se_save_session(&paths, &session).expect("save");
    let loaded = se_load_session(&paths).expect("load");
    assert_eq!(
        loaded.as_ref().map(|s| s.account_id.as_str()),
        Some("account123")
    );
    se_clear_session(&paths).expect("clear");
    assert!(se_load_session(&paths).expect("load2").is_none());

    let _ = std::fs::remove_dir_all(&dir);
}
