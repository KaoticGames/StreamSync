//! Recovery report range validation and mock client coverage.

use stream_sync_core::voice_delivery::{
    validate_recovery_report_range_meta, MockVoiceV2Client, StemRangeMeta, VoiceV2Client,
    VoiceV2ClientError,
};

#[test]
fn validate_recovery_report_meta_requires_sha_etag() {
    let meta = StemRangeMeta {
        status: 206,
        content_range: "bytes 0-2/3".into(),
        content_length: 3,
        etag: "\"deadbeef\"".into(),
        total_length: 3,
    };
    let sha = "deadbeef".repeat(8);
    assert!(validate_recovery_report_range_meta(&meta, 0, 3, 3, &sha).is_err());
    let meta_ok = StemRangeMeta {
        etag: format!("\"{sha}\""),
        ..meta
    };
    validate_recovery_report_range_meta(&meta_ok, 0, 3, 3, &sha).unwrap();
}

#[test]
fn mock_recovery_report_404_when_not_seeded() {
    let mock = MockVoiceV2Client::new();
    let mut out = Vec::new();
    let err = mock
        .fetch_recovery_report_range("sess", 0, 1, &mut out)
        .unwrap_err();
    assert_eq!(err, VoiceV2ClientError::RecoveryReportNotFound);
}
