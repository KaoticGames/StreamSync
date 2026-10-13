//! Recovery report range validation and mock client coverage.

use sha2::{Digest, Sha256};
use stream_sync_core::voice_delivery::{
    canonicalize_finalized_manifest_for_digest, compute_finalized_manifest_digest,
    parse_syndicate_finalized_manifest, validate_recovery_report_range_meta, MockVoiceV2Client,
    StemRangeMeta, VoiceV2Client, VoiceV2ClientError,
};

const REVIEWER_FIXTURE: &str = include_str!(
    "fixtures/voice_delivery_v2_finalized_manifest_with_recovery_report_reviewer.json"
);

#[test]
fn reviewer_fixture_bytes_and_digest_gate() {
    let digest = Sha256::digest(REVIEWER_FIXTURE.as_bytes());
    let fixture_sha256 = format!("{:x}", digest);
    assert!(!fixture_sha256.is_empty());
    let raw: serde_json::Value = serde_json::from_str(REVIEWER_FIXTURE).unwrap();
    let parsed = parse_syndicate_finalized_manifest(&raw).unwrap();
    let canonical = canonicalize_finalized_manifest_for_digest(&parsed);
    assert!(canonical.contains(r#""version":1,"recoveryReport":"#));
    assert_eq!(
        compute_finalized_manifest_digest(&parsed),
        "6c6d8f7c4b5fe3d9db6490c6b966038c72f05961ee6c3dc2daa931670efd135a"
    );
}

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
