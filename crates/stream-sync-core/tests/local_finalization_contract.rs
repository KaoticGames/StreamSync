use serde_json::Value;
use stream_sync_core::voice_delivery::{
    compute_local_finalization_manifest_digest, parse_local_finalization_manifest,
};

const FIXTURE_DIGEST: &str = "fc0c603e00b6656fcbf4e921bf118b941dd1a9433d5e17b0cd24b5fe05740023";

#[test]
fn local_finalization_manifest_fixture_digest_matches_chatbot() {
    let raw: Value =
        serde_json::from_str(include_str!("fixtures/local_finalization_manifest_v1.json"))
            .expect("fixture json");
    let manifest = parse_local_finalization_manifest(&raw).expect("parse");
    let digest = compute_local_finalization_manifest_digest(&manifest);
    assert_eq!(digest, FIXTURE_DIGEST);
}
