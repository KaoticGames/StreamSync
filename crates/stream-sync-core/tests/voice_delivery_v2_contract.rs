//! Contract fixture aligned with Syndicate `@syndicate/voice-delivery-v2-protocol` at `70b50a07`.

use stream_sync_core::voice_delivery::{
    compute_finalized_manifest_digest, parse_pending_response, parse_syndicate_finalized_manifest,
};

#[test]
fn pending_contract_fixture_matches_syndicate_golden_digest() {
    let raw = include_str!("fixtures/voice_delivery_v2_pending_contract.json");
    let body: serde_json::Value = serde_json::from_str(raw).expect("fixture json");
    let pending = parse_pending_response(&body).expect("strict pending parse");
    assert_eq!(pending.len(), 1);
    let item = &pending[0];
    assert_eq!(
        item.manifest_digest,
        "96881d02814a25a59b548b0ef30a4da3c0109f6371bc4b6ee23382447485168e"
    );
    let computed = compute_finalized_manifest_digest(&item.manifest);
    assert_eq!(computed, item.manifest_digest);
    let reparsed =
        parse_syndicate_finalized_manifest(&body["deliveries"][0]["manifest"]).expect("manifest");
    assert_eq!(
        compute_finalized_manifest_digest(&reparsed),
        item.manifest_digest
    );
}
