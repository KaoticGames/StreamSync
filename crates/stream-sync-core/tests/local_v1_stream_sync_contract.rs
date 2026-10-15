//! Cross-check parsers against the canonical ChatBot StreamSync contract fixture.

use serde_json::Value;
use stream_sync_core::voice_delivery::local_finalization::{
    parse_lease_renew_release_body, parse_local_claim_response, parse_local_pending_response,
    LEASE_GENERATION_HEADER, LEASE_TOKEN_HEADER,
};

#[test]
fn canonical_local_v1_stream_sync_contract_fixture() {
    let raw = include_str!("fixtures/local_v1_stream_sync_contract.json");
    let fixture: Value = serde_json::from_str(raw).expect("fixture json");
    assert_eq!(
        fixture["leaseProofHeaders"]["generation"].as_str(),
        Some(LEASE_GENERATION_HEADER)
    );
    assert_eq!(
        fixture["leaseProofHeaders"]["token"].as_str(),
        Some(LEASE_TOKEN_HEADER)
    );

    let pending = parse_local_pending_response(&fixture["pendingResponse"]).expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sealed_at_ms, 1_704_067_200_000);

    let claim = parse_local_claim_response(&fixture["claimResponse"]).expect("claim");
    assert_eq!(claim.generation, 1);
    assert_eq!(claim.token, "cafebabecafebabecafebabecafebabe");

    parse_lease_renew_release_body(&fixture["renewRequest"]).expect("renew");
    parse_lease_renew_release_body(&fixture["releaseRequest"]).expect("release");

    let headers = fixture["leaseProofHeaderExample"]
        .as_object()
        .expect("headers");
    assert_eq!(
        headers
            .get(LEASE_GENERATION_HEADER)
            .and_then(|v| v.as_str()),
        Some("1")
    );

    let embedded_manifest = serde_json::json!({
        "ok": true,
        "deliveries": [{
            "sessionId": "fd6bbbee-040a-4892-86ff-2c4fdf30ce2c",
            "manifestDigest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "sealedAtMs": 1,
            "manifest": {}
        }]
    });
    assert!(parse_local_pending_response(&embedded_manifest).is_err());
}
