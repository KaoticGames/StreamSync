//! Windows must report namespace durability honestly (never promote `Unavailable` to `Proven`).

#[cfg(windows)]
mod honesty {
    use super::super::test_support::{minimal_synj_segment_bytes, sha256_lower_hex, SESSION_ID};
    use crate::voice_delivery::direct_local::{
        DirectCumulativeCheckpoint, DirectDurabilityReport, DirectGenerationStore,
        DirectNamespaceDurabilityProbe, DirectSegmentDurabilityPipeline,
    };
    use crate::voice_delivery::fs::{DestRoot, NamespaceDurability};

    #[test]
    fn windows_namespace_durability_reported_unavailable_not_proven() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
        let probe = DirectNamespaceDurabilityProbe::for_store(&store);
        let report = probe
            .report_after_segment_and_checkpoint_durability()
            .expect("probe");
        assert_eq!(
            report.namespace_durability,
            NamespaceDurability::Unavailable
        );
        assert!(!report.claims_proven_namespace_durability());
    }

    #[test]
    fn windows_segment_pipeline_does_not_upgrade_namespace_to_proven() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
        let bytes = minimal_synj_segment_bytes();
        let identity = store
            .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
            .expect("identity");
        let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
        let mut pipeline = DirectSegmentDurabilityPipeline::new(&store);
        let outcome = pipeline
            .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
            .expect("persist");
        assert!(outcome.cumulative_ack_eligible);
        let report: DirectDurabilityReport = pipeline.last_durability_report().expect("report");
        assert_eq!(
            report.namespace_durability,
            NamespaceDurability::Unavailable
        );
        assert_ne!(report.namespace_durability, NamespaceDurability::Proven);
    }
}
