//! Production (non-test) builds must not expose processor bypass or fake Opus success.

#[test]
fn local_finalization_public_api_does_not_export_processor_entrypoints() {
    let src = include_str!("../src/voice_delivery/local_finalization/mod.rs");
    assert!(
        !src.contains("process_verified_segments"),
        "processor entrypoints must stay crate-private"
    );
}

#[test]
fn opus_decode_production_source_has_no_fake_pcm_success() {
    let src = include_str!("../src/voice_delivery/local_finalization/opus_decode.rs");
    assert!(
        src.contains("#[cfg(all(test, not(feature = \"local-opus-native\")))]"),
        "fake decoder must be test-only"
    );
    assert!(
        src.contains("#[cfg(not(any(feature = \"local-opus-native\", test)))]"),
        "production no-feature path must refuse decode"
    );
    assert!(
        src.contains("Err(OpusDecodeError::Unavailable)"),
        "decode must surface unavailable without native Opus"
    );
}

#[cfg(not(feature = "local-opus-native"))]
#[test]
fn offline_rerender_refuses_without_native_opus() {
    use std::fs;
    use stream_sync_core::voice_delivery::local_finalization::{
        analyze_local_journal_session_with_options, encode_journal_file,
        offline_journal_forensics_options, run_offline_journal_rerender,
        session_forensics_certified_clean, OfflineJournalRerenderError,
        OfflineJournalRerenderNetworkSeal, OfflineJournalRerenderOptions,
        JOURNAL_FORMAT_VERSION_V2,
    };

    let tmp = tempfile::tempdir().unwrap();
    let session_id = "550e8400-e29b-41d4-a716-446655440099";
    let session_root = tmp.path().join(session_id);
    fs::create_dir_all(session_root.join("journal/segments")).unwrap();
    let body = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
    fs::write(session_root.join("journal/segments/00000000.synj"), body).unwrap();
    let manifest =
        stream_sync_core::voice_delivery::local_finalization::build_minimal_manifest_json(
            session_id,
            &[("123456789012345678", "wrap_epoch")],
            960,
        );
    fs::write(session_root.join("manifest.json"), manifest).unwrap();
    let report = analyze_local_journal_session_with_options(
        &session_root,
        offline_journal_forensics_options(),
    )
    .unwrap();
    assert!(session_forensics_certified_clean(&report));
    let out = tmp.path().join("dest").join("recovered");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    let err = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &out,
        OfflineJournalRerenderOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(err, OfflineJournalRerenderError::OpusUnavailable));
}
