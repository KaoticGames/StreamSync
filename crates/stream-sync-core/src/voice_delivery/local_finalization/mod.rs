//! Local Opus journal finalization (In-Sync processor).

mod accumulator;
mod cancellation;
mod client;
mod contract;
mod journal_forensics;
mod journal_store;
mod lease_keeper;
mod local_publication;
mod manifest;
mod offline_journal_rerender;
mod offline_source_snapshot;
mod opus_decode;
mod opus_journal_timeline;
mod opus_packet_duration;
mod orchestrator;
mod packet_loss_policy;
mod paths;
#[cfg(debug_assertions)]
mod post_publication_receipt_gate;
mod processor;
mod progress;
mod receipt;
mod report;
mod rtp_generation_inference;
mod storage_preflight;
pub(crate) mod synj;

pub use accumulator::{
    lost_marker_span_samples, AccumulatorError, AccumulatorPlacementPolicy, PacketDisposition,
    ReadyPacket, UserPacketAccumulator,
};
pub use cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
pub use client::{
    HttpLocalFinalizationClient, LocalFinalizationClient, LocalPendingItem, LOCAL_PENDING_PATH,
};
pub use contract::{
    parse_lease_renew_release_body, parse_local_claim_response, parse_local_pending_response,
    LeaseGrant, LeaseProof, LEASE_GENERATION_HEADER, LEASE_TOKEN_HEADER,
};
pub use journal_forensics::{
    analyze_local_journal_session, analyze_local_journal_session_with_options,
    build_minimal_manifest_json, forensics_report_to_json, local_journal_forensics_exit_code,
    offline_journal_forensics_options, render_forensics_table, session_forensics_certified_clean,
    LocalJournalForensicsOptions, LocalJournalForensicsReport,
};
pub use journal_store::LocalJournalStore;
pub use local_publication::{resolve_published_paths, PublishedLocalOutput};
pub use manifest::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id,
    parse_local_finalization_manifest, LocalFinalizationManifest, LocalFinalizationObjectRef,
    LocalFinalizationOutputPolicy, LocalFinalizationSegment, LocalFinalizationUser,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION, LOCAL_FINALIZATION_WAV_FORMAT,
};
pub use offline_journal_rerender::{
    run_offline_journal_rerender, OfflineJournalRerenderError, OfflineJournalRerenderNetworkSeal,
    OfflineJournalRerenderOptions, OfflineJournalRerenderOutcome, OFFLINE_RECOVERY_COMPLETE_MARKER,
    OFFLINE_RECOVERY_MANIFEST_FILENAME,
};
pub use opus_decode::assert_local_opus_available;
pub use orchestrator::{
    LocalFinalizationControls, LocalFinalizationOrchestrator, LocalOrchestratorError,
};
pub use packet_loss_policy::{
    BoundedPacketLossError, BoundedPacketLossTracker, LocalPacketLossPolicySummary,
    PacketLossSource, LOCAL_PACKET_LOSS_MAX_SKIPPED, LOCAL_PACKET_LOSS_POLICY_VERSION,
};
pub use paths::{
    resolve_canonical_session_paths, validate_path_under_parent, PublishedSessionPaths,
};
#[doc(hidden)]
pub use paths::{resolve_durable_publication_authority, DurablePublicationAuthority};
#[cfg(debug_assertions)]
pub use post_publication_receipt_gate::{
    install_post_publication_receipt_gate, pause_post_publication_receipt_gate,
    PostPublicationReceiptBoundary, PostPublicationReceiptGateGuard,
};
pub use processor::LocalProcessorError;
pub use progress::{
    BoundedProgressEvents, LocalFinalizationPhase, LocalFinalizationProgressEvent,
    IN_SYNC_PROGRESS_EVENT_RETENTION_CAP,
};
pub use receipt::{
    assert_local_finalization_receipt_matches_manifest, parse_local_finalization_receipt,
    LocalFinalizationReceipt,
};
pub use storage_preflight::{bytes_to_gib, StoragePreflightFailure};
#[cfg(debug_assertions)]
pub use storage_preflight::{install_storage_preflight_injection, StoragePreflightInjectGuard};
pub use synj::{
    encode_journal_file, encode_journal_record, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
};
