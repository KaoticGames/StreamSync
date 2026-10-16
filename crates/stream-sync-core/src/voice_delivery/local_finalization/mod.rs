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
mod opus_decode;
mod orchestrator;
mod paths;
mod processor;
mod progress;
mod receipt;
mod report;
mod storage_preflight;
mod synj;

pub use cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
pub use client::{
    HttpLocalFinalizationClient, LocalFinalizationClient, LocalPendingItem, LOCAL_PENDING_PATH,
};
pub use contract::{
    parse_lease_renew_release_body, parse_local_claim_response, parse_local_pending_response,
    LeaseGrant, LeaseProof, LEASE_GENERATION_HEADER, LEASE_TOKEN_HEADER,
};
pub use journal_forensics::{
    analyze_local_journal_session, build_minimal_manifest_json, forensics_report_to_json,
    render_forensics_table, LocalJournalForensicsReport,
};
pub use journal_store::LocalJournalStore;
pub use local_publication::{resolve_published_paths, PublishedLocalOutput};
pub use manifest::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id,
    parse_local_finalization_manifest, LocalFinalizationManifest,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
};
pub use opus_decode::assert_local_opus_available;
pub use orchestrator::{
    LocalFinalizationControls, LocalFinalizationOrchestrator, LocalOrchestratorError,
};
pub use paths::{
    resolve_canonical_session_paths, validate_path_under_parent, PublishedSessionPaths,
};
pub use progress::{LocalFinalizationPhase, LocalFinalizationProgressEvent};
pub use receipt::{
    assert_local_finalization_receipt_matches_manifest, parse_local_finalization_receipt,
    LocalFinalizationReceipt,
};
pub use storage_preflight::{bytes_to_gib, StoragePreflightFailure};
pub use synj::{
    encode_journal_file, encode_journal_record, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
};
