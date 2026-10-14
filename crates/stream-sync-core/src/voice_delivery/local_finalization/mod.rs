//! Local Opus journal finalization (In-Sync processor).

mod accumulator;
mod cancellation;
mod client;
mod journal_store;
mod local_publication;
mod manifest;
mod opus_decode;
mod orchestrator;
mod paths;
mod processor;
mod progress;
mod receipt;
mod report;
mod synj;

pub use cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
pub use client::{
    parse_local_pending_response, HttpLocalFinalizationClient, LocalFinalizationClient,
    LocalPendingDelivery, LOCAL_PENDING_PATH,
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
