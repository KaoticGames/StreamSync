//! Local Opus journal finalization (In-Sync processor).

mod cancellation;
mod client;
mod journal_store;
mod manifest;
mod opus_decode;
mod orchestrator;
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
pub use manifest::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id,
    parse_local_finalization_manifest, LocalFinalizationManifest,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
};
pub use orchestrator::{LocalFinalizationOrchestrator, LocalOrchestratorError};
pub use progress::{LocalFinalizationPhase, LocalFinalizationProgressEvent};
pub use receipt::{
    assert_local_finalization_receipt_matches_manifest, parse_local_finalization_receipt,
    LocalFinalizationReceipt,
};
