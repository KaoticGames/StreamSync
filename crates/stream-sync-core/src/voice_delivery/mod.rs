//! Phase 0C durable voice delivery + Phase 5 finalized-stem host orchestration.

mod api_binding;
mod bounds;
mod client;
mod finalized_manifest;
mod hash;
mod identity;
mod ingest;
pub mod local_recovery;
pub use local_recovery::LocalRecoverySweepOutcome;
mod manifest;
mod marker;
mod orchestrator;
mod publication;
mod receipt_state;
mod records;
mod session;
mod state;
mod wav;

mod ids;
mod lock;

pub use client::parse_pending_response;
pub use client::{
    validate_stem_range_meta, HttpVoiceV2Client, MockVoiceV2Client, PendingDelivery,
    ReceiptRequestBody, VoiceV2Client, VoiceV2ClientError,
};
pub use finalized_manifest::{
    compute_finalized_manifest_digest, parse_syndicate_finalized_manifest,
    SyndicateFinalizedManifest,
};
pub use ingest::{PartialError, PartialStemWriter};
pub use orchestrator::{DeliveryPhase, FinalizedVoiceDelivery, OrchestratorError};

pub(crate) mod fs;

#[allow(unused_imports)]
pub(crate) use fs::{DestRoot, DirHandle, FinalParentPublication, FsError};
#[allow(unused_imports)]
pub(crate) use ids::{lock_file_relative_components, stable_lock_file_basename};
#[allow(unused_imports)]
pub(crate) use lock::{DeliveryDomainLock, LockError};

#[cfg(test)]
mod subprocess_env {
    pub const LOCK_HOLDER: &str = "STREAMSYNC_VOICE_LOCK_HOLDER";
    pub const LOCK_TRY: &str = "STREAMSYNC_VOICE_LOCK_TRY";
    pub const LOCK_READY: &str = "STREAMSYNC_VOICE_LOCK_READY";
    pub const LOCK_RACE_CHILD: &str = "STREAMSYNC_VOICE_LOCK_RACE_CHILD";
    pub const LOCK_RACE_READY_DIR: &str = "STREAMSYNC_VOICE_LOCK_RACE_READY_DIR";
    pub const LOCK_RACE_GO: &str = "STREAMSYNC_VOICE_LOCK_RACE_GO";
    pub const LOCK_RACE_OUTCOME_DIR: &str = "STREAMSYNC_VOICE_LOCK_RACE_OUTCOME_DIR";
    pub const LOCK_RACE_RELEASE: &str = "STREAMSYNC_VOICE_LOCK_RACE_RELEASE";
    pub const PUB_CRASH_CHILD: &str = "STREAMSYNC_VOICE_PUB_CRASH_CHILD";
    pub const PUB_CRASH_POINT: &str = "STREAMSYNC_VOICE_PUB_CRASH_POINT";
    pub const PUB_CRASH_READY: &str = "STREAMSYNC_VOICE_PUB_CRASH_READY";
    pub const PUB_CRASH_GO: &str = "STREAMSYNC_VOICE_PUB_CRASH_GO";
    pub const PUB_CRASH_TMP: &str = "STREAMSYNC_VOICE_PUB_CRASH_TMP";
    pub const PUB_CRASH_OUTCOME: &str = "STREAMSYNC_VOICE_PUB_CRASH_OUTCOME";
    pub const PUB_CRASH_ABORT_MARKER: &str = "STREAMSYNC_VOICE_PUB_CRASH_ABORT_MARKER";
}
