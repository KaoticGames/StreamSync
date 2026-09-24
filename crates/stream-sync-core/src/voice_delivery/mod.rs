//! Phase 0C voice delivery filesystem foundation (slices 0–4).
//! Public API remains minimal until later phases.
//!
//! `dead_code` is allowed here until ledger/ingest slices wire these entry points.
#![allow(dead_code)]

mod ids;
mod lock;

pub(crate) mod fs;

#[allow(unused_imports)]
pub(crate) use fs::{DestRoot, DirHandle, FinalParentPublication, FsError};
#[allow(unused_imports)]
pub(crate) use ids::{lock_file_relative_components, stable_lock_file_basename};
#[allow(unused_imports)]
pub(crate) use lock::{acquire_delivery_domain_lock, DeliveryDomainLock, LockError};

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
}
