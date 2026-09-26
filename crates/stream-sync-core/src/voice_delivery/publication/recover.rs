//! Deterministic recovery from authoritative ledger + observed stage/final layout.

use super::error::PublicationError;
use super::publish::{
    finalize_from_final_only, observe_publication_presence, publish_prepared,
    verify_final_session_directory, PreparedPublication, PresenceSlot,
    PublicationOperationRecorder, PublishOptions,
};
use crate::voice_delivery::marker::verify_marker_directory_membership;
use crate::voice_delivery::records::ledger_generation::{LedgerGeneration, LedgerStore};
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;

pub const QUARANTINE_AMBIGUOUS: &str = "ambiguous_publication";
pub const QUARANTINE_MISSING: &str = "missing_publication";
pub const QUARANTINE_PUBLISHED_AMBIGUOUS: &str = "published_with_staging_present";
pub const QUARANTINE_PUBLISHED_FINAL_INVALID: &str = "published_final_invalid";
pub const QUARANTINE_PROBE_ARTIFACT: &str = "publication_child_artifact";
pub const QUARANTINE_SEALED_INVALID: &str = "sealed_staging_invalid";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    ResumeIngest,
    AdvancedToPublishIntent(LedgerGeneration),
    Published(LedgerGeneration),
    IdempotentPublished,
    Quarantined(LedgerGeneration),
}

pub fn recover_delivery(
    guard: &DeliverySessionGuard,
    recorder: Option<&mut PublicationOperationRecorder>,
) -> Result<RecoveryOutcome, PublicationError> {
    let store = LedgerStore::open_for_guard(guard)?;
    let highest = store.read_highest_valid()?;
    let identity = guard.identity();
    let manifest = guard.manifest();
    let final_parent = guard.final_parent_dir();
    let presence = match observe_publication_presence(final_parent, identity) {
        Ok(p) => p,
        Err(PublicationError::OtherArtifact { .. }) => {
            return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
                &store,
                guard,
                QUARANTINE_PROBE_ARTIFACT,
                false,
                false,
            )?));
        }
        Err(e) => return Err(e),
    };

    match highest.as_ref().map(|h| h.state) {
        None | Some(LedgerState::Receiving) => Ok(RecoveryOutcome::ResumeIngest),
        Some(LedgerState::Sealed) => {
            let staging = match &presence.stage {
                PresenceSlot::Directory(h) => h,
                PresenceSlot::Missing => {
                    return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
                        &store,
                        guard,
                        QUARANTINE_MISSING,
                        false,
                        presence.final_present(),
                    )?));
                }
            };
            if verify_marker_directory_membership(staging, manifest, identity).is_err() {
                return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
                    &store,
                    guard,
                    QUARANTINE_SEALED_INVALID,
                    true,
                    presence.final_present(),
                )?));
            }
            let intent = store.commit(guard, LedgerState::PublishIntent)?;
            Ok(RecoveryOutcome::AdvancedToPublishIntent(intent))
        }
        Some(LedgerState::PublishIntent) => {
            recover_publish_intent(guard, &store, presence, recorder, PublishOptions::default())
        }
        Some(LedgerState::Published) => recover_published(guard, &store, presence),
        Some(LedgerState::Quarantined) => {
            let q = highest.expect("quarantined");
            Ok(RecoveryOutcome::Quarantined(q))
        }
    }
}

fn recover_publish_intent(
    guard: &DeliverySessionGuard,
    store: &LedgerStore,
    presence: super::publish::PublicationPresence,
    mut recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
) -> Result<RecoveryOutcome, PublicationError> {
    match (presence.stage_present(), presence.final_present()) {
        (true, true) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_AMBIGUOUS,
            true,
            true,
        )?)),
        (false, false) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_MISSING,
            false,
            false,
        )?)),
        (true, false) => {
            let prepared = PreparedPublication::from_guard(guard);
            match publish_prepared(prepared, recorder, options) {
                Ok(published) => Ok(RecoveryOutcome::Published(published)),
                Err(PublicationError::AmbiguousPublication) => Ok(RecoveryOutcome::Quarantined(
                    quarantine_idempotent(store, guard, QUARANTINE_AMBIGUOUS, true, true)?,
                )),
                Err(e) => Err(e),
            }
        }
        (false, true) => {
            if let Some(r) = recorder.as_mut() {
                r.record(super::publish::PublicationOperation::PublishIntentObserved);
            }
            match finalize_from_final_only(guard, recorder, options) {
                Ok(published) => Ok(RecoveryOutcome::Published(published)),
                Err(_) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
                    store,
                    guard,
                    QUARANTINE_PUBLISHED_FINAL_INVALID,
                    false,
                    true,
                )?)),
            }
        }
    }
}

fn recover_published(
    guard: &DeliverySessionGuard,
    store: &LedgerStore,
    presence: super::publish::PublicationPresence,
) -> Result<RecoveryOutcome, PublicationError> {
    if presence.stage_present() {
        return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_PUBLISHED_AMBIGUOUS,
            true,
            presence.final_present(),
        )?));
    }
    if !presence.final_present() {
        return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_PUBLISHED_FINAL_INVALID,
            false,
            false,
        )?));
    }
    let identity = guard.identity();
    let manifest = guard.manifest();
    match verify_final_session_directory(guard.final_parent_dir(), identity, manifest) {
        Ok(()) => Ok(RecoveryOutcome::IdempotentPublished),
        Err(_) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_PUBLISHED_FINAL_INVALID,
            false,
            true,
        )?)),
    }
}

/// Repeated recovery of the same ambiguity does not allocate unbounded generations.
fn quarantine_idempotent(
    store: &LedgerStore,
    guard: &DeliverySessionGuard,
    reason: &str,
    observed_stage_present: bool,
    observed_final_present: bool,
) -> Result<LedgerGeneration, PublicationError> {
    if let Some(prior) = store.read_highest_valid()? {
        if prior.state == LedgerState::Quarantined
            && prior.quarantine_reason.as_deref() == Some(reason)
            && prior.observed_stage_present == Some(observed_stage_present)
            && prior.observed_final_present == Some(observed_final_present)
        {
            return Ok(prior);
        }
    }
    store
        .commit_quarantined(
            guard,
            reason,
            observed_stage_present,
            observed_final_present,
        )
        .map_err(PublicationError::Ledger)
}
