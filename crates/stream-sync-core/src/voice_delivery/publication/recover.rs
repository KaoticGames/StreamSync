//! Deterministic recovery from authoritative ledger + observed stage/final layout.

use super::error::PublicationError;
use super::publish::{
    finalize_from_final_only, observe_publication_presence, publish_prepared,
    verify_final_session_directory, PreparedPublication, PublicationOperationRecorder,
    PublishOptions,
};
use crate::voice_delivery::marker::prove_post_marker_membership;
use crate::voice_delivery::records::ledger_generation::{LedgerGeneration, LedgerStore};
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;

pub const QUARANTINE_AMBIGUOUS: &str = "ambiguous_publication";
pub const QUARANTINE_MISSING: &str = "missing_publication";
pub const QUARANTINE_PUBLISHED_AMBIGUOUS: &str = "published_with_staging_present";
pub const QUARANTINE_PUBLISHED_FINAL_INVALID: &str = "published_final_invalid";

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
    let presence = observe_publication_presence(final_parent, identity)?;

    match highest.as_ref().map(|h| h.state) {
        None | Some(LedgerState::Receiving) => {
            if highest.is_some() {
                return Ok(RecoveryOutcome::ResumeIngest);
            }
            Ok(RecoveryOutcome::ResumeIngest)
        }
        Some(LedgerState::Sealed) => {
            let staging = guard
                .final_parent_dir()
                .open_child_dir(&identity.staging_token)
                .map_err(PublicationError::Fs)?;
            prove_post_marker_membership(&staging, manifest, identity)?;
            let intent = store.commit(guard, LedgerState::PublishIntent)?;
            Ok(RecoveryOutcome::AdvancedToPublishIntent(intent))
        }
        Some(LedgerState::PublishIntent) => recover_publish_intent(
            guard,
            &store,
            presence,
            recorder,
            PublishOptions { crash: None },
        ),
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
    recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
) -> Result<RecoveryOutcome, PublicationError> {
    match (presence.stage_present, presence.final_present) {
        (true, true) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_AMBIGUOUS,
            presence.stage_present,
            presence.final_present,
        )?)),
        (false, false) => Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_MISSING,
            presence.stage_present,
            presence.final_present,
        )?)),
        (true, false) => {
            let prepared = PreparedPublication::from_guard(guard);
            let published = publish_prepared(prepared, recorder, options)?;
            Ok(RecoveryOutcome::Published(published))
        }
        (false, true) => {
            let published = finalize_from_final_only(guard, recorder, options)?;
            Ok(RecoveryOutcome::Published(published))
        }
    }
}

fn recover_published(
    guard: &DeliverySessionGuard,
    store: &LedgerStore,
    presence: super::publish::PublicationPresence,
) -> Result<RecoveryOutcome, PublicationError> {
    if presence.stage_present {
        return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_PUBLISHED_AMBIGUOUS,
            presence.stage_present,
            presence.final_present,
        )?));
    }
    if !presence.final_present {
        return Ok(RecoveryOutcome::Quarantined(quarantine_idempotent(
            store,
            guard,
            QUARANTINE_PUBLISHED_FINAL_INVALID,
            presence.stage_present,
            presence.final_present,
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
            presence.stage_present,
            presence.final_present,
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
