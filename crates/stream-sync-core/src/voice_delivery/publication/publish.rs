//! Durable publication execution from PublishIntent.

use super::error::PublicationError;
use crate::voice_delivery::fs::durability::{sync_dir_exact, NamespaceDurability};
use crate::voice_delivery::fs::FinalParentPublication;
use crate::voice_delivery::fs::{DirHandle, FsError, ValidatedFinalName};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::manifest::ValidatedManifest;
use crate::voice_delivery::marker::DeliveryMarker;
use crate::voice_delivery::marker::{prove_post_marker_membership, MARKER_FILENAME};
use crate::voice_delivery::records::ledger_generation::{LedgerGeneration, LedgerStore};
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;

/// Observed stage/final directory presence under the final-parent handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublicationPresence {
    pub stage_present: bool,
    pub final_present: bool,
}

pub(crate) fn observe_publication_presence(
    final_parent: &DirHandle,
    identity: &DeliveryImmutableIdentity,
) -> Result<PublicationPresence, FsError> {
    let stage_present = final_parent.open_child_dir(&identity.staging_token).is_ok();
    let final_present = final_parent
        .open_child_dir(&identity.final_session_name)
        .is_ok();
    Ok(PublicationPresence {
        stage_present,
        final_present,
    })
}

/// Operation order witness for fault-injection tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationOperation {
    PublishIntentObserved,
    StagingReverified,
    RenameNoReplace,
    ParentNamespaceDurability,
    FinalReopenVerify,
    PublishedCommitted,
}

#[derive(Default)]
pub struct PublicationOperationRecorder {
    pub ops: Vec<PublicationOperation>,
}

impl PublicationOperationRecorder {
    pub fn record(&mut self, op: PublicationOperation) {
        self.ops.push(op);
    }
}

/// Typestate token: authoritative ledger is PublishIntent and identity matches the guard.
pub struct PreparedPublication<'guard> {
    guard: &'guard DeliverySessionGuard,
}

impl<'guard> PreparedPublication<'guard> {
    pub(crate) fn guard(&self) -> &'guard DeliverySessionGuard {
        self.guard
    }

    pub(crate) fn from_guard(guard: &'guard DeliverySessionGuard) -> Self {
        Self { guard }
    }
}

/// Load durable PublishIntent under the held lock and bind publication authority to the guard.
pub fn prepare_publication(
    guard: &DeliverySessionGuard,
) -> Result<PreparedPublication<'_>, PublicationError> {
    let store = LedgerStore::open_for_guard(guard)?;
    let highest = store
        .read_highest_valid()?
        .ok_or(PublicationError::NotPublishIntent)?;
    if highest.state != LedgerState::PublishIntent {
        return Err(PublicationError::NotPublishIntent);
    }
    Ok(PreparedPublication { guard })
}

/// Crash injection boundaries for child-process hard-crash tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationCrashPoint {
    AfterPublishIntentBeforeRename,
    AfterRenameBeforeParentSync,
    AfterParentSyncBeforeFinalVerify,
    AfterFinalVerifyBeforePublished,
    DuringPublishedLedgerWrite,
}

pub struct PublishOptions {
    pub crash: Option<PublicationCrashPoint>,
}

/// Execute publication rename + verification + Published ledger commit.
pub fn publish_prepared(
    prepared: PreparedPublication<'_>,
    mut recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
) -> Result<LedgerGeneration, PublicationError> {
    let guard = prepared.guard;
    let store = LedgerStore::open_for_guard(guard)?;
    let highest = store.read_highest_valid()?.expect("prepare invariant");
    if highest.state != LedgerState::PublishIntent {
        return Err(PublicationError::NotPublishIntent);
    }
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::PublishIntentObserved);
    }

    if options.crash == Some(PublicationCrashPoint::AfterPublishIntentBeforeRename) {
        hard_abort();
    }

    let identity = guard.identity();
    let manifest = guard.manifest();
    let final_parent = guard.final_parent_dir();
    let presence = observe_publication_presence(final_parent, identity)?;
    if presence.stage_present && presence.final_present {
        return Err(PublicationError::AmbiguousPublication);
    }
    if !presence.stage_present {
        if presence.final_present {
            return finalize_from_final_only(guard, recorder, options);
        }
        return Err(PublicationError::MissingPublication);
    }

    let staging = final_parent
        .open_child_dir(&identity.staging_token)
        .map_err(PublicationError::Fs)?;
    prove_post_marker_membership(&staging, manifest, identity)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::StagingReverified);
    }

    let final_name = ValidatedFinalName::validate(&identity.final_session_name)
        .map_err(PublicationError::Fs)?;
    let publication = FinalParentPublication::new(final_parent.clone_handle()?);
    match publication.rename_stage_to_final(&identity.staging_token, &final_name) {
        Ok(()) => {}
        Err(FsError::AlreadyExists) => {
            return Err(classify_destination_collision(
                final_parent,
                identity,
                manifest,
            ));
        }
        Err(e) => return Err(PublicationError::Fs(e)),
    }
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::RenameNoReplace);
    }

    if options.crash == Some(PublicationCrashPoint::AfterRenameBeforeParentSync) {
        hard_abort();
    }

    let namespace_durability = sync_dir_exact(final_parent)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::ParentNamespaceDurability);
    }

    if options.crash == Some(PublicationCrashPoint::AfterParentSyncBeforeFinalVerify) {
        hard_abort();
    }

    verify_final_session_directory(final_parent, identity, manifest)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::FinalReopenVerify);
    }

    if options.crash == Some(PublicationCrashPoint::AfterFinalVerifyBeforePublished) {
        hard_abort();
    }

    let published =
        commit_published_with_optional_crash(&store, guard, namespace_durability, options.crash)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::PublishedCommitted);
    }
    Ok(published)
}

pub(crate) fn finalize_from_final_only(
    guard: &DeliverySessionGuard,
    mut recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
) -> Result<LedgerGeneration, PublicationError> {
    let store = LedgerStore::open_for_guard(guard)?;
    let identity = guard.identity();
    let manifest = guard.manifest();
    let final_parent = guard.final_parent_dir();
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::PublishIntentObserved);
    }
    let namespace_durability = sync_dir_exact(final_parent)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::ParentNamespaceDurability);
    }
    verify_final_session_directory(final_parent, identity, manifest)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::FinalReopenVerify);
    }
    if options.crash == Some(PublicationCrashPoint::AfterFinalVerifyBeforePublished) {
        hard_abort();
    }
    let published =
        commit_published_with_optional_crash(&store, guard, namespace_durability, options.crash)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::PublishedCommitted);
    }
    Ok(published)
}

pub(crate) fn verify_final_session_directory(
    final_parent: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
) -> Result<(), PublicationError> {
    let final_dir = final_parent
        .open_child_dir(&identity.final_session_name)
        .map_err(PublicationError::Fs)?;
    prove_post_marker_membership(&final_dir, manifest, identity)?;
    Ok(())
}

fn classify_destination_collision(
    final_parent: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
) -> PublicationError {
    match final_parent.open_child_dir(&identity.final_session_name) {
        Ok(dir) => {
            if marker_matches_delivery(&dir, identity, manifest) {
                PublicationError::DestinationExists
            } else {
                PublicationError::UnrelatedDestination
            }
        }
        Err(FsError::AlreadyExists) | Err(FsError::SymlinkOrReparseComponent(_)) => {
            PublicationError::UnrelatedDestination
        }
        Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotADirectory => {
            PublicationError::UnrelatedDestination
        }
        Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            PublicationError::DestinationExists
        }
        Err(e) => PublicationError::Fs(e),
    }
}

fn marker_matches_delivery(
    dir: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
) -> bool {
    match dir.read_file_all(MARKER_FILENAME) {
        Ok(bytes) => DeliveryMarker::parse(&bytes, identity, manifest).is_ok(),
        Err(_) => false,
    }
}

fn commit_published_with_optional_crash(
    store: &LedgerStore,
    guard: &DeliverySessionGuard,
    namespace_durability: NamespaceDurability,
    crash: Option<PublicationCrashPoint>,
) -> Result<LedgerGeneration, PublicationError> {
    if crash == Some(PublicationCrashPoint::DuringPublishedLedgerWrite) {
        write_malformed_higher_generation_and_abort(store);
    }
    store
        .commit_published(guard, namespace_durability)
        .map_err(PublicationError::Ledger)
}

fn write_malformed_higher_generation_and_abort(store: &LedgerStore) -> ! {
    use crate::voice_delivery::records::generation_read::allocate_next_generation;
    let generation = allocate_next_generation(&store.dir).unwrap_or(999);
    let name = format!("gen-{generation}.json");
    if let Ok(mut file) = store.dir.create_new_file(&name) {
        let _ = file.write_all_at(0, b"{\"state\":\"published\",\"incomplete\":");
    }
    hard_abort();
}

pub(crate) fn hard_abort() -> ! {
    std::process::abort();
}
