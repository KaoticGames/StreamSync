//! Durable publication execution from PublishIntent.

use super::error::PublicationError;
use crate::voice_delivery::fs::durability::{sync_dir_exact, NamespaceDurability};
use crate::voice_delivery::fs::ChildDirProbe;
use crate::voice_delivery::fs::FinalParentPublication;
use crate::voice_delivery::fs::{DirHandle, FsError, ValidatedFinalName};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::manifest::ValidatedManifest;
use crate::voice_delivery::marker::verify_marker_directory_membership;
use crate::voice_delivery::records::ledger_generation::{LedgerGeneration, LedgerStore};
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;

/// Observed stage/final directory presence under the final-parent handle (fail-closed probe).
pub(crate) struct PublicationPresence {
    pub stage: PresenceSlot,
    pub final_session: PresenceSlot,
}

pub(crate) enum PresenceSlot {
    Missing,
    Directory(DirHandle),
}

impl std::fmt::Debug for PublicationPresence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationPresence")
            .field("stage", &self.stage)
            .field("final_session", &self.final_session)
            .finish()
    }
}

impl std::fmt::Debug for PresenceSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PresenceSlot::Missing => write!(f, "Missing"),
            PresenceSlot::Directory(_) => write!(f, "Directory(..)"),
        }
    }
}

impl PublicationPresence {
    pub(crate) fn stage_present(&self) -> bool {
        matches!(self.stage, PresenceSlot::Directory(_))
    }

    pub(crate) fn final_present(&self) -> bool {
        matches!(self.final_session, PresenceSlot::Directory(_))
    }
}

fn map_child_probe_err(role: &'static str, err: FsError) -> PublicationError {
    match err {
        FsError::NotADirectory(_) => PublicationError::OtherArtifact { role },
        other => PublicationError::Fs(other),
    }
}

pub(crate) fn observe_publication_presence(
    final_parent: &DirHandle,
    identity: &DeliveryImmutableIdentity,
) -> Result<PublicationPresence, PublicationError> {
    let stage = match final_parent.probe_child_dir(&identity.staging_token) {
        Ok(probe) => match probe {
            ChildDirProbe::Missing => PresenceSlot::Missing,
            ChildDirProbe::Directory(h) => PresenceSlot::Directory(h),
        },
        Err(e) => return Err(map_child_probe_err("stage", e)),
    };
    let final_session = match final_parent.probe_child_dir(&identity.final_session_name) {
        Ok(probe) => match probe {
            ChildDirProbe::Missing => PresenceSlot::Missing,
            ChildDirProbe::Directory(h) => PresenceSlot::Directory(h),
        },
        Err(e) => return Err(map_child_probe_err("final", e)),
    };
    Ok(PublicationPresence {
        stage,
        final_session,
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
        if op == PublicationOperation::PublishIntentObserved
            && self.ops.contains(&PublicationOperation::PublishIntentObserved)
        {
            return;
        }
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

#[cfg(test)]
pub(crate) type RenameInjectFn = fn(&DirHandle, &str, &ValidatedFinalName) -> Result<(), FsError>;

#[derive(Default)]
pub struct PublishOptions {
    pub crash: Option<PublicationCrashPoint>,
    #[cfg(test)]
    pub rename_inject: Option<RenameInjectFn>,
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
    if presence.stage_present() && presence.final_present() {
        return Err(PublicationError::AmbiguousPublication);
    }
    if !presence.stage_present() {
        if presence.final_present() {
            return finalize_from_final_only(guard, recorder, options, true);
        }
        return Err(PublicationError::MissingPublication);
    }

    let staging = match &presence.stage {
        PresenceSlot::Directory(h) => h,
        PresenceSlot::Missing => return Err(PublicationError::MissingPublication),
    };
    verify_marker_directory_membership(staging, manifest, identity)?;
    if let Some(r) = recorder.as_mut() {
        r.record(PublicationOperation::StagingReverified);
    }

    let final_name =
        ValidatedFinalName::validate(&identity.final_session_name).map_err(PublicationError::Fs)?;
    let rename_result =
        perform_stage_rename(final_parent, &identity.staging_token, &final_name, &options);
    match rename_result {
        Ok(()) => {}
        Err(e) => {
            return reconcile_after_rename_error(guard, recorder, options, e, manifest, identity);
        }
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

fn perform_stage_rename(
    final_parent: &DirHandle,
    stage_token: &str,
    final_name: &ValidatedFinalName,
    options: &PublishOptions,
) -> Result<(), FsError> {
    #[cfg(test)]
    if let Some(inject) = options.rename_inject {
        return inject(final_parent, stage_token, final_name);
    }
    #[cfg(not(test))]
    let _options = options;
    let publication = FinalParentPublication::new(final_parent.clone_handle()?);
    publication.rename_stage_to_final(stage_token, final_name)
}

fn rename_error_detail(err: &FsError) -> String {
    match err {
        FsError::AlreadyExists => "already_exists".into(),
        FsError::Io(e) => format!("io:{:?}", e.kind()),
        FsError::SymlinkOrReparseComponent(c) => format!("reparse:{c}"),
        FsError::NotADirectory(c) => format!("not_directory:{c}"),
        other => format!("{other}"),
    }
}

fn reconcile_after_rename_error(
    guard: &DeliverySessionGuard,
    recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
    rename_err: FsError,
    manifest: &ValidatedManifest,
    identity: &DeliveryImmutableIdentity,
) -> Result<LedgerGeneration, PublicationError> {
    let detail = rename_error_detail(&rename_err);
    let final_parent = guard.final_parent_dir();
    let presence = observe_publication_presence(final_parent, identity)?;
    match (presence.stage_present(), presence.final_present()) {
        (false, true) => {
            if verify_final_session_directory(final_parent, identity, manifest).is_ok() {
                return finalize_from_final_only(guard, recorder, options, true);
            }
            if matches!(rename_err, FsError::AlreadyExists) {
                return Err(classify_destination_collision(
                    final_parent,
                    identity,
                    manifest,
                ));
            }
            Err(PublicationError::RenameIndeterminate { detail })
        }
        (true, false) => {
            if matches!(rename_err, FsError::AlreadyExists) {
                return Err(classify_destination_collision(
                    final_parent,
                    identity,
                    manifest,
                ));
            }
            Err(PublicationError::RenameFailed { detail })
        }
        (true, true) => {
            if matches!(rename_err, FsError::AlreadyExists) {
                return Err(PublicationError::AmbiguousPublication);
            }
            Err(PublicationError::RenameIndeterminate { detail })
        }
        (false, false) => Err(PublicationError::RenameIndeterminate { detail }),
    }
}

pub(crate) fn finalize_from_final_only(
    guard: &DeliverySessionGuard,
    mut recorder: Option<&mut PublicationOperationRecorder>,
    options: PublishOptions,
    intent_already_observed: bool,
) -> Result<LedgerGeneration, PublicationError> {
    let store = LedgerStore::open_for_guard(guard)?;
    let identity = guard.identity();
    let manifest = guard.manifest();
    let final_parent = guard.final_parent_dir();
    if !intent_already_observed {
        if let Some(r) = recorder.as_mut() {
            r.record(PublicationOperation::PublishIntentObserved);
        }
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
    let presence = observe_publication_presence(final_parent, identity)?;
    let final_dir = match presence.final_session {
        PresenceSlot::Directory(h) => h,
        PresenceSlot::Missing => return Err(PublicationError::MissingPublication),
    };
    verify_marker_directory_membership(&final_dir, manifest, identity)?;
    Ok(())
}

fn classify_destination_collision(
    final_parent: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
) -> PublicationError {
    match final_parent.probe_child_dir(&identity.final_session_name) {
        Ok(ChildDirProbe::Directory(dir)) => {
            if marker_matches_delivery(&dir, identity, manifest) {
                PublicationError::DestinationExists
            } else {
                PublicationError::UnrelatedDestination
            }
        }
        Ok(ChildDirProbe::Missing) => PublicationError::DestinationExists,
        Err(FsError::SymlinkOrReparseComponent(_)) | Err(FsError::NotADirectory(_)) => {
            PublicationError::UnrelatedDestination
        }
        Err(e) => PublicationError::Fs(e),
    }
}

fn marker_matches_delivery(
    dir: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
) -> bool {
    verify_marker_directory_membership(dir, manifest, identity).is_ok()
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
    #[cfg(test)]
    {
        use crate::voice_delivery::subprocess_env::PUB_CRASH_ABORT_MARKER;
        if let Ok(path) = std::env::var(PUB_CRASH_ABORT_MARKER) {
            let _ = std::fs::write(path, b"pre_abort\n");
        }
    }
    std::process::abort();
}
