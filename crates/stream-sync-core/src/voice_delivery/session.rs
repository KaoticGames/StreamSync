//! Proof of held delivery-domain lock bound to one destination root and identity.

use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::ids::{ledger_dir_relative_components, validate_stage_basename};
use crate::voice_delivery::lock::{acquire_delivery_domain_lock, DeliveryDomainLock, LockError};
use crate::voice_delivery::manifest::ValidatedManifest;
use thiserror::Error;

#[cfg(test)]
use crate::voice_delivery::fs::PortableParentComponent;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("delivery session identity mismatch")]
    IdentityMismatch,
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error("invalid session: {0}")]
    Invalid(String),
}

/// Holds the destination root capability, cooperative lock, and immutable delivery identity.
/// Mutating stores and writers are opened only through this guard.
pub struct DeliverySessionGuard {
    root: DestRoot,
    identity: DeliveryImmutableIdentity,
    manifest: ValidatedManifest,
    final_parent: DirHandle,
    staging: Option<DirHandle>,
    _lock: DeliveryDomainLock,
}

impl DeliverySessionGuard {
    /// Acquire lock and bind an already-validated authoritative identity + stem manifest.
    pub fn begin_bound(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        create_staging_if_missing: bool,
        try_wait_lock: bool,
    ) -> Result<Self, SessionError> {
        identity
            .assert_stem_manifest(&manifest)
            .map_err(|_| SessionError::IdentityMismatch)?;
        validate_stage_basename(&identity.staging_token)?;
        let lock = acquire_delivery_domain_lock(&root, &identity.delivery_uuid, try_wait_lock)?;
        Self::assemble(root, identity, manifest, lock, create_staging_if_missing)
    }

    /// First open for a new download (creates staging when absent).
    pub fn begin(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        try_wait_lock: bool,
    ) -> Result<Self, SessionError> {
        Self::begin_bound(root, identity, manifest, true, try_wait_lock)
    }

    /// Local/tests when authoritative API digest is unavailable (stem digest only).
    #[cfg(test)]
    pub(crate) fn begin_uniform_digest(
        root: DestRoot,
        delivery_uuid: impl Into<String>,
        manifest: ValidatedManifest,
        staging_token: impl Into<String>,
        final_parent_relative: Vec<PortableParentComponent>,
        final_session_name: &str,
        try_wait_lock: bool,
    ) -> Result<Self, SessionError> {
        let identity = DeliveryImmutableIdentity::new_bound(
            delivery_uuid,
            manifest.digest(),
            &manifest,
            staging_token,
            final_parent_relative,
            final_session_name,
        )
        .map_err(|e| SessionError::Invalid(e.to_string()))?;
        Self::begin(root, identity, manifest, try_wait_lock)
    }

    /// Reopen a delivery session for recovery without synthesizing a missing staging directory.
    pub(crate) fn begin_for_recovery(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        try_wait_lock: bool,
    ) -> Result<Self, SessionError> {
        Self::begin_bound(root, identity, manifest, false, try_wait_lock)
    }

    fn assemble(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        lock: DeliveryDomainLock,
        create_staging_if_missing: bool,
    ) -> Result<Self, SessionError> {
        let mut final_parent = root.handle().clone_handle()?;
        for comp in identity.final_parent_components() {
            final_parent = final_parent.create_or_open_child_dir(comp.as_str())?;
        }
        let staging = match final_parent.open_child_dir(&identity.staging_token) {
            Ok(dir) => Some(dir),
            Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                if create_staging_if_missing {
                    final_parent.create_child_dir(&identity.staging_token)?;
                    Some(final_parent.open_child_dir(&identity.staging_token)?)
                } else {
                    None
                }
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            root,
            identity,
            manifest,
            final_parent,
            staging,
            _lock: lock,
        })
    }

    pub fn identity(&self) -> &DeliveryImmutableIdentity {
        &self.identity
    }

    pub fn manifest(&self) -> &ValidatedManifest {
        &self.manifest
    }

    pub fn dest_root(&self) -> &DestRoot {
        &self.root
    }

    pub fn final_parent_dir(&self) -> &DirHandle {
        &self.final_parent
    }

    pub fn staging_dir(&self) -> Result<&DirHandle, SessionError> {
        self.staging
            .as_ref()
            .ok_or_else(|| SessionError::Invalid("staging directory absent".into()))
    }

    pub(crate) fn ledger_dir(&self) -> Result<DirHandle, FsError> {
        let comps = ledger_dir_relative_components(&self.identity.opaque_delivery_id);
        let mut current = self.root.handle().clone_handle()?;
        for comp in comps.iter() {
            current = current.create_or_open_child_dir(comp)?;
        }
        Ok(current)
    }

    pub(crate) fn receipt_dir(&self) -> Result<DirHandle, FsError> {
        let comps = crate::voice_delivery::ids::receipt_dir_relative_components(
            &self.identity.opaque_delivery_id,
        );
        let mut current = self.root.handle().clone_handle()?;
        for comp in comps.iter() {
            current = current.create_or_open_child_dir(comp)?;
        }
        Ok(current)
    }

    pub(crate) fn checkpoint_dir_for(
        &self,
        stem_checkpoint_key: &str,
    ) -> Result<DirHandle, FsError> {
        let comps = crate::voice_delivery::ids::checkpoint_stem_dir_relative_components(
            &self.identity.opaque_delivery_id,
            stem_checkpoint_key,
        );
        let mut current = self.root.handle().clone_handle()?;
        for comp in comps.iter() {
            current = current.create_or_open_child_dir(comp)?;
        }
        Ok(current)
    }

    pub(crate) fn assert_same_identity(
        &self,
        identity: &DeliveryImmutableIdentity,
    ) -> Result<(), SessionError> {
        if identity != &self.identity {
            return Err(SessionError::IdentityMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod delivery_bound_lock {
    use super::*;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::state::LedgerState;

    fn stem_manifest() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .unwrap()
    }

    fn stage_token(suffix: &str) -> String {
        format!(".streamsync-stage-{suffix}")
    }

    #[test]
    fn lock_for_delivery_a_cannot_commit_delivery_b_ledger() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let manifest_a = stem_manifest();
        let guard_a = DeliverySessionGuard::begin_uniform_digest(
            root,
            "delivery-a",
            manifest_a,
            stage_token("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            vec![],
            "final-a",
            true,
        )
        .unwrap();

        let root_b = DestRoot::open(tmp.path()).unwrap();
        let manifest_b = stem_manifest();
        let guard_b = DeliverySessionGuard::begin_uniform_digest(
            root_b,
            "delivery-b",
            manifest_b,
            stage_token("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            vec![],
            "final-b",
            true,
        )
        .unwrap();

        let store_b = LedgerStore::open_for_guard(&guard_b).unwrap();
        let err = store_b.commit(&guard_a, LedgerState::Receiving);
        assert!(matches!(
            err,
            Err(crate::voice_delivery::records::ledger_generation::LedgerGenerationError::IdentityMismatch)
        ));
    }
}

#[cfg(test)]
mod session_constructor_substitution {
    use super::*;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    fn manifest_a() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .unwrap()
    }

    fn manifest_b() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "b.wav".into(),
            byte_count: 44,
            sha256: "b".repeat(64),
        }])
        .unwrap()
    }

    #[test]
    fn same_uuid_different_manifest_yields_distinct_identity_and_staging_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = "shared-uuid";
        let stage_a = ".streamsync-stage-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let stage_b = ".streamsync-stage-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let digest_a = {
            let guard_a = DeliverySessionGuard::begin(
                DestRoot::open(tmp.path()).unwrap(),
                DeliveryImmutableIdentity::new_bound(
                    uuid,
                    manifest_a().digest(),
                    &manifest_a(),
                    stage_a,
                    vec![PortableParentComponent::validate("guild").unwrap()],
                    "sess-a",
                )
                .unwrap(),
                manifest_a(),
                true,
            )
            .unwrap();
            assert!(tmp.path().join("guild").join(stage_a).is_dir());
            guard_a.identity().stem_set_digest.clone()
        };
        let guard_b = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            DeliveryImmutableIdentity::new_bound(
                uuid,
                manifest_b().digest(),
                &manifest_b(),
                stage_b,
                vec![PortableParentComponent::validate("guild").unwrap()],
                "sess-b",
            )
            .unwrap(),
            manifest_b(),
            true,
        )
        .unwrap();
        assert_ne!(digest_a, guard_b.identity().stem_set_digest);
        assert_ne!(stage_a, guard_b.identity().staging_token);
        assert!(tmp.path().join("guild").join(stage_b).is_dir());
        assert!(!tmp.path().join(stage_a).exists());
    }

    #[test]
    fn staging_geometry_under_final_parent_not_dest_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef";
        let m = manifest_a();
        let guard = DeliverySessionGuard::begin(
            root,
            DeliveryImmutableIdentity::new_bound(
                "geo-test",
                m.digest(),
                &m,
                stage,
                vec![
                    PortableParentComponent::validate("guild").unwrap(),
                    PortableParentComponent::validate("channel").unwrap(),
                ],
                "published",
            )
            .unwrap(),
            m,
            true,
        )
        .unwrap();
        let expected = tmp.path().join("guild").join("channel").join(stage);
        assert!(expected.is_dir());
        assert!(!tmp.path().join(stage).exists());
        let _ = guard.final_parent_dir();
    }

    #[test]
    fn begin_bound_rejects_stem_manifest_substitution() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let m = manifest_a();
        let identity = DeliveryImmutableIdentity::new_bound(
            "sub-test",
            "c".repeat(64),
            &m,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
        )
        .unwrap();
        let err = DeliverySessionGuard::begin_bound(root, identity, manifest_b(), true, true);
        assert!(matches!(err, Err(SessionError::IdentityMismatch)));
    }
}
