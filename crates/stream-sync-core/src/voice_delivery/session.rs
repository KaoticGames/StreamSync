//! Proof of held delivery-domain lock bound to one destination root and identity.

use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError, PortableParentComponent};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::ids::{ledger_dir_relative_components, validate_stage_basename};
use crate::voice_delivery::lock::{acquire_delivery_domain_lock, DeliveryDomainLock, LockError};
use crate::voice_delivery::manifest::ValidatedManifest;
use crate::voice_delivery::records::ledger_generation::LedgerStore;
use crate::voice_delivery::state::LedgerState;
use thiserror::Error;

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
    staging: DirHandle,
    _lock: DeliveryDomainLock,
}

impl DeliverySessionGuard {
    /// Acquire the stable delivery lock and bind root, identity, manifest, final-parent, and staging.
    pub fn begin(
        root: DestRoot,
        delivery_uuid: impl Into<String>,
        manifest: ValidatedManifest,
        staging_token: impl Into<String>,
        final_parent_relative: Vec<PortableParentComponent>,
        final_session_name: &str,
        try_wait_lock: bool,
    ) -> Result<Self, SessionError> {
        let identity = DeliveryImmutableIdentity::new(
            delivery_uuid,
            &manifest,
            staging_token,
            final_parent_relative,
            final_session_name,
        )
        .map_err(|e| SessionError::Invalid(e.to_string()))?;
        if identity.manifest_digest != manifest.digest() {
            return Err(SessionError::Invalid(
                "manifest digest does not match identity".into(),
            ));
        }
        validate_stage_basename(&identity.staging_token)?;
        let lock = acquire_delivery_domain_lock(&root, &identity.delivery_uuid, try_wait_lock)?;
        Self::assemble(root, identity, manifest, lock)
    }

    fn assemble(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        lock: DeliveryDomainLock,
    ) -> Result<Self, SessionError> {
        let mut final_parent = root.handle().clone_handle()?;
        for comp in identity.final_parent_components() {
            final_parent = final_parent.create_or_open_child_dir(comp.as_str())?;
        }
        let staging = match final_parent.open_child_dir(&identity.staging_token) {
            Ok(dir) => dir,
            Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                final_parent.create_child_dir(&identity.staging_token)?;
                final_parent.open_child_dir(&identity.staging_token)?
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

    pub fn staging_dir(&self) -> &DirHandle {
        &self.staging
    }

    pub(crate) fn ledger_dir(&self) -> Result<DirHandle, FsError> {
        let comps = ledger_dir_relative_components(&self.identity.opaque_delivery_id);
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
        let guard_a = DeliverySessionGuard::begin(
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
        let guard_b = DeliverySessionGuard::begin(
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
                uuid,
                manifest_a(),
                stage_a,
                vec![PortableParentComponent::validate("guild").unwrap()],
                "sess-a",
                true,
            )
            .unwrap();
            assert!(tmp.path().join("guild").join(stage_a).is_dir());
            guard_a.identity().manifest_digest.clone()
        };
        let guard_b = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            uuid,
            manifest_b(),
            stage_b,
            vec![PortableParentComponent::validate("guild").unwrap()],
            "sess-b",
            true,
        )
        .unwrap();
        assert_ne!(digest_a, guard_b.identity().manifest_digest);
        assert_ne!(stage_a, guard_b.identity().staging_token);
        assert!(tmp.path().join("guild").join(stage_b).is_dir());
        assert!(!tmp.path().join(stage_a).exists());
    }

    #[test]
    fn staging_geometry_under_final_parent_not_dest_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef";
        let guard = DeliverySessionGuard::begin(
            root,
            "geo-test",
            manifest_a(),
            stage,
            vec![
                PortableParentComponent::validate("guild").unwrap(),
                PortableParentComponent::validate("channel").unwrap(),
            ],
            "published",
            true,
        )
        .unwrap();
        let expected = tmp.path().join("guild").join("channel").join(stage);
        assert!(expected.is_dir());
        assert!(!tmp.path().join(stage).exists());
        let _ = guard.final_parent_dir();
    }
}
