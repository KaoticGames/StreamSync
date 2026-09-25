//! Proof of held delivery-domain lock bound to one destination root and identity.

use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::ids::{ledger_dir_relative_components, validate_stage_basename};
use crate::voice_delivery::lock::DeliveryDomainLock;
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
    #[error("invalid session: {0}")]
    Invalid(String),
}

/// Holds the destination root capability, cooperative lock, and immutable delivery identity.
/// Mutating stores and writers are opened only through this guard.
pub struct DeliverySessionGuard {
    root: DestRoot,
    identity: DeliveryImmutableIdentity,
    manifest: ValidatedManifest,
    staging: DirHandle,
    _lock: DeliveryDomainLock,
}

impl DeliverySessionGuard {
    pub fn open(
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        lock: DeliveryDomainLock,
    ) -> Result<Self, SessionError> {
        if identity.manifest_digest != manifest.digest() {
            return Err(SessionError::Invalid(
                "manifest digest does not match identity".into(),
            ));
        }
        validate_stage_basename(&identity.staging_token)?;
        let staging = match root.open_child_dir(&identity.staging_token) {
            Ok(dir) => dir,
            Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                root.create_child_dir(&identity.staging_token)?;
                root.open_child_dir(&identity.staging_token)?
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            root,
            identity,
            manifest,
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

    pub(crate) fn assert_same_delivery_uuid(&self, uuid: &str) -> Result<(), SessionError> {
        if self.identity.delivery_uuid != uuid {
            return Err(SessionError::IdentityMismatch);
        }
        Ok(())
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
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};

    fn stem_manifest() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .unwrap()
    }

    #[test]
    fn lock_for_delivery_a_cannot_commit_delivery_b_ledger() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let manifest_a = stem_manifest();
        let id_a = DeliveryImmutableIdentity::new(
            "delivery-a",
            &manifest_a,
            ".streamsync-stage-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![],
            "final-a",
        )
        .unwrap();
        root.create_child_dir(".streamsync-stage-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        let lock_a = acquire_delivery_domain_lock(&root, "delivery-a", true).unwrap();
        let guard_a = DeliverySessionGuard::open(root, id_a.clone(), manifest_a, lock_a).unwrap();

        let root_b = DestRoot::open(tmp.path()).unwrap();
        let manifest_b = stem_manifest();
        let id_b = DeliveryImmutableIdentity::new(
            "delivery-b",
            &manifest_b,
            ".streamsync-stage-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            vec![],
            "final-b",
        )
        .unwrap();
        root_b
            .create_child_dir(".streamsync-stage-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap();
        let lock_b = acquire_delivery_domain_lock(&root_b, "delivery-b", true).unwrap();
        let guard_b = DeliverySessionGuard::open(root_b, id_b, manifest_b, lock_b).unwrap();

        let store_b = LedgerStore::open_for_guard(&guard_b).unwrap();
        let err = store_b.commit(&guard_a, LedgerState::Receiving);
        assert!(matches!(
            err,
            Err(crate::voice_delivery::records::ledger_generation::LedgerGenerationError::IdentityMismatch)
        ));
    }
}
