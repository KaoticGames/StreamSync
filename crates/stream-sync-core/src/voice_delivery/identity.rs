//! Immutable delivery and stem identities bound into records and checkpoints.

use crate::voice_delivery::fs::{FsError, PortableParentComponent, ValidatedFinalName};
use crate::voice_delivery::ids::{delivery_opaque_dir_id, validate_stage_basename};
use crate::voice_delivery::manifest::ValidatedManifest;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryImmutableIdentity {
    pub delivery_uuid: String,
    /// Syndicate API `manifestDigest` (canonical finalized manifest identity).
    pub manifest_digest: String,
    /// Local validated stem-list digest (`ValidatedManifest::digest()`).
    pub stem_set_digest: String,
    pub opaque_delivery_id: String,
    pub staging_token: String,
    pub final_parent_relative: Vec<PortableParentComponent>,
    pub final_session_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StemArtifactId {
    delivery_uuid: String,
    manifest_digest: String,
    stem_portable_name: String,
}

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("invalid identity field: {0}")]
    Invalid(String),
    #[error("identity mismatch")]
    Mismatch,
    #[error(transparent)]
    Fs(#[from] FsError),
}

impl DeliveryImmutableIdentity {
    /// Bind identity using authoritative API manifest digest and validated stem manifest.
    pub fn new_bound(
        delivery_uuid: impl Into<String>,
        manifest_digest: impl Into<String>,
        manifest: &ValidatedManifest,
        staging_token: impl Into<String>,
        final_parent_relative: Vec<PortableParentComponent>,
        final_session_name: &str,
    ) -> Result<Self, IdentityError> {
        let delivery_uuid = delivery_uuid.into();
        if delivery_uuid.is_empty() {
            return Err(IdentityError::Invalid("empty delivery uuid".into()));
        }
        let staging_token = staging_token.into();
        validate_stage_basename(&staging_token)?;
        let final_name = ValidatedFinalName::validate(final_session_name)?;
        let manifest_digest = manifest_digest.into();
        if manifest_digest.len() != 64 {
            return Err(IdentityError::Invalid("manifest digest length".into()));
        }
        let stem_set_digest = manifest.digest();
        let opaque_delivery_id = delivery_opaque_dir_id(&delivery_uuid);
        Ok(Self {
            delivery_uuid,
            manifest_digest,
            stem_set_digest,
            opaque_delivery_id,
            staging_token,
            final_parent_relative,
            final_session_name: final_name.as_str().to_string(),
        })
    }

    pub fn assert_stem_manifest(&self, manifest: &ValidatedManifest) -> Result<(), IdentityError> {
        if self.stem_set_digest != manifest.digest() {
            return Err(IdentityError::Mismatch);
        }
        Ok(())
    }

    pub fn from_ledger_record(
        record: &crate::voice_delivery::records::ledger_generation::LedgerGeneration,
    ) -> Result<Self, IdentityError> {
        validate_stage_basename(&record.staging_token)?;
        let final_name = ValidatedFinalName::validate(&record.final_session_name)?;
        if record.manifest_digest.len() != 64 || record.stem_set_digest.len() != 64 {
            return Err(IdentityError::Invalid("digest length".into()));
        }
        Ok(Self {
            delivery_uuid: record.delivery_uuid.clone(),
            manifest_digest: record.manifest_digest.clone(),
            stem_set_digest: record.stem_set_digest.clone(),
            opaque_delivery_id: delivery_opaque_dir_id(&record.delivery_uuid),
            staging_token: record.staging_token.clone(),
            final_parent_relative: record.final_parent_relative.clone(),
            final_session_name: final_name.as_str().to_string(),
        })
    }

    pub fn final_parent_components(&self) -> &[PortableParentComponent] {
        &self.final_parent_relative
    }

    pub fn matches_ledger_record(
        &self,
        record: &crate::voice_delivery::records::ledger_generation::LedgerGeneration,
    ) -> bool {
        self.delivery_uuid == record.delivery_uuid
            && self.manifest_digest == record.manifest_digest
            && self.stem_set_digest == record.stem_set_digest
            && self.staging_token == record.staging_token
            && self.final_parent_relative == record.final_parent_relative
            && self.final_session_name == record.final_session_name
    }

    pub fn foreign_identity_in_record(
        &self,
        delivery_uuid: &str,
        manifest_digest: &str,
        stem_set_digest: &str,
        staging_token: &str,
        final_parent_relative: &[PortableParentComponent],
        final_session_name: &str,
    ) -> bool {
        self.delivery_uuid != delivery_uuid
            || self.manifest_digest != manifest_digest
            || self.stem_set_digest != stem_set_digest
            || self.staging_token != staging_token
            || self.final_parent_relative.as_slice() != final_parent_relative
            || self.final_session_name != final_session_name
    }
}

impl StemArtifactId {
    pub fn from_manifest_stem(
        identity: &DeliveryImmutableIdentity,
        manifest: &ValidatedManifest,
        stem_portable_name: &str,
    ) -> Result<Self, IdentityError> {
        let entry = manifest
            .stems
            .iter()
            .find(|s| s.file_name == stem_portable_name)
            .ok_or(IdentityError::Invalid(format!(
                "stem {stem_portable_name} not in manifest"
            )))?;
        Ok(Self {
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            stem_portable_name: entry.file_name.clone(),
        })
    }

    pub(crate) fn delivery_uuid(&self) -> &str {
        &self.delivery_uuid
    }

    pub(crate) fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    pub(crate) fn stem_portable_name(&self) -> &str {
        &self.stem_portable_name
    }

    pub(crate) fn matches_identity(&self, identity: &DeliveryImmutableIdentity) -> bool {
        self.delivery_uuid == identity.delivery_uuid
            && self.manifest_digest == identity.manifest_digest
    }

    fn identity_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.delivery_uuid, self.manifest_digest, self.stem_portable_name
        )
    }

    pub fn checkpoint_dir_key(&self) -> String {
        let digest = Sha256::digest(self.identity_key().as_bytes());
        format!("{:x}", digest)
    }

    pub fn partial_basename(&self) -> String {
        format!("{}.partial", self.checkpoint_dir_key())
    }

    pub fn manifest_entry<'a>(
        &'a self,
        manifest: &'a ValidatedManifest,
    ) -> Result<&'a crate::voice_delivery::manifest::StemManifestEntry, IdentityError> {
        manifest
            .stems
            .iter()
            .find(|s| s.file_name == self.stem_portable_name)
            .ok_or(IdentityError::Mismatch)
    }
}
