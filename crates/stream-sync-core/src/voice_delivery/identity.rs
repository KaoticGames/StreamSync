//! Immutable delivery and stem identities bound into records and checkpoints.

use crate::voice_delivery::fs::{FsError, ValidatedFinalName};
use crate::voice_delivery::ids::{delivery_opaque_dir_id, validate_stage_basename};
use crate::voice_delivery::manifest::ValidatedManifest;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryImmutableIdentity {
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub opaque_delivery_id: String,
    pub staging_token: String,
    pub final_parent_relative: Vec<String>,
    pub final_session_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StemArtifactId {
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub stem_portable_name: String,
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
    pub fn new(
        delivery_uuid: impl Into<String>,
        manifest: &ValidatedManifest,
        staging_token: impl Into<String>,
        final_parent_relative: Vec<String>,
        final_session_name: &str,
    ) -> Result<Self, IdentityError> {
        let delivery_uuid = delivery_uuid.into();
        if delivery_uuid.is_empty() {
            return Err(IdentityError::Invalid("empty delivery uuid".into()));
        }
        let staging_token = staging_token.into();
        validate_stage_basename(&staging_token)?;
        for comp in &final_parent_relative {
            if comp.is_empty() || comp.contains('/') || comp.contains('\\') {
                return Err(IdentityError::Invalid(
                    "invalid final parent component".into(),
                ));
            }
        }
        let final_name = ValidatedFinalName::validate(final_session_name)?;
        let manifest_digest = manifest.digest();
        if manifest_digest.len() != 64 {
            return Err(IdentityError::Invalid("manifest digest length".into()));
        }
        let opaque_delivery_id = delivery_opaque_dir_id(&delivery_uuid);
        Ok(Self {
            delivery_uuid,
            manifest_digest,
            opaque_delivery_id,
            staging_token,
            final_parent_relative,
            final_session_name: final_name.as_str().to_string(),
        })
    }

    pub fn matches_record_fields(
        &self,
        delivery_uuid: &str,
        manifest_digest: &str,
        staging_token: &str,
    ) -> bool {
        self.delivery_uuid == delivery_uuid
            && self.manifest_digest == manifest_digest
            && self.staging_token == staging_token
    }
}

impl StemArtifactId {
    pub fn new(
        identity: &DeliveryImmutableIdentity,
        stem_portable_name: impl Into<String>,
    ) -> Self {
        Self {
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            stem_portable_name: stem_portable_name.into(),
        }
    }

    pub fn partial_basename(&self) -> String {
        let key = format!(
            "{}:{}:{}",
            self.delivery_uuid, self.manifest_digest, self.stem_portable_name
        );
        let digest = Sha256::digest(key.as_bytes());
        format!("{:x}.partial", digest)
    }
}
