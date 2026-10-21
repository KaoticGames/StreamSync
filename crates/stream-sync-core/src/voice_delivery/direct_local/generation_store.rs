//! Generation-scoped on-disk layout for direct-local SYNJ segments.

use super::layout::{
    cumulative_checkpoint_relative_path, generation_relative_components, segment_filename,
    segment_relative_path,
};
use super::segment_identity::{LogicalSegmentIdentity, LogicalSegmentIdentityError};
#[cfg(test)]
use crate::voice_delivery::fs::durable_payload::write_durable_payload_create_new;
use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError};
use std::path::PathBuf;
use thiserror::Error;

pub use super::layout::DirectSegmentFileName;

#[derive(Debug, Error)]
pub enum DirectGenerationStoreError {
    #[error("segment already exists")]
    SegmentAlreadyExists { segment_sequence: u64 },
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("malformed identity")]
    MalformedIdentity(LogicalSegmentIdentityError),
    #[error(transparent)]
    Fs(#[from] FsError),
}

pub struct DirectGenerationStore {
    session_id: String,
    delivery_generation: u64,
    generation_dir: DirHandle,
}

impl DirectGenerationStore {
    pub fn open_for_session(
        root: &DestRoot,
        session_id: &str,
        delivery_generation: u64,
    ) -> Result<Self, DirectGenerationStoreError> {
        let components = generation_relative_components(session_id, delivery_generation)
            .map_err(DirectGenerationStoreError::Fs)?;
        let mut dir = root
            .handle()
            .clone_handle()
            .map_err(DirectGenerationStoreError::Fs)?;
        for comp in &components {
            dir = dir
                .create_or_open_child_dir(comp)
                .map_err(DirectGenerationStoreError::Fs)?;
        }
        Ok(Self {
            session_id: session_id.to_string(),
            delivery_generation,
            generation_dir: dir,
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn delivery_generation(&self) -> u64 {
        self.delivery_generation
    }

    pub(crate) fn generation_dir(&self) -> &DirHandle {
        &self.generation_dir
    }

    pub fn logical_identity_for_incoming(
        &self,
        segment_sequence: u64,
        byte_length: u64,
        sha256_hex: &str,
    ) -> Result<LogicalSegmentIdentity, DirectGenerationStoreError> {
        LogicalSegmentIdentity::try_new(
            &self.session_id,
            self.delivery_generation,
            segment_sequence,
            byte_length,
            sha256_hex,
        )
        .map_err(DirectGenerationStoreError::MalformedIdentity)
    }

    #[cfg(test)]
    pub(crate) fn write_complete_segment_create_new(
        &self,
        identity: &LogicalSegmentIdentity,
        bytes: &[u8],
    ) -> Result<(), DirectGenerationStoreError> {
        self.assert_identity_matches_store(identity)?;
        let name =
            segment_filename(identity.segment_sequence).map_err(DirectGenerationStoreError::Fs)?;
        if self.generation_dir.read_file_all(&name).is_ok() {
            return Err(DirectGenerationStoreError::SegmentAlreadyExists {
                segment_sequence: identity.segment_sequence,
            });
        }
        write_durable_payload_create_new(&self.generation_dir, &name, bytes)
            .map_err(DirectGenerationStoreError::Fs)?;
        Ok(())
    }

    pub fn segment_relative_path(&self, identity: &LogicalSegmentIdentity) -> PathBuf {
        segment_relative_path(
            &self.session_id,
            self.delivery_generation,
            identity.segment_sequence,
        )
        .unwrap_or_default()
    }

    pub fn cumulative_checkpoint_relative_path(&self) -> PathBuf {
        cumulative_checkpoint_relative_path(&self.session_id, self.delivery_generation)
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn read_segment_bytes(
        &self,
        identity: &LogicalSegmentIdentity,
    ) -> Result<Vec<u8>, DirectGenerationStoreError> {
        self.assert_identity_matches_store(identity)?;
        let name =
            segment_filename(identity.segment_sequence).map_err(DirectGenerationStoreError::Fs)?;
        self.generation_dir
            .read_file_all(&name)
            .map_err(DirectGenerationStoreError::Fs)
    }

    pub(crate) fn segment_basename(
        &self,
        identity: &LogicalSegmentIdentity,
    ) -> Result<String, FsError> {
        self.assert_identity_matches_store(identity)
            .map_err(|_| FsError::InvalidComponent("identity".into()))?;
        segment_filename(identity.segment_sequence)
    }

    fn assert_identity_matches_store(
        &self,
        identity: &LogicalSegmentIdentity,
    ) -> Result<(), DirectGenerationStoreError> {
        if identity.session_id != self.session_id
            || identity.delivery_generation != self.delivery_generation
        {
            return Err(DirectGenerationStoreError::IdentityMismatch);
        }
        Ok(())
    }
}
