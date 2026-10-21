//! Identical segment retries are idempotent; conflicting bytes or identity fail closed.

use super::generation_store::{DirectGenerationStore, DirectGenerationStoreError};
use super::segment_identity::LogicalSegmentIdentity;
use crate::voice_delivery::hash::hex_digest;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum DirectSegmentRetryError {
    #[error("conflicting segment payload")]
    ConflictingPayload,
    #[error("conflicting segment identity")]
    ConflictingIdentity,
    #[error("store error")]
    Store,
}

pub(crate) struct DirectIdempotentSegmentRetry<'a> {
    store: &'a DirectGenerationStore,
}

impl<'a> DirectIdempotentSegmentRetry<'a> {
    pub(crate) fn new(store: &'a DirectGenerationStore) -> Self {
        Self { store }
    }

    pub(crate) fn persist_idempotent(
        &self,
        identity: &LogicalSegmentIdentity,
        bytes: &[u8],
    ) -> Result<(), DirectSegmentRetryError> {
        match self.store.read_segment_bytes(identity) {
            Ok(on_disk) => classify_durable_retry(&on_disk, identity, bytes),
            Err(DirectGenerationStoreError::Fs(crate::voice_delivery::fs::FsError::Io(e)))
                if e.kind() == std::io::ErrorKind::NotFound =>
            {
                if identity.byte_length != bytes.len() as u64 {
                    return Err(DirectSegmentRetryError::ConflictingIdentity);
                }
                let actual_digest = hex_digest(&Sha256::digest(bytes));
                if actual_digest != identity.sha256_hex {
                    return Err(DirectSegmentRetryError::ConflictingIdentity);
                }
                self.store
                    .write_complete_segment_create_new(identity, bytes)
                    .map_err(|_| DirectSegmentRetryError::Store)?;
                Ok(())
            }
            Err(DirectGenerationStoreError::SegmentAlreadyExists { .. }) => {
                self.persist_idempotent(identity, bytes)
            }
            Err(_) => Err(DirectSegmentRetryError::Store),
        }
    }
}

fn classify_durable_retry(
    on_disk: &[u8],
    identity: &LogicalSegmentIdentity,
    retry_bytes: &[u8],
) -> Result<(), DirectSegmentRetryError> {
    let on_disk_digest = hex_digest(&Sha256::digest(on_disk));
    let on_disk_matches_identity =
        on_disk.len() as u64 == identity.byte_length && on_disk_digest == identity.sha256_hex;
    if !on_disk_matches_identity {
        return Err(DirectSegmentRetryError::ConflictingIdentity);
    }
    if on_disk == retry_bytes {
        return Ok(());
    }
    Err(DirectSegmentRetryError::ConflictingPayload)
}
