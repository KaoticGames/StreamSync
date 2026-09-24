//! Resumable partial stem writer with immutable checkpoint generations.

use crate::voice_delivery::fs::durability::sync_file;
use crate::voice_delivery::fs::{DirHandle, VoiceFile};
use crate::voice_delivery::hash::{sha256_hex_prefix, HashError, LiveSha256};
use crate::voice_delivery::identity::StemArtifactId;
use crate::voice_delivery::records::checkpoint_generation::CheckpointStore;
use crate::voice_delivery::session::DeliverySessionGuard;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PartialError {
    #[error("existing partial length {existing} exceeds expected {expected}")]
    OversizedExisting { existing: u64, expected: u64 },
    #[error("offset {offset} is beyond file capacity {capacity}")]
    OutOfRange { offset: u64, capacity: u64 },
    #[error("range length overflow")]
    LengthOverflow,
    #[error("non-contiguous write at {offset}, expected {expected}")]
    NonContiguous { offset: u64, expected: u64 },
    #[error("resume binding mismatch")]
    BindingMismatch,
    #[error("checkpoint corrupt or inconsistent")]
    CheckpointInvalid,
    #[error("sparse or uncheckpointed tail in partial file")]
    UncheckpointedTail,
    #[error("prefix digest mismatch at checkpoint")]
    PrefixDigestMismatch,
    #[error("invalid stream chunk size")]
    InvalidChunkSize,
    #[error(transparent)]
    Hash(#[from] HashError),
    #[error(transparent)]
    Checkpoint(
        #[from] crate::voice_delivery::records::checkpoint_generation::CheckpointGenerationError,
    ),
    #[error(transparent)]
    Fs(#[from] crate::voice_delivery::fs::FsError),
    #[error("io: {0}")]
    Io(String),
}

pub struct PartialStemWriter {
    file: VoiceFile,
    partial_name: String,
    expected_total: u64,
    expected_full_sha256: String,
    durable_contiguous_len: u64,
    live_contiguous_len: u64,
    hasher: LiveSha256,
    artifact: StemArtifactId,
    checkpoint_store: CheckpointStore,
    chunk_size: usize,
}

impl PartialStemWriter {
    pub fn open_or_resume(
        _guard: &DeliverySessionGuard,
        staging_dir: &DirHandle,
        checkpoint_store: CheckpointStore,
        artifact: StemArtifactId,
        expected_total: u64,
        expected_full_sha256: String,
        chunk_size: usize,
    ) -> Result<Self, PartialError> {
        if chunk_size == 0 {
            return Err(PartialError::InvalidChunkSize);
        }
        let partial_name = artifact.partial_basename();
        let checkpoint = checkpoint_store.read_highest_valid(&artifact)?;
        let file = staging_dir
            .open_or_create_partial(&partial_name)
            .map_err(PartialError::Fs)?;
        let physical = file.len()?;
        let (durable, hasher) = if let Some(cp) = checkpoint {
            if cp.expected_total_bytes != expected_total
                || cp.expected_full_sha256 != expected_full_sha256
            {
                return Err(PartialError::BindingMismatch);
            }
            if physical > expected_total {
                return Err(PartialError::OversizedExisting {
                    existing: physical,
                    expected: expected_total,
                });
            }
            if physical > cp.durable_contiguous_len {
                file.set_len(cp.durable_contiguous_len)?;
                sync_file(&file)?;
            } else if physical < cp.durable_contiguous_len {
                return Err(PartialError::UncheckpointedTail);
            }
            let digest = sha256_hex_prefix(file.std_file(), cp.durable_contiguous_len, chunk_size)?;
            if digest != cp.prefix_digest {
                return Err(PartialError::PrefixDigestMismatch);
            }
            let mut live = LiveSha256::new();
            hash_prefix_into(&file, cp.durable_contiguous_len, chunk_size, &mut live)?;
            (cp.durable_contiguous_len, live)
        } else {
            if physical > 0 {
                return Err(PartialError::CheckpointInvalid);
            }
            (0, LiveSha256::new())
        };
        Ok(Self {
            file,
            partial_name,
            expected_total,
            expected_full_sha256,
            durable_contiguous_len: durable,
            live_contiguous_len: durable,
            hasher,
            artifact,
            checkpoint_store,
            chunk_size,
        })
    }

    pub fn resumable_offset(&self) -> u64 {
        self.durable_contiguous_len
    }

    pub fn write_contiguous(&mut self, offset: u64, data: &[u8]) -> Result<(), PartialError> {
        let len = data.len();
        let end = offset
            .checked_add(len as u64)
            .ok_or(PartialError::LengthOverflow)?;
        if end > self.expected_total {
            return Err(PartialError::OutOfRange {
                offset,
                capacity: self.expected_total,
            });
        }
        if offset != self.live_contiguous_len {
            return Err(PartialError::NonContiguous {
                offset,
                expected: self.live_contiguous_len,
            });
        }
        self.file.write_all_at(offset, data)?;
        self.hasher.update(data);
        self.live_contiguous_len = end;
        Ok(())
    }

    pub fn commit_checkpoint(&mut self, guard: &DeliverySessionGuard) -> Result<u64, PartialError> {
        if self.live_contiguous_len < self.durable_contiguous_len {
            return Err(PartialError::CheckpointInvalid);
        }
        sync_file(&self.file)?;
        let prefix = self.hasher.prefix_digest_hex();
        let _cp = self.checkpoint_store.commit(
            guard,
            &self.artifact,
            self.expected_total,
            &self.expected_full_sha256,
            self.live_contiguous_len,
            &prefix,
        )?;
        self.durable_contiguous_len = self.live_contiguous_len;
        Ok(self.durable_contiguous_len)
    }
}

fn hash_prefix_into(
    file: &VoiceFile,
    len: u64,
    chunk_size: usize,
    live: &mut LiveSha256,
) -> Result<(), PartialError> {
    let mut offset = 0u64;
    let mut remaining = len;
    let mut buf = vec![0u8; chunk_size];
    while remaining > 0 {
        let take = remaining.min(chunk_size as u64);
        let take_usize = usize::try_from(take).map_err(|_| PartialError::LengthOverflow)?;
        file.read_exact_at(offset, &mut buf[..take_usize])?;
        live.update(&buf[..take_usize]);
        offset += take;
        remaining -= take;
    }
    Ok(())
}

impl PartialStemWriter {
    pub fn prefix_digest_hex(&self) -> String {
        self.hasher.prefix_digest_hex()
    }
}

// DirHandle helper for partial open
trait StagingPartialOpen {
    fn open_or_create_partial(
        &self,
        name: &str,
    ) -> Result<VoiceFile, crate::voice_delivery::fs::FsError>;
}

impl StagingPartialOpen for DirHandle {
    fn open_or_create_partial(
        &self,
        name: &str,
    ) -> Result<VoiceFile, crate::voice_delivery::fs::FsError> {
        crate::voice_delivery::fs::file::open_or_create_file_at(self, name)
    }
}

#[cfg(test)]
mod partial_prefix_hasher_checkpoint {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::hash::hex_digest;
    use crate::voice_delivery::identity::DeliveryImmutableIdentity;
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use sha2::{Digest, Sha256};

    fn fixture() -> (
        tempfile::TempDir,
        DestRoot,
        DeliveryImmutableIdentity,
        DeliverySessionGuard,
        DirHandle,
        CheckpointStore,
        StemArtifactId,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stems = vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 1000,
            sha256: "a".repeat(64),
        }];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let identity = DeliveryImmutableIdentity::new(
            "delivery-partial",
            &manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
        )
        .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-partial", true).unwrap();
        let guard = DeliverySessionGuard::new(lock);
        let stage = ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef";
        let _ = root.create_child_dir(stage);
        let staging = root.open_child_dir(stage).unwrap();
        let cp_store = CheckpointStore::open_at_root(&root, &identity).unwrap();
        let artifact = StemArtifactId::new(&identity, "a.wav");
        (tmp, root, identity, guard, staging, cp_store, artifact)
    }

    #[test]
    fn partial_prefix_hasher_checkpoint() {
        let (_tmp, _root, _id, guard, staging, cp_store, artifact) = fixture();
        let expected_sha = "b".repeat(64);
        let mut w = PartialStemWriter::open_or_resume(
            &guard,
            &staging,
            cp_store,
            artifact.clone(),
            100,
            expected_sha.clone(),
            4096,
        )
        .unwrap();
        w.write_contiguous(0, &[1, 2, 3, 4]).unwrap();
        let off = w.commit_checkpoint(&guard).unwrap();
        assert_eq!(off, 4);
        let cp_store2 = CheckpointStore::open_at_root(&_root, &_id).unwrap();
        let mut w2 = PartialStemWriter::open_or_resume(
            &guard,
            &staging,
            cp_store2,
            artifact,
            100,
            expected_sha,
            4096,
        )
        .unwrap();
        assert_eq!(w2.resumable_offset(), 4);
        w2.write_contiguous(4, &[5]).unwrap();
        let digest = w2.prefix_digest_hex();
        let mut expected = Sha256::new();
        expected.update([1, 2, 3, 4, 5]);
        assert_eq!(digest, hex_digest(&expected.finalize()));
    }
}

#[cfg(test)]
mod partial_sparse_prefix_digest_fail {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::identity::DeliveryImmutableIdentity;
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    #[cfg(target_os = "linux")]
    fn partial_sparse_prefix_digest_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stems = vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 64,
            sha256: "c".repeat(64),
        }];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let identity = DeliveryImmutableIdentity::new(
            "delivery-sparse",
            &manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
        )
        .unwrap();
        root.create_child_dir(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .unwrap();
        let staging = root
            .open_child_dir(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-sparse", true).unwrap();
        let guard = DeliverySessionGuard::new(lock);
        let cp_store = CheckpointStore::open_at_root(&root, &identity).unwrap();
        let artifact = StemArtifactId::new(&identity, "a.wav");
        let expected_sha = "d".repeat(64);
        let mut w = PartialStemWriter::open_or_resume(
            &guard,
            &staging,
            cp_store,
            artifact.clone(),
            32,
            expected_sha.clone(),
            4096,
        )
        .unwrap();
        w.write_contiguous(0, &[9; 16]).unwrap();
        w.commit_checkpoint(&guard).unwrap();
        let partial_path = tmp
            .path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(artifact.partial_basename());
        let mut f = OpenOptions::new().write(true).open(&partial_path).unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&[0]).unwrap();
        f.sync_all().unwrap();
        let cp_store2 = CheckpointStore::open_at_root(&root, &identity).unwrap();
        let result = PartialStemWriter::open_or_resume(
            &guard,
            &staging,
            cp_store2,
            artifact,
            32,
            expected_sha,
            4096,
        );
        assert!(matches!(result, Err(PartialError::PrefixDigestMismatch)));
    }
}
