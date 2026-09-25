//! Resumable partial stem writer with immutable checkpoint generations.

use crate::voice_delivery::fs::durability::sync_file;
use crate::voice_delivery::fs::VoiceFile;
use crate::voice_delivery::hash::{HashError, LiveSha256};
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
    #[error("overlap replay byte mismatch at {offset}")]
    OverlapMismatch { offset: u64 },
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

pub struct PartialStemWriter<'guard> {
    _guard: &'guard DeliverySessionGuard,
    file: VoiceFile,
    expected_total: u64,
    expected_full_sha256: String,
    durable_contiguous_len: u64,
    live_contiguous_len: u64,
    hasher: LiveSha256,
    artifact: StemArtifactId,
    checkpoint_store: CheckpointStore<'guard>,
    chunk_size: usize,
}

impl<'guard> PartialStemWriter<'guard> {
    pub fn open_or_resume(
        guard: &'guard DeliverySessionGuard,
        stem_portable_name: &str,
        chunk_size: usize,
    ) -> Result<Self, PartialError> {
        Self::open_or_resume_inner(guard, stem_portable_name, chunk_size, None)
    }

    fn open_or_resume_inner(
        guard: &'guard DeliverySessionGuard,
        stem_portable_name: &str,
        chunk_size: usize,
        prefix_read_counter: Option<&mut u64>,
    ) -> Result<Self, PartialError> {
        if chunk_size == 0 {
            return Err(PartialError::InvalidChunkSize);
        }
        let manifest = guard.manifest();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), manifest, stem_portable_name)
                .map_err(|_| PartialError::BindingMismatch)?;
        let stem = artifact
            .manifest_entry(manifest)
            .map_err(|_| PartialError::BindingMismatch)?;
        let expected_total = stem.byte_count;
        let expected_full_sha256 = stem.sha256.clone();
        let checkpoint_store = CheckpointStore::open_for_guard(guard, artifact.clone())?;
        let staging_dir = guard.staging_dir();
        let partial_name = artifact.partial_basename();
        let checkpoint = checkpoint_store.read_highest_valid()?;
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
            if cp.durable_contiguous_len > expected_total {
                return Err(PartialError::CheckpointInvalid);
            }
            if physical > cp.durable_contiguous_len {
                file.set_len(cp.durable_contiguous_len)?;
                sync_file(&file)?;
            } else if physical < cp.durable_contiguous_len {
                return Err(PartialError::UncheckpointedTail);
            }
            let physical_after = file.len()?;
            if physical_after > expected_total {
                return Err(PartialError::OversizedExisting {
                    existing: physical_after,
                    expected: expected_total,
                });
            }
            let mut live = LiveSha256::new();
            hash_prefix_into(
                &file,
                cp.durable_contiguous_len,
                chunk_size,
                &mut live,
                prefix_read_counter,
            )?;
            let digest = live.prefix_digest_hex();
            if digest != cp.prefix_digest {
                return Err(PartialError::PrefixDigestMismatch);
            }
            (cp.durable_contiguous_len, live)
        } else {
            if physical > 0 {
                return Err(PartialError::CheckpointInvalid);
            }
            (0, LiveSha256::new())
        };
        Ok(Self {
            _guard: guard,
            file,
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
        if offset > self.live_contiguous_len {
            return Err(PartialError::NonContiguous {
                offset,
                expected: self.live_contiguous_len,
            });
        }
        if offset + len as u64 <= self.live_contiguous_len {
            verify_overlap_prefix(&self.file, offset, data, self.chunk_size)?;
            return Ok(());
        }
        if offset < self.live_contiguous_len {
            let overlap = (self.live_contiguous_len - offset) as usize;
            verify_overlap_prefix(&self.file, offset, &data[..overlap], self.chunk_size)?;
            let tail = &data[overlap..];
            self.file.write_all_at(self.live_contiguous_len, tail)?;
            self.hasher.update(tail);
            self.live_contiguous_len = end;
            return Ok(());
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

    pub fn commit_checkpoint(&mut self) -> Result<u64, PartialError> {
        if self.live_contiguous_len < self.durable_contiguous_len {
            return Err(PartialError::CheckpointInvalid);
        }
        sync_file(&self.file)?;
        let prefix = self.hasher.prefix_digest_hex();
        let _cp = self
            .checkpoint_store
            .commit(self.live_contiguous_len, &prefix)?;
        self.durable_contiguous_len = self.live_contiguous_len;
        Ok(self.durable_contiguous_len)
    }

    pub fn prefix_digest_hex(&self) -> String {
        self.hasher.prefix_digest_hex()
    }
}

fn verify_overlap_prefix(
    file: &VoiceFile,
    offset: u64,
    data: &[u8],
    chunk_size: usize,
) -> Result<(), PartialError> {
    let mut pos = 0usize;
    let mut file_off = offset;
    while pos < data.len() {
        let take = (data.len() - pos).min(chunk_size);
        let mut buf = vec![0u8; take];
        file.read_exact_at(file_off, &mut buf)?;
        if buf != data[pos..pos + take] {
            return Err(PartialError::OverlapMismatch { offset: file_off });
        }
        pos += take;
        file_off += take as u64;
    }
    Ok(())
}

/// When `chunk_reads` is set, increments once per physical read issued while rebuilding the prefix hasher.
pub(crate) fn hash_prefix_into(
    file: &VoiceFile,
    len: u64,
    chunk_size: usize,
    live: &mut LiveSha256,
    mut chunk_reads: Option<&mut u64>,
) -> Result<(), PartialError> {
    let mut offset = 0u64;
    let mut remaining = len;
    let mut buf = vec![0u8; chunk_size];
    while remaining > 0 {
        let take = remaining.min(chunk_size as u64);
        let take_usize = usize::try_from(take).map_err(|_| PartialError::LengthOverflow)?;
        file.read_exact_at(offset, &mut buf[..take_usize])?;
        if let Some(counter) = chunk_reads.as_deref_mut() {
            *counter += 1;
        }
        live.update(&buf[..take_usize]);
        offset += take;
        remaining -= take;
    }
    Ok(())
}

trait StagingPartialOpen {
    fn open_or_create_partial(
        &self,
        name: &str,
    ) -> Result<VoiceFile, crate::voice_delivery::fs::FsError>;
}

impl StagingPartialOpen for crate::voice_delivery::fs::DirHandle {
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
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use sha2::{Digest, Sha256};

    fn fixture() -> (tempfile::TempDir, DeliverySessionGuard, StemArtifactId) {
        let tmp = tempfile::tempdir().unwrap();
        let stems = vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 1000,
            sha256: "a".repeat(64),
        }];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let guard = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            "delivery-partial",
            manifest.clone(),
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), &manifest, "a.wav").unwrap();
        (tmp, guard, artifact)
    }

    #[test]
    fn partial_prefix_hasher_checkpoint() {
        let (_tmp, guard, _artifact) = fixture();
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 4096).unwrap();
        w.write_contiguous(0, &[1, 2, 3, 4]).unwrap();
        let off = w.commit_checkpoint().unwrap();
        assert_eq!(off, 4);
        let mut w2 = PartialStemWriter::open_or_resume(&guard, "a.wav", 4096).unwrap();
        assert_eq!(w2.resumable_offset(), 4);
        w2.write_contiguous(4, &[5]).unwrap();
        let digest = w2.prefix_digest_hex();
        let mut expected = Sha256::new();
        expected.update([1, 2, 3, 4, 5]);
        assert_eq!(digest, hex_digest(&expected.finalize()));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod partial_sparse_prefix_digest_fail {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs::OpenOptions;

    #[test]
    #[cfg(target_os = "linux")]
    fn partial_sparse_hole_in_committed_prefix_fails() {
        use std::os::unix::io::AsRawFd;
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stems = vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 64,
            sha256: "c".repeat(64),
        }];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let guard = DeliverySessionGuard::begin(
            root,
            "delivery-sparse",
            manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
                .unwrap();
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 4096).unwrap();
        w.write_contiguous(0, &[9; 16]).unwrap();
        w.commit_checkpoint().unwrap();
        let partial_path = tmp
            .path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(artifact.partial_basename());
        let f = OpenOptions::new().write(true).open(&partial_path).unwrap();
        let fd = f.as_raw_fd();
        let rc = unsafe {
            libc::fallocate(
                fd,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                8,
                4,
            )
        };
        assert_eq!(rc, 0);
        f.sync_all().unwrap();
        drop(f);
        let result = PartialStemWriter::open_or_resume(&guard, "a.wav", 4096);
        assert!(matches!(result, Err(PartialError::PrefixDigestMismatch)));
    }
}

#[cfg(test)]
mod partial_resume_adversarial {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn guard(bytes: u64, sha: &str) -> (tempfile::TempDir, DeliverySessionGuard) {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: bytes,
            sha256: sha.to_string(),
        }])
        .unwrap();
        let guard = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            "partial-adv",
            manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        (tmp, guard)
    }

    #[test]
    fn oversized_physical_tail_truncates_to_checkpoint_on_resume() {
        let (_tmp, guard) = guard(100, &"a".repeat(64));
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 64).unwrap();
        w.write_contiguous(0, &[1; 20]).unwrap();
        w.commit_checkpoint().unwrap();
        let name = StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
            .unwrap()
            .partial_basename();
        let path = _tmp
            .path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(name);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[9; 10]).unwrap();
        f.sync_all().unwrap();
        let w2 = PartialStemWriter::open_or_resume(&guard, "a.wav", 64).unwrap();
        assert_eq!(w2.resumable_offset(), 20);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 20);
        assert_eq!(bytes, [1; 20]);
    }

    #[test]
    fn shorter_than_checkpoint_fails() {
        let (_tmp, guard) = guard(100, &"a".repeat(64));
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 64).unwrap();
        w.write_contiguous(0, &[1; 30]).unwrap();
        w.commit_checkpoint().unwrap();
        let name = StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
            .unwrap()
            .partial_basename();
        let path = _tmp
            .path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(name);
        std::fs::write(&path, [1; 10]).unwrap();
        let err = PartialStemWriter::open_or_resume(&guard, "a.wav", 64);
        assert!(matches!(err, Err(PartialError::UncheckpointedTail)));
    }

    #[test]
    fn overlap_mismatch_fails() {
        let (_tmp, guard) = guard(100, &"a".repeat(64));
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 64).unwrap();
        w.write_contiguous(0, &[1, 2, 3, 4]).unwrap();
        let digest_before = w.prefix_digest_hex();
        let offset_before = w.resumable_offset();
        let err = w.write_contiguous(2, &[9, 9]);
        assert!(matches!(err, Err(PartialError::OverlapMismatch { .. })));
        assert_eq!(w.resumable_offset(), offset_before);
        assert_eq!(w.prefix_digest_hex(), digest_before);
        let name = StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
            .unwrap()
            .partial_basename();
        let path = _tmp
            .path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(name);
        assert_eq!(std::fs::read(&path).unwrap(), [1, 2, 3, 4]);
    }
}

#[cfg(test)]
mod partial_contiguous_overlap_behavior {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::hash::hex_digest;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use sha2::{Digest, Sha256};

    fn guard(bytes: u64) -> (tempfile::TempDir, DeliverySessionGuard) {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: bytes,
            sha256: "b".repeat(64),
        }])
        .unwrap();
        let guard = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            "overlap-beh",
            manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        (tmp, guard)
    }

    fn partial_path(tmp: &tempfile::TempDir, guard: &DeliverySessionGuard) -> std::path::PathBuf {
        let name = StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
            .unwrap()
            .partial_basename();
        tmp.path()
            .join(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .join(name)
    }

    #[test]
    fn full_duplicate_replay_is_idempotent() {
        let (tmp, guard) = guard(64);
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 8).unwrap();
        let chunk = [10, 11, 12, 13];
        w.write_contiguous(0, &chunk).unwrap();
        let digest = w.prefix_digest_hex();
        let offset = w.resumable_offset();
        w.write_contiguous(0, &chunk).unwrap();
        assert_eq!(w.resumable_offset(), offset);
        assert_eq!(w.prefix_digest_hex(), digest);
        assert_eq!(std::fs::read(partial_path(&tmp, &guard)).unwrap(), chunk);
    }

    #[test]
    fn partial_overlap_appends_tail_and_updates_digest() {
        let (tmp, guard) = guard(64);
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", 8).unwrap();
        w.write_contiguous(0, &[1, 2, 3, 4]).unwrap();
        w.commit_checkpoint().unwrap();
        let mut w2 = PartialStemWriter::open_or_resume(&guard, "a.wav", 8).unwrap();
        w2.write_contiguous(2, &[3, 4, 5, 6, 7]).unwrap();
        assert_eq!(w2.resumable_offset(), 4);
        assert!(matches!(
            w2.write_contiguous(8, &[0]),
            Err(PartialError::NonContiguous { .. })
        ));
        let on_disk = std::fs::read(partial_path(&tmp, &guard)).unwrap();
        assert_eq!(on_disk, [1, 2, 3, 4, 5, 6, 7]);
        let mut expected = Sha256::new();
        expected.update([1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(w2.prefix_digest_hex(), hex_digest(&expected.finalize()));
    }
}

#[cfg(test)]
mod partial_restart_single_prefix_pass {
    use super::*;
    use crate::voice_delivery::fs::file::open_or_create_file_at;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;

    #[test]
    fn resume_rebuilds_prefix_hasher_in_one_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 200,
            sha256: "c".repeat(64),
        }])
        .unwrap();
        let guard = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            "one-pass",
            manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        let chunk_size = 32usize;
        const PREFIX_LEN: u64 = 100;
        let mut w = PartialStemWriter::open_or_resume(&guard, "a.wav", chunk_size).unwrap();
        w.write_contiguous(0, &[7; PREFIX_LEN as usize]).unwrap();
        w.commit_checkpoint().unwrap();
        let expected_chunks = PREFIX_LEN.div_ceil(chunk_size as u64);
        let mut resume_reads = 0u64;
        let w2 = PartialStemWriter::open_or_resume_inner(
            &guard,
            "a.wav",
            chunk_size,
            Some(&mut resume_reads),
        )
        .unwrap();
        assert_eq!(resume_reads, expected_chunks);
        assert_eq!(resume_reads, 4);
        assert_eq!(w2.resumable_offset(), PREFIX_LEN);
        let mut second_pass_reads = 0u64;
        let mut live = LiveSha256::new();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
                .unwrap();
        let file =
            open_or_create_file_at(guard.staging_dir(), &artifact.partial_basename()).unwrap();
        hash_prefix_into(
            &file,
            PREFIX_LEN,
            chunk_size,
            &mut live,
            Some(&mut second_pass_reads),
        )
        .unwrap();
        assert_eq!(second_pass_reads, expected_chunks);
        assert_eq!(w2.prefix_digest_hex(), live.prefix_digest_hex());
    }
}
