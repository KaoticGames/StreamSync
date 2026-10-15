//! Durable `.in-sync/<session-id>/` journal ingest under the voice recording parent.

use super::manifest::LocalFinalizationManifest;
use super::storage_preflight::{storage_preflight, StoragePreflightFailure};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const IN_SYNC_DIR: &str = ".in-sync";
const CHECKPOINT_FILE: &str = "download-checkpoint.json";
const MANIFEST_FILE: &str = "manifest.json";
const SEGMENTS_DIR: &str = "journal/segments";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegmentCheckpoint {
    pub segment_seq: u32,
    pub bytes_complete: u64,
    pub sha256: Option<String>,
    pub complete: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadCheckpoint {
    pub segments: Vec<SegmentCheckpoint>,
}

#[derive(Debug, Error)]
pub enum JournalStoreError {
    #[error("journal store: {0}")]
    Other(String),
    #[error("journal store io: {0}")]
    Io(#[from] std::io::Error),
    #[error("journal store conflict")]
    Conflict,
}

pub struct LocalJournalStore {
    session_root: PathBuf,
}

impl LocalJournalStore {
    pub fn open(recording_parent: &Path, session_id: &str) -> Result<Self, JournalStoreError> {
        let root = recording_parent.join(IN_SYNC_DIR).join(session_id);
        std::fs::create_dir_all(&root)?;
        sync_dir(&root)?;
        Ok(Self { session_root: root })
    }

    pub fn session_root(&self) -> &Path {
        &self.session_root
    }

    pub fn segment_path(&self, segment_seq: u32) -> PathBuf {
        self.session_root
            .join(SEGMENTS_DIR)
            .join(format!("{:08}.synj", segment_seq))
    }

    pub fn persist_manifest(
        &self,
        manifest: &LocalFinalizationManifest,
    ) -> Result<(), JournalStoreError> {
        let path = self.session_root.join(MANIFEST_FILE);
        if path.exists() {
            let existing = std::fs::read_to_string(&path)?;
            let parsed: serde_json::Value = serde_json::from_str(&existing)
                .map_err(|e| JournalStoreError::Other(e.to_string()))?;
            let existing_manifest = super::manifest::parse_local_finalization_manifest(&parsed)
                .map_err(|e| JournalStoreError::Other(e.to_string()))?;
            if existing_manifest.session_id != manifest.session_id {
                return Err(JournalStoreError::Conflict);
            }
            return Ok(());
        }
        let json = serde_json::to_string_pretty(manifest)
            .map_err(|e| JournalStoreError::Other(e.to_string()))?;
        write_atomic(&path, json.as_bytes())?;
        Ok(())
    }

    pub fn load_checkpoint(&self) -> Result<DownloadCheckpoint, JournalStoreError> {
        let path = self.session_root.join(CHECKPOINT_FILE);
        if !path.exists() {
            return Ok(DownloadCheckpoint {
                segments: Vec::new(),
            });
        }
        let raw = std::fs::read_to_string(&path)?;
        serde_json::from_str(&raw).map_err(|e| JournalStoreError::Other(e.to_string()))
    }

    pub fn save_checkpoint(
        &self,
        checkpoint: &DownloadCheckpoint,
    ) -> Result<(), JournalStoreError> {
        let path = self.session_root.join(CHECKPOINT_FILE);
        let json = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| JournalStoreError::Other(e.to_string()))?;
        write_atomic(&path, json.as_bytes())?;
        Ok(())
    }

    pub fn verify_segment_complete(
        &self,
        segment_seq: u32,
        expected_len: u64,
        expected_sha: &str,
    ) -> Result<bool, JournalStoreError> {
        let path = self.segment_path(segment_seq);
        if !path.exists() {
            return Ok(false);
        }
        let meta = std::fs::metadata(&path)?;
        if meta.len() != expected_len {
            return Err(JournalStoreError::Conflict);
        }
        let file = File::open(&path)?;
        let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
            .map_err(|e| JournalStoreError::Other(e.to_string()))?;
        if sha != expected_sha {
            return Err(JournalStoreError::Conflict);
        }
        Ok(true)
    }

    pub fn open_segment_writer(
        &self,
        segment_seq: u32,
        append: bool,
    ) -> Result<File, JournalStoreError> {
        let dir = self.session_root.join(SEGMENTS_DIR);
        std::fs::create_dir_all(&dir)?;
        sync_dir(&dir)?;
        let path = self.segment_path(segment_seq);
        if !append && path.exists() {
            let _ = std::fs::remove_file(&path);
        }
        OpenOptions::new()
            .create(true)
            .append(append)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(JournalStoreError::Io)
    }

    pub fn disk_preflight_bytes(
        manifest: &LocalFinalizationManifest,
        reserve: u64,
    ) -> Result<u64, StoragePreflightFailure> {
        let raw = manifest
            .segments
            .iter()
            .try_fold(0u64, |acc, s| acc.checked_add(s.byte_length))
            .ok_or(StoragePreflightFailure::RequiredBytesOverflow)?;
        let wav_each = crate::voice_delivery::finalized_manifest::expected_wav_file_bytes(
            manifest.target_sample_count_48k,
        );
        let user_count = manifest.users.len() as u64;
        let wav_total = wav_each
            .checked_mul(user_count)
            .ok_or(StoragePreflightFailure::RequiredBytesOverflow)?;
        let with_reserve = raw
            .checked_add(wav_total)
            .and_then(|v| v.checked_add(reserve))
            .and_then(|v| v.checked_add(8 * 1024 * 1024))
            .ok_or(StoragePreflightFailure::RequiredBytesOverflow)?;
        Ok(with_reserve)
    }

    pub fn run_storage_preflight(
        recording_parent: &Path,
        manifest: &LocalFinalizationManifest,
        reserve: u64,
    ) -> Result<(), StoragePreflightFailure> {
        let required = Self::disk_preflight_bytes(manifest, reserve)?;
        storage_preflight(recording_parent, required)
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), JournalStoreError> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    if path.exists() {
        return Err(JournalStoreError::Conflict);
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod preflight_tests {
    use super::*;
    use crate::voice_delivery::local_finalization::manifest::{
        LocalFinalizationManifest, LocalFinalizationObjectRef, LocalFinalizationOutputPolicy,
        LocalFinalizationSegment, LocalFinalizationUser,
        LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    };

    fn minimal_manifest(
        segments: Vec<LocalFinalizationSegment>,
        users: usize,
    ) -> LocalFinalizationManifest {
        let user = LocalFinalizationUser {
            discord_user_id: "123456789012345678".into(),
            path_nick: "u".into(),
            opaque_stem_id: "a".repeat(32),
        };
        LocalFinalizationManifest {
            version: 1,
            finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
            session_id: "sess".into(),
            guild_id: "g".into(),
            channel_id: "c".into(),
            session_start_wall_ms: 0,
            session_start_monotonic_ns: "0".into(),
            sealed_stop_wall_ms: 0,
            sealed_stop_monotonic_ns: "0".into(),
            target_sample_count_48k: 48_000,
            journal_sha256: "j".into(),
            recording_manifest: LocalFinalizationObjectRef {
                key: "k".into(),
                sha256: "h".into(),
                byte_length: 0,
            },
            sealed_index: LocalFinalizationObjectRef {
                key: "k".into(),
                sha256: "h".into(),
                byte_length: 0,
            },
            segments,
            users: (0..users).map(|_| user.clone()).collect(),
            output: LocalFinalizationOutputPolicy {
                wav_format: crate::voice_delivery::local_finalization::manifest::LOCAL_FINALIZATION_WAV_FORMAT.into(),
                recovery_report_relative_path: crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH.into(),
            },
        }
    }

    #[test]
    fn disk_preflight_bytes_overflow_on_segment_sum() {
        let manifest = minimal_manifest(
            vec![
                LocalFinalizationSegment {
                    segment_seq: 0,
                    key: "k".into(),
                    byte_length: u64::MAX,
                    sha256: "s".into(),
                },
                LocalFinalizationSegment {
                    segment_seq: 1,
                    key: "k2".into(),
                    byte_length: 1,
                    sha256: "s".into(),
                },
            ],
            1,
        );
        let err = LocalJournalStore::disk_preflight_bytes(&manifest, 0).unwrap_err();
        assert_eq!(err, StoragePreflightFailure::RequiredBytesOverflow);
    }
}

fn sync_dir(path: &Path) -> Result<(), JournalStoreError> {
    #[cfg(unix)]
    {
        let f = File::open(path)?;
        f.sync_all()?;
    }
    #[cfg(windows)]
    {
        let _ = path;
    }
    Ok(())
}
