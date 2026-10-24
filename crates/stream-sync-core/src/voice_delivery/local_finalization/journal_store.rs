//! Durable `.in-sync/<session-id>/` journal ingest under the voice recording parent.

use super::manifest::LocalFinalizationManifest;
use super::paths::resolve_durable_publication_authority;
use super::receipt::{parse_local_finalization_receipt, LocalFinalizationReceipt};
use super::storage_preflight::{storage_preflight, StoragePreflightFailure};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const IN_SYNC_DIR: &str = ".in-sync";
const CHECKPOINT_FILE: &str = "download-checkpoint.json";
const CHECKPOINT_BAK_FILE: &str = "download-checkpoint.json.bak";
/// Legacy fixed temp from immutable `write_atomic` (`download-checkpoint.json` → `.tmp` extension).
const CHECKPOINT_LEGACY_TMP: &str = "download-checkpoint.tmp";
const MANIFEST_FILE: &str = "manifest.json";
const RECEIPT_INTENT_FILE: &str = "receipt-intent.json";
const RECEIPT_ACK_FILE: &str = "receipt-ack.json";
const SEGMENTS_DIR: &str = "journal/segments";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptDurableState {
    pub intent: Option<LocalFinalizationReceipt>,
    pub ack: Option<LocalFinalizationReceipt>,
}

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
            let existing_bytes = std::fs::read(&path)?;
            let existing =
                std::str::from_utf8(&existing_bytes).map_err(|_| JournalStoreError::Conflict)?;
            let parsed: serde_json::Value =
                serde_json::from_str(existing).map_err(|_| JournalStoreError::Conflict)?;
            let existing_manifest = super::manifest::parse_local_finalization_manifest(&parsed)
                .map_err(|_| JournalStoreError::Conflict)?;
            if existing_manifest != *manifest {
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
        let primary = self.session_root.join(CHECKPOINT_FILE);
        let backup = self.session_root.join(CHECKPOINT_BAK_FILE);

        if primary.exists() {
            if let Ok(cp) = parse_checkpoint_file(&primary) {
                cleanup_legacy_checkpoint_tmp(&self.session_root)?;
                return Ok(cp);
            }
        }

        if backup.exists() {
            match parse_checkpoint_file(&backup) {
                Ok(cp) => {
                    if primary.exists() {
                        std::fs::remove_file(&primary)?;
                    }
                    std::fs::rename(&backup, &primary)?;
                    sync_dir(&self.session_root)?;
                    cleanup_legacy_checkpoint_tmp(&self.session_root)?;
                    return Ok(cp);
                }
                Err(_) if primary.exists() => return Err(JournalStoreError::Conflict),
                Err(e) => return Err(e),
            }
        }

        if primary.exists() {
            return Err(JournalStoreError::Conflict);
        }

        cleanup_legacy_checkpoint_tmp(&self.session_root)?;
        Ok(DownloadCheckpoint {
            segments: Vec::new(),
        })
    }

    pub fn save_checkpoint(
        &self,
        checkpoint: &DownloadCheckpoint,
    ) -> Result<(), JournalStoreError> {
        let json = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| JournalStoreError::Other(e.to_string()))?;
        let unique_tmp = self
            .session_root
            .join(format!("download-checkpoint.{}.tmp", uuid::Uuid::new_v4()));
        {
            let mut f = File::create(&unique_tmp)?;
            f.write_all(json.as_bytes())?;
            f.sync_all()?;
        }
        checkpoint_commit_replace(&self.session_root, &unique_tmp)?;
        Ok(())
    }

    /// Bounded checkpoint shape vs manifest: unique known segment seqs, bytes ≤ expected.
    /// On-disk segment length and full-file SHA remain authoritative at resume/verify time.
    pub fn validate_checkpoint_against_manifest(
        checkpoint: &DownloadCheckpoint,
        manifest: &LocalFinalizationManifest,
    ) -> Result<(), JournalStoreError> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for entry in &checkpoint.segments {
            if !seen.insert(entry.segment_seq) {
                return Err(JournalStoreError::Conflict);
            }
            let expected = manifest
                .segments
                .iter()
                .find(|s| s.segment_seq == entry.segment_seq);
            match expected {
                None => return Err(JournalStoreError::Conflict),
                Some(seg) => {
                    if entry.bytes_complete > seg.byte_length {
                        return Err(JournalStoreError::Conflict);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn segment_bytes_on_disk(&self, segment_seq: u32) -> Result<u64, JournalStoreError> {
        let path = self.segment_path(segment_seq);
        if !path.exists() {
            return Ok(0);
        }
        Ok(std::fs::metadata(&path)?.len())
    }

    /// Resume offset for an in-progress segment: durable on-disk byte length (≤ expected).
    /// Checkpoint values may lag or run ahead after a crash; only the file prefix is resumed.
    pub fn reconcile_segment_resume_bytes(
        &self,
        segment_seq: u32,
        expected_len: u64,
    ) -> Result<u64, JournalStoreError> {
        let actual = self.segment_bytes_on_disk(segment_seq)?;
        if actual > expected_len {
            return Err(JournalStoreError::Conflict);
        }
        Ok(actual)
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
        let len = meta.len();
        if len < expected_len {
            return Ok(false);
        }
        if len > expected_len {
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

    pub fn load_receipt_durable_state(&self) -> Result<ReceiptDurableState, JournalStoreError> {
        let intent_path = self.session_root.join(RECEIPT_INTENT_FILE);
        let ack_path = self.session_root.join(RECEIPT_ACK_FILE);
        let intent = if intent_path.exists() {
            Some(load_receipt_file(&intent_path)?)
        } else {
            None
        };
        let ack = if ack_path.exists() {
            Some(load_receipt_file(&ack_path)?)
        } else {
            None
        };
        if ack.is_some() && intent.is_none() {
            return Err(JournalStoreError::Conflict);
        }
        if let (Some(i), Some(a)) = (&intent, &ack) {
            if i != a {
                return Err(JournalStoreError::Conflict);
            }
        }
        Ok(ReceiptDurableState { intent, ack })
    }

    /// Durable receipt body to POST after publication; write-once with exact equality on replay.
    pub fn persist_receipt_intent(
        &self,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), JournalStoreError> {
        let path = self.session_root.join(RECEIPT_INTENT_FILE);
        if path.exists() {
            let existing = load_receipt_file(&path)?;
            if existing != *receipt {
                return Err(JournalStoreError::Conflict);
            }
            return Ok(());
        }
        write_receipt_atomic(&path, receipt)?;
        Ok(())
    }

    /// Durable acknowledgement bound to the persisted intent; must match intent exactly.
    pub fn persist_receipt_ack(
        &self,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), JournalStoreError> {
        let intent_path = self.session_root.join(RECEIPT_INTENT_FILE);
        if !intent_path.exists() {
            return Err(JournalStoreError::Conflict);
        }
        let intent = load_receipt_file(&intent_path)?;
        if intent != *receipt {
            return Err(JournalStoreError::Conflict);
        }
        let ack_path = self.session_root.join(RECEIPT_ACK_FILE);
        if ack_path.exists() {
            let existing = load_receipt_file(&ack_path)?;
            if existing != *receipt {
                return Err(JournalStoreError::Conflict);
            }
            return Ok(());
        }
        write_receipt_atomic(&ack_path, receipt)?;
        Ok(())
    }

    pub fn verify_canonical_publication_matches_receipt(
        recording_parent: &Path,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), JournalStoreError> {
        let authority =
            resolve_durable_publication_authority(recording_parent, &receipt.session_id)
                .map_err(|_| JournalStoreError::Conflict)?;
        if receipt.output_manifest_digest != authority.output_manifest_digest {
            return Err(JournalStoreError::Conflict);
        }
        let paths = &authority.paths;
        let session_dir = &paths.final_session_dir;
        if !session_dir.is_dir() {
            return Err(JournalStoreError::Conflict);
        }
        for stem in &receipt.stems {
            let file_path = session_dir.join(&stem.relative_path);
            verify_file_digest(&file_path, stem.byte_length, &stem.sha256)?;
        }
        if let Some(report) = &receipt.recovery_report {
            let file_path = paths
                .recovery_report_path
                .as_ref()
                .filter(|p| p.is_file())
                .cloned()
                .unwrap_or_else(|| session_dir.join(&report.relative_path));
            verify_file_digest(&file_path, report.byte_length, &report.sha256)?;
        }
        Ok(())
    }
}

fn load_receipt_file(path: &Path) -> Result<LocalFinalizationReceipt, JournalStoreError> {
    let raw = std::fs::read_to_string(path)?;
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|_| JournalStoreError::Conflict)?;
    parse_local_finalization_receipt(&value).map_err(|_| JournalStoreError::Conflict)
}

fn write_receipt_atomic(
    path: &Path,
    receipt: &LocalFinalizationReceipt,
) -> Result<(), JournalStoreError> {
    let value =
        serde_json::to_value(receipt).map_err(|e| JournalStoreError::Other(e.to_string()))?;
    parse_local_finalization_receipt(&value).map_err(|_| JournalStoreError::Conflict)?;
    let json = serde_json::to_string_pretty(receipt)
        .map_err(|e| JournalStoreError::Other(e.to_string()))?;
    write_atomic(path, json.as_bytes())
}

fn verify_file_digest(
    path: &Path,
    expected_len: u64,
    expected_sha: &str,
) -> Result<(), JournalStoreError> {
    if !path.is_file() {
        return Err(JournalStoreError::Conflict);
    }
    let meta = std::fs::metadata(path)?;
    if meta.len() != expected_len {
        return Err(JournalStoreError::Conflict);
    }
    let file = File::open(path)?;
    let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
        .map_err(|e| JournalStoreError::Other(e.to_string()))?;
    if sha != expected_sha {
        return Err(JournalStoreError::Conflict);
    }
    Ok(())
}

fn parse_checkpoint_file(path: &Path) -> Result<DownloadCheckpoint, JournalStoreError> {
    let raw = std::fs::read_to_string(path)?;
    serde_json::from_str(&raw).map_err(|e| JournalStoreError::Other(e.to_string()))
}

fn cleanup_legacy_checkpoint_tmp(session_root: &Path) -> Result<(), JournalStoreError> {
    let legacy = session_root.join(CHECKPOINT_LEGACY_TMP);
    if legacy.exists() {
        std::fs::remove_file(&legacy)?;
    }
    Ok(())
}

/// Durable checkpoint replacement: unique temp → primary, prior primary → `.bak`.
/// On commit failure restores `.bak` to primary when primary is absent.
fn checkpoint_commit_replace(
    session_root: &Path,
    unique_tmp: &Path,
) -> Result<(), JournalStoreError> {
    let primary = session_root.join(CHECKPOINT_FILE);
    let backup = session_root.join(CHECKPOINT_BAK_FILE);

    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }

    let rotated_primary_to_bak = if primary.exists() {
        std::fs::rename(&primary, &backup)?;
        true
    } else {
        false
    };

    match std::fs::rename(unique_tmp, &primary) {
        Ok(()) => {
            sync_dir(session_root)?;
            let _ = std::fs::remove_file(unique_tmp);
            Ok(())
        }
        Err(e) => {
            if rotated_primary_to_bak && !primary.exists() && backup.exists() {
                let _ = std::fs::rename(&backup, &primary);
                let _ = sync_dir(session_root);
            }
            let _ = std::fs::remove_file(unique_tmp);
            Err(JournalStoreError::Io(e))
        }
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

    fn open_test_store(dir: &Path) -> LocalJournalStore {
        LocalJournalStore::open(dir, "test-session").expect("open store")
    }

    fn write_segment(store: &LocalJournalStore, seq: u32, bytes: &[u8]) {
        let path = store.segment_path(seq);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn zero_byte_segment_is_incomplete_not_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        write_segment(&store, 0, &[]);
        assert!(!store
            .verify_segment_complete(0, 100, &"a".repeat(64))
            .unwrap());
        assert_eq!(store.reconcile_segment_resume_bytes(0, 100).unwrap(), 0);
    }

    #[test]
    fn partial_segment_resumes_from_file_length() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let partial = vec![0u8; 50];
        write_segment(&store, 0, &partial);
        assert!(!store
            .verify_segment_complete(0, 100, &"a".repeat(64))
            .unwrap());
        assert_eq!(store.reconcile_segment_resume_bytes(0, 100).unwrap(), 50);
    }

    #[test]
    fn stale_checkpoint_reconciles_to_shorter_on_disk_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        write_segment(&store, 0, &[1, 2, 3]);
        let cp = DownloadCheckpoint {
            segments: vec![SegmentCheckpoint {
                segment_seq: 0,
                bytes_complete: 9_999,
                sha256: None,
                complete: false,
            }],
        };
        store.save_checkpoint(&cp).unwrap();
        assert_eq!(store.reconcile_segment_resume_bytes(0, 100).unwrap(), 3);
    }

    #[test]
    fn overlength_segment_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        write_segment(&store, 0, &[0; 101]);
        assert!(matches!(
            store.verify_segment_complete(0, 100, &"a".repeat(64)),
            Err(JournalStoreError::Conflict)
        ));
        assert!(matches!(
            store.reconcile_segment_resume_bytes(0, 100),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn exact_length_wrong_hash_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let body = b"hello segment";
        write_segment(&store, 0, body);
        let sha = sha256_hex_reader(std::io::Cursor::new(body), DEFAULT_STREAM_CHUNK).unwrap();
        assert!(matches!(
            store.verify_segment_complete(0, body.len() as u64, &"0".repeat(64)),
            Err(JournalStoreError::Conflict)
        ));
        assert!(store
            .verify_segment_complete(0, body.len() as u64, &sha)
            .unwrap());
    }

    #[test]
    fn open_segment_writer_clears_zero_byte_file_on_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let _ = store.open_segment_writer(0, false).unwrap();
        assert_eq!(store.segment_bytes_on_disk(0).unwrap(), 0);
        let _ = store.open_segment_writer(0, false).unwrap();
        assert!(!store.segment_path(0).exists() || store.segment_bytes_on_disk(0).unwrap() == 0);
        assert!(!store
            .verify_segment_complete(0, 10, &"a".repeat(64))
            .unwrap());
    }

    fn sample_checkpoint(bytes_complete: u64) -> DownloadCheckpoint {
        DownloadCheckpoint {
            segments: vec![SegmentCheckpoint {
                segment_seq: 0,
                bytes_complete,
                sha256: None,
                complete: false,
            }],
        }
    }

    #[test]
    fn checkpoint_first_save_and_load() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let cp = sample_checkpoint(12);
        store.save_checkpoint(&cp).unwrap();
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 12);
        assert!(store.session_root().join(CHECKPOINT_FILE).exists());
    }

    #[test]
    fn checkpoint_second_save_replaces_without_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        store.save_checkpoint(&sample_checkpoint(1)).unwrap();
        store.save_checkpoint(&sample_checkpoint(99)).unwrap();
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 99);
        assert!(store.session_root().join(CHECKPOINT_BAK_FILE).exists());
    }

    #[test]
    fn checkpoint_stale_legacy_tmp_ignored_when_primary_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        store.save_checkpoint(&sample_checkpoint(5)).unwrap();
        let legacy = store.session_root().join(CHECKPOINT_LEGACY_TMP);
        std::fs::write(&legacy, b"not authoritative").unwrap();
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 5);
        assert!(!legacy.exists());
    }

    #[test]
    fn checkpoint_load_promotes_valid_bak_when_primary_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let root = store.session_root();
        let json = serde_json::to_string_pretty(&sample_checkpoint(42)).unwrap();
        std::fs::write(root.join(CHECKPOINT_BAK_FILE), json).unwrap();
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 42);
        assert!(root.join(CHECKPOINT_FILE).exists());
        assert!(!root.join(CHECKPOINT_BAK_FILE).exists());
    }

    #[test]
    fn checkpoint_load_promotes_valid_bak_when_primary_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let root = store.session_root();
        std::fs::write(root.join(CHECKPOINT_FILE), b"{not json").unwrap();
        let json = serde_json::to_string_pretty(&sample_checkpoint(7)).unwrap();
        std::fs::write(root.join(CHECKPOINT_BAK_FILE), json).unwrap();
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 7);
    }

    #[test]
    fn checkpoint_load_fails_when_primary_and_bak_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let root = store.session_root();
        std::fs::write(root.join(CHECKPOINT_FILE), b"{bad").unwrap();
        std::fs::write(root.join(CHECKPOINT_BAK_FILE), b"{also bad").unwrap();
        assert!(matches!(
            store.load_checkpoint(),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn checkpoint_commit_failure_restores_primary_from_bak() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let root = store.session_root();
        let v1 = serde_json::to_string_pretty(&sample_checkpoint(11)).unwrap();
        std::fs::write(root.join(CHECKPOINT_FILE), v1).unwrap();
        let missing_tmp = root.join("download-checkpoint.nope.tmp");
        let err = checkpoint_commit_replace(root, &missing_tmp);
        assert!(err.is_err());
        assert!(root.join(CHECKPOINT_FILE).exists());
        let loaded = store.load_checkpoint().unwrap();
        assert_eq!(loaded.segments[0].bytes_complete, 11);
    }

    #[test]
    fn validate_checkpoint_rejects_unknown_segment_and_duplicate_seq() {
        let manifest = minimal_manifest(
            vec![LocalFinalizationSegment {
                segment_seq: 0,
                key: "k".into(),
                byte_length: 100,
                sha256: "s".into(),
            }],
            1,
        );
        let dup = DownloadCheckpoint {
            segments: vec![
                SegmentCheckpoint {
                    segment_seq: 0,
                    bytes_complete: 1,
                    sha256: None,
                    complete: false,
                },
                SegmentCheckpoint {
                    segment_seq: 0,
                    bytes_complete: 2,
                    sha256: None,
                    complete: false,
                },
            ],
        };
        assert!(matches!(
            LocalJournalStore::validate_checkpoint_against_manifest(&dup, &manifest),
            Err(JournalStoreError::Conflict)
        ));
        let unknown = DownloadCheckpoint {
            segments: vec![SegmentCheckpoint {
                segment_seq: 9,
                bytes_complete: 1,
                sha256: None,
                complete: false,
            }],
        };
        assert!(matches!(
            LocalJournalStore::validate_checkpoint_against_manifest(&unknown, &manifest),
            Err(JournalStoreError::Conflict)
        ));
        let over = DownloadCheckpoint {
            segments: vec![SegmentCheckpoint {
                segment_seq: 0,
                bytes_complete: 101,
                sha256: None,
                complete: false,
            }],
        };
        assert!(matches!(
            LocalJournalStore::validate_checkpoint_against_manifest(&over, &manifest),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn receipt_state_absent_when_no_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let state = store.load_receipt_durable_state().unwrap();
        assert!(state.intent.is_none());
        assert!(state.ack.is_none());
    }

    fn sample_receipt(session_id: &str) -> LocalFinalizationReceipt {
        use crate::voice_delivery::local_finalization::manifest::LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION;
        use crate::voice_delivery::local_finalization::receipt::{
            LocalFinalizationReceiptRecoveryReport, LocalFinalizationReceiptStem,
            LOCAL_FINALIZATION_RECEIPT_VERSION,
        };
        use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
        LocalFinalizationReceipt {
            version: LOCAL_FINALIZATION_RECEIPT_VERSION,
            session_id: session_id.into(),
            delivery_manifest_digest: "a".repeat(64),
            finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
            device_id: "device".into(),
            completed_at_ms: 1_700_000_000_000,
            output_manifest_digest: "b".repeat(64),
            stems: vec![LocalFinalizationReceiptStem {
                discord_user_id: "123456789012345678".into(),
                relative_path: "player.wav".into(),
                byte_length: 100,
                sha256: "a".repeat(64),
            }],
            recovery_report: Some(LocalFinalizationReceiptRecoveryReport {
                relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
                byte_length: 50,
                sha256: "b".repeat(64),
            }),
            discarded_packet_count: 0,
            silence_range_count: 0,
        }
    }

    #[test]
    fn receipt_intent_persist_and_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        let state = store.load_receipt_durable_state().unwrap();
        assert_eq!(state.intent.as_ref(), Some(&receipt));
        assert!(state.ack.is_none());
        store.persist_receipt_intent(&receipt).unwrap();
    }

    #[test]
    fn receipt_intent_mismatch_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        let mut other = receipt.clone();
        other.completed_at_ms += 1;
        assert!(matches!(
            store.persist_receipt_intent(&other),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn receipt_ack_without_intent_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        let ack_path = store.session_root().join(RECEIPT_ACK_FILE);
        std::fs::write(&ack_path, serde_json::to_string_pretty(&receipt).unwrap()).unwrap();
        assert!(matches!(
            store.load_receipt_durable_state(),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn receipt_ack_must_match_intent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        store.persist_receipt_ack(&receipt).unwrap();
        let state = store.load_receipt_durable_state().unwrap();
        assert_eq!(state.intent, state.ack);
        let mut other = receipt.clone();
        other.completed_at_ms += 1;
        assert!(matches!(
            store.persist_receipt_ack(&other),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn receipt_corrupt_json_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let intent_path = store.session_root().join(RECEIPT_INTENT_FILE);
        std::fs::write(&intent_path, b"{not valid receipt").unwrap();
        assert!(matches!(
            store.load_receipt_durable_state(),
            Err(JournalStoreError::Conflict)
        ));
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
