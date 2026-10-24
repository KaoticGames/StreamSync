//! Durable `.in-sync/<session-id>/` journal ingest under the voice recording parent.

use super::manifest::LocalFinalizationManifest;
use super::paths::{resolve_durable_publication_authority, DurablePublicationAuthority};
use super::receipt::{
    parse_local_finalization_receipt, LocalFinalizationReceipt,
    LocalFinalizationReceiptRecoveryReport, LocalFinalizationReceiptStem,
};
use super::storage_preflight::{storage_preflight, StoragePreflightFailure};
use crate::voice_delivery::fs::durability::{sync_dir_exact, NamespaceDurability};
use crate::voice_delivery::fs::durable_payload::write_durable_payload_create_new;
use crate::voice_delivery::fs::{
    open_lock_file_at_root, rename_no_replace_same_parent, DestRoot, DirHandle, FsError,
};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::publication::verify_final_session_directory;
use crate::voice_delivery::session::DeliverySessionGuard;
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
pub(crate) const RECEIPT_INSTALL_LOCK: &str = ".receipt-install.lock";
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
        let root = DestRoot::open(&self.session_root).map_err(journal_from_fs)?;
        let dir = root.handle();
        let intent = load_optional_receipt_from_dir(dir, RECEIPT_INTENT_FILE)?;
        let ack = load_optional_receipt_from_dir(dir, RECEIPT_ACK_FILE)?;
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
        persist_receipt_create_new(&self.session_root, RECEIPT_INTENT_FILE, receipt)
    }

    /// Durable acknowledgement bound to the persisted intent; must match intent exactly.
    pub fn persist_receipt_ack(
        &self,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), JournalStoreError> {
        let root = DestRoot::open(&self.session_root).map_err(journal_from_fs)?;
        let dir = root.handle();
        let intent = match dir.read_file_all(RECEIPT_INTENT_FILE) {
            Ok(bytes) => parse_receipt_bytes(&bytes)?,
            Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(JournalStoreError::Conflict);
            }
            Err(e) => return Err(journal_from_fs(e)),
        };
        if intent != *receipt {
            return Err(JournalStoreError::Conflict);
        }
        persist_receipt_create_new(&self.session_root, RECEIPT_ACK_FILE, receipt)
    }

    pub fn verify_canonical_publication_matches_receipt(
        recording_parent: &Path,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), JournalStoreError> {
        let authority =
            resolve_durable_publication_authority(recording_parent, &receipt.session_id)
                .map_err(|_| JournalStoreError::Conflict)?;
        verify_receipt_against_publication_authority(&authority, receipt)?;
        verify_published_artifacts_from_authority(recording_parent, &authority)?;
        Ok(())
    }
}

pub(crate) fn verify_receipt_against_publication_authority(
    authority: &DurablePublicationAuthority,
    receipt: &LocalFinalizationReceipt,
) -> Result<(), JournalStoreError> {
    if receipt.session_id != authority.manifest.session_id {
        return Err(JournalStoreError::Conflict);
    }
    if receipt.output_manifest_digest != authority.output_manifest_digest {
        return Err(JournalStoreError::Conflict);
    }
    verify_receipt_stems_match_authority_manifest(&authority.manifest, &receipt.stems)?;
    verify_receipt_recovery_report_matches_authority(
        authority.manifest.recovery_report.as_ref(),
        receipt.recovery_report.as_ref(),
    )?;
    Ok(())
}

fn verify_receipt_stems_match_authority_manifest(
    manifest: &crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest,
    receipt_stems: &[LocalFinalizationReceiptStem],
) -> Result<(), JournalStoreError> {
    let mut unmatched = receipt_stems.to_vec();
    for auth_stem in &manifest.stems {
        let idx = unmatched.iter().position(|r| {
            r.discord_user_id == auth_stem.discord_user_id
                && r.relative_path == auth_stem.wav_relative_path
                && r.byte_length == auth_stem.wav_bytes
                && r.sha256 == auth_stem.sha256
        });
        match idx {
            Some(i) => {
                unmatched.remove(i);
            }
            None => return Err(JournalStoreError::Conflict),
        }
    }
    if !unmatched.is_empty() {
        return Err(JournalStoreError::Conflict);
    }
    Ok(())
}

fn verify_receipt_recovery_report_matches_authority(
    authority_report: Option<&crate::voice_delivery::finalized_manifest::SyndicateRecoveryReport>,
    receipt_report: Option<&LocalFinalizationReceiptRecoveryReport>,
) -> Result<(), JournalStoreError> {
    match (authority_report, receipt_report) {
        (None, None) => Ok(()),
        (Some(auth), Some(rec)) => {
            if rec.relative_path != auth.relative_path
                || rec.byte_length != auth.byte_length
                || rec.sha256 != auth.sha256
            {
                return Err(JournalStoreError::Conflict);
            }
            Ok(())
        }
        _ => Err(JournalStoreError::Conflict),
    }
}

fn verify_published_artifacts_from_authority(
    recording_parent: &Path,
    authority: &DurablePublicationAuthority,
) -> Result<(), JournalStoreError> {
    let root = DestRoot::open(recording_parent).map_err(|_| JournalStoreError::Conflict)?;
    let guard = DeliverySessionGuard::begin_for_recovery(
        root,
        authority.ledger_identity.clone(),
        authority.validated_manifest.clone(),
        authority.recovery_report.clone(),
        true,
    )
    .map_err(|_| JournalStoreError::Conflict)?;
    verify_final_session_directory(
        guard.final_parent_dir(),
        guard.identity(),
        guard.manifest(),
        guard.recovery_report(),
    )
    .map_err(|_| JournalStoreError::Conflict)?;
    Ok(())
}

fn load_optional_receipt_from_dir(
    dir: &DirHandle,
    name: &str,
) -> Result<Option<LocalFinalizationReceipt>, JournalStoreError> {
    match dir.read_file_all(name) {
        Ok(bytes) => Ok(Some(parse_receipt_bytes(&bytes)?)),
        Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(journal_from_fs(e)),
    }
}

fn receipt_json_bytes(receipt: &LocalFinalizationReceipt) -> Result<Vec<u8>, JournalStoreError> {
    let value =
        serde_json::to_value(receipt).map_err(|e| JournalStoreError::Other(e.to_string()))?;
    parse_local_finalization_receipt(&value).map_err(|_| JournalStoreError::Conflict)?;
    serde_json::to_string_pretty(receipt)
        .map_err(|e| JournalStoreError::Other(e.to_string()))
        .map(|s| s.into_bytes())
}

fn parse_receipt_bytes(bytes: &[u8]) -> Result<LocalFinalizationReceipt, JournalStoreError> {
    let raw = std::str::from_utf8(bytes).map_err(|_| JournalStoreError::Conflict)?;
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| JournalStoreError::Conflict)?;
    parse_local_finalization_receipt(&value).map_err(|_| JournalStoreError::Conflict)
}

fn journal_from_fs(err: FsError) -> JournalStoreError {
    match err {
        FsError::Io(io) => JournalStoreError::Io(io),
        other => JournalStoreError::Other(other.to_string()),
    }
}

/// RAII exclusive receipt-install lock; file is never deleted on release.
struct ReceiptInstallLockGuard {
    file: File,
}

fn acquire_receipt_install_lock(
    session_root: &Path,
) -> Result<ReceiptInstallLockGuard, JournalStoreError> {
    use fs2::FileExt;

    let root = DestRoot::open(session_root).map_err(journal_from_fs)?;
    let file = open_lock_file_at_root(&root, &[RECEIPT_INSTALL_LOCK.to_string()])
        .map_err(journal_from_fs)?;
    file.lock_exclusive().map_err(JournalStoreError::Io)?;
    Ok(ReceiptInstallLockGuard { file })
}

impl Drop for ReceiptInstallLockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

fn require_proven_namespace_durability(
    durability: NamespaceDurability,
) -> Result<(), JournalStoreError> {
    match durability {
        NamespaceDurability::Proven => Ok(()),
        NamespaceDurability::Unavailable => Err(JournalStoreError::Other(
            "receipt install requires proven session directory durability".into(),
        )),
    }
}

fn receipt_stage_install_rename(
    dir: &DirHandle,
    stage_name: &str,
    dest_name: &str,
) -> Result<(), FsError> {
    #[cfg(windows)]
    {
        use crate::voice_delivery::fs::rename_no_replace_write_through_same_parent;
        rename_no_replace_write_through_same_parent(dir, stage_name, dest_name)
    }
    #[cfg(not(windows))]
    {
        rename_no_replace_same_parent(dir, stage_name, dest_name)
    }
}

/// After an exact-equal collision, Linux proves namespace durability via `sync_dir_exact`.
#[cfg(not(windows))]
fn receipt_install_collision_namespace_barrier(dir: &DirHandle) -> Result<(), JournalStoreError> {
    let durability = sync_dir_exact(dir).map_err(journal_from_fs)?;
    require_proven_namespace_durability(durability)
}

/// Exact-equal collision on Windows: lock + no-follow readback; optional flush of the destination file.
#[cfg(windows)]
fn receipt_install_collision_namespace_barrier(
    dir: &DirHandle,
    dest_name: &str,
) -> Result<(), JournalStoreError> {
    use crate::voice_delivery::fs::durability::sync_file;
    use crate::voice_delivery::fs::file::open_existing_file_at;

    // Defense in depth only; write-through winning moves + the install lock are the namespace proof.
    let file = open_existing_file_at(dir, dest_name).map_err(journal_from_fs)?;
    sync_file(&file).map_err(journal_from_fs)
}

/// Create-new durable receipt install: unique stage file → platform no-replace install under lock.
fn persist_receipt_create_new(
    session_root: &Path,
    dest_name: &str,
    receipt: &LocalFinalizationReceipt,
) -> Result<(), JournalStoreError> {
    let json_bytes = receipt_json_bytes(receipt)?;
    let root = DestRoot::open(session_root).map_err(journal_from_fs)?;
    let dir = root.handle();
    let _install_lock = acquire_receipt_install_lock(session_root)?;
    let stage_name = format!(".receipt-write-{}", uuid::Uuid::new_v4().simple());

    if let Err(e) = write_durable_payload_create_new(dir, &stage_name, &json_bytes) {
        let _ = dir.remove_child_file(&stage_name);
        return Err(journal_from_fs(e));
    }

    match receipt_stage_install_rename(dir, &stage_name, dest_name) {
        Ok(()) => {
            #[cfg(not(windows))]
            {
                let durability = sync_dir_exact(dir).map_err(journal_from_fs)?;
                require_proven_namespace_durability(durability)?;
            }
            Ok(())
        }
        Err(FsError::AlreadyExists) => {
            let _ = dir.remove_child_file(&stage_name);
            let existing_bytes = dir.read_file_all(dest_name).map_err(journal_from_fs)?;
            match parse_receipt_bytes(&existing_bytes) {
                Ok(existing) if existing == *receipt => {
                    #[cfg(not(windows))]
                    receipt_install_collision_namespace_barrier(dir)?;
                    #[cfg(windows)]
                    receipt_install_collision_namespace_barrier(dir, dest_name)?;
                    Ok(())
                }
                _ => Err(JournalStoreError::Conflict),
            }
        }
        Err(e) => {
            let _ = dir.remove_child_file(&stage_name);
            Err(journal_from_fs(e))
        }
    }
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
    use std::sync::{Arc, Barrier};
    use std::thread;

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

    fn sample_publication_authority(
        session_dir: &Path,
        manifest: crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest,
        digest: &str,
    ) -> DurablePublicationAuthority {
        use crate::voice_delivery::identity::DeliveryImmutableIdentity;
        use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};

        let validated_manifest = ValidatedManifest {
            schema_version: 1,
            stems: manifest
                .stems
                .iter()
                .map(|s| StemManifestEntry {
                    file_name: s.wav_relative_path.clone(),
                    byte_count: s.wav_bytes,
                    sha256: s.sha256.clone(),
                })
                .collect(),
        };
        let ledger_identity = DeliveryImmutableIdentity::new_bound(
            &manifest.session_id,
            digest,
            &validated_manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final-only",
            None,
        )
        .expect("sample publication authority identity");
        let recovery_report_path = manifest
            .recovery_report
            .as_ref()
            .map(|r| session_dir.join(&r.relative_path));
        DurablePublicationAuthority {
            paths: crate::voice_delivery::local_finalization::paths::PublishedSessionPaths {
                final_session_dir: session_dir.to_path_buf(),
                recovery_report_path,
            },
            output_manifest_digest: digest.to_string(),
            manifest,
            ledger_identity,
            validated_manifest,
            recovery_report: None,
        }
    }

    fn sample_syndicate_manifest(
        session_id: &str,
        stems: Vec<crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem>,
        recovery: Option<crate::voice_delivery::finalized_manifest::SyndicateRecoveryReport>,
    ) -> crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest {
        crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest {
            version: 1,
            session_id: session_id.into(),
            guild_id: "1".into(),
            channel_id: "2".into(),
            session_start_wall_ms: 0,
            session_start_monotonic_ns: "0".into(),
            sealed_stop_wall_ms: 1,
            sealed_stop_monotonic_ns: "1".into(),
            target_sample_count_48k: 1,
            journal_format_version: 2,
            journal_sha256: "c".repeat(64),
            stems,
            recovery_report: recovery,
        }
    }

    #[test]
    fn publication_authority_rejects_extra_receipt_stem() {
        let tmp = tempfile::tempdir().unwrap();
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let stem = crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem {
            discord_user_id: "123456789012345678".into(),
            path_nick: "player".into(),
            wav_relative_path: "player.wav".into(),
            wav_bytes: 4,
            sha256: "a".repeat(64),
        };
        let manifest = sample_syndicate_manifest(session_id, vec![stem.clone()], None);
        let authority = sample_publication_authority(tmp.path(), manifest, "d".repeat(64).as_str());
        let mut receipt = sample_receipt(session_id);
        receipt.stems.push(receipt.stems[0].clone());
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn publication_authority_rejects_missing_receipt_stem() {
        let tmp = tempfile::tempdir().unwrap();
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let stem = crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem {
            discord_user_id: "123456789012345678".into(),
            path_nick: "player".into(),
            wav_relative_path: "player.wav".into(),
            wav_bytes: 4,
            sha256: "a".repeat(64),
        };
        let manifest = sample_syndicate_manifest(session_id, vec![stem], None);
        let authority = sample_publication_authority(tmp.path(), manifest, "d".repeat(64).as_str());
        let mut receipt = sample_receipt(session_id);
        receipt.stems.clear();
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn publication_authority_rejects_stem_hash_and_path_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let stem = crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem {
            discord_user_id: "123456789012345678".into(),
            path_nick: "player".into(),
            wav_relative_path: "player.wav".into(),
            wav_bytes: 4,
            sha256: "a".repeat(64),
        };
        let manifest = sample_syndicate_manifest(session_id, vec![stem], None);
        let authority = sample_publication_authority(tmp.path(), manifest, "d".repeat(64).as_str());
        let mut receipt = sample_receipt(session_id);
        receipt.stems[0].sha256 = "f".repeat(64);
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
        receipt = sample_receipt(session_id);
        receipt.stems[0].relative_path = "other.wav".into();
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
        receipt = sample_receipt(session_id);
        receipt.stems[0].byte_length = 99;
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn publication_authority_rejects_recovery_report_presence_and_content_mismatch() {
        use crate::voice_delivery::finalized_manifest::SyndicateRecoveryReport;
        use crate::voice_delivery::recovery_report::{
            RECOVERY_REPORT_CONTENT_TYPE, RECOVERY_REPORT_RELATIVE_PATH,
        };

        let tmp = tempfile::tempdir().unwrap();
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let stem = crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem {
            discord_user_id: "123456789012345678".into(),
            path_nick: "player".into(),
            wav_relative_path: "player.wav".into(),
            wav_bytes: 4,
            sha256: "a".repeat(64),
        };
        let report = SyndicateRecoveryReport {
            relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
            byte_length: 50,
            sha256: "b".repeat(64),
            content_type: RECOVERY_REPORT_CONTENT_TYPE.into(),
        };
        let manifest = sample_syndicate_manifest(session_id, vec![stem], Some(report));
        let authority = sample_publication_authority(tmp.path(), manifest, "d".repeat(64).as_str());
        let mut receipt = sample_receipt(session_id);
        receipt.recovery_report = None;
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
        receipt = sample_receipt(session_id);
        receipt.recovery_report.as_mut().unwrap().sha256 = "e".repeat(64);
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
    }

    #[test]
    fn publication_authority_verifies_on_disk_stems_from_manifest_not_receipt_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let body = b"abcd";
        let sha = sha256_hex_reader(std::io::Cursor::new(body), DEFAULT_STREAM_CHUNK).unwrap();
        let stem = crate::voice_delivery::finalized_manifest::SyndicateFinalizedStem {
            discord_user_id: "123456789012345678".into(),
            path_nick: "player".into(),
            wav_relative_path: "player.wav".into(),
            wav_bytes: body.len() as u64,
            sha256: sha.clone(),
        };
        let manifest = sample_syndicate_manifest(session_id, vec![stem], None);
        let authority = sample_publication_authority(tmp.path(), manifest, "d".repeat(64).as_str());
        std::fs::create_dir_all(&authority.paths.final_session_dir).unwrap();
        std::fs::write(authority.paths.final_session_dir.join("player.wav"), body).unwrap();
        let mut receipt = sample_receipt(session_id);
        receipt.output_manifest_digest = authority.output_manifest_digest.clone();
        receipt.stems[0].relative_path = "rogue.wav".into();
        receipt.stems[0].byte_length = body.len() as u64;
        receipt.stems[0].sha256 = sha;
        std::fs::write(authority.paths.final_session_dir.join("rogue.wav"), body).unwrap();
        assert!(matches!(
            super::verify_receipt_against_publication_authority(&authority, &receipt),
            Err(JournalStoreError::Conflict)
        ));
        assert!(
            matches!(
                super::verify_published_artifacts_from_authority(tmp.path(), &authority),
                Err(JournalStoreError::Conflict)
            ),
            "unexpected directory members must fail canonical publication verification"
        );
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

    /// Regression (HIGH): identical receipt AlreadyExists collision must re-sync the session
    /// directory before reporting idempotent Ok; otherwise durability can be skipped permanently.
    #[test]
    #[cfg(target_os = "linux")]
    fn receipt_intent_identical_collision_requires_dir_sync_before_ok() {
        use crate::voice_delivery::fs::durability::fail_next_sync_dir_exact::{
            install_fail_next_sync_dir_exact, INJECTED_MESSAGE,
        };

        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();

        let _guard = install_fail_next_sync_dir_exact();
        let err = store
            .persist_receipt_intent(&receipt)
            .expect_err("identical intent replay must sync directory before Ok");
        assert!(
            matches!(&err, JournalStoreError::Io(io) if io.to_string().contains(INJECTED_MESSAGE)),
            "expected injected sync_dir_exact durability error, got {err:?}"
        );
    }

    /// Winning intent install must not treat `NamespaceDurability::Unavailable` as success (Linux).
    #[test]
    #[cfg(target_os = "linux")]
    fn receipt_intent_winning_install_rejects_unavailable_dir_sync() {
        use crate::voice_delivery::fs::durability::fail_next_sync_dir_exact::install_next_sync_dir_exact_unavailable;

        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        let _guard = install_next_sync_dir_exact_unavailable();
        assert!(
            store.persist_receipt_intent(&receipt).is_err(),
            "winning intent install must fail when directory durability is Unavailable"
        );
    }

    /// Winning ack install must not treat `NamespaceDurability::Unavailable` as success (Linux).
    #[test]
    #[cfg(target_os = "linux")]
    fn receipt_ack_winning_install_rejects_unavailable_dir_sync() {
        use crate::voice_delivery::fs::durability::fail_next_sync_dir_exact::install_next_sync_dir_exact_unavailable;

        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        let _guard = install_next_sync_dir_exact_unavailable();
        assert!(
            store.persist_receipt_ack(&receipt).is_err(),
            "winning ack install must fail when directory durability is Unavailable"
        );
    }

    /// Receipt installs must serialize on the stable session-root lock file.
    #[test]
    #[cfg(target_os = "linux")]
    fn persist_receipt_intent_blocks_on_external_receipt_install_lock() {
        use fs2::FileExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let tmp = tempfile::tempdir().unwrap();
        let recording_parent = tmp.path().to_path_buf();
        let holder = open_test_store(&recording_parent);
        let lock_path = holder.session_root().join(super::RECEIPT_INSTALL_LOCK);
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        lock_file.lock_exclusive().unwrap();

        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);
        let child = std::thread::spawn({
            let recording_parent = recording_parent.clone();
            let receipt = receipt.clone();
            move || {
                let store =
                    LocalJournalStore::open(&recording_parent, "test-session").expect("open");
                started_tx.send(()).expect("signal start");
                let result = store.persist_receipt_intent(&receipt);
                done_tx.send(result).expect("signal done");
            }
        });

        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("child thread must start persist_receipt_intent");
        match done_rx.recv_timeout(Duration::from_millis(750)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Ok(result) => {
                panic!(
                    "persist_receipt_intent must not complete while external receipt-install lock is held; got {result:?}"
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("child exited before releasing external lock");
            }
        }

        lock_file.unlock().unwrap();
        let result = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("persist_receipt_intent must complete after lock release");
        result.expect("persist after lock release");
        child.join().expect("join child");

        let store = open_test_store(&recording_parent);
        let state = store.load_receipt_durable_state().unwrap();
        assert_eq!(state.intent.as_ref(), Some(&receipt));
        assert!(state.ack.is_none());
    }

    /// Same collision-path durability requirement for acknowledgement installs.
    #[test]
    #[cfg(target_os = "linux")]
    fn receipt_ack_identical_collision_requires_dir_sync_before_ok() {
        use crate::voice_delivery::fs::durability::fail_next_sync_dir_exact::{
            install_fail_next_sync_dir_exact, INJECTED_MESSAGE,
        };

        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        store.persist_receipt_ack(&receipt).unwrap();

        let _guard = install_fail_next_sync_dir_exact();
        let err = store
            .persist_receipt_ack(&receipt)
            .expect_err("identical ack replay must sync directory before Ok");
        assert!(
            matches!(&err, JournalStoreError::Io(io) if io.to_string().contains(INJECTED_MESSAGE)),
            "expected injected sync_dir_exact durability error, got {err:?}"
        );
    }

    #[test]
    fn receipt_intent_mismatch_is_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();
        let intent_path = store.session_root().join(RECEIPT_INTENT_FILE);
        let before = std::fs::read(&intent_path).unwrap();
        let mut other = receipt.clone();
        other.completed_at_ms += 1;
        assert!(matches!(
            store.persist_receipt_intent(&other),
            Err(JournalStoreError::Conflict)
        ));
        assert_eq!(std::fs::read(&intent_path).unwrap(), before);
        let state = store.load_receipt_durable_state().unwrap();
        assert_eq!(state.intent.as_ref(), Some(&receipt));
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

    fn spawn_concurrent_receipt_writer(
        store: LocalJournalStore,
        receipt: LocalFinalizationReceipt,
        ack: bool,
        start: Arc<Barrier>,
    ) -> thread::JoinHandle<Result<(), JournalStoreError>> {
        thread::spawn(move || {
            start.wait();
            if ack {
                store.persist_receipt_ack(&receipt)
            } else {
                store.persist_receipt_intent(&receipt)
            }
        })
    }

    fn assert_single_winner_install_semantics(
        receipt_a: &LocalFinalizationReceipt,
        receipt_b: &LocalFinalizationReceipt,
        result_a: Result<(), JournalStoreError>,
        result_b: Result<(), JournalStoreError>,
        durable: &LocalFinalizationReceipt,
    ) {
        let ok_inputs: Vec<&LocalFinalizationReceipt> =
            [(&result_a, receipt_a), (&result_b, receipt_b)]
                .iter()
                .filter_map(|(r, input)| r.as_ref().ok().map(|_| *input))
                .collect();
        let conflicts = [&result_a, &result_b]
            .iter()
            .filter(|r| matches!(r, Err(JournalStoreError::Conflict)))
            .count();
        assert_eq!(
            ok_inputs.len(),
            1,
            "expected exactly one Ok and one Conflict; got results {:?} / {:?}",
            result_a,
            result_b
        );
        assert_eq!(conflicts, 1, "expected Conflict for losing writer");
        assert_eq!(
            ok_inputs[0], durable,
            "successful call input must equal durable receipt body"
        );
        assert_ne!(
            receipt_a, receipt_b,
            "conflicting concurrent installs require distinct valid receipts"
        );
    }

    fn assert_identical_concurrent_both_ok(
        receipt: &LocalFinalizationReceipt,
        result_a: Result<(), JournalStoreError>,
        result_b: Result<(), JournalStoreError>,
        durable: Option<&LocalFinalizationReceipt>,
    ) {
        assert!(
            result_a.is_ok(),
            "identical concurrent persist must return Ok for writer A: {:?}",
            result_a
        );
        assert!(
            result_b.is_ok(),
            "identical concurrent persist must return Ok for writer B: {:?}",
            result_b
        );
        if let Some(durable) = durable {
            assert_eq!(durable, receipt);
        }
    }

    /// Concurrent conflicting intent installs: exactly one winner (create-new + no-replace rename).
    #[test]
    fn receipt_intent_conflicting_concurrent_create_atomic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let mut receipt_a = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        let mut receipt_b = receipt_a.clone();
        receipt_a.completed_at_ms = 1_700_000_000_001;
        receipt_b.completed_at_ms = 1_700_000_000_002;

        let start = Arc::new(Barrier::new(2));
        let store_a = open_test_store(tmp.path());
        let store_b = open_test_store(tmp.path());
        let handle_a =
            spawn_concurrent_receipt_writer(store_a, receipt_a.clone(), false, Arc::clone(&start));
        let handle_b =
            spawn_concurrent_receipt_writer(store_b, receipt_b.clone(), false, Arc::clone(&start));

        let result_a = handle_a.join().expect("writer a join");
        let result_b = handle_b.join().expect("writer b join");
        let durable = store
            .load_receipt_durable_state()
            .expect("load durable state")
            .intent
            .expect("intent installed");

        assert_single_winner_install_semantics(
            &receipt_a, &receipt_b, result_a, result_b, &durable,
        );
    }

    /// Identical concurrent intent installs are idempotent (both Ok).
    #[test]
    fn receipt_intent_identical_concurrent_idempotent_atomic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");

        let start = Arc::new(Barrier::new(2));
        let store_a = open_test_store(tmp.path());
        let store_b = open_test_store(tmp.path());
        let handle_a =
            spawn_concurrent_receipt_writer(store_a, receipt.clone(), false, Arc::clone(&start));
        let handle_b =
            spawn_concurrent_receipt_writer(store_b, receipt.clone(), false, Arc::clone(&start));

        let result_a = handle_a.join().expect("writer a join");
        let result_b = handle_b.join().expect("writer b join");
        let state = store
            .load_receipt_durable_state()
            .expect("load durable state");
        let durable = state.intent.as_ref();

        assert_identical_concurrent_both_ok(&receipt, result_a, result_b, durable);
    }

    /// Identical concurrent ack installs are idempotent (both Ok).
    #[test]
    fn receipt_ack_identical_concurrent_idempotent_atomic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_test_store(tmp.path());
        let receipt = sample_receipt("550e8400-e29b-41d4-a716-446655440099");
        store.persist_receipt_intent(&receipt).unwrap();

        let start = Arc::new(Barrier::new(2));
        let store_a = open_test_store(tmp.path());
        let store_b = open_test_store(tmp.path());
        let handle_a =
            spawn_concurrent_receipt_writer(store_a, receipt.clone(), true, Arc::clone(&start));
        let handle_b =
            spawn_concurrent_receipt_writer(store_b, receipt.clone(), true, Arc::clone(&start));

        let result_a = handle_a.join().expect("writer a join");
        let result_b = handle_b.join().expect("writer b join");
        let state = store
            .load_receipt_durable_state()
            .expect("load durable state");
        assert_identical_concurrent_both_ok(&receipt, result_a, result_b, state.ack.as_ref());
        assert_eq!(state.intent.as_ref(), Some(&receipt));
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

    /// Receipt durable-state loads must not follow symlinks (ambient `exists`/`read`).
    #[cfg(unix)]
    mod receipt_symlink_no_follow_security {
        use super::*;
        use std::os::unix::fs::symlink;

        const SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440099";

        fn write_external_valid_receipt(path: &Path, receipt: &LocalFinalizationReceipt) {
            let json = serde_json::to_string_pretty(receipt).unwrap();
            std::fs::write(path, json).unwrap();
        }

        fn assert_load_fails_closed(store: &LocalJournalStore, receipt: &LocalFinalizationReceipt) {
            let result = store.load_receipt_durable_state();
            assert!(
                result.is_err(),
                "load_receipt_durable_state must fail closed when receipt paths are symlinks; \
                 got {:?} — ambient read followed the link to the external receipt",
                result
            );
            if let Ok(state) = result {
                assert_ne!(
                    state.intent.as_ref(),
                    Some(receipt),
                    "must not return receipt body loaded through receipt-intent.json symlink"
                );
                assert_ne!(
                    state.ack.as_ref(),
                    Some(receipt),
                    "must not return receipt body loaded through receipt-ack.json symlink"
                );
            }
        }

        #[test]
        fn load_receipt_durable_state_rejects_symlinked_receipt_intent() {
            let tmp = tempfile::tempdir().unwrap();
            let store = open_test_store(tmp.path());
            let receipt = sample_receipt(SESSION_ID);
            let external = tmp.path().join("external-receipt-intent.json");
            write_external_valid_receipt(&external, &receipt);
            let external_before = std::fs::read(&external).unwrap();

            let intent_link = store.session_root().join(RECEIPT_INTENT_FILE);
            symlink(&external, &intent_link).unwrap();

            assert_load_fails_closed(&store, &receipt);
            assert_eq!(std::fs::read(&external).unwrap(), external_before);
            assert!(
                intent_link.is_symlink(),
                "session receipt-intent must remain the symlink under test"
            );
            assert!(
                !store.session_root().join(RECEIPT_ACK_FILE).exists(),
                "symlinked intent load must not create receipt-ack.json"
            );
        }

        #[test]
        fn load_receipt_durable_state_rejects_symlinked_receipt_ack() {
            let tmp = tempfile::tempdir().unwrap();
            let store = open_test_store(tmp.path());
            let receipt = sample_receipt(SESSION_ID);
            store.persist_receipt_intent(&receipt).unwrap();

            let external = tmp.path().join("external-receipt-ack.json");
            write_external_valid_receipt(&external, &receipt);
            let external_before = std::fs::read(&external).unwrap();

            let ack_link = store.session_root().join(RECEIPT_ACK_FILE);
            symlink(&external, &ack_link).unwrap();

            assert_load_fails_closed(&store, &receipt);
            assert_eq!(std::fs::read(&external).unwrap(), external_before);
            assert!(
                ack_link.is_symlink(),
                "receipt-ack must remain a symlink, not a canonical durable file"
            );
            let intent_path = store.session_root().join(RECEIPT_INTENT_FILE);
            assert!(
                intent_path.exists() && !intent_path.is_symlink(),
                "canonical intent from persist_receipt_intent must be unchanged"
            );
        }

        #[test]
        fn persist_receipt_ack_rejects_symlinked_receipt_intent() {
            let tmp = tempfile::tempdir().unwrap();
            let store = open_test_store(tmp.path());
            let receipt = sample_receipt(SESSION_ID);
            let external = tmp.path().join("external-receipt-for-ack.json");
            write_external_valid_receipt(&external, &receipt);
            let external_before = std::fs::read(&external).unwrap();

            let intent_link = store.session_root().join(RECEIPT_INTENT_FILE);
            symlink(&external, &intent_link).unwrap();

            let result = store.persist_receipt_ack(&receipt);
            assert!(
                result.is_err(),
                "persist_receipt_ack must fail closed when intent is a symlink to a valid receipt; \
                 ambient read followed the link and got {:?}",
                result
            );

            assert_eq!(std::fs::read(&external).unwrap(), external_before);
            assert!(intent_link.is_symlink());
            assert!(
                !store.session_root().join(RECEIPT_ACK_FILE).exists(),
                "must not install receipt-ack.json when intent was resolved through a symlink"
            );
        }
    }
}
