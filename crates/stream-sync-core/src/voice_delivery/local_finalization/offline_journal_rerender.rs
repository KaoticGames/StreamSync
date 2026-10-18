//! Offline, read-only local journal recovery render (no cloud / API / receipt mutation).

use super::journal_forensics::{analyze_local_journal_session, session_forensics_certified_clean};
use super::journal_store::LocalJournalStore;
use super::manifest::{
    parse_local_finalization_manifest, LocalFinalizationManifest,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
};
use super::offline_source_snapshot::{
    enumerate_journal_segments_exact, materialize_verified_source_snapshot,
    observe_journal_segments, remove_source_snapshot, OFFLINE_SOURCE_SNAPSHOT_BASENAME,
};
use super::opus_decode::assert_local_opus_available;
use super::processor::{
    process_verified_segments_with_options, LocalProcessorOptions, LocalProcessorProgress,
    PROCESSOR_PROGRESS_EVERY_SEGMENTS,
};
use super::storage_preflight::{storage_preflight, StoragePreflightFailure};
use crate::voice_delivery::finalized_manifest::expected_wav_file_bytes;
use crate::voice_delivery::fs::durability::{sync_dir_exact, NamespaceDurability};
use crate::voice_delivery::fs::durable_payload::write_durable_payload_create_new;
use crate::voice_delivery::fs::{
    ChildDirProbe, DestRoot, FinalParentPublication, FsError, ValidatedFinalName,
};
use crate::voice_delivery::hash::{hex_digest, sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::records::record_digest_hex;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const OFFLINE_RECOVERY_MANIFEST_FILENAME: &str = "Syndicate Offline Recovery Manifest.json";
pub const OFFLINE_RECOVERY_COMPLETE_MARKER: &str = ".streamsync-offline-recovery-complete.json";
pub const OFFLINE_RECOVERY_SCHEMA_VERSION: u32 = 1;
pub const OFFLINE_RECOVERY_ALGORITHM_VERSION: &str = "syndicate-offline-journal-recovery-v1";

const OFFLINE_DISK_RESERVE_BYTES: u64 = 512 * 1024 * 1024;
const OFFLINE_METADATA_OVERHEAD_BYTES: u64 = 4 * 1024 * 1024;

/// Compile-time boundary: offline rerender must not import HTTP/cloud clients (see module tests).
pub struct OfflineJournalRerenderNetworkSeal;

#[derive(Debug, Error)]
pub enum OfflineJournalRerenderError {
    #[error("output path required")]
    OutputPathRequired,
    #[error("output already exists")]
    OutputAlreadyExists,
    #[error("destination invalid: {0}")]
    DestinationInvalid(String),
    #[error("session identity invalid: {0}")]
    SessionIdentity(String),
    #[error("session forensics not certified clean")]
    ForensicsNotCertified,
    #[error("segment verification failed: {0}")]
    SegmentVerification(String),
    #[error("storage preflight: {0}")]
    StoragePreflight(StoragePreflightFailure),
    #[error("native Opus decoder unavailable")]
    OpusUnavailable,
    #[error("processor: {0}")]
    Processor(String),
    #[error("publish: {0}")]
    Publish(String),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone)]
pub struct OfflineJournalRerenderOutcome {
    pub final_dir: PathBuf,
    pub recovery_manifest_path: PathBuf,
    pub completion_marker_path: PathBuf,
    pub staging_directory_namespace_durability: NamespaceDurability,
    pub final_parent_namespace_durability: NamespaceDurability,
}

#[derive(Debug, Clone, Default)]
pub struct OfflineJournalRerenderOptions {
    #[doc(hidden)]
    pub _private: (),
}

#[derive(Default)]
pub(crate) struct OfflineJournalRerenderTestHooks {
    pub fail_after_staging: bool,
    pub post_snapshot: Option<Box<dyn Fn(&Path) + Send + Sync>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfflineRecoveryManifestStem {
    pub discord_user_id: String,
    pub path_nick: String,
    pub file_name: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfflineRecoveryReportRef {
    pub relative_path: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfflineRecoveryPacketLossPolicy {
    pub version: String,
    pub total_journal_records: u64,
    pub max_skipped: u64,
    pub skipped_count: u64,
    pub framing_or_lost_placement: u64,
    pub decode_failure: u64,
    pub placement_overlap: u64,
    pub out_of_window: u64,
    pub consecutive_loss_detected: bool,
    pub within_policy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfflineRecoveryManifest {
    pub schema_version: u32,
    pub recovery_algorithm_version: String,
    pub finalizer_algorithm_version: String,
    pub offline_recovery: bool,
    pub cloud_receipt_posted: bool,
    pub source_session_id: String,
    pub source_guild_id: String,
    pub source_channel_id: String,
    pub source_session_root: String,
    pub source_manifest_sha256: String,
    pub source_journal_sha256: String,
    pub source_segment_count: u32,
    pub source_verified_total_bytes: u64,
    pub target_sample_count_48k: u64,
    pub target_wav_format: String,
    pub stems: Vec<OfflineRecoveryManifestStem>,
    pub recovery_report: OfflineRecoveryReportRef,
    pub discarded_packet_count: u64,
    pub silence_range_count: u64,
    pub packet_loss_policy: OfflineRecoveryPacketLossPolicy,
    pub tool_package_version: String,
    pub tool_build_git_sha: String,
    pub staging_directory_namespace_durability: NamespaceDurability,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfflineRecoveryCompleteMarker {
    pub schema_version: u32,
    pub offline_recovery: bool,
    pub cloud_receipt_posted: bool,
    pub source_session_id: String,
    pub source_session_root: String,
    pub recovery_manifest_relative_path: String,
    pub recovery_manifest_sha256: String,
    pub tool_build_git_sha: String,
    pub staging_directory_namespace_durability: NamespaceDurability,
    pub record_digest: String,
}

impl OfflineRecoveryCompleteMarker {
    fn serialize_bytes(&self) -> Result<Vec<u8>, OfflineJournalRerenderError> {
        let mut tmp = self.clone();
        tmp.record_digest = String::new();
        let payload = serde_json::to_vec(&tmp)
            .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
        let digest = record_digest_hex(&payload);
        tmp.record_digest = digest;
        serde_json::to_vec(&tmp).map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))
    }
}

pub fn run_offline_journal_rerender(
    _network_seal: OfflineJournalRerenderNetworkSeal,
    session_root: &Path,
    output_dir: &Path,
    _options: OfflineJournalRerenderOptions,
) -> Result<OfflineJournalRerenderOutcome, OfflineJournalRerenderError> {
    run_offline_journal_rerender_inner(session_root, output_dir, None)
}

pub(crate) fn run_offline_journal_rerender_with_hooks(
    session_root: &Path,
    output_dir: &Path,
    hooks: OfflineJournalRerenderTestHooks,
) -> Result<OfflineJournalRerenderOutcome, OfflineJournalRerenderError> {
    run_offline_journal_rerender_inner(session_root, output_dir, Some(hooks))
}

fn run_offline_journal_rerender_inner(
    session_root: &Path,
    output_dir: &Path,
    hooks: Option<OfflineJournalRerenderTestHooks>,
) -> Result<OfflineJournalRerenderOutcome, OfflineJournalRerenderError> {
    if output_dir.as_os_str().is_empty() {
        return Err(OfflineJournalRerenderError::OutputPathRequired);
    }

    let tool_build_git_sha = required_tool_build_git_sha()?;

    let manifest_path = session_root.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest_value: serde_json::Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
    let manifest = parse_local_finalization_manifest(&manifest_value)
        .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;

    validate_session_root_identity(session_root, &manifest)?;
    enumerate_journal_segments_exact(session_root, &manifest)?;

    let source_forensics = analyze_local_journal_session(session_root)
        .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
    if !session_forensics_certified_clean(&source_forensics) {
        return Err(OfflineJournalRerenderError::ForensicsNotCertified);
    }

    let dest_parent =
        output_dir
            .parent()
            .ok_or(OfflineJournalRerenderError::DestinationInvalid(
                "output has no parent directory".into(),
            ))?;
    let final_name = output_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            OfflineJournalRerenderError::DestinationInvalid("invalid output final name".into())
        })?;
    let validated_final = ValidatedFinalName::validate(final_name).map_err(map_fs_err)?;

    let required_bytes = offline_recovery_required_bytes(&manifest)
        .map_err(OfflineJournalRerenderError::StoragePreflight)?;
    storage_preflight(dest_parent, required_bytes)
        .map_err(OfflineJournalRerenderError::StoragePreflight)?;

    let dest_root = DestRoot::open(dest_parent).map_err(map_fs_err)?;
    let final_parent = dest_root.handle().clone_handle().map_err(map_fs_err)?;
    match final_parent.probe_child_dir(validated_final.as_str()) {
        Ok(ChildDirProbe::Missing) => {}
        Ok(ChildDirProbe::Directory(_)) => {
            return Err(OfflineJournalRerenderError::OutputAlreadyExists);
        }
        Err(e) => {
            return Err(OfflineJournalRerenderError::DestinationInvalid(
                e.to_string(),
            ))
        }
    }

    if assert_local_opus_available().is_err() {
        return Err(OfflineJournalRerenderError::OpusUnavailable);
    }

    let publication = FinalParentPublication::new(final_parent.clone_handle().map_err(map_fs_err)?);
    let stage_token = publication
        .create_stage_dir()
        .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;
    let staging_path = dest_parent.join(&stage_token);

    if hooks.as_ref().is_some_and(|h| h.fail_after_staging) {
        eprintln!(
            "offline_journal_rerender: staging preserved at {}",
            staging_path.display()
        );
        return Err(OfflineJournalRerenderError::Other(
            "offline rerender test hook: fail after staging".into(),
        ));
    }

    let run_result = (|| {
        let source_observation_before = observe_journal_segments(session_root, &manifest)?;
        let (snapshot_root, segment_paths) = materialize_verified_source_snapshot(
            &staging_path,
            session_root,
            &manifest,
            &manifest_bytes,
        )?;

        if let Some(hooks) = &hooks {
            if let Some(cb) = &hooks.post_snapshot {
                cb(session_root);
            }
        }
        let source_observation_after = observe_journal_segments(session_root, &manifest)?;
        if source_observation_before != source_observation_after {
            return Err(OfflineJournalRerenderError::SegmentVerification(
                "source journal/segments changed after verified snapshot".into(),
            ));
        }

        let forensics = analyze_local_journal_session(&snapshot_root)
            .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
        if !session_forensics_certified_clean(&forensics) {
            return Err(OfflineJournalRerenderError::ForensicsNotCertified);
        }

        let progress = |p: LocalProcessorProgress| {
            if p.total_segments == 0 {
                return;
            }
            let emit = p.processed_segments == p.total_segments
                || p.processed_segments % PROCESSOR_PROGRESS_EVERY_SEGMENTS == 0;
            if !emit {
                return;
            }
            let percent = (p.processed_segments as u64 * 100) / p.total_segments as u64;
            println!(
                "offline_journal_rerender: segments {}/{} ({}%)",
                p.processed_segments, p.total_segments, percent
            );
        };
        let processor_options = LocalProcessorOptions {
            cancel: None,
            paused: None,
            progress: Some(std::sync::Arc::new(progress)),
        };
        let output = process_verified_segments_with_options(
            &manifest,
            &segment_paths,
            &staging_path,
            processor_options,
        )
        .map_err(|e| OfflineJournalRerenderError::Processor(e.to_string()))?;

        remove_source_snapshot(&staging_path, &manifest.session_id)?;

        let source_manifest_sha256 = hex_digest(&Sha256::digest(&manifest_bytes));
        let staging_handle = final_parent
            .open_child_dir(&stage_token)
            .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;
        let staging_namespace = sync_dir_exact(&staging_handle)
            .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;
        warn_namespace_unavailable("staging directory", staging_namespace);

        let recovery_manifest = build_recovery_manifest(
            session_root,
            &manifest,
            &source_manifest_sha256,
            &output,
            tool_build_git_sha,
            staging_namespace,
        )?;
        let recovery_manifest_json = serde_json::to_string_pretty(&recovery_manifest)
            .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
        write_durable_recovery_file(
            &staging_handle,
            OFFLINE_RECOVERY_MANIFEST_FILENAME,
            recovery_manifest_json.as_bytes(),
        )?;
        let recovery_manifest_sha256 =
            hex_digest(&Sha256::digest(recovery_manifest_json.as_bytes()));

        let marker = OfflineRecoveryCompleteMarker {
            schema_version: OFFLINE_RECOVERY_SCHEMA_VERSION,
            offline_recovery: true,
            cloud_receipt_posted: false,
            source_session_id: manifest.session_id.clone(),
            source_session_root: session_root.to_string_lossy().into_owned(),
            recovery_manifest_relative_path: OFFLINE_RECOVERY_MANIFEST_FILENAME.into(),
            recovery_manifest_sha256: recovery_manifest_sha256.clone(),
            tool_build_git_sha: tool_build_git_sha.into(),
            staging_directory_namespace_durability: staging_namespace,
            record_digest: String::new(),
        };
        let marker_bytes = marker.serialize_bytes()?;
        write_durable_recovery_file(
            &staging_handle,
            OFFLINE_RECOVERY_COMPLETE_MARKER,
            &marker_bytes,
        )?;

        verify_staging_outputs(
            &staging_path,
            &staging_handle,
            session_root,
            &manifest,
            &output,
            &recovery_manifest_sha256,
            tool_build_git_sha,
            staging_namespace,
        )?;

        publication
            .rename_stage_to_final(&stage_token, &validated_final)
            .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;

        let final_parent_handle = final_parent.clone_handle().map_err(map_fs_err)?;
        let final_parent_namespace = sync_dir_exact(&final_parent_handle)
            .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;
        warn_namespace_unavailable("final parent directory", final_parent_namespace);

        let final_dir = dest_parent.join(validated_final.as_str());
        Ok(OfflineJournalRerenderOutcome {
            final_dir: final_dir.clone(),
            recovery_manifest_path: final_dir.join(OFFLINE_RECOVERY_MANIFEST_FILENAME),
            completion_marker_path: final_dir.join(OFFLINE_RECOVERY_COMPLETE_MARKER),
            staging_directory_namespace_durability: staging_namespace,
            final_parent_namespace_durability: final_parent_namespace,
        })
    })();

    if run_result.is_err() {
        eprintln!(
            "offline_journal_rerender: staging preserved at {}",
            staging_path.display()
        );
    }
    run_result
}

fn required_tool_build_git_sha() -> Result<&'static str, OfflineJournalRerenderError> {
    match option_env!("STREAMSYNC_GIT_SHA") {
        Some(s) if is_valid_git_sha(s) => Ok(s),
        _ => Err(OfflineJournalRerenderError::Other(
            "tool build git SHA unavailable; rebuild from a git checkout".into(),
        )),
    }
}

fn is_valid_git_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn is_valid_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn warn_namespace_unavailable(label: &str, durability: NamespaceDurability) {
    if durability == NamespaceDurability::Unavailable {
        eprintln!(
            "offline_journal_rerender: warning: {label} namespace durability unavailable on this platform (file sync/hash/readback succeeded)"
        );
    }
}

fn write_durable_recovery_file(
    staging: &crate::voice_delivery::fs::DirHandle,
    name: &str,
    bytes: &[u8],
) -> Result<(), OfflineJournalRerenderError> {
    write_durable_payload_create_new(staging, name, bytes)
        .map_err(|e| OfflineJournalRerenderError::Publish(e.to_string()))?;
    Ok(())
}

fn validate_session_root_identity(
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
) -> Result<(), OfflineJournalRerenderError> {
    let dir_name = session_root
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            OfflineJournalRerenderError::SessionIdentity(
                "session root has no directory name".into(),
            )
        })?;
    if dir_name != manifest.session_id {
        return Err(OfflineJournalRerenderError::SessionIdentity(format!(
            "session root directory name {dir_name} does not match manifest sessionId {}",
            manifest.session_id
        )));
    }
    Ok(())
}

pub(crate) fn offline_recovery_required_bytes(
    manifest: &LocalFinalizationManifest,
) -> Result<u64, StoragePreflightFailure> {
    let base = LocalJournalStore::disk_preflight_bytes(manifest, OFFLINE_DISK_RESERVE_BYTES)?;
    base.checked_add(OFFLINE_METADATA_OVERHEAD_BYTES)
        .ok_or(StoragePreflightFailure::RequiredBytesOverflow)
}

fn build_recovery_manifest(
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
    source_manifest_sha256: &str,
    output: &super::processor::LocalProcessOutput,
    tool_build_git_sha: &str,
    staging_namespace: NamespaceDurability,
) -> Result<OfflineRecoveryManifest, OfflineJournalRerenderError> {
    let verified_total_bytes = manifest
        .segments
        .iter()
        .try_fold(0u64, |acc, s| acc.checked_add(s.byte_length))
        .ok_or_else(|| OfflineJournalRerenderError::Other("segment bytes overflow".into()))?;

    let mut stems = Vec::with_capacity(manifest.users.len());
    for user in &manifest.users {
        let file_name = format!("{}.wav", user.path_nick);
        let sha = output
            .stem_sha256
            .get(&user.discord_user_id)
            .ok_or_else(|| {
                OfflineJournalRerenderError::Other(format!("missing stem hash {}", user.path_nick))
            })?;
        let byte_length = expected_wav_file_bytes(manifest.target_sample_count_48k);
        stems.push(OfflineRecoveryManifestStem {
            discord_user_id: user.discord_user_id.clone(),
            path_nick: user.path_nick.clone(),
            file_name,
            byte_length,
            sha256: sha.clone(),
        });
    }

    let report_sha = output
        .report_sha256
        .as_deref()
        .ok_or_else(|| OfflineJournalRerenderError::Other("missing report digest".into()))?;
    let report_bytes = output
        .report_bytes
        .ok_or_else(|| OfflineJournalRerenderError::Other("missing report bytes".into()))?;

    Ok(OfflineRecoveryManifest {
        schema_version: OFFLINE_RECOVERY_SCHEMA_VERSION,
        recovery_algorithm_version: OFFLINE_RECOVERY_ALGORITHM_VERSION.into(),
        finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
        offline_recovery: true,
        cloud_receipt_posted: false,
        source_session_id: manifest.session_id.clone(),
        source_guild_id: manifest.guild_id.clone(),
        source_channel_id: manifest.channel_id.clone(),
        source_session_root: session_root.to_string_lossy().into_owned(),
        source_manifest_sha256: source_manifest_sha256.into(),
        source_journal_sha256: manifest.journal_sha256.clone(),
        source_segment_count: manifest.segments.len() as u32,
        source_verified_total_bytes: verified_total_bytes,
        target_sample_count_48k: manifest.target_sample_count_48k,
        target_wav_format: manifest.output.wav_format.clone(),
        stems,
        recovery_report: OfflineRecoveryReportRef {
            relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
            byte_length: report_bytes,
            sha256: report_sha.to_string(),
        },
        discarded_packet_count: output.discarded_packet_count,
        silence_range_count: output.silence_range_count,
        packet_loss_policy: OfflineRecoveryPacketLossPolicy {
            version: output.packet_loss_policy.version.clone(),
            total_journal_records: output.packet_loss_policy.total_journal_records,
            max_skipped: output.packet_loss_policy.max_skipped,
            skipped_count: output.packet_loss_policy.skipped_count,
            framing_or_lost_placement: output
                .packet_loss_policy
                .source_counts
                .framing_or_lost_placement,
            decode_failure: output.packet_loss_policy.source_counts.decode_failure,
            placement_overlap: output.packet_loss_policy.source_counts.placement_overlap,
            out_of_window: output.packet_loss_policy.source_counts.out_of_window,
            consecutive_loss_detected: output.packet_loss_policy.consecutive_loss_detected,
            within_policy: output.packet_loss_policy.within_policy,
        },
        tool_package_version: env!("CARGO_PKG_VERSION").into(),
        tool_build_git_sha: tool_build_git_sha.into(),
        staging_directory_namespace_durability: staging_namespace,
    })
}

pub(crate) fn parse_offline_recovery_complete_marker_strict(
    bytes: &[u8],
) -> Result<OfflineRecoveryCompleteMarker, OfflineJournalRerenderError> {
    let marker: OfflineRecoveryCompleteMarker = serde_json::from_slice(bytes)
        .map_err(|_| OfflineJournalRerenderError::Other("completion marker json".into()))?;
    if marker.schema_version != OFFLINE_RECOVERY_SCHEMA_VERSION {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker schema".into(),
        ));
    }
    if !marker.offline_recovery {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker offlineRecovery".into(),
        ));
    }
    if marker.cloud_receipt_posted {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker cloudReceiptPosted".into(),
        ));
    }
    if marker.source_session_id.is_empty() || marker.source_session_root.is_empty() {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker source identity".into(),
        ));
    }
    if marker.recovery_manifest_relative_path != OFFLINE_RECOVERY_MANIFEST_FILENAME {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker manifest path".into(),
        ));
    }
    if !is_valid_sha256_hex(&marker.recovery_manifest_sha256) {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker manifest digest".into(),
        ));
    }
    if !is_valid_git_sha(&marker.tool_build_git_sha) {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker tool build git sha".into(),
        ));
    }
    if !is_valid_sha256_hex(&marker.record_digest) {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker record digest".into(),
        ));
    }
    let mut tmp = marker.clone();
    let expected = tmp.record_digest.clone();
    tmp.record_digest = String::new();
    let payload =
        serde_json::to_vec(&tmp).map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
    if record_digest_hex(&payload) != expected {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker digest mismatch".into(),
        ));
    }
    Ok(marker)
}

fn verify_staging_outputs(
    staging: &Path,
    staging_handle: &crate::voice_delivery::fs::DirHandle,
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
    output: &super::processor::LocalProcessOutput,
    recovery_manifest_sha256: &str,
    tool_build_git_sha: &str,
    staging_namespace: NamespaceDurability,
) -> Result<(), OfflineJournalRerenderError> {
    for user in &manifest.users {
        let wav_name = format!("{}.wav", user.path_nick);
        let path = staging.join(&wav_name);
        let expected_sha = output
            .stem_sha256
            .get(&user.discord_user_id)
            .ok_or_else(|| OfflineJournalRerenderError::Other("stem hash".into()))?;
        let file = File::open(&path)?;
        let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
            .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
        if sha != *expected_sha {
            return Err(OfflineJournalRerenderError::Other(format!(
                "stem {} digest mismatch after write",
                user.path_nick
            )));
        }
    }
    let report_path = staging.join(RECOVERY_REPORT_RELATIVE_PATH);
    let report_sha = output
        .report_sha256
        .as_deref()
        .ok_or_else(|| OfflineJournalRerenderError::Other("report digest missing".into()))?;
    let file = File::open(&report_path)?;
    let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
        .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
    if sha != report_sha {
        return Err(OfflineJournalRerenderError::Other(
            "recovery report digest mismatch".into(),
        ));
    }
    let manifest_path = staging.join(OFFLINE_RECOVERY_MANIFEST_FILENAME);
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let computed = hex_digest(&Sha256::digest(&manifest_bytes));
    if computed != recovery_manifest_sha256 {
        return Err(OfflineJournalRerenderError::Other(
            "recovery manifest digest mismatch".into(),
        ));
    }
    let parsed_manifest: OfflineRecoveryManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| OfflineJournalRerenderError::Other(e.to_string()))?;
    if parsed_manifest.schema_version != OFFLINE_RECOVERY_SCHEMA_VERSION
        || !parsed_manifest.offline_recovery
        || parsed_manifest.cloud_receipt_posted
        || parsed_manifest.tool_build_git_sha != tool_build_git_sha
    {
        return Err(OfflineJournalRerenderError::Other(
            "recovery manifest schema binding".into(),
        ));
    }

    let marker_bytes = std::fs::read(staging.join(OFFLINE_RECOVERY_COMPLETE_MARKER))?;
    let marker = parse_offline_recovery_complete_marker_strict(&marker_bytes)?;
    if marker.source_session_id != manifest.session_id {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker session id".into(),
        ));
    }
    if marker.source_session_root != session_root.to_string_lossy() {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker session root".into(),
        ));
    }
    if marker.recovery_manifest_sha256 != recovery_manifest_sha256 {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker manifest hash".into(),
        ));
    }
    if marker.staging_directory_namespace_durability != staging_namespace {
        return Err(OfflineJournalRerenderError::Other(
            "completion marker namespace durability".into(),
        ));
    }
    if staging.join(OFFLINE_SOURCE_SNAPSHOT_BASENAME).exists() {
        return Err(OfflineJournalRerenderError::Other(
            "source snapshot must be removed before publish".into(),
        ));
    }
    let _ = staging_handle;
    Ok(())
}

fn map_fs_err(e: FsError) -> OfflineJournalRerenderError {
    OfflineJournalRerenderError::DestinationInvalid(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice_delivery::local_finalization::manifest::{
        LocalFinalizationManifest, LocalFinalizationObjectRef, LocalFinalizationOutputPolicy,
        LocalFinalizationSegment, LocalFinalizationUser, LOCAL_FINALIZATION_WAV_FORMAT,
    };
    use crate::voice_delivery::local_finalization::synj::{
        encode_journal_file, encode_journal_record, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
    };

    fn sample_record(user: &str, seq: u16, capture: u64) -> JournalRecord {
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: seq,
            rtp_timestamp: capture as u32,
            arrived_at_ns: u64::from(seq) * 1_000_000,
            session_capture_offset_samples: Some(capture),
            opus_payload: vec![0x08, 0x01],
        }
    }

    fn write_verified_session(
        session_root: &Path,
        manifest: &LocalFinalizationManifest,
        segment_bodies: &[Vec<u8>],
    ) {
        std::fs::create_dir_all(session_root.join("journal/segments")).unwrap();
        assert_eq!(manifest.segments.len(), segment_bodies.len());
        for (seg, body) in manifest.segments.iter().zip(segment_bodies) {
            let path = session_root
                .join("journal/segments")
                .join(format!("{:08}.synj", seg.segment_seq));
            std::fs::write(&path, body).unwrap();
        }
        let json = serde_json::to_string_pretty(manifest).unwrap();
        std::fs::write(session_root.join("manifest.json"), json).unwrap();
    }

    fn manifest_with_segments(
        session_id: &str,
        users: Vec<LocalFinalizationUser>,
        segments: Vec<(u32, Vec<u8>)>,
    ) -> LocalFinalizationManifest {
        let guild_id = "123456789012345678";
        let seg_entries: Vec<LocalFinalizationSegment> = segments
            .iter()
            .map(|(seq, body)| {
                let sha =
                    sha256_hex_reader(std::io::Cursor::new(body), DEFAULT_STREAM_CHUNK).unwrap();
                LocalFinalizationSegment {
                    segment_seq: *seq,
                    key: format!(
                        "voice-delivery-v2/{guild_id}/{session_id}/journal/{:08}.synj",
                        *seq
                    ),
                    byte_length: body.len() as u64,
                    sha256: sha,
                }
            })
            .collect();
        LocalFinalizationManifest {
            version: 1,
            finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
            session_id: session_id.into(),
            guild_id: guild_id.into(),
            channel_id: "234567890123456789".into(),
            session_start_wall_ms: 0,
            session_start_monotonic_ns: "0".into(),
            sealed_stop_wall_ms: 0,
            sealed_stop_monotonic_ns: "0".into(),
            target_sample_count_48k: 960,
            journal_sha256: "a".repeat(64),
            recording_manifest: LocalFinalizationObjectRef {
                key: format!(
                    "voice-delivery-v2/{guild_id}/{session_id}/manifest/recording.manifest.json"
                ),
                sha256: "b".repeat(64),
                byte_length: 4096,
            },
            sealed_index: LocalFinalizationObjectRef {
                key: format!("voice-delivery-v2/{guild_id}/{session_id}/journal/index.sealed.json"),
                sha256: "c".repeat(64),
                byte_length: 512,
            },
            segments: seg_entries,
            users,
            output: LocalFinalizationOutputPolicy {
                wav_format: LOCAL_FINALIZATION_WAV_FORMAT.into(),
                recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
            },
        }
    }

    fn certified_single_user_session(tmp: &Path) -> (PathBuf, LocalFinalizationManifest) {
        let session_id = "550e8400-e29b-41d4-a716-446655440099";
        let session_root = tmp.join(session_id);
        let user = "123456789012345678";
        let body = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
        let users = vec![LocalFinalizationUser {
            discord_user_id: user.into(),
            path_nick: "wrap_epoch".into(),
            opaque_stem_id:
                crate::voice_delivery::local_finalization::derive_voice_v2_opaque_stem_id(
                    session_id,
                    "123456789012345678",
                    user,
                    "wrap_epoch",
                ),
        }];
        let manifest = manifest_with_segments(session_id, users, vec![(0, body.clone())]);
        write_verified_session(&session_root, &manifest, &[body]);
        let report = analyze_local_journal_session(&session_root).unwrap();
        assert!(session_forensics_certified_clean(&report));
        (session_root, manifest)
    }

    fn run_success_outcome(
        tmp: &tempfile::TempDir,
        session_root: &Path,
    ) -> OfflineJournalRerenderOutcome {
        let out = tmp.path().join("dest").join("recovered");
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        run_offline_journal_rerender_with_hooks(
            session_root,
            &out,
            OfflineJournalRerenderTestHooks::default(),
        )
        .unwrap()
    }

    #[test]
    fn verify_segments_rejects_extra_and_missing_synj() {
        let session_id = "550e8400-e29b-41d4-a716-446655440020";
        let user = "123456789012345678";
        let body = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
        let manifest = manifest_with_segments(
            session_id,
            vec![LocalFinalizationUser {
                discord_user_id: user.into(),
                path_nick: "p".into(),
                opaque_stem_id:
                    crate::voice_delivery::local_finalization::derive_voice_v2_opaque_stem_id(
                        session_id,
                        "123456789012345678",
                        user,
                        "p",
                    ),
            }],
            vec![(0, body.clone())],
        );
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(session_id);
        write_verified_session(&root, &manifest, &[body]);
        assert!(enumerate_journal_segments_exact(&root, &manifest).is_ok());
        std::fs::remove_file(root.join("journal/segments/00000000.synj")).unwrap();
        assert!(matches!(
            enumerate_journal_segments_exact(&root, &manifest),
            Err(OfflineJournalRerenderError::SegmentVerification(_))
        ));
        std::fs::write(root.join("journal/segments/00000001.synj"), b"extra").unwrap();
        assert!(matches!(
            enumerate_journal_segments_exact(&root, &manifest),
            Err(OfflineJournalRerenderError::SegmentVerification(_))
        ));
    }

    #[test]
    fn segment_paths_follow_manifest_order_in_snapshot() {
        let user = "123456789012345678";
        let b0 = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
        let b1 = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
        let manifest = manifest_with_segments(
            "550e8400-e29b-41d4-a716-446655440001",
            vec![LocalFinalizationUser {
                discord_user_id: user.into(),
                path_nick: "p".into(),
                opaque_stem_id: "a".repeat(32),
            }],
            vec![(0, b0.clone()), (1, b1.clone())],
        );
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(&manifest.session_id);
        write_verified_session(&root, &manifest, &[b0, b1]);
        let staging = tmp.path().join("stage");
        std::fs::create_dir_all(&staging).unwrap();
        let bytes = std::fs::read(root.join("manifest.json")).unwrap();
        let (_, paths) =
            materialize_verified_source_snapshot(&staging, &root, &manifest, &bytes).unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths[0].to_string_lossy().contains("00000000"));
        assert!(paths[1].to_string_lossy().contains("00000001"));
    }

    #[test]
    fn offline_module_has_no_cloud_client_imports() {
        let src = include_str!("offline_journal_rerender.rs");
        let src = src.split("#[cfg(test)]").next().unwrap_or(src);
        for needle in [
            "reqwest",
            "LocalFinalizationClient",
            "VoiceV2Client",
            "claim_delivery",
            "post_receipt",
            "HttpLocalFinalizationClient",
        ] {
            assert!(
                !src.contains(needle),
                "offline_journal_rerender must not reference {needle}"
            );
        }
    }

    #[test]
    fn recovery_manifest_binds_source_manifest_digest() {
        let tmp = tempfile::tempdir().unwrap();
        let (session_root, manifest) = certified_single_user_session(tmp.path());
        let bytes = std::fs::read(session_root.join("manifest.json")).unwrap();
        let digest = hex_digest(&Sha256::digest(&bytes));
        let staging = tmp.path().join("staging");
        std::fs::create_dir_all(&staging).unwrap();
        let (snap, seg_paths) =
            materialize_verified_source_snapshot(&staging, &session_root, &manifest, &bytes)
                .unwrap();
        let out =
            super::super::processor::process_verified_segments(&manifest, &seg_paths, &staging)
                .unwrap();
        let sha = required_tool_build_git_sha().unwrap();
        let recovery = build_recovery_manifest(
            &session_root,
            &manifest,
            &digest,
            &out,
            sha,
            NamespaceDurability::Proven,
        )
        .unwrap();
        assert_eq!(recovery.source_manifest_sha256, digest);
        assert_eq!(recovery.tool_build_git_sha, sha);
        assert!(recovery.offline_recovery);
        assert!(!recovery.cloud_receipt_posted);
        let _ = snap;
    }

    #[test]
    fn success_publishes_recovery_manifest_and_completion_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let (session_root, manifest) = certified_single_user_session(tmp.path());
        let outcome = run_success_outcome(&tmp, &session_root);
        assert!(outcome.recovery_manifest_path.is_file());
        assert!(outcome.completion_marker_path.is_file());
        let recovery_json = std::fs::read_to_string(&outcome.recovery_manifest_path).unwrap();
        assert!(recovery_json.contains("\"toolBuildGitSha\""));
        assert!(recovery_json.contains("\"offlineRecovery\": true"));
        let marker = std::fs::read(&outcome.completion_marker_path).unwrap();
        parse_offline_recovery_complete_marker_strict(&marker).unwrap();
        assert!(outcome.final_dir.join("wrap_epoch.wav").is_file());
    }

    #[test]
    fn failure_preserves_source_and_leaves_staging_without_final() {
        let tmp = tempfile::tempdir().unwrap();
        let (session_root, _) = certified_single_user_session(tmp.path());
        let manifest_before = std::fs::read(session_root.join("manifest.json")).unwrap();
        let out = tmp.path().join("dest").join("recovered-fail");
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        assert!(run_offline_journal_rerender_with_hooks(
            &session_root,
            &out,
            OfflineJournalRerenderTestHooks {
                fail_after_staging: true,
                post_snapshot: None,
            },
        )
        .is_err());
        assert!(!out.exists());
        assert_eq!(
            std::fs::read(session_root.join("manifest.json")).unwrap(),
            manifest_before
        );
        let parent = out.parent().unwrap();
        assert!(parent.read_dir().unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".streamsync-stage-")));
    }

    #[test]
    fn truncated_completion_marker_fails_strict_parse() {
        let tmp = tempfile::tempdir().unwrap();
        let (session_root, _) = certified_single_user_session(tmp.path());
        let outcome = run_success_outcome(&tmp, &session_root);
        let bytes = std::fs::read(&outcome.completion_marker_path).unwrap();
        let truncated = &bytes[..bytes.len().saturating_sub(8)];
        assert!(parse_offline_recovery_complete_marker_strict(truncated).is_err());
    }

    #[test]
    fn source_mutation_after_snapshot_refuses_publish() {
        let tmp = tempfile::tempdir().unwrap();
        let (session_root, manifest) = certified_single_user_session(tmp.path());
        let out = tmp.path().join("dest").join("recovered-mutate");
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        let seg = session_root.join("journal/segments/00000000.synj");
        let err = run_offline_journal_rerender_with_hooks(
            &session_root,
            &out,
            OfflineJournalRerenderTestHooks {
                fail_after_staging: false,
                post_snapshot: Some(Box::new(move |_| {
                    let mut bytes = std::fs::read(&seg).unwrap();
                    if !bytes.is_empty() {
                        bytes[0] ^= 0x01;
                    }
                    std::fs::write(&seg, bytes).unwrap();
                })),
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            OfflineJournalRerenderError::SegmentVerification(_)
        ));
        assert!(!out.exists());
        assert!(std::fs::read(&session_root.join("manifest.json")).is_ok());
        let _ = manifest;
    }

    #[test]
    fn marker_digest_mismatch_rejected_by_strict_parser() {
        let marker = OfflineRecoveryCompleteMarker {
            schema_version: OFFLINE_RECOVERY_SCHEMA_VERSION,
            offline_recovery: true,
            cloud_receipt_posted: false,
            source_session_id: "s".into(),
            source_session_root: "/tmp/s".into(),
            recovery_manifest_relative_path: OFFLINE_RECOVERY_MANIFEST_FILENAME.into(),
            recovery_manifest_sha256: "a".repeat(64),
            tool_build_git_sha: "b".repeat(40),
            staging_directory_namespace_durability: NamespaceDurability::Proven,
            record_digest: "c".repeat(64),
        };
        let bytes = marker.serialize_bytes().unwrap();
        let mut bad = bytes.clone();
        bad[10] ^= 0x7;
        assert!(parse_offline_recovery_complete_marker_strict(&bad).is_err());
    }
}
