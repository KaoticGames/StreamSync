//! Canonical published output paths for In-Sync UI (no arbitrary user paths).

use super::journal_store::IN_SYNC_DIR;
use super::local_publication::resolve_published_paths;
use super::manifest::{local_manifest_to_syndicate_finalized, parse_local_finalization_manifest};
use crate::voice_delivery::api_binding::read_api_binding;
use crate::voice_delivery::finalized_manifest::{
    bind_delivery_identity, compute_finalized_manifest_digest, expected_wav_file_bytes,
    SyndicateFinalizedManifest, SyndicateFinalizedStem,
};
use crate::voice_delivery::fs::DestRoot;
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::records::ledger_generation::read_highest_valid_in_opaque_dir;
use crate::voice_delivery::recovery_report::recovery_report_spec_from_manifest;
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PublishedSessionPaths {
    pub final_session_dir: PathBuf,
    pub recovery_report_path: Option<PathBuf>,
}

/// Publication paths plus ledger-bound finalized manifest authority for receipt recovery.
#[derive(Debug, Clone)]
pub struct DurablePublicationAuthority {
    pub paths: PublishedSessionPaths,
    pub output_manifest_digest: String,
    pub manifest: SyndicateFinalizedManifest,
}

pub fn validate_path_under_parent(parent: &Path, candidate: &Path) -> Result<PathBuf, String> {
    let parent = parent
        .canonicalize()
        .map_err(|e| format!("recording parent: {e}"))?;
    let candidate = candidate
        .canonicalize()
        .map_err(|e| format!("path unavailable: {e}"))?;
    if !candidate.starts_with(&parent) {
        return Err("path outside recording parent".into());
    }
    Ok(candidate)
}

pub fn resolve_canonical_session_paths(
    recording_parent: &Path,
    session_id: &str,
) -> Result<PublishedSessionPaths, String> {
    resolve_durable_publication_authority(recording_parent, session_id)
        .map(|a| a.paths)
        .or_else(|_| resolve_from_local_manifest(recording_parent, session_id))
}

/// Ledger + api-binding authority for receipt recovery (no local-manifest fallback).
pub fn resolve_durable_publication_authority(
    recording_parent: &Path,
    session_id: &str,
) -> Result<DurablePublicationAuthority, String> {
    resolve_from_ledger(recording_parent, session_id)
}

fn resolve_from_ledger(
    recording_parent: &Path,
    session_id: &str,
) -> Result<DurablePublicationAuthority, String> {
    let root = DestRoot::open(recording_parent).map_err(|e| e.to_string())?;
    let control = root
        .handle()
        .open_child_dir(".streamsync-control")
        .map_err(|e| e.to_string())?;
    let ledgers = control
        .open_child_dir("ledgers")
        .map_err(|e| e.to_string())?;
    for opaque in ledgers.list_child_names().map_err(|e| e.to_string())? {
        let ledger_dir = ledgers.open_child_dir(&opaque).map_err(|e| e.to_string())?;
        let record = match read_highest_valid_in_opaque_dir(&ledger_dir) {
            Ok(Some(r)) => r,
            _ => continue,
        };
        if record.state != LedgerState::Published || record.delivery_uuid != session_id {
            continue;
        }
        let identity =
            DeliveryImmutableIdentity::from_ledger_record(&record).map_err(|e| e.to_string())?;
        let binding = read_api_binding(&ledger_dir, &identity).map_err(|e| e.to_string())?;
        let (validated_manifest, id) =
            bind_delivery_identity(&binding.manifest, &identity.manifest_digest)
                .map_err(|e| e.to_string())?;
        let finalized_manifest = binding.manifest.clone();
        let recovery =
            recovery_report_spec_from_manifest(finalized_manifest.recovery_report.as_ref());
        let guard = DeliverySessionGuard::begin_for_recovery(
            DestRoot::open(recording_parent).map_err(|e| e.to_string())?,
            id,
            validated_manifest,
            recovery,
            true,
        )
        .map_err(|e| e.to_string())?;
        let published = resolve_published_paths(recording_parent, &guard);
        return Ok(DurablePublicationAuthority {
            paths: PublishedSessionPaths {
                final_session_dir: published.final_session_dir,
                recovery_report_path: published.recovery_report_path,
            },
            output_manifest_digest: identity.manifest_digest.clone(),
            manifest: finalized_manifest,
        });
    }
    Err("not found".into())
}

fn resolve_from_local_manifest(
    recording_parent: &Path,
    session_id: &str,
) -> Result<PublishedSessionPaths, String> {
    let manifest_path = recording_parent
        .join(IN_SYNC_DIR)
        .join(session_id)
        .join("manifest.json");
    let raw =
        std::fs::read_to_string(&manifest_path).map_err(|_| "session not found".to_string())?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let manifest = parse_local_finalization_manifest(&value).map_err(|e| e.to_string())?;
    let wav_bytes = expected_wav_file_bytes(manifest.target_sample_count_48k);
    let stems: Vec<SyndicateFinalizedStem> = manifest
        .users
        .iter()
        .map(|u| SyndicateFinalizedStem {
            discord_user_id: u.discord_user_id.clone(),
            path_nick: u.path_nick.clone(),
            wav_relative_path: format!("{}.wav", u.path_nick),
            wav_bytes,
            sha256: String::new(),
        })
        .collect();
    let syndicate =
        local_manifest_to_syndicate_finalized(&manifest, stems, None).map_err(|e| e.to_string())?;
    let digest = compute_finalized_manifest_digest(&syndicate);
    let (_validated, identity) =
        bind_delivery_identity(&syndicate, &digest).map_err(|e| e.to_string())?;
    let mut path = recording_parent.to_path_buf();
    for comp in identity.final_parent_relative.iter() {
        path.push(comp.as_str());
    }
    path.push(&identity.final_session_name);
    if !path.is_dir() {
        return Err("published output not present".into());
    }
    Ok(PublishedSessionPaths {
        final_session_dir: path,
        recovery_report_path: None,
    })
}
