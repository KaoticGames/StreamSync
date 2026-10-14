//! Install locally decoded artifacts into the voice_delivery v2 publication pipeline.

use super::processor::LocalProcessOutput;
use crate::voice_delivery::api_binding::persist_api_binding_if_absent;
use crate::voice_delivery::finalized_manifest::{
    bind_delivery_identity, SyndicateFinalizedManifest,
};
use crate::voice_delivery::fs::durability::sync_dir_exact;
use crate::voice_delivery::fs::file::{create_new_file_at, open_existing_file_at};
use crate::voice_delivery::fs::{DestRoot, DirHandle};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::marker::verify_stem_from_handle;
use crate::voice_delivery::orchestrator::{FinalizedVoiceDelivery, OrchestratorError};
use crate::voice_delivery::publication::{
    prepare_publication, publish_prepared, recover_delivery, PublishOptions, RecoveryOutcome,
};
use crate::voice_delivery::records::ledger_generation::LedgerStore;
use crate::voice_delivery::recovery_report::{
    recovery_report_spec_from_manifest, verify_recovery_report_from_handle,
};
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PublishedLocalOutput {
    pub final_session_dir: PathBuf,
    pub recovery_report_path: Option<PathBuf>,
}

impl FinalizedVoiceDelivery {
    /// Stage verified local WAV/report artifacts, publish atomically, return committed final paths.
    pub fn publish_local_decoded(
        &self,
        syndicate: &SyndicateFinalizedManifest,
        manifest_digest: &str,
        sealed_at: &str,
        output: &LocalProcessOutput,
    ) -> Result<PublishedLocalOutput, OrchestratorError> {
        let (manifest, identity) = bind_delivery_identity(syndicate, manifest_digest)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let root = DestRoot::open(self.recording_parent())
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let recovery_report =
            recovery_report_spec_from_manifest(syndicate.recovery_report.as_ref());
        let guard = self.open_session_for_delivery(
            root,
            identity,
            manifest,
            recovery_report,
            &syndicate.session_id,
        )?;
        let ledger_dir = guard
            .ledger_dir()
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        persist_api_binding_if_absent(
            &ledger_dir,
            guard.identity(),
            guard.manifest(),
            syndicate,
            sealed_at,
        )
        .map_err(|e| OrchestratorError::Other(e.to_string()))?;

        let ledger = LedgerStore::open_for_guard(&guard)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        if ledger
            .read_highest_valid()
            .map_err(|e| OrchestratorError::Other(e.to_string()))?
            .is_none()
        {
            ledger
                .commit(&guard, LedgerState::Receiving)
                .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        }

        let recovery =
            recover_delivery(&guard, None).map_err(|e| OrchestratorError::Other(e.to_string()))?;
        match recovery {
            RecoveryOutcome::Published(_) | RecoveryOutcome::IdempotentPublished => {}
            RecoveryOutcome::Quarantined(_) => {
                return Err(OrchestratorError::Other("publication quarantined".into()));
            }
            RecoveryOutcome::AdvancedToPublishIntent(_) => {}
            RecoveryOutcome::ResumeIngest => {
                install_outputs_into_staging(&guard, syndicate, output)?;
                guard
                    .seal_and_write_publish_intent()
                    .map_err(|e| OrchestratorError::Other(e.to_string()))?;
            }
        }

        let prepared =
            prepare_publication(&guard).map_err(|e| OrchestratorError::Other(e.to_string()))?;
        publish_prepared(prepared, None, PublishOptions::default())
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        Ok(resolve_published_paths(self.recording_parent(), &guard))
    }
}

fn install_outputs_into_staging(
    guard: &DeliverySessionGuard,
    syndicate: &SyndicateFinalizedManifest,
    output: &LocalProcessOutput,
) -> Result<(), OrchestratorError> {
    let staging = guard
        .staging_dir()
        .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    for stem in &syndicate.stems {
        let wav_name = format!("{}.wav", stem.path_nick);
        let src = output
            .stem_paths
            .get(&stem.discord_user_id)
            .ok_or_else(|| OrchestratorError::Other(format!("missing stem {}", stem.path_nick)))?;
        install_verified_file(&staging, &wav_name, src, stem.wav_bytes, &stem.sha256)?;
        let file = open_existing_file_at(&staging, &wav_name)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let stem_entry = guard
            .manifest()
            .stems
            .iter()
            .find(|s| s.file_name == wav_name)
            .expect("stem");
        verify_stem_from_handle(&file, stem_entry)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    }
    if let Some(spec) = recovery_report_spec_from_manifest(syndicate.recovery_report.as_ref()) {
        let src = output
            .report_path
            .as_ref()
            .ok_or_else(|| OrchestratorError::Other("missing recovery report".into()))?;
        let sha = output
            .report_sha256
            .as_deref()
            .ok_or_else(|| OrchestratorError::Other("missing recovery report digest".into()))?;
        install_verified_file(&staging, &spec.file_name, src, spec.byte_count, sha)?;
        let file = open_existing_file_at(&staging, &spec.file_name)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        verify_recovery_report_from_handle(&file, &spec)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    }
    sync_dir_exact(&staging).map_err(|e| OrchestratorError::Other(e.to_string()))?;
    Ok(())
}

fn install_verified_file(
    staging: &DirHandle,
    dest_name: &str,
    src: &Path,
    expected_len: u64,
    expected_sha: &str,
) -> Result<(), OrchestratorError> {
    if open_existing_file_at(staging, dest_name).is_ok() {
        return Ok(());
    }
    let meta = std::fs::metadata(src).map_err(|e| OrchestratorError::Other(e.to_string()))?;
    if meta.len() != expected_len {
        return Err(OrchestratorError::Other(format!(
            "source {} length {} != {}",
            src.display(),
            meta.len(),
            expected_len
        )));
    }
    let file = File::open(src).map_err(|e| OrchestratorError::Other(e.to_string()))?;
    let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
        .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    if sha != expected_sha {
        return Err(OrchestratorError::Other(format!(
            "source {} digest mismatch",
            src.display()
        )));
    }
    let mut dest = create_new_file_at(staging, dest_name)
        .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    let mut src_file = File::open(src).map_err(|e| OrchestratorError::Other(e.to_string()))?;
    let mut offset = 0u64;
    let mut buf = vec![0u8; DEFAULT_STREAM_CHUNK as usize];
    loop {
        let n = src_file
            .read(&mut buf)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        if n == 0 {
            break;
        }
        dest.write_all_at(offset, &buf[..n])
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        offset += n as u64;
    }
    dest.std_file()
        .sync_all()
        .map_err(|e| OrchestratorError::Other(e.to_string()))?;
    Ok(())
}

pub fn resolve_published_paths(
    recording_parent: &Path,
    guard: &DeliverySessionGuard,
) -> PublishedLocalOutput {
    let identity = guard.identity();
    let mut path = recording_parent.to_path_buf();
    for comp in identity.final_parent_relative.iter() {
        path.push(comp.as_str());
    }
    path.push(&identity.final_session_name);
    let report_path = guard
        .recovery_report()
        .map(|spec| path.join(&spec.file_name));
    PublishedLocalOutput {
        final_session_dir: path,
        recovery_report_path: report_path,
    }
}
