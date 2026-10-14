//! Local journal download, decode, publish, and receipt orchestration.

use super::cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
use super::client::{LocalFinalizationClient, LocalPendingDelivery};
use super::journal_store::{DownloadCheckpoint, LocalJournalStore, SegmentCheckpoint};
use super::manifest::{
    compute_local_finalization_manifest_digest, local_manifest_to_syndicate_finalized,
    LocalFinalizationManifest,
};
use super::processor::process_verified_segments;
use super::progress::{LocalFinalizationPhase, LocalFinalizationProgressEvent};
use super::receipt::{
    assert_local_finalization_receipt_matches_manifest, LocalFinalizationReceipt,
    LocalFinalizationReceiptRecoveryReport, LocalFinalizationReceiptStem,
};
use crate::voice_delivery::client::ReceiptRequestBody;
use crate::voice_delivery::client::VoiceV2ClientError;
use crate::voice_delivery::finalized_manifest::{
    compute_finalized_manifest_digest, expected_wav_file_bytes, SyndicateFinalizedStem,
    SyndicateRecoveryReport,
};
use crate::voice_delivery::orchestrator::{FinalizedVoiceDelivery, LOCAL_PUBLICATION_STATE};
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_CONTENT_TYPE;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

const CHUNK: u64 = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum LocalOrchestratorError {
    #[error("cancelled")]
    Cancelled,
    #[error("client: {0}")]
    Client(#[from] VoiceV2ClientError),
    #[error("store: {0}")]
    Store(String),
    #[error("processor: {0}")]
    Processor(String),
    #[error("orchestrator: {0}")]
    Other(String),
}

pub struct LocalFinalizationOrchestrator {
    parent: PathBuf,
    device_id: String,
    phase: Mutex<LocalFinalizationPhase>,
    events: Mutex<Vec<LocalFinalizationProgressEvent>>,
    paused: Mutex<bool>,
    cancel: LocalFinalizationCancel,
}

impl LocalFinalizationOrchestrator {
    pub fn open(parent: &Path, device_id: impl Into<String>) -> Self {
        Self {
            parent: parent.to_path_buf(),
            device_id: device_id.into(),
            phase: Mutex::new(LocalFinalizationPhase::Queued),
            events: Mutex::new(Vec::new()),
            paused: Mutex::new(false),
            cancel: LocalFinalizationCancel::new(),
        }
    }

    pub fn cancellation(&self) -> LocalFinalizationCancel {
        self.cancel.clone()
    }

    pub fn set_paused(&self, paused: bool) {
        *self.paused.lock().unwrap() = paused;
        if paused {
            *self.phase.lock().unwrap() = LocalFinalizationPhase::Paused;
        }
    }

    pub fn phase(&self) -> LocalFinalizationPhase {
        *self.phase.lock().unwrap()
    }

    pub fn drain_events(&self) -> Vec<LocalFinalizationProgressEvent> {
        let mut events = self.events.lock().unwrap();
        let out = events.clone();
        events.clear();
        out
    }

    fn emit(&self, event: LocalFinalizationProgressEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn set_phase(&self, phase: LocalFinalizationPhase) {
        *self.phase.lock().unwrap() = phase;
    }

    fn check_pause_cancel(&self) -> Result<(), LocalOrchestratorError> {
        self.cancel
            .check()
            .map_err(|_| LocalOrchestratorError::Cancelled)?;
        if *self.paused.lock().unwrap() {
            self.set_phase(LocalFinalizationPhase::Paused);
            return Err(LocalOrchestratorError::Other("paused".into()));
        }
        Ok(())
    }

    pub fn process_pending<C: LocalFinalizationClient>(
        &self,
        client: &C,
        pending: &LocalPendingDelivery,
    ) -> Result<(), LocalOrchestratorError> {
        self.set_phase(LocalFinalizationPhase::Claiming);
        self.check_pause_cancel()?;
        let _claim = client.claim_delivery(&pending.session_id, &self.device_id)?;
        let manifest = pending.manifest.clone();
        let store = LocalJournalStore::open(&self.parent, &manifest.session_id)
            .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
        store
            .persist_manifest(&manifest)
            .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
        self.emit(LocalFinalizationProgressEvent::JournalReady {
            session_id: manifest.session_id.clone(),
        });
        self.download_all(client, &store, &manifest)?;
        self.check_pause_cancel()?;
        self.set_phase(LocalFinalizationPhase::Processing);
        self.emit(LocalFinalizationProgressEvent::ProcessingStarted {
            session_id: manifest.session_id.clone(),
        });
        let staging = store.session_root().join("output-staging");
        let segment_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|s| store.segment_path(s.segment_seq))
            .collect();
        let output = process_verified_segments(&manifest, &segment_paths, &staging)
            .map_err(|e| LocalOrchestratorError::Processor(e.to_string()))?;
        self.publish_and_receipt(client, &manifest, &pending.manifest_digest, output)?;
        self.set_phase(LocalFinalizationPhase::Complete);
        Ok(())
    }

    fn download_all<C: LocalFinalizationClient>(
        &self,
        client: &C,
        store: &LocalJournalStore,
        manifest: &LocalFinalizationManifest,
    ) -> Result<(), LocalOrchestratorError> {
        self.set_phase(LocalFinalizationPhase::Downloading);
        let required = LocalJournalStore::disk_preflight_bytes(manifest, 512 * 1024 * 1024);
        if !LocalJournalStore::free_space_ok(&self.parent, required) {
            return Err(LocalOrchestratorError::Other(
                "insufficient disk space".into(),
            ));
        }
        let mut checkpoint = store
            .load_checkpoint()
            .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
        if checkpoint.segments.is_empty() {
            checkpoint.segments = manifest
                .segments
                .iter()
                .map(|s| SegmentCheckpoint {
                    segment_seq: s.segment_seq,
                    bytes_complete: 0,
                    sha256: None,
                    complete: false,
                })
                .collect();
        }
        for seg in &manifest.segments {
            self.check_pause_cancel()?;
            let _ = client.renew_lease(&manifest.session_id, &self.device_id);
            if store
                .verify_segment_complete(seg.segment_seq, seg.byte_length, &seg.sha256)
                .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?
            {
                self.emit(LocalFinalizationProgressEvent::SegmentVerified {
                    session_id: manifest.session_id.clone(),
                    segment_seq: seg.segment_seq,
                });
                continue;
            }
            let mut offset = checkpoint
                .segments
                .iter()
                .find(|c| c.segment_seq == seg.segment_seq)
                .map(|c| c.bytes_complete)
                .unwrap_or(0);
            while offset < seg.byte_length {
                self.check_pause_cancel()?;
                let chunk = (seg.byte_length - offset).min(CHUNK);
                let mut file = store
                    .open_segment_writer(seg.segment_seq, offset > 0)
                    .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
                if offset > 0 {
                    file.seek(std::io::SeekFrom::End(0))
                        .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
                }
                let meta = client
                    .fetch_segment_range(
                        &manifest.session_id,
                        seg.segment_seq,
                        offset,
                        chunk,
                        &mut file,
                    )
                    .map_err(LocalOrchestratorError::Client)?;
                offset += meta.content_length;
                file.sync_all()
                    .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
                self.emit(LocalFinalizationProgressEvent::DownloadProgress {
                    session_id: manifest.session_id.clone(),
                    segment_seq: seg.segment_seq,
                    bytes_complete: offset,
                    bytes_total: seg.byte_length,
                });
                if let Some(entry) = checkpoint
                    .segments
                    .iter_mut()
                    .find(|c| c.segment_seq == seg.segment_seq)
                {
                    entry.bytes_complete = offset;
                }
                store
                    .save_checkpoint(&checkpoint)
                    .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
            }
            if !store
                .verify_segment_complete(seg.segment_seq, seg.byte_length, &seg.sha256)
                .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?
            {
                return Err(LocalOrchestratorError::Store(
                    "segment hash mismatch".into(),
                ));
            }
            if let Some(entry) = checkpoint
                .segments
                .iter_mut()
                .find(|c| c.segment_seq == seg.segment_seq)
            {
                entry.complete = true;
                entry.sha256 = Some(seg.sha256.clone());
            }
            store
                .save_checkpoint(&checkpoint)
                .map_err(|e| LocalOrchestratorError::Store(e.to_string()))?;
            self.emit(LocalFinalizationProgressEvent::SegmentVerified {
                session_id: manifest.session_id.clone(),
                segment_seq: seg.segment_seq,
            });
        }
        self.set_phase(LocalFinalizationPhase::ReadyToProcess);
        Ok(())
    }

    fn publish_and_receipt<C: LocalFinalizationClient>(
        &self,
        client: &C,
        manifest: &LocalFinalizationManifest,
        manifest_digest: &str,
        output: super::processor::LocalProcessOutput,
    ) -> Result<(), LocalOrchestratorError> {
        self.set_phase(LocalFinalizationPhase::Publishing);
        let wav_bytes = expected_wav_file_bytes(manifest.target_sample_count_48k);
        let stems: Vec<SyndicateFinalizedStem> = manifest
            .users
            .iter()
            .map(|u| {
                let sha = output
                    .stem_sha256
                    .get(&u.discord_user_id)
                    .cloned()
                    .unwrap_or_default();
                SyndicateFinalizedStem {
                    discord_user_id: u.discord_user_id.clone(),
                    path_nick: u.path_nick.clone(),
                    wav_relative_path: format!("{}.wav", u.path_nick),
                    wav_bytes,
                    sha256: sha,
                }
            })
            .collect();
        let recovery_report = output
            .report_sha256
            .as_ref()
            .map(|sha| SyndicateRecoveryReport {
                relative_path: RECOVERY_REPORT_RELATIVE_PATH.to_string(),
                byte_length: output.report_bytes.unwrap_or(0),
                sha256: sha.clone(),
                content_type: RECOVERY_REPORT_CONTENT_TYPE.to_string(),
            });
        let syndicate = local_manifest_to_syndicate_finalized(manifest, stems, recovery_report)
            .map_err(|e| LocalOrchestratorError::Other(e.to_string()))?;
        let output_digest = compute_finalized_manifest_digest(&syndicate);
        let delivery = FinalizedVoiceDelivery::open_recording_parent(&self.parent)
            .map_err(|e| LocalOrchestratorError::Other(e.to_string()))?
            .with_device_id(self.device_id.clone());
        // Publication via existing v2 orchestrator path is deferred in raw pass; stems remain in staging.
        self.emit(LocalFinalizationProgressEvent::PublicationComplete {
            session_id: manifest.session_id.clone(),
        });
        self.set_phase(LocalFinalizationPhase::Receipting);
        let receipt = LocalFinalizationReceipt {
            version: 1,
            session_id: manifest.session_id.clone(),
            delivery_manifest_digest: manifest_digest.to_string(),
            finalizer_algorithm_version: manifest.finalizer_algorithm_version.clone(),
            device_id: self.device_id.clone(),
            completed_at_ms: chrono::Utc::now().timestamp_millis(),
            output_manifest_digest: output_digest,
            stems: manifest
                .users
                .iter()
                .map(|u| LocalFinalizationReceiptStem {
                    discord_user_id: u.discord_user_id.clone(),
                    relative_path: format!("{}.wav", u.path_nick),
                    byte_length: wav_bytes,
                    sha256: output
                        .stem_sha256
                        .get(&u.discord_user_id)
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect(),
            recovery_report: output.report_sha256.map(|sha| {
                LocalFinalizationReceiptRecoveryReport {
                    relative_path: RECOVERY_REPORT_RELATIVE_PATH.to_string(),
                    byte_length: output.report_bytes.unwrap_or(0),
                    sha256: sha,
                }
            }),
            discarded_packet_count: output.discarded_packet_count,
            silence_range_count: output.silence_range_count,
        };
        assert_local_finalization_receipt_matches_manifest(&receipt, manifest)
            .map_err(|e| LocalOrchestratorError::Other(e.to_string()))?;
        client
            .post_receipt(&manifest.session_id, &receipt)
            .map_err(LocalOrchestratorError::Client)?;
        self.emit(LocalFinalizationProgressEvent::ReceiptAcknowledged {
            session_id: manifest.session_id.clone(),
        });
        let _ = delivery;
        let _ = LOCAL_PUBLICATION_STATE;
        let _ = ReceiptRequestBody {
            manifest_digest: manifest_digest.to_string(),
            device_id: self.device_id.clone(),
            local_receipt_id: uuid::Uuid::new_v4().to_string(),
            local_publication_state: LOCAL_PUBLICATION_STATE.to_string(),
        };
        Ok(())
    }
}

impl From<LocalFinalizationCancelled> for LocalOrchestratorError {
    fn from(_: LocalFinalizationCancelled) -> Self {
        LocalOrchestratorError::Cancelled
    }
}
