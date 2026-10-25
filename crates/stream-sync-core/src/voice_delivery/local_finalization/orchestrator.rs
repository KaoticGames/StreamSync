//! Local journal download, decode, publish, and receipt orchestration.

use super::cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
use super::client::{LocalFinalizationClient, LocalPendingItem};
use super::contract::{sealed_at_rfc3339_from_ms, LeaseGrant, LeaseProof};
use super::journal_store::{JournalStoreError, LocalJournalStore, SegmentCheckpoint};
use super::lease_keeper::LeaseKeeper;
use super::manifest::{
    compute_local_finalization_manifest_digest, local_manifest_to_syndicate_finalized,
    LocalFinalizationManifest,
};
use super::processor::{
    process_verified_segments_with_options, LocalProcessorError, LocalProcessorOptions,
    LocalProcessorProgress,
};
use super::progress::{
    BoundedProgressEvents, LocalFinalizationPhase, LocalFinalizationProgressEvent,
};
use super::receipt::{
    assert_local_finalization_receipt_matches_manifest, LocalFinalizationReceipt,
    LocalFinalizationReceiptRecoveryReport, LocalFinalizationReceiptStem,
};
use super::storage_preflight::StoragePreflightFailure;
use crate::voice_delivery::client::VoiceV2ClientError;
use crate::voice_delivery::finalized_manifest::{
    compute_finalized_manifest_digest, expected_wav_file_bytes, SyndicateFinalizedStem,
    SyndicateRecoveryReport,
};
use crate::voice_delivery::orchestrator::FinalizedVoiceDelivery;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_CONTENT_TYPE;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;

const CHUNK: u64 = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum LocalOrchestratorError {
    #[error("cancelled")]
    Cancelled,
    #[error("paused")]
    Paused,
    #[error("pause lease release failed")]
    PauseLeaseReleaseFailed,
    #[error("client: {0}")]
    Client(#[from] VoiceV2ClientError),
    #[error("store: {0}")]
    Store(String),
    #[error("journal store conflict")]
    StoreConflict,
    #[error("processor: {0}")]
    Processor(String),
    #[error("orchestrator: {0}")]
    Other(String),
    #[error("storage preflight: {0}")]
    StoragePreflight(StoragePreflightFailure),
}

/// Shared pause/cancel handles wired from the In-Sync worker and HTTP control plane.
#[derive(Clone)]
pub struct LocalFinalizationControls {
    pub cancel: LocalFinalizationCancel,
    pub paused: Arc<AtomicBool>,
    /// When true, pause at segment boundaries releases the delivery lease instead of renewing.
    pub release_lease_on_pause: Arc<AtomicBool>,
}

impl Default for LocalFinalizationControls {
    fn default() -> Self {
        Self {
            cancel: LocalFinalizationCancel::new(),
            paused: Arc::new(AtomicBool::new(false)),
            release_lease_on_pause: Arc::new(AtomicBool::new(true)),
        }
    }
}

pub struct LocalFinalizationOrchestrator {
    parent: PathBuf,
    device_id: String,
    phase: Mutex<LocalFinalizationPhase>,
    events: Arc<Mutex<BoundedProgressEvents>>,
    controls: LocalFinalizationControls,
}

impl LocalFinalizationOrchestrator {
    pub fn open(parent: &Path, device_id: impl Into<String>) -> Self {
        Self::open_with_controls(parent, device_id, LocalFinalizationControls::default())
    }

    pub fn open_with_controls(
        parent: &Path,
        device_id: impl Into<String>,
        controls: LocalFinalizationControls,
    ) -> Self {
        Self {
            parent: parent.to_path_buf(),
            device_id: device_id.into(),
            phase: Mutex::new(LocalFinalizationPhase::Queued),
            events: Arc::new(Mutex::new(BoundedProgressEvents::default())),
            controls,
        }
    }

    pub fn controls(&self) -> &LocalFinalizationControls {
        &self.controls
    }

    pub fn cancellation(&self) -> LocalFinalizationCancel {
        self.controls.cancel.clone()
    }

    pub fn set_paused(&self, paused: bool) {
        self.controls.paused.store(paused, Ordering::SeqCst);
        if paused {
            *self.phase.lock().unwrap() = LocalFinalizationPhase::Paused;
        }
    }

    pub fn phase(&self) -> LocalFinalizationPhase {
        *self.phase.lock().unwrap()
    }

    pub fn drain_events(&self) -> Vec<LocalFinalizationProgressEvent> {
        self.events.lock().unwrap().drain()
    }

    /// Non-destructive bounded snapshot for live `/api/in-sync/status` while `process_pending` runs.
    pub fn progress_events_snapshot(&self) -> Vec<LocalFinalizationProgressEvent> {
        self.events.lock().unwrap().snapshot()
    }

    fn emit(&self, event: LocalFinalizationProgressEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn set_phase(&self, phase: LocalFinalizationPhase) {
        *self.phase.lock().unwrap() = phase;
    }

    fn check_pause_cancel<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        _client: &C,
        _session_id: &str,
        _lease: &mut LeaseSession<C>,
    ) -> Result<(), LocalOrchestratorError> {
        self.controls
            .cancel
            .check()
            .map_err(|_| LocalOrchestratorError::Cancelled)?;
        if self.controls.paused.load(Ordering::SeqCst) {
            self.set_phase(LocalFinalizationPhase::Paused);
            return Err(LocalOrchestratorError::Paused);
        }
        Ok(())
    }

    fn ensure_lease_alive<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        lease: &LeaseSession<C>,
    ) -> Result<(), LocalOrchestratorError> {
        if lease.keeper.as_ref().is_some_and(|k| k.renew_failed()) {
            return Err(LocalOrchestratorError::Other("lease renewal failed".into()));
        }
        Ok(())
    }

    pub fn process_pending<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        client: &C,
        pending: &LocalPendingItem,
    ) -> Result<(), LocalOrchestratorError> {
        let sealed_at = sealed_at_rfc3339_from_ms(pending.sealed_at_ms);
        self.set_phase(LocalFinalizationPhase::Claiming);
        self.controls
            .cancel
            .check()
            .map_err(|_| LocalOrchestratorError::Cancelled)?;

        let grant = client
            .claim_delivery(&pending.session_id, &self.device_id)
            .map_err(LocalOrchestratorError::Client)?;
        if grant.session_id != pending.session_id
            || grant.manifest_digest != pending.manifest_digest
        {
            let _ = client.release_delivery(&pending.session_id, &self.device_id, &grant.proof());
            return Err(LocalOrchestratorError::Client(
                VoiceV2ClientError::IdentityConflict,
            ));
        }

        let mut lease = LeaseSession::new(client.clone(), grant, &self.device_id);

        let run_result = (|| {
            let manifest = client
                .fetch_manifest(&pending.session_id, lease.proof())
                .map_err(LocalOrchestratorError::Client)?;
            let computed = compute_local_finalization_manifest_digest(&manifest);
            if computed != pending.manifest_digest || manifest.session_id != pending.session_id {
                return Err(LocalOrchestratorError::Client(
                    VoiceV2ClientError::IdentityConflict,
                ));
            }

            let store = LocalJournalStore::open(&self.parent, &manifest.session_id)
                .map_err(map_journal_store_err)?;
            store
                .persist_manifest(&manifest)
                .map_err(map_journal_store_err)?;
            self.emit(LocalFinalizationProgressEvent::JournalReady {
                session_id: manifest.session_id.clone(),
            });

            if self.try_receipt_only_completion(
                client,
                &store,
                &manifest,
                &pending.manifest_digest,
                &mut lease,
            )? {
                lease.stop_keeper_without_release();
                self.set_phase(LocalFinalizationPhase::Complete);
                return Ok(());
            }

            self.check_pause_cancel(client, &pending.session_id, &mut lease)?;
            self.ensure_lease_alive(&lease)?;
            self.download_all(client, &store, &manifest, &mut lease)?;
            self.check_pause_cancel(client, &manifest.session_id, &mut lease)?;
            self.ensure_lease_alive(&lease)?;
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
            self.ensure_lease_alive(&lease)?;
            let session_id = manifest.session_id.clone();
            let orch_events = Arc::clone(&self.events);
            let progress_sink = Arc::new(move |p: LocalProcessorProgress| {
                if p.processed_segments == 0 {
                    return;
                }
                orch_events.lock().unwrap().push(
                    LocalFinalizationProgressEvent::TimelineProgress {
                        session_id: session_id.clone(),
                        processed_segments: p.processed_segments,
                        total_segments: p.total_segments,
                    },
                );
            });
            let output = process_verified_segments_with_options(
                &manifest,
                &segment_paths,
                &staging,
                LocalProcessorOptions {
                    cancel: Some(self.controls.cancel.clone()),
                    paused: Some(self.controls.paused.clone()),
                    progress: Some(progress_sink),
                },
            )
            .map_err(map_processor_err)?;
            self.ensure_lease_alive(&lease)?;
            self.publish_and_receipt(
                client,
                &manifest,
                &pending.manifest_digest,
                &sealed_at,
                output,
                &mut lease,
            )?;
            lease.stop_keeper_without_release();
            self.set_phase(LocalFinalizationPhase::Complete);
            Ok(())
        })();

        if matches!(&run_result, Err(LocalOrchestratorError::Paused)) {
            lease.stop_keeper();
            if self.controls.release_lease_on_pause.load(Ordering::SeqCst) {
                match client.release_delivery(&pending.session_id, &self.device_id, lease.proof()) {
                    Ok(()) => return run_result,
                    Err(_) => {
                        return Err(LocalOrchestratorError::PauseLeaseReleaseFailed);
                    }
                }
            }
            return run_result;
        }
        if matches!(&run_result, Err(LocalOrchestratorError::Cancelled)) {
            self.set_phase(LocalFinalizationPhase::Cancelled);
        }
        if run_result.is_err() && !lease.receipt_posted {
            lease.release_best_effort(client, &self.device_id);
        } else if lease.receipt_posted {
            lease.stop_keeper_without_release();
        }
        run_result
    }

    fn download_all<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        client: &C,
        store: &LocalJournalStore,
        manifest: &LocalFinalizationManifest,
        lease: &mut LeaseSession<C>,
    ) -> Result<(), LocalOrchestratorError> {
        self.set_phase(LocalFinalizationPhase::Downloading);
        if let Err(pref) =
            LocalJournalStore::run_storage_preflight(&self.parent, manifest, 512 * 1024 * 1024)
        {
            return Err(LocalOrchestratorError::StoragePreflight(pref));
        }
        let mut checkpoint = store.load_checkpoint().map_err(map_journal_store_err)?;
        if !checkpoint.segments.is_empty() {
            LocalJournalStore::validate_checkpoint_against_manifest(&checkpoint, manifest)
                .map_err(map_journal_store_err)?;
        }
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
            self.check_pause_cancel(client, &manifest.session_id, lease)?;
            self.ensure_lease_alive(lease)?;
            if store
                .verify_segment_complete(seg.segment_seq, seg.byte_length, &seg.sha256)
                .map_err(map_journal_store_err)?
            {
                self.emit(LocalFinalizationProgressEvent::SegmentVerified {
                    session_id: manifest.session_id.clone(),
                    segment_seq: seg.segment_seq,
                });
                continue;
            }
            let mut offset = store
                .reconcile_segment_resume_bytes(seg.segment_seq, seg.byte_length)
                .map_err(map_journal_store_err)?;
            if let Some(entry) = checkpoint
                .segments
                .iter_mut()
                .find(|c| c.segment_seq == seg.segment_seq)
            {
                if entry.bytes_complete != offset {
                    entry.bytes_complete = offset;
                    store
                        .save_checkpoint(&checkpoint)
                        .map_err(map_journal_store_err)?;
                }
            }
            while offset < seg.byte_length {
                self.check_pause_cancel(client, &manifest.session_id, lease)?;
                self.ensure_lease_alive(lease)?;
                let chunk = (seg.byte_length - offset).min(CHUNK);
                let mut file = store
                    .open_segment_writer(seg.segment_seq, offset > 0)
                    .map_err(map_journal_store_err)?;
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
                        lease.proof(),
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
                    .map_err(map_journal_store_err)?;
            }
            if !store
                .verify_segment_complete(seg.segment_seq, seg.byte_length, &seg.sha256)
                .map_err(map_journal_store_err)?
            {
                return Err(LocalOrchestratorError::StoreConflict);
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
                .map_err(map_journal_store_err)?;
            self.emit(LocalFinalizationProgressEvent::SegmentVerified {
                session_id: manifest.session_id.clone(),
                segment_seq: seg.segment_seq,
            });
        }
        self.set_phase(LocalFinalizationPhase::ReadyToProcess);
        Ok(())
    }

    fn try_receipt_only_completion<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        client: &C,
        store: &LocalJournalStore,
        manifest: &LocalFinalizationManifest,
        manifest_digest: &str,
        lease: &mut LeaseSession<C>,
    ) -> Result<bool, LocalOrchestratorError> {
        let state = store
            .load_receipt_durable_state()
            .map_err(map_journal_store_err)?;
        let intent = match state.intent {
            None => return Ok(false),
            Some(receipt) => receipt,
        };
        self.validate_durable_receipt_for_recovery(&intent, manifest, manifest_digest)?;
        LocalJournalStore::verify_canonical_publication_matches_receipt(&self.parent, &intent)
            .map_err(map_journal_store_err)?;

        if state.ack.is_some() {
            lease.receipt_posted = true;
            return Ok(true);
        }

        self.set_phase(LocalFinalizationPhase::Receipting);
        #[cfg(all(debug_assertions, feature = "lifecycle-test-support"))]
        super::post_publication_receipt_gate::pause_post_publication_receipt_gate(
            &self.parent,
            &manifest.session_id,
            super::post_publication_receipt_gate::PostPublicationReceiptBoundary::BeforeReceiptReplayPost,
        );
        self.check_pause_cancel(client, &manifest.session_id, lease)?;
        lease.fence_lease_authority(client, &self.device_id)?;
        client
            .post_receipt(&manifest.session_id, &intent, lease.proof())
            .map_err(LocalOrchestratorError::Client)?;
        store
            .persist_receipt_ack(&intent)
            .map_err(map_journal_store_err)?;
        lease.receipt_posted = true;
        self.emit(LocalFinalizationProgressEvent::ReceiptAcknowledged {
            session_id: manifest.session_id.clone(),
        });
        Ok(true)
    }

    fn validate_durable_receipt_for_recovery(
        &self,
        receipt: &LocalFinalizationReceipt,
        manifest: &LocalFinalizationManifest,
        manifest_digest: &str,
    ) -> Result<(), LocalOrchestratorError> {
        assert_local_finalization_receipt_matches_manifest(receipt, manifest)
            .map_err(|_| LocalOrchestratorError::StoreConflict)?;
        if receipt.device_id != self.device_id {
            return Err(LocalOrchestratorError::StoreConflict);
        }
        if receipt.delivery_manifest_digest != manifest_digest {
            return Err(LocalOrchestratorError::StoreConflict);
        }
        Ok(())
    }

    fn publish_and_receipt<C: LocalFinalizationClient + Clone + 'static>(
        &self,
        client: &C,
        manifest: &LocalFinalizationManifest,
        manifest_digest: &str,
        sealed_at: &str,
        output: super::processor::LocalProcessOutput,
        lease: &mut LeaseSession<C>,
    ) -> Result<(), LocalOrchestratorError> {
        let store = LocalJournalStore::open(&self.parent, &manifest.session_id)
            .map_err(map_journal_store_err)?;
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
        #[cfg(all(debug_assertions, feature = "lifecycle-test-support"))]
        super::post_publication_receipt_gate::pause_post_publication_receipt_gate(
            &self.parent,
            &manifest.session_id,
            super::post_publication_receipt_gate::PostPublicationReceiptBoundary::BeforePublication,
        );
        self.check_pause_cancel(client, &manifest.session_id, lease)?;
        let published = delivery
            .publish_local_decoded(&syndicate, &output_digest, sealed_at, &output)
            .map_err(|e| LocalOrchestratorError::Other(e.to_string()))?;
        self.emit(LocalFinalizationProgressEvent::PublicationComplete {
            session_id: manifest.session_id.clone(),
            final_session_dir: published.final_session_dir.to_string_lossy().to_string(),
            recovery_report_path: published
                .recovery_report_path
                .map(|p| p.to_string_lossy().to_string()),
        });
        #[cfg(all(debug_assertions, feature = "lifecycle-test-support"))]
        super::post_publication_receipt_gate::pause_post_publication_receipt_gate(
            &self.parent,
            &manifest.session_id,
            super::post_publication_receipt_gate::PostPublicationReceiptBoundary::AfterPublicationBeforeReceiptIntent,
        );
        self.check_pause_cancel(client, &manifest.session_id, lease)?;
        lease.fence_lease_authority(client, &self.device_id)?;
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
        store
            .persist_receipt_intent(&receipt)
            .map_err(map_journal_store_err)?;
        #[cfg(all(debug_assertions, feature = "lifecycle-test-support"))]
        super::post_publication_receipt_gate::pause_post_publication_receipt_gate(
            &self.parent,
            &manifest.session_id,
            super::post_publication_receipt_gate::PostPublicationReceiptBoundary::AfterReceiptIntentBeforeFreshReceiptPost,
        );
        self.check_pause_cancel(client, &manifest.session_id, lease)?;
        lease.fence_lease_authority(client, &self.device_id)?;
        client
            .post_receipt(&manifest.session_id, &receipt, lease.proof())
            .map_err(LocalOrchestratorError::Client)?;
        store
            .persist_receipt_ack(&receipt)
            .map_err(map_journal_store_err)?;
        lease.receipt_posted = true;
        self.emit(LocalFinalizationProgressEvent::ReceiptAcknowledged {
            session_id: manifest.session_id.clone(),
        });
        Ok(())
    }
}

impl From<LocalFinalizationCancelled> for LocalOrchestratorError {
    fn from(_: LocalFinalizationCancelled) -> Self {
        LocalOrchestratorError::Cancelled
    }
}

fn map_processor_err(e: LocalProcessorError) -> LocalOrchestratorError {
    match e {
        LocalProcessorError::Cancelled => LocalOrchestratorError::Cancelled,
        LocalProcessorError::Paused => LocalOrchestratorError::Paused,
        other => LocalOrchestratorError::Processor(other.to_string()),
    }
}

fn map_journal_store_err(e: JournalStoreError) -> LocalOrchestratorError {
    match e {
        JournalStoreError::Conflict => LocalOrchestratorError::StoreConflict,
        other => LocalOrchestratorError::Store(other.to_string()),
    }
}

struct LeaseSession<C: LocalFinalizationClient + Clone> {
    session_id: String,
    proof: LeaseProof,
    expires_at_ms: i64,
    keeper: Option<LeaseKeeper>,
    receipt_posted: bool,
    _client: C,
}

impl<C: LocalFinalizationClient + Clone + 'static> LeaseSession<C> {
    fn new(client: C, grant: LeaseGrant, device_id: &str) -> Self {
        let proof = grant.proof();
        let keeper = LeaseKeeper::spawn(
            client.clone(),
            grant.session_id.clone(),
            device_id.to_string(),
            proof.clone(),
            grant.expires_at_ms,
        );
        Self {
            session_id: grant.session_id,
            proof,
            expires_at_ms: grant.expires_at_ms,
            keeper: Some(keeper),
            receipt_posted: false,
            _client: client,
        }
    }

    /// Synchronous lease-authority fence at receipt boundaries (not the background keeper interval).
    fn fence_lease_authority(
        &mut self,
        client: &C,
        device_id: &str,
    ) -> Result<(), LocalOrchestratorError> {
        if self.keeper.as_ref().is_some_and(|k| k.renew_failed()) {
            return Err(LocalOrchestratorError::Other("lease renewal failed".into()));
        }
        match client.renew_lease(&self.session_id, device_id, &self.proof) {
            Ok(next_expires) => {
                if let Some(ms) = next_expires {
                    self.expires_at_ms = ms;
                }
                Ok(())
            }
            Err(err) => Err(LocalOrchestratorError::Client(err)),
        }
    }

    fn proof(&self) -> &LeaseProof {
        &self.proof
    }

    fn stop_keeper(&mut self) {
        if let Some(mut k) = self.keeper.take() {
            k.stop();
        }
    }

    fn stop_keeper_without_release(&mut self) {
        self.stop_keeper();
    }

    fn release_best_effort(&mut self, client: &C, device_id: &str) {
        self.stop_keeper();
        let _ = client.release_delivery(&self.session_id, device_id, &self.proof);
    }
}
