//! Narrow orchestration API over durable voice_delivery primitives (Phase 5).

use crate::voice_delivery::api_binding::persist_api_binding_if_absent;
use crate::voice_delivery::client::{
    validate_stem_range_meta, PendingDelivery, ReceiptRequestBody, VoiceV2Client,
    VoiceV2ClientError, VOICE_V2_MAX_CHUNK_BYTES,
};
use crate::voice_delivery::finalized_manifest::{bind_delivery_identity, SyndicateFinalizedStem};
use crate::voice_delivery::fs::rename_no_replace_same_parent;
use crate::voice_delivery::fs::DestRoot;
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::manifest::ValidatedManifest;
use crate::voice_delivery::marker::verify_stem_from_handle;
use crate::voice_delivery::publication::{
    prepare_publication, publish_prepared, recover_delivery, PublishOptions, RecoveryOutcome,
};
use crate::voice_delivery::receipt_state::{ReceiptGenerationError, ReceiptPhase, ReceiptStore};
use crate::voice_delivery::records::ledger_generation::LedgerStore;
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;
use crate::voice_delivery::PartialStemWriter;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

pub const LOCAL_PUBLICATION_STATE: &str = "published";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryPhase {
    Waiting,
    DownloadingStem { stem: String, offset: u64 },
    Sealing,
    Publishing,
    ReceiptPending,
    Completed,
    Error(String),
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("terminal client: {0}")]
    ClientTerminal(#[from] VoiceV2ClientError),
    #[error("retryable client: {0}")]
    ClientRetryable(VoiceV2ClientError),
    #[error("session: {0}")]
    Session(#[from] crate::voice_delivery::session::SessionError),
    #[error("orchestrator: {0}")]
    Other(String),
}

impl OrchestratorError {
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            OrchestratorError::ClientRetryable(e) => e.retry_after(),
            _ => None,
        }
    }
}

fn map_client_err(e: VoiceV2ClientError) -> OrchestratorError {
    if e.class() == crate::voice_delivery::client::VoiceV2ErrorClass::Retryable {
        OrchestratorError::ClientRetryable(e)
    } else {
        OrchestratorError::ClientTerminal(e)
    }
}

/// Phase 5 host-side delivery orchestrator (filesystem + API); no legacy chunk paths.
pub struct FinalizedVoiceDelivery {
    parent: PathBuf,
    device_id: String,
}

impl FinalizedVoiceDelivery {
    pub fn open_recording_parent(parent: &Path) -> Result<Self, OrchestratorError> {
        let _probe = DestRoot::open(parent).map_err(|e| OrchestratorError::Other(e.to_string()))?;
        Ok(Self {
            parent: parent.to_path_buf(),
            device_id: String::new(),
        })
    }

    pub fn with_device_id(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = device_id.into();
        self
    }

    pub fn deliver_pending<C: VoiceV2Client>(
        &self,
        client: &C,
        pending: &PendingDelivery,
        phase: &mut DeliveryPhase,
    ) -> Result<(), OrchestratorError> {
        let (manifest, identity) =
            bind_delivery_identity(&pending.manifest, &pending.manifest_digest)
                .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        self.deliver_bound(
            client,
            &pending.manifest,
            &pending.sealed_at,
            manifest,
            identity,
            phase,
        )
    }

    pub(crate) fn deliver_bound<C: VoiceV2Client>(
        &self,
        client: &C,
        syndicate: &crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest,
        sealed_at: &str,
        manifest: ValidatedManifest,
        identity: DeliveryImmutableIdentity,
        phase: &mut DeliveryPhase,
    ) -> Result<(), OrchestratorError> {
        let root =
            DestRoot::open(&self.parent).map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let guard = self.open_session_for_delivery(
            root,
            identity.clone(),
            manifest,
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
            RecoveryOutcome::ResumeIngest => {
                self.download_all_stems(client, &guard, syndicate, phase)?;
                *phase = DeliveryPhase::Sealing;
                guard
                    .seal_and_write_publish_intent()
                    .map_err(|e| OrchestratorError::Other(e.to_string()))?;
            }
            RecoveryOutcome::AdvancedToPublishIntent(_) => {}
            RecoveryOutcome::Published(_) | RecoveryOutcome::IdempotentPublished => {
                *phase = DeliveryPhase::ReceiptPending;
                self.post_receipt_if_needed(client, &guard, &identity)?;
                *phase = DeliveryPhase::Completed;
                return Ok(());
            }
            RecoveryOutcome::Quarantined(_) => {
                *phase = DeliveryPhase::Error("quarantined".into());
                return Ok(());
            }
        }

        self.publish_if_needed(&guard, phase)?;
        self.post_receipt_if_needed(client, &guard, &identity)?;
        *phase = DeliveryPhase::Completed;
        Ok(())
    }

    fn open_session_for_delivery(
        &self,
        root: DestRoot,
        identity: DeliveryImmutableIdentity,
        manifest: ValidatedManifest,
        _session_id: &str,
    ) -> Result<DeliverySessionGuard, OrchestratorError> {
        if let Ok(guard) = DeliverySessionGuard::begin_for_recovery(
            DestRoot::open(&self.parent).map_err(|e| OrchestratorError::Other(e.to_string()))?,
            identity.clone(),
            manifest.clone(),
            true,
        ) {
            let ledger = LedgerStore::open_for_guard(&guard)
                .map_err(|e| OrchestratorError::Other(e.to_string()))?;
            if ledger
                .read_highest_valid()
                .ok()
                .flatten()
                .is_some_and(|g| g.state == LedgerState::Published)
            {
                return Ok(guard);
            }
        }
        DeliverySessionGuard::begin_bound(root, identity, manifest, true, true).map_err(Into::into)
    }

    fn download_all_stems<C: VoiceV2Client>(
        &self,
        client: &C,
        guard: &DeliverySessionGuard,
        syndicate: &crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest,
        phase: &mut DeliveryPhase,
    ) -> Result<(), OrchestratorError> {
        for stem in &syndicate.stems {
            self.download_one_stem(client, guard, syndicate, stem, phase)?;
        }
        Ok(())
    }

    fn download_one_stem<C: VoiceV2Client>(
        &self,
        client: &C,
        guard: &DeliverySessionGuard,
        syndicate: &crate::voice_delivery::finalized_manifest::SyndicateFinalizedManifest,
        stem: &SyndicateFinalizedStem,
        phase: &mut DeliveryPhase,
    ) -> Result<(), OrchestratorError> {
        let wav_name = format!("{}.wav", stem.path_nick);
        let chunk = VOICE_V2_MAX_CHUNK_BYTES as usize;
        let mut writer = PartialStemWriter::open_or_resume(guard, &wav_name, chunk)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        while writer.resumable_offset() < stem.wav_bytes {
            let offset = writer.resumable_offset();
            *phase = DeliveryPhase::DownloadingStem {
                stem: stem.path_nick.clone(),
                offset,
            };
            let remaining = stem.wav_bytes - offset;
            let req_len = remaining.min(VOICE_V2_MAX_CHUNK_BYTES);
            let mut cursor = Cursor::new(Vec::new());
            let meta = client
                .fetch_stem_range(
                    &syndicate.session_id,
                    &stem.path_nick,
                    offset,
                    req_len,
                    &mut cursor,
                )
                .map_err(map_client_err)?;
            let bytes = cursor.into_inner();
            validate_stem_range_meta(
                &meta,
                offset,
                stem.wav_bytes,
                &stem.sha256,
                bytes.len() as u64,
            )
            .map_err(map_client_err)?;
            writer
                .write_contiguous(offset, &bytes)
                .map_err(|e| OrchestratorError::Other(e.to_string()))?;
            writer
                .commit_checkpoint()
                .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        }
        self.promote_partial_to_wav(guard, &wav_name, &writer)?;
        Ok(())
    }

    fn promote_partial_to_wav(
        &self,
        guard: &DeliverySessionGuard,
        wav_name: &str,
        writer: &PartialStemWriter<'_>,
    ) -> Result<(), OrchestratorError> {
        let staging = guard
            .staging_dir()
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let partial = writer.partial_basename();
        rename_no_replace_same_parent(staging, &partial, wav_name)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let file = crate::voice_delivery::fs::file::open_existing_file_at(staging, wav_name)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let stem = guard
            .manifest()
            .stems
            .iter()
            .find(|s| s.file_name == wav_name)
            .expect("stem");
        verify_stem_from_handle(&file, stem)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        Ok(())
    }

    fn publish_if_needed(
        &self,
        guard: &DeliverySessionGuard,
        phase: &mut DeliveryPhase,
    ) -> Result<(), OrchestratorError> {
        *phase = DeliveryPhase::Publishing;
        let prepared =
            prepare_publication(guard).map_err(|e| OrchestratorError::Other(e.to_string()))?;
        publish_prepared(prepared, None, PublishOptions::default())
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        Ok(())
    }

    fn post_receipt_if_needed<C: VoiceV2Client>(
        &self,
        client: &C,
        guard: &DeliverySessionGuard,
        identity: &DeliveryImmutableIdentity,
    ) -> Result<(), OrchestratorError> {
        let ledger = LedgerStore::open_for_guard(guard)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        let published = ledger
            .read_highest_valid()
            .map_err(|e| OrchestratorError::Other(e.to_string()))?
            .filter(|g| g.state == LedgerState::Published);
        if published.is_none() {
            return Ok(());
        }
        let gen = published.unwrap().generation;
        let local_receipt_id = format!("{}:{}", identity.delivery_uuid, gen);
        let receipt = ReceiptStore::open_for_guard(guard)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        match receipt.read_highest_valid() {
            Ok(Some(state)) if state.phase == ReceiptPhase::Acked => {
                if state.local_receipt_id == local_receipt_id {
                    return Ok(());
                }
                return Err(OrchestratorError::Other("receipt id drift".into()));
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                receipt
                    .commit_pending(&local_receipt_id)
                    .map_err(|e| OrchestratorError::Other(e.to_string()))?;
            }
            Err(ReceiptGenerationError::IdentityMismatch) => {
                return Err(OrchestratorError::Other("receipt identity conflict".into()));
            }
            Err(e) => return Err(OrchestratorError::Other(e.to_string())),
        }
        let body = ReceiptRequestBody {
            manifest_digest: identity.manifest_digest.clone(),
            device_id: self.device_id.clone(),
            local_receipt_id: local_receipt_id.clone(),
            local_publication_state: LOCAL_PUBLICATION_STATE.into(),
        };
        client
            .post_receipt(&identity.delivery_uuid, &body)
            .map_err(map_client_err)?;
        receipt
            .commit_acked(&local_receipt_id)
            .map_err(|e| OrchestratorError::Other(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod orchestrator_e2e {
    use super::*;
    use crate::voice_delivery::client::{MockVoiceV2Client, PendingDelivery, VoiceV2ClientError};
    use crate::voice_delivery::finalized_manifest::{
        bind_delivery_identity, compute_finalized_manifest_digest,
        parse_syndicate_finalized_manifest, syndicate_manifest_tests::minimal_wav_value,
    };
    use crate::voice_delivery::hash::hex_digest;
    use sha2::{Digest, Sha256};
    use std::time::Duration;

    fn build_wav_bytes() -> (Vec<u8>, String) {
        let header = crate::voice_delivery::wav::minimal_wav_header(0).unwrap();
        let sha = hex_digest(&Sha256::digest(&header));
        (header, sha)
    }

    #[test]
    fn delivers_minimal_manifest_with_mock_http() {
        let tmp = tempfile::tempdir().unwrap();
        let (wav, sha) = build_wav_bytes();
        let mut raw = minimal_wav_value();
        raw["stems"][0]["sha256"] = serde_json::json!(sha);
        let manifest = parse_syndicate_finalized_manifest(&raw).unwrap();
        let digest = compute_finalized_manifest_digest(&manifest);
        let pending = PendingDelivery {
            session_id: manifest.session_id.clone(),
            manifest_digest: digest,
            sealed_at: "2020-01-01T00:00:00.000Z".into(),
            manifest,
        };
        let mock = MockVoiceV2Client::new();
        mock.set_stem_bytes(&pending.session_id, "user1", wav);
        mock.set_pending(vec![pending.clone()]);
        let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
            .unwrap()
            .with_device_id("device-test");
        let mut phase = DeliveryPhase::Waiting;
        orch.deliver_pending(&mock, &pending, &mut phase).unwrap();
        assert_eq!(phase, DeliveryPhase::Completed);
        assert_eq!(mock.receipt_calls(), 1);
    }

    #[test]
    fn published_receipt_pending_survives_retry_after_then_acks_without_staging() {
        let tmp = tempfile::tempdir().unwrap();
        let (wav, sha) = build_wav_bytes();
        let mut raw = minimal_wav_value();
        raw["stems"][0]["sha256"] = serde_json::json!(sha);
        let manifest = parse_syndicate_finalized_manifest(&raw).unwrap();
        let digest = compute_finalized_manifest_digest(&manifest);
        let pending = PendingDelivery {
            session_id: manifest.session_id.clone(),
            manifest_digest: digest,
            sealed_at: "2020-01-01T00:00:00.000Z".into(),
            manifest,
        };
        let mock = MockVoiceV2Client::new();
        mock.set_stem_bytes(&pending.session_id, "user1", wav);
        mock.set_pending(vec![pending.clone()]);
        mock.push_receipt_error(VoiceV2ClientError::RetryAfter(Duration::from_secs(4)));
        let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
            .unwrap()
            .with_device_id("device-test");
        let mut phase = DeliveryPhase::Waiting;
        let err = orch
            .deliver_pending(&mock, &pending, &mut phase)
            .unwrap_err();
        assert!(matches!(
            err,
            OrchestratorError::ClientRetryable(VoiceV2ClientError::RetryAfter(_))
        ));
        let mut phase2 = DeliveryPhase::Waiting;
        orch.deliver_pending(&mock, &pending, &mut phase2).unwrap();
        assert_eq!(phase2, DeliveryPhase::Completed);
        assert_eq!(mock.receipt_calls(), 1);
    }

    #[test]
    fn api_manifest_digest_differs_from_stem_set_digest() {
        let raw = minimal_wav_value();
        let syndicate = parse_syndicate_finalized_manifest(&raw).unwrap();
        let api_digest = compute_finalized_manifest_digest(&syndicate);
        let (manifest, identity) = bind_delivery_identity(&syndicate, &api_digest).unwrap();
        assert_eq!(identity.manifest_digest, api_digest);
        assert_eq!(identity.stem_set_digest, manifest.digest());
        assert_ne!(identity.manifest_digest, identity.stem_set_digest);
    }
}
