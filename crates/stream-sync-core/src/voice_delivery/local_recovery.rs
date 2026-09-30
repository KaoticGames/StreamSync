//! Scan local control-dir ledgers and resume durable delivery state without API pending.

use crate::voice_delivery::api_binding::read_api_binding;
use crate::voice_delivery::client::VoiceV2Client;
use crate::voice_delivery::finalized_manifest::syndicate_to_validated_manifest;
use crate::voice_delivery::fs::DestRoot;
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::orchestrator::{
    DeliveryPhase, FinalizedVoiceDelivery, OrchestratorError,
};
use crate::voice_delivery::records::ledger_generation::{
    opaque_dir_has_canonical_generations, read_highest_valid_in_opaque_dir, LedgerGenerationError,
};
use crate::voice_delivery::state::LedgerState;
use std::path::Path;
use std::time::Duration;
use tracing::warn;

const CONTROL_DIR: &str = ".streamsync-control";
const LEDGERS_SEGMENT: &str = "ledgers";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LocalRecoverySweepOutcome {
    /// Longest server-required backoff observed from retryable client errors (if any).
    pub retry_after: Option<Duration>,
    pub terminal_failures: u32,
    pub quarantine_skips: u32,
    pub deliveries_attempted: u32,
}

impl LocalRecoverySweepOutcome {
    fn observe_orchestrator_error(&mut self, e: OrchestratorError) {
        match e {
            OrchestratorError::ClientRetryable(err) => {
                if let Some(wait) = err.retry_after() {
                    self.retry_after =
                        Some(self.retry_after.map_or(wait, |existing| existing.max(wait)));
                }
            }
            OrchestratorError::ClientTerminal(_) => {
                self.terminal_failures = self.terminal_failures.saturating_add(1);
            }
            other => {
                self.terminal_failures = self.terminal_failures.saturating_add(1);
                warn!("voice v2 local recovery failed: {other}");
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        if let Some(wait) = other.retry_after {
            self.retry_after = Some(self.retry_after.map_or(wait, |existing| existing.max(wait)));
        }
        self.terminal_failures = self
            .terminal_failures
            .saturating_add(other.terminal_failures);
        self.quarantine_skips = self.quarantine_skips.saturating_add(other.quarantine_skips);
        self.deliveries_attempted = self
            .deliveries_attempted
            .saturating_add(other.deliveries_attempted);
    }
}

pub fn sweep_local_recoveries<C: VoiceV2Client>(
    recording_parent: &Path,
    orch: &FinalizedVoiceDelivery,
    client: &C,
) -> LocalRecoverySweepOutcome {
    let mut outcome = LocalRecoverySweepOutcome::default();
    let Ok(root) = DestRoot::open(recording_parent) else {
        return outcome;
    };
    let Ok(control) = root.handle().open_child_dir(CONTROL_DIR) else {
        return outcome;
    };
    let Ok(ledgers) = control.open_child_dir(LEDGERS_SEGMENT) else {
        return outcome;
    };
    let Ok(opaque_dirs) = ledgers.list_child_names() else {
        return outcome;
    };
    for opaque in opaque_dirs {
        let Ok(ledger_dir) = ledgers.open_child_dir(&opaque) else {
            continue;
        };
        let record = match read_highest_valid_in_opaque_dir(&ledger_dir) {
            Ok(Some(rec)) => rec,
            Ok(None) => {
                if opaque_dir_has_canonical_generations(&ledger_dir).unwrap_or(false) {
                    outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
                    warn!(
                        "voice v2 local recovery quarantine {opaque}: no valid ledger generation"
                    );
                }
                continue;
            }
            Err(LedgerGenerationError::IdentityMismatch) => {
                outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
                warn!("voice v2 local recovery quarantine {opaque}: conflicting ledger identity");
                continue;
            }
            Err(e) => {
                outcome.terminal_failures = outcome.terminal_failures.saturating_add(1);
                warn!("voice v2 local recovery ledger scan failed {opaque}: {e}");
                continue;
            }
        };
        if record.state != LedgerState::Published {
            continue;
        }
        let Ok(identity) = DeliveryImmutableIdentity::from_ledger_record(&record) else {
            outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
            continue;
        };
        let Ok(binding) = read_api_binding(&ledger_dir, &identity) else {
            outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
            warn!("voice v2 local recovery quarantine {opaque}: api binding missing/invalid");
            continue;
        };
        let Ok(manifest) = syndicate_to_validated_manifest(&binding.manifest) else {
            outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
            continue;
        };
        if identity.assert_stem_manifest(&manifest).is_err() {
            outcome.quarantine_skips = outcome.quarantine_skips.saturating_add(1);
            warn!("voice v2 local recovery quarantine {opaque}: stem mismatch");
            continue;
        }
        outcome.deliveries_attempted = outcome.deliveries_attempted.saturating_add(1);
        let mut phase = DeliveryPhase::Waiting;
        if let Err(e) = orch.deliver_bound(
            client,
            &binding.manifest,
            &binding.sealed_at,
            manifest,
            identity,
            &mut phase,
        ) {
            outcome.observe_orchestrator_error(e);
        }
    }
    outcome
}

#[cfg(test)]
mod local_recovery_tests {
    use super::*;
    use crate::voice_delivery::fs::durability::NamespaceDurability;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::records::generation_filename;
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::session::DeliverySessionGuard;

    fn stem_manifest() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .unwrap()
    }

    fn stage_token(suffix: &str) -> String {
        format!(".streamsync-stage-{suffix}")
    }

    fn delete_staging_dirs(root: &std::path::Path) {
        if root
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with(".streamsync-stage-"))
        {
            let _ = std::fs::remove_dir_all(root);
            return;
        }
        if root.is_dir() {
            for entry in std::fs::read_dir(root).into_iter().flatten().flatten() {
                delete_staging_dirs(&entry.path());
            }
        }
    }

    #[test]
    fn startup_sweep_recovers_valid_lower_when_higher_generation_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let manifest = stem_manifest();
        let identity = DeliveryImmutableIdentity::new_bound(
            "local-sweep-lower-valid",
            manifest.digest(),
            &manifest,
            stage_token("01234567890123456789012345678901"),
            vec![],
            "published-final",
        )
        .unwrap();
        let guard = DeliverySessionGuard::begin(root, identity, manifest, true).unwrap();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        store.commit(&guard, LedgerState::Receiving).unwrap();
        store.commit(&guard, LedgerState::Sealed).unwrap();
        store.commit(&guard, LedgerState::PublishIntent).unwrap();
        let published = store
            .commit_published(&guard, NamespaceDurability::Proven)
            .unwrap();
        assert_eq!(published.state, LedgerState::Published);
        let ledger_dir = guard.ledger_dir().unwrap();
        let corrupt_name = generation_filename(published.generation + 50).unwrap();
        ledger_dir
            .create_new_file(&corrupt_name)
            .unwrap()
            .write_all_at(0, b"{ torn ledger")
            .unwrap();
        let recovered = read_highest_valid_in_opaque_dir(&ledger_dir)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.generation, published.generation);
        assert_eq!(recovered.state, LedgerState::Published);
    }

    #[test]
    fn published_receipt_local_sweep_retry_after_combined_for_worker_delay() {
        use crate::voice_delivery::client::{
            MockVoiceV2Client, PendingDelivery, VoiceV2ClientError,
        };
        use crate::voice_delivery::finalized_manifest::{
            compute_finalized_manifest_digest, parse_syndicate_finalized_manifest,
            syndicate_manifest_tests::minimal_wav_value,
        };
        use crate::voice_delivery::hash::hex_digest;
        use sha2::{Digest, Sha256};

        let tmp = tempfile::tempdir().unwrap();
        let header = crate::voice_delivery::wav::minimal_wav_header(0).unwrap();
        let sha = hex_digest(&Sha256::digest(&header));
        let mut raw = minimal_wav_value();
        raw["stems"][0]["sha256"] = serde_json::json!(sha);
        let syndicate = parse_syndicate_finalized_manifest(&raw).unwrap();
        let digest = compute_finalized_manifest_digest(&syndicate);
        let pending = PendingDelivery {
            session_id: syndicate.session_id.clone(),
            manifest_digest: digest,
            sealed_at: "2020-01-01T00:00:00.000Z".into(),
            manifest: syndicate,
        };
        let mock = MockVoiceV2Client::new();
        mock.set_stem_bytes(&pending.session_id, "user1", header);
        mock.set_pending(vec![pending.clone()]);
        mock.push_receipt_error(VoiceV2ClientError::RetryAfter(Duration::from_secs(17)));
        let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
            .unwrap()
            .with_device_id("dev");
        let mut phase = DeliveryPhase::Waiting;
        let _ = orch
            .deliver_pending(&mock, &pending, &mut phase)
            .unwrap_err();
        delete_staging_dirs(tmp.path());
        mock.push_receipt_error(VoiceV2ClientError::RetryAfter(Duration::from_secs(17)));
        let outcome = sweep_local_recoveries(tmp.path(), &orch, &mock);
        assert_eq!(outcome.retry_after, Some(Duration::from_secs(17)));
        assert!(outcome.deliveries_attempted >= 1);
        let worker_delay = crate::discord_voice_v2::combine_v2_worker_delay(outcome, None);
        assert_eq!(worker_delay, Duration::from_secs(17));
    }
}
