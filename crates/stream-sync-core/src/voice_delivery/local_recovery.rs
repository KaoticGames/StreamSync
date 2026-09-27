//! Scan local control-dir ledgers and resume durable delivery state without API pending.

use crate::voice_delivery::api_binding::read_api_binding;
use crate::voice_delivery::client::VoiceV2Client;
use crate::voice_delivery::finalized_manifest::syndicate_to_validated_manifest;
use crate::voice_delivery::fs::DestRoot;
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::orchestrator::{
    DeliveryPhase, FinalizedVoiceDelivery, OrchestratorError,
};
use crate::voice_delivery::records::ledger_generation::{parse_ledger_bytes, LedgerGeneration};
use crate::voice_delivery::records::{parse_generation_filename, GEN_PREFIX};
use std::path::Path;
use tracing::warn;

const CONTROL_DIR: &str = ".streamsync-control";
const LEDGERS_SEGMENT: &str = "ledgers";

pub fn sweep_local_recoveries<C: VoiceV2Client>(
    recording_parent: &Path,
    orch: &FinalizedVoiceDelivery,
    client: &C,
) {
    let Ok(root) = DestRoot::open(recording_parent) else {
        return;
    };
    let Ok(control) = root.handle().open_child_dir(CONTROL_DIR) else {
        return;
    };
    let Ok(ledgers) = control.open_child_dir(LEDGERS_SEGMENT) else {
        return;
    };
    let Ok(opaque_dirs) = ledgers.list_child_names() else {
        return;
    };
    for opaque in opaque_dirs {
        let Ok(ledger_dir) = ledgers.open_child_dir(&opaque) else {
            continue;
        };
        let Some(record) = highest_valid_ledger_scan(&ledger_dir) else {
            continue;
        };
        let Ok(identity) = DeliveryImmutableIdentity::from_ledger_record(&record) else {
            continue;
        };
        let Ok(binding) = read_api_binding(&ledger_dir, &identity) else {
            continue;
        };
        let Ok(manifest) = syndicate_to_validated_manifest(&binding.manifest) else {
            continue;
        };
        if identity.assert_stem_manifest(&manifest).is_err() {
            warn!("voice v2 local recovery quarantine {opaque}: stem mismatch");
            continue;
        }
        let mut phase = DeliveryPhase::Waiting;
        if let Err(e) = orch.deliver_bound(
            client,
            &binding.manifest,
            &binding.sealed_at,
            manifest,
            identity,
            &mut phase,
        ) {
            match e {
                OrchestratorError::ClientRetryable(_) => {}
                other => warn!("voice v2 local recovery failed {opaque}: {other}"),
            }
        }
    }
}

fn highest_valid_ledger_scan(
    ledger_dir: &crate::voice_delivery::fs::DirHandle,
) -> Option<LedgerGeneration> {
    let names = ledger_dir.list_child_names().ok()?;
    let mut best: Option<LedgerGeneration> = None;
    for name in names {
        if !name.starts_with(GEN_PREFIX) {
            continue;
        }
        let bytes = ledger_dir.read_file_all(&name).ok()?;
        let rec = parse_ledger_bytes(&bytes).ok()?;
        if parse_generation_filename(&name) != Some(rec.generation) {
            continue;
        }
        if best.as_ref().is_none_or(|b| rec.generation > b.generation) {
            best = Some(rec);
        }
    }
    best
}
