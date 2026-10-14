//! Local journal finalization worker (In-Sync processor poll loop).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::voice_delivery::{
    HttpLocalFinalizationClient, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, VoiceV2ClientError,
};
use std::path::Path;
use std::time::Duration;
use tracing::warn;

pub const LOCAL_POLL_LIMIT: u32 = 5;
const LOCAL_DEFAULT_WAIT: Duration = Duration::from_secs(3);

pub async fn local_poll_once(state: &AppState) -> Duration {
    let cfg = state.discord_voice_config.read().await.clone();
    if cfg.local_processing_mode == LocalProcessingMode::Manual {
        let manual = state.in_sync_manual_queue.read().await.clone();
        if manual.is_empty() {
            return LOCAL_DEFAULT_WAIT;
        }
    }
    let key = cfg
        .host_token
        .clone()
        .filter(|v| v.starts_with("sdk_"))
        .unwrap_or_default();
    if key.is_empty() {
        return LOCAL_DEFAULT_WAIT;
    }
    let parent = cfg
        .recording_parent
        .clone()
        .filter(|v| !v.trim().is_empty());
    if parent.is_none() {
        return LOCAL_DEFAULT_WAIT;
    }
    let parent = parent.unwrap();
    let device_id = cfg.device_id.clone();
    let base = crate::syndicate_connection::api_base();
    let client = HttpLocalFinalizationClient::new(base, key);
    let orch = LocalFinalizationOrchestrator::open(Path::new(&parent), device_id);
    if *state.in_sync_cancel.read().await {
        orch.cancellation().cancel();
    }
    let pending = match client.fetch_pending(LOCAL_POLL_LIMIT) {
        Ok(list) => list,
        Err(VoiceV2ClientError::RetryAfter(d)) => return d,
        Err(e) => {
            warn!("local finalization pending error: {e}");
            return LOCAL_DEFAULT_WAIT;
        }
    };
    if pending.is_empty() {
        update_local_runtime(state, LocalFinalizationPhase::Queued, None).await;
        return LOCAL_DEFAULT_WAIT;
    }
    for item in pending {
        if cfg.local_processing_mode == LocalProcessingMode::Manual {
            let allowed = state
                .in_sync_manual_queue
                .read()
                .await
                .contains(&item.session_id);
            if !allowed {
                continue;
            }
        }
        update_local_runtime(
            state,
            LocalFinalizationPhase::Claiming,
            Some(item.session_id.clone()),
        )
        .await;
        let session_id = item.session_id.clone();
        match orch.process_pending(&client, &item) {
            Ok(()) => {
                update_local_runtime(
                    state,
                    LocalFinalizationPhase::Complete,
                    Some(session_id.clone()),
                )
                .await;
                let mut q = state.in_sync_manual_queue.write().await;
                q.retain(|id| id != &session_id);
            }
            Err(e) => {
                warn!("local finalization failed for {session_id}: {e}");
                update_local_runtime(
                    state,
                    LocalFinalizationPhase::FailedRetryable,
                    Some(session_id),
                )
                .await;
            }
        }
        let events = orch.drain_events();
        if !events.is_empty() {
            let mut sink = state.in_sync_progress_events.write().await;
            sink.extend(events);
        }
    }
    LOCAL_DEFAULT_WAIT
}

async fn update_local_runtime(
    state: &AppState,
    phase: LocalFinalizationPhase,
    session_id: Option<String>,
) {
    let mut runtime = state.in_sync_runtime.write().await;
    runtime.phase = Some(phase);
    runtime.active_session_id = session_id;
}
