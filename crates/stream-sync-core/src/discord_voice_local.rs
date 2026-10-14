//! Local journal finalization worker (In-Sync processor poll loop).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::in_sync::{ensure_in_sync_native_opus, record_published_paths, set_active_orchestrator};
use crate::voice_delivery::local_finalization::PublishedSessionPaths;
use crate::voice_delivery::{
    HttpLocalFinalizationClient, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, LocalFinalizationProgressEvent, VoiceV2ClientError,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const LOCAL_POLL_LIMIT: u32 = 5;
const LOCAL_DEFAULT_WAIT: Duration = Duration::from_secs(3);

pub async fn local_poll_once(state: &AppState) -> Duration {
    if let Err(e) = ensure_in_sync_native_opus() {
        warn!("in-sync disabled: {e}");
        return LOCAL_DEFAULT_WAIT;
    }
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
    let controls = state.in_sync_worker.read().await.controls.clone();
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        Path::new(&parent),
        device_id,
        controls,
    ));
    set_active_orchestrator(state, Some(orch.clone())).await;
    let pending = match client.fetch_pending(LOCAL_POLL_LIMIT) {
        Ok(list) => list,
        Err(VoiceV2ClientError::RetryAfter(d)) => return d,
        Err(e) => {
            warn!("local finalization pending error: {e}");
            set_active_orchestrator(state, None).await;
            return LOCAL_DEFAULT_WAIT;
        }
    };
    if pending.is_empty() {
        update_local_runtime(state, LocalFinalizationPhase::Queued, None, None, None).await;
        set_active_orchestrator(state, None).await;
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
            None,
            None,
        )
        .await;
        let session_id = item.session_id.clone();
        match orch.process_pending(&client, &item) {
            Ok(()) => {
                let events = orch.drain_events();
                let (out_dir, report) = published_paths_from_events(&events, &session_id);
                if !events.is_empty() {
                    let mut sink = state.in_sync_progress_events.write().await;
                    sink.extend(events);
                }
                if let Some(dir) = out_dir.clone() {
                    record_published_paths(
                        state,
                        &session_id,
                        PublishedSessionPaths {
                            final_session_dir: PathBuf::from(dir),
                            recovery_report_path: report.as_ref().map(PathBuf::from),
                        },
                    )
                    .await;
                }
                update_local_runtime(
                    state,
                    LocalFinalizationPhase::Complete,
                    Some(session_id.clone()),
                    out_dir,
                    report,
                )
                .await;
                let mut q = state.in_sync_manual_queue.write().await;
                q.retain(|id| id != &session_id);
            }
            Err(e) => {
                warn!("local finalization failed for {session_id}: {e}");
                let events = orch.drain_events();
                if !events.is_empty() {
                    let mut sink = state.in_sync_progress_events.write().await;
                    sink.extend(events);
                }
                update_local_runtime(
                    state,
                    LocalFinalizationPhase::FailedRetryable,
                    Some(session_id),
                    None,
                    None,
                )
                .await;
            }
        }
    }
    set_active_orchestrator(state, None).await;
    LOCAL_DEFAULT_WAIT
}

fn published_paths_from_events(
    events: &[LocalFinalizationProgressEvent],
    session_id: &str,
) -> (Option<String>, Option<String>) {
    for event in events {
        if let LocalFinalizationProgressEvent::PublicationComplete {
            session_id: sid,
            final_session_dir,
            recovery_report_path,
        } = event
        {
            if sid == session_id {
                return (
                    Some(final_session_dir.clone()),
                    recovery_report_path.clone(),
                );
            }
        }
    }
    (None, None)
}

async fn update_local_runtime(
    state: &AppState,
    phase: LocalFinalizationPhase,
    session_id: Option<String>,
    published_output_dir: Option<String>,
    recovery_report_path: Option<String>,
) {
    let mut runtime = state.in_sync_runtime.write().await;
    runtime.phase = Some(phase);
    runtime.active_session_id = session_id;
    if published_output_dir.is_some() {
        runtime.published_output_dir = published_output_dir;
    }
    if recovery_report_path.is_some() {
        runtime.recovery_report_path = recovery_report_path;
    }
}
