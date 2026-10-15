//! Independent In-Sync supervisor (discovery + processing), not tied to Discord ingest.

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::in_sync::{
    bound_client_error, ensure_in_sync_native_opus, record_published_paths,
    set_active_orchestrator, set_in_sync_discovery_blocked, set_in_sync_pending_deliveries,
    set_in_sync_pending_fetch_error, update_in_sync_supervisor_snapshot, InSyncSupervisorStatus,
};
use crate::voice_delivery::local_finalization::PublishedSessionPaths;
use crate::voice_delivery::local_finalization::{
    LocalFinalizationOrchestrator, LocalFinalizationPhase, LocalFinalizationProgressEvent,
    LocalOrchestratorError, LocalPendingItem,
};
use crate::voice_delivery::{
    HttpLocalFinalizationClient, LocalFinalizationClient, VoiceV2ClientError,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn};

pub const LOCAL_POLL_LIMIT: u32 = 5;
const LOCAL_DEFAULT_WAIT: Duration = Duration::from_secs(3);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const MIN_BACKOFF: Duration = Duration::from_secs(2);

pub fn ensure_in_sync_supervisor(state: Arc<AppState>) {
    let wake = state.in_sync_wake.clone();
    tokio::spawn(async move {
        info!("in-sync supervisor: starting");
        update_in_sync_supervisor_snapshot(&state, InSyncSupervisorStatus::Starting, None, None)
            .await;
        let mut backoff = MIN_BACKOFF;
        loop {
            let cycle = tokio::task::spawn_blocking({
                let state = state.clone();
                move || blocking_supervisor_cycle(&state)
            })
            .await;

            let wait = match cycle {
                Ok(Ok(d)) => {
                    backoff = MIN_BACKOFF;
                    d
                }
                Ok(Err(e)) => {
                    warn!("in-sync supervisor cycle error: {e}");
                    update_in_sync_supervisor_snapshot(
                        &state,
                        InSyncSupervisorStatus::Error,
                        None,
                        Some(e),
                    )
                    .await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    backoff
                }
                Err(join_err) => {
                    warn!("in-sync supervisor task join failed: {join_err}");
                    update_in_sync_supervisor_snapshot(
                        &state,
                        InSyncSupervisorStatus::Error,
                        None,
                        Some("supervisor worker panicked".into()),
                    )
                    .await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    backoff
                }
            };

            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = wake.notified() => {
                    info!("in-sync supervisor: wake");
                }
            }
        }
    });
}

fn blocking_supervisor_cycle(state: &AppState) -> Result<Duration, String> {
    if let Err(e) = ensure_in_sync_native_opus() {
        warn!("in-sync supervisor blocked: {e}");
        set_in_sync_discovery_blocked_blocking(state, &e);
        update_in_sync_supervisor_snapshot_sync(
            state,
            InSyncSupervisorStatus::Blocked,
            None,
            Some(e),
        );
        return Ok(LOCAL_DEFAULT_WAIT);
    }
    let cfg = state.discord_voice_config.blocking_read().clone();
    let key = cfg
        .host_token
        .clone()
        .filter(|v| v.starts_with("sdk_"))
        .unwrap_or_default();
    if key.is_empty() {
        let msg = "host SDK token (sdk_…) not configured";
        set_in_sync_discovery_blocked_blocking(state, msg);
        update_in_sync_supervisor_snapshot_sync(
            state,
            InSyncSupervisorStatus::Blocked,
            None,
            Some(msg.into()),
        );
        return Ok(LOCAL_DEFAULT_WAIT);
    }
    let parent = cfg
        .recording_parent
        .clone()
        .filter(|v| !v.trim().is_empty());
    if parent.is_none() {
        let msg = "recording parent folder not configured";
        set_in_sync_discovery_blocked_blocking(state, msg);
        update_in_sync_supervisor_snapshot_sync(
            state,
            InSyncSupervisorStatus::Blocked,
            None,
            Some(msg.into()),
        );
        return Ok(LOCAL_DEFAULT_WAIT);
    }
    let parent = parent.unwrap();
    let device_id = cfg.device_id.clone();
    let base = crate::syndicate_connection::api_base();
    let client = HttpLocalFinalizationClient::new(base, key);

    update_in_sync_supervisor_snapshot_sync(state, InSyncSupervisorStatus::Polling, None, None);

    let pending = match client.fetch_pending(LOCAL_POLL_LIMIT) {
        Ok(list) => list,
        Err(VoiceV2ClientError::RetryAfter(d)) => {
            update_in_sync_supervisor_snapshot_sync(
                state,
                InSyncSupervisorStatus::Polling,
                None,
                None,
            );
            return Ok(d);
        }
        Err(e) => {
            let msg = bound_client_error(&e);
            set_in_sync_pending_fetch_error_blocking(state, &msg);
            update_in_sync_supervisor_snapshot_sync(
                state,
                InSyncSupervisorStatus::Error,
                None,
                Some(msg),
            );
            return Ok(LOCAL_DEFAULT_WAIT);
        }
    };

    set_in_sync_pending_deliveries_blocking(state, &pending);
    let status = if pending.is_empty() {
        InSyncSupervisorStatus::Ready
    } else {
        InSyncSupervisorStatus::Ready
    };
    update_in_sync_supervisor_snapshot_sync(state, status, Some(pending.len()), None);

    if pending.is_empty() {
        update_local_runtime_blocking(state, LocalFinalizationPhase::Queued, None, None, None);
        set_active_orchestrator_blocking(state, None);
        return Ok(LOCAL_DEFAULT_WAIT);
    }

    let manual_queue = if cfg.local_processing_mode == LocalProcessingMode::Manual {
        Some(state.in_sync_manual_queue.blocking_read().clone())
    } else {
        None
    };
    if manual_queue.is_some() {
        let allowed_any = pending
            .iter()
            .any(|item| manual_queue.as_ref().unwrap().contains(&item.session_id));
        if !allowed_any {
            set_active_orchestrator_blocking(state, None);
            return Ok(LOCAL_DEFAULT_WAIT);
        }
    }

    let controls = state.in_sync_worker.blocking_read().controls.clone();
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        Path::new(&parent),
        device_id,
        controls,
    ));
    set_active_orchestrator_blocking(state, Some(orch.clone()));

    let deferred = state.in_sync_storage_deferred.blocking_read().clone();
    for item in pending {
        if deferred.contains(&item.session_id) {
            continue;
        }
        if let Some(manual) = manual_queue.as_ref() {
            if !manual.contains(&item.session_id) {
                continue;
            }
        }
        update_local_runtime_blocking(
            state,
            LocalFinalizationPhase::Claiming,
            Some(item.session_id.clone()),
            None,
            None,
        );
        let session_id = item.session_id.clone();
        match orch.process_pending(&client, &item) {
            Ok(()) => {
                info!("in-sync supervisor: session {session_id} complete");
                state
                    .in_sync_storage_deferred
                    .blocking_write()
                    .remove(&session_id);
                clear_runtime_storage_blocked_blocking(state);
                let events = orch.drain_events();
                let (out_dir, report) = published_paths_from_events(&events, &session_id);
                if !events.is_empty() {
                    let mut sink = state.in_sync_progress_events.blocking_write();
                    sink.extend(events);
                }
                if let Some(dir) = out_dir.clone() {
                    record_published_paths_blocking(
                        state,
                        &session_id,
                        PublishedSessionPaths {
                            final_session_dir: dir,
                            recovery_report_path: report.clone(),
                        },
                    );
                }
                update_local_runtime_blocking(
                    state,
                    LocalFinalizationPhase::Complete,
                    Some(session_id.clone()),
                    out_dir.map(|p| p.to_string_lossy().to_string()),
                    report.map(|p| p.to_string_lossy().to_string()),
                );
                let mut q = state.in_sync_manual_queue.blocking_write();
                q.retain(|id| id != &session_id);
            }
            Err(LocalOrchestratorError::Paused) => {
                info!("in-sync supervisor: session {session_id} paused at checkpoint");
                update_local_runtime_blocking(
                    state,
                    LocalFinalizationPhase::Paused,
                    Some(session_id),
                    None,
                    None,
                );
                break;
            }
            Err(LocalOrchestratorError::Cancelled) => {
                info!("in-sync supervisor: session {session_id} cancelled");
                let mut q = state.in_sync_manual_queue.blocking_write();
                q.retain(|id| id != &session_id);
                update_local_runtime_blocking(
                    state,
                    LocalFinalizationPhase::Cancelled,
                    Some(session_id),
                    None,
                    None,
                );
                break;
            }
            Err(LocalOrchestratorError::StoragePreflight(pref)) => {
                warn!("in-sync supervisor: session {session_id} storage blocked: {pref}");
                let events = orch.drain_events();
                if !events.is_empty() {
                    let mut sink = state.in_sync_progress_events.blocking_write();
                    sink.extend(events);
                }
                let mut q = state.in_sync_manual_queue.blocking_write();
                q.retain(|id| id != &session_id);
                state
                    .in_sync_storage_deferred
                    .blocking_write()
                    .insert(session_id.clone());
                update_local_runtime_storage_blocked_blocking(
                    state,
                    &session_id,
                    LocalFinalizationPhase::FailedBlocked,
                    pref,
                );
                break;
            }
            Err(e) => {
                warn!("in-sync supervisor: session {session_id} failed: {e}");
                let events = orch.drain_events();
                if !events.is_empty() {
                    let mut sink = state.in_sync_progress_events.blocking_write();
                    sink.extend(events);
                }
                update_local_runtime_blocking(
                    state,
                    LocalFinalizationPhase::FailedRetryable,
                    Some(session_id),
                    None,
                    None,
                );
            }
        }
    }
    set_active_orchestrator_blocking(state, None);
    Ok(LOCAL_DEFAULT_WAIT)
}

fn published_paths_from_events(
    events: &[LocalFinalizationProgressEvent],
    session_id: &str,
) -> (Option<std::path::PathBuf>, Option<std::path::PathBuf>) {
    for event in events {
        if let LocalFinalizationProgressEvent::PublicationComplete {
            session_id: sid,
            final_session_dir,
            recovery_report_path,
        } = event
        {
            if sid == session_id {
                return (
                    Some(std::path::PathBuf::from(final_session_dir)),
                    recovery_report_path
                        .as_ref()
                        .map(|p| std::path::PathBuf::from(p)),
                );
            }
        }
    }
    (None, None)
}

fn update_local_runtime_blocking(
    state: &AppState,
    phase: LocalFinalizationPhase,
    session_id: Option<String>,
    published_output_dir: Option<String>,
    recovery_report_path: Option<String>,
) {
    let mut runtime = state.in_sync_runtime.blocking_write();
    runtime.phase = Some(phase);
    runtime.active_session_id = session_id;
    if published_output_dir.is_some() {
        runtime.published_output_dir = published_output_dir;
    }
    if recovery_report_path.is_some() {
        runtime.recovery_report_path = recovery_report_path;
    }
}

fn clear_runtime_storage_blocked_blocking(state: &AppState) {
    let mut runtime = state.in_sync_runtime.blocking_write();
    runtime.storage_blocked = None;
    runtime.storage_blocked_session_id = None;
}

fn update_local_runtime_storage_blocked_blocking(
    state: &AppState,
    session_id: &str,
    phase: LocalFinalizationPhase,
    failure: crate::voice_delivery::StoragePreflightFailure,
) {
    let mut runtime = state.in_sync_runtime.blocking_write();
    runtime.phase = Some(phase);
    runtime.active_session_id = Some(session_id.to_string());
    runtime.storage_blocked = Some(failure);
    runtime.storage_blocked_session_id = Some(session_id.to_string());
}

fn set_active_orchestrator_blocking(
    state: &AppState,
    orch: Option<Arc<LocalFinalizationOrchestrator>>,
) {
    let mut slot = state.in_sync_worker.blocking_write();
    slot.active = orch;
}

fn set_in_sync_pending_deliveries_blocking(state: &AppState, deliveries: &[LocalPendingItem]) {
    let sessions = deliveries
        .iter()
        .map(crate::in_sync::pending_summary_from_item)
        .collect();
    let mut slot = state.in_sync_pending.blocking_write();
    slot.sessions = sessions;
    slot.discovery = crate::in_sync::InSyncDiscoverySnapshot {
        status: "ok".into(),
        message: None,
    };
}

fn set_in_sync_discovery_blocked_blocking(state: &AppState, message: &str) {
    let mut slot = state.in_sync_pending.blocking_write();
    slot.sessions.clear();
    slot.discovery = crate::in_sync::InSyncDiscoverySnapshot {
        status: "blocked".into(),
        message: Some(crate::in_sync::bound_discovery_message(message)),
    };
}

fn set_in_sync_pending_fetch_error_blocking(state: &AppState, message: &str) {
    let mut slot = state.in_sync_pending.blocking_write();
    slot.sessions.clear();
    slot.discovery = crate::in_sync::InSyncDiscoverySnapshot {
        status: "error".into(),
        message: Some(crate::in_sync::bound_discovery_message(message)),
    };
}

fn record_published_paths_blocking(
    state: &AppState,
    session_id: &str,
    paths: PublishedSessionPaths,
) {
    let mut slot = state.in_sync_worker.blocking_write();
    slot.published_by_session
        .insert(session_id.to_string(), paths);
}

fn update_in_sync_supervisor_snapshot_sync(
    state: &AppState,
    status: InSyncSupervisorStatus,
    pending_count: Option<usize>,
    error: Option<String>,
) {
    let now = chrono::Utc::now().timestamp_millis();
    let mut snap = state.in_sync_supervisor.blocking_write();
    snap.status = status.as_str().into();
    snap.last_attempt_at_ms = Some(now);
    if error.is_none() {
        snap.last_success_at_ms = Some(now);
    }
    if let Some(n) = pending_count {
        snap.pending_count = n;
    }
    snap.last_error = error.map(crate::in_sync::bound_discovery_message);
}

pub async fn wake_in_sync_supervisor(state: &AppState) {
    state.in_sync_wake.notify_one();
}
