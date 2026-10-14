//! In-Sync recordings API (local finalization queue + status).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::voice_delivery::local_finalization::{
    assert_local_opus_available, LocalFinalizationControls, LocalFinalizationOrchestrator,
    PublishedSessionPaths,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct InSyncWorkerState {
    pub controls: LocalFinalizationControls,
    pub active: Option<Arc<LocalFinalizationOrchestrator>>,
    pub published_by_session: HashMap<String, PublishedSessionPaths>,
}

pub async fn worker_state(state: &AppState) -> InSyncWorkerState {
    state.in_sync_worker.read().await.clone()
}

pub async fn set_active_orchestrator(
    state: &AppState,
    orch: Option<Arc<LocalFinalizationOrchestrator>>,
) {
    let mut slot = state.in_sync_worker.write().await;
    slot.active = orch;
}

pub async fn record_published_paths(
    state: &AppState,
    session_id: &str,
    paths: PublishedSessionPaths,
) {
    let mut slot = state.in_sync_worker.write().await;
    slot.published_by_session
        .insert(session_id.to_string(), paths);
}

/// Central cancellation entry for entitlement teardown and the HTTP cancel route.
pub async fn cancel_in_sync_work(state: Arc<AppState>) {
    let slot = state.in_sync_worker.read().await;
    slot.controls.cancel.cancel();
    if let Some(orch) = slot.active.as_ref() {
        orch.cancellation().cancel();
    }
}

pub fn ensure_in_sync_native_opus() -> Result<(), String> {
    assert_local_opus_available().map_err(|_| {
        "local Opus decoder unavailable: build with feature local-opus-native (static libopus on Windows via local-opus-vendored)".into()
    })
}

pub async fn status_json(state: &AppState) -> Value {
    let cfg = state.discord_voice_config.read().await.clone();
    let runtime = state.in_sync_runtime.read().await.clone();
    let queue = state.in_sync_manual_queue.read().await.clone();
    let events = state.in_sync_progress_events.read().await.clone();
    let published = state
        .in_sync_worker
        .read()
        .await
        .published_by_session
        .clone();
    let published_ids: Vec<String> = published.keys().cloned().collect();
    json!({
        "localProcessingMode": cfg.local_processing_mode,
        "phase": runtime.phase,
        "activeSessionId": runtime.active_session_id,
        "manualQueue": queue,
        "progressEvents": events,
        "publishedSessions": published_ids,
        "publishedOutputDir": runtime.published_output_dir,
        "recoveryReportPath": runtime.recovery_report_path,
    })
}

#[derive(Deserialize)]
pub struct ProcessNowBody {
    #[serde(alias = "sessionId")]
    pub session_id: String,
}

pub async fn enqueue_process_now(state: Arc<AppState>, session_id: &str) -> Result<(), String> {
    let id = session_id.trim();
    if id.is_empty() {
        return Err("session_id required".into());
    }
    let mut q = state.in_sync_manual_queue.write().await;
    if !q.iter().any(|s| s == id) {
        q.push(id.to_string());
    }
    Ok(())
}

pub async fn set_processing_mode(
    state: Arc<AppState>,
    mode: LocalProcessingMode,
) -> Result<(), String> {
    let mut cfg = state.discord_voice_config.write().await;
    cfg.local_processing_mode = mode;
    state
        .save_discord_voice_config()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn set_paused(state: Arc<AppState>, paused: bool) -> Result<(), String> {
    let slot = state.in_sync_worker.read().await;
    slot.controls
        .paused
        .store(paused, std::sync::atomic::Ordering::SeqCst);
    if let Some(orch) = slot.active.as_ref() {
        orch.set_paused(paused);
    }
    Ok(())
}

pub async fn request_cancel(state: Arc<AppState>) -> Result<(), String> {
    cancel_in_sync_work(state).await;
    Ok(())
}
