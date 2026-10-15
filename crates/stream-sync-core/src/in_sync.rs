//! In-Sync recordings API (local finalization queue + status).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::voice_delivery::local_finalization::{
    assert_local_opus_available, LocalFinalizationControls, LocalFinalizationOrchestrator,
    PublishedSessionPaths,
};
use crate::voice_delivery::{LocalPendingDelivery, VoiceV2ClientError};
use serde::Deserialize;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

const IN_SYNC_DISCOVERY_MESSAGE_MAX: usize = 240;

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InSyncDiscoverySnapshot {
    /// `ok` | `blocked` | `error`
    pub status: String,
    pub message: Option<String>,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InSyncPendingSessionSummary {
    pub session_id: String,
    pub guild_id: String,
    pub channel_id: String,
    pub manifest_digest: String,
    pub sealed_at: String,
    pub segment_count: u32,
    pub target_duration_secs: Option<f64>,
}

#[derive(Clone, Default)]
pub struct InSyncPendingState {
    pub sessions: Vec<InSyncPendingSessionSummary>,
    pub discovery: InSyncDiscoverySnapshot,
}

pub fn pending_summary_from_delivery(item: &LocalPendingDelivery) -> InSyncPendingSessionSummary {
    let duration_secs = (item.manifest.target_sample_count_48k as f64) / 48_000.0;
    InSyncPendingSessionSummary {
        session_id: item.session_id.clone(),
        guild_id: item.manifest.guild_id.clone(),
        channel_id: item.manifest.channel_id.clone(),
        manifest_digest: item.manifest_digest.clone(),
        sealed_at: item.sealed_at.clone(),
        segment_count: item.manifest.segments.len() as u32,
        target_duration_secs: Some(duration_secs),
    }
}

fn bound_discovery_message(message: impl AsRef<str>) -> String {
    let s = message.as_ref();
    if s.len() <= IN_SYNC_DISCOVERY_MESSAGE_MAX {
        return s.to_string();
    }
    format!(
        "{}…",
        s.chars()
            .take(IN_SYNC_DISCOVERY_MESSAGE_MAX)
            .collect::<String>()
    )
}

pub fn bound_client_error(err: &VoiceV2ClientError) -> String {
    bound_discovery_message(err.to_string())
}

pub async fn set_in_sync_discovery_blocked(state: &AppState, message: impl Into<String>) {
    let mut slot = state.in_sync_pending.write().await;
    slot.sessions.clear();
    slot.discovery = InSyncDiscoverySnapshot {
        status: "blocked".into(),
        message: Some(bound_discovery_message(message.into())),
    };
}

pub async fn set_in_sync_pending_fetch_error(state: &AppState, message: impl Into<String>) {
    let mut slot = state.in_sync_pending.write().await;
    slot.sessions.clear();
    slot.discovery = InSyncDiscoverySnapshot {
        status: "error".into(),
        message: Some(bound_discovery_message(message.into())),
    };
}

pub async fn set_in_sync_pending_deliveries(state: &AppState, deliveries: &[LocalPendingDelivery]) {
    let sessions = deliveries
        .iter()
        .map(pending_summary_from_delivery)
        .collect();
    let mut slot = state.in_sync_pending.write().await;
    slot.sessions = sessions;
    slot.discovery = InSyncDiscoverySnapshot {
        status: "ok".into(),
        message: None,
    };
}

pub async fn in_sync_session_is_pending(state: &AppState, session_id: &str) -> bool {
    let slot = state.in_sync_pending.read().await;
    slot.sessions.iter().any(|s| s.session_id == session_id)
}

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
    let pending = state.in_sync_pending.read().await.clone();
    json!({
        "localProcessingMode": cfg.local_processing_mode,
        "phase": runtime.phase,
        "activeSessionId": runtime.active_session_id,
        "manualQueue": queue,
        "pendingSessions": pending.sessions,
        "discovery": pending.discovery,
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
    if !in_sync_session_is_pending(&state, id).await {
        return Err("session is not in the cloud pending list; refresh and try again".into());
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
