//! In-Sync recordings API (local finalization queue + status).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use crate::voice_delivery::local_finalization::{
    assert_local_opus_available, LocalFinalizationControls, LocalFinalizationOrchestrator,
    LocalPendingItem,
};
use crate::voice_delivery::VoiceV2ClientError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

const IN_SYNC_DISCOVERY_MESSAGE_MAX: usize = 240;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InSyncSupervisorStatus {
    Starting,
    Polling,
    Ready,
    Blocked,
    Error,
    Stopped,
}

impl InSyncSupervisorStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Polling => "polling",
            Self::Ready => "ready",
            Self::Blocked => "blocked",
            Self::Error => "error",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InSyncSupervisorSnapshot {
    pub status: String,
    pub last_attempt_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub pending_count: usize,
    pub last_error: Option<String>,
}

impl Default for InSyncSupervisorStatus {
    fn default() -> Self {
        Self::Starting
    }
}

impl InSyncSupervisorSnapshot {
    pub fn from_status(status: InSyncSupervisorStatus) -> Self {
        Self {
            status: status.as_str().into(),
            ..Default::default()
        }
    }
}

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
    pub manifest_digest: String,
    pub sealed_at_ms: i64,
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub segment_count: Option<u32>,
    pub target_duration_secs: Option<f64>,
}

#[derive(Clone, Default)]
pub struct InSyncPendingState {
    pub sessions: Vec<InSyncPendingSessionSummary>,
    pub discovery: InSyncDiscoverySnapshot,
}

pub fn pending_summary_from_item(item: &LocalPendingItem) -> InSyncPendingSessionSummary {
    InSyncPendingSessionSummary {
        session_id: item.session_id.clone(),
        guild_id: None,
        channel_id: None,
        manifest_digest: item.manifest_digest.clone(),
        sealed_at_ms: item.sealed_at_ms,
        segment_count: None,
        target_duration_secs: None,
    }
}

pub fn bound_discovery_message(message: impl AsRef<str>) -> String {
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
    update_in_sync_supervisor_snapshot(state, InSyncSupervisorStatus::Blocked, None, None).await;
}

pub async fn set_in_sync_pending_fetch_error(state: &AppState, message: impl Into<String>) {
    let mut slot = state.in_sync_pending.write().await;
    slot.sessions.clear();
    slot.discovery = InSyncDiscoverySnapshot {
        status: "error".into(),
        message: Some(bound_discovery_message(message.into())),
    };
}

pub async fn set_in_sync_pending_deliveries(state: &AppState, deliveries: &[LocalPendingItem]) {
    let sessions = deliveries.iter().map(pending_summary_from_item).collect();
    let mut slot = state.in_sync_pending.write().await;
    slot.sessions = sessions;
    slot.discovery = InSyncDiscoverySnapshot {
        status: "ok".into(),
        message: None,
    };
}

pub async fn update_in_sync_supervisor_snapshot(
    state: &AppState,
    status: InSyncSupervisorStatus,
    pending_count: Option<usize>,
    error: Option<String>,
) {
    let now = chrono::Utc::now().timestamp_millis();
    let mut snap = state.in_sync_supervisor.write().await;
    snap.status = status.as_str().into();
    snap.last_attempt_at_ms = Some(now);
    if error.is_none() {
        snap.last_success_at_ms = Some(now);
    }
    if let Some(n) = pending_count {
        snap.pending_count = n;
    }
    snap.last_error = error.map(bound_discovery_message);
}

pub async fn in_sync_session_is_pending(state: &AppState, session_id: &str) -> bool {
    let slot = state.in_sync_pending.read().await;
    slot.sessions.iter().any(|s| s.session_id == session_id)
}

#[derive(Clone, Default)]
pub struct InSyncWorkerState {
    pub controls: LocalFinalizationControls,
    pub active: Option<Arc<LocalFinalizationOrchestrator>>,
    pub published_by_session:
        HashMap<String, crate::voice_delivery::local_finalization::PublishedSessionPaths>,
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
    paths: crate::voice_delivery::local_finalization::PublishedSessionPaths,
) {
    let mut slot = state.in_sync_worker.write().await;
    slot.published_by_session
        .insert(session_id.to_string(), paths);
}

/// Signal cancellation for the active in-sync finalization job (shared worker controls + orchestrator).
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
    let supervisor = state.in_sync_supervisor.read().await.clone();
    let supervisor_status = if supervisor.status.is_empty() {
        InSyncSupervisorStatus::Starting.as_str()
    } else {
        supervisor.status.as_str()
    };
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
        "supervisorStatus": supervisor_status,
        "supervisorLastAttemptAtMs": supervisor.last_attempt_at_ms,
        "supervisorLastSuccessAtMs": supervisor.last_success_at_ms,
        "supervisorPendingCount": supervisor.pending_count,
        "supervisorLastError": supervisor.last_error,
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
    {
        let worker = state.in_sync_worker.write().await;
        worker.controls.cancel.reset();
    }
    let mut q = state.in_sync_manual_queue.write().await;
    if !q.iter().any(|s| s == id) {
        q.push(id.to_string());
    }
    crate::in_sync_supervisor::wake_in_sync_supervisor(&state).await;
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
    if !paused {
        crate::in_sync_supervisor::wake_in_sync_supervisor(&state).await;
    }
    Ok(())
}

pub async fn request_cancel(state: Arc<AppState>) -> Result<(), String> {
    let active_session = state.in_sync_runtime.read().await.active_session_id.clone();
    cancel_in_sync_work(state.clone()).await;
    {
        let mut q = state.in_sync_manual_queue.write().await;
        if let Some(id) = active_session {
            q.retain(|s| s != &id);
        } else {
            q.clear();
        }
    }
    crate::in_sync_supervisor::wake_in_sync_supervisor(&state).await;
    Ok(())
}

pub fn new_in_sync_wake() -> Arc<Notify> {
    Arc::new(Notify::new())
}
