//! In-Sync recordings API (local finalization queue + status).

use crate::app_state::AppState;
use crate::config_types::LocalProcessingMode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn status_json(state: &AppState) -> Value {
    let cfg = state.discord_voice_config.read().await.clone();
    let runtime = state.in_sync_runtime.read().await.clone();
    let queue = state.in_sync_manual_queue.read().await.clone();
    let events = state.in_sync_progress_events.read().await.clone();
    json!({
        "localProcessingMode": cfg.local_processing_mode,
        "phase": runtime.phase,
        "activeSessionId": runtime.active_session_id,
        "manualQueue": queue,
        "progressEvents": events,
        "workshopPlaceholder": true,
    })
}

#[derive(Deserialize)]
pub struct ProcessNowBody {
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
    if paused {
        *state.in_sync_cancel.write().await = false;
    }
    Ok(())
}

pub async fn request_cancel(state: Arc<AppState>) -> Result<(), String> {
    *state.in_sync_cancel.write().await = true;
    Ok(())
}
