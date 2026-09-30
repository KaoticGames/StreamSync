//! Persistent update-check cadence and dismissal state.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use stream_sync_core::write_json;

pub const UPDATE_STATE_FILE: &str = "update-state.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LastResult {
    Available,
    Current,
    Error,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UpdateState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_check_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dismissed_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_result: Option<LastResult>,
}

impl UpdateState {
    pub fn path_in(user_data: &Path) -> PathBuf {
        user_data.join(UPDATE_STATE_FILE)
    }

    pub fn load(user_data: &Path) -> Self {
        let path = Self::path_in(user_data);
        if !path.is_file() {
            return Self::default();
        }
        match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, user_data: &Path) -> Result<(), String> {
        let path = Self::path_in(user_data);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        write_json(&path, self).map_err(|e| e.to_string())
    }
}
