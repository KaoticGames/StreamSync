//! Shared update decision engine for launch and manual checks.

use crate::update_state::{LastResult, UpdateState};
use crate::updater::{
    check_for_updates_url, is_newer_version, validate_update_manifest, UpdateManifest,
};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tauri::{AppHandle, Emitter};
use tauri_plugin_updater::UpdaterExt;

const SUCCESS_CADENCE: Duration = Duration::hours(24);
const FAILURE_BACKOFF: Duration = Duration::hours(1);

static CHECK_GENERATION: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckMode {
    Background,
    Forced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAvailablePayload {
    pub version: String,
    pub notes: String,
    pub release_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ManualCheckResult {
    UpToDate,
    UpdateAvailable(UpdateAvailablePayload),
    Failed { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackgroundCheckResult {
    SilentCurrent,
    SilentOffline,
    SkippedCadence,
    UpdateAvailable(UpdateAvailablePayload),
    SecurityError,
}

pub struct UpdateService {
    user_data: std::path::PathBuf,
    state: Mutex<UpdateState>,
    in_flight: Mutex<Option<u64>>,
}

impl UpdateService {
    pub fn new(user_data: std::path::PathBuf) -> Self {
        Self {
            user_data: user_data.clone(),
            state: Mutex::new(UpdateState::load(&user_data)),
            in_flight: Mutex::new(None),
        }
    }

    pub fn current_state(&self) -> UpdateState {
        self.state.lock().map(|s| s.clone()).unwrap_or_default()
    }

    pub fn dismiss_version(&self, version: &str) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|e| e.to_string())?;
        state.dismissed_version = Some(version.trim().to_string());
        state.save(&self.user_data).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn should_run_launch_check(
        now: DateTime<Utc>,
        last_attempt: Option<DateTime<Utc>>,
        last_success: Option<DateTime<Utc>>,
        mode: CheckMode,
    ) -> bool {
        if mode == CheckMode::Forced {
            return true;
        }
        if let Some(success) = last_success {
            if now - success < SUCCESS_CADENCE {
                return false;
            }
        }
        if let Some(attempt) = last_attempt {
            if now - attempt < FAILURE_BACKOFF {
                return false;
            }
        }
        true
    }

    fn begin_check(&self) -> Option<u64> {
        let mut guard = self.in_flight.lock().ok()?;
        if guard.is_some() {
            return None;
        }
        let generation = CHECK_GENERATION.fetch_add(1, Ordering::SeqCst) + 1;
        *guard = Some(generation);
        Some(generation)
    }

    fn finish_check(&self, generation: u64) -> bool {
        let Ok(mut guard) = self.in_flight.lock() else {
            return false;
        };
        if *guard != Some(generation) {
            return false;
        }
        *guard = None;
        true
    }

    fn record_attempt(&self, result: LastResult, success: bool) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|e| e.to_string())?;
        let now = Utc::now();
        state.last_attempt_at = Some(now);
        state.last_result = Some(result);
        if success {
            state.last_successful_check_at = Some(now);
        }
        state.save(&self.user_data).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn should_notify(&self, version: &str) -> bool {
        let state = self.state.lock().ok();
        state
            .and_then(|s| s.dismissed_version.clone())
            .map(|dismissed| dismissed != version)
            .unwrap_or(true)
    }

    pub async fn check_updates(
        &self,
        app: &AppHandle,
        mode: CheckMode,
    ) -> Result<BackgroundCheckResult, ManualCheckResult> {
        let state = self.current_state();
        let now = Utc::now();
        if mode == CheckMode::Background
            && !Self::should_run_launch_check(
                now,
                state.last_attempt_at,
                state.last_successful_check_at,
                mode,
            )
        {
            return Ok(BackgroundCheckResult::SkippedCadence);
        }

        let generation = match self.begin_check() {
            Some(g) => g,
            None if mode == CheckMode::Background => {
                return Ok(BackgroundCheckResult::SkippedCadence);
            }
            None => {
                return Err(ManualCheckResult::Failed {
                    message: "Update check already in progress".into(),
                });
            }
        };

        let outcome = self.perform_check(app, mode).await;
        if !self.finish_check(generation) {
            if mode == CheckMode::Background {
                return Ok(BackgroundCheckResult::SkippedCadence);
            }
            return Err(ManualCheckResult::Failed {
                message: "Update check was superseded".into(),
            });
        }

        outcome
    }

    async fn perform_check(
        &self,
        app: &AppHandle,
        mode: CheckMode,
    ) -> Result<BackgroundCheckResult, ManualCheckResult> {
        let current_version = app.package_info().version.to_string();

        let update = match app.updater() {
            Ok(updater) => updater.check().await,
            Err(e) => {
                let _ = self.record_attempt(LastResult::Error, false);
                return map_check_error(mode, e.to_string());
            }
        };

        let update = match update {
            Ok(Some(update)) => update,
            Ok(None) => {
                let _ = self.record_attempt(LastResult::Current, true);
                return map_current(mode);
            }
            Err(e) => {
                let offline = e.to_string().to_ascii_lowercase().contains("network")
                    || e.to_string().to_ascii_lowercase().contains("offline")
                    || e.to_string().to_ascii_lowercase().contains("connection");
                let result = if offline {
                    LastResult::Offline
                } else {
                    LastResult::Error
                };
                let _ = self.record_attempt(result, false);
                return map_check_error(mode, e.to_string());
            }
        };

        let manifest = UpdateManifest {
            version: update.version.clone(),
            notes: update.body.clone().unwrap_or_default(),
            release_url: release_notes_url(&update.version),
        };

        if let Err(reason) = validate_update_manifest(&manifest, &current_version) {
            tracing::error!(target: "security", "update manifest rejected: {reason}");
            let _ = self.record_attempt(LastResult::Error, false);
            if mode == CheckMode::Background {
                return Ok(BackgroundCheckResult::SecurityError);
            }
            return Err(ManualCheckResult::Failed {
                message: "Unable to verify update metadata".into(),
            });
        }

        if !is_newer_version(&current_version, &manifest.version).unwrap_or(false) {
            let _ = self.record_attempt(LastResult::Current, true);
            return map_current(mode);
        }

        let _ = self.record_attempt(LastResult::Available, true);
        let payload = UpdateAvailablePayload {
            version: manifest.version,
            notes: manifest.notes,
            release_url: manifest.release_url,
        };

        if mode == CheckMode::Background && !self.should_notify(&payload.version) {
            return Ok(BackgroundCheckResult::SilentCurrent);
        }

        if mode == CheckMode::Forced {
            return Err(ManualCheckResult::UpdateAvailable(payload));
        }

        Ok(BackgroundCheckResult::UpdateAvailable(payload))
    }

    pub async fn download_and_install(&self, app: &AppHandle, version: &str) -> Result<(), String> {
        let updater = app.updater().map_err(|e| e.to_string())?;
        let update = updater
            .check()
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "No update available".to_string())?;

        if update.version != version {
            return Err("Update version changed before install".into());
        }

        let app_handle = app.clone();
        let result = update
            .download_and_install(
                |chunk, total| {
                    let _ = app_handle.emit(
                        "update-download-progress",
                        serde_json::json!({
                            "chunkLength": chunk,
                            "contentLength": total,
                        }),
                    );
                },
                || {
                    let _ = app_handle.emit("update-download-finished", ());
                },
            )
            .await;

        if let Err(e) = result {
            tracing::error!(target: "security", "update install failed: {e}");
            return Err(format!("Update verification failed: {e}"));
        }

        app.restart();
    }

    pub fn fallback_page_url(&self, app: &AppHandle) -> Result<String, String> {
        let env_page = std::env::var("STREAMSYNC_UPDATE_PAGE").ok();
        let version = app.package_info().version.to_string();
        check_for_updates_url(env_page.as_deref(), &version)
    }
}

fn release_notes_url(version: &str) -> String {
    format!("https://github.com/KaoticGames/StreamSync/releases/tag/v{version}")
}

fn map_current(mode: CheckMode) -> Result<BackgroundCheckResult, ManualCheckResult> {
    match mode {
        CheckMode::Background => Ok(BackgroundCheckResult::SilentCurrent),
        CheckMode::Forced => Err(ManualCheckResult::UpToDate),
    }
}

fn map_check_error(
    mode: CheckMode,
    message: String,
) -> Result<BackgroundCheckResult, ManualCheckResult> {
    let offline = message.to_ascii_lowercase().contains("network")
        || message.to_ascii_lowercase().contains("offline")
        || message.to_ascii_lowercase().contains("connection");
    match mode {
        CheckMode::Background if offline => Ok(BackgroundCheckResult::SilentOffline),
        CheckMode::Background => Ok(BackgroundCheckResult::SecurityError),
        CheckMode::Forced => Err(ManualCheckResult::Failed {
            message: if offline {
                "Unable to check for updates (offline)".into()
            } else {
                "Unable to check for updates".into()
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updater::{validate_update_manifest, UpdateManifest};
    use chrono::TimeZone;

    #[test]
    fn semver_compare() {
        assert!(is_newer_version("2.1.0", "2.1.1").unwrap());
        assert!(!is_newer_version("2.1.0", "2.1.0").unwrap());
        assert!(!is_newer_version("2.1.0", "2.0.9").unwrap());
        assert!(!is_newer_version("2.1.0", "2.1.0-beta").unwrap());
        assert!(is_newer_version("2.1.0-beta", "2.1.0").unwrap());
    }

    #[test]
    fn launch_cadence() {
        let now = Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0).unwrap();
        let success_23h = now - Duration::hours(23);
        let success_25h = now - Duration::hours(25);
        let attempt_30m = now - Duration::minutes(30);
        let attempt_61m = now - Duration::minutes(61);

        assert!(!UpdateService::should_run_launch_check(
            now,
            None,
            Some(success_23h),
            CheckMode::Background
        ));
        assert!(UpdateService::should_run_launch_check(
            now,
            None,
            Some(success_25h),
            CheckMode::Background
        ));
        assert!(!UpdateService::should_run_launch_check(
            now,
            Some(attempt_30m),
            None,
            CheckMode::Background
        ));
        assert!(UpdateService::should_run_launch_check(
            now,
            Some(attempt_61m),
            None,
            CheckMode::Background
        ));
        assert!(UpdateService::should_run_launch_check(
            now,
            Some(attempt_30m),
            Some(success_25h),
            CheckMode::Forced
        ));
    }

    #[test]
    fn manual_check_reports() {
        let current = map_current(CheckMode::Forced);
        assert_eq!(current, Err(ManualCheckResult::UpToDate));

        let failed = map_check_error(CheckMode::Forced, "network down".into());
        assert!(matches!(
            failed,
            Err(ManualCheckResult::Failed { message }) if message.contains("offline")
        ));
    }

    #[test]
    fn offline_launch_silent() {
        let result = map_check_error(CheckMode::Background, "network error".into());
        assert_eq!(result, Ok(BackgroundCheckResult::SilentOffline));
    }

    #[test]
    fn concurrent_checks() {
        let dir = std::env::temp_dir().join(format!("streamsync-update-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let service = UpdateService::new(dir.clone());
        let first = service.begin_check();
        assert!(first.is_some());
        assert!(service.begin_check().is_none());
        assert!(service.finish_check(first.unwrap()));
        assert!(service.begin_check().is_some());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn completed_check_clears_only_its_own_generation() {
        let dir =
            std::env::temp_dir().join(format!("streamsync-update-finish-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let service = UpdateService::new(dir.clone());

        let generation = service.begin_check().expect("first check starts");
        assert!(service.finish_check(generation));
        assert!(
            service.begin_check().is_some(),
            "completed check releases gate"
        );
        assert!(
            !service.finish_check(generation),
            "stale generation cannot finish a newer check"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn updater_security_failures() {
        let manifest = UpdateManifest {
            version: "not-semver".into(),
            notes: String::new(),
            release_url: release_notes_url("2.1.1"),
        };
        assert!(validate_update_manifest(&manifest, "2.1.0").is_err());

        let manifest = UpdateManifest {
            version: "2.1.1".into(),
            notes: String::new(),
            release_url: "https://evil.example/tag/v2.1.1".into(),
        };
        assert!(validate_update_manifest(&manifest, "2.1.0").is_err());
    }

    #[test]
    fn dismissed_version() {
        let dir = std::env::temp_dir().join(format!("streamsync-dismiss-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let service = UpdateService::new(dir.clone());
        service.dismiss_version("2.1.1").unwrap();
        assert!(!service.should_notify("2.1.1"));
        assert!(service.should_notify("2.1.2"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn fallback_url() {
        let dir = std::env::temp_dir().join(format!("streamsync-fallback-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let url = check_for_updates_url(None, "2.1.0").unwrap();
        assert_eq!(
            url,
            "https://syndicateai.net/update?app=stream-sync&v=2.1.0"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
