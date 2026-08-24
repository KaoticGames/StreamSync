//! Idempotent delegated-session teardown that never aborts the calling worker mid-cleanup.
//!
//! ## Revocation delay bound
//! If the Syndicate SSE push is missed or the watch transport fails, StreamSync still
//! revalidates the connection key at least every [`MAX_DELEGATED_REVOCATION_DELAY`].
//! That is the maximum window delegated platform access may continue after a remote
//! revoke when push delivery fails.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Maximum time delegated access may continue after a remote revoke when SSE push fails.
/// Refresh/watch loops must revalidate at least this often.
pub const MAX_DELEGATED_REVOCATION_DELAY: Duration = Duration::from_secs(300);

/// Which delegated background worker is invoking teardown (so it is not aborted mid-flight).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegatedWorker {
    Refresh,
    Watch,
}

#[derive(Debug, Default)]
pub struct TeardownGate {
    /// True once a teardown has begun or completed for the current delegated generation.
    started: AtomicBool,
}

impl TeardownGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if this caller should perform teardown; false if another caller already did.
    pub fn try_begin(&self) -> bool {
        self.started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn reset(&self) {
        self.started.store(false, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
}

/// Abort delegated worker handles except the caller, clearing slots so they are not restarted.
pub async fn stop_delegated_worker_handles(
    refresh_handle: &RwLock<Option<JoinHandle<()>>>,
    watch_handle: &RwLock<Option<JoinHandle<()>>>,
    except: Option<DelegatedWorker>,
) {
    if except != Some(DelegatedWorker::Refresh) {
        if let Some(h) = refresh_handle.write().await.take() {
            h.abort();
        }
    } else {
        let _ = refresh_handle.write().await.take();
    }
    if except != Some(DelegatedWorker::Watch) {
        if let Some(h) = watch_handle.write().await.take() {
            h.abort();
        }
    } else {
        let _ = watch_handle.write().await.take();
    }
}

/// Cap a sleep so delegated credentials are revalidated within the max revocation delay.
pub fn capped_revalidation_sleep(until_refresh: Duration) -> Duration {
    until_refresh.min(MAX_DELEGATED_REVOCATION_DELAY)
}

/// Build the Syndicate connection-key events URL without embedding the raw key.
pub fn connection_key_events_url(api_base: &str) -> String {
    format!(
        "{}/api/stream-sync/connection-keys/events",
        api_base.trim_end_matches('/')
    )
}

/// Authorization header value for connection-key event streams (never place the key in the URL).
pub fn connection_key_authorization(key: &str) -> String {
    format!("Bearer {}", key.trim())
}

/// Redact a connection key from free-form error text for logs.
pub fn redact_connection_key(text: &str, key: &str) -> String {
    let key = key.trim();
    if key.is_empty() {
        return text.to_string();
    }
    text.replace(key, "[redacted-connection-key]")
}

/// Shared progress markers for teardown regression tests.
#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub struct TeardownProgress {
    pub cleared_delegated: std::sync::Arc<AtomicBool>,
    pub stopped_platform: std::sync::Arc<AtomicBool>,
    pub personal_fallback: std::sync::Arc<AtomicBool>,
    pub finished: std::sync::Arc<AtomicBool>,
}

#[cfg(test)]
impl TeardownProgress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> (bool, bool, bool, bool) {
        (
            self.cleared_delegated.load(Ordering::SeqCst),
            self.stopped_platform.load(Ordering::SeqCst),
            self.personal_fallback.load(Ordering::SeqCst),
            self.finished.load(Ordering::SeqCst),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Documents the historical self-abort failure mode: aborting the caller's JoinHandle
    /// before later awaits means personal fallback / platform stop never run.
    #[tokio::test]
    async fn self_abort_before_await_skips_tail_cleanup() {
        let progress = TeardownProgress::new();
        let slot: Arc<RwLock<Option<JoinHandle<()>>>> = Arc::new(RwLock::new(None));

        let progress2 = progress.clone();
        let slot2 = slot.clone();
        let handle = tokio::spawn(async move {
            progress2.cleared_delegated.store(true, Ordering::SeqCst);
            // Historical bug: abort own handle, then await further cleanup.
            if let Some(h) = slot2.write().await.take() {
                h.abort();
            }
            progress2.stopped_platform.store(true, Ordering::SeqCst);
            tokio::task::yield_now().await;
            progress2.personal_fallback.store(true, Ordering::SeqCst);
            progress2.finished.store(true, Ordering::SeqCst);
        });
        *slot.write().await = Some(handle);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let (cleared, stopped, fallback, finished) = progress.snapshot();
        assert!(cleared, "pre-abort work may complete");
        // Under the buggy pattern, post-abort awaits must not be relied upon.
        assert!(
            !fallback && !finished,
            "self-abort must not be treated as completing teardown (stopped={stopped})"
        );
    }

    #[tokio::test]
    async fn teardown_except_caller_completes_tail_cleanup() {
        let progress = TeardownProgress::new();
        let refresh: Arc<RwLock<Option<JoinHandle<()>>>> = Arc::new(RwLock::new(None));
        let watch: Arc<RwLock<Option<JoinHandle<()>>>> = Arc::new(RwLock::new(None));
        let gate = TeardownGate::new();

        let progress2 = progress.clone();
        let refresh2 = refresh.clone();
        let watch2 = watch.clone();
        let handle = tokio::spawn(async move {
            assert!(gate.try_begin());
            progress2.cleared_delegated.store(true, Ordering::SeqCst);
            stop_delegated_worker_handles(&refresh2, &watch2, Some(DelegatedWorker::Watch)).await;
            progress2.stopped_platform.store(true, Ordering::SeqCst);
            tokio::task::yield_now().await;
            progress2.personal_fallback.store(true, Ordering::SeqCst);
            progress2.finished.store(true, Ordering::SeqCst);
        });
        *watch.write().await = Some(handle);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if progress.finished.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (cleared, stopped, fallback, finished) = progress.snapshot();
        assert!(cleared && stopped && fallback && finished);
        assert!(watch.read().await.is_none());
    }

    #[tokio::test]
    async fn teardown_gate_is_idempotent() {
        let gate = TeardownGate::new();
        assert!(!gate.is_started());
        assert!(gate.try_begin());
        assert!(gate.is_started());
        assert!(!gate.try_begin());
        gate.reset();
        assert!(!gate.is_started());
        assert!(gate.try_begin());
    }

    #[test]
    fn revalidation_sleep_is_capped() {
        assert_eq!(
            capped_revalidation_sleep(Duration::from_secs(3600)),
            MAX_DELEGATED_REVOCATION_DELAY
        );
        assert_eq!(
            capped_revalidation_sleep(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn events_url_never_embeds_key() {
        let url = connection_key_events_url("https://api.example.test");
        assert_eq!(
            url,
            "https://api.example.test/api/stream-sync/connection-keys/events"
        );
        assert!(!url.contains("key="));
        let auth = connection_key_authorization("ssk_test_placeholder_not_real");
        assert!(auth.starts_with("Bearer ssk_"));
        assert!(!url.contains("ssk_"));
    }

    #[test]
    fn redact_connection_key_strips_secret() {
        let key = "ssk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let msg = format!("watch failed for {key} with timeout");
        let redacted = redact_connection_key(&msg, key);
        assert!(!redacted.contains(key));
        assert!(redacted.contains("[redacted-connection-key]"));
    }
}
