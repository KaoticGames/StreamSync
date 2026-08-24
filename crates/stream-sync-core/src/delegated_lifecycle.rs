//! Delegated takeover lifecycle — durable revocation, generation fencing, bounded authority lease.
//!
//! ## Maximum revocation delay
//! Under push failure, delegated platform access must terminate within
//! [`MAX_DELEGATED_REVOCATION_DELAY`] plus at most one [`SYNDICATE_HTTP_TIMEOUT`] for the
//! in-flight validation request and up to [`SYNDICATE_SSE_READ_TIMEOUT`] for an open SSE read.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;

/// Maximum wall-clock delegated access after last successful validation when SSE push fails.
pub const MAX_DELEGATED_REVOCATION_DELAY: Duration = Duration::from_secs(300);
/// Upper bound on Syndicate exchange/refresh HTTP requests.
pub const SYNDICATE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on a single SSE read wait (no heartbeat).
pub const SYNDICATE_SSE_READ_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard cap on buffered SSE bytes before the parser fails closed.
pub const MAX_SSE_BUFFER_BYTES: usize = 64 * 1024;
/// Remote API error text is truncated before redaction/display.
pub const MAX_REMOTE_ERROR_CHARS: usize = 512;

pub type DelegatedGeneration = u64;

/// Which delegated background worker observed a revoke signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegatedWorker {
    Refresh,
    Watch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeardownPhase {
    Idle,
    Running,
    Completed,
    FailedRetryable(String),
}

#[derive(Debug)]
struct TeardownInner {
    generation: DelegatedGeneration,
    phase: TeardownPhase,
    waiters: Arc<Notify>,
}

impl Default for TeardownInner {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: TeardownPhase::Idle,
            waiters: Arc::new(Notify::new()),
        }
    }
}

/// Coordinates delegated teardown outside worker tasks. Concurrent callers await one result.
#[derive(Debug)]
pub struct TeardownCoordinator {
    active_generation: AtomicU64,
    inner: Mutex<TeardownInner>,
}

impl TeardownCoordinator {
    pub fn new() -> Self {
        Self {
            active_generation: AtomicU64::new(0),
            inner: Mutex::new(TeardownInner::default()),
        }
    }

    pub async fn install_generation_async(&self, generation: DelegatedGeneration) {
        self.active_generation.store(generation, Ordering::SeqCst);
        let mut inner = self.inner.lock().await;
        inner.generation = generation;
        inner.phase = TeardownPhase::Idle;
    }

    /// Run teardown once for `generation`, or join an in-flight attempt. Stale generations no-op.
    pub async fn run_or_join<F, Fut>(
        &self,
        generation: DelegatedGeneration,
        work: F,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        if generation != self.active_generation.load(Ordering::SeqCst) {
            return Ok(());
        }

        loop {
            let mut inner = self.inner.lock().await;
            if generation != self.active_generation.load(Ordering::SeqCst) {
                return Ok(());
            }
            if inner.generation != generation {
                inner.generation = generation;
                inner.phase = TeardownPhase::Idle;
            }
            match &inner.phase {
                TeardownPhase::Completed => return Ok(()),
                TeardownPhase::FailedRetryable(_) => {
                    inner.phase = TeardownPhase::Running;
                    inner.waiters = Arc::new(Notify::new());
                    drop(inner);
                    break;
                }
                TeardownPhase::Running => {
                    let waiters = inner.waiters.clone();
                    drop(inner);
                    waiters.notified().await;
                    continue;
                }
                TeardownPhase::Idle => {
                    inner.phase = TeardownPhase::Running;
                    inner.waiters = Arc::new(Notify::new());
                    drop(inner);
                    break;
                }
            }
        }

        let result = work().await;
        let mut inner = self.inner.lock().await;
        if generation != self.active_generation.load(Ordering::SeqCst) {
            return Ok(());
        }
        inner.phase = match &result {
            Ok(()) => TeardownPhase::Completed,
            Err(err) => TeardownPhase::FailedRetryable(err.clone()),
        };
        inner.waiters.notify_waiters();
        result
    }

    #[cfg(test)]
    pub async fn phase_for(&self, generation: DelegatedGeneration) -> TeardownPhase {
        let inner = self.inner.lock().await;
        if inner.generation == generation {
            inner.phase.clone()
        } else {
            TeardownPhase::Idle
        }
    }
}

impl Default for TeardownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Monotonic authority lease — successful validation resets; backoff cannot extend past deadline.
#[derive(Debug, Clone)]
pub struct AuthorityLease {
    deadline: Instant,
}

impl AuthorityLease {
    pub fn new_validated() -> Self {
        Self {
            deadline: Instant::now() + MAX_DELEGATED_REVOCATION_DELAY,
        }
    }

    pub fn renew_on_success(&mut self) {
        self.deadline = Instant::now() + MAX_DELEGATED_REVOCATION_DELAY;
    }

    pub fn merge_connection_expiry(&mut self, connection_expires_at: Option<&str>) {
        if let Some(iso) = connection_expires_at {
            if let Ok(exp) = chrono::DateTime::parse_from_rfc3339(iso) {
                let until = exp
                    .signed_duration_since(chrono::Utc::now())
                    .to_std()
                    .unwrap_or(Duration::ZERO);
                let cap = Instant::now() + until;
                if cap < self.deadline {
                    self.deadline = cap;
                }
            }
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Sleep budget for the next retry/revalidation attempt.
    pub fn sleep_budget(&self, preferred: Duration) -> Duration {
        preferred.min(self.remaining())
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

/// Build the Syndicate connection-key events URL without embedding the raw key.
pub fn connection_key_events_url(api_base: &str) -> String {
    format!(
        "{}/api/stream-sync/connection-keys/events",
        api_base.trim_end_matches('/')
    )
}

/// Build the Syndicate Kick feed URL without embedding the raw key.
pub fn kick_feed_url(api_base: &str) -> String {
    format!(
        "{}/api/stream-sync/kick-feed",
        api_base.trim_end_matches('/')
    )
}

/// Authorization header value for connection-key streams (never place the key in the URL).
pub fn connection_key_authorization(key: &str) -> String {
    format!("Bearer {}", key.trim())
}

/// Redact a connection key from free-form error text for logs.
pub fn redact_connection_key(text: &str, key: &str) -> String {
    let key = key.trim();
    if key.is_empty() {
        return bound_remote_message(text);
    }
    bound_remote_message(&text.replace(key, "[redacted-connection-key]"))
}

pub fn bound_remote_message(text: &str) -> String {
    text.chars().take(MAX_REMOTE_ERROR_CHARS).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseBufferError {
    Overflow,
}

/// Incrementally extract complete SSE frames (supports LF and CRLF delimiters).
pub fn drain_sse_frames(buf: &mut String) -> Result<Vec<String>, SseBufferError> {
    if buf.len() > MAX_SSE_BUFFER_BYTES {
        return Err(SseBufferError::Overflow);
    }
    let mut frames = Vec::new();
    while let Some((idx, len)) = find_sse_delimiter(buf) {
        let frame = buf[..idx].to_string();
        *buf = buf[idx + len..].to_string();
        if !frame.trim().is_empty() {
            frames.push(frame);
        }
    }
    Ok(frames)
}

pub fn append_sse_chunk(buf: &mut String, chunk: &str) -> Result<Vec<String>, SseBufferError> {
    if buf.len() + chunk.len() > MAX_SSE_BUFFER_BYTES {
        return Err(SseBufferError::Overflow);
    }
    buf.push_str(chunk);
    drain_sse_frames(buf)
}

fn find_sse_delimiter(buf: &str) -> Option<(usize, usize)> {
    let crlf = buf.find("\r\n\r\n").map(|i| (i, 4));
    let lf = buf.find("\n\n").map(|i| (i, 2));
    match (crlf, lf) {
        (Some(a), Some(b)) if a.0 <= b.0 => Some(a),
        (Some(a), Some(_)) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, None) => None,
    }
}

pub fn parse_sse_json_data(frame: &str) -> Option<serde_json::Value> {
    let mut data = String::new();
    for line in frame.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    if data.is_empty() {
        return None;
    }
    serde_json::from_str(&data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn coordinator_retries_after_failure_then_completes() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await;
        let attempts = Arc::new(AtomicU64::new(0));
        let a1 = attempts.clone();
        let c1 = coord.clone();
        let first = tokio::spawn(async move {
            c1.run_or_join(1, || async {
                a1.fetch_add(1, Ordering::SeqCst);
                Err("disk".into())
            })
            .await
        });
        let a2 = attempts.clone();
        let c2 = coord.clone();
        let second = tokio::spawn(async move {
            c2.run_or_join(1, || async {
                a2.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        assert_eq!(first.await.unwrap(), Err("disk".into()));
        assert_eq!(second.await.unwrap(), Ok(()));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Completed);
    }

    #[tokio::test]
    async fn coordinator_stale_generation_is_ignored() {
        let coord = TeardownCoordinator::new();
        coord.install_generation_async(2).await;
        assert!(coord.run_or_join(1, || async { Ok(()) }).await.is_ok());
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Idle);
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_result() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(3).await;
        let started = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = coord.clone();
            let s = started.clone();
            handles.push(tokio::spawn(async move {
                c.run_or_join(3, || async {
                    if s.fetch_add(1, Ordering::SeqCst) == 0 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(())
                    } else {
                        Ok(())
                    }
                })
                .await
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn authority_lease_backoff_cannot_extend_deadline() {
        let mut lease = AuthorityLease::new_validated();
        lease.deadline = Instant::now() + Duration::from_millis(50);
        let budget = lease.sleep_budget(Duration::from_secs(60));
        assert!(budget <= Duration::from_millis(50));
    }

    #[test]
    fn sse_parser_handles_lf_crlf_and_split_delimiters() {
        let mut buf = "data: {\"type\":\"revoked\"}\r\n\r\n".to_string();
        let frames = drain_sse_frames(&mut buf).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(parse_sse_json_data(&frames[0]).is_some());

        let mut buf = "data: {\"type\":\"revoked\"}\n\n".to_string();
        assert_eq!(drain_sse_frames(&mut buf).unwrap().len(), 1);

        let mut buf = "data: {\"type\":\"a\"}\r\n\r".to_string();
        assert!(drain_sse_frames(&mut buf).unwrap().is_empty());
        let frames = append_sse_chunk(&mut buf, "\n\r\n").unwrap();
        assert_eq!(frames.len(), 1);

        let mut buf = String::new();
        let f1 = append_sse_chunk(&mut buf, "data: {\"type\":\"a\"}\n\n").unwrap();
        let f2 = append_sse_chunk(&mut buf, "data: {\"type\":\"b\"}\n\n").unwrap();
        assert_eq!(f1.len(), 1);
        assert_eq!(f2.len(), 1);
    }

    #[test]
    fn sse_buffer_overflow_fails_closed() {
        let mut buf = "x".repeat(MAX_SSE_BUFFER_BYTES);
        assert_eq!(
            append_sse_chunk(&mut buf, "y").unwrap_err(),
            SseBufferError::Overflow
        );
    }

    #[test]
    fn redact_connection_key_strips_secret() {
        let key = "ssk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let msg = format!("watch failed for {key} with timeout");
        let redacted = redact_connection_key(&msg, key);
        assert!(!redacted.contains(key));
        assert!(redacted.contains("[redacted-connection-key]"));
    }

    #[tokio::test]
    async fn stop_except_caller_does_not_abort_self() {
        let refresh: Arc<RwLock<Option<JoinHandle<()>>>> = Arc::new(RwLock::new(None));
        let watch: Arc<RwLock<Option<JoinHandle<()>>>> = Arc::new(RwLock::new(None));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done2 = done.clone();
        let watch2 = watch.clone();
        let handle = tokio::spawn(async move {
            stop_delegated_worker_handles(&refresh, &watch2, Some(DelegatedWorker::Watch)).await;
            done2.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        *watch.write().await = Some(handle);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }
}
