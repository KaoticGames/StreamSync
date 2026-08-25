//! Delegated takeover lifecycle — durable revocation, generation fencing, bounded authority lease.
//!
//! ## Maximum revocation delay
//! The enforceable maximum delegated access window after the last successful remote validation is
//! [`MAX_DELEGATED_REVOCATION_DELAY`] (300 seconds). Syndicate HTTP and SSE timeouts are consumed
//! from that lease budget and must not extend it.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;

/// Maximum wall-clock delegated access after last successful remote validation when push fails.
pub const MAX_DELEGATED_REVOCATION_DELAY: Duration = Duration::from_secs(300);
/// Upper bound on Syndicate exchange/refresh HTTP requests (within the lease budget).
pub const SYNDICATE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on a single SSE read wait (within the lease budget).
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

type ResultWatch = watch::Sender<Option<Result<(), String>>>;

#[derive(Debug)]
struct TeardownInner {
    generation: DelegatedGeneration,
    phase: TeardownPhase,
    /// Shared attempt result. `None` means still running; `Some` is the terminal result.
    result_tx: Option<ResultWatch>,
}

impl Default for TeardownInner {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: TeardownPhase::Idle,
            result_tx: None,
        }
    }
}

/// Coordinates delegated teardown outside worker tasks. Concurrent callers await one result.
#[derive(Debug)]
pub struct TeardownCoordinator {
    active_generation: AtomicU64,
    /// Sync mutex so drop guards can mark FailedRetryable without async.
    inner: Arc<std::sync::Mutex<TeardownInner>>,
}

impl TeardownCoordinator {
    pub fn new() -> Self {
        Self {
            active_generation: AtomicU64::new(0),
            inner: Arc::new(std::sync::Mutex::new(TeardownInner::default())),
        }
    }

    pub fn active_generation(&self) -> DelegatedGeneration {
        self.active_generation.load(Ordering::SeqCst)
    }

    /// Install a newer generation. Rejects stale/lower generations (monotonic).
    /// Resolves any in-flight waiters for the previous generation as superseded.
    pub async fn install_generation_async(
        &self,
        generation: DelegatedGeneration,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().map_err(|e| e.to_string())?;
        let current = self.active_generation.load(Ordering::SeqCst);
        if generation < current {
            return Err(format!(
                "stale generation install rejected: {generation} < {current}"
            ));
        }
        if generation == current && generation != 0 {
            // Re-arm after a completed/failed teardown for the same generation id.
            if let Some(tx) = inner.result_tx.take() {
                let _ = tx.send(Some(Err("generation reinstalled".into())));
            }
            inner.phase = TeardownPhase::Idle;
            return Ok(());
        }
        if let Some(tx) = inner.result_tx.take() {
            let _ = tx.send(Some(Err("superseded by newer generation".into())));
        }
        self.active_generation.store(generation, Ordering::SeqCst);
        inner.generation = generation;
        inner.phase = TeardownPhase::Idle;
        Ok(())
    }

    /// Run teardown once for `generation`, or join an in-flight attempt. Stale generations no-op.
    ///
    /// Concurrent joiners share the owner's result via a watch channel (no lost-wakeup).
    /// Retries only begin after an explicit FailedRetryable / Idle boundary — a joiner never
    /// silently starts a second attempt while the owner is still Running.
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

        let owner_tx: Option<ResultWatch> = {
            loop {
                enum Step {
                    Own(ResultWatch),
                    Wait(watch::Receiver<Option<Result<(), String>>>),
                }
                let step = {
                    let mut inner = self.inner.lock().map_err(|e| e.to_string())?;
                    if generation != self.active_generation.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    if inner.generation != generation {
                        if let Some(tx) = inner.result_tx.take() {
                            let _ = tx.send(Some(Err("superseded by newer generation".into())));
                        }
                        inner.generation = generation;
                        inner.phase = TeardownPhase::Idle;
                        inner.result_tx = None;
                    }
                    match &inner.phase {
                        TeardownPhase::Completed => return Ok(()),
                        TeardownPhase::FailedRetryable(_) | TeardownPhase::Idle => {
                            let (tx, _rx) = watch::channel(None);
                            inner.phase = TeardownPhase::Running;
                            inner.result_tx = Some(tx.clone());
                            Step::Own(tx)
                        }
                        TeardownPhase::Running => {
                            let Some(tx) = inner.result_tx.clone() else {
                                inner.phase = TeardownPhase::FailedRetryable(
                                    "teardown channel missing".into(),
                                );
                                continue;
                            };
                            Step::Wait(tx.subscribe())
                        }
                    }
                };
                match step {
                    Step::Own(tx) => break Some(tx),
                    Step::Wait(mut rx) => loop {
                        {
                            let borrowed = rx.borrow();
                            if let Some(result) = borrowed.as_ref() {
                                return result.clone();
                            }
                        }
                        if rx.changed().await.is_err() {
                            if let Ok(mut inner) = self.inner.lock() {
                                if inner.generation == generation
                                    && matches!(inner.phase, TeardownPhase::Running)
                                {
                                    inner.phase = TeardownPhase::FailedRetryable(
                                        "teardown owner cancelled".into(),
                                    );
                                    inner.result_tx = None;
                                }
                            }
                            return Err("teardown owner cancelled".into());
                        }
                    },
                }
            }
        };

        let Some(tx) = owner_tx else {
            return Ok(());
        };

        struct PublishOnDrop {
            inner: Arc<std::sync::Mutex<TeardownInner>>,
            generation: DelegatedGeneration,
            tx: ResultWatch,
            published: bool,
        }
        impl Drop for PublishOnDrop {
            fn drop(&mut self) {
                if self.published {
                    return;
                }
                let _ = self.tx.send(Some(Err("teardown owner cancelled".into())));
                if let Ok(mut inner) = self.inner.lock() {
                    if inner.generation == self.generation
                        && matches!(inner.phase, TeardownPhase::Running)
                    {
                        inner.phase =
                            TeardownPhase::FailedRetryable("teardown owner cancelled".into());
                        inner.result_tx = None;
                    }
                }
            }
        }
        let mut guard = PublishOnDrop {
            inner: self.inner.clone(),
            generation,
            tx: tx.clone(),
            published: false,
        };

        let result = work().await;

        {
            let mut inner = self.inner.lock().map_err(|e| e.to_string())?;
            if generation != self.active_generation.load(Ordering::SeqCst) {
                let _ = tx.send(Some(Err("superseded by newer generation".into())));
                guard.published = true;
                return Ok(());
            }
            inner.phase = match &result {
                Ok(()) => TeardownPhase::Completed,
                Err(err) => TeardownPhase::FailedRetryable(err.clone()),
            };
            let _ = tx.send(Some(result.clone()));
            guard.published = true;
        }
        result
    }

    #[cfg(test)]
    pub async fn phase_for(&self, generation: DelegatedGeneration) -> TeardownPhase {
        let inner = self.inner.lock().unwrap();
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

/// Generation-scoped monotonic authority lease.
///
/// Only [`AuthorityLease::renew_after_successful_remote_validation`] extends the deadline.
/// Local stored `connection_expires_at` may only shorten it.
#[derive(Debug, Clone)]
pub struct AuthorityLease {
    generation: DelegatedGeneration,
    deadline: Instant,
}

impl AuthorityLease {
    /// Placeholder lease (generation 0, already expired) until a session is validated.
    pub fn inactive() -> Self {
        Self {
            generation: 0,
            deadline: Instant::now(),
        }
    }

    /// Bootstrap after process restart with persisted delegated state: one HTTP-budget window to
    /// revalidate. Does not grant a fresh five-minute lease.
    pub fn pending_remote_validation(generation: DelegatedGeneration) -> Self {
        Self {
            generation,
            deadline: Instant::now() + SYNDICATE_HTTP_TIMEOUT,
        }
    }

    pub fn generation(&self) -> DelegatedGeneration {
        self.generation
    }

    /// Reset the lease after a successful remote Syndicate validation for this generation.
    pub fn renew_after_successful_remote_validation(
        &mut self,
        generation: DelegatedGeneration,
        connection_expires_at: Option<&str>,
    ) {
        if generation == 0 {
            return;
        }
        if self.generation != 0 && self.generation != generation {
            return;
        }
        self.generation = generation;
        self.deadline = Instant::now() + MAX_DELEGATED_REVOCATION_DELAY;
        self.cap_by_connection_expiry(connection_expires_at);
    }

    /// Shorten the deadline from remote/local connection expiry. Never extends.
    pub fn cap_by_connection_expiry(&mut self, connection_expires_at: Option<&str>) {
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

    /// Sleep budget for the next retry/revalidation attempt (never beyond the lease).
    pub fn sleep_budget(&self, preferred: Duration) -> Duration {
        preferred.min(self.remaining())
    }

    #[cfg(test)]
    pub fn set_deadline_for_test(&mut self, deadline: Instant) {
        self.deadline = deadline;
    }
}

/// Abort delegated worker handles except the caller, clearing slots so they are not restarted.
pub async fn stop_delegated_worker_handles(
    refresh_handle: &RwLock<Option<GenerationTask>>,
    watch_handle: &RwLock<Option<GenerationTask>>,
    except: Option<DelegatedWorker>,
) {
    if except != Some(DelegatedWorker::Refresh) {
        if let Some(task) = refresh_handle.write().await.take() {
            task.handle.abort();
        }
    } else {
        let _ = refresh_handle.write().await.take();
    }
    if except != Some(DelegatedWorker::Watch) {
        if let Some(task) = watch_handle.write().await.take() {
            task.handle.abort();
        }
    } else {
        let _ = watch_handle.write().await.take();
    }
}

/// Worker JoinHandle tagged with the generation that owns it.
#[derive(Debug)]
pub struct GenerationTask {
    pub generation: DelegatedGeneration,
    pub handle: JoinHandle<()>,
}

/// Atomically install a generation-tagged worker, aborting any previous slot occupant.
pub async fn install_generation_task(
    slot: &RwLock<Option<GenerationTask>>,
    generation: DelegatedGeneration,
    handle: JoinHandle<()>,
) {
    let mut guard = slot.write().await;
    if let Some(prev) = guard.take() {
        prev.handle.abort();
    }
    *guard = Some(GenerationTask { generation, handle });
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
    use tokio::sync::{Barrier, RwLock};

    #[tokio::test]
    async fn coordinator_retries_after_failure_then_completes() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();
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
        assert_eq!(first.await.unwrap(), Err("disk".into()));
        let a2 = attempts.clone();
        let c2 = coord.clone();
        let second = tokio::spawn(async move {
            c2.run_or_join(1, || async {
                a2.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        assert_eq!(second.await.unwrap(), Ok(()));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Completed);
    }

    #[tokio::test]
    async fn coordinator_stale_generation_is_ignored() {
        let coord = TeardownCoordinator::new();
        coord.install_generation_async(2).await.unwrap();
        assert!(coord.run_or_join(1, || async { Ok(()) }).await.is_ok());
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Idle);
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_result() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(3).await.unwrap();
        let started = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = coord.clone();
            let s = started.clone();
            handles.push(tokio::spawn(async move {
                c.run_or_join(3, || async {
                    if s.fetch_add(1, Ordering::SeqCst) == 0 {
                        tokio::time::sleep(Duration::from_millis(40)).await;
                        Ok(())
                    } else {
                        unreachable!("joiner must not run owner work");
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

    #[tokio::test]
    async fn coordinator_joiner_shares_owner_failure_not_second_attempt() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let started = Arc::new(AtomicU64::new(0));
        let c1 = coord.clone();
        let b1 = barrier.clone();
        let s1 = started.clone();
        let owner = tokio::spawn(async move {
            c1.run_or_join(1, || async {
                s1.fetch_add(1, Ordering::SeqCst);
                b1.wait().await;
                Err("disk".into())
            })
            .await
        });
        // Wait until owner is Running.
        for _ in 0..50 {
            if matches!(coord.phase_for(1).await, TeardownPhase::Running) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let c2 = coord.clone();
        let s2 = started.clone();
        let joiner = tokio::spawn(async move {
            c2.run_or_join(1, || async {
                s2.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        barrier.wait().await;
        assert_eq!(owner.await.unwrap(), Err("disk".into()));
        assert_eq!(joiner.await.unwrap(), Err("disk".into()));
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coordinator_owner_cancel_wakes_joiners_retryable() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();
        let (enter_tx, enter_rx) = oneshot_pair();
        let c1 = coord.clone();
        let owner = tokio::spawn(async move {
            c1.run_or_join(1, || async {
                let _ = enter_tx.send(());
                std::future::pending::<Result<(), String>>().await
            })
            .await
        });
        enter_rx.await.unwrap();
        let c2 = coord.clone();
        let joiner = tokio::spawn(async move { c2.run_or_join(1, || async { Ok(()) }).await });
        owner.abort();
        let joined = tokio::time::timeout(Duration::from_secs(2), joiner)
            .await
            .expect("joiner must not hang")
            .unwrap();
        assert!(joined.is_err());
        assert!(matches!(
            coord.phase_for(1).await,
            TeardownPhase::FailedRetryable(_)
        ));
        // Explicit retry after failure boundary succeeds.
        assert!(coord.run_or_join(1, || async { Ok(()) }).await.is_ok());
    }

    #[tokio::test]
    async fn install_newer_generation_wakes_old_waiters() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();
        let (enter_tx, enter_rx) = oneshot_pair();
        let (joined_tx, joined_rx) = oneshot_pair();
        let c1 = coord.clone();
        let owner = tokio::spawn(async move {
            c1.run_or_join(1, || async {
                let _ = enter_tx.send(());
                std::future::pending::<Result<(), String>>().await
            })
            .await
        });
        enter_rx.await.unwrap();
        let c2 = coord.clone();
        let joiner = tokio::spawn(async move {
            let result = c2.run_or_join(1, || async { Ok(()) }).await;
            let _ = joined_tx.send(());
            result
        });
        // Ensure joiner has subscribed before superseding generation 1.
        tokio::time::sleep(Duration::from_millis(30)).await;
        coord.install_generation_async(2).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), joined_rx)
            .await
            .expect("gen1 joiner must be woken");
        let joined = joiner.await.unwrap();
        assert!(
            joined.is_err(),
            "waiting gen1 joiner must observe supersede"
        );
        owner.abort();
    }

    #[tokio::test]
    async fn install_generation_is_monotonic() {
        let coord = TeardownCoordinator::new();
        coord.install_generation_async(2).await.unwrap();
        assert!(coord.install_generation_async(1).await.is_err());
        assert_eq!(coord.active_generation(), 2);
    }

    #[test]
    fn authority_lease_backoff_cannot_extend_deadline() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease.set_deadline_for_test(Instant::now() + Duration::from_millis(50));
        let budget = lease.sleep_budget(Duration::from_secs(60));
        assert!(budget <= Duration::from_millis(50));
    }

    #[test]
    fn local_connection_expiry_cannot_extend_lease() {
        let mut lease = AuthorityLease {
            generation: 1,
            deadline: Instant::now() + Duration::from_secs(10),
        };
        let far = (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339();
        lease.cap_by_connection_expiry(Some(&far));
        assert!(lease.remaining() <= Duration::from_secs(10) + Duration::from_millis(50));
        // renew without matching generation is ignored when generation already set differently
        lease.renew_after_successful_remote_validation(2, None);
        assert!(lease.remaining() <= Duration::from_secs(10) + Duration::from_millis(50));
        lease.renew_after_successful_remote_validation(1, None);
        assert!(lease.remaining() > Duration::from_secs(200));
    }

    #[test]
    fn restart_lease_does_not_grant_full_window() {
        let lease = AuthorityLease::pending_remote_validation(7);
        assert!(lease.remaining() <= SYNDICATE_HTTP_TIMEOUT + Duration::from_millis(50));
        assert!(lease.remaining() < MAX_DELEGATED_REVOCATION_DELAY);
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
        let key = "ssk_test_placeholder_key_aaaaaaaa";
        let msg = format!("watch failed for {key} with timeout");
        let redacted = redact_connection_key(&msg, key);
        assert!(!redacted.contains(key));
        assert!(redacted.contains("[redacted-connection-key]"));
    }

    #[tokio::test]
    async fn stop_except_caller_does_not_abort_self() {
        let refresh: Arc<RwLock<Option<GenerationTask>>> = Arc::new(RwLock::new(None));
        let watch: Arc<RwLock<Option<GenerationTask>>> = Arc::new(RwLock::new(None));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done2 = done.clone();
        let watch2 = watch.clone();
        let handle = tokio::spawn(async move {
            stop_delegated_worker_handles(&refresh, &watch2, Some(DelegatedWorker::Watch)).await;
            done2.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        *watch.write().await = Some(GenerationTask {
            generation: 1,
            handle,
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }

    fn oneshot_pair() -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        tokio::sync::oneshot::channel()
    }
}
