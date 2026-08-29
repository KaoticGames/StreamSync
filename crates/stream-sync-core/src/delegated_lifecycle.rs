//! Delegated takeover lifecycle — durable revocation, generation fencing, bounded authority lease.
//!
//! ## Maximum revocation delay
//! The enforceable maximum delegated access window after the last successful remote validation is
//! [`MAX_DELEGATED_REVOCATION_DELAY`] (300 seconds). Syndicate HTTP and SSE timeouts are consumed
//! from that lease budget and must not extend it.
//!
//! ## Durable revoke failure model (B5)
//! A successful local revoke response requires durable pending-marker and/or tombstone persistence.
//! If the pending-marker write fails, the route returns an error and in-memory delegated authority
//! is stripped immediately so live workers cannot continue. Startup never activates stored
//! delegated credentials when a pending or tombstone marker exists.
//! **Residual:** if every durable write fails *and* the process crashes before memory is stripped,
//! local revocation intent cannot be recovered solely from an unchanged credential file. That
//! total-storage-failure + crash case is unavoidable without an independent durable authority
//! source; do not claim crash-persistent local revocation when all durable writes fail.

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
    /// Monotonic teardown attempt epoch within the coordinator. Owner drop/completion may
    /// mutate phase only when this still matches the attempt they owned.
    attempt: u64,
    phase: TeardownPhase,
    /// Shared attempt result. `None` means still running; `Some` is the terminal result.
    result_tx: Option<ResultWatch>,
}

impl Default for TeardownInner {
    fn default() -> Self {
        Self {
            generation: 0,
            attempt: 0,
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
    ///
    /// Same-generation install is idempotent: a Running teardown is left untouched.
    /// Completed / FailedRetryable / Idle re-arms to Idle without cancelling a runner.
    /// Strictly newer generations supersede in-flight waiters.
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
            // Idempotent same-gen install: never cancel a Running teardown.
            if matches!(inner.phase, TeardownPhase::Running) {
                return Ok(());
            }
            // Re-arm after Completed / FailedRetryable / Idle without waking waiters
            // (there is no Running channel to cancel).
            inner.result_tx = None;
            inner.phase = TeardownPhase::Idle;
            return Ok(());
        }
        if let Some(tx) = inner.result_tx.take() {
            let _ = tx.send(Some(Err("superseded by newer generation".into())));
        }
        self.active_generation.store(generation, Ordering::SeqCst);
        inner.generation = generation;
        inner.phase = TeardownPhase::Idle;
        inner.result_tx = None;
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

        let owner: Option<(ResultWatch, u64)> = {
            loop {
                enum Step {
                    Own(ResultWatch, u64),
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
                            inner.attempt = inner.attempt.saturating_add(1);
                            let my_attempt = inner.attempt;
                            inner.phase = TeardownPhase::Running;
                            inner.result_tx = Some(tx.clone());
                            Step::Own(tx, my_attempt)
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
                    Step::Own(tx, attempt) => break Some((tx, attempt)),
                    Step::Wait(mut rx) => loop {
                        {
                            let borrowed = rx.borrow();
                            if let Some(result) = borrowed.as_ref() {
                                return result.clone();
                            }
                        }
                        if rx.changed().await.is_err() {
                            // Sender dropped — prefer last published value over synthesizing cancel.
                            {
                                let borrowed = rx.borrow();
                                if let Some(result) = borrowed.as_ref() {
                                    return result.clone();
                                }
                            }
                            if let Ok(mut inner) = self.inner.lock() {
                                // Do not corrupt a replacement attempt that already installed a
                                // new result channel (old sender drop wakes stale joiners).
                                if inner.generation == generation
                                    && matches!(inner.phase, TeardownPhase::Running)
                                    && inner.result_tx.is_none()
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

        let Some((tx, my_attempt)) = owner else {
            return Ok(());
        };

        struct PublishOnDrop {
            inner: Arc<std::sync::Mutex<TeardownInner>>,
            generation: DelegatedGeneration,
            attempt: u64,
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
                        && inner.attempt == self.attempt
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
            attempt: my_attempt,
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
            // Stale owner (ABA): a newer attempt owns the generation — do not mutate.
            if inner.attempt != my_attempt {
                guard.published = true;
                return result;
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

    pub async fn phase_for(&self, generation: DelegatedGeneration) -> TeardownPhase {
        let inner = self.inner.lock().unwrap();
        if inner.generation == generation {
            inner.phase.clone()
        } else {
            TeardownPhase::Idle
        }
    }

    #[cfg(test)]
    pub fn attempt_for_test(&self) -> u64 {
        self.inner.lock().unwrap().attempt
    }

    /// Test-only: apply an owner completion as if `attempt` still owned the generation.
    /// Returns whether the coordinator phase was mutated.
    #[cfg(test)]
    pub fn apply_owner_completion_for_test(
        &self,
        generation: DelegatedGeneration,
        attempt: u64,
        result: Result<(), String>,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.generation != generation || inner.attempt != attempt {
            return false;
        }
        if !matches!(inner.phase, TeardownPhase::Running) {
            return false;
        }
        inner.phase = match &result {
            Ok(()) => TeardownPhase::Completed,
            Err(err) => TeardownPhase::FailedRetryable(err.clone()),
        };
        if let Some(tx) = &inner.result_tx {
            let _ = tx.send(Some(result));
        }
        true
    }
}

impl Default for TeardownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time `(generation, deadline)` for generation-bound network races.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityLeaseSnapshot {
    pub generation: DelegatedGeneration,
    pub deadline: Instant,
}

/// Whether a lease may run platform clients or only Syndicate revalidation/watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityLeasePhase {
    Inactive,
    /// HTTP-budget window for Syndicate revalidation only — no platform activation.
    PendingValidation,
    /// Remote validation succeeded; platform operations are permitted until deadline.
    Validated,
}

/// Generation-scoped monotonic authority lease.
///
/// Only [`AuthorityLease::renew_after_successful_remote_validation`] extends the deadline of an
/// already-valid lease. [`AuthorityLease::install_validated_generation`] installs a fresh lease for
/// a new/replacement session. Local stored `connection_expires_at` may only shorten the deadline.
#[derive(Debug, Clone)]
pub struct AuthorityLease {
    generation: DelegatedGeneration,
    deadline: Instant,
    phase: AuthorityLeasePhase,
}

impl AuthorityLease {
    /// Placeholder lease (generation 0, already expired) until a session is validated.
    pub fn inactive() -> Self {
        Self {
            generation: 0,
            deadline: Instant::now(),
            phase: AuthorityLeasePhase::Inactive,
        }
    }

    /// Bootstrap after process restart with persisted delegated state: one HTTP-budget window to
    /// revalidate. Does not grant a fresh five-minute lease or platform activation.
    pub fn pending_remote_validation(generation: DelegatedGeneration) -> Self {
        Self {
            generation,
            deadline: Instant::now() + SYNDICATE_HTTP_TIMEOUT,
            phase: AuthorityLeasePhase::PendingValidation,
        }
    }

    pub fn generation(&self) -> DelegatedGeneration {
        self.generation
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn phase(&self) -> AuthorityLeasePhase {
        self.phase
    }

    pub fn snapshot(&self) -> AuthorityLeaseSnapshot {
        AuthorityLeaseSnapshot {
            generation: self.generation,
            deadline: self.deadline(),
        }
    }

    /// True when this lease is bound to `generation` and the absolute deadline has not passed.
    pub fn owns_generation(&self, generation: DelegatedGeneration) -> bool {
        generation != 0 && self.generation == generation && !self.is_expired()
    }

    /// Platform HTTP/IRC/Kick operations require a validated lease for the generation.
    pub fn allows_platform_operations(&self, generation: DelegatedGeneration) -> bool {
        self.phase == AuthorityLeasePhase::Validated && self.owns_generation(generation)
    }

    /// Syndicate refresh/watch/SSE may run under pending or validated leases.
    pub fn allows_syndicate_revalidation(&self, generation: DelegatedGeneration) -> bool {
        matches!(
            self.phase,
            AuthorityLeasePhase::PendingValidation | AuthorityLeasePhase::Validated
        ) && self.owns_generation(generation)
    }

    /// Reset the lease after a successful remote Syndicate validation for this generation.
    ///
    /// Rejects an already-expired lease (late success cannot resurrect authority) and rejects
    /// generation mismatches when a non-zero generation is already bound.
    pub fn renew_after_successful_remote_validation(
        &mut self,
        generation: DelegatedGeneration,
        connection_expires_at: Option<&str>,
    ) -> Result<(), String> {
        if generation == 0 {
            return Err("cannot renew lease for generation 0".into());
        }
        if self.is_expired() {
            return Err("lease already expired; cannot resurrect".into());
        }
        if self.generation != 0 && self.generation != generation {
            return Err(format!(
                "lease generation mismatch: bound {} != {}",
                self.generation, generation
            ));
        }
        self.generation = generation;
        self.deadline = Instant::now() + MAX_DELEGATED_REVOCATION_DELAY;
        self.phase = AuthorityLeasePhase::Validated;
        self.cap_by_connection_expiry(connection_expires_at);
        Ok(())
    }

    /// Install a freshly validated generation (new session apply / legitimate replacement).
    ///
    /// Always binds `generation` and a fresh [`MAX_DELEGATED_REVOCATION_DELAY`] deadline, then
    /// caps by connection expiry. Does not check prior expiry (unlike renew).
    pub fn install_validated_generation(
        &mut self,
        generation: DelegatedGeneration,
        connection_expires_at: Option<&str>,
    ) {
        self.generation = generation;
        self.deadline = Instant::now() + MAX_DELEGATED_REVOCATION_DELAY;
        self.phase = AuthorityLeasePhase::Validated;
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

    /// Bound a connect/request/read timeout to the remaining lease budget.
    pub fn request_timeout(&self, configured: Duration) -> Duration {
        configured.min(self.remaining())
    }

    #[cfg(test)]
    pub fn set_deadline_for_test(&mut self, deadline: Instant) {
        self.deadline = deadline;
    }
}

/// Owns a worker handle and aborts it on drop unless disarmed.
pub struct AbortOnDrop(Option<JoinHandle<()>>);

impl AbortOnDrop {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self(Some(handle))
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
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

/// True when the slot holds a live worker for `generation`.
pub fn generation_task_alive(task: &GenerationTask, generation: DelegatedGeneration) -> bool {
    task.generation == generation && !task.handle.is_finished()
}

/// Drop a completed/panicked worker for `generation` so a restart can install.
pub async fn clear_finished_generation_task(
    slot: &RwLock<Option<GenerationTask>>,
    generation: DelegatedGeneration,
) {
    let mut guard = slot.write().await;
    if guard
        .as_ref()
        .is_some_and(|t| t.generation == generation && t.handle.is_finished())
    {
        guard.take();
    }
}

/// Release the slot if it still belongs to `generation` (worker self-clear on exit).
/// Does not abort the handle — the caller is the exiting task.
pub async fn release_generation_slot_if_owned(
    slot: &RwLock<Option<GenerationTask>>,
    generation: DelegatedGeneration,
) {
    let mut guard = slot.write().await;
    if guard.as_ref().is_some_and(|t| t.generation == generation) {
        let _ = guard.take();
    }
}

/// Atomically install a generation-tagged worker.
///
/// Returns `true` if `handle` was installed. If the slot already holds a newer generation, or an
/// equal generation with a live worker, aborts the incoming handle, keeps the current occupant,
/// and returns `false`. A finished equal-generation occupant may be replaced (liveness restart).
pub async fn install_generation_task(
    slot: &RwLock<Option<GenerationTask>>,
    generation: DelegatedGeneration,
    handle: JoinHandle<()>,
) -> bool {
    let mut guard = slot.write().await;
    if let Some(prev) = guard.as_ref() {
        if prev.generation > generation {
            handle.abort();
            return false;
        }
        if prev.generation == generation && !prev.handle.is_finished() {
            handle.abort();
            return false;
        }
    }
    if let Some(prev) = guard.take() {
        if !prev.handle.is_finished() {
            prev.handle.abort();
        }
    }
    *guard = Some(GenerationTask { generation, handle });
    true
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

    #[tokio::test]
    async fn same_generation_install_does_not_cancel_running_teardown() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();
        let (enter_tx, enter_rx) = oneshot_pair();
        let (release_tx, release_rx) = oneshot_pair();
        let c1 = coord.clone();
        let owner = tokio::spawn(async move {
            c1.run_or_join(1, || async {
                let _ = enter_tx.send(());
                let _ = release_rx.await;
                Ok(())
            })
            .await
        });
        enter_rx.await.unwrap();
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Running);

        // Concurrent same-gen install while Running must be a no-op.
        coord.install_generation_async(1).await.unwrap();
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Running);

        let c2 = coord.clone();
        let joiner = tokio::spawn(async move { c2.run_or_join(1, || async { Ok(()) }).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let _ = release_tx.send(());
        assert!(owner.await.unwrap().is_ok());
        assert!(joiner.await.unwrap().is_ok());
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Completed);
    }

    #[tokio::test]
    async fn stale_owner_completion_cannot_complete_replacement_attempt() {
        let coord = Arc::new(TeardownCoordinator::new());
        coord.install_generation_async(1).await.unwrap();

        // Attempt 1 fails → FailedRetryable.
        assert_eq!(
            coord
                .run_or_join(1, || async { Err("attempt1".into()) })
                .await,
            Err("attempt1".into())
        );
        assert!(matches!(
            coord.phase_for(1).await,
            TeardownPhase::FailedRetryable(_)
        ));
        let attempt1 = coord.attempt_for_test();
        assert!(attempt1 >= 1);

        let (enter_tx, enter_rx) = oneshot_pair();
        let (release_tx, release_rx) = oneshot_pair();
        let work_started = Arc::new(AtomicU64::new(0));
        let c2 = coord.clone();
        let started = work_started.clone();
        let owner2 = tokio::spawn(async move {
            c2.run_or_join(1, || async {
                started.fetch_add(1, Ordering::SeqCst);
                let _ = enter_tx.send(());
                let _ = release_rx.await;
                Err("attempt2".into())
            })
            .await
        });
        enter_rx.await.unwrap();
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Running);
        let attempt2 = coord.attempt_for_test();
        assert!(attempt2 > attempt1);

        // Same-gen reinstall while Running is a no-op.
        coord.install_generation_async(1).await.unwrap();
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Running);
        assert_eq!(coord.attempt_for_test(), attempt2);

        // Delayed attempt-1 completion must not mark Completed over attempt 2.
        assert!(!coord.apply_owner_completion_for_test(1, attempt1, Ok(())));
        assert_eq!(coord.phase_for(1).await, TeardownPhase::Running);

        let c3 = coord.clone();
        let started_j = work_started.clone();
        let joiner = tokio::spawn(async move {
            c3.run_or_join(1, || async {
                started_j.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let _ = release_tx.send(());
        assert_eq!(owner2.await.unwrap(), Err("attempt2".into()));
        assert_eq!(joiner.await.unwrap(), Err("attempt2".into()));
        assert_eq!(work_started.load(Ordering::SeqCst), 1);
        assert!(matches!(
            coord.phase_for(1).await,
            TeardownPhase::FailedRetryable(_)
        ));
    }

    #[tokio::test]
    async fn stale_generation_task_cannot_replace_newer() {
        let slot: RwLock<Option<GenerationTask>> = RwLock::new(None);
        let newer = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(install_generation_task(&slot, 2, newer).await);
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));

        let stale = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(!install_generation_task(&slot, 1, stale).await);
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));

        // Equal generation with a live worker is rejected (keep current).
        let equal = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(!install_generation_task(&slot, 2, equal).await);
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));

        // Finished equal-generation occupant may be replaced for liveness restart.
        if let Some(task) = slot.write().await.take() {
            task.handle.abort();
            let _ = task.handle.await;
        };
        let finished = tokio::spawn(async {});
        assert!(install_generation_task(&slot, 2, finished).await);
        for _ in 0..20 {
            if slot
                .read()
                .await
                .as_ref()
                .is_some_and(|t| t.handle.is_finished())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        clear_finished_generation_task(&slot, 2).await;
        assert!(slot.read().await.is_none());

        let restart = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(install_generation_task(&slot, 2, restart).await);
        if let Some(task) = slot.write().await.take() {
            task.handle.abort();
        };
    }

    #[test]
    fn authority_lease_backoff_cannot_extend_deadline() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease.set_deadline_for_test(Instant::now() + Duration::from_millis(50));
        let budget = lease.sleep_budget(Duration::from_secs(60));
        assert!(budget <= Duration::from_millis(50));
        let remaining = lease.remaining();
        let timeout = lease.request_timeout(Duration::from_secs(30));
        assert!(timeout <= remaining);
        assert!(timeout <= Duration::from_secs(30));
        assert!(lease.deadline() <= Instant::now() + Duration::from_millis(50));
    }

    #[test]
    fn local_connection_expiry_cannot_extend_lease() {
        let mut lease = AuthorityLease {
            generation: 1,
            deadline: Instant::now() + Duration::from_secs(10),
            phase: AuthorityLeasePhase::Validated,
        };
        let far = (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339();
        lease.cap_by_connection_expiry(Some(&far));
        assert!(lease.remaining() <= Duration::from_secs(10) + Duration::from_millis(50));
        // renew without matching generation is rejected when generation already set differently
        assert!(lease
            .renew_after_successful_remote_validation(2, None)
            .is_err());
        assert!(lease.remaining() <= Duration::from_secs(10) + Duration::from_millis(50));
        lease
            .renew_after_successful_remote_validation(1, None)
            .unwrap();
        assert!(lease.remaining() > Duration::from_secs(200));
    }

    #[test]
    fn expired_renew_is_rejected() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease.set_deadline_for_test(Instant::now() - Duration::from_secs(1));
        assert!(lease.is_expired());
        assert!(lease
            .renew_after_successful_remote_validation(1, None)
            .is_err());
        assert!(lease.is_expired());
    }

    #[test]
    fn late_success_cannot_resurrect_expired_lease() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease
            .renew_after_successful_remote_validation(1, None)
            .unwrap();
        assert!(!lease.is_expired());
        lease.set_deadline_for_test(Instant::now() - Duration::from_millis(5));
        assert!(lease.is_expired());
        assert!(lease
            .renew_after_successful_remote_validation(1, None)
            .is_err());
        assert!(lease.is_expired());
        assert!(lease.remaining().is_zero());
    }

    #[test]
    fn install_validated_generation_switches_generation() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease
            .renew_after_successful_remote_validation(1, None)
            .unwrap();
        assert_eq!(lease.generation(), 1);
        // Renew still rejects gen 2 while gen 1 is bound.
        assert!(lease
            .renew_after_successful_remote_validation(2, None)
            .is_err());
        assert_eq!(lease.generation(), 1);
        lease.install_validated_generation(2, None);
        assert_eq!(lease.generation(), 2);
        assert!(lease.remaining() > Duration::from_secs(200));
        // install_validated_generation also works after expiry (replacement session).
        lease.set_deadline_for_test(Instant::now() - Duration::from_secs(1));
        assert!(lease.is_expired());
        lease.install_validated_generation(3, None);
        assert_eq!(lease.generation(), 3);
        assert!(!lease.is_expired());
    }

    #[test]
    fn restart_lease_does_not_grant_full_window() {
        let lease = AuthorityLease::pending_remote_validation(7);
        assert_eq!(lease.phase(), AuthorityLeasePhase::PendingValidation);
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

    #[tokio::test]
    async fn stale_starter_cannot_pre_abort_newer_worker() {
        let slot: RwLock<Option<GenerationTask>> = RwLock::new(None);
        let (b_started_tx, b_started_rx) = tokio::sync::oneshot::channel();
        let (b_grant_tx, b_grant_rx) = tokio::sync::oneshot::channel();
        let b_handle = tokio::spawn(async move {
            if b_grant_rx.await.is_err() {
                return;
            }
            let _ = b_started_tx.send(());
            std::future::pending::<()>().await;
        });
        assert!(install_generation_task(&slot, 2, b_handle).await);
        let _ = b_grant_tx.send(());
        b_started_rx.await.expect("gen-2 worker must run");

        let (a_ran_tx, mut a_ran_rx) = tokio::sync::oneshot::channel();
        let (a_grant_tx, a_grant_rx) = tokio::sync::oneshot::channel();
        let a_handle = tokio::spawn(async move {
            if a_grant_rx.await.is_err() {
                return;
            }
            let _ = a_ran_tx.send(());
        });
        assert!(!install_generation_task(&slot, 1, a_handle).await);
        assert!(a_ran_rx.try_recv().is_err(), "stale gen must not run body");
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));
        let _ = a_grant_tx.send(());
    }

    #[tokio::test]
    async fn delayed_stale_starter_body_never_runs_after_newer_install() {
        let slot: RwLock<Option<GenerationTask>> = RwLock::new(None);
        let body_ops = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Pause A before it can claim.
        let (a_release_tx, a_release_rx) = tokio::sync::oneshot::channel();
        let (a_grant_tx, a_grant_rx) = tokio::sync::oneshot::channel();
        let ops_a = body_ops.clone();
        let a_handle = tokio::spawn(async move {
            let _ = a_release_rx.await;
            if a_grant_rx.await.is_err() {
                return;
            }
            ops_a.fetch_add(1, Ordering::SeqCst);
        });

        // Install B first (as if it won the race).
        let (b_grant_tx, b_grant_rx) = tokio::sync::oneshot::channel();
        let b_handle = tokio::spawn(async move {
            if b_grant_rx.await.is_err() {
                return;
            }
            std::future::pending::<()>().await;
        });
        assert!(install_generation_task(&slot, 2, b_handle).await);
        let _ = b_grant_tx.send(());

        // Release A: it tries install and must be rejected before body.
        assert!(!install_generation_task(&slot, 1, a_handle).await);
        let _ = a_release_tx.send(());
        let _ = a_grant_tx.send(());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(body_ops.load(Ordering::SeqCst), 0);
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));
        if let Some(task) = slot.write().await.take() {
            task.handle.abort();
        };
    }

    #[tokio::test]
    async fn release_own_slot_does_not_clear_newer_generation() {
        let slot: RwLock<Option<GenerationTask>> = RwLock::new(None);
        let older = tokio::spawn(async {});
        assert!(install_generation_task(&slot, 1, older).await);
        for _ in 0..20 {
            if slot
                .read()
                .await
                .as_ref()
                .is_some_and(|t| t.handle.is_finished())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let newer = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(install_generation_task(&slot, 2, newer).await);
        release_generation_slot_if_owned(&slot, 1).await;
        assert_eq!(slot.read().await.as_ref().map(|t| t.generation), Some(2));
        if let Some(task) = slot.write().await.take() {
            task.handle.abort();
        };
    }

    #[test]
    fn generation_bound_lease_snapshot_rejects_mismatch() {
        let mut lease = AuthorityLease::pending_remote_validation(1);
        lease
            .renew_after_successful_remote_validation(1, None)
            .unwrap();
        let snap = lease.snapshot();
        assert_eq!(snap.generation, 1);
        lease.install_validated_generation(2, None);
        assert_ne!(lease.snapshot().generation, snap.generation);
        assert!(!lease.owns_generation(1));
        assert!(lease.owns_generation(2));
    }

    fn oneshot_pair() -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        tokio::sync::oneshot::channel()
    }
}
