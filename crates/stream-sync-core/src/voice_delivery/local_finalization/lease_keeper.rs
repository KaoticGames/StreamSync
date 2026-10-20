//! Time-based lease renewal for multi-hour local finalization work.

use super::client::LocalFinalizationClient;
use super::contract::LeaseProof;
use crate::voice_delivery::client::VoiceV2ClientError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MIN_RENEW_INTERVAL: Duration = Duration::from_secs(15);
const RENEW_LEAD_MS: i64 = 120_000;

pub struct LeaseKeeper {
    stop: Arc<AtomicBool>,
    renew_failed: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LeaseKeeper {
    pub fn spawn<C: LocalFinalizationClient + Clone + 'static>(
        client: C,
        session_id: String,
        device_id: String,
        proof: LeaseProof,
        expires_at_ms: i64,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let renew_failed = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let renew_failed_thread = renew_failed.clone();
        let handle = thread::spawn(move || {
            let proof = proof;
            let mut expires_at_ms = expires_at_ms;
            while !stop_thread.load(Ordering::SeqCst) {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let renew_at = expires_at_ms - RENEW_LEAD_MS;
                let sleep_ms = if renew_at > now_ms {
                    (renew_at - now_ms) as u64
                } else {
                    0
                };
                let sleep_for = Duration::from_millis(sleep_ms).max(MIN_RENEW_INTERVAL);
                let deadline = Instant::now() + sleep_for;
                while Instant::now() < deadline {
                    if stop_thread.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(200));
                }
                if stop_thread.load(Ordering::SeqCst) {
                    return;
                }
                match client.renew_lease(&session_id, &device_id, &proof) {
                    Ok(next_expires) => {
                        if let Some(ms) = next_expires {
                            expires_at_ms = ms;
                        } else {
                            expires_at_ms =
                                chrono::Utc::now().timestamp_millis() + RENEW_LEAD_MS * 2;
                        }
                    }
                    Err(VoiceV2ClientError::RetryAfter(wait)) => {
                        thread::sleep(wait);
                    }
                    Err(_) => {
                        renew_failed_thread.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            }
        });
        Self {
            stop,
            renew_failed,
            handle: Some(handle),
        }
    }

    pub fn renew_failed(&self) -> bool {
        self.renew_failed.load(Ordering::SeqCst)
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        self.stop();
    }
}
