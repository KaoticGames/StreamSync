//! Job-scoped cancellation for an in-flight local finalization run.
//!
//! Call [`LocalFinalizationCancel::cancel`] from [`crate::in_sync::request_cancel`] (HTTP) only.
//! Reset via [`LocalFinalizationCancel::reset`] when the user explicitly enqueues Process Now —
//! never inside automatic supervisor retries. In-Sync discovery/processing is keyed to the Discord
//! host `sdk_` token and device binding, not Twitch delegated-session lifecycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct LocalFinalizationCancel {
    inner: Arc<AtomicBool>,
}

impl LocalFinalizationCancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        self.inner.store(false, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }

    pub fn check(&self) -> Result<(), LocalFinalizationCancelled> {
        if self.is_cancelled() {
            Err(LocalFinalizationCancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalFinalizationCancelled;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_is_resettable_for_a_new_job() {
        let gate = LocalFinalizationCancel::new();
        assert!(gate.check().is_ok());
        gate.cancel();
        assert!(gate.is_cancelled());
        assert!(gate.check().is_err());
        gate.reset();
        assert!(!gate.is_cancelled());
        assert!(gate.check().is_ok());
    }
}
