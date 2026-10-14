//! Subscription-expiry / authority cancellation seam for local finalization work.

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
