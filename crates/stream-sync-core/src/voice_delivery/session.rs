//! Proof of held delivery-domain lock for all mutating voice_delivery operations.

use crate::voice_delivery::lock::DeliveryDomainLock;

/// All ingest, ledger, checkpoint, and seal mutations require this guard.
pub struct DeliverySessionGuard {
    _lock: DeliveryDomainLock,
}

impl DeliverySessionGuard {
    pub fn new(lock: DeliveryDomainLock) -> Self {
        Self { _lock: lock }
    }
}
