//! Dark direct-local receiver durable core (slice 2, vertical 1).

mod checkpoint;
mod durability_pipeline;
mod generation_store;
#[cfg(test)]
mod idempotent_retry;
mod layout;
mod limits;
mod segment_identity;
mod session_binding;
mod synj_validate;

pub use checkpoint::DirectCumulativeCheckpoint;
#[cfg(test)]
pub use durability_pipeline::DirectDurabilityBoundary;
pub use durability_pipeline::{
    DirectDurabilityReport, DirectNamespaceDurabilityProbe, DirectSegmentDurabilityFailure,
    DirectSegmentDurabilityFailureKind, DirectSegmentDurabilityPipeline,
    DirectSegmentPersistOutcome,
};
pub use generation_store::{
    DirectGenerationStore, DirectGenerationStoreError, DirectSegmentFileName,
};
pub use limits::{DirectReceiverLimits, DirectReceiverLimitsError, DirectReceiverPreflight};
pub use segment_identity::{LogicalSegmentIdentity, LogicalSegmentIdentityError};
pub use session_binding::{DirectSessionBinding, DirectSessionBindingError};

#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "binding_tests.rs"]
mod binding;

#[cfg(test)]
#[path = "durability_tests.rs"]
mod durability;

#[cfg(test)]
#[path = "generation_store_tests.rs"]
mod generation_store_tests;

#[cfg(test)]
#[path = "idempotent_retry_tests.rs"]
mod idempotent_retry_tests;

#[cfg(test)]
#[path = "limits_tests.rs"]
mod limits_tests;

#[cfg(test)]
#[path = "segment_identity_tests.rs"]
mod segment_identity_tests;

#[cfg(test)]
#[path = "windows_namespace_tests.rs"]
mod windows_namespace;
