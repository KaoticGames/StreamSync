//! Dark direct-local receiver durable core (slice 2, vertical 1–2).

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
pub mod wss;

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

#[cfg(test)]
#[path = "wss_test_support.rs"]
mod wss_test_support;

#[cfg(test)]
#[path = "wss_connect_tests.rs"]
mod wss_connect;

#[cfg(test)]
#[path = "wss_protocol_tests.rs"]
mod wss_protocol;

#[cfg(test)]
#[path = "wss_segment_fsm_tests.rs"]
mod wss_segment_fsm;

#[cfg(test)]
#[path = "wss_hello_resume_tests.rs"]
mod wss_hello_resume;

#[cfg(test)]
#[path = "wss_session_ack_tests.rs"]
mod wss_session_ack;

#[cfg(test)]
#[path = "wss_session_lifecycle_tests.rs"]
mod wss_session_lifecycle;

#[cfg(test)]
#[path = "wss_session_time_bounds_tests.rs"]
mod wss_session_time_bounds;

#[cfg(test)]
#[path = "wss_dark_policy_tests.rs"]
mod wss_dark_policy;
