//! Publication execution and crash/restart recovery (Phase 0C slices 10–11).

mod error;
mod publish;
mod recover;

pub(crate) use publish::{prepare_publication, publish_prepared, PublishOptions};
pub(crate) use recover::{recover_delivery, RecoveryOutcome};

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests;
