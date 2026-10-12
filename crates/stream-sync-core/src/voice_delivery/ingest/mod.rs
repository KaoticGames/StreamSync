//! Ingest paths (resumable partial stems).

mod partial;

pub use partial::{PartialError, PartialRecoveryReportWriter, PartialStemWriter};
