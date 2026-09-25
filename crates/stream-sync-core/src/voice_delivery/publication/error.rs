//! Publication and recovery errors (fail closed).

use crate::voice_delivery::fs::FsError;
use crate::voice_delivery::marker::MarkerError;
use crate::voice_delivery::records::ledger_generation::LedgerGenerationError;
use crate::voice_delivery::session::SessionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PublicationError {
    #[error("publication requires durable PublishIntent ledger state")]
    NotPublishIntent,
    #[error("recovery cannot publish while receiving")]
    ReceivingNoPublish,
    #[error("destination already exists")]
    DestinationExists,
    #[error("unrelated pre-existing destination")]
    UnrelatedDestination,
    #[error("ambiguous publication layout")]
    AmbiguousPublication,
    #[error("missing publication artifacts")]
    MissingPublication,
    #[error("publication child is not a directory: {role}")]
    OtherArtifact { role: &'static str },
    #[error("rename failed with stage intact: {detail}")]
    RenameFailed { detail: String },
    #[error("rename outcome indeterminate: {detail}")]
    RenameIndeterminate { detail: String },
    #[error("published final directory invalid")]
    PublishedFinalInvalid,
    #[error("terminal quarantined state")]
    Quarantined,
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Marker(#[from] MarkerError),
    #[error(transparent)]
    Ledger(#[from] LedgerGenerationError),
    #[error(transparent)]
    Fs(#[from] FsError),
}
