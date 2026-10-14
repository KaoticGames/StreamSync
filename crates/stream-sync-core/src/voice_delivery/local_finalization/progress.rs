//! Observational progress events for In-Sync UI and future workshop layer.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LocalFinalizationProgressEvent {
    JournalReady {
        session_id: String,
    },
    DownloadProgress {
        session_id: String,
        segment_seq: u32,
        bytes_complete: u64,
        bytes_total: u64,
    },
    SegmentVerified {
        session_id: String,
        segment_seq: u32,
    },
    ProcessingStarted {
        session_id: String,
    },
    TimelineProgress {
        session_id: String,
        processed_segments: u32,
        total_segments: u32,
    },
    PacketRejected {
        session_id: String,
        count: u64,
    },
    SilenceInserted {
        session_id: String,
        ranges: u64,
    },
    StemCompleted {
        session_id: String,
        path_nick: String,
    },
    ReportWritten {
        session_id: String,
        byte_length: u64,
    },
    PublicationComplete {
        session_id: String,
        final_session_dir: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        recovery_report_path: Option<String>,
    },
    ReceiptAcknowledged {
        session_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalFinalizationPhase {
    Queued,
    Claiming,
    ManifestVerifying,
    Downloading,
    ReadyToProcess,
    Processing,
    Paused,
    Publishing,
    Receipting,
    Complete,
    FailedRetryable,
    Quarantined,
}
