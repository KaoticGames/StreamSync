//! Observational progress events for In-Sync UI and future workshop layer.

use serde::Serialize;

/// Shared retention cap for orchestrator progress and `/api/in-sync/status` sinks.
pub const IN_SYNC_PROGRESS_EVENT_RETENTION_CAP: usize = 128;

#[derive(Debug, Clone, Default, Serialize)]
#[serde(transparent)]
pub struct BoundedProgressEvents(Vec<LocalFinalizationProgressEvent>);

impl BoundedProgressEvents {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&mut self, event: LocalFinalizationProgressEvent) {
        self.0.push(event);
        self.trim_to_cap();
    }

    pub fn extend(&mut self, events: impl IntoIterator<Item = LocalFinalizationProgressEvent>) {
        self.0.extend(events);
        self.trim_to_cap();
    }

    pub fn snapshot(&self) -> Vec<LocalFinalizationProgressEvent> {
        self.0.clone()
    }

    pub fn drain(&mut self) -> Vec<LocalFinalizationProgressEvent> {
        let out = self.0.clone();
        self.0.clear();
        out
    }

    fn trim_to_cap(&mut self) {
        let excess = self
            .0
            .len()
            .saturating_sub(IN_SYNC_PROGRESS_EVENT_RETENTION_CAP);
        if excess > 0 {
            self.0.drain(0..excess);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
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
    Cancelled,
    FailedRetryable,
    /// Storage/configuration preflight failed; requires user correction and explicit Process now.
    FailedBlocked,
    Quarantined,
}
