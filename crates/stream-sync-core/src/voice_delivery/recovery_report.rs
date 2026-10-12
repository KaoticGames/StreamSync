//! Optional Syndicate audio recovery report (plain-text sidecar beside WAV stems).

use crate::voice_delivery::finalized_manifest::SyndicateRecoveryReport;
use crate::voice_delivery::fs::VoiceFile;
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::marker::MarkerError;
use thiserror::Error;

pub const RECOVERY_REPORT_RELATIVE_PATH: &str = "Syndicate Audio Recovery Report.txt";
pub const RECOVERY_REPORT_CONTENT_TYPE: &str = "text/plain";
/// Upper bound for recovery report payload (plain text).
pub const RECOVERY_REPORT_MAX_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReportSpec {
    pub file_name: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReportIdentityBinding {
    pub file_name: String,
    pub byte_count: u64,
    pub sha256: String,
}

impl RecoveryReportSpec {
    pub fn from_syndicate(report: &SyndicateRecoveryReport) -> Self {
        Self {
            file_name: report.relative_path.clone(),
            byte_count: report.byte_length,
            sha256: report.sha256.clone(),
        }
    }

    pub fn identity_binding(&self) -> RecoveryReportIdentityBinding {
        RecoveryReportIdentityBinding {
            file_name: self.file_name.clone(),
            byte_count: self.byte_count,
            sha256: self.sha256.clone(),
        }
    }
}

pub fn recovery_report_spec_from_manifest(
    report: Option<&SyndicateRecoveryReport>,
) -> Option<RecoveryReportSpec> {
    report.map(RecoveryReportSpec::from_syndicate)
}

pub fn verify_recovery_report_from_handle(
    file: &VoiceFile,
    expected: &RecoveryReportSpec,
) -> Result<(), MarkerError> {
    let len = file.len()?;
    if len != expected.byte_count {
        return Err(MarkerError::StemVerification(format!(
            "recovery report length {} != {}",
            len, expected.byte_count
        )));
    }
    let mut reader = file.std_file();
    std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))
        .map_err(|e| MarkerError::StemVerification(e.to_string()))?;
    let digest = sha256_hex_reader(reader, DEFAULT_STREAM_CHUNK)
        .map_err(|e| MarkerError::StemVerification(e.to_string()))?;
    if digest != expected.sha256 {
        return Err(MarkerError::StemVerification(
            "recovery report sha256 mismatch".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RecoveryReportManifestError {
    #[error("invalid recovery report: {0}")]
    Invalid(String),
}

pub fn validate_recovery_report_byte_length(
    byte_length: u64,
) -> Result<(), RecoveryReportManifestError> {
    if byte_length == 0 {
        return Err(RecoveryReportManifestError::Invalid(
            "byteLength must be positive".into(),
        ));
    }
    if byte_length > RECOVERY_REPORT_MAX_BYTES {
        return Err(RecoveryReportManifestError::Invalid(
            "byteLength exceeds maximum".into(),
        ));
    }
    Ok(())
}
