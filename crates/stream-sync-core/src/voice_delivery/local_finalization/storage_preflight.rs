//! Bounded destination storage preflight (open + free-space query).

use crate::voice_delivery::fs::{DestRoot, FsError};
use serde::Serialize;
use std::fmt;
use std::path::Path;

/// Typed storage preflight outcome (no path secrets).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoragePreflightFailure {
    RequiredBytesOverflow,
    DestinationUnsupported {
        message: String,
    },
    DestinationInaccessible {
        message: String,
    },
    FreeSpaceQueryFailed {
        message: String,
    },
    InsufficientSpace {
        required_bytes: u64,
        available_bytes: u64,
    },
}

impl fmt::Display for StoragePreflightFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequiredBytesOverflow => write!(f, "required byte estimate overflow"),
            Self::DestinationUnsupported { message } => write!(f, "{message}"),
            Self::DestinationInaccessible { message } => write!(f, "{message}"),
            Self::FreeSpaceQueryFailed { message } => write!(f, "{message}"),
            Self::InsufficientSpace {
                required_bytes,
                available_bytes,
            } => write!(
                f,
                "insufficient disk space (need {required_bytes} bytes, {available_bytes} available)"
            ),
        }
    }
}

impl StoragePreflightFailure {
    /// Deterministic configuration/qualification failures — supervisor must not auto-retry.
    pub fn is_blocked_non_retryable(&self) -> bool {
        true
    }

    pub fn required_gib(&self) -> Option<f64> {
        match self {
            Self::InsufficientSpace { required_bytes, .. } => Some(bytes_to_gib(*required_bytes)),
            _ => None,
        }
    }

    pub fn available_gib(&self) -> Option<f64> {
        match self {
            Self::InsufficientSpace {
                available_bytes, ..
            } => Some(bytes_to_gib(*available_bytes)),
            _ => None,
        }
    }
}

pub fn bytes_to_gib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

fn map_dest_open_err(e: FsError) -> StoragePreflightFailure {
    match e {
        FsError::UnsupportedStorage(msg) => {
            StoragePreflightFailure::DestinationUnsupported { message: msg }
        }
        FsError::SymlinkOrReparseRoot
        | FsError::SymlinkOrReparseComponent(_)
        | FsError::NotADirectory(_)
        | FsError::InvalidComponent(_) => StoragePreflightFailure::DestinationInaccessible {
            message: e.to_string(),
        },
        FsError::AlreadyExists => StoragePreflightFailure::DestinationInaccessible {
            message: "destination path is not a directory".into(),
        },
        FsError::Io(io) => StoragePreflightFailure::DestinationInaccessible {
            message: io.to_string(),
        },
        FsError::Unsupported => StoragePreflightFailure::DestinationUnsupported {
            message: "filesystem backend unsupported on this platform".into(),
        },
        FsError::CrossParentRename
        | FsError::InvalidFinalName(_)
        | FsError::InvalidStageBasename => StoragePreflightFailure::DestinationInaccessible {
            message: e.to_string(),
        },
    }
}

/// Open the recording parent and compare available bytes to `required` (checked at call site).
pub fn storage_preflight(
    recording_parent: &Path,
    required: u64,
) -> Result<(), StoragePreflightFailure> {
    match DestRoot::open(recording_parent) {
        Ok(_) => {}
        Err(e) => return Err(map_dest_open_err(e)),
    }

    #[cfg(windows)]
    {
        let query = stream_sync_windows_fs::disk_space::query_available_bytes(recording_parent);
        match query {
            stream_sync_windows_fs::disk_space::DiskSpaceQuery::Ok { available_bytes } => {
                if available_bytes < required {
                    return Err(StoragePreflightFailure::InsufficientSpace {
                        required_bytes: required,
                        available_bytes,
                    });
                }
            }
            stream_sync_windows_fs::disk_space::DiskSpaceQuery::Failed { os_error_code } => {
                let detail = match os_error_code {
                    Some(code) => format!("GetDiskFreeSpaceExW failed (os error {code})"),
                    None => "GetDiskFreeSpaceExW failed".into(),
                };
                return Err(StoragePreflightFailure::FreeSpaceQueryFailed { message: detail });
            }
        }
    }

    #[cfg(not(windows))]
    {
        let _ = required;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_to_gib_roundtrip_order_of_magnitude() {
        let gib = bytes_to_gib(9 * 1024 * 1024 * 1024 * 1024);
        assert!(gib > 9000.0 && gib < 9500.0);
    }
}
