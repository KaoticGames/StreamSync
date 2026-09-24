//! Filesystem errors for voice delivery (fail closed).

use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("unsupported platform or filesystem primitive")]
    Unsupported,
    #[error("invalid path component: {0}")]
    InvalidComponent(String),
    #[error("destination root is a symlink or reparse point")]
    SymlinkOrReparseRoot,
    #[error("path component is a symlink or reparse point: {0}")]
    SymlinkOrReparseComponent(String),
    #[error("destination already exists")]
    AlreadyExists,
    #[error("cross-parent rename is not representable")]
    CrossParentRename,
    #[error("invalid final session name: {0}")]
    InvalidFinalName(String),
    #[error("invalid stage basename")]
    InvalidStageBasename,
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl FsError {
    pub(crate) fn from_io(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::AlreadyExists {
            return FsError::AlreadyExists;
        }
        FsError::Io(err)
    }
}
