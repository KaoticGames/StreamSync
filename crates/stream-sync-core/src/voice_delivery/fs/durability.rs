//! Durability helpers (`sync_dir_exact` targets the opened directory fd only).

use super::dir::DirHandle;
use super::error::FsError;
use super::file::VoiceFile;

/// Result of attempting POSIX-style directory namespace durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceDurability {
    /// Directory metadata durability was proven (e.g. Linux `fsync` on the dir fd).
    Proven,
    /// Platform does not expose an equivalent guarantee for this handle.
    Unavailable,
}

/// `fsync` / flush the supplied open file handle only.
pub fn sync_file(file: &VoiceFile) -> Result<(), FsError> {
    file.std_file().sync_all().map_err(FsError::from)
}

/// `fsync` the supplied directory handle on Linux; Windows reports `Unavailable` (no faux dir flush).
pub fn sync_dir_exact(dir: &DirHandle) -> Result<NamespaceDurability, FsError> {
    #[cfg(target_os = "linux")]
    {
        rustix::fs::fsync(dir.as_fd()).map_err(|e| FsError::Io(e.into()))?;
        Ok(NamespaceDurability::Proven)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = dir;
        Ok(NamespaceDurability::Unavailable)
    }
    #[cfg(windows)]
    {
        let _ = dir;
        Ok(NamespaceDurability::Unavailable)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = dir;
        Err(FsError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice_delivery::fs::{DestRoot, DirHandle};
    use std::fs;

    fn temp_final_parent() -> (tempfile::TempDir, DirHandle) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_path = tmp.path().join("final-parent");
        fs::create_dir_all(&parent_path).expect("mkdir");
        let root = DestRoot::open(tmp.path()).expect("open root");
        let parent = root
            .open_dir_relative(&["final-parent"])
            .expect("open parent");
        (tmp, parent)
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn sync_dir_exact_proven_on_linux() {
        let (_tmp, parent) = temp_final_parent();
        assert_eq!(
            sync_dir_exact(&parent).expect("sync"),
            NamespaceDurability::Proven
        );
    }

    #[test]
    #[cfg(windows)]
    fn sync_dir_exact_unavailable_on_windows() {
        let (_tmp, parent) = temp_final_parent();
        assert_eq!(
            sync_dir_exact(&parent).expect("sync"),
            NamespaceDurability::Unavailable
        );
    }
}
