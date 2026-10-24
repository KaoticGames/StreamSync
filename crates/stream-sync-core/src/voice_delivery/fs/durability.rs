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
    #[cfg(test)]
    if let Some(err) = fail_next_sync_dir_exact::take_armed_failure() {
        return Err(err);
    }

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

/// Test-only: fail the next `sync_dir_exact` on the installing thread (crate unit tests).
#[cfg(test)]
pub(crate) mod fail_next_sync_dir_exact {
    use super::FsError;
    use std::cell::RefCell;
    use std::io;

    /// Stable synthetic error returned when the armed injection fires (test-only).
    pub(crate) const INJECTED_MESSAGE: &str = "injected fail-next sync_dir_exact (test only)";

    thread_local! {
        static ARMED: RefCell<bool> = const { RefCell::new(false) };
    }

    fn synthetic_fs_error() -> FsError {
        FsError::Io(io::Error::other(INJECTED_MESSAGE))
    }

    pub(crate) fn take_armed_failure() -> Option<FsError> {
        ARMED.with(|cell| {
            if *cell.borrow() {
                *cell.borrow_mut() = false;
                Some(synthetic_fs_error())
            } else {
                None
            }
        })
    }

    /// RAII guard: disarms any still-pending injection when dropped (panic-safe).
    pub(crate) struct FailNextSyncDirExactGuard;

    impl Drop for FailNextSyncDirExactGuard {
        fn drop(&mut self) {
            ARMED.with(|cell| {
                *cell.borrow_mut() = false;
            });
        }
    }

    pub(crate) fn install_fail_next_sync_dir_exact() -> FailNextSyncDirExactGuard {
        ARMED.with(|cell| {
            if *cell.borrow() {
                panic!("duplicate fail-next sync_dir_exact injection on the current thread");
            }
            *cell.borrow_mut() = true;
        });
        FailNextSyncDirExactGuard
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

    #[test]
    fn fail_next_sync_dir_exact_consumes_once_then_succeeds() {
        use super::fail_next_sync_dir_exact::{install_fail_next_sync_dir_exact, INJECTED_MESSAGE};

        let (_tmp, parent) = temp_final_parent();
        let _guard = install_fail_next_sync_dir_exact();
        let err = sync_dir_exact(&parent).expect_err("injected failure");
        assert!(
            matches!(&err, FsError::Io(io) if io.to_string().contains(INJECTED_MESSAGE)),
            "unexpected injected error: {err:?}"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            sync_dir_exact(&parent).expect("second sync after consume"),
            NamespaceDurability::Proven
        );
    }

    #[test]
    #[should_panic = "duplicate fail-next sync_dir_exact injection on the current thread"]
    fn fail_next_sync_dir_exact_rejects_duplicate_install() {
        use super::fail_next_sync_dir_exact::install_fail_next_sync_dir_exact;

        let _first = install_fail_next_sync_dir_exact();
        let _second = install_fail_next_sync_dir_exact();
    }
}
