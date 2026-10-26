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
    if let Some(outcome) = sync_dir_exact_test_injection::take_injected_outcome(dir) {
        return outcome;
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

/// Test-only: ordered or consume-once `sync_dir_exact` outcome injection on the installing thread.
#[cfg(test)]
pub(crate) mod sync_dir_exact_test_injection {
    use super::{DirHandle, FsError, NamespaceDurability};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io;

    /// Stable synthetic error returned when the armed Io injection fires (test-only).
    pub(crate) const INJECTED_MESSAGE: &str = "injected fail-next sync_dir_exact (test only)";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ArmedOutcome {
        Io,
        Unavailable,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum QueuedOutcome {
        PassThrough,
        Io,
        Unavailable,
    }

    thread_local! {
        static ARMED: RefCell<Option<ArmedOutcome>> = const { RefCell::new(None) };
        static QUEUE: RefCell<Option<VecDeque<QueuedOutcome>>> = const { RefCell::new(None) };
    }

    fn synthetic_fs_error() -> FsError {
        FsError::Io(io::Error::other(INJECTED_MESSAGE))
    }

    fn assert_no_conflicting_injection() {
        ARMED.with(|armed| {
            QUEUE.with(|queue| {
                if armed.borrow().is_some() || queue.borrow().is_some() {
                    panic!(
                        "duplicate or conflicting sync_dir_exact injection on the current thread"
                    );
                }
            });
        });
    }

    pub(crate) fn take_injected_outcome(
        _dir: &DirHandle,
    ) -> Option<Result<NamespaceDurability, FsError>> {
        let queue_outcome = QUEUE.with(|queue_cell| {
            let mut queue_slot = queue_cell.borrow_mut();
            if let Some(queue) = queue_slot.as_mut() {
                match queue.pop_front() {
                    None => panic!(
                        "sync_dir_exact called more times than queued bootstrap durability outcomes"
                    ),
                    Some(QueuedOutcome::PassThrough) => return Some(None),
                    Some(QueuedOutcome::Io) => {
                        return Some(Some(Err(synthetic_fs_error())));
                    }
                    Some(QueuedOutcome::Unavailable) => {
                        return Some(Some(Ok(NamespaceDurability::Unavailable)));
                    }
                }
            }
            None
        });
        if let Some(inner) = queue_outcome {
            return inner;
        }

        ARMED.with(|cell| {
            let mut armed = cell.borrow_mut();
            match *armed {
                Some(ArmedOutcome::Io) => {
                    *armed = None;
                    Some(Err(synthetic_fs_error()))
                }
                Some(ArmedOutcome::Unavailable) => {
                    *armed = None;
                    Some(Ok(NamespaceDurability::Unavailable))
                }
                None => None,
            }
        })
    }

    /// RAII guard: disarms any still-pending injection when dropped (panic-safe).
    pub(crate) struct FailNextSyncDirExactGuard;

    impl Drop for FailNextSyncDirExactGuard {
        fn drop(&mut self) {
            ARMED.with(|cell| {
                *cell.borrow_mut() = None;
            });
        }
    }

    /// RAII guard: clears any unconsumed queue entries when dropped (panic-safe).
    pub(crate) struct SyncDirExactOutcomeQueueGuard;

    impl Drop for SyncDirExactOutcomeQueueGuard {
        fn drop(&mut self) {
            QUEUE.with(|cell| {
                *cell.borrow_mut() = None;
            });
        }
    }

    fn install_armed(outcome: ArmedOutcome) -> FailNextSyncDirExactGuard {
        assert_no_conflicting_injection();
        ARMED.with(|cell| {
            *cell.borrow_mut() = Some(outcome);
        });
        FailNextSyncDirExactGuard
    }

    pub(crate) fn install_fail_next_sync_dir_exact() -> FailNextSyncDirExactGuard {
        install_armed(ArmedOutcome::Io)
    }

    pub(crate) fn install_next_sync_dir_exact_unavailable() -> FailNextSyncDirExactGuard {
        install_armed(ArmedOutcome::Unavailable)
    }

    pub(crate) fn install_sync_dir_exact_outcome_queue(
        outcomes: Vec<QueuedOutcome>,
    ) -> SyncDirExactOutcomeQueueGuard {
        assert_no_conflicting_injection();
        QUEUE.with(|cell| {
            *cell.borrow_mut() = Some(VecDeque::from(outcomes));
        });
        SyncDirExactOutcomeQueueGuard
    }

    pub(crate) fn queue_unconsumed_len() -> usize {
        QUEUE.with(|cell| cell.borrow().as_ref().map(|q| q.len()).unwrap_or(0))
    }
}

#[cfg(test)]
pub(crate) use sync_dir_exact_test_injection as fail_next_sync_dir_exact;

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
    #[should_panic = "duplicate or conflicting sync_dir_exact injection on the current thread"]
    fn fail_next_sync_dir_exact_rejects_duplicate_install() {
        use super::fail_next_sync_dir_exact::install_fail_next_sync_dir_exact;

        let _first = install_fail_next_sync_dir_exact();
        let _second = install_fail_next_sync_dir_exact();
    }

    #[test]
    fn next_sync_dir_exact_unavailable_injection_consumes_once() {
        use super::fail_next_sync_dir_exact::install_next_sync_dir_exact_unavailable;

        let (_tmp, parent) = temp_final_parent();
        let _guard = install_next_sync_dir_exact_unavailable();
        assert_eq!(
            sync_dir_exact(&parent).expect("injected unavailable"),
            NamespaceDurability::Unavailable
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            sync_dir_exact(&parent).expect("second sync after consume"),
            NamespaceDurability::Proven
        );
    }

    #[test]
    #[should_panic = "duplicate or conflicting sync_dir_exact injection on the current thread"]
    fn sync_dir_exact_injection_rejects_mixed_duplicate_install() {
        use super::fail_next_sync_dir_exact::{
            install_fail_next_sync_dir_exact, install_next_sync_dir_exact_unavailable,
        };

        let _first = install_fail_next_sync_dir_exact();
        let _second = install_next_sync_dir_exact_unavailable();
    }

    #[test]
    fn sync_dir_exact_outcome_queue_consumes_in_order() {
        use super::fail_next_sync_dir_exact::{
            install_sync_dir_exact_outcome_queue, queue_unconsumed_len, QueuedOutcome,
            INJECTED_MESSAGE,
        };

        let (_tmp, parent) = temp_final_parent();
        let _guard = install_sync_dir_exact_outcome_queue(vec![
            QueuedOutcome::Unavailable,
            QueuedOutcome::Io,
        ]);
        assert_eq!(
            sync_dir_exact(&parent).expect("first queued"),
            NamespaceDurability::Unavailable
        );
        let err = sync_dir_exact(&parent).expect_err("second queued");
        assert!(
            matches!(&err, FsError::Io(io) if io.to_string().contains(INJECTED_MESSAGE)),
            "unexpected: {err:?}"
        );
        assert_eq!(queue_unconsumed_len(), 0);
    }

    #[test]
    #[should_panic = "duplicate or conflicting sync_dir_exact injection on the current thread"]
    fn sync_dir_exact_outcome_queue_rejects_conflicting_fail_next() {
        use super::fail_next_sync_dir_exact::{
            install_fail_next_sync_dir_exact, install_sync_dir_exact_outcome_queue, QueuedOutcome,
        };

        let _queue = install_sync_dir_exact_outcome_queue(vec![QueuedOutcome::Io]);
        let _fail = install_fail_next_sync_dir_exact();
    }
}
