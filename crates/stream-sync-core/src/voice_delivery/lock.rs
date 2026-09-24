//! Stable per-delivery cooperative lock (file never deleted on release).

use crate::voice_delivery::fs::{open_lock_file_at_root, DestRoot, FsError};
use crate::voice_delivery::ids::lock_file_relative_components;
use fs2::FileExt;
use std::fs::File;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LockError {
    #[error("delivery domain lock held by another cooperative process")]
    Busy,
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// RAII exclusive lock; dropping unlocks but **never** deletes the lock file.
pub struct DeliveryDomainLock {
    file: File,
    lock_path_display: String,
}

impl DeliveryDomainLock {
    pub fn lock_path_display(&self) -> &str {
        &self.lock_path_display
    }
}

/// Acquire the stable lock for `canonical_delivery_id` under `dest_root`.
pub fn acquire_delivery_domain_lock(
    dest_root: &DestRoot,
    canonical_delivery_id: &str,
    try_wait: bool,
) -> Result<DeliveryDomainLock, LockError> {
    let components = lock_file_relative_components(canonical_delivery_id);
    let rel: Vec<String> = components.into();
    let display = rel.join("/");
    let file = open_lock_file_at_root(dest_root, &rel)?;
    if try_wait {
        match file.try_lock_exclusive() {
            Ok(()) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::AlreadyExists =>
            {
                return Err(LockError::Busy);
            }
            Err(e) => return Err(LockError::Io(e)),
        }
    } else {
        file.lock_exclusive()?;
    }
    Ok(DeliveryDomainLock {
        file,
        lock_path_display: display,
    })
}

impl Drop for DeliveryDomainLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod stable_delivery_lock_cross_process {
    use super::*;
    use crate::voice_delivery::subprocess_env::{LOCK_HOLDER, LOCK_READY, LOCK_TRY};
    use std::fs;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    fn test_dest_root() -> (tempfile::TempDir, DestRoot) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("open root");
        (tmp, root)
    }

    fn wait_for_ready(ready: &std::path::Path, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if ready.is_file() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("holder did not signal readiness within {:?}", timeout);
    }

    #[test]
    fn lock_file_survives_drop() {
        let (_tmp, root) = test_dest_root();
        let id = "delivery:stable:1";
        let components = lock_file_relative_components(id);
        let lock_path = _tmp
            .path()
            .join(&components[0])
            .join(&components[1])
            .join(&components[2]);
        {
            let _lock = acquire_delivery_domain_lock(&root, id, false).expect("acquire");
        }
        assert!(lock_path.is_file());
    }

    #[test]
    fn stable_delivery_lock_cross_process() {
        if std::env::var(LOCK_HOLDER).ok().as_deref() == Some("1") {
            let tmp = std::env::var("STREAMSYNC_VOICE_LOCK_TMP").expect("tmp");
            let ready = std::env::var(LOCK_READY).expect("ready");
            let root = DestRoot::open(std::path::Path::new(&tmp)).expect("root");
            let id = std::env::var("STREAMSYNC_VOICE_LOCK_ID").expect("id");
            let _lock = acquire_delivery_domain_lock(&root, &id, false).expect("holder lock");
            fs::write(&ready, b"ready\n").expect("ready signal");
            std::thread::sleep(Duration::from_secs(60));
            return;
        }
        if std::env::var(LOCK_TRY).ok().as_deref() == Some("1") {
            let tmp = std::env::var("STREAMSYNC_VOICE_LOCK_TMP").expect("tmp");
            let root = DestRoot::open(std::path::Path::new(&tmp)).expect("root");
            let id = std::env::var("STREAMSYNC_VOICE_LOCK_ID").expect("id");
            let err = acquire_delivery_domain_lock(&root, &id, true);
            assert!(matches!(err, Err(LockError::Busy)));
            return;
        }

        let (tmp, _root) = test_dest_root();
        let id = "delivery:cross-process:test";
        let ready = tmp.path().join("holder-ready.signal");
        let exe = std::env::current_exe().expect("exe");
        let test_name = "voice_delivery::lock::stable_delivery_lock_cross_process::stable_delivery_lock_cross_process";

        let mut holder = Command::new(&exe)
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(LOCK_HOLDER, "1")
            .env(LOCK_READY, &ready)
            .env("STREAMSYNC_VOICE_LOCK_TMP", tmp.path())
            .env("STREAMSYNC_VOICE_LOCK_ID", id)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn holder");

        wait_for_ready(&ready, Duration::from_secs(15));

        let try_child = Command::new(&exe)
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(LOCK_TRY, "1")
            .env("STREAMSYNC_VOICE_LOCK_TMP", tmp.path())
            .env("STREAMSYNC_VOICE_LOCK_ID", id)
            .status()
            .expect("try status");
        assert!(try_child.success());

        holder.kill().expect("kill holder");
        let _ = holder.wait();

        let root = DestRoot::open(tmp.path()).expect("reopen");
        let _lock =
            acquire_delivery_domain_lock(&root, id, true).expect("acquire after holder exit");
    }

    #[cfg(windows)]
    fn try_file_symlink(link: &std::path::Path, target: &std::path::Path) -> Result<(), String> {
        use std::os::windows::process::CommandExt;
        use std::process::Command;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let link_s = link.to_string_lossy();
        let target_s = target.to_string_lossy();
        let output = Command::new("cmd")
            .args(["/C", "mklink", &link_s, &target_s])
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .map_err(|e| format!("spawn mklink: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            Err(format!(
                "mklink failed (status {:?}): {}{}",
                output.status.code(),
                stdout,
                stderr
            ))
        }
    }

    #[test]
    #[cfg(windows)]
    fn lock_file_reparse_rejected() {
        let (tmp, root) = test_dest_root();
        let id = "delivery:reparse-lock";
        let components = lock_file_relative_components(id);
        let lock_path = tmp
            .path()
            .join(&components[0])
            .join(&components[1])
            .join(&components[2]);
        fs::create_dir_all(lock_path.parent().expect("parent")).expect("mkdirs");
        let real_target = tmp.path().join("real-lock-target");
        fs::write(&real_target, b"lock-bytes").expect("target file");
        match try_file_symlink(&lock_path, &real_target) {
            Ok(()) => {
                let err = acquire_delivery_domain_lock(&root, id, true);
                assert!(
                    matches!(
                        err,
                        Err(LockError::Fs(FsError::SymlinkOrReparseComponent(_)))
                    ),
                    "expected reparse rejection, got {:?}",
                    err
                );
            }
            Err(reason) => {
                eprintln!("SKIP lock_file_reparse_rejected: {reason}");
            }
        }
    }

    /// Documents out-of-scope malicious unlink/recreate; library never unlinks lock files.
    #[test]
    #[cfg(unix)]
    fn malicious_unlink_splits_lock_domain_documentation_only() {
        let (tmp, root) = test_dest_root();
        let id = "delivery:unlink-doc";
        let components = lock_file_relative_components(id);
        let lock_path = tmp
            .path()
            .join(&components[0])
            .join(&components[1])
            .join(&components[2]);
        let _a = acquire_delivery_domain_lock(&root, id, false).expect("lock a");
        fs::remove_file(&lock_path).expect("unlink (out of scope attack)");
        let _b = acquire_delivery_domain_lock(&root, id, false).expect("lock b after recreate");
        // Two cooperative domains possible after malicious unlink — not defended in Phase 0C.
    }
}
