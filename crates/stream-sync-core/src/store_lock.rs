//! Cross-process and in-process store locking (safe, no lifetime transmute).

use anyhow::Result;
use fs2::FileExt;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

static IN_PROCESS_STORE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

fn in_process_lock(lock_path: &Path) -> Arc<Mutex<()>> {
    let table = IN_PROCESS_STORE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = table.lock().expect("in-process lock table");
    guard
        .entry(lock_path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Run `f` while holding both the in-process per-path mutex and an OS advisory lock.
///
/// Guard lifetimes stay lexical — no transmute or self-referential structs.
pub fn with_cross_process_lock<R>(lock_path: &Path, f: impl FnOnce() -> Result<R>) -> Result<R> {
    let process_mtx = in_process_lock(lock_path);
    let _in_process = process_mtx.lock().expect("in-process store mutex poisoned");
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    file.lock_exclusive()?;
    let result = f();
    let _ = file.unlock();
    result
}

/// Exclusive desktop instance lock. Hold the returned file for the process lifetime.
pub fn try_acquire_instance_lock(userdata: &Path) -> Result<std::fs::File, InstanceLockError> {
    fs::create_dir_all(userdata).map_err(InstanceLockError::Io)?;
    let path = userdata.join("stream-sync.instance.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(InstanceLockError::Io)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(file),
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            Err(InstanceLockError::AlreadyHeld)
        }
        Err(e) => Err(InstanceLockError::Io(e)),
    }
}

#[derive(Debug)]
pub enum InstanceLockError {
    AlreadyHeld,
    Io(std::io::Error),
}

impl std::fmt::Display for InstanceLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceLockError::AlreadyHeld => write!(f, "Stream Sync is already running"),
            InstanceLockError::Io(e) => write!(f, "instance lock: {e}"),
        }
    }
}

impl std::error::Error for InstanceLockError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_instance_lock_is_denied_until_first_drops() {
        let dir = std::env::temp_dir().join(format!(
            "ss-instance-lock-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let first = try_acquire_instance_lock(&dir).expect("first lock");
        match try_acquire_instance_lock(&dir) {
            Err(InstanceLockError::AlreadyHeld) => {}
            other => panic!("expected AlreadyHeld, got {other:?}"),
        }
        drop(first);
        let second = try_acquire_instance_lock(&dir).expect("lock after drop");
        drop(second);
        let _ = fs::remove_dir_all(&dir);
    }
}
