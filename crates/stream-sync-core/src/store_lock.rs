//! Cross-process and in-process store locking.

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

/// Holds an in-process mutex and OS advisory lock for one store path.
pub struct CrossProcessLock {
    _process_mtx: Arc<Mutex<()>>,
    _in_process: std::sync::MutexGuard<'static, ()>,
    file: std::fs::File,
}

impl Drop for CrossProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub fn acquire_cross_process_lock(lock_path: &Path) -> Result<CrossProcessLock> {
    let process_mtx = in_process_lock(lock_path);
    let in_process_guard = process_mtx.lock().expect("in-process store mutex poisoned");
    // Safety: `_process_mtx` is stored in the same struct and outlives `_in_process`.
    let in_process_guard = unsafe {
        std::mem::transmute::<std::sync::MutexGuard<'_, ()>, std::sync::MutexGuard<'static, ()>>(
            in_process_guard,
        )
    };
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(lock_path)?;
    file.lock_exclusive()?;
    Ok(CrossProcessLock {
        _process_mtx: process_mtx,
        _in_process: in_process_guard,
        file,
    })
}
