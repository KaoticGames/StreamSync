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
