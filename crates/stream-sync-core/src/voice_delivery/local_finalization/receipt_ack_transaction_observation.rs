//! cfg(test)-only observation at `persist_receipt_ack` after install lock + canonical intent match.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{LazyLock, Mutex};

fn session_root_scope(session_root: &Path) -> PathBuf {
    let abs = std::path::absolute(session_root).unwrap_or_else(|_| session_root.to_path_buf());
    let mut normalized = PathBuf::new();
    for component in abs.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

struct Registry {
    observers: HashMap<PathBuf, Sender<()>>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
    Mutex::new(Registry {
        observers: HashMap::new(),
    })
});

pub(crate) struct ReceiptAckAfterIntentReadGuard {
    session_root: PathBuf,
}

impl Drop for ReceiptAckAfterIntentReadGuard {
    fn drop(&mut self) {
        let mut registry = REGISTRY
            .lock()
            .expect("receipt ack after-intent-read registry");
        registry.observers.remove(&self.session_root);
    }
}

/// Fires once per `persist_receipt_ack` after the install lock is held and canonical intent matches.
pub(crate) fn install_receipt_ack_after_intent_read_observation(
    session_root: &Path,
) -> (ReceiptAckAfterIntentReadGuard, Receiver<()>) {
    let scope = session_root_scope(session_root);
    let (tx, rx) = mpsc::channel();
    let mut registry = REGISTRY
        .lock()
        .expect("receipt ack after-intent-read registry");
    if registry.observers.contains_key(&scope) {
        panic!("duplicate receipt ack after-intent-read observation for session root {scope:?}");
    }
    registry.observers.insert(scope.clone(), tx);
    let guard = ReceiptAckAfterIntentReadGuard {
        session_root: scope,
    };
    (guard, rx)
}

pub(crate) fn signal_after_intent_read_validated(session_root: &Path) {
    let scope = session_root_scope(session_root);
    let registry = REGISTRY
        .lock()
        .expect("receipt ack after-intent-read registry");
    if let Some(tx) = registry.observers.get(&scope) {
        let _ = tx.send(());
    }
}
