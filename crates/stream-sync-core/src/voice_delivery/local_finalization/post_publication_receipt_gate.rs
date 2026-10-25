//! Synchronization at the publication→receipt boundary (`lifecycle-test-support` / unit tests only).

#[cfg(any(test, all(debug_assertions, feature = "lifecycle-test-support")))]
pub mod injection {
    use std::collections::HashMap;
    use std::path::{Component, Path, PathBuf};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    const WAIT: Duration = Duration::from_secs(30);

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum PostPublicationReceiptBoundary {
        /// After canonical publication is durably committed and `PublicationComplete` is emitted.
        AfterPublicationBeforeReceiptIntent,
        /// Before replaying a persisted receipt intent to the cloud (`try_receipt_only_completion`).
        BeforeReceiptReplayPost,
    }

    struct GateState {
        boundary: PostPublicationReceiptBoundary,
        arrived_tx: Option<Sender<()>>,
        resume_rx: Option<Receiver<()>>,
    }

    type GateKey = (PathBuf, String, PostPublicationReceiptBoundary);

    struct Registry {
        gates: HashMap<GateKey, GateState>,
    }

    static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
        Mutex::new(Registry {
            gates: HashMap::new(),
        })
    });

    /// Absolute, lexically normalized recording parent for registry equality (no `canonicalize`).
    fn recording_parent_gate_scope(parent: &Path) -> PathBuf {
        let abs = std::path::absolute(parent).unwrap_or_else(|_| parent.to_path_buf());
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

    fn gate_key(
        recording_parent: &Path,
        session_id: &str,
        boundary: PostPublicationReceiptBoundary,
    ) -> GateKey {
        (
            recording_parent_gate_scope(recording_parent),
            session_id.to_string(),
            boundary,
        )
    }

    pub struct PostPublicationReceiptGateGuard {
        recording_parent: PathBuf,
        session_id: String,
        boundary: PostPublicationReceiptBoundary,
    }

    impl Drop for PostPublicationReceiptGateGuard {
        fn drop(&mut self) {
            let mut registry = REGISTRY.lock().expect("post-publication receipt gate lock");
            registry.gates.remove(&(
                self.recording_parent.clone(),
                self.session_id.clone(),
                self.boundary,
            ));
        }
    }

    pub fn install_post_publication_receipt_gate(
        recording_parent: &Path,
        session_id: &str,
        boundary: PostPublicationReceiptBoundary,
    ) -> (PostPublicationReceiptGateGuard, Receiver<()>, Sender<()>) {
        let (arrived_tx, arrived_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let scope = recording_parent_gate_scope(recording_parent);
        let key = gate_key(recording_parent, session_id, boundary);
        let mut registry = REGISTRY.lock().expect("post-publication receipt gate lock");
        if registry.gates.contains_key(&key) {
            panic!(
                "duplicate post-publication receipt gate for parent {scope:?} session {session_id} boundary {boundary:?}"
            );
        }
        registry.gates.insert(
            key,
            GateState {
                boundary,
                arrived_tx: Some(arrived_tx),
                resume_rx: Some(resume_rx),
            },
        );
        let guard = PostPublicationReceiptGateGuard {
            recording_parent: scope,
            session_id: session_id.to_string(),
            boundary,
        };
        (guard, arrived_rx, resume_tx)
    }

    pub fn pause_blocking(
        recording_parent: &Path,
        session_id: &str,
        boundary: PostPublicationReceiptBoundary,
    ) {
        let (resume_rx, arrived_tx) = {
            let mut registry = REGISTRY.lock().expect("post-publication receipt gate lock");
            let key = gate_key(recording_parent, session_id, boundary);
            let Some(state) = registry.gates.get_mut(&key) else {
                return;
            };
            if state.boundary != boundary {
                return;
            }
            let arrived_tx = state.arrived_tx.take();
            let resume_rx = state.resume_rx.take();
            (resume_rx, arrived_tx)
        };
        if let Some(tx) = arrived_tx {
            let _ = tx.send(());
        }
        if let Some(rx) = resume_rx {
            let deadline = std::time::Instant::now() + WAIT;
            while std::time::Instant::now() < deadline {
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            panic!(
                "post-publication receipt gate timed out (parent {recording_parent:?}, session {session_id}, boundary {boundary:?})"
            );
        }
    }
}

#[cfg(any(test, all(debug_assertions, feature = "lifecycle-test-support")))]
pub use injection::{
    install_post_publication_receipt_gate, pause_blocking as pause_post_publication_receipt_gate,
    PostPublicationReceiptBoundary, PostPublicationReceiptGateGuard,
};

#[cfg(not(any(test, all(debug_assertions, feature = "lifecycle-test-support"))))]
pub enum PostPublicationReceiptBoundary {}
