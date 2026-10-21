//! Proves vertical-2 WSS session is not referenced from production AppState/Tauri/orchestrator wiring.

use std::path::{Path, PathBuf};

const FORBIDDEN_PRODUCTION_REFERENCES: &[&str] = &[
    "DirectLocalWssDarkSession",
    "direct_local::wss::",
    "DirectLocalWssRunConfig",
    "run_until_stop",
    "DirectWssTransport",
];

const SCAN_ROOTS: &[&str] = &[
    "crates/stream-sync-core/src",
    "crates/stream-sync-desktop/src",
    "crates/stream-sync-server/src",
];

fn is_test_only_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.contains("direct_local/wss/")
        || s.contains("direct_local\\wss\\")
        || s.contains("direct_local/wss_")
        || s.contains("direct_local\\wss_")
        || s.ends_with("_tests.rs")
        || s.contains("/tests/")
        || s.contains("\\tests\\")
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.is_dir() {
        return;
    }
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") && !is_test_only_path(&path) {
            out.push(path);
        }
    }
}

#[test]
fn no_production_module_starts_or_references_direct_local_wss_session() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let mut files = Vec::new();
    for rel in SCAN_ROOTS {
        collect_rs_files(&repo.join(rel), &mut files);
    }
    for path in files {
        let contents = std::fs::read_to_string(&path).expect("read source");
        for needle in FORBIDDEN_PRODUCTION_REFERENCES {
            assert!(
                !contents.contains(needle),
                "production starter/wiring file {} must not reference {}",
                path.strip_prefix(repo).unwrap_or(&path).display(),
                needle
            );
        }
    }
}
