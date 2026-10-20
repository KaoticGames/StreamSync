//! Exercises build-script git provenance helpers (same module as `build.rs`).

#[path = "../build/git_provenance.rs"]
mod git_provenance;

use git_provenance::{
    git_provenance_watch_paths, is_tracked_tree_clean, is_valid_git_sha, repo_root_from_manifest,
    resolve_git_sha,
};
use std::path::PathBuf;

#[test]
fn watch_paths_include_existing_head() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = repo_root_from_manifest(&manifest_dir).expect("git checkout");
    let paths = git_provenance_watch_paths(&repo_root);
    assert!(
        paths.iter().any(|p| p.ends_with("HEAD")),
        "expected HEAD in {:?}",
        paths
    );
    let _tracked_tree_clean = is_tracked_tree_clean(&repo_root);
    if let Some(sha) = resolve_git_sha(&manifest_dir) {
        assert!(is_valid_git_sha(&sha));
    }
}

#[test]
fn forty_hex_validation_matches_build_script() {
    let good = "0123456789abcdef".repeat(2) + "01234567";
    assert_eq!(good.len(), 40);
    assert!(is_valid_git_sha(&good));
}
