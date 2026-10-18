//! Embed validated git provenance for offline recovery tooling (`STREAMSYNC_GIT_SHA`).

#[path = "build/git_provenance.rs"]
mod git_provenance;

use git_provenance::{git_provenance_watch_paths, repo_root_from_manifest, resolve_git_sha};

fn main() {
    println!("cargo:rerun-if-env-changed=STREAMSYNC_GIT_SHA");

    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    if let Some(repo_root) = repo_root_from_manifest(&manifest_dir) {
        for path in git_provenance_watch_paths(&repo_root) {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    if let Some(sha) = resolve_git_sha(&manifest_dir) {
        println!("cargo:rustc-env=STREAMSYNC_GIT_SHA={}", sha);
    }
}
