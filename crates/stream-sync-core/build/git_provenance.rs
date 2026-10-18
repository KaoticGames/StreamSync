//! Git provenance helpers for `build.rs` (included from integration tests via `#[path]`).

use std::path::{Path, PathBuf};
use std::process::Command;

/// Watch paths for `cargo:rerun-if-changed` derived from the real git dir (works in worktrees).
pub fn git_provenance_watch_paths(repo_root: &Path) -> Vec<PathBuf> {
    let candidates = ["HEAD", "refs/heads", "packed-refs"];
    let mut paths = Vec::new();
    for name in candidates {
        if let Some(p) = git_rev_parse_git_path(repo_root, name) {
            if p.exists() {
                paths.push(p);
            }
        }
    }
    paths
}

pub fn repo_root_from_manifest(manifest_dir: &Path) -> Option<PathBuf> {
    git_stdout(manifest_dir, &["rev-parse", "--show-toplevel"]).map(|s| PathBuf::from(s))
}

/// Tracked-tree cleanliness only (`git diff` / `git diff --cached`); untracked/ignored files are ignored.
pub fn is_tracked_tree_clean(repo_root: &Path) -> bool {
    git_ok(repo_root, &["diff", "--quiet"]) && git_ok(repo_root, &["diff", "--cached", "--quiet"])
}

pub fn resolve_git_sha(manifest_dir: &Path) -> Option<String> {
    if let Ok(env_sha) = std::env::var("STREAMSYNC_GIT_SHA") {
        if is_valid_git_sha(&env_sha) {
            return Some(env_sha);
        }
        panic!(
            "STREAMSYNC_GIT_SHA must be exactly 40 lowercase hex digits, got {:?}",
            env_sha
        );
    }

    let repo_root = repo_root_from_manifest(manifest_dir)?;
    if !is_tracked_tree_clean(&repo_root) {
        return None;
    }

    let sha = git_stdout(&repo_root, &["rev-parse", "HEAD"])?;
    if is_valid_git_sha(&sha) {
        Some(sha)
    } else {
        None
    }
}

pub fn git_rev_parse_git_path(repo_root: &Path, git_path: &str) -> Option<PathBuf> {
    let raw = git_stdout(repo_root, &["rev-parse", "--git-path", git_path])?;
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return Some(path);
    }
    let git_dir = git_stdout(repo_root, &["rev-parse", "--git-dir"])?;
    let git_dir = PathBuf::from(git_dir);
    let base = if git_dir.is_absolute() {
        git_dir
    } else {
        repo_root.join(git_dir)
    };
    Some(base.join(path))
}

pub fn is_valid_git_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn git_stdout(repo_root: &Path, args: &[&str]) -> Option<String> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(repo_root).args(args);
    match cmd.output() {
        Ok(out) if out.status.success() => {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        }
        _ => None,
    }
}

fn git_ok(repo_root: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_valid_git_sha_rejects_bad_lengths_and_uppercase() {
        assert!(is_valid_git_sha("a".repeat(40).as_str()));
        assert!(!is_valid_git_sha("a".repeat(39).as_str()));
        assert!(!is_valid_git_sha("A".repeat(40).as_str()));
        assert!(!is_valid_git_sha("g".repeat(40).as_str()));
    }

    #[test]
    fn repo_root_and_head_path_resolve_in_git_checkout() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = repo_root_from_manifest(&manifest_dir).expect("git checkout");
        assert!(repo_root.is_dir());
        assert!(manifest_dir.starts_with(&repo_root));

        let head = git_rev_parse_git_path(&repo_root, "HEAD").expect("HEAD path");
        assert!(head.is_file(), "HEAD should exist at {}", head.display());
    }
}
