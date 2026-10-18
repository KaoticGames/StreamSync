//! Embed validated git provenance for offline recovery tooling (`STREAMSYNC_GIT_SHA`).

fn main() {
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");
    println!("cargo:rerun-if-env-changed=STREAMSYNC_GIT_SHA");

    if let Some(sha) = resolve_git_sha() {
        println!("cargo:rustc-env=STREAMSYNC_GIT_SHA={}", sha);
    }
}

fn resolve_git_sha() -> Option<String> {
    if let Ok(env_sha) = std::env::var("STREAMSYNC_GIT_SHA") {
        if is_valid_git_sha(&env_sha) {
            return Some(env_sha);
        }
        panic!(
            "STREAMSYNC_GIT_SHA must be exactly 40 lowercase hex digits, got {:?}",
            env_sha
        );
    }
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let repo_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(&manifest_dir);
    let output = std::process::Command::new("git")
        .args(["-C", repo_root.to_str().unwrap(), "rev-parse", "HEAD"])
        .output();
    match output {
        Ok(out) if out.status.success() => {
            let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if is_valid_git_sha(&sha) {
                Some(sha)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_valid_git_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
