//! Offline local journal recovery render (read-only source; no cloud mutation).
//!
//! ```text
//! cargo run -p stream-sync-core --release --features local-opus-vendored --example local_journal_rerender -- <session-root> --output <new-final-dir>
//! ```

use std::env;
use std::path::PathBuf;
use stream_sync_core::voice_delivery::local_finalization::{
    run_offline_journal_rerender, OfflineJournalRerenderNetworkSeal, OfflineJournalRerenderOptions,
};

fn main() {
    match run() {
        Ok(path) => {
            if path.as_os_str().is_empty() {
                return;
            }
            println!("offline_journal_rerender: published {}", path.display());
        }
        Err(e) => {
            eprintln!("local_journal_rerender: {e}");
            std::process::exit(1);
        }
    }
}

fn required_build_git_sha() -> Result<&'static str, String> {
    match option_env!("STREAMSYNC_GIT_SHA") {
        Some(s) if s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()) => Ok(s),
        _ => Err(
            "STREAMSYNC_GIT_SHA unavailable; build from a git checkout (build.rs sets provenance)"
                .into(),
        ),
    }
}

fn run() -> Result<PathBuf, String> {
    let build_sha = required_build_git_sha()?;
    println!("offline_journal_rerender: tool_build_git_sha={}", build_sha);

    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        eprintln!("usage: local_journal_rerender <session-root> --output <new-final-dir>");
        return Ok(PathBuf::new());
    }
    let session_root = PathBuf::from(&args[0]);
    let mut output: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--output" {
            i += 1;
            let path = args
                .get(i)
                .ok_or_else(|| "--output requires a path".to_string())?;
            output = Some(PathBuf::from(path));
            i += 1;
        } else {
            return Err(format!("unknown argument: {}", args[i]));
        }
    }
    let output = output.ok_or_else(|| "--output is required".to_string())?;
    if output.as_os_str().is_empty() {
        return Err("output path is empty".into());
    }

    let outcome = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &output,
        OfflineJournalRerenderOptions::default(),
    )
    .map_err(|e| e.to_string())?;
    Ok(outcome.final_dir)
}
