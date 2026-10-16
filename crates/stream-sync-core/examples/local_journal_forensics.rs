//! Read-only local journal forensics CLI (metadata only; no Opus decode).
//!
//! ```text
//! cargo run -p stream-sync-core --example local_journal_forensics -- <session-root> [--json <path>]
//! ```

use std::env;
use std::fs;
use std::path::PathBuf;
use stream_sync_core::voice_delivery::local_finalization::{
    analyze_local_journal_session, forensics_report_to_json, render_forensics_table,
};

fn main() {
    if let Err(e) = run() {
        eprintln!("local_journal_forensics: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        eprintln!("usage: local_journal_forensics <session-root> [--json <output-path>]");
        return Ok(());
    }
    let session_root = PathBuf::from(&args[0]);
    let mut json_path: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--json" {
            i += 1;
            let path = args
                .get(i)
                .ok_or_else(|| "--json requires a path".to_string())?;
            json_path = Some(PathBuf::from(path));
            i += 1;
        } else {
            return Err(format!("unknown argument: {}", args[i]));
        }
    }

    let report = analyze_local_journal_session(&session_root).map_err(|e| e.to_string())?;
    print!("{}", render_forensics_table(&report));
    if let Some(path) = json_path {
        let json = forensics_report_to_json(&report).map_err(|e| e.to_string())?;
        fs::write(&path, json).map_err(|e| e.to_string())?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}
