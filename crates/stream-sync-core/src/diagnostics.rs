//! Rotating file diagnostics with secret redaction (Phase 7.4).

use chrono::{DateTime, Duration, Utc};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const LOG_PREFIX: &str = "stream-sync-";
pub const LOG_SUFFIX: &str = ".log";
pub const LOG_RETENTION_DAYS: i64 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeLogsReport {
    pub deleted: usize,
    pub kept: usize,
}

pub fn log_file_name_for(day: DateTime<Utc>) -> String {
    format!("{LOG_PREFIX}{}{LOG_SUFFIX}", day.format("%Y-%m-%d"))
}

/// Redact reusable credentials from a log line. Never invent replacements.
pub fn redact_secrets(input: &str) -> String {
    let mut out = input.to_string();
    out = redact_prefixed(&out, "oauth:");
    out = redact_prefixed(&out, "Bearer ");
    out = redact_prefixed(&out, "ssk_");
    out = redact_prefixed(&out, "ssd_");
    out = redact_prefixed(&out, "sdk_");
    out = redact_json_string_field(&out, "accessToken");
    out = redact_json_string_field(&out, "access_token");
    out = redact_json_string_field(&out, "refreshToken");
    out = redact_json_string_field(&out, "refresh_token");
    out = redact_json_string_field(&out, "connection_key");
    out = redact_json_string_field(&out, "jwt");
    out = redact_jwt_like(&out);
    out
}

fn is_secret_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '+' | '/' | '=')
}

fn redact_prefixed(input: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(idx) = rest.find(prefix) {
        out.push_str(&rest[..idx]);
        out.push_str(prefix);
        out.push_str("[REDACTED]");
        let after = &rest[idx + prefix.len()..];
        let skip = after
            .find(|c: char| !is_secret_char(c))
            .unwrap_or(after.len());
        rest = &after[skip..];
    }
    out.push_str(rest);
    out
}

fn redact_json_string_field(input: &str, field: &str) -> String {
    let patterns = [
        format!("\"{field}\":\""),
        format!("\"{field}\": \""),
        format!("{field}=\""),
    ];
    let mut current = input.to_string();
    for pat in patterns {
        let mut out = String::new();
        let mut rest = current.as_str();
        while let Some(idx) = rest.find(&pat) {
            out.push_str(&rest[..idx]);
            out.push_str(&pat);
            out.push_str("[REDACTED]");
            let after = &rest[idx + pat.len()..];
            let skip = after.find('"').unwrap_or(after.len());
            rest = if skip < after.len() {
                &after[skip..]
            } else {
                ""
            };
        }
        out.push_str(rest);
        current = out;
    }
    current
}

fn redact_jwt_like(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == 'e' && input[i..].starts_with("eyJ") {
            out.push_str("[REDACTED]");
            let rest = &input[i..];
            let skip = rest
                .find(|ch: char| !is_secret_char(ch))
                .unwrap_or(rest.len());
            for _ in 0..skip.saturating_sub(1) {
                chars.next();
            }
            continue;
        }
        out.push(c);
    }
    out
}

pub fn purge_log_files(logs_dir: &Path, now: DateTime<Utc>) -> io::Result<PurgeLogsReport> {
    fs::create_dir_all(logs_dir)?;
    let keep_name = log_file_name_for(now);
    let mut deleted = 0usize;
    let mut kept = 0usize;
    let entries = match fs::read_dir(logs_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(PurgeLogsReport {
                deleted: 0,
                kept: 0,
            });
        }
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(LOG_PREFIX) || !name.ends_with(LOG_SUFFIX) {
            continue;
        }
        if name == keep_name {
            kept += 1;
            continue;
        }
        fs::remove_file(&path)?;
        deleted += 1;
    }
    Ok(PurgeLogsReport { deleted, kept })
}

pub fn prune_logs_older_than(logs_dir: &Path, now: DateTime<Utc>, days: i64) -> io::Result<usize> {
    let cutoff = now - Duration::days(days);
    let mut deleted = 0usize;
    let Ok(entries) = fs::read_dir(logs_dir) else {
        return Ok(0);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(day) = name
            .strip_prefix(LOG_PREFIX)
            .and_then(|s| s.strip_suffix(LOG_SUFFIX))
        else {
            continue;
        };
        if chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
            .ok()
            .and_then(|d| {
                d.and_hms_opt(0, 0, 0)
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            })
            .map(|dt| dt < cutoff)
            .unwrap_or(false)
        {
            if fs::remove_file(&path).is_ok() {
                deleted += 1;
            }
        }
    }
    Ok(deleted)
}

struct DailyRedactingFile {
    logs_dir: PathBuf,
    current_name: String,
    file: File,
}

impl DailyRedactingFile {
    fn open(logs_dir: PathBuf, now: DateTime<Utc>) -> io::Result<Self> {
        fs::create_dir_all(&logs_dir)?;
        let name = log_file_name_for(now);
        let path = logs_dir.join(&name);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            logs_dir,
            current_name: name,
            file,
        })
    }

    fn rotate_if_needed(&mut self, now: DateTime<Utc>) -> io::Result<()> {
        let name = log_file_name_for(now);
        if name != self.current_name {
            let path = self.logs_dir.join(&name);
            self.file = OpenOptions::new().create(true).append(true).open(&path)?;
            self.current_name = name;
        }
        Ok(())
    }
}

impl Write for DailyRedactingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.rotate_if_needed(Utc::now());
        let text = String::from_utf8_lossy(buf);
        let redacted = redact_secrets(&text);
        self.file.write_all(redacted.as_bytes())?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Install stdout + rotating file tracing. Safe to call once at process start.
pub fn init_tracing(logs_dir: &Path) -> anyhow::Result<()> {
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    use tracing_subscriber::EnvFilter;

    fs::create_dir_all(logs_dir)?;
    let _ = prune_logs_older_than(logs_dir, Utc::now(), LOG_RETENTION_DAYS);
    let file = DailyRedactingFile::open(logs_dir.to_path_buf(), Utc::now())?;
    let file = Mutex::new(file);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,stream_sync_core=info,stream_sync_desktop_lib=info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(std::io::stdout.and(file))
        .try_init()
        .ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_secrets_strips_tokens_and_jwts() {
        let raw = concat!(
            "oauth:secretvalue Bearer abc.def ",
            r#"{"accessToken":"tok123","jwt":"eyJhbGciOiJIUzI1NiJ9.aa.bb"} "#,
            "ssk_deadbeef ssd_dock sdk_host"
        );
        let redacted = redact_secrets(raw);
        assert!(!redacted.contains("secretvalue"));
        assert!(!redacted.contains("tok123"));
        assert!(!redacted.contains("eyJhbGciOiJIUzI1NiJ9"));
        assert!(!redacted.contains("deadbeef"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn purge_log_files_keeps_current_day() {
        let dir = std::env::temp_dir().join(format!(
            "ss-purge-logs-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now();
        let today = dir.join(log_file_name_for(now));
        let old = dir.join(log_file_name_for(now - Duration::days(2)));
        fs::write(&today, "today").unwrap();
        fs::write(&old, "old").unwrap();
        let report = purge_log_files(&dir, now).unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(report.kept, 1);
        assert!(today.is_file());
        assert!(!old.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prune_logs_older_than_seven_days() {
        let dir = std::env::temp_dir().join(format!(
            "ss-prune-logs-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now();
        let keep = dir.join(log_file_name_for(now - Duration::days(3)));
        let drop = dir.join(log_file_name_for(now - Duration::days(10)));
        fs::write(&keep, "keep").unwrap();
        fs::write(&drop, "drop").unwrap();
        let deleted = prune_logs_older_than(&dir, now, 7).unwrap();
        assert_eq!(deleted, 1);
        assert!(keep.is_file());
        assert!(!drop.exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
