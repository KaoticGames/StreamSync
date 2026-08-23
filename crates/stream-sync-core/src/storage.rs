//! Writable paths + atomic JSON I/O (port of overlay-server/storage.js).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// True when `dir` is the Stream Sync workspace root (UI + Cargo workspace).
pub fn is_stream_sync_workspace(dir: &Path) -> bool {
    dir.join("Cargo.toml").is_file()
        && dir.join("crates").join("stream-sync-core").is_dir()
        && dir.join("shell.html").is_file()
        && dir
            .join("overlay-server")
            .join("events-studio.html")
            .is_file()
}

/// Packaged or dev bundle folder (has UI static files, may lack `.env`).
pub fn is_stream_sync_ui_bundle(dir: &Path) -> bool {
    dir.join("shell.html").is_file()
        && dir
            .join("overlay-server")
            .join("events-studio.html")
            .is_file()
}

/// `rust/` workspace directory (contains workspace `Cargo.toml` and `crates/`).
pub fn rust_workspace_root() -> PathBuf {
    if let Ok(r) = std::env::var("STREAMSYNC_RUST_ROOT") {
        let trimmed = r.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..8 {
        if dir.join("Cargo.toml").is_file() && dir.join("crates").join("stream-sync-core").is_dir()
        {
            return dir;
        }
        if !dir.pop() {
            break;
        }
    }

    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// UI/static asset root (`shell.html`, `overlay-server/`) — workspace root in dev, Tauri bundle when packaged.
pub fn resolve_ui_assets_root() -> PathBuf {
    if let Ok(r) =
        std::env::var("STREAMSYNC_UI_ROOT").or_else(|_| std::env::var("STREAMSYNC_REPO_ROOT"))
    {
        let trimmed = r.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let rust = rust_workspace_root();
    if is_stream_sync_workspace(&rust) {
        return rust;
    }

    for mut dir in discovery_start_dirs() {
        for _ in 0..12 {
            if is_stream_sync_workspace(&dir) {
                return dir;
            }
            if is_stream_sync_ui_bundle(&dir) {
                return dir;
            }
            if !dir.pop() {
                break;
            }
        }
    }

    rust
}

/// Back-compat alias for UI asset root (not config).
pub fn resolve_repo_root() -> PathBuf {
    resolve_ui_assets_root()
}

fn discovery_start_dirs() -> Vec<PathBuf> {
    let mut out = vec![
        PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        rust_workspace_root(),
    ];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            out.push(parent.to_path_buf());
        }
    }
    out
}

/// Rust workspace `.env` only (`rust/.env`).
pub fn rust_dotenv_path() -> PathBuf {
    rust_workspace_root().join(".env")
}

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct StoragePaths {
    pub root: PathBuf,
    pub dock_config: PathBuf,
    pub overlay_config: PathBuf,
    pub events_overlay_config: PathBuf,
    pub profiles: PathBuf,
    pub tokens_dir: PathBuf,
    pub twitch_tokens: PathBuf,
    /// Personal Kick OAuth tokens (send + feed ticket).
    pub kick_tokens: PathBuf,
    /// Syndicate takeover session (separate from personal OAuth tokens).
    pub twitch_delegated: PathBuf,
    /// Which saved identity is active: local vs delegated.
    pub twitch_active_mode: PathBuf,
    pub fonts_dir: PathBuf,
    /// Local copies of events alert media (`/events-media/...`).
    pub events_media_dir: PathBuf,
    /// Per-installation localhost control capability.
    pub control_token: PathBuf,
}

fn looks_like_asar(p: &Path) -> bool {
    p.to_string_lossy()
        .to_ascii_lowercase()
        .contains("app.asar")
}

fn assert_writable_root(root: &Path) -> Result<PathBuf> {
    let r = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    if looks_like_asar(&r) {
        anyhow::bail!(
            "Storage root points inside app.asar (read-only): {}\n\
             STREAMSYNC_USERDATA must be a user-writable directory.",
            r.display()
        );
    }
    fs::create_dir_all(&r).with_context(|| format!("create userData dir {}", r.display()))?;
    let probe = r.join(format!(
        ".writetest-{}-{}.tmp",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    ));
    fs::write(&probe, "ok")?;
    let _ = fs::remove_file(&probe);
    Ok(r)
}

/// Resolve userData root: `STREAMSYNC_USERDATA` or `~/.stream-sync`.
pub fn user_data_root() -> Result<PathBuf> {
    let root = std::env::var("STREAMSYNC_USERDATA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".stream-sync")
        });
    assert_writable_root(&root)
}

pub fn get_paths() -> Result<StoragePaths> {
    let root = user_data_root()?;

    let dock_config =
        env_path("STREAMSYNC_DOCK_CONFIG").unwrap_or_else(|| root.join("dock-config.json"));
    let overlay_config =
        env_path("STREAMSYNC_OVERLAY_CONFIG").unwrap_or_else(|| root.join("overlay-config.json"));
    let events_overlay_config = env_path("STREAMSYNC_EVENTS_OVERLAY_CONFIG")
        .unwrap_or_else(|| root.join("events-overlay-config.json"));
    let twitch_tokens =
        env_path("STREAMSYNC_TOKENS_FILE").unwrap_or_else(|| root.join("twitch-tokens.json"));
    let kick_tokens =
        env_path("STREAMSYNC_KICK_TOKENS_FILE").unwrap_or_else(|| root.join("kick-tokens.json"));
    let twitch_delegated = root.join("twitch-delegated.json");
    let twitch_active_mode = root.join("twitch-active-mode.json");
    let fonts_dir = env_path("STREAMSYNC_FONTS_DIR").unwrap_or_else(|| root.join("fonts"));
    let events_media_dir =
        env_path("STREAMSYNC_EVENTS_MEDIA_DIR").unwrap_or_else(|| root.join("events-media"));
    let control_token = root.join("control-token.txt");

    let tokens_dir = root.join("tokens");
    fs::create_dir_all(&tokens_dir).ok();
    fs::create_dir_all(&fonts_dir).ok();
    fs::create_dir_all(&events_media_dir).ok();

    Ok(StoragePaths {
        root: root.clone(),
        dock_config,
        overlay_config,
        events_overlay_config,
        profiles: root.join("profiles.json"),
        tokens_dir,
        twitch_tokens,
        kick_tokens,
        twitch_delegated,
        twitch_active_mode,
        fonts_dir,
        events_media_dir,
        control_token,
    })
}

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var(key)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
}

/// Legacy Electron app userData (`%APPDATA%/stream-sync` on Windows).
pub fn legacy_electron_user_data() -> Option<PathBuf> {
    if let Ok(appdata) = std::env::var("APPDATA") {
        let p = PathBuf::from(appdata).join("stream-sync");
        if p.is_dir() {
            return Some(p);
        }
    }
    None
}

/// Candidate env files for defaults (installer bundle + dev), lowest priority first.
fn default_env_candidates(rust_root: &Path) -> Vec<PathBuf> {
    let mut out = vec![
        rust_root.join("config").join("bundled.env"),
        rust_root.join(".env"),
    ];
    if let Ok(ui) = std::env::var("STREAMSYNC_UI_ROOT") {
        let trimmed = ui.trim();
        if !trimmed.is_empty() {
            out.push(PathBuf::from(trimmed).join("config").join("bundled.env"));
        }
    }
    out
}

fn first_existing_env_file(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates.iter().find(|p| p.is_file()).cloned()
}

/// Load env: bundled defaults, `rust/.env` (dev overrides), legacy Electron, userData (overrides).
pub fn load_streamsync_dotenv(userdata: &Path, rust_root: &Path) {
    let candidates = default_env_candidates(rust_root);
    if let Some(bundled) = first_existing_env_file(&candidates) {
        let _ = dotenvy::from_path(&bundled);
    }

    let rust_env = rust_root.join(".env");
    if rust_env.is_file() {
        let _ = dotenvy::from_path_override(&rust_env);
    }

    if let Some(legacy) = legacy_electron_user_data() {
        let legacy_env = legacy.join(".env");
        if legacy_env.is_file() {
            let _ = dotenvy::from_path(&legacy_env);
        }
    }
    let user_env = userdata.join(".env");
    if user_env.is_file() {
        let _ = dotenvy::from_path_override(&user_env);
    }
}

fn env_nonempty(key: &str) -> bool {
    std::env::var(key)
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
}

fn parse_dotenv_value(raw: &str, key: &str) -> Option<String> {
    for line in raw.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        let Some((k, v)) = t.split_once('=') else {
            continue;
        };
        if k.trim() == key {
            let val = v.trim().trim_matches('"').trim_matches('\'');
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// If `TWITCH_CLIENT_ID` is still missing, copy from bundled/rust env into `userdata/.env`.
pub fn bootstrap_twitch_env_from_rust(userdata: &Path, rust_root: &Path) -> Result<()> {
    if env_nonempty("TWITCH_CLIENT_ID") {
        return Ok(());
    }
    let Some(source) = first_existing_env_file(&default_env_candidates(rust_root)) else {
        return Ok(());
    };
    let raw = fs::read_to_string(&source)?;
    let Some(client_id) = parse_dotenv_value(&raw, "TWITCH_CLIENT_ID") else {
        return Ok(());
    };
    let redirect = parse_dotenv_value(&raw, "TWITCH_REDIRECT_URI").unwrap_or_else(|| {
        std::env::var("TWITCH_REDIRECT_URI")
            .unwrap_or_else(|_| "http://localhost:4040/auth/twitch/callback".into())
    });

    fs::create_dir_all(userdata)?;
    let user_env = userdata.join(".env");
    let mut out = if user_env.is_file() {
        fs::read_to_string(&user_env)?
    } else {
        String::new()
    };
    if !out.ends_with('\n') && !out.is_empty() {
        out.push('\n');
    }
    if !out.contains("TWITCH_CLIENT_ID=") {
        out.push_str(&format!("TWITCH_CLIENT_ID={client_id}\n"));
    }
    if !out.contains("TWITCH_REDIRECT_URI=") {
        out.push_str(&format!("TWITCH_REDIRECT_URI={redirect}\n"));
    }
    write_file_atomic(&user_env, out.as_bytes())?;
    let _ = dotenvy::from_path_override(&user_env);
    Ok(())
}

fn now_ts() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H-%M-%S%.3fZ")
        .to_string()
        .replace(':', "-")
}

pub fn write_file_atomic(target: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = target.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    ));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    let bak = target.with_extension("bak");
    let _ = fs::remove_file(&bak);
    if target.exists() {
        let _ = fs::rename(target, &bak);
    }
    match fs::rename(&tmp, target) {
        Ok(()) => Ok(()),
        Err(_) => {
            let _ = fs::remove_file(target);
            if fs::rename(&tmp, target).is_err() {
                fs::copy(&tmp, target)?;
                let _ = fs::remove_file(&tmp);
            }
            Ok(())
        }
    }
}

pub fn read_json_or_default<T>(path: &Path, default: &T) -> Result<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    if !path.exists() {
        write_json(path, default)?;
        return Ok(default.clone());
    }
    match fs::read_to_string(path) {
        Ok(raw) => match serde_json::from_str::<T>(&raw) {
            Ok(v) => Ok(v),
            Err(_) => {
                let corrupt = path.with_extension(format!("corrupt-{}", now_ts()));
                let _ = fs::rename(path, &corrupt);
                write_json(path, default)?;
                Ok(default.clone())
            }
        },
        Err(_) => {
            write_json(path, default)?;
            Ok(default.clone())
        }
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let data = serde_json::to_string_pretty(value)?;
    write_file_atomic(path, data.as_bytes())
}

/// Simple `dirs` helper without extra crate — home via USERPROFILE/HOME.
mod dirs {
    use std::path::PathBuf;
    pub fn home_dir() -> Option<PathBuf> {
        if let Ok(p) = std::env::var("USERPROFILE") {
            return Some(PathBuf::from(p));
        }
        std::env::var("HOME").ok().map(PathBuf::from)
    }
}
