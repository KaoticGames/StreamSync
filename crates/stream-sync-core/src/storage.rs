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
    /// Tombstone written when delegated authority is revoked (startup fail-closed).
    pub twitch_delegated_revoked: PathBuf,
    /// Crash-persistent intent that durable revoke is still pending (independent of tombstone).
    pub twitch_delegated_revoke_pending: PathBuf,
    /// Which saved identity is active: local vs delegated.
    pub twitch_active_mode: PathBuf,
    pub fonts_dir: PathBuf,
    /// Local copies of events alert media (`/events-media/...`).
    pub events_media_dir: PathBuf,
    /// Per-installation localhost control capability.
    pub control_token: PathBuf,
    /// Scoped OBS chat-dock credentials (not the master control token).
    pub dock_credentials: PathBuf,
}

fn looks_like_asar(p: &Path) -> bool {
    p.to_string_lossy()
        .to_ascii_lowercase()
        .contains("app.asar")
}

fn configured_user_data_root() -> PathBuf {
    std::env::var("STREAMSYNC_USERDATA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".stream-sync")
        })
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

/// Resolve an existing or absent userdata root without creating, probing, or chmod.
fn resolve_readonly_root(root: &Path) -> Result<PathBuf> {
    if looks_like_asar(root) {
        anyhow::bail!(
            "Storage root points inside app.asar (read-only): {}\n\
             STREAMSYNC_USERDATA must be a user-writable directory.",
            root.display()
        );
    }
    if root.exists() {
        if !root.is_dir() {
            anyhow::bail!("STREAMSYNC_USERDATA is not a directory: {}", root.display());
        }
        return Ok(fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf()));
    }
    // Absent root is allowed: callers use ephemeral in-memory defaults and must not create it.
    Ok(root.to_path_buf())
}

/// Resolve userData root: `STREAMSYNC_USERDATA` or `~/.stream-sync` (creates/probes writable).
pub fn user_data_root() -> Result<PathBuf> {
    assert_writable_root(&configured_user_data_root())
}

/// Resolve userData root for readonly mode — never create_dir_all or write probes.
pub fn user_data_root_readonly() -> Result<PathBuf> {
    resolve_readonly_root(&configured_user_data_root())
}

pub fn get_paths() -> Result<StoragePaths> {
    get_paths_for_mode(false)
}

pub fn get_paths_readonly() -> Result<StoragePaths> {
    get_paths_for_mode(true)
}

/// Build [`StoragePaths`] for an explicit userdata root (tests / injected config).
/// Does not read or write `STREAMSYNC_USERDATA`.
pub fn paths_for_root(root: &Path, readonly: bool) -> Result<StoragePaths> {
    let root = if readonly {
        resolve_readonly_root(root)?
    } else {
        assert_writable_root(root)?
    };
    paths_under_root(root, readonly)
}

fn get_paths_for_mode(readonly: bool) -> Result<StoragePaths> {
    let root = if readonly {
        user_data_root_readonly()?
    } else {
        user_data_root()?
    };
    paths_under_root(root, readonly)
}

fn paths_under_root(root: PathBuf, readonly: bool) -> Result<StoragePaths> {
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
    let twitch_delegated_revoked = root.join("twitch-delegated.revoked");
    let twitch_delegated_revoke_pending = root.join("twitch-delegated.revoke-pending");
    let twitch_active_mode = root.join("twitch-active-mode.json");
    let fonts_dir = env_path("STREAMSYNC_FONTS_DIR").unwrap_or_else(|| root.join("fonts"));
    let events_media_dir =
        env_path("STREAMSYNC_EVENTS_MEDIA_DIR").unwrap_or_else(|| root.join("events-media"));
    let control_token = root.join("control-token.txt");
    let dock_credentials = root.join("dock-credentials.json");

    let tokens_dir = root.join("tokens");
    if !readonly {
        fs::create_dir_all(&tokens_dir).ok();
        fs::create_dir_all(&fonts_dir).ok();
        fs::create_dir_all(&events_media_dir).ok();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&root, fs::Permissions::from_mode(0o700));
            let _ = fs::set_permissions(&tokens_dir, fs::Permissions::from_mode(0o700));
        }
    }

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
        twitch_delegated_revoked,
        twitch_delegated_revoke_pending,
        twitch_active_mode,
        fonts_dir,
        events_media_dir,
        control_token,
        dock_credentials,
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
    write_file_atomic_inner(target, data, false)
}

/// Atomic write for secrets: restrictive permissions, no reusable `.bak`.
pub fn write_secret_file(target: &Path, data: &[u8]) -> Result<()> {
    write_file_atomic_inner(target, data, true)
}

fn write_file_atomic_inner(target: &Path, data: &[u8], secret: bool) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
        if secret {
            apply_secret_dir_permissions(parent);
        }
    }
    let tmp = target.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let write_result = (|| -> Result<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }
    if secret {
        if let Err(error) = apply_secret_file_permissions(&tmp) {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        // Never leave a reusable previous secret on disk.
        let bak = target.with_extension("bak");
        let _ = fs::remove_file(&bak);
        if target.exists() {
            let _ = fs::remove_file(target);
        }
    } else {
        let bak = target.with_extension("bak");
        let _ = fs::remove_file(&bak);
        if target.exists() {
            let _ = fs::rename(target, &bak);
        }
    }
    match fs::rename(&tmp, target) {
        Ok(()) => {
            if secret {
                apply_secret_file_permissions(target)?;
            }
            Ok(())
        }
        Err(_) => {
            let _ = fs::remove_file(target);
            if fs::rename(&tmp, target).is_err() {
                fs::copy(&tmp, target)?;
                let _ = fs::remove_file(&tmp);
            }
            if secret {
                apply_secret_file_permissions(target)?;
            }
            Ok(())
        }
    }
}

fn apply_secret_dir_permissions(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
    }
    #[cfg(windows)]
    {
        let _ = dir;
        // Best-effort: Windows ACL tightening is applied on the secret file itself.
    }
}

fn apply_secret_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let meta = fs::metadata(path)?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            anyhow::bail!(
                "secret file permissions too broad after write: {:o} ({})",
                mode,
                path.display()
            );
        }
    }
    #[cfg(windows)]
    {
        // Restrict to the current user via icacls (no world/Everyone read).
        let path_str = path.to_string_lossy().to_string();
        let user = std::env::var("USERNAME").unwrap_or_else(|_| "Administrators".into());
        let _ = std::process::Command::new("icacls")
            .args([&path_str, "/inheritance:r"])
            .output();
        let grant = format!("{user}:F");
        let _ = std::process::Command::new("icacls")
            .args([&path_str, "/grant:r", &grant])
            .output();
    }
    Ok(())
}

/// Repair overly broad permissions on an existing secret file (Unix).
pub fn ensure_secret_file_permissions(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    apply_secret_file_permissions(path)
}

/// Load JSON when present; return default without creating or repairing files.
pub fn read_json_if_exists<T>(path: &Path, default: &T) -> Result<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    if !path.is_file() {
        return Ok(default.clone());
    }
    let raw = fs::read_to_string(path)?;
    match serde_json::from_str::<T>(&raw) {
        Ok(v) => Ok(v),
        Err(_) => Ok(default.clone()),
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

/// Best-effort directory metadata sync after delegated credential removal.
pub fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        #[cfg(unix)]
        {
            let dir = fs::File::open(parent)?;
            dir.sync_all()?;
        }
        #[cfg(not(unix))]
        {
            let _ = parent;
        }
    }
    Ok(())
}

/// Remove a file durably: rename away, sync parent, then delete the quarantine copy.
pub fn remove_file_durable(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let quarantine = path.with_extension(format!("revoked-{}", now_ts()));
    match fs::rename(path, &quarantine) {
        Ok(()) => {
            sync_parent_dir(path)?;
            fs::remove_file(&quarantine)?;
            sync_parent_dir(path)?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => {
            fs::remove_file(path)?;
            sync_parent_dir(path)?;
            Ok(())
        }
    }
}

/// Durable revoked tombstone for delegated takeover credentials.
pub fn write_delegated_revoked_tombstone(path: &Path) -> Result<()> {
    let payload = serde_json::json!({
        "revoked_at": chrono::Utc::now().to_rfc3339(),
    });
    write_file_atomic(path, serde_json::to_string(&payload)?.as_bytes())
}

/// Crash-persistent marker that durable delegated revoke is still incomplete.
pub fn write_delegated_revoke_pending(path: &Path) -> Result<()> {
    let payload = serde_json::json!({
        "pending_at": chrono::Utc::now().to_rfc3339(),
    });
    write_file_atomic(path, serde_json::to_string(&payload)?.as_bytes())
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
