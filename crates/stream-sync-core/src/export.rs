//! User-data backup export as a ZIP archive.

use crate::storage::StoragePaths;
use anyhow::{Context, Result};
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::Path;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const BACKUP_FORMAT: &str = "stream-sync-backup";
const BACKUP_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct BackupManifest {
    pub format: &'static str,
    pub version: u32,
    pub exported_at: String,
    pub app_version: String,
}

/// Build a ZIP containing Stream Sync user data (configs, fonts, media, imports, tokens, logs).
pub fn build_backup_zip(paths: &StoragePaths, logs_dir: Option<&Path>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut zip = ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated);

        let manifest = BackupManifest {
            format: BACKUP_FORMAT,
            version: BACKUP_VERSION,
            exported_at: chrono::Utc::now().to_rfc3339(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        write_zip_bytes(&mut zip, "manifest.json", manifest_json.as_bytes(), options)?;

        let root = &paths.root;
        add_root_file(&mut zip, root, "dock-config.json", &paths.dock_config, options)?;
        add_root_file(&mut zip, root, "overlay-config.json", &paths.overlay_config, options)?;
        add_root_file(
            &mut zip,
            root,
            "events-overlay-config.json",
            &paths.events_overlay_config,
            options,
        )?;
        add_root_file(&mut zip, root, "profiles.json", &paths.profiles, options)?;
        add_root_file(
            &mut zip,
            root,
            "twitch-tokens.json",
            &paths.twitch_tokens,
            options,
        )?;
        add_root_file(
            &mut zip,
            root,
            "kick-tokens.json",
            &paths.kick_tokens,
            options,
        )?;
        // Intentionally exclude twitch-delegated.json — takeover keys must not leak via backup.

        let se_session = root.join("streamelements-session.json");
        add_root_file(
            &mut zip,
            root,
            "streamelements-session.json",
            &se_session,
            options,
        )?;

        let dotenv = root.join(".env");
        add_root_file(&mut zip, root, ".env", &dotenv, options)?;

        add_dir_to_zip(&mut zip, root, &paths.fonts_dir, options)?;
        add_dir_to_zip(&mut zip, root, &paths.events_media_dir, options)?;
        add_dir_to_zip(&mut zip, root, &paths.tokens_dir, options)?;

        let imports = root.join("imports");
        add_dir_to_zip(&mut zip, root, &imports, options)?;

        if let Some(logs) = logs_dir {
            add_dir_to_zip(&mut zip, root, logs, options)?;
        }

        zip.finish().context("finish zip")?;
    }
    Ok(buf)
}

fn add_root_file<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    _root: &Path,
    archive_name: &str,
    disk_path: &Path,
    options: SimpleFileOptions,
) -> Result<()> {
    if !disk_path.is_file() {
        return Ok(());
    }
    let data = fs::read(disk_path)
        .with_context(|| format!("read {}", disk_path.display()))?;
    write_zip_bytes(zip, archive_name, &data, options)
}

fn write_zip_bytes<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    archive_name: &str,
    data: &[u8],
    options: SimpleFileOptions,
) -> Result<()> {
    zip.start_file(archive_name, options)
        .with_context(|| format!("zip start {archive_name}"))?;
    zip.write_all(data)
        .with_context(|| format!("zip write {archive_name}"))?;
    Ok(())
}

fn add_dir_to_zip<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    root: &Path,
    dir: &Path,
    options: SimpleFileOptions,
) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let canonical_root = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    walk_dir(zip, &canonical_root, dir, options)
}

fn walk_dir<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    root: &Path,
    dir: &Path,
    options: SimpleFileOptions,
) -> Result<()> {
    let entries = fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if should_skip_backup_path(&path) {
            continue;
        }
        if path.is_dir() {
            walk_dir(zip, root, &path, options)?;
        } else if path.is_file() {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            write_zip_bytes(zip, &rel, &data, options)?;
        }
    }
    Ok(())
}

fn should_skip_backup_path(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if name.starts_with(".writetest") {
        return true;
    }
    if name.ends_with(".tmp") || name.ends_with(".bak") {
        return true;
    }
    if name.contains("corrupt-") {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StoragePaths;
    use std::io::Read;

    fn temp_paths() -> (std::path::PathBuf, StoragePaths) {
        let root = std::env::temp_dir().join(format!(
            "stream-sync-export-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(root.join("overlay-config.json"), r#"{"profiles":{}}"#).unwrap();
        let paths = StoragePaths {
            root: root.clone(),
            dock_config: root.join("dock-config.json"),
            overlay_config: root.join("overlay-config.json"),
            events_overlay_config: root.join("events-overlay-config.json"),
            profiles: root.join("profiles.json"),
            tokens_dir: root.join("tokens"),
            twitch_tokens: root.join("twitch-tokens.json"),
            kick_tokens: root.join("kick-tokens.json"),
            twitch_delegated: root.join("twitch-delegated.json"),
            twitch_active_mode: root.join("twitch-active-mode.json"),
            fonts_dir: root.join("fonts"),
            events_media_dir: root.join("events-media"),
        };
        (root, paths)
    }

    #[test]
    fn backup_zip_contains_manifest_and_config() {
        let (root, paths) = temp_paths();
        let zip_bytes = build_backup_zip(&paths, None).expect("zip");
        let _ = fs::remove_dir_all(root);
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).expect("open zip");
        let mut manifest = String::new();
        archive
            .by_name("manifest.json")
            .expect("manifest")
            .read_to_string(&mut manifest)
            .expect("read");
        assert!(manifest.contains(BACKUP_FORMAT));
        assert!(archive.by_name("overlay-config.json").is_ok());
    }

    #[test]
    fn backup_zip_excludes_twitch_delegated() {
        let (root, paths) = temp_paths();
        fs::write(
            &paths.twitch_delegated,
            r#"{"connection_key":"ssk_secret","client_id":"cid","access_token":"tok","channel_login":"x","channel_twitch_id":"1","twitch_expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        let zip_bytes = build_backup_zip(&paths, None).expect("zip");
        let _ = fs::remove_dir_all(root);
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).expect("open zip");
        assert!(
            archive.by_name("twitch-delegated.json").is_err(),
            "takeover session must not appear in backup zip"
        );
    }
}
