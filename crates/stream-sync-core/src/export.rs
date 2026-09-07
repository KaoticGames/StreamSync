//! User-data backup export as a ZIP archive.

use crate::storage::StoragePaths;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

const BACKUP_FORMAT: &str = "stream-sync-backup";
const BACKUP_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize)]
pub struct BackupManifest {
    pub format: &'static str,
    pub version: u32,
    pub exported_at: String,
    pub app_version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestoreReport {
    pub files_written: usize,
}

#[derive(Debug, Deserialize)]
struct BackupManifestInput {
    format: String,
}

/// Build a ZIP containing Stream Sync user data (configs, fonts, media, imports, logs).
pub fn build_backup_zip(paths: &StoragePaths, logs_dir: Option<&Path>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut zip = ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

        let manifest = BackupManifest {
            format: BACKUP_FORMAT,
            version: BACKUP_VERSION,
            exported_at: chrono::Utc::now().to_rfc3339(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        write_zip_bytes(&mut zip, "manifest.json", manifest_json.as_bytes(), options)?;

        let root = &paths.root;
        add_root_file(
            &mut zip,
            root,
            "dock-config.json",
            &paths.dock_config,
            options,
        )?;
        add_root_file(
            &mut zip,
            root,
            "overlay-config.json",
            &paths.overlay_config,
            options,
        )?;
        add_root_file(
            &mut zip,
            root,
            "events-overlay-config.json",
            &paths.events_overlay_config,
            options,
        )?;
        add_root_file(
            &mut zip,
            root,
            "discord-voice-config.json",
            &paths.discord_voice_config,
            options,
        )?;
        add_root_file(&mut zip, root, "profiles.json", &paths.profiles, options)?;
        // Intentionally exclude twitch-delegated.json — takeover keys must not leak via backup.

        add_dir_to_zip(&mut zip, root, &paths.fonts_dir, options)?;
        add_dir_to_zip(&mut zip, root, &paths.events_media_dir, options)?;

        let imports = root.join("imports");
        add_dir_to_zip(&mut zip, root, &imports, options)?;

        if let Some(logs) = logs_dir {
            add_dir_to_zip(&mut zip, root, logs, options)?;
        }

        zip.finish().context("finish zip")?;
    }
    Ok(buf)
}

/// Restore a backup ZIP into userData (config/media/imports only).
pub fn restore_backup_zip(paths: &StoragePaths, zip_bytes: &[u8]) -> Result<RestoreReport> {
    let mut archive =
        ZipArchive::new(std::io::Cursor::new(zip_bytes)).context("open backup zip")?;

    let mut manifest_raw = String::new();
    archive
        .by_name("manifest.json")
        .context("manifest.json missing from backup zip")?
        .read_to_string(&mut manifest_raw)
        .context("read manifest.json")?;
    let manifest: BackupManifestInput =
        serde_json::from_str(&manifest_raw).context("parse manifest.json")?;
    if manifest.format != BACKUP_FORMAT {
        anyhow::bail!(
            "unsupported backup format: expected {}, got {}",
            BACKUP_FORMAT,
            manifest.format
        );
    }

    // Validate all entry paths first so zip-slip fails before any writes happen.
    for i in 0..archive.len() {
        let entry = archive.by_index(i).context("read zip entry metadata")?;
        let name = entry.name();
        if name == "manifest.json" {
            continue;
        }
        sanitize_restore_relative_path(name)?;
    }

    let mut archive =
        ZipArchive::new(std::io::Cursor::new(zip_bytes)).context("re-open backup zip")?;
    let mut files_written = 0usize;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).context("read zip entry")?;
        let name = entry.name().to_string();
        if name == "manifest.json" {
            continue;
        }
        let rel = sanitize_restore_relative_path(&name)?;
        if should_skip_restore_path(&rel) || !is_restore_target_allowed(&rel) {
            continue;
        }

        let target = paths.root.join(&rel);
        if entry.is_dir() {
            fs::create_dir_all(&target).with_context(|| format!("mkdir {}", target.display()))?;
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("read zip entry {}", name))?;
        fs::write(&target, &bytes).with_context(|| format!("write {}", target.display()))?;
        files_written += 1;
    }

    Ok(RestoreReport { files_written })
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
    let data = fs::read(disk_path).with_context(|| format!("read {}", disk_path.display()))?;
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
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    if matches!(
        name,
        "twitch-tokens.json"
            | "kick-tokens.json"
            | "twitch-delegated.json"
            | "streamelements-session.json"
            | ".env"
            | "control-token.txt"
            | "dock-credentials.json"
            | ".streamsync-secret-store"
            | "tokens"
    ) {
        return true;
    }
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

fn should_skip_restore_path(path: &Path) -> bool {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if should_skip_backup_path(&current) {
            return true;
        }
    }
    false
}

fn sanitize_restore_relative_path(raw: &str) -> Result<PathBuf> {
    let normalized = raw.replace('\\', "/");
    let path = normalized.trim();
    if path.is_empty() {
        anyhow::bail!("zip entry has empty path");
    }
    if path.starts_with('/') || path.starts_with("//") {
        anyhow::bail!("zip entry path must be relative: {raw}");
    }
    if let Some(first) = path.split('/').next() {
        if first.len() >= 2 {
            let bytes = first.as_bytes();
            if bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
                anyhow::bail!("zip entry path must not use windows drive prefix: {raw}");
            }
        }
    }

    let mut rel = PathBuf::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            anyhow::bail!("zip-slip path rejected: {raw}");
        }
        if part.contains(':') {
            anyhow::bail!("zip entry path must not contain windows prefixes: {raw}");
        }
        rel.push(part);
    }

    if rel.as_os_str().is_empty() {
        anyhow::bail!("zip entry has empty normalized path: {raw}");
    }
    Ok(rel)
}

fn is_restore_target_allowed(path: &Path) -> bool {
    let components: Vec<_> = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();
    if components.is_empty() {
        return false;
    }
    if matches!(components[0], "fonts" | "events-media" | "imports") {
        return true;
    }
    if components.len() != 1 {
        return false;
    }
    matches!(
        components[0],
        "dock-config.json" | "overlay-config.json" | "events-overlay-config.json" | "profiles.json"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{paths_for_root, StoragePaths};
    use std::io::Read;
    use std::sync::atomic::{AtomicU64, Ordering};

    static EXPORT_TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_paths() -> (std::path::PathBuf, StoragePaths) {
        let n = EXPORT_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "stream-sync-export-test-{}-{n}",
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
            discord_voice_config: root.join("discord-voice-config.json"),
            profiles: root.join("profiles.json"),
            tokens_dir: root.join("tokens"),
            twitch_tokens: root.join("twitch-tokens.json"),
            kick_tokens: root.join("kick-tokens.json"),
            twitch_delegated: root.join("twitch-delegated.json"),
            twitch_delegated_revoked: root.join("twitch-delegated.revoked"),
            twitch_delegated_revoke_pending: root.join("twitch-delegated.revoke-pending"),
            twitch_active_mode: root.join("twitch-active-mode.json"),
            fonts_dir: root.join("fonts"),
            events_media_dir: root.join("events-media"),
            control_token: root.join("control-token.txt"),
            dock_credentials: root.join("dock-credentials.json"),
            twitch_tokens_rollback_pending: root.join("twitch-tokens.rollback-pending"),
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

    #[test]
    fn backup_zip_excludes_reusable_credentials() {
        let (root, paths) = temp_paths();

        fs::write(&paths.twitch_tokens, r#"{"accessToken":"tok"}"#).unwrap();
        fs::write(&paths.kick_tokens, r#"{"token":"kick"}"#).unwrap();
        fs::write(
            root.join("streamelements-session.json"),
            r#"{"session":"secret"}"#,
        )
        .unwrap();
        fs::write(root.join(".env"), "TWITCH_CLIENT_ID=abc").unwrap();
        fs::write(
            &paths.twitch_delegated,
            r#"{"connection_key":"ssk_secret","access_token":"tok"}"#,
        )
        .unwrap();
        fs::create_dir_all(&paths.tokens_dir).unwrap();
        fs::write(paths.tokens_dir.join("control-token.txt"), "control").unwrap();
        fs::write(&paths.control_token, "control").unwrap();
        fs::write(&paths.dock_credentials, r#"{"dock":"secret"}"#).unwrap();
        let secret_store = root.join(".streamsync-secret-store");
        fs::create_dir_all(&secret_store).unwrap();
        fs::write(secret_store.join("dummy"), "secret").unwrap();

        fs::create_dir_all(&paths.events_media_dir).unwrap();
        fs::write(paths.events_media_dir.join("foo.png"), "png").unwrap();
        fs::create_dir_all(&paths.fonts_dir).unwrap();
        fs::write(paths.fonts_dir.join("x.ttf"), "font").unwrap();
        let imports_se = root.join("imports").join("streamelements");
        fs::create_dir_all(&imports_se).unwrap();
        fs::write(imports_se.join("abc.json"), r#"{"ok":true}"#).unwrap();

        let zip_bytes = build_backup_zip(&paths, None).expect("zip");
        let _ = fs::remove_dir_all(&root);

        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).expect("open zip");
        let mut names = Vec::new();
        for i in 0..archive.len() {
            let file = archive.by_index(i).expect("zip entry");
            names.push(file.name().to_string());
        }
        let all_names = names.join("\n");

        for forbidden in [
            "twitch-tokens.json",
            "kick-tokens.json",
            "streamelements-session.json",
            ".env",
            "twitch-delegated.json",
            "tokens/",
            "control-token.txt",
            "dock-credentials.json",
            ".streamsync-secret-store",
            ".streamsync-secret-store/dummy",
        ] {
            assert!(
                !all_names.contains(forbidden),
                "backup zip should exclude {forbidden}, got entries: {all_names}"
            );
        }

        for expected in [
            "overlay-config.json",
            "events-media/foo.png",
            "fonts/x.ttf",
            "imports/streamelements/abc.json",
        ] {
            assert!(
                all_names.contains(expected),
                "backup zip should include {expected}, got entries: {all_names}"
            );
        }
    }

    #[test]
    fn restore_backup_roundtrip_keeps_config_media_and_imports() {
        let (src_root, src_paths) = temp_paths();
        let overlay_bytes = br#"{"profiles":{"chat-default":{"fontSize":14}}}"#.to_vec();
        let events_overlay_bytes =
            br#"{"profiles":{"default":{"profileName":"Default"}}}"#.to_vec();
        let media_bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a];
        let font_bytes = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let import_bytes = br#"{"overlayId":"abc","ok":true}"#.to_vec();

        fs::write(&src_paths.overlay_config, &overlay_bytes).unwrap();
        fs::write(&src_paths.events_overlay_config, &events_overlay_bytes).unwrap();
        fs::create_dir_all(src_paths.events_media_dir.join("default")).unwrap();
        fs::write(
            src_paths.events_media_dir.join("default").join("alert.png"),
            &media_bytes,
        )
        .unwrap();
        fs::create_dir_all(&src_paths.fonts_dir).unwrap();
        fs::write(src_paths.fonts_dir.join("test.ttf"), &font_bytes).unwrap();
        let imports_dir = src_root.join("imports").join("streamelements");
        fs::create_dir_all(&imports_dir).unwrap();
        fs::write(imports_dir.join("overlay.json"), &import_bytes).unwrap();

        let zip_bytes = build_backup_zip(&src_paths, None).expect("zip");

        let n = EXPORT_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let restore_root = std::env::temp_dir().join(format!(
            "stream-sync-restore-test-{}-{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&restore_root);
        fs::create_dir_all(&restore_root).unwrap();
        let restore_paths = paths_for_root(&restore_root, false).expect("restore paths");

        let report = restore_backup_zip(&restore_paths, &zip_bytes).expect("restore");
        assert!(report.files_written >= 5);

        assert_eq!(
            fs::read(&restore_paths.overlay_config).unwrap(),
            overlay_bytes
        );
        assert_eq!(
            fs::read(&restore_paths.events_overlay_config).unwrap(),
            events_overlay_bytes
        );
        assert_eq!(
            fs::read(
                restore_paths
                    .events_media_dir
                    .join("default")
                    .join("alert.png")
            )
            .unwrap(),
            media_bytes
        );
        assert_eq!(
            fs::read(restore_paths.fonts_dir.join("test.ttf")).unwrap(),
            font_bytes
        );
        assert_eq!(
            fs::read(
                restore_paths
                    .root
                    .join("imports")
                    .join("streamelements")
                    .join("overlay.json")
            )
            .unwrap(),
            import_bytes
        );

        let _ = fs::remove_dir_all(src_root);
        let _ = fs::remove_dir_all(restore_root);
    }

    #[test]
    fn restore_backup_skips_reusable_credentials() {
        let n = EXPORT_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let restore_root = std::env::temp_dir().join(format!(
            "stream-sync-restore-skip-test-{}-{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&restore_root);
        fs::create_dir_all(&restore_root).unwrap();
        let restore_paths = paths_for_root(&restore_root, false).expect("restore paths");

        let mut buf = Vec::new();
        {
            let mut zip = ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options =
                SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
            let manifest_json = serde_json::json!({
                "format": BACKUP_FORMAT,
                "version": BACKUP_VERSION,
                "exported_at": "2026-01-01T00:00:00Z",
                "app_version": "test",
            })
            .to_string();
            write_zip_bytes(&mut zip, "manifest.json", manifest_json.as_bytes(), options).unwrap();
            write_zip_bytes(&mut zip, "overlay-config.json", br#"{"ok":true}"#, options).unwrap();
            for entry in [
                "twitch-tokens.json",
                "kick-tokens.json",
                "streamelements-session.json",
                ".env",
                "twitch-delegated.json",
                "control-token.txt",
                "dock-credentials.json",
                "tokens/x",
                ".streamsync-secret-store/x",
            ] {
                write_zip_bytes(&mut zip, entry, b"secret", options).unwrap();
            }
            zip.finish().unwrap();
        }

        let report = restore_backup_zip(&restore_paths, &buf).expect("restore");
        assert_eq!(report.files_written, 1);
        assert_eq!(
            fs::read(&restore_paths.overlay_config).unwrap(),
            br#"{"ok":true}"#
        );

        for forbidden in [
            "twitch-tokens.json",
            "kick-tokens.json",
            "streamelements-session.json",
            ".env",
            "twitch-delegated.json",
            "control-token.txt",
            "dock-credentials.json",
            "tokens/x",
            ".streamsync-secret-store/x",
        ] {
            assert!(
                !restore_root.join(forbidden).exists(),
                "restore should skip {forbidden}"
            );
        }

        let _ = fs::remove_dir_all(restore_root);
    }

    #[test]
    fn restore_backup_rejects_zip_slip() {
        let n = EXPORT_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let restore_root = std::env::temp_dir().join(format!(
            "stream-sync-restore-slip-test-{}-{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&restore_root);
        fs::create_dir_all(&restore_root).unwrap();
        let restore_paths = paths_for_root(&restore_root, false).expect("restore paths");

        let outside = restore_root
            .parent()
            .unwrap_or(std::path::Path::new("/tmp"))
            .join("outside.txt");
        let _ = fs::remove_file(&outside);

        let mut buf = Vec::new();
        {
            let mut zip = ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options =
                SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
            let manifest_json = serde_json::json!({
                "format": BACKUP_FORMAT,
                "version": BACKUP_VERSION,
                "exported_at": "2026-01-01T00:00:00Z",
                "app_version": "test",
            })
            .to_string();
            write_zip_bytes(&mut zip, "manifest.json", manifest_json.as_bytes(), options).unwrap();
            write_zip_bytes(&mut zip, "overlay-config.json", br#"{"ok":true}"#, options).unwrap();
            write_zip_bytes(&mut zip, "../outside.txt", b"bad", options).unwrap();
            zip.finish().unwrap();
        }

        let err = restore_backup_zip(&restore_paths, &buf).expect_err("zip-slip must fail");
        assert!(
            err.to_string().contains("zip-slip")
                || err.to_string().contains("relative")
                || err.to_string().contains("windows"),
            "unexpected error: {err:#}"
        );
        assert!(
            !outside.exists(),
            "restore must not create files outside userdata"
        );
        assert!(
            !restore_paths.overlay_config.exists(),
            "restore should not write files when zip-slip is present"
        );

        let _ = fs::remove_file(outside);
        let _ = fs::remove_dir_all(restore_root);
    }
}
