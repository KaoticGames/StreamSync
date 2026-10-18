//! Immutable verified source snapshot under staging (TOCTOU guard).

use super::manifest::LocalFinalizationManifest;
use super::offline_journal_rerender::OfflineJournalRerenderError;
use crate::voice_delivery::fs::durable_payload::{
    copy_durable_create_new, ensure_parent, write_durable_payload_at_path,
};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

pub const OFFLINE_SOURCE_SNAPSHOT_BASENAME: &str = ".streamsync-offline-source-snapshot";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFileObservation {
    pub file_name: String,
    pub byte_length: u64,
    pub sha256: String,
}

/// Require `journal/segments` to contain exactly the manifest-declared `.synj` files (canonical names).
pub fn enumerate_journal_segments_exact(
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
) -> Result<(), OfflineJournalRerenderError> {
    let segments_dir = session_root.join("journal/segments");
    if !segments_dir.is_dir() {
        return Err(OfflineJournalRerenderError::SegmentVerification(
            "journal/segments missing".into(),
        ));
    }
    let expected: BTreeSet<String> = manifest
        .segments
        .iter()
        .map(|s| format!("{:08}.synj", s.segment_seq))
        .collect();
    let mut observed = BTreeSet::new();
    for entry in std::fs::read_dir(&segments_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".synj") {
            return Err(OfflineJournalRerenderError::SegmentVerification(format!(
                "unexpected non-segment file in journal/segments: {name}"
            )));
        }
        if !observed.insert(name.clone()) {
            return Err(OfflineJournalRerenderError::SegmentVerification(format!(
                "duplicate segment file {name}"
            )));
        }
    }
    if observed != expected {
        let extra = observed.difference(&expected).cloned().collect::<Vec<_>>();
        let missing = expected.difference(&observed).cloned().collect::<Vec<_>>();
        return Err(OfflineJournalRerenderError::SegmentVerification(format!(
            "segment directory set mismatch (extra={extra:?}, missing={missing:?})"
        )));
    }
    Ok(())
}

pub fn observe_journal_segments(
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
) -> Result<Vec<SegmentFileObservation>, OfflineJournalRerenderError> {
    let mut out = Vec::with_capacity(manifest.segments.len());
    for seg in &manifest.segments {
        let path = session_root
            .join("journal/segments")
            .join(format!("{:08}.synj", seg.segment_seq));
        let meta = std::fs::metadata(&path)?;
        let sha = sha256_hex_reader(File::open(&path)?, DEFAULT_STREAM_CHUNK)
            .map_err(|e| OfflineJournalRerenderError::SegmentVerification(e.to_string()))?;
        out.push(SegmentFileObservation {
            file_name: format!("{:08}.synj", seg.segment_seq),
            byte_length: meta.len(),
            sha256: sha,
        });
    }
    Ok(out)
}

fn observations_match(before: &[SegmentFileObservation], after: &[SegmentFileObservation]) -> bool {
    before.len() == after.len()
        && before.iter().zip(after.iter()).all(|(a, b)| {
            a.file_name == b.file_name && a.byte_length == b.byte_length && a.sha256 == b.sha256
        })
}

/// Copy manifest bytes and each segment once through an open read handle into `staging/snapshot/<session_id>/`.
pub fn materialize_verified_source_snapshot(
    staging: &Path,
    source_session_root: &Path,
    manifest: &LocalFinalizationManifest,
    manifest_bytes: &[u8],
) -> Result<(PathBuf, Vec<PathBuf>), OfflineJournalRerenderError> {
    enumerate_journal_segments_exact(source_session_root, manifest)?;
    let before = observe_journal_segments(source_session_root, manifest)?;

    let snapshot_session_root = staging
        .join(OFFLINE_SOURCE_SNAPSHOT_BASENAME)
        .join(&manifest.session_id);
    std::fs::create_dir_all(snapshot_session_root.join("journal/segments"))?;

    let manifest_dest = snapshot_session_root.join("manifest.json");
    ensure_parent(&manifest_dest).map_err(OfflineJournalRerenderError::Io)?;
    write_durable_payload_at_path(&manifest_dest, manifest_bytes)
        .map_err(OfflineJournalRerenderError::Io)?;

    let mut segment_paths = Vec::with_capacity(manifest.segments.len());
    for seg in &manifest.segments {
        let name = format!("{:08}.synj", seg.segment_seq);
        let source_path = source_session_root.join("journal/segments").join(&name);
        let dest_path = snapshot_session_root.join("journal/segments").join(&name);
        ensure_parent(&dest_path).map_err(OfflineJournalRerenderError::Io)?;
        let source_file = File::open(&source_path)?;
        copy_durable_create_new(&dest_path, source_file, seg.byte_length, &seg.sha256)
            .map_err(OfflineJournalRerenderError::Io)?;
        segment_paths.push(dest_path);
    }

    let after = observe_journal_segments(source_session_root, manifest)?;
    if !observations_match(&before, &after) {
        return Err(OfflineJournalRerenderError::SegmentVerification(
            "source journal/segments changed during snapshot copy".into(),
        ));
    }

    Ok((snapshot_session_root, segment_paths))
}

pub fn remove_source_snapshot(staging: &Path, session_id: &str) -> Result<(), io::Error> {
    let snapshot_root = staging
        .join(OFFLINE_SOURCE_SNAPSHOT_BASENAME)
        .join(session_id);
    if snapshot_root.exists() {
        std::fs::remove_dir_all(&snapshot_root)?;
    }
    let parent = staging.join(OFFLINE_SOURCE_SNAPSHOT_BASENAME);
    if parent.is_dir() && parent.read_dir()?.next().is_none() {
        std::fs::remove_dir(&parent)?;
    }
    Ok(())
}
