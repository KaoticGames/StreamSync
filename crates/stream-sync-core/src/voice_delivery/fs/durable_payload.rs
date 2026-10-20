//! Create-new durable payload writes (never overwrite) with flush, fsync, and readback verify.

use super::dir::DirHandle;
use super::durability::sync_file;
use super::error::FsError;
use super::file::VoiceFile;
use crate::voice_delivery::hash::{hex_digest, sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::cell::Cell;
use std::io::{self, Write};
use std::path::Path;

#[cfg(test)]
thread_local! {
    static TEST_INJECT_SYNC_FAIL: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn test_inject_sync_fail(enabled: bool) {
    TEST_INJECT_SYNC_FAIL.set(enabled);
}

#[derive(Debug, Clone)]
pub struct DurablePayloadWrite {
    pub byte_length: u64,
    pub sha256: String,
}

/// Write `bytes` as a new file `name` under `dir` (create-new only), sync, and readback-verify digest.
pub fn write_durable_payload_create_new(
    dir: &DirHandle,
    name: &str,
    bytes: &[u8],
) -> Result<DurablePayloadWrite, FsError> {
    let mut file = dir.create_new_file(name)?;
    file.write_all_at(0, bytes)?;
    durable_finish_voice_file(&file, bytes)?;
    Ok(DurablePayloadWrite {
        byte_length: bytes.len() as u64,
        sha256: hex_digest(&Sha256::digest(bytes)),
    })
}

/// Create-new write at an absolute path (parent must exist).
pub fn write_durable_payload_at_path(
    path: &Path,
    bytes: &[u8],
) -> Result<DurablePayloadWrite, io::Error> {
    use std::fs::OpenOptions;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    #[cfg(test)]
    if TEST_INJECT_SYNC_FAIL.get() {
        return Err(io::Error::other("injected sync failure"));
    }
    file.sync_all()?;
    let readback = std::fs::read(path)?;
    if readback != bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "durable readback mismatch",
        ));
    }
    Ok(DurablePayloadWrite {
        byte_length: bytes.len() as u64,
        sha256: hex_digest(&Sha256::digest(bytes)),
    })
}

/// Stream-copy from `reader` into a create-new file at `dest_path`, hashing while copying.
pub fn copy_durable_create_new<R: io::Read>(
    dest_path: &Path,
    mut reader: R,
    expected_len: u64,
    expected_sha256: &str,
) -> Result<DurablePayloadWrite, io::Error> {
    use std::fs::OpenOptions;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest_path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; DEFAULT_STREAM_CHUNK];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    file.flush()?;
    #[cfg(test)]
    if TEST_INJECT_SYNC_FAIL.get() {
        return Err(io::Error::other("injected sync failure"));
    }
    file.sync_all()?;
    if total != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment length mismatch during snapshot copy",
        ));
    }
    let sha = hex_digest(&hasher.finalize());
    if sha != expected_sha256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment sha256 mismatch during snapshot copy",
        ));
    }
    let readback_sha = sha256_hex_reader(std::fs::File::open(dest_path)?, DEFAULT_STREAM_CHUNK)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    if readback_sha != expected_sha256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment readback digest mismatch",
        ));
    }
    Ok(DurablePayloadWrite {
        byte_length: total,
        sha256: sha,
    })
}

fn durable_finish_voice_file(file: &VoiceFile, expected: &[u8]) -> Result<(), FsError> {
    #[cfg(test)]
    if TEST_INJECT_SYNC_FAIL.get() {
        return Err(FsError::Io(io::Error::other("injected sync failure")));
    }
    sync_file(file)?;
    let len = file.len()?;
    if len != expected.len() as u64 {
        return Err(FsError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "durable length mismatch after write",
        )));
    }
    let mut readback = vec![0u8; expected.len()];
    file.read_exact_at(0, &mut readback)?;
    if readback != expected {
        return Err(FsError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "durable readback mismatch",
        )));
    }
    Ok(())
}

/// Ensure parent directories exist for `path` (single-file create-new helper).
pub fn ensure_parent(path: &Path) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;

    #[test]
    fn injected_sync_failure_surfaces_from_create_new_write() {
        test_inject_sync_fail(true);
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let dir = root.open_dir_relative(&[]).unwrap();
        assert!(write_durable_payload_create_new(&dir, "payload.txt", b"one").is_err());
        test_inject_sync_fail(false);
    }

    #[test]
    fn injected_sync_failure_is_thread_local() {
        test_inject_sync_fail(true);
        let child_result = std::thread::spawn(|| {
            let tmp = tempfile::tempdir().unwrap();
            let root = DestRoot::open(tmp.path()).unwrap();
            let dir = root.open_dir_relative(&[]).unwrap();
            write_durable_payload_create_new(&dir, "payload.txt", b"one")
        })
        .join()
        .unwrap();
        test_inject_sync_fail(false);
        assert!(child_result.is_ok());
    }

    #[test]
    fn durable_payload_rejects_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let dir = root.open_dir_relative(&[]).unwrap();
        write_durable_payload_create_new(&dir, "payload.txt", b"one").unwrap();
        assert!(write_durable_payload_create_new(&dir, "payload.txt", b"two").is_err());
    }
}
