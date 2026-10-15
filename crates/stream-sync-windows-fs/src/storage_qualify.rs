//! Destination-root storage qualification: local fixed NTFS or canonical UNC SMB share paths.

use std::io;
use std::path::Path;

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetDriveTypeW, GetVolumeInformationW, GetVolumePathNameW,
};

const DRIVE_FIXED: u32 = 3;
const DRIVE_REMOTE: u32 = 4;

/// Win32 `ERROR_INSUFFICIENT_BUFFER` / `ERROR_FILENAME_EXCED_RANGE` from `GetVolumePathNameW`.
const ERROR_INSUFFICIENT_BUFFER: i32 = 122;
const ERROR_FILENAME_EXCED_RANGE: i32 = 206;

/// Hard ceiling for `GetVolumePathNameW` wide-character buffer growth (documented limit for retry).
#[cfg_attr(not(windows), allow(dead_code))]
const VOLUME_PATH_BUFFER_CEILING: usize = 32_768;
const WIN32_MAX_PATH: usize = 260;

/// How a destination root was qualified (drives open-time guarantees).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestRootQualification {
    /// Local volume: `GetDriveTypeW` reports fixed disk and `GetVolumeInformationW` reports NTFS.
    LocalFixedNtfs,
    /// Canonical `\\server\share\...` UNC: structure validated lexically; directory open + reparse
    /// checks happen in `DestRoot::open` via `CreateFileW`. Remote filesystem type is **not**
    /// verified (SMB shares may be ext4, btrfs, etc. on the NAS).
    UncCanonicalShare,
}

#[derive(Debug)]
pub enum StorageQualifyError {
    /// Path begins with `\\` but is not a supported canonical UNC share path.
    InvalidUncPath {
        detail: String,
    },
    /// `\\?\` or `\\.\` device-namespace paths are not supported.
    DeviceNamespacePath,
    /// Disallowed `.` / `..` or empty share path component in a UNC path.
    UnsafeUncPath {
        detail: String,
    },
    UnsupportedDriveType {
        drive_type: u32,
    },
    UnsupportedFilesystem {
        filesystem: String,
    },
    Io(io::Error),
}

impl std::fmt::Display for StorageQualifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUncPath { detail } => write!(f, "invalid UNC path: {detail}"),
            Self::DeviceNamespacePath => write!(
                f,
                "device-namespace paths (\\\\?\\ or \\\\.\\) are not supported"
            ),
            Self::UnsafeUncPath { detail } => write!(f, "unsafe UNC path: {detail}"),
            Self::UnsupportedDriveType { drive_type } => {
                write!(
                    f,
                    "unsupported drive type {drive_type} (local fixed disk required; mapped network drives are not supported)"
                )
            }
            Self::UnsupportedFilesystem { filesystem } => {
                write!(
                    f,
                    "unsupported filesystem {filesystem:?} (NTFS required for local fixed destinations)"
                )
            }
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StorageQualifyError {}

/// Lexical destination-root shape (no I/O). Drives routing in [`qualify_dest_root`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexicalDestRoot {
    /// `\\?\` or `\\.\` Win32 device-namespace prefix.
    DeviceNamespace,
    /// Forward-slash `//server/share` (not supported).
    InvalidForwardSlashUnc,
    /// Backslash `\\server\share\...` candidate for canonical UNC validation.
    CanonicalUncShare,
    /// Local path candidate (drive-relative, rooted, or relative).
    LocalCandidate,
}

/// Pure cross-platform lexical classifier for destination-root qualification routing.
pub fn classify_lexical_dest_root(path: &Path) -> LexicalDestRoot {
    let s = path.to_string_lossy();
    if s.starts_with(r"\\?\") || s.starts_with(r"\\.\") {
        return LexicalDestRoot::DeviceNamespace;
    }
    if s.starts_with("//") {
        return LexicalDestRoot::InvalidForwardSlashUnc;
    }
    if s.starts_with(r"\\") {
        return LexicalDestRoot::CanonicalUncShare;
    }
    LexicalDestRoot::LocalCandidate
}

/// Returns true when `path` uses the canonical UNC share prefix `\\` (not device namespace).
pub fn path_looks_like_unc(path: &Path) -> bool {
    classify_lexical_dest_root(path) == LexicalDestRoot::CanonicalUncShare
}

fn ensure_local_lexical_dest_candidate(path: &Path) -> Result<(), StorageQualifyError> {
    match classify_lexical_dest_root(path) {
        LexicalDestRoot::DeviceNamespace => Err(StorageQualifyError::DeviceNamespacePath),
        LexicalDestRoot::InvalidForwardSlashUnc => Err(StorageQualifyError::InvalidUncPath {
            detail: "forward-slash UNC is not supported; use \\\\server\\share\\...".into(),
        }),
        LexicalDestRoot::CanonicalUncShare => Err(StorageQualifyError::InvalidUncPath {
            detail: "use qualify_dest_root for UNC paths".into(),
        }),
        LexicalDestRoot::LocalCandidate => Ok(()),
    }
}

fn qualify_dest_root_from_lexical(
    path: &Path,
) -> Result<DestRootQualification, StorageQualifyError> {
    match classify_lexical_dest_root(path) {
        LexicalDestRoot::DeviceNamespace => Err(StorageQualifyError::DeviceNamespacePath),
        LexicalDestRoot::InvalidForwardSlashUnc => Err(StorageQualifyError::InvalidUncPath {
            detail: "forward-slash UNC is not supported; use \\\\server\\share\\...".into(),
        }),
        LexicalDestRoot::CanonicalUncShare => {
            validate_canonical_unc_share_path(path)?;
            Ok(DestRootQualification::UncCanonicalShare)
        }
        LexicalDestRoot::LocalCandidate => {
            qualify_local_ntfs_dest_root(path)?;
            Ok(DestRootQualification::LocalFixedNtfs)
        }
    }
}

/// Pure lexical validation for canonical UNC share paths `\\server\share\...` (no I/O).
///
/// Rejects device namespaces, missing server/share, `.` / `..` components, and empty interior
/// segments. Does not contact the SMB server.
pub fn validate_canonical_unc_share_path(path: &Path) -> Result<(), StorageQualifyError> {
    let s = path.to_string_lossy();
    if !s.starts_with(r"\\") {
        return Err(StorageQualifyError::InvalidUncPath {
            detail: "path does not begin with \\\\".into(),
        });
    }
    if s.starts_with(r"\\?\") || s.starts_with(r"\\.\") {
        return Err(StorageQualifyError::DeviceNamespacePath);
    }
    if s.starts_with("//") {
        return Err(StorageQualifyError::InvalidUncPath {
            detail: "forward-slash UNC is not supported; use \\\\server\\share\\...".into(),
        });
    }
    let rest = s.trim_end_matches('\\');
    let body = rest.strip_prefix(r"\\").unwrap_or("");
    if body.is_empty() {
        return Err(StorageQualifyError::InvalidUncPath {
            detail: "missing server and share".into(),
        });
    }
    let parts: Vec<&str> = body.split('\\').collect();
    if parts.len() < 2 {
        return Err(StorageQualifyError::InvalidUncPath {
            detail: "UNC path must include \\\\server\\share".into(),
        });
    }
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            return Err(StorageQualifyError::InvalidUncPath {
                detail: format!("empty path component at index {i}"),
            });
        }
        if *part == "." || *part == ".." {
            return Err(StorageQualifyError::UnsafeUncPath {
                detail: format!("disallowed component {:?}", part),
            });
        }
    }
    Ok(())
}

/// Classify whether a path is a supported canonical UNC share root (lexical only).
pub fn classify_canonical_unc(path: &Path) -> Result<(), StorageQualifyError> {
    if classify_lexical_dest_root(path) != LexicalDestRoot::CanonicalUncShare {
        return Err(StorageQualifyError::InvalidUncPath {
            detail: "not a canonical UNC share path".into(),
        });
    }
    validate_canonical_unc_share_path(path)
}

/// Pure classification helpers (unit-tested without creating mapped/network volumes).
pub fn classify_drive_type(drive_type: u32) -> Result<(), StorageQualifyError> {
    if drive_type == DRIVE_REMOTE {
        return Err(StorageQualifyError::UnsupportedDriveType { drive_type });
    }
    if drive_type != DRIVE_FIXED {
        return Err(StorageQualifyError::UnsupportedDriveType { drive_type });
    }
    Ok(())
}

pub fn classify_filesystem_name(name: &str) -> Result<(), StorageQualifyError> {
    if name.eq_ignore_ascii_case("NTFS") {
        Ok(())
    } else {
        Err(StorageQualifyError::UnsupportedFilesystem {
            filesystem: name.to_string(),
        })
    }
}

/// `GetVolumePathNameW` signals an undersized buffer with either Win32 error code.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn get_volume_path_win32_error_is_retryable(raw_os_error: Option<i32>) -> bool {
    matches!(
        raw_os_error,
        Some(ERROR_INSUFFICIENT_BUFFER) | Some(ERROR_FILENAME_EXCED_RANGE)
    )
}

/// Initial TCHAR capacity: at least `MAX_PATH` and the input path length, capped at [`VOLUME_PATH_BUFFER_CEILING`].
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn initial_get_volume_path_buffer_capacity(
    input_wide_len_including_nul: usize,
    ceiling: usize,
) -> usize {
    input_wide_len_including_nul
        .max(WIN32_MAX_PATH)
        .min(ceiling)
}

/// Double capacity until [`ceiling`]; returns `None` when no larger size is available.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn next_get_volume_path_buffer_capacity(
    current: usize,
    ceiling: usize,
) -> Option<usize> {
    if current >= ceiling {
        return None;
    }
    let doubled = current.saturating_mul(2);
    let next = if doubled > current {
        doubled
    } else {
        current.saturating_add(1)
    };
    Some(next.min(ceiling))
}

/// `GetDriveTypeW` requires a trailing backslash on the volume root; content is before the final NUL.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn volume_root_has_trailing_backslash_before_nul(prefix: &[u16]) -> bool {
    if prefix.len() < 2 {
        return false;
    }
    let nul_idx = prefix.len() - 1;
    if prefix[nul_idx] != 0 {
        return false;
    }
    prefix[nul_idx - 1] == b'\\' as u16
}

#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) enum FinishGetVolumePathBuffer {
    Ok(Vec<u16>),
    Retry,
}

/// After `GetVolumePathNameW` reports success, extract a NUL-terminated root or request a larger buffer.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn finish_get_volume_path_buffer(
    buf: &[u16],
    current_cap: usize,
    ceiling: usize,
) -> Result<FinishGetVolumePathBuffer, io::Error> {
    let prefix = nul_terminated_utf16_prefix(buf)?;
    if volume_root_has_trailing_backslash_before_nul(&prefix) {
        return Ok(FinishGetVolumePathBuffer::Ok(prefix));
    }
    if current_cap >= ceiling {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetVolumePathNameW result missing trailing backslash before NUL (buffer truncation at ceiling)",
        ));
    }
    Ok(FinishGetVolumePathBuffer::Retry)
}

/// Win32 APIs that return `BOOL` leave a NUL-terminated string in a fixed buffer; scan for the first NUL.
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn nul_terminated_utf16_prefix(buf: &[u16]) -> Result<Vec<u16>, io::Error> {
    let nul_index = buf.iter().position(|&c| c == 0).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Win32 populated buffer missing NUL terminator",
        )
    })?;
    Ok(buf[..=nul_index].to_vec())
}

#[cfg(windows)]
fn wide_null_terminated(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

#[cfg(windows)]
fn win32_last_error() -> io::Error {
    io::Error::last_os_error()
}

#[cfg(windows)]
fn get_volume_path_root(path: &Path) -> Result<Vec<u16>, io::Error> {
    let wide = wide_null_terminated(path);
    let ceiling = VOLUME_PATH_BUFFER_CEILING;
    let mut cap = initial_get_volume_path_buffer_capacity(wide.len(), ceiling);
    loop {
        let mut buf = vec![0u16; cap];
        let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), buf.as_mut_ptr(), cap as u32) };
        if ok == 0 {
            let err = win32_last_error();
            if get_volume_path_win32_error_is_retryable(err.raw_os_error()) {
                match next_get_volume_path_buffer_capacity(cap, ceiling) {
                    Some(next) => {
                        cap = next;
                        continue;
                    }
                    None => return Err(err),
                }
            }
            return Err(err);
        }
        match finish_get_volume_path_buffer(&buf, cap, ceiling)? {
            FinishGetVolumePathBuffer::Ok(prefix) => return Ok(prefix),
            FinishGetVolumePathBuffer::Retry => {
                match next_get_volume_path_buffer_capacity(cap, ceiling) {
                    Some(next) => {
                        cap = next;
                        continue;
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "GetVolumePathNameW result missing trailing backslash before NUL (buffer truncation at ceiling)",
                        ));
                    }
                }
            }
        }
    }
}

/// Fail closed unless `path` resolves to a local fixed NTFS volume root (not UNC, not mapped remote).
#[cfg(windows)]
pub fn qualify_local_ntfs_dest_root(path: &Path) -> Result<(), StorageQualifyError> {
    ensure_local_lexical_dest_candidate(path)?;
    let volume_root = get_volume_path_root(path).map_err(StorageQualifyError::Io)?;
    let drive_type = unsafe { GetDriveTypeW(volume_root.as_ptr()) };
    classify_drive_type(drive_type)?;

    let mut fs_name = vec![0u16; 256];
    let ok = unsafe {
        GetVolumeInformationW(
            volume_root.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            fs_name.as_mut_ptr(),
            fs_name.len() as u32,
        )
    };
    if ok == 0 {
        return Err(StorageQualifyError::Io(win32_last_error()));
    }
    let end = fs_name
        .iter()
        .position(|&c| c == 0)
        .unwrap_or(fs_name.len());
    let name = String::from_utf16_lossy(&fs_name[..end]);
    classify_filesystem_name(&name)
}

/// Qualify a destination root: canonical UNC share (lexical) or local fixed NTFS (Win32 volume query).
#[cfg(windows)]
pub fn qualify_dest_root(path: &Path) -> Result<DestRootQualification, StorageQualifyError> {
    qualify_dest_root_from_lexical(path)
}

#[cfg(not(windows))]
pub fn qualify_local_ntfs_dest_root(path: &Path) -> Result<(), StorageQualifyError> {
    ensure_local_lexical_dest_candidate(path)?;
    Ok(())
}

#[cfg(not(windows))]
pub fn qualify_dest_root(path: &Path) -> Result<DestRootQualification, StorageQualifyError> {
    qualify_dest_root_from_lexical(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_unc_accepted_without_io() {
        assert!(validate_canonical_unc_share_path(Path::new(r"\\server\share")).is_ok());
        assert!(validate_canonical_unc_share_path(Path::new(r"\\server\share\dest\sub")).is_ok());
        assert!(validate_canonical_unc_share_path(Path::new(r"\\Hades\nas\DnD_Things")).is_ok());
        assert!(classify_canonical_unc(Path::new(r"\\server\share\root")).is_ok());
    }

    #[test]
    fn device_namespace_unc_rejected() {
        assert!(matches!(
            validate_canonical_unc_share_path(Path::new(r"\\?\UNC\server\share")),
            Err(StorageQualifyError::DeviceNamespacePath)
        ));
        assert!(matches!(
            validate_canonical_unc_share_path(Path::new(r"\\.\pipe\foo")),
            Err(StorageQualifyError::DeviceNamespacePath)
        ));
    }

    #[test]
    fn lexical_dest_root_classifier() {
        assert_eq!(
            classify_lexical_dest_root(Path::new(r"\\?\UNC\server\share")),
            LexicalDestRoot::DeviceNamespace
        );
        assert_eq!(
            classify_lexical_dest_root(Path::new(r"\\.\pipe\foo")),
            LexicalDestRoot::DeviceNamespace
        );
        assert_eq!(
            classify_lexical_dest_root(Path::new("//server/share")),
            LexicalDestRoot::InvalidForwardSlashUnc
        );
        assert_eq!(
            classify_lexical_dest_root(Path::new(r"\\server\share")),
            LexicalDestRoot::CanonicalUncShare
        );
        assert_eq!(
            classify_lexical_dest_root(Path::new("/tmp/dest")),
            LexicalDestRoot::LocalCandidate
        );
    }

    #[test]
    fn qualify_dest_root_lexical_routing() {
        assert_eq!(
            qualify_dest_root(Path::new(r"\\server\share\dest")).unwrap(),
            DestRootQualification::UncCanonicalShare
        );
        assert!(matches!(
            qualify_dest_root(Path::new(r"\\?\C:\temp")),
            Err(StorageQualifyError::DeviceNamespacePath)
        ));
        assert!(matches!(
            qualify_dest_root(Path::new(r"\\.\pipe\foo")),
            Err(StorageQualifyError::DeviceNamespacePath)
        ));
        assert!(matches!(
            qualify_dest_root(Path::new("//server/share")),
            Err(StorageQualifyError::InvalidUncPath { .. })
        ));
        #[cfg(not(windows))]
        assert_eq!(
            qualify_dest_root(Path::new("/tmp/local-dest")).unwrap(),
            DestRootQualification::LocalFixedNtfs
        );
    }

    #[test]
    fn qualify_local_ntfs_rejects_non_local_lexical_shapes() {
        assert!(matches!(
            qualify_local_ntfs_dest_root(Path::new(r"\\?\UNC\server\share")),
            Err(StorageQualifyError::DeviceNamespacePath)
        ));
        assert!(matches!(
            qualify_local_ntfs_dest_root(Path::new(r"\\server\share")),
            Err(StorageQualifyError::InvalidUncPath { .. })
        ));
        assert!(matches!(
            qualify_local_ntfs_dest_root(Path::new("//server/share")),
            Err(StorageQualifyError::InvalidUncPath { .. })
        ));
        #[cfg(not(windows))]
        qualify_local_ntfs_dest_root(Path::new("/tmp/local-dest")).unwrap();
    }

    #[test]
    fn malformed_unc_rejected() {
        assert!(matches!(
            validate_canonical_unc_share_path(Path::new(r"\\server")),
            Err(StorageQualifyError::InvalidUncPath { .. })
        ));
        assert!(matches!(
            validate_canonical_unc_share_path(Path::new(r"\\server\share\..")),
            Err(StorageQualifyError::UnsafeUncPath { .. })
        ));
        assert!(matches!(
            validate_canonical_unc_share_path(Path::new(r"//server/share")),
            Err(StorageQualifyError::InvalidUncPath { .. })
        ));
    }

    #[test]
    fn drive_type_table() {
        assert!(classify_drive_type(DRIVE_FIXED).is_ok());
        assert!(matches!(
            classify_drive_type(DRIVE_REMOTE),
            Err(StorageQualifyError::UnsupportedDriveType { .. })
        ));
        assert!(matches!(
            classify_drive_type(2),
            Err(StorageQualifyError::UnsupportedDriveType { .. })
        ));
    }

    #[test]
    fn filesystem_name_table() {
        assert!(classify_filesystem_name("NTFS").is_ok());
        assert!(classify_filesystem_name("ntfs").is_ok());
        assert!(matches!(
            classify_filesystem_name("ReFS"),
            Err(StorageQualifyError::UnsupportedFilesystem { .. })
        ));
        assert!(matches!(
            classify_filesystem_name("FAT32"),
            Err(StorageQualifyError::UnsupportedFilesystem { .. })
        ));
    }

    #[test]
    fn nul_terminated_utf16_prefix_extracts_through_nul() {
        let buf = vec![b'C' as u16, b':' as u16, b'\\' as u16, 0, b'X' as u16];
        let got = nul_terminated_utf16_prefix(&buf).expect("prefix");
        assert_eq!(got, vec![b'C' as u16, b':' as u16, b'\\' as u16, 0]);
        assert_eq!(got.len(), 4);
    }

    #[test]
    fn nul_terminated_utf16_prefix_errors_when_no_nul() {
        let buf = vec![b'C' as u16, b':' as u16];
        let err = nul_terminated_utf16_prefix(&buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn get_volume_path_win32_retryable_errors() {
        assert!(get_volume_path_win32_error_is_retryable(Some(122)));
        assert!(get_volume_path_win32_error_is_retryable(Some(206)));
        assert!(!get_volume_path_win32_error_is_retryable(Some(5)));
        assert!(!get_volume_path_win32_error_is_retryable(None));
    }

    #[test]
    fn volume_root_trailing_backslash_before_nul() {
        let valid = vec![b'C' as u16, b':' as u16, b'\\' as u16, 0];
        assert!(volume_root_has_trailing_backslash_before_nul(&valid));
        let truncated = vec![b'C' as u16, b':' as u16, 0];
        assert!(!volume_root_has_trailing_backslash_before_nul(&truncated));
    }

    #[test]
    fn finish_get_volume_path_buffer_accepts_well_formed_root() {
        let buf = vec![b'C' as u16, b':' as u16, b'\\' as u16, 0];
        let got =
            finish_get_volume_path_buffer(&buf, 260, VOLUME_PATH_BUFFER_CEILING).expect("finish");
        assert_eq!(
            got,
            FinishGetVolumePathBuffer::Ok(vec![b'C' as u16, b':' as u16, b'\\' as u16, 0])
        );
    }

    #[test]
    fn finish_get_volume_path_buffer_retries_when_backslash_missing() {
        let buf = vec![b'C' as u16, b':' as u16, 0, b'\\' as u16];
        let got =
            finish_get_volume_path_buffer(&buf, 2, VOLUME_PATH_BUFFER_CEILING).expect("finish");
        assert_eq!(got, FinishGetVolumePathBuffer::Retry);
    }

    #[test]
    fn finish_get_volume_path_buffer_invalid_at_ceiling_without_backslash() {
        let buf = vec![b'C' as u16, b':' as u16, 0];
        let err = finish_get_volume_path_buffer(
            &buf,
            VOLUME_PATH_BUFFER_CEILING,
            VOLUME_PATH_BUFFER_CEILING,
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn next_get_volume_path_buffer_capacity_stops_at_ceiling_without_overflow() {
        let ceiling = VOLUME_PATH_BUFFER_CEILING;
        let mut cap = 1usize;
        let mut steps = 0usize;
        while let Some(next) = next_get_volume_path_buffer_capacity(cap, ceiling) {
            assert!(next > cap);
            assert!(next <= ceiling);
            cap = next;
            steps += 1;
            assert!(steps < 64, "growth should converge quickly");
        }
        assert_eq!(cap, ceiling);
        assert!(next_get_volume_path_buffer_capacity(ceiling, ceiling).is_none());
        assert!(next_get_volume_path_buffer_capacity(usize::MAX, ceiling).is_none());
    }

    #[test]
    fn initial_get_volume_path_buffer_capacity_uses_path_and_max_path() {
        assert_eq!(
            initial_get_volume_path_buffer_capacity(10, VOLUME_PATH_BUFFER_CEILING),
            WIN32_MAX_PATH
        );
        assert_eq!(
            initial_get_volume_path_buffer_capacity(400, VOLUME_PATH_BUFFER_CEILING),
            400
        );
        assert_eq!(
            initial_get_volume_path_buffer_capacity(100_000, VOLUME_PATH_BUFFER_CEILING),
            VOLUME_PATH_BUFFER_CEILING
        );
    }

    #[test]
    #[cfg(windows)]
    fn env_temp_dir_is_local_ntfs_on_ci_baseline() {
        let tmp = std::env::temp_dir();
        qualify_local_ntfs_dest_root(&tmp).unwrap_or_else(|e| {
            panic!("windows-latest CI baseline expects local fixed NTFS temp_dir {tmp:?}: {e}");
        });
    }
}
