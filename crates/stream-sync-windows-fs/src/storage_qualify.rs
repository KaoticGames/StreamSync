//! Phase 0C destination-root storage qualification (local fixed NTFS only).

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

#[derive(Debug)]
pub enum StorageQualifyError {
    UncPath,
    UnsupportedDriveType { drive_type: u32 },
    UnsupportedFilesystem { filesystem: String },
    Io(io::Error),
}

impl std::fmt::Display for StorageQualifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UncPath => write!(f, "UNC paths are not supported for Phase 0C"),
            Self::UnsupportedDriveType { drive_type } => {
                write!(
                    f,
                    "unsupported drive type {drive_type} (local fixed disk required)"
                )
            }
            Self::UnsupportedFilesystem { filesystem } => {
                write!(
                    f,
                    "unsupported filesystem {filesystem:?} (NTFS required for Phase 0C baseline)"
                )
            }
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StorageQualifyError {}

/// Lexical UNC rejection before Win32 volume queries (deterministic, no I/O).
pub fn reject_lexical_unc(path: &Path) -> Result<(), StorageQualifyError> {
    let s = path.to_string_lossy();
    if s.starts_with(r"\\") {
        return Err(StorageQualifyError::UncPath);
    }
    Ok(())
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

/// Fail closed unless `path` resolves to a local fixed NTFS volume root.
#[cfg(windows)]
pub fn qualify_local_ntfs_dest_root(path: &Path) -> Result<(), StorageQualifyError> {
    reject_lexical_unc(path)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_unc_rejected_without_io() {
        let err = reject_lexical_unc(Path::new(r"\\server\share\dest"));
        assert!(matches!(err, Err(StorageQualifyError::UncPath)));
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
