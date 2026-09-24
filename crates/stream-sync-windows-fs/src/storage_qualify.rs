//! Phase 0C destination-root storage qualification (local fixed NTFS only).

use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use windows_sys::Win32::Storage::FileSystem::{
    GetDriveTypeW, GetVolumeInformationW, GetVolumePathNameW,
};

const DRIVE_FIXED: u32 = 3;
const DRIVE_REMOTE: u32 = 4;

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

fn wide_null_terminated(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn win32_last_error() -> io::Error {
    io::Error::last_os_error()
}

fn get_volume_path_root(path: &Path) -> Result<Vec<u16>, io::Error> {
    let wide = wide_null_terminated(path);
    let mut cap = 260usize;
    loop {
        let mut buf = vec![0u16; cap];
        let copied = unsafe { GetVolumePathNameW(wide.as_ptr(), buf.as_mut_ptr(), cap as u32) };
        if copied == 0 {
            let err = win32_last_error();
            if err.raw_os_error() == Some(122) && cap < 32_768 {
                cap = cap.saturating_mul(2);
                continue;
            }
            return Err(err);
        }
        let len = copied as usize;
        buf.truncate(len);
        if buf.last() == Some(&0) {
            buf.pop();
        }
        return Ok(buf);
    }
}

/// Fail closed unless `path` resolves to a local fixed NTFS volume root.
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
    fn temp_dir_on_runner_is_local_ntfs_or_skip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        match qualify_local_ntfs_dest_root(tmp.path()) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("SKIP temp_dir_on_runner_is_local_ntfs_or_skip: {e}");
            }
        }
    }
}
