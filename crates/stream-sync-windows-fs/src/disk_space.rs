//! Available free bytes on the volume hosting a path (Windows).

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::Path;

#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

/// Result of querying free space for a path (no secrets; bounded for UI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskSpaceQuery {
    Ok { available_bytes: u64 },
    Failed { os_error_code: Option<i32> },
}

impl DiskSpaceQuery {
    pub fn available_bytes(&self) -> Option<u64> {
        match self {
            Self::Ok { available_bytes } => Some(*available_bytes),
            Self::Failed { .. } => None,
        }
    }
}

/// Query available bytes for the volume containing `path` via `GetDiskFreeSpaceExW`.
#[cfg(windows)]
pub fn query_available_bytes(path: &Path) -> DiskSpaceQuery {
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut free_bytes_available: u64 = 0;
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_bytes_available as *mut u64,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return DiskSpaceQuery::Failed {
            os_error_code: std::io::Error::last_os_error().raw_os_error(),
        };
    }
    DiskSpaceQuery::Ok {
        available_bytes: free_bytes_available,
    }
}

/// Returns available bytes for the volume containing `path`, or `None` if the query fails.
#[cfg(windows)]
pub fn available_bytes_for_path(path: &Path) -> Option<u64> {
    query_available_bytes(path).available_bytes()
}

#[cfg(not(windows))]
pub fn query_available_bytes(_path: &Path) -> DiskSpaceQuery {
    DiskSpaceQuery::Failed {
        os_error_code: None,
    }
}

#[cfg(not(windows))]
pub fn available_bytes_for_path(_path: &Path) -> Option<u64> {
    None
}
