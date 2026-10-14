//! Available free bytes on the volume hosting a path (Windows).

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::path::Path;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

/// Returns available bytes for the volume containing `path`, or `None` if the query fails.
#[cfg(windows)]
pub fn available_bytes_for_path(path: &Path) -> Option<u64> {
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
        return None;
    }
    Some(free_bytes_available)
}

#[cfg(not(windows))]
pub fn available_bytes_for_path(_path: &std::path::Path) -> Option<u64> {
    None
}
