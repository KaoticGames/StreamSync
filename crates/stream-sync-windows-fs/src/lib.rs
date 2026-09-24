//! Narrow Win32 filesystem helpers shared by `stream-sync-core` voice delivery (Windows cfg only).

#[cfg(windows)]
pub mod rename_buffer;

pub mod storage_qualify;

#[cfg(windows)]
mod win {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use windows_sys::Win32::Foundation::{GetLastError, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FileRenameInfo, SetFileInformationByHandle, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_GENERIC_READ, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    use crate::rename_buffer::{build_no_replace_rename_buffer, built_rename_info_view};

    /// Compile-time layout check for `FILE_RENAME_INFO` buffer sizing used by voice delivery slice 1.
    pub fn file_rename_info_buffer_size_for_dst(dst: &str) -> usize {
        let wide_len = dst.encode_utf16().count();
        super::rename_buffer::file_rename_info_buffer_size(wide_len)
            .expect("fixture name fits")
            .0
    }

    pub fn set_file_rename_info_smoke(
        staging_handle: isize,
        parent_handle: isize,
        dst: &str,
    ) -> Result<(), u32> {
        let dst_wide: Vec<u16> = dst.encode_utf16().collect();
        let built =
            build_no_replace_rename_buffer(parent_handle as _, &dst_wide).map_err(|_| 1u32)?;
        let (ptr, size) = built_rename_info_view(&built);
        let ok = unsafe {
            SetFileInformationByHandle(
                staging_handle as _,
                FileRenameInfo,
                ptr as *mut c_void,
                size,
            )
        };
        if ok == 0 {
            return Err(unsafe { GetLastError() });
        }
        Ok(())
    }

    pub fn open_directory_handle(path: &Path) -> Result<isize, u32> {
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                FILE_GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(unsafe { GetLastError() });
        }
        Ok(handle as isize)
    }
}

#[cfg(windows)]
pub use win::{
    file_rename_info_buffer_size_for_dst, open_directory_handle, set_file_rename_info_smoke,
};

#[cfg(not(windows))]
pub fn non_windows_build_placeholder() {
    // Windows backend is cfg-gated; Linux CI compile-checks this crate for MSVC target.
}
