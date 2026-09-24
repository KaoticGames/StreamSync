//! Isolated stable Windows API signature check for Phase 0C delivery primitives.
//! Compiles without the full `stream-sync-core` dependency graph (no `ring` / MSVC linker).

#[cfg(windows)]
mod win {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FileRenameInfo, SetFileInformationByHandle, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_GENERIC_READ, FILE_RENAME_INFO, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    /// Compile-time layout check for `FILE_RENAME_INFO` buffer sizing used by voice delivery slice 1.
    pub fn file_rename_info_buffer_size_for_dst(dst: &str) -> usize {
        let wide_len = dst.encode_utf16().count();
        let name_bytes = wide_len * 2;
        std::mem::size_of::<FILE_RENAME_INFO>() - std::mem::size_of::<u16>() + name_bytes
    }

    pub fn set_file_rename_info_smoke(
        staging_handle: isize,
        parent_handle: isize,
        dst: &str,
    ) -> Result<(), u32> {
        let dst_wide: Vec<u16> = dst.encode_utf16().collect();
        let name_bytes = (dst_wide.len() * 2) as u32;
        let buffer_size = file_rename_info_buffer_size_for_dst(dst);
        let mut buffer = vec![0u8; buffer_size];
        let info = buffer.as_mut_ptr() as *mut FILE_RENAME_INFO;
        unsafe {
            (*info).Anonymous.ReplaceIfExists = 0;
            (*info).RootDirectory = parent_handle as _;
            (*info).FileNameLength = name_bytes;
            std::ptr::copy_nonoverlapping(
                dst_wide.as_ptr(),
                (*info).FileName.as_mut_ptr(),
                dst_wide.len(),
            );
        }
        let ok = unsafe {
            SetFileInformationByHandle(
                staging_handle as _,
                FileRenameInfo,
                buffer.as_mut_ptr() as *mut c_void,
                buffer_size as u32,
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
                0,
            )
        };
        if handle == 0 {
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
    // Full `stream-sync-core` Windows backend is cfg-gated; Linux CI validates this crate only.
}
