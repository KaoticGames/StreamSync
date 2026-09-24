//! Isolated stable Windows API signature check for Phase 0C delivery primitives.
//! Compiles without the full `stream-sync-core` dependency graph (no `ring` / MSVC linker).

#[cfg(windows)]
mod win {
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::{
        GetVolumeInformationW, MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    pub fn volume_serial_for_path(path: &Path) -> Result<u32, u32> {
        let mut probe = path.to_path_buf();
        while !probe.exists() {
            if !probe.pop() {
                break;
            }
        }
        let wide: Vec<u16> = probe.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut serial = 0u32;
        let ok = unsafe {
            GetVolumeInformationW(
                wide.as_ptr(),
                std::ptr::null_mut(),
                0,
                &mut serial,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if ok == 0 {
            return Err(unsafe { GetLastError() });
        }
        Ok(serial)
    }

    pub fn move_file_no_replace(src: &Path, dst: &Path) -> Result<(), u32> {
        let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
        let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
        let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), MOVEFILE_WRITE_THROUGH) };
        if ok == 0 {
            return Err(unsafe { GetLastError() });
        }
        Ok(())
    }

    pub fn move_file_replace(src: &Path, dst: &Path) -> Result<(), u32> {
        let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
        let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
        let flags = MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING;
        let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), flags) };
        if ok == 0 {
            return Err(unsafe { GetLastError() });
        }
        Ok(())
    }
}

#[cfg(windows)]
pub use win::{move_file_no_replace, move_file_replace, volume_serial_for_path};

#[cfg(not(windows))]
pub fn non_windows_build_placeholder() {}
