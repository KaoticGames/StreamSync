//! Shared `FILE_RENAME_INFO` buffer sizing and construction for Phase 0C voice delivery.

use std::ffi::c_void;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem::FILE_RENAME_INFO;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameBufferError {
    NameTooLong,
    SizeOverflow,
}

/// Returns `(total_buffer_bytes, total_u32_for_api, file_name_length_bytes)`.
pub fn file_rename_info_buffer_size(
    wchar_len: usize,
) -> Result<(usize, u32, u32), RenameBufferError> {
    let name_bytes = wchar_len
        .checked_mul(2)
        .ok_or(RenameBufferError::NameTooLong)?;
    let with_nul = name_bytes
        .checked_add(2)
        .ok_or(RenameBufferError::NameTooLong)?;
    let base = std::mem::size_of::<FILE_RENAME_INFO>() - std::mem::size_of::<u16>();
    let total = base
        .checked_add(with_nul)
        .ok_or(RenameBufferError::NameTooLong)?;
    let total_u32 = u32::try_from(total).map_err(|_| RenameBufferError::SizeOverflow)?;
    let name_bytes_u32 = u32::try_from(name_bytes).map_err(|_| RenameBufferError::SizeOverflow)?;
    Ok((total, total_u32, name_bytes_u32))
}

pub struct BuiltFileRenameInfo {
    pub buffer: Vec<u8>,
    pub size_u32: u32,
}

/// Build a no-replace rename buffer (`ReplaceIfExists = 0`) with UTF-16 `dst` and trailing NUL.
pub fn build_no_replace_rename_buffer(
    parent_directory: HANDLE,
    dst_utf16: &[u16],
) -> Result<BuiltFileRenameInfo, RenameBufferError> {
    let (buffer_size, buffer_size_u32, name_bytes) = file_rename_info_buffer_size(dst_utf16.len())?;
    let mut buffer = vec![0u8; buffer_size];
    let info = buffer.as_mut_ptr() as *mut FILE_RENAME_INFO;
    let name_ptr = unsafe { (*info).FileName.as_mut_ptr() };
    unsafe {
        (*info).Anonymous.ReplaceIfExists = 0;
        (*info).RootDirectory = parent_directory;
        (*info).FileNameLength = name_bytes;
        std::ptr::copy_nonoverlapping(dst_utf16.as_ptr(), name_ptr, dst_utf16.len());
        *name_ptr.add(dst_utf16.len()) = 0;
    }
    Ok(BuiltFileRenameInfo {
        buffer,
        size_u32: buffer_size_u32,
    })
}

pub fn built_rename_info_view(buf: &BuiltFileRenameInfo) -> (*const c_void, u32) {
    (buf.buffer.as_ptr() as *const c_void, buf.size_u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_char_name_buffer_layout() {
        let dst = "published-session";
        let wide: Vec<u16> = dst.encode_utf16().collect();
        let built = build_no_replace_rename_buffer(0 as HANDLE, &wide).expect("build");
        let (total, size_u32, name_bytes) = file_rename_info_buffer_size(wide.len()).expect("size");
        assert_eq!(built.buffer.len(), total);
        assert_eq!(built.size_u32, size_u32);

        let info = built.buffer.as_ptr() as *const FILE_RENAME_INFO;
        assert_eq!(unsafe { (*info).FileNameLength }, name_bytes);
        assert_eq!(name_bytes as usize, wide.len() * 2);

        let name_ptr = unsafe { (*info).FileName.as_ptr() };
        let read_back: Vec<u16> =
            unsafe { std::slice::from_raw_parts(name_ptr, wide.len()) }.to_vec();
        assert_eq!(read_back, wide);
        assert_eq!(unsafe { *name_ptr.add(wide.len()) }, 0);
    }
}
