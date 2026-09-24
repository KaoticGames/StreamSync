//! Aligned `FILE_RENAME_INFO` buffer sizing and construction for voice delivery.

use std::ffi::c_void;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem::FILE_RENAME_INFO;

const _: () = assert!(
    std::mem::align_of::<FILE_RENAME_INFO>() <= std::mem::align_of::<usize>(),
    "FILE_RENAME_INFO must fit in usize-aligned storage"
);

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

struct AlignedRenameStorage {
    words: Vec<usize>,
}

impl AlignedRenameStorage {
    fn new(api_byte_len: usize) -> Self {
        let word_bytes = std::mem::size_of::<usize>();
        let words_needed = api_byte_len.div_ceil(word_bytes);
        Self {
            words: vec![0; words_needed],
        }
    }

    fn as_mut_file_rename_info(&mut self) -> *mut FILE_RENAME_INFO {
        let ptr = self.words.as_mut_ptr();
        debug_assert_eq!(
            ptr as usize % std::mem::align_of::<FILE_RENAME_INFO>(),
            0,
            "FILE_RENAME_INFO storage must be aligned"
        );
        ptr as *mut FILE_RENAME_INFO
    }

    fn api_ptr(&self) -> *const c_void {
        self.words.as_ptr() as *const c_void
    }
}

pub struct BuiltFileRenameInfo {
    storage: AlignedRenameStorage,
    pub size_u32: u32,
}

/// Build a no-replace rename buffer (`ReplaceIfExists = 0`) with UTF-16 `dst` and trailing NUL.
pub fn build_no_replace_rename_buffer(
    parent_directory: HANDLE,
    dst_utf16: &[u16],
) -> Result<BuiltFileRenameInfo, RenameBufferError> {
    let (buffer_size, buffer_size_u32, name_bytes) = file_rename_info_buffer_size(dst_utf16.len())?;
    let mut storage = AlignedRenameStorage::new(buffer_size);
    let info = storage.as_mut_file_rename_info();
    let name_ptr = unsafe { (*info).FileName.as_mut_ptr() };
    unsafe {
        (*info).Anonymous.ReplaceIfExists = 0;
        (*info).RootDirectory = parent_directory;
        (*info).FileNameLength = name_bytes;
        std::ptr::copy_nonoverlapping(dst_utf16.as_ptr(), name_ptr, dst_utf16.len());
        *name_ptr.add(dst_utf16.len()) = 0;
    }
    Ok(BuiltFileRenameInfo {
        storage,
        size_u32: buffer_size_u32,
    })
}

impl BuiltFileRenameInfo {
    pub fn as_file_rename_info(&self) -> *const FILE_RENAME_INFO {
        self.storage.words.as_ptr() as *const FILE_RENAME_INFO
    }
}

pub fn built_rename_info_view(buf: &BuiltFileRenameInfo) -> (*const c_void, u32) {
    (buf.storage.api_ptr(), buf.size_u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_size_overflow_on_absurd_name() {
        let err = file_rename_info_buffer_size(usize::MAX);
        assert!(matches!(err, Err(RenameBufferError::NameTooLong)));
    }

    #[test]
    fn multi_char_name_buffer_layout_and_alignment() {
        let dst = "published-session";
        let wide: Vec<u16> = dst.encode_utf16().collect();
        let built = build_no_replace_rename_buffer(42 as HANDLE, &wide).expect("build");
        let (total, size_u32, name_bytes) = file_rename_info_buffer_size(wide.len()).expect("size");
        assert_eq!(built.size_u32, size_u32);
        assert_eq!(built.size_u32, total as u32);
        assert_eq!(
            built.as_file_rename_info() as usize % std::mem::align_of::<FILE_RENAME_INFO>(),
            0
        );

        let info = built.as_file_rename_info();
        assert_eq!(unsafe { (*info).Anonymous.ReplaceIfExists }, 0);
        assert_eq!(unsafe { (*info).RootDirectory }, 42 as HANDLE);
        assert_eq!(unsafe { (*info).FileNameLength }, name_bytes);
        assert_eq!(name_bytes as usize, wide.len() * 2);

        let name_ptr = unsafe { (*info).FileName.as_ptr() };
        let read_back: Vec<u16> =
            unsafe { std::slice::from_raw_parts(name_ptr, wide.len()) }.to_vec();
        assert_eq!(read_back, wide);
        assert_eq!(unsafe { *name_ptr.add(wide.len()) }, 0);

        let (ptr, api_size) = built_rename_info_view(&built);
        assert_eq!(api_size, size_u32);
        assert_eq!(ptr, built.storage.api_ptr());
    }
}
