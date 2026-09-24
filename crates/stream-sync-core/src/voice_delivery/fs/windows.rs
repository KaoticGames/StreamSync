//! Windows directory capability backend (absolute path walk + `FileRenameInfo`).

use super::dir::DirHandle;
use super::error::FsError;
use std::ffi::c_void;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{FromRawHandle, IntoRawHandle};
use std::path::{Path, PathBuf};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, DUPLICATE_SAME_ACCESS, ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS,
    HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, FileRenameInfo, GetFileInformationByHandleEx,
    SetFileInformationByHandle, DELETE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_RENAME_INFO,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_ATTRIBUTES, OPEN_ALWAYS,
    OPEN_EXISTING,
};

pub(crate) struct OwnedDirHandle {
    handle: HANDLE,
    /// Validated absolute path for single-component child walks (`hTemplateFile` is never used).
    absolute_path: PathBuf,
}

impl OwnedDirHandle {
    pub(crate) fn raw(&self) -> HANDLE {
        self.handle
    }

    pub(crate) fn absolute_path(&self) -> &Path {
        &self.absolute_path
    }

    pub(crate) fn duplicate(&self) -> Result<Self, FsError> {
        let mut dup = INVALID_HANDLE_VALUE;
        let ok = unsafe {
            windows_sys::Win32::Foundation::DuplicateHandle(
                windows_sys::Win32::System::Threading::GetCurrentProcess(),
                self.handle,
                windows_sys::Win32::System::Threading::GetCurrentProcess(),
                &mut dup,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(FsError::Io(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32,
            )));
        }
        Ok(Self {
            handle: dup,
            absolute_path: self.absolute_path.clone(),
        })
    }
}

impl Drop for OwnedDirHandle {
    fn drop(&mut self) {
        if self.handle != INVALID_HANDLE_VALUE && self.handle != 0 as HANDLE {
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn validated_absolute_path(path: &Path) -> Result<PathBuf, FsError> {
    let abs = std::fs::canonicalize(path).map_err(FsError::Io)?;
    if !abs.is_absolute() {
        return Err(FsError::InvalidComponent("path not absolute".into()));
    }
    Ok(abs)
}

fn map_exists_win32(err: u32) -> Option<FsError> {
    if err == ERROR_ALREADY_EXISTS || err == ERROR_FILE_EXISTS {
        Some(FsError::AlreadyExists)
    } else {
        None
    }
}

fn open_directory_at_path(path: &Path) -> Result<HANDLE, FsError> {
    let wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }
    Ok(handle)
}

fn is_reparse_point(handle: HANDLE) -> Result<bool, FsError> {
    use windows_sys::Win32::Storage::FileSystem::{FileAttributeTagInfo, FILE_ATTRIBUTE_TAG_INFO};
    let mut info = FILE_ATTRIBUTE_TAG_INFO {
        FileAttributes: 0,
        ReparseTag: 0,
    };
    let ok = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileAttributeTagInfo,
            &mut info as *mut _ as *mut c_void,
            std::mem::size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    };
    if ok == 0 {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }
    Ok((info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0)
}

fn file_rename_info_buffer_size(wchar_len: usize) -> Result<(usize, u32), FsError> {
    let name_bytes = wchar_len
        .checked_mul(2)
        .ok_or_else(|| FsError::InvalidComponent("rename name too long".into()))?;
    let with_nul = name_bytes
        .checked_add(2)
        .ok_or_else(|| FsError::InvalidComponent("rename name too long".into()))?;
    let base = std::mem::size_of::<FILE_RENAME_INFO>() - std::mem::size_of::<u16>();
    let total = base
        .checked_add(with_nul)
        .ok_or_else(|| FsError::InvalidComponent("rename name too long".into()))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| FsError::InvalidComponent("rename buffer size overflow".into()))?;
    Ok((total, total_u32))
}

pub(crate) fn open_root_dir(path: &Path) -> Result<OwnedDirHandle, FsError> {
    let abs = validated_absolute_path(path)?;
    let handle = open_directory_at_path(&abs)?;
    if is_reparse_point(handle)? {
        unsafe {
            CloseHandle(handle);
        }
        return Err(FsError::SymlinkOrReparseRoot);
    }
    Ok(OwnedDirHandle {
        handle,
        absolute_path: abs,
    })
}

pub(crate) fn open_dir_at(parent: &DirHandle, name: &str) -> Result<DirHandle, FsError> {
    let parent_path = parent.windows_handle().absolute_path();
    let child_path = parent_path.join(name);
    let handle = open_directory_at_path(&child_path)?;
    if is_reparse_point(handle)? {
        unsafe {
            CloseHandle(handle);
        }
        return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
    }
    Ok(DirHandle::from_windows(OwnedDirHandle {
        handle,
        absolute_path: child_path,
    }))
}

pub(crate) fn mkdir_at(parent: &DirHandle, name: &str) -> Result<(), FsError> {
    let parent_path = parent.windows_handle().absolute_path();
    let child_path = parent_path.join(name);
    let wide = wide_path(&child_path);
    let ok = unsafe { CreateDirectoryW(wide.as_ptr(), std::ptr::null()) };
    if ok == 0 {
        let err = unsafe { GetLastError() };
        if let Some(mapped) = map_exists_win32(err) {
            return Err(mapped);
        }
        return Err(FsError::Io(io::Error::from_raw_os_error(err as i32)));
    }
    let handle = open_directory_at_path(&child_path)?;
    if is_reparse_point(handle)? {
        unsafe {
            CloseHandle(handle);
        }
        return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
    }
    unsafe {
        CloseHandle(handle);
    }
    Ok(())
}

pub(crate) fn rename_no_replace_same_parent(
    final_parent: &DirHandle,
    src_name: &str,
    dst_name: &str,
) -> Result<(), FsError> {
    let parent_h = final_parent.raw_handle();
    let parent_path = final_parent.windows_handle().absolute_path();
    let src_path = parent_path.join(src_name);

    let src_wide = wide_path(&src_path);
    let src_handle = unsafe {
        CreateFileW(
            src_wide.as_ptr(),
            DELETE | FILE_WRITE_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if src_handle == INVALID_HANDLE_VALUE {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }
    if is_reparse_point(src_handle)? {
        unsafe {
            CloseHandle(src_handle);
        }
        return Err(FsError::SymlinkOrReparseComponent(src_name.to_string()));
    }

    let dst_wide: Vec<u16> = dst_name.encode_utf16().collect();
    let (buffer_size, buffer_size_u32) = file_rename_info_buffer_size(dst_wide.len())?;
    let mut buffer = vec![0u8; buffer_size];
    let info = buffer.as_mut_ptr() as *mut FILE_RENAME_INFO;
    let name_bytes = (dst_wide.len() * 2) as u32;
    unsafe {
        (*info).Anonymous.ReplaceIfExists = 0;
        (*info).RootDirectory = parent_h;
        (*info).FileNameLength = name_bytes;
        std::ptr::copy_nonoverlapping(
            dst_wide.as_ptr(),
            (*info).FileName.as_mut_ptr(),
            dst_wide.len(),
        );
        (*info).FileName[dst_wide.len()] = 0;
    }

    let ok = unsafe {
        SetFileInformationByHandle(
            src_handle,
            FileRenameInfo,
            buffer.as_mut_ptr() as *mut c_void,
            buffer_size_u32,
        )
    };
    unsafe {
        CloseHandle(src_handle);
    }
    if ok == 0 {
        let err = unsafe { GetLastError() };
        if let Some(mapped) = map_exists_win32(err) {
            return Err(mapped);
        }
        return Err(FsError::Io(io::Error::from_raw_os_error(err as i32)));
    }
    Ok(())
}

pub(crate) fn clone_dir_handle(dir: &DirHandle) -> Result<DirHandle, FsError> {
    Ok(DirHandle::from_windows(dir.windows_handle().duplicate()?))
}

pub(crate) fn open_lock_file(
    root: &DirHandle,
    rel_components: &[String],
) -> Result<std::fs::File, FsError> {
    let mut dir = clone_dir_handle(root)?;
    for (i, comp) in rel_components.iter().enumerate() {
        if i == rel_components.len() - 1 {
            let file_path = dir.windows_handle().absolute_path().join(comp);
            let wide = wide_path(&file_path);
            let handle = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    std::ptr::null(),
                    OPEN_ALWAYS,
                    windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL,
                    std::ptr::null_mut(),
                )
            };
            if handle == INVALID_HANDLE_VALUE {
                return Err(FsError::Io(io::Error::from_raw_os_error(
                    unsafe { GetLastError() } as i32,
                )));
            }
            return Ok(unsafe { std::fs::File::from_raw_handle(handle as _) });
        }
        match dir.open_child_dir(comp) {
            Ok(next) => dir = next,
            Err(FsError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                dir.create_child_dir(comp)?;
                dir = dir.open_child_dir(comp)?;
            }
            Err(e) => return Err(e),
        }
    }
    Err(FsError::InvalidComponent("empty lock path".into()))
}
