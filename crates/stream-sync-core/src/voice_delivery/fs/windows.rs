//! Windows directory capability backend (`CreateFileW` + `FileRenameInfo`).

use super::dir::DirHandle;
use super::error::FsError;
use std::ffi::c_void;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{FromRawHandle, IntoRawHandle};
use std::path::Path;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, DUPLICATE_SAME_ACCESS, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileRenameInfo, FlushFileBuffers, GetFileInformationByHandleEx,
    SetFileInformationByHandle, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_RENAME_INFO,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};

pub(crate) struct OwnedDirHandle {
    handle: HANDLE,
}

impl OwnedDirHandle {
    pub(crate) fn raw(&self) -> HANDLE {
        self.handle
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
        Ok(Self { handle: dup })
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

fn open_directory_handle(
    path: Option<&Path>,
    name: Option<&str>,
    parent: Option<HANDLE>,
    extra_flags: u32,
) -> Result<OwnedDirHandle, FsError> {
    let mut wide: Vec<u16> = if let Some(p) = path {
        wide_path(p)
    } else if let Some(n) = name {
        n.encode_utf16().chain(Some(0)).collect()
    } else {
        return Err(FsError::InvalidComponent("missing path".into()));
    };

    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | extra_flags,
            0,
        )
    };

    if handle == INVALID_HANDLE_VALUE {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }

    let _ = parent;
    Ok(OwnedDirHandle { handle })
}

fn is_reparse_point(handle: HANDLE) -> Result<bool, FsError> {
    use windows_sys::Win32::Storage::FileSystem::{
        FileAttributeTagInfo, GetFileInformationByHandleEx, FILE_ATTRIBUTE_TAG_INFO,
    };
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

pub(crate) fn open_root_dir(path: &Path) -> Result<OwnedDirHandle, FsError> {
    let h = open_directory_handle(Some(path), None, None, 0)?;
    if is_reparse_point(h.handle)? {
        return Err(FsError::SymlinkOrReparseRoot);
    }
    Ok(h)
}

pub(crate) fn open_dir_at(parent: &DirHandle, name: &str) -> Result<DirHandle, FsError> {
    let parent_h = parent.raw_handle();
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            parent_h,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }
    if is_reparse_point(handle)? {
        unsafe {
            CloseHandle(handle);
        }
        return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
    }
    Ok(DirHandle::from_windows(OwnedDirHandle { handle }))
}

pub(crate) fn mkdir_at(parent: &DirHandle, name: &str) -> Result<(), FsError> {
    use windows_sys::Win32::Storage::FileSystem::CREATE_NEW;
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            CREATE_NEW,
            FILE_FLAG_BACKUP_SEMANTICS,
            parent.raw_handle(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        let err = unsafe { GetLastError() };
        const ERROR_ALREADY_EXISTS: u32 = 183;
        if err == ERROR_ALREADY_EXISTS {
            return Err(FsError::AlreadyExists);
        }
        return Err(FsError::Io(io::Error::from_raw_os_error(err as i32)));
    }
    unsafe {
        CloseHandle(handle);
    }
    Ok(())
}

const DELETE: u32 = 0x0001_0000;
const FILE_WRITE_ATTRIBUTES: u32 = 0x0000_0100;

pub(crate) fn rename_no_replace_same_parent(
    final_parent: &DirHandle,
    src_name: &str,
    dst_name: &str,
) -> Result<(), FsError> {
    let parent_h = final_parent.raw_handle();
    let src_wide: Vec<u16> = src_name.encode_utf16().collect();
    let dst_wide: Vec<u16> = dst_name.encode_utf16().collect();

    let src_handle = unsafe {
        CreateFileW(
            src_wide.as_ptr(),
            DELETE | FILE_WRITE_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | 0x4, // FILE_SHARE_DELETE
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            parent_h,
        )
    };
    if src_handle == INVALID_HANDLE_VALUE {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
    }

    let name_bytes = (dst_wide.len() * 2) as u32;
    let buffer_size =
        std::mem::size_of::<FILE_RENAME_INFO>() - std::mem::size_of::<u16>() + name_bytes as usize;
    let mut buffer = vec![0u8; buffer_size];
    let info = buffer.as_mut_ptr() as *mut FILE_RENAME_INFO;
    unsafe {
        (*info).Anonymous.ReplaceIfExists = 0;
        (*info).RootDirectory = parent_h;
        (*info).FileNameLength = name_bytes;
        std::ptr::copy_nonoverlapping(
            dst_wide.as_ptr(),
            (*info).FileName.as_mut_ptr(),
            dst_wide.len(),
        );
    }

    let ok = unsafe {
        SetFileInformationByHandle(
            src_handle,
            FileRenameInfo,
            buffer.as_mut_ptr() as *mut c_void,
            buffer_size as u32,
        )
    };
    unsafe {
        CloseHandle(src_handle);
    }
    if ok == 0 {
        let err = unsafe { GetLastError() };
        const ERROR_ALREADY_EXISTS: u32 = 183;
        if err == ERROR_ALREADY_EXISTS {
            return Err(FsError::AlreadyExists);
        }
        return Err(FsError::Io(io::Error::from_raw_os_error(err as i32)));
    }
    Ok(())
}

pub(crate) fn flush_dir_best_effort(dir: &DirHandle) -> Result<(), FsError> {
    let ok = unsafe { FlushFileBuffers(dir.raw_handle()) };
    if ok == 0 {
        return Err(FsError::Io(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32,
        )));
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
            let wide: Vec<u16> = comp.encode_utf16().chain(Some(0)).collect();
            let handle = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    std::ptr::null(),
                    windows_sys::Win32::Storage::FileSystem::OPEN_ALWAYS,
                    FILE_FLAG_BACKUP_SEMANTICS,
                    dir.raw_handle(),
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
