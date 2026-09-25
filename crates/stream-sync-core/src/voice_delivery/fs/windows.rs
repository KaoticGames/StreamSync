//! Windows directory backend: validated absolute-path walks, reparse rejection, and `FileRenameInfo`.
//!
//! Same-parent rename uses cooperative geometry: source and destination absolute paths are siblings
//! under the stored validated `final_parent.absolute_path()` plus validated basenames. The source is
//! opened with reparse inspection and DELETE access; `SetFileInformationByHandle(FileRenameInfo)` uses
//! `RootDirectory = NULL` and the full absolute destination UTF-16 path (`ReplaceIfExists = FALSE`).
//! Atomic no-replace on local NTFS holds, but path resolution is cooperative (accidental/stale state),
//! not a defense against malicious same-user ancestor replacement.

use super::dir::DirHandle;
use super::error::FsError;
use super::file::VoiceFile;
use std::ffi::c_void;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::path::{Path, PathBuf};
use stream_sync_windows_fs::rename_buffer::{
    build_no_replace_rename_buffer, built_rename_info_view, RenameBufferError,
};
use stream_sync_windows_fs::storage_qualify::StorageQualifyError;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, DUPLICATE_SAME_ACCESS, ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS,
    HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, GetFileInformationByHandleEx, SetFileInformationByHandle,
    DELETE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_RENAME_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, FILE_WRITE_ATTRIBUTES, OPEN_ALWAYS, OPEN_EXISTING,
};

fn map_rename_buffer_err(e: RenameBufferError) -> FsError {
    match e {
        RenameBufferError::NameTooLong | RenameBufferError::SizeOverflow => {
            FsError::InvalidComponent("rename name too long".into())
        }
    }
}

fn map_storage_qualify_err(e: StorageQualifyError) -> FsError {
    FsError::UnsupportedStorage(e.to_string())
}

pub(crate) struct OwnedDirHandle {
    handle: HANDLE,
    /// Validated absolute path for single-component child walks (`hTemplateFile` is never used).
    absolute_path: PathBuf,
}

// SAFETY: Each `OwnedDirHandle` exclusively owns its Win32 `HANDLE` and calls `CloseHandle` in
// `Drop` (or transfers sole ownership via `into_std_file`, which forgets this wrapper). Values
// produced by `duplicate()` are independent kernel handles with the same single-owner invariant.
// `HANDLE` is not `Send` in the type system because it is a raw pointer; moving this struct
// between threads does not share mutable access to the handle — callers must still synchronize
// concurrent directory operations separately.
unsafe impl Send for OwnedDirHandle {}

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

/// Closes the Win32 handle on drop unless consumed via `into_std_file`.
struct OwnedWinHandle(HANDLE);

impl OwnedWinHandle {
    fn from_create_result(handle: HANDLE) -> Result<Self, FsError> {
        if handle == INVALID_HANDLE_VALUE {
            return Err(FsError::Io(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32,
            )));
        }
        Ok(Self(handle))
    }

    fn raw(&self) -> HANDLE {
        self.0
    }

    fn ensure_not_reparse_root(&self) -> Result<(), FsError> {
        if is_reparse_point(self.raw())? {
            return Err(FsError::SymlinkOrReparseRoot);
        }
        Ok(())
    }

    fn ensure_not_reparse_component(&self, name: &str) -> Result<(), FsError> {
        if is_reparse_point(self.raw())? {
            return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
        }
        Ok(())
    }

    fn into_std_file(self) -> std::fs::File {
        let handle = self.0;
        std::mem::forget(self);
        unsafe { std::fs::File::from_raw_handle(handle as _) }
    }

    fn into_dir_handle(self, absolute_path: PathBuf) -> OwnedDirHandle {
        let handle = self.0;
        std::mem::forget(self);
        OwnedDirHandle {
            handle,
            absolute_path,
        }
    }
}

impl Drop for OwnedWinHandle {
    fn drop(&mut self) {
        if self.0 != INVALID_HANDLE_VALUE && self.0 != 0 as HANDLE {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

/// Lexical absolute path without dereferencing filesystem links (unlike `canonicalize`).
fn validated_absolute_path(path: &Path) -> Result<PathBuf, FsError> {
    let abs = std::path::absolute(path).map_err(FsError::Io)?;
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

fn open_directory_at_path(path: &Path) -> Result<OwnedWinHandle, FsError> {
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
    OwnedWinHandle::from_create_result(handle)
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

pub(crate) fn open_root_dir(path: &Path) -> Result<OwnedDirHandle, FsError> {
    let abs = validated_absolute_path(path)?;
    stream_sync_windows_fs::storage_qualify::qualify_local_ntfs_dest_root(&abs)
        .map_err(map_storage_qualify_err)?;
    let opened = open_directory_at_path(&abs)?;
    opened.ensure_not_reparse_root()?;
    Ok(opened.into_dir_handle(abs))
}

pub(crate) fn probe_child_dir(
    parent: &DirHandle,
    name: &str,
) -> Result<super::dir::ChildDirProbe, FsError> {
    let parent_path = parent.windows_handle().absolute_path();
    let child_path = parent_path.join(name);
    match open_directory_at_path(&child_path) {
        Ok(opened) => {
            if is_reparse_point(opened.raw())? {
                return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
            }
            Ok(super::dir::ChildDirProbe::Directory(
                DirHandle::from_windows(opened.into_dir_handle(child_path)),
            ))
        }
        Err(FsError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
            Ok(super::dir::ChildDirProbe::Missing)
        }
        Err(FsError::Io(e))
            if e.kind() == io::ErrorKind::PermissionDenied || e.raw_os_error() == Some(5) =>
        {
            Err(FsError::Io(e))
        }
        Err(FsError::Io(e)) => {
            use windows_sys::Win32::Storage::FileSystem::{
                CreateFileW, GetFileAttributesW, FILE_ATTRIBUTE_DIRECTORY,
            };
            let wide = wide_path(&child_path);
            let attrs = unsafe { GetFileAttributesW(wide.as_ptr()) };
            if attrs == u32::MAX {
                let err = unsafe { GetLastError() };
                if err == windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND
                    || err == windows_sys::Win32::Foundation::ERROR_PATH_NOT_FOUND
                {
                    return Ok(super::dir::ChildDirProbe::Missing);
                }
                return Err(FsError::Io(io::Error::from_raw_os_error(err as i32)));
            }
            if (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0 {
                if (attrs & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
                    return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
                }
                return Err(FsError::NotADirectory(name.to_string()));
            }
            Err(FsError::Io(e))
        }
        Err(other) => Err(other),
    }
}

pub(crate) fn open_dir_at(parent: &DirHandle, name: &str) -> Result<DirHandle, FsError> {
    let parent_path = parent.windows_handle().absolute_path();
    let child_path = parent_path.join(name);
    let opened = open_directory_at_path(&child_path)?;
    opened.ensure_not_reparse_component(name)?;
    Ok(DirHandle::from_windows(opened.into_dir_handle(child_path)))
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
    let opened = open_directory_at_path(&child_path)?;
    opened.ensure_not_reparse_component(name)?;
    Ok(())
}

pub(crate) fn rename_no_replace_same_parent(
    final_parent: &DirHandle,
    src_name: &str,
    dst_name: &str,
) -> Result<(), FsError> {
    let parent_path = final_parent.windows_handle().absolute_path();
    let src_path = parent_path.join(src_name);
    let dst_path = parent_path.join(dst_name);

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
    let src = OwnedWinHandle::from_create_result(src_handle)?;
    src.ensure_not_reparse_component(src_name)?;

    let dst_wide: Vec<u16> = dst_path.as_os_str().encode_wide().collect();
    let built = build_no_replace_rename_buffer(&dst_wide).map_err(map_rename_buffer_err)?;
    let (ptr, size) = built_rename_info_view(&built);

    let ok = unsafe {
        SetFileInformationByHandle(
            src.raw(),
            windows_sys::Win32::Storage::FileSystem::FileRenameInfo,
            ptr as *mut c_void,
            size,
        )
    };
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

fn open_file_at_path(path: &Path, create_new: bool) -> Result<VoiceFile, FsError> {
    let wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            if create_new {
                windows_sys::Win32::Storage::FileSystem::CREATE_NEW
            } else {
                OPEN_EXISTING
            },
            FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    match OwnedWinHandle::from_create_result(handle) {
        Ok(h) => {
            h.ensure_not_reparse_component(
                path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
            )?;
            Ok(VoiceFile::from_std_file(h.into_std_file()))
        }
        Err(FsError::Io(e)) if e.raw_os_error() == Some(ERROR_FILE_EXISTS as i32) => {
            Err(FsError::AlreadyExists)
        }
        Err(e) => Err(e),
    }
}

pub(crate) fn open_file_at(
    parent: &DirHandle,
    name: &str,
    create_new: bool,
) -> Result<VoiceFile, FsError> {
    let path = parent.windows_handle().absolute_path().join(name);
    open_file_at_path(&path, create_new)
}

pub(crate) fn open_file_create_or_open(
    parent: &DirHandle,
    name: &str,
) -> Result<VoiceFile, FsError> {
    let path = parent.windows_handle().absolute_path().join(name);
    let wide = wide_path(&path);
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_ALWAYS,
            FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    let opened = OwnedWinHandle::from_create_result(handle)?;
    opened.ensure_not_reparse_component(name)?;
    Ok(VoiceFile::from_std_file(opened.into_std_file()))
}

pub(crate) fn list_child_names(parent: &DirHandle) -> Result<Vec<String>, FsError> {
    let path = parent.windows_handle().absolute_path();
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&path).map_err(FsError::from)? {
        let entry = entry.map_err(FsError::from)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "." || name == ".." {
            continue;
        }
        names.push(name);
    }
    Ok(names)
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
                    FILE_FLAG_OPEN_REPARSE_POINT,
                    std::ptr::null_mut(),
                )
            };
            let opened = OwnedWinHandle::from_create_result(handle)?;
            opened.ensure_not_reparse_component(comp)?;
            return Ok(opened.into_std_file());
        }
        match dir.open_child_dir(comp) {
            Ok(next) => dir = next,
            Err(FsError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                dir = dir.create_or_open_child_dir(comp)?;
            }
            Err(e) => return Err(e),
        }
    }
    Err(FsError::InvalidComponent("empty lock path".into()))
}

#[cfg(test)]
mod file_rename_buffer_tests {
    use super::*;
    use stream_sync_windows_fs::rename_buffer::file_rename_info_buffer_size;

    #[test]
    fn multi_char_rename_buffer_matches_shared_helper() {
        let dst = r"C:\temp\final-parent\published-session";
        let wide: Vec<u16> = dst.encode_utf16().collect();
        let built = build_no_replace_rename_buffer(&wide).expect("build rename buffer");
        let (_, size_u32, name_bytes) =
            file_rename_info_buffer_size(wide.len()).expect("size helper");
        assert_eq!(built.size_u32, size_u32);
        assert_eq!(
            built.as_file_rename_info() as usize % std::mem::align_of::<FILE_RENAME_INFO>(),
            0
        );

        let info = built.as_file_rename_info();
        assert_eq!(unsafe { (*info).RootDirectory }, 0 as HANDLE);
        assert_eq!(unsafe { (*info).FileNameLength }, name_bytes);
        let name_ptr = unsafe { (*info).FileName.as_ptr() };
        let read_back: Vec<u16> =
            unsafe { std::slice::from_raw_parts(name_ptr, wide.len()) }.to_vec();
        assert_eq!(read_back, wide);
        assert_eq!(unsafe { *name_ptr.add(wide.len()) }, 0);
    }
}

#[cfg(test)]
mod dest_root_storage_qualify {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use stream_sync_windows_fs::storage_qualify::reject_lexical_unc;

    #[test]
    fn unc_lexical_fixture_rejected_at_open() {
        match DestRoot::open(std::path::Path::new(r"\\server\share\root")) {
            Ok(_) => panic!("expected unsupported storage, got Ok DestRoot"),
            Err(FsError::UnsupportedStorage(_)) => {}
            Err(other) => panic!("expected unsupported storage, got {other:?}"),
        }
        assert!(reject_lexical_unc(std::path::Path::new(r"\\server\share\root")).is_err());
    }
}
