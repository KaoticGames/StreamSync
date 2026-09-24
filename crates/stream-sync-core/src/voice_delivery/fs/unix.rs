//! Linux (and generic Unix) directory capability backend.

use super::dir::DirHandle;
use super::error::FsError;
use rustix::fs::{mkdirat, open, openat, renameat_with, Mode, OFlags, RenameFlags};
use rustix::io::fcntl_dupfd_cloexec;
use std::io;
use std::path::Path;

fn reject_symlink_path(path: &Path) -> Result<(), FsError> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Err(FsError::SymlinkOrReparseRoot);
    }
    Ok(())
}

fn reject_symlink_at(parent: &DirHandle, name: &str) -> Result<(), FsError> {
    use rustix::fs::statat;
    use rustix::fs::AtFlags;
    use rustix::fs::FileType;
    match statat(parent.as_fd(), name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => {
            if FileType::from_raw_mode(stat.st_mode) == FileType::Symlink {
                return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
            }
            Ok(())
        }
        Err(e) => Err(FsError::Io(e.into())),
    }
}

pub(crate) fn open_root_dir(path: &Path) -> Result<rustix::fd::OwnedFd, FsError> {
    reject_symlink_path(path)?;
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NOCTTY;
    match open(path, flags, Mode::empty()) {
        Ok(fd) => Ok(fd),
        Err(rustix::io::Errno::LOOP) => Err(FsError::SymlinkOrReparseRoot),
        Err(e) => Err(FsError::Io(e.into())),
    }
}

pub(crate) fn open_dir_at(parent: &DirHandle, name: &str) -> Result<DirHandle, FsError> {
    reject_symlink_at(parent, name)?;
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NOCTTY;
    match openat(parent.as_fd(), name, flags, Mode::empty()) {
        Ok(fd) => Ok(DirHandle::from_owned_fd(fd)),
        Err(rustix::io::Errno::LOOP) => Err(FsError::SymlinkOrReparseComponent(name.to_string())),
        Err(e) => Err(FsError::Io(e.into())),
    }
}

pub(crate) fn mkdir_at(parent: &DirHandle, name: &str) -> Result<(), FsError> {
    let mode =
        Mode::RUSR | Mode::WUSR | Mode::XUSR | Mode::RGRP | Mode::XGRP | Mode::ROTH | Mode::XOTH;
    match mkdirat(parent.as_fd(), name, mode) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::EXIST) => Err(FsError::AlreadyExists),
        Err(e) => Err(FsError::Io(e.into())),
    }
}

/// Same final-parent fd for source and destination; `RENAME_NOREPLACE` on Linux.
pub(crate) fn rename_no_replace_same_parent(
    final_parent: &DirHandle,
    src_name: &str,
    dst_name: &str,
) -> Result<(), FsError> {
    #[cfg(target_os = "linux")]
    {
        match renameat_with(
            final_parent.as_fd(),
            src_name,
            final_parent.as_fd(),
            dst_name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => Ok(()),
            Err(rustix::io::Errno::EXIST) => Err(FsError::AlreadyExists),
            Err(e) => Err(FsError::Io(e.into())),
        }
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = (final_parent, src_name, dst_name);
        Err(FsError::Unsupported)
    }
}

pub(crate) fn clone_dir_handle(dir: &DirHandle) -> Result<DirHandle, FsError> {
    let dup = fcntl_dupfd_cloexec(dir.as_fd(), 0).map_err(|e| FsError::Io(e.into()))?;
    Ok(DirHandle::from_owned_fd(dup))
}

pub(crate) fn open_lock_file(
    root: &DirHandle,
    rel_components: &[String],
) -> Result<std::fs::File, FsError> {
    let mut dir = clone_dir_handle(root)?;
    for (i, comp) in rel_components.iter().enumerate() {
        if i == rel_components.len() - 1 {
            let flags = OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE;
            let mode = Mode::RUSR | Mode::WUSR;
            let fd = openat(dir.as_fd(), comp, flags, mode).map_err(|e| FsError::Io(e.into()))?;
            return Ok(std::fs::File::from(fd));
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
