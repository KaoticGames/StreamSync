//! Linux (and generic Unix) directory capability backend.

use super::dir::DirHandle;
use super::error::FsError;
use super::file::VoiceFile;
use rustix::fs::{mkdirat, open, openat, Mode, OFlags};
#[cfg(target_os = "linux")]
use rustix::fs::{renameat_with, RenameFlags};
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
        Err(rustix::io::Errno::NOENT) => Ok(()),
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

/// Descriptor-relative child probe: `Missing` or opened directory handle (no-follow).
pub(crate) fn probe_child_dir(
    parent: &DirHandle,
    name: &str,
) -> Result<super::dir::ChildDirProbe, FsError> {
    use rustix::fs::{statat, AtFlags, FileType};
    match statat(parent.as_fd(), name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => Ok(super::dir::ChildDirProbe::Missing),
        Err(e) => Err(FsError::Io(e.into())),
        Ok(stat) => {
            let ft = FileType::from_raw_mode(stat.st_mode);
            if ft == FileType::Symlink {
                return Err(FsError::SymlinkOrReparseComponent(name.to_string()));
            }
            if ft == FileType::Directory {
                open_dir_at(parent, name).map(super::dir::ChildDirProbe::Directory)
            } else {
                Err(FsError::NotADirectory(name.to_string()))
            }
        }
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
    let mode = Mode::RUSR | Mode::WUSR | Mode::XUSR;
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

pub(crate) fn open_file_at(
    parent: &DirHandle,
    name: &str,
    create_new: bool,
) -> Result<VoiceFile, FsError> {
    reject_symlink_at(parent, name)?;
    let flags = if create_new {
        OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL
    } else {
        OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW
    };
    let mode = Mode::RUSR | Mode::WUSR;
    match openat(parent.as_fd(), name, flags, mode) {
        Ok(fd) => Ok(VoiceFile::from_std_file(std::fs::File::from(fd))),
        Err(rustix::io::Errno::EXIST) => Err(FsError::AlreadyExists),
        Err(rustix::io::Errno::LOOP) => Err(FsError::SymlinkOrReparseComponent(name.to_string())),
        Err(e) => Err(FsError::Io(e.into())),
    }
}

pub(crate) fn open_file_create_or_open(
    parent: &DirHandle,
    name: &str,
) -> Result<VoiceFile, FsError> {
    reject_symlink_at(parent, name)?;
    let flags = OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE;
    let mode = Mode::RUSR | Mode::WUSR;
    match openat(parent.as_fd(), name, flags, mode) {
        Ok(fd) => Ok(VoiceFile::from_std_file(std::fs::File::from(fd))),
        Err(rustix::io::Errno::LOOP) => Err(FsError::SymlinkOrReparseComponent(name.to_string())),
        Err(e) => Err(FsError::Io(e.into())),
    }
}

pub(crate) fn list_child_names(parent: &DirHandle) -> Result<Vec<String>, FsError> {
    use rustix::fs::{Dir, OFlags};
    let fd = openat(
        parent.as_fd(),
        ".",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| FsError::Io(e.into()))?;
    let dir = Dir::new(fd).map_err(|e| FsError::Io(e.into()))?;
    let mut names = Vec::new();
    for entry in dir {
        let entry = entry.map_err(|e| FsError::Io(e.into()))?;
        let name = entry.file_name().to_string_lossy();
        if name == "." || name == ".." {
            continue;
        }
        names.push(name.into_owned());
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
            let flags = OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE;
            let mode = Mode::RUSR | Mode::WUSR;
            let fd = openat(dir.as_fd(), comp, flags, mode).map_err(|e| FsError::Io(e.into()))?;
            return Ok(std::fs::File::from(fd));
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
