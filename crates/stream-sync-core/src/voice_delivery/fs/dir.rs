//! Directory handles (capability-style); no ambient paths after acquisition.

use super::error::FsError;
use super::unix;
#[cfg(windows)]
use super::windows;
#[cfg(unix)]
use rustix::fd::AsFd;
use std::path::Path;

/// Owned directory handle — root or descendant of an opened `DestRoot`.
pub struct DirHandle {
    inner: DirHandleInner,
}

enum DirHandleInner {
    #[cfg(unix)]
    Unix(rustix::fd::OwnedFd),
    #[cfg(windows)]
    Windows(windows::OwnedDirHandle),
}

impl DirHandle {
    #[cfg(unix)]
    pub(crate) fn from_owned_fd(fd: rustix::fd::OwnedFd) -> Self {
        Self {
            inner: DirHandleInner::Unix(fd),
        }
    }

    #[cfg(windows)]
    pub(crate) fn from_windows(handle: windows::OwnedDirHandle) -> Self {
        Self {
            inner: DirHandleInner::Windows(handle),
        }
    }

    #[cfg(unix)]
    pub(crate) fn as_fd(&self) -> rustix::fd::BorrowedFd<'_> {
        match &self.inner {
            DirHandleInner::Unix(fd) => fd.as_fd(),
        }
    }

    #[cfg(windows)]
    pub(crate) fn raw_handle(&self) -> windows_sys::Win32::Foundation::HANDLE {
        match &self.inner {
            DirHandleInner::Windows(h) => h.raw(),
        }
    }

    #[cfg(windows)]
    pub(crate) fn windows_handle(&self) -> &windows::OwnedDirHandle {
        match &self.inner {
            DirHandleInner::Windows(h) => h,
        }
    }

    pub(crate) fn open_child_dir(&self, name: &str) -> Result<DirHandle, FsError> {
        super::validate_single_component(name)?;
        #[cfg(unix)]
        {
            unix::open_dir_at(self, name)
        }
        #[cfg(windows)]
        {
            windows::open_dir_at(self, name)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = name;
            Err(FsError::Unsupported)
        }
    }

    pub(crate) fn create_child_dir(&self, name: &str) -> Result<(), FsError> {
        super::validate_single_component(name)?;
        #[cfg(unix)]
        {
            unix::mkdir_at(self, name)
        }
        #[cfg(windows)]
        {
            windows::mkdir_at(self, name)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = name;
            Err(FsError::Unsupported)
        }
    }

    pub(crate) fn clone_handle(&self) -> Result<DirHandle, FsError> {
        #[cfg(unix)]
        {
            unix::clone_dir_handle(self)
        }
        #[cfg(windows)]
        {
            windows::clone_dir_handle(self)
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(FsError::Unsupported)
        }
    }
}

/// Configured publication root opened without following symlinks/reparse points.
pub struct DestRoot {
    handle: DirHandle,
}

impl DestRoot {
    /// Open `DEST_ROOT` from an absolute path (one-time bootstrap only).
    pub fn open(path: &Path) -> Result<Self, FsError> {
        #[cfg(unix)]
        {
            let handle = unix::open_root_dir(path)?;
            Ok(Self {
                handle: DirHandle::from_owned_fd(handle),
            })
        }
        #[cfg(windows)]
        {
            let handle = windows::open_root_dir(path)?;
            Ok(Self {
                handle: DirHandle::from_windows(handle),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Err(FsError::Unsupported)
        }
    }

    pub(crate) fn handle(&self) -> &DirHandle {
        &self.handle
    }

    /// Walk validated single-component segments relative to this root.
    pub fn open_dir_relative(&self, components: &[&str]) -> Result<DirHandle, FsError> {
        let mut current = self.handle.clone_handle()?;
        for comp in components {
            current = current.open_child_dir(comp)?;
        }
        Ok(current)
    }

    pub(crate) fn open_child_dir(&self, name: &str) -> Result<DirHandle, FsError> {
        self.handle.open_child_dir(name)
    }

    pub(crate) fn create_child_dir(&self, name: &str) -> Result<(), FsError> {
        self.handle.create_child_dir(name)
    }
}

pub(crate) fn validate_single_component(name: &str) -> Result<(), FsError> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(FsError::InvalidComponent(name.to_string()));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(FsError::InvalidComponent(name.to_string()));
    }
    Ok(())
}

/// Protocol-facing final session directory name (validated, not lossy-mapped).
pub struct ValidatedFinalName(String);

impl ValidatedFinalName {
    pub fn validate(name: &str) -> Result<Self, FsError> {
        validate_single_component(name)?;
        if name.starts_with(".streamsync-") {
            return Err(FsError::InvalidFinalName(
                "reserved .streamsync- prefix".into(),
            ));
        }
        if name.len() > 200 {
            return Err(FsError::InvalidFinalName("name too long".into()));
        }
        Ok(Self(name.to_string()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
