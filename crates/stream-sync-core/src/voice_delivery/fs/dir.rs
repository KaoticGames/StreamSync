//! Directory handles for voice delivery publication geometry.
//!
//! **Unix:** descriptor-relative `openat`/`mkdirat` after `DestRoot` bootstrap — no ambient path strings on handles.
//!
//! **Windows:** holds a directory `HANDLE` plus a validated absolute path used for single-component
//! `CreateFileW` / `CreateDirectoryW` walks with reparse rejection (accidental/stale-state safety).
//! This is not Linux `openat` capability semantics and does not defend against malicious same-user
//! replacement or races on ancestor directories.

use super::error::FsError;
use super::unix;
#[cfg(windows)]
use super::windows;
#[cfg(unix)]
use rustix::fd::AsFd;
use std::path::Path;

/// Owned directory handle — root or descendant of an opened `DestRoot`.
///
/// On Unix, operations are relative to the held directory file descriptor. On Windows, child operations
/// use the stored validated absolute path and directory handle together (see module docs).
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

    /// Idempotent control-directory segment: create if missing, or open and revalidate if a peer won the race.
    pub(crate) fn create_or_open_child_dir(&self, name: &str) -> Result<DirHandle, FsError> {
        super::validate_single_component(name)?;
        match self.open_child_dir(name) {
            Ok(handle) => Ok(handle),
            Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                match self.create_child_dir(name) {
                    Ok(()) => self.open_child_dir(name),
                    Err(FsError::AlreadyExists) => self.open_child_dir(name),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
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
///
/// On Windows, `open` rejects UNC paths, mapped/network drives, and non-NTFS volumes (local fixed NTFS only).
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

/// Portable ASCII final-session component grammar (reject nonportable inputs; never map/sanitize).
///
/// Allowed bytes: `A–Z`, `a–z`, `0–9`, `-`, `_`, `.` (no leading/trailing `.` or space).
/// Max length 128. Rejects control chars, `:`, separators, Unicode, Windows reserved device
/// basenames (even with extension), and any `.streamsync-` prefix.
pub struct ValidatedFinalName(String);

const FINAL_NAME_MAX_LEN: usize = 128;

fn is_windows_reserved_device_basename(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    matches!(
        stem,
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn is_portable_final_name_byte(b: u8) -> bool {
    matches!(
        b,
        b'a'..=b'z'
            | b'A'..=b'Z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
    )
}

fn has_reserved_streamsync_prefix(name: &str) -> bool {
    const PREFIX: &str = ".streamsync-";
    name.len() >= PREFIX.len() && name[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
}

pub(crate) fn validate_portable_final_name(name: &str) -> Result<(), FsError> {
    validate_single_component(name)?;
    if has_reserved_streamsync_prefix(name) {
        return Err(FsError::InvalidFinalName(
            "reserved .streamsync- prefix".into(),
        ));
    }
    if name.is_empty() || name.len() > FINAL_NAME_MAX_LEN {
        return Err(FsError::InvalidFinalName("length out of range".into()));
    }
    if name.starts_with('.') || name.ends_with('.') || name.starts_with(' ') || name.ends_with(' ')
    {
        return Err(FsError::InvalidFinalName(
            "leading or trailing dot/space".into(),
        ));
    }
    if name.contains(':') {
        return Err(FsError::InvalidFinalName("colon not portable".into()));
    }
    if !name.bytes().all(is_portable_final_name_byte) {
        return Err(FsError::InvalidFinalName(
            "non-portable or non-ASCII character".into(),
        ));
    }
    if is_windows_reserved_device_basename(name) {
        return Err(FsError::InvalidFinalName(
            "windows reserved device name".into(),
        ));
    }
    Ok(())
}

impl ValidatedFinalName {
    pub fn validate(name: &str) -> Result<Self, FsError> {
        validate_portable_final_name(name)?;
        Ok(Self(name.to_string()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod validated_final_name_portable {
    use super::*;

    #[test]
    fn accepts_safe_ascii_names() {
        for name in ["my-session", "Guild_42", "a.b-c", "Z9"] {
            assert!(ValidatedFinalName::validate(name).is_ok());
        }
    }

    #[test]
    fn rejects_streamsync_prefix_case_insensitive() {
        let err = ValidatedFinalName::validate(".STREAMSYNC-reserved-name");
        assert!(matches!(err, Err(FsError::InvalidFinalName(_))));
    }

    #[test]
    fn rejects_nonportable_inputs() {
        let invalid = [
            "",
            ".hidden",
            "trail.",
            " lead",
            "trail ",
            "bad:name",
            "unicode-🎙",
            "CON",
            "con.txt",
            "LPT1.log",
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            &"x".repeat(129),
        ];
        for name in invalid {
            let err = ValidatedFinalName::validate(name);
            assert!(
                matches!(
                    err,
                    Err(FsError::InvalidFinalName(_)) | Err(FsError::InvalidComponent(_))
                ),
                "expected reject for {:?}",
                name
            );
        }
    }
}
