//! Durability helpers (`sync_dir_exact` targets the opened directory fd only).

use super::dir::DirHandle;
use super::error::FsError;

/// `fsync` the supplied directory handle (not its parent).
pub fn sync_dir_exact(dir: &DirHandle) -> Result<(), FsError> {
    #[cfg(unix)]
    {
        rustix::fs::fsync(dir.as_fd()).map_err(|e| FsError::Io(e.into()))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        super::windows::flush_dir_best_effort(dir)?;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = dir;
        Err(FsError::Unsupported)
    }
}
