//! Handle-owned regular files (no-follow open from directory fd).

use super::dir::DirHandle;
use super::error::FsError;
use std::io::{Read, Seek, SeekFrom, Write};

pub struct VoiceFile {
    file: std::fs::File,
}

impl VoiceFile {
    pub(crate) fn from_std_file(file: std::fs::File) -> Self {
        Self { file }
    }

    pub fn len(&self) -> Result<u64, FsError> {
        let meta = self.file.metadata().map_err(FsError::from)?;
        Ok(meta.len())
    }

    pub fn set_len(&self, len: u64) -> Result<(), FsError> {
        self.file.set_len(len).map_err(FsError::from)
    }

    pub fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), FsError> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset)).map_err(FsError::from)?;
        file.read_exact(buf).map_err(FsError::from)
    }

    pub fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), FsError> {
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(FsError::from)?;
        self.file.write_all(data).map_err(FsError::from)
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), FsError> {
        self.file.seek(SeekFrom::End(0)).map_err(FsError::from)?;
        self.file.write_all(data).map_err(FsError::from)
    }

    pub fn std_file(&self) -> &std::fs::File {
        &self.file
    }

    pub fn std_file_mut(&mut self) -> &mut std::fs::File {
        &mut self.file
    }
}

pub(crate) fn open_existing_file_at(dir: &DirHandle, name: &str) -> Result<VoiceFile, FsError> {
    super::validate_single_component(name)?;
    #[cfg(unix)]
    {
        super::unix::open_file_at(dir, name, false)
    }
    #[cfg(windows)]
    {
        super::windows::open_file_at(dir, name, false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (dir, name);
        Err(FsError::Unsupported)
    }
}

pub(crate) fn create_new_file_at(dir: &DirHandle, name: &str) -> Result<VoiceFile, FsError> {
    super::validate_single_component(name)?;
    #[cfg(unix)]
    {
        super::unix::open_file_at(dir, name, true)
    }
    #[cfg(windows)]
    {
        super::windows::open_file_at(dir, name, true)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (dir, name);
        Err(FsError::Unsupported)
    }
}

pub(crate) fn open_or_create_file_at(dir: &DirHandle, name: &str) -> Result<VoiceFile, FsError> {
    super::validate_single_component(name)?;
    #[cfg(unix)]
    {
        super::unix::open_file_create_or_open(dir, name)
    }
    #[cfg(windows)]
    {
        super::windows::open_file_create_or_open(dir, name)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (dir, name);
        Err(FsError::Unsupported)
    }
}
