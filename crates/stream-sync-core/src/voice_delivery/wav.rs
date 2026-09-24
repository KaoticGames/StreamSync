//! Canonical 44-byte stereo PCM WAV header validation and construction.

use crate::voice_delivery::bounds::{
    BYTES_PER_SAMPLE, CHANNELS_STEREO, RIFF_MAX_CHUNK_BYTES, SAMPLE_RATE_HZ, STEREO_PCM_FRAME_BYTES,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WavError {
    #[error("WAV header too short")]
    HeaderTooShort,
    #[error("not a RIFF WAVE file")]
    NotRiffWave,
    #[error("missing fmt chunk")]
    MissingFmt,
    #[error("unexpected fmt chunk length")]
    UnexpectedFmtLen,
    #[error("expected PCM format")]
    NotPcm,
    #[error("expected stereo channels")]
    NotStereo,
    #[error("expected 48 kHz sample rate")]
    WrongSampleRate,
    #[error("unexpected byte rate")]
    WrongByteRate,
    #[error("unexpected block alignment")]
    WrongBlockAlign,
    #[error("expected 16-bit samples")]
    WrongBitsPerSample,
    #[error("missing data chunk")]
    MissingData,
    #[error("data chunk length not frame aligned")]
    DataNotFrameAligned,
    #[error("RIFF size inconsistent with data chunk")]
    RiffSizeInconsistent,
    #[error("file too short for RIFF")]
    FileTooShort,
    #[error("RIFF size inconsistent with file length")]
    RiffFileLenInconsistent,
    #[error("file length inconsistent with data chunk and RIFF padding")]
    FileLenInconsistent,
    #[error("WAV data size must be frame aligned")]
    DataSizeUnaligned,
    #[error("WAV data size exceeds classic RIFF limit")]
    DataTooLarge,
    #[error("WAV data size exceeds u32")]
    DataExceedsU32,
    #[error("RIFF size field overflow")]
    RiffOverflow,
}

/// Minimal WAV header for a fixed data chunk size (classic RIFF, not RF64).
pub fn minimal_wav_header(data_bytes: u64) -> Result<Vec<u8>, WavError> {
    if !data_bytes.is_multiple_of(STEREO_PCM_FRAME_BYTES) {
        return Err(WavError::DataSizeUnaligned);
    }
    if data_bytes > RIFF_MAX_CHUNK_BYTES {
        return Err(WavError::DataTooLarge);
    }
    let data_u32 = u32::try_from(data_bytes).map_err(|_| WavError::DataExceedsU32)?;
    let riff_size = 36u32.checked_add(data_u32).ok_or(WavError::RiffOverflow)?;
    let mut h = Vec::with_capacity(44);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&riff_size.to_le_bytes());
    h.extend_from_slice(b"WAVEfmt ");
    h.extend_from_slice(&16u32.to_le_bytes());
    h.extend_from_slice(&1u16.to_le_bytes());
    h.extend_from_slice(&CHANNELS_STEREO.to_le_bytes());
    h.extend_from_slice(&(SAMPLE_RATE_HZ as u32).to_le_bytes());
    let byte_rate = (SAMPLE_RATE_HZ * CHANNELS_STEREO as u64 * BYTES_PER_SAMPLE as u64) as u32;
    h.extend_from_slice(&byte_rate.to_le_bytes());
    let block_align = CHANNELS_STEREO * BYTES_PER_SAMPLE;
    h.extend_from_slice(&block_align.to_le_bytes());
    h.extend_from_slice(&16u16.to_le_bytes());
    h.extend_from_slice(b"data");
    h.extend_from_slice(&data_u32.to_le_bytes());
    Ok(h)
}

pub fn parse_wav_data_chunk_len(header: &[u8]) -> Result<u64, WavError> {
    validate_canonical_pcm_wav_header(header, None)
}

/// Validate the canonical 44-byte stereo PCM WAV header from `minimal_wav_header`.
/// When `file_len` is provided, RIFF size must equal `file_len - 8` and the file must cover header + data (+ optional pad).
pub fn validate_canonical_pcm_wav_header(
    header: &[u8],
    file_len: Option<u64>,
) -> Result<u64, WavError> {
    if header.len() < 44 {
        return Err(WavError::HeaderTooShort);
    }
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        return Err(WavError::NotRiffWave);
    }
    let riff_size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
    if &header[12..16] != b"fmt " {
        return Err(WavError::MissingFmt);
    }
    let fmt_len = u32::from_le_bytes(header[16..20].try_into().unwrap());
    if fmt_len != 16 {
        return Err(WavError::UnexpectedFmtLen);
    }
    let audio_format = u16::from_le_bytes(header[20..22].try_into().unwrap());
    if audio_format != 1 {
        return Err(WavError::NotPcm);
    }
    let channels = u16::from_le_bytes(header[22..24].try_into().unwrap());
    if channels != CHANNELS_STEREO {
        return Err(WavError::NotStereo);
    }
    let sample_rate = u32::from_le_bytes(header[24..28].try_into().unwrap()) as u64;
    if sample_rate != SAMPLE_RATE_HZ {
        return Err(WavError::WrongSampleRate);
    }
    let byte_rate = u32::from_le_bytes(header[28..32].try_into().unwrap()) as u64;
    let expected_byte_rate = SAMPLE_RATE_HZ * CHANNELS_STEREO as u64 * BYTES_PER_SAMPLE as u64;
    if byte_rate != expected_byte_rate {
        return Err(WavError::WrongByteRate);
    }
    let block_align = u16::from_le_bytes(header[32..34].try_into().unwrap());
    if block_align as u64 != STEREO_PCM_FRAME_BYTES {
        return Err(WavError::WrongBlockAlign);
    }
    let bits_per_sample = u16::from_le_bytes(header[34..36].try_into().unwrap());
    if bits_per_sample != 16 {
        return Err(WavError::WrongBitsPerSample);
    }
    if &header[36..40] != b"data" {
        return Err(WavError::MissingData);
    }
    let data_len = u32::from_le_bytes(header[40..44].try_into().unwrap()) as u64;
    if !data_len.is_multiple_of(STEREO_PCM_FRAME_BYTES) {
        return Err(WavError::DataNotFrameAligned);
    }
    if riff_size != 36 + data_len {
        return Err(WavError::RiffSizeInconsistent);
    }
    if let Some(len) = file_len {
        if len < 8 {
            return Err(WavError::FileTooShort);
        }
        if riff_size != len - 8 {
            return Err(WavError::RiffFileLenInconsistent);
        }
        let padded_data = data_len + (data_len % 2);
        if len != 44 + padded_data {
            return Err(WavError::FileLenInconsistent);
        }
    }
    Ok(data_len)
}

#[cfg(test)]
mod wav_header_read_uses_fixed_44_bytes_without_usize_file_length {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};

    fn sparse_extend_file(path: &std::path::Path, new_len: u64) -> std::io::Result<()> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        f.seek(SeekFrom::Start(new_len.saturating_sub(1)))?;
        f.write_all(&[0])?;
        f.sync_all()?;
        Ok(())
    }

    #[test]
    fn wav_header_read_uses_fixed_44_bytes_without_usize_file_length() {
        let dir = tempfile::tempdir().unwrap();
        let data_bytes = 1_024u64;
        let header = minimal_wav_header(data_bytes).unwrap();
        let path = dir.path().join("sparse.wav");
        let total_len = u64::from(u32::MAX) + 4096;
        sparse_extend_file(&path, total_len).unwrap();
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), total_len);
        let mut header_buf = [0u8; 44];
        let mut rf = File::open(&path).unwrap();
        rf.read_exact(&mut header_buf).unwrap();
        assert_eq!(parse_wav_data_chunk_len(&header_buf).unwrap(), data_bytes);
        assert!(validate_canonical_pcm_wav_header(&header_buf, Some(total_len)).is_err());
    }
}
