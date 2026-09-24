//! Stereo s16 @ 48 kHz session bounds (u64 end-to-end).

use thiserror::Error;

pub const SAMPLE_RATE_HZ: u64 = 48_000;
pub const CHANNELS_STEREO: u16 = 2;
pub const BYTES_PER_SAMPLE: u16 = 2;

pub const SESSION_4_5H_SAMPLE_FRAMES: u64 = 777_600_000;
pub const SESSION_4_5H_DATA_BYTES: u64 = 3_110_400_000;

pub const SESSION_6H_SAMPLE_FRAMES: u64 = 1_036_800_000;
pub const SESSION_6H_DATA_BYTES: u64 = 4_147_200_000;

pub const STEREO_PCM_FRAME_BYTES: u64 = (CHANNELS_STEREO as u64) * (BYTES_PER_SAMPLE as u64);

pub const RIFF_PCM_OVERHEAD_BYTES: u64 = 36;
pub const RIFF_MAX_CHUNK_BYTES: u64 =
    ((u32::MAX as u64) - RIFF_PCM_OVERHEAD_BYTES) / STEREO_PCM_FRAME_BYTES * STEREO_PCM_FRAME_BYTES;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BoundsError {
    #[error("sample frame count overflow")]
    SampleFramesOverflow,
    #[error("data byte count overflow")]
    DataBytesOverflow,
    #[error("session exceeds RIFF/WAV size limit ({RIFF_MAX_CHUNK_BYTES} data bytes)")]
    ExceedsRiffLimit,
}

pub fn wav_data_bytes_for_frames(
    sample_frames: u64,
    channels: u16,
    bytes_per_sample: u16,
) -> Result<u64, BoundsError> {
    let channels_u64 = channels as u64;
    let bps = bytes_per_sample as u64;
    let product = sample_frames
        .checked_mul(channels_u64)
        .and_then(|v| v.checked_mul(bps))
        .ok_or(BoundsError::DataBytesOverflow)?;
    if product > RIFF_MAX_CHUNK_BYTES {
        return Err(BoundsError::ExceedsRiffLimit);
    }
    Ok(product)
}

pub fn validate_plan_session_bounds(sample_frames: u64) -> Result<u64, BoundsError> {
    wav_data_bytes_for_frames(sample_frames, CHANNELS_STEREO, BYTES_PER_SAMPLE)
}

#[cfg(test)]
mod plan_duration_bounds_match_unsigned_expectations {
    use super::*;

    #[test]
    fn plan_duration_bounds_match_unsigned_expectations() {
        assert_eq!(
            validate_plan_session_bounds(SESSION_4_5H_SAMPLE_FRAMES).unwrap(),
            SESSION_4_5H_DATA_BYTES
        );
        assert_eq!(
            validate_plan_session_bounds(SESSION_6H_SAMPLE_FRAMES).unwrap(),
            SESSION_6H_DATA_BYTES
        );
        const _: () = assert!(SESSION_6H_DATA_BYTES < RIFF_MAX_CHUNK_BYTES);
        let over_frames = (RIFF_MAX_CHUNK_BYTES / STEREO_PCM_FRAME_BYTES) + 1;
        assert_eq!(
            validate_plan_session_bounds(over_frames),
            Err(BoundsError::ExceedsRiffLimit)
        );
    }
}

#[cfg(test)]
mod riff_max_data_bytes_exact_limit_and_reject_one_over {
    use super::*;
    use crate::voice_delivery::wav::minimal_wav_header;

    #[test]
    fn riff_max_data_bytes_exact_limit_and_reject_one_over() {
        assert_eq!(RIFF_MAX_CHUNK_BYTES, 4_294_967_256);
        assert_eq!(
            RIFF_MAX_CHUNK_BYTES,
            ((u32::MAX as u64) - RIFF_PCM_OVERHEAD_BYTES) / STEREO_PCM_FRAME_BYTES
                * STEREO_PCM_FRAME_BYTES
        );
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES).is_ok());
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES + 4).is_err());
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES + 1).is_err());
        let max_aligned_frames = RIFF_MAX_CHUNK_BYTES / STEREO_PCM_FRAME_BYTES;
        assert_eq!(
            wav_data_bytes_for_frames(max_aligned_frames, CHANNELS_STEREO, BYTES_PER_SAMPLE)
                .unwrap(),
            max_aligned_frames * STEREO_PCM_FRAME_BYTES
        );
        assert_eq!(
            wav_data_bytes_for_frames(max_aligned_frames + 1, CHANNELS_STEREO, BYTES_PER_SAMPLE),
            Err(BoundsError::ExceedsRiffLimit)
        );
    }
}
