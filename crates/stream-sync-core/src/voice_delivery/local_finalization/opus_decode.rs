//! Opus decode seam — `audiopus` when `local-opus-native` is enabled, stub otherwise.

use super::synj::JournalRecord;

#[derive(Debug)]
pub enum OpusDecodeError {
    Failed,
}

pub struct StereoOpusDecoder;

impl StereoOpusDecoder {
    pub fn new() -> Result<Self, OpusDecodeError> {
        Ok(Self)
    }

    /// Decode one packet into stereo PCM at 48 kHz. Returns sample frames written.
    pub fn decode_packet(
        &mut self,
        record: &JournalRecord,
        pcm_out: &mut [i16],
    ) -> Result<usize, OpusDecodeError> {
        #[cfg(feature = "local-opus-native")]
        {
            use audiopus::coder::Decoder;
            use audiopus::{Channels, SampleRate};
            // Decoder state is recreated per call in raw stub path; production should pool per user/epoch.
            let mut decoder = Decoder::new(SampleRate::Hz48000, Channels::Stereo)
                .map_err(|_| OpusDecodeError::Failed)?;
            return decoder
                .decode(Some(&record.opus_payload), pcm_out, false)
                .map_err(|_| OpusDecodeError::Failed);
        }
        #[cfg(not(feature = "local-opus-native"))]
        {
            let _ = (record, pcm_out);
            Err(OpusDecodeError::Failed)
        }
    }
}
