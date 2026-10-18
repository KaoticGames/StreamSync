//! Opus decode — native `audiopus` in production; explicit test-only stub without the feature.

use super::synj::JournalRecord;

#[derive(Debug)]
pub enum OpusDecodeError {
    Failed,
    Unavailable,
}

#[cfg(not(feature = "local-opus-native"))]
struct OpusStubDecoder;

pub struct StereoOpusDecoder {
    #[cfg(feature = "local-opus-native")]
    inner: audiopus::coder::Decoder,
    #[cfg(not(feature = "local-opus-native"))]
    _stub: OpusStubDecoder,
}

impl StereoOpusDecoder {
    pub fn new() -> Result<Self, OpusDecodeError> {
        #[cfg(feature = "local-opus-native")]
        {
            use audiopus::{Channels, SampleRate};
            let inner = audiopus::coder::Decoder::new(SampleRate::Hz48000, Channels::Stereo)
                .map_err(|_| OpusDecodeError::Failed)?;
            return Ok(Self { inner });
        }
        #[cfg(not(feature = "local-opus-native"))]
        {
            return Ok(Self {
                _stub: OpusStubDecoder,
            });
        }
    }

    pub fn decode_packet(
        &mut self,
        record: &JournalRecord,
        pcm_out: &mut [i16],
    ) -> Result<usize, OpusDecodeError> {
        #[cfg(feature = "local-opus-native")]
        {
            use audiopus::packet::Packet;
            use audiopus::MutSignals;
            let packet =
                Packet::try_from(&record.opus_payload).map_err(|_| OpusDecodeError::Failed)?;
            let mut out = MutSignals::try_from(pcm_out).map_err(|_| OpusDecodeError::Failed)?;
            return self
                .inner
                .decode(Some(packet), out, false)
                .map_err(|_| OpusDecodeError::Failed);
        }
        #[cfg(not(feature = "local-opus-native"))]
        {
            let _ = pcm_out;
            if record.opus_payload.is_empty() || record.opus_payload == [0xff, 0xff] {
                return Err(OpusDecodeError::Failed);
            }
            Ok(960)
        }
    }
}

/// Production / offline recovery entrypoints require native Opus (`local-opus-native`).
pub fn assert_local_opus_available() -> Result<(), OpusDecodeError> {
    #[cfg(feature = "local-opus-native")]
    {
        return StereoOpusDecoder::new().map(|_| ());
    }
    #[cfg(not(feature = "local-opus-native"))]
    {
        Err(OpusDecodeError::Unavailable)
    }
}
