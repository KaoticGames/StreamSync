//! Opus decode — native `audiopus` in production; explicit test-only stub without the feature.

use super::synj::JournalRecord;

#[derive(Debug)]
pub enum OpusDecodeError {
    Failed,
    Unavailable,
}

#[cfg(all(test, not(feature = "local-opus-native")))]
struct OpusStubDecoder;

pub struct StereoOpusDecoder {
    #[cfg(feature = "local-opus-native")]
    inner: audiopus::coder::Decoder,
    #[cfg(all(test, not(feature = "local-opus-native")))]
    _stub: OpusStubDecoder,
}

impl StereoOpusDecoder {
    pub fn new() -> Result<Self, OpusDecodeError> {
        #[cfg(feature = "local-opus-native")]
        {
            use audiopus::{Channels, SampleRate};
            let inner = audiopus::coder::Decoder::new(SampleRate::Hz48000, Channels::Stereo)
                .map_err(|_| OpusDecodeError::Failed)?;
            Ok(Self { inner })
        }
        #[cfg(all(test, not(feature = "local-opus-native")))]
        {
            Ok(Self {
                _stub: OpusStubDecoder,
            })
        }
        #[cfg(not(any(feature = "local-opus-native", test)))]
        {
            Err(OpusDecodeError::Unavailable)
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
            let out = MutSignals::try_from(pcm_out).map_err(|_| OpusDecodeError::Failed)?;
            self.inner
                .decode(Some(packet), out, false)
                .map_err(|_| OpusDecodeError::Failed)
        }
        #[cfg(all(test, not(feature = "local-opus-native")))]
        {
            let _ = pcm_out;
            if record.opus_payload.is_empty() || record.opus_payload == [0xff, 0xff] {
                return Err(OpusDecodeError::Failed);
            }
            Ok(960)
        }
        #[cfg(not(any(feature = "local-opus-native", test)))]
        {
            let _ = (record, pcm_out);
            Err(OpusDecodeError::Unavailable)
        }
    }
}

/// Production / offline recovery entrypoints require native Opus (`local-opus-native`).
pub fn assert_local_opus_available() -> Result<(), OpusDecodeError> {
    #[cfg(feature = "local-opus-native")]
    {
        StereoOpusDecoder::new().map(|_| ())
    }
    #[cfg(not(feature = "local-opus-native"))]
    {
        Err(OpusDecodeError::Unavailable)
    }
}
