//! Opus decode — native `audiopus` in production; explicit test-only stub without the feature.

use super::synj::JournalRecord;

#[derive(Debug)]
pub enum OpusDecodeError {
    Failed,
    Unavailable,
}

#[cfg(all(not(feature = "local-opus-native"), test))]
struct TestOpusStub;

pub struct StereoOpusDecoder {
    #[cfg(feature = "local-opus-native")]
    inner: audiopus::coder::Decoder,
    #[cfg(all(not(feature = "local-opus-native"), test))]
    _test_stub: TestOpusStub,
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
        #[cfg(all(not(feature = "local-opus-native"), test))]
        {
            return Ok(Self {
                _test_stub: TestOpusStub,
            });
        }
        #[cfg(all(not(feature = "local-opus-native"), not(test)))]
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
            let mut out = MutSignals::try_from(pcm_out).map_err(|_| OpusDecodeError::Failed)?;
            return self
                .inner
                .decode(Some(packet), out, false)
                .map_err(|_| OpusDecodeError::Failed);
        }
        #[cfg(not(feature = "local-opus-native"))]
        {
            let _ = (record, pcm_out);
            Err(OpusDecodeError::Unavailable)
        }
    }
}

/// Production builds with In-Sync processing must link native Opus (see `local-opus-native`).
pub fn assert_local_opus_available() -> Result<(), OpusDecodeError> {
    StereoOpusDecoder::new().map(|_| ())
}
