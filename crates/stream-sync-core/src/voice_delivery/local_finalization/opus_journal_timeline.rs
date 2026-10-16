//! Strict Opus packet validation for journal timeline placement (parity with ChatBot `opusToc.ts`).

const RTP_CLOCK_HZ: u64 = 48_000;
const OPUS_MAX_PACKET_SAMPLES: u64 = 120 * RTP_CLOCK_HZ / 1000;

/// Validates framing and returns total packet duration in 48 kHz samples.
pub fn validate_opus_packet_for_journal_timeline(payload: &[u8]) -> Result<u64, String> {
    if payload.is_empty() {
        return Err("opus payload empty".into());
    }
    let (frame_count, header_len) = opus_packet_frame_count_and_header_len(payload)?;
    let frame_samples = opus_packet_samples_per_frame_48k(payload[0])?;
    if frame_samples == 0 {
        return Err("opus frame duration is zero".into());
    }
    let mut offset = header_len;
    for frame_idx in 0..frame_count {
        let (frame_len, consumed) = opus_frame_length_at(payload, offset, frame_idx, frame_count)?;
        offset += consumed;
        if frame_len == 0 {
            return Err("opus frame length is zero".into());
        }
        offset += frame_len;
        if offset > payload.len() {
            return Err("opus frame extends past payload".into());
        }
    }
    if offset != payload.len() {
        return Err("opus payload has trailing bytes".into());
    }
    let total = frame_count * frame_samples;
    if total == 0 || total > OPUS_MAX_PACKET_SAMPLES {
        return Err("opus packet duration exceeds 120ms or is zero".into());
    }
    Ok(total)
}

fn opus_packet_frame_count_and_header_len(payload: &[u8]) -> Result<(u64, usize), String> {
    let code = payload[0] & 0x03;
    match code {
        0 => Ok((1, 1)),
        1 => Ok((2, 1)),
        2 => {
            if payload.len() < 2 {
                return Err("code 2 missing length byte".into());
            }
            Ok((2, 2))
        }
        3 => {
            if payload.len() < 2 {
                return Err("code 3 missing control byte".into());
            }
            let frame_count = payload[1] & 0x3f;
            if frame_count < 1 || frame_count > 48 {
                return Err("code 3 frame count M must be 1..48".into());
            }
            Ok((u64::from(frame_count), 2))
        }
        _ => Err("invalid opus TOC code".into()),
    }
}

fn opus_frame_length_at(
    payload: &[u8],
    offset: usize,
    frame_idx: u64,
    frame_count: u64,
) -> Result<(usize, usize), String> {
    let code = payload[0] & 0x03;
    if code == 0 || code == 1 {
        if frame_idx > 0 && code == 0 {
            return Err("code 0 cannot have multiple frames".into());
        }
        let per = payload.len() - 1;
        if code == 1 {
            let half = per / 2;
            let rem = per % 2;
            let len = if frame_idx == 0 { half + rem } else { half };
            return Ok((len, 0));
        }
        return Ok((per, 0));
    }
    if code == 2 {
        if offset >= payload.len() {
            return Err("code 2 missing VBR length".into());
        }
        let len = usize::from(payload[offset]);
        return Ok((len, 1));
    }
    // code 3: VBR multi-frame
    if offset >= payload.len() {
        return Err("code 3 missing VBR length".into());
    }
    if frame_idx < frame_count - 1 {
        let len = usize::from(payload[offset]);
        return Ok((len, 1));
    }
    let remaining = payload.len() - offset;
    Ok((remaining, 0))
}

fn opus_packet_samples_per_frame_48k(toc_byte: u8) -> Result<u64, String> {
    let fs = RTP_CLOCK_HZ;
    if toc_byte & 0x80 != 0 {
        let audiosize = (toc_byte >> 3) & 0x3;
        return Ok((fs << audiosize) / 400);
    }
    if (toc_byte & 0x60) == 0x60 {
        return Ok(if toc_byte & 0x08 != 0 {
            fs / 50
        } else {
            fs / 100
        });
    }
    let audiosize = (toc_byte >> 3) & 0x3;
    if audiosize == 3 {
        return Ok((fs * 60) / 1000);
    }
    Ok((fs << audiosize) / 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toc_payload(toc: u8, rest: &[u8]) -> Vec<u8> {
        let mut p = vec![toc];
        p.extend_from_slice(rest);
        p
    }

    #[test]
    fn validates_20ms_packet() {
        assert_eq!(
            validate_opus_packet_for_journal_timeline(&toc_payload(0x08, &[0x01])).unwrap(),
            960
        );
    }

    #[test]
    fn validates_2_5ms_packet() {
        let toc = 0x80u8;
        assert_eq!(
            validate_opus_packet_for_journal_timeline(&toc_payload(toc, &[0x01])).unwrap(),
            120
        );
    }

    #[test]
    fn validates_60ms_packet() {
        let toc = 0x58u8;
        assert_eq!(
            validate_opus_packet_for_journal_timeline(&toc_payload(toc, &[0x01])).unwrap(),
            2880
        );
    }

    #[test]
    fn rejects_malformed_framing() {
        assert!(validate_opus_packet_for_journal_timeline(&[0x0b]).is_err());
    }
}
