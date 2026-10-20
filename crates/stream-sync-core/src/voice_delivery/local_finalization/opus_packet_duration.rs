//! Opus packet duration from TOC (RFC 6716), aligned with ChatBot `opusToc.ts`.

const RTP_CLOCK_HZ: u64 = 48_000;
const OPUS_MAX_PACKET_SAMPLES: u64 = 120 * RTP_CLOCK_HZ / 1000;

pub fn opus_packet_duration_samples(payload: &[u8]) -> Result<u64, String> {
    if payload.is_empty() {
        return Err("opus payload empty".into());
    }
    let frame_count = opus_packet_nb_frames(payload)?;
    let frame_samples = opus_packet_samples_per_frame_48k(payload[0]);
    let total = frame_count * frame_samples;
    if total == 0 || total > OPUS_MAX_PACKET_SAMPLES {
        return Err("opus packet duration exceeds 120ms or is zero".into());
    }
    Ok(total)
}

fn opus_packet_nb_frames(payload: &[u8]) -> Result<u64, String> {
    if payload.is_empty() {
        return Err("opus payload empty".into());
    }
    let code = payload[0] & 0x03;
    if code == 0 {
        return Ok(1);
    }
    if code != 3 {
        return Ok(2);
    }
    if payload.len() < 2 {
        return Err("code 3 missing control byte".into());
    }
    let frame_count = payload[1] & 0x3f;
    if !(1..=48).contains(&frame_count) {
        return Err("code 3 frame count M must be 1..48".into());
    }
    Ok(u64::from(frame_count))
}

fn opus_packet_samples_per_frame_48k(toc_byte: u8) -> u64 {
    let fs = RTP_CLOCK_HZ;
    if toc_byte & 0x80 != 0 {
        let audiosize = (toc_byte >> 3) & 0x3;
        return (fs << audiosize) / 400;
    }
    if (toc_byte & 0x60) == 0x60 {
        return if toc_byte & 0x08 != 0 {
            fs / 50
        } else {
            fs / 100
        };
    }
    let audiosize = (toc_byte >> 3) & 0x3;
    if audiosize == 3 {
        return (fs * 60) / 1000;
    }
    (fs << audiosize) / 100
}
