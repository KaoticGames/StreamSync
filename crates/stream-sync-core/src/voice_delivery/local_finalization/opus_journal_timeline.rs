//! Strict Opus packet validation for journal timeline placement (parity with ChatBot `opusToc.ts`).

use super::opus_packet_duration::opus_packet_duration_samples;

/// TOC duration plus RFC 6716 strict layout — required before timeline audio placement or native decode.
pub fn validate_opus_packet_for_journal_timeline(payload: &[u8]) -> Result<u64, String> {
    let total = opus_packet_duration_samples(payload)?;
    validate_opus_packet_framing_strict(payload)?;
    Ok(total)
}

/// RFC 6716 frame layout checks (code 1 parity, code 2 VBR lengths, code 3 padding/VBR).
pub fn validate_opus_packet_framing_strict(payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() {
        return Err("opus payload empty".into());
    }
    validate_strict_for_toc_byte(payload[0], payload, 0)?;
    Ok(())
}

fn read_opus_length_prefix(payload: &[u8], offset: usize) -> Result<(usize, usize), String> {
    if offset >= payload.len() {
        return Err("truncated opus length prefix".into());
    }
    let first = payload[offset];
    if first <= 251 {
        return Ok((usize::from(first), offset + 1));
    }
    let second_offset = offset + 1;
    if second_offset >= payload.len() {
        return Err("truncated opus length prefix continuation".into());
    }
    let second = payload[second_offset];
    let length = usize::from(first) + 4 * usize::from(second);
    Ok((length, second_offset + 1))
}

fn parse_padding_length(payload: &[u8], offset: usize) -> Result<(usize, usize), String> {
    let mut padding = 0usize;
    let mut idx = offset;
    loop {
        if idx >= payload.len() {
            return Err("truncated code 3 padding length".into());
        }
        let b = payload[idx];
        idx += 1;
        if b == 255 {
            padding = padding
                .checked_add(254)
                .ok_or_else(|| "code 3 padding length overflow".to_string())?;
            continue;
        }
        padding = padding
            .checked_add(usize::from(b))
            .ok_or_else(|| "code 3 padding length overflow".to_string())?;
        break;
    }
    Ok((padding, idx))
}

struct Code3Layout;

fn parse_code3_layout(payload: &[u8], toc_index: usize) -> Result<Code3Layout, String> {
    if toc_index >= payload.len() {
        return Err("code 3 missing control byte".into());
    }
    let ctrl = payload[toc_index];
    let vbr = ctrl & 0x80 != 0;
    let padding_flag = ctrl & 0x40 != 0;
    let frame_count = ctrl & 0x3f;
    if frame_count < 1 || frame_count > 48 {
        return Err("code 3 frame count M must be 1..48".into());
    }
    let mut idx = toc_index + 1;
    let mut trailing_padding_bytes = 0usize;
    if padding_flag {
        let (padding, next) = parse_padding_length(payload, idx)?;
        trailing_padding_bytes = padding;
        idx = next;
        if trailing_padding_bytes > payload.len() {
            return Err("code 3 padding length overflow".into());
        }
    }
    let frame_data_end = payload
        .len()
        .checked_sub(trailing_padding_bytes)
        .ok_or_else(|| "code 3 frame data beyond packet".to_string())?;
    if frame_data_end < idx {
        return Err("code 3 frame data beyond packet".into());
    }
    let available = frame_data_end - idx;
    if available == 0 {
        return Err("code 3 missing frame payload".into());
    }

    if !vbr {
        if available % usize::from(frame_count) != 0 {
            return Err("code 3 CBR frame body not divisible by M".into());
        }
        let per_frame = available / usize::from(frame_count);
        if per_frame == 0 {
            return Err("code 3 CBR zero-length frames".into());
        }
    } else if frame_count == 1 {
        if available < 1 {
            return Err("code 3 VBR missing sole frame".into());
        }
    } else {
        let mut prefix_end = idx;
        let mut frame_lengths: Vec<usize> = Vec::new();
        for _ in 0..frame_count - 1 {
            let (length, next) = read_opus_length_prefix(payload, prefix_end)?;
            if length == 0 {
                return Err("code 3 VBR frame length must be positive".into());
            }
            frame_lengths.push(length);
            prefix_end = next;
            if prefix_end > frame_data_end {
                return Err("code 3 VBR frame length prefixes exceed packet".into());
            }
        }
        let data_start = prefix_end;
        let data_bytes = frame_data_end - data_start;
        let sum: usize = frame_lengths.iter().sum();
        match data_bytes.checked_sub(sum) {
            None | Some(0) => {
                return Err(if sum > data_bytes {
                    "code 3 VBR framing inconsistent".into()
                } else {
                    "code 3 VBR final frame empty".into()
                });
            }
            Some(_) => {}
        }
    }

    Ok(Code3Layout)
}

fn validate_strict_for_toc_byte(toc: u8, payload: &[u8], offset: usize) -> Result<(), String> {
    let code = toc & 0x03;
    match code {
        0 => {
            if offset + 1 >= payload.len() {
                return Err("code 0 missing frame payload".into());
            }
        }
        1 => {
            let body_bytes = payload.len().checked_sub(offset + 1).unwrap_or(0);
            if body_bytes < 2 || body_bytes % 2 != 0 {
                return Err("code 1 requires even payload >= 2 bytes after TOC".into());
            }
        }
        2 => {
            let (length, next) = read_opus_length_prefix(payload, offset + 1)?;
            if length == 0 {
                return Err("code 2 first frame length must be positive".into());
            }
            let frame1_end = match next.checked_add(length) {
                None => return Err("code 2 first frame truncated".into()),
                Some(end) if end > payload.len() => {
                    return Err("code 2 first frame truncated".into());
                }
                Some(end) => end,
            };
            let frame2_len = payload
                .len()
                .checked_sub(frame1_end)
                .ok_or_else(|| "code 2 first frame truncated".to_string())?;
            if frame2_len == 0 {
                return Err("code 2 second frame missing".into());
            }
        }
        3 => {
            parse_code3_layout(payload, offset + 1)?;
        }
        _ => return Err("invalid opus TOC code".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toc_payload(toc: u8, rest: &[u8]) -> Vec<u8> {
        let mut p = vec![toc];
        p.extend_from_slice(rest);
        p
    }

    fn assert_err_contains(payload: &[u8], substr: &str) {
        let err = validate_opus_packet_for_journal_timeline(payload)
            .err()
            .unwrap_or_else(|| panic!("expected error containing {substr:?}"));
        assert!(
            err.contains(substr),
            "error {:?} should contain {:?}",
            err,
            substr
        );
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
    fn code0_missing_payload_rejected() {
        assert_err_contains(&[0x08], "code 0 missing frame payload");
    }

    #[test]
    fn code0_minimal_valid() {
        assert!(validate_opus_packet_for_journal_timeline(&[0x08, 0x01]).is_ok());
    }

    #[test]
    fn code1_even_equal_frames_accepted() {
        let p = [0x09, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 1920);
    }

    #[test]
    fn code1_odd_body_rejected() {
        assert_err_contains(&[0x09, 0x01, 0x02, 0x03], "code 1 requires even payload");
    }

    #[test]
    fn code2_valid_two_frames() {
        let p = [0x0a, 0x02, 0xaa, 0xbb, 0xcc, 0xdd];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 1920);
    }

    #[test]
    fn code2_truncated_prefix_rejected() {
        assert_err_contains(&[0x0a, 0xfc], "truncated");
    }

    #[test]
    fn code2_prefix_beyond_body_rejected() {
        assert_err_contains(&[0x0a, 0xfc, 0x01], "truncated");
    }

    #[test]
    fn code2_zero_first_length_rejected() {
        assert_err_contains(
            &[0x0a, 0x00, 0xaa],
            "code 2 first frame length must be positive",
        );
    }

    #[test]
    fn code2_missing_second_frame_rejected() {
        assert_err_contains(&[0x0a, 0x02, 0xaa, 0xbb], "code 2 second frame missing");
    }

    #[test]
    fn code2_extended_length_prefix_accepted() {
        let first_len = 252usize;
        let mut p = vec![0x0a, 252, 0];
        p.extend(std::iter::repeat(0xaa).take(first_len));
        p.push(0xbb);
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 1920);
    }

    #[test]
    fn code3_cbr_m1_accepted() {
        let p = [0x0b, 0x01, 0xab, 0xcd];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 960);
    }

    #[test]
    fn code3_cbr_multi_frame_accepted() {
        let p = [0x0b, 0x02, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 1920);
    }

    #[test]
    fn code3_cbr_indivisible_rejected() {
        assert_err_contains(
            &[0x0b, 0x02, 0x01, 0x02, 0x03],
            "code 3 CBR frame body not divisible by M",
        );
    }

    #[test]
    fn code3_vbr_accepted() {
        let p = [0x0b, 0x83, 0x01, 0x01, 0xaa, 0xbb, 0xcc];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 2880);
    }

    #[test]
    fn code3_vbr_zero_prefix_rejected() {
        assert_err_contains(
            &[0x0b, 0x82, 0x00, 0x01, 0xaa],
            "code 3 VBR frame length must be positive",
        );
    }

    #[test]
    fn code3_vbr_truncated_prefix_rejected() {
        assert_err_contains(&[0x0b, 0x82, 0xfc], "truncated opus length prefix");
    }

    #[test]
    fn code3_vbr_prefix_sum_exceeds_remaining_data_rejected() {
        // VBR with M=3: two length prefixes (5 + 5) exceed the single payload byte after headers.
        let payload = [0x0b, 0x83, 0x05, 0x05, 0xaa];
        let result = validate_opus_packet_for_journal_timeline(&payload);
        assert!(
            result.is_err(),
            "prefix sum over remaining data must return Err, got {:?}",
            result
        );
        assert_err_contains(&payload, "code 3 VBR framing inconsistent");
    }

    #[test]
    fn code3_padding_accepted() {
        let p = [0x0b, 0x41, 0x02, 0xab, 0x00, 0x00];
        assert!(validate_opus_packet_for_journal_timeline(&p).is_ok());
    }

    #[test]
    fn code3_padding_255_continuation_accepted() {
        let mut p = vec![0x0b, 0x41, 255, 255, 1];
        p.push(0xab);
        p.extend([0x00u8; 509]);
        assert!(validate_opus_packet_for_journal_timeline(&p).is_ok());
    }

    #[test]
    fn code3_padding_overflow_rejected() {
        assert_err_contains(&[0x0b, 0x41, 0x0a, 0xab], "code 3 padding length overflow");
    }

    #[test]
    fn code3_padding_truncation_rejected() {
        assert_err_contains(&[0x0b, 0x41, 0x02, 0xab], "code 3 frame data beyond packet");
    }

    #[test]
    fn code3_m0_rejected() {
        assert_err_contains(&[0x0b, 0x00], "code 3 frame count M must be 1..48");
    }

    #[test]
    fn code3_m_over_48_rejected() {
        assert_err_contains(&[0x0b, 0x31, 0xab], "code 3 frame count M must be 1..48");
    }

    #[test]
    fn duration_120ms_max_accepted() {
        let p = [0x1b, 0x02, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(validate_opus_packet_for_journal_timeline(&p).unwrap(), 5760);
    }

    #[test]
    fn duration_over_120ms_rejected() {
        assert_err_contains(
            &[0x1b, 0x03, 0x01, 0x02, 0x03],
            "opus packet duration exceeds 120ms or is zero",
        );
    }

    #[test]
    fn production_opus_20ms_fixture() {
        assert_eq!(
            validate_opus_packet_for_journal_timeline(&[0xfc, 0x01]).unwrap(),
            960
        );
    }

    #[test]
    fn production_malformed_code2_rejected() {
        assert_err_contains(
            &[0x9a, 0xfc, 0x00, 0x01, 0x02],
            "code 2 first frame truncated",
        );
    }

    #[test]
    fn read_opus_length_prefix_extended() {
        let (len, next) = read_opus_length_prefix(&[252, 0], 0).unwrap();
        assert_eq!(len, 252);
        assert_eq!(next, 2);
        let (len2, _) = read_opus_length_prefix(&[255, 10], 0).unwrap();
        assert_eq!(len2, 255 + 40);
    }

    #[test]
    fn rejects_malformed_framing() {
        assert!(validate_opus_packet_for_journal_timeline(&[0x0b]).is_err());
    }
}
