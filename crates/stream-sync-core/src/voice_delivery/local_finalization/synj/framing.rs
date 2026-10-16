//! Opus journal `.synj` framing (mirrors ChatBot `opusJournal/framing.ts`).

use super::crc32::crc32_buffer;
use thiserror::Error;

pub const JOURNAL_MAGIC: [u8; 4] = *b"SYNJ";
pub const JOURNAL_FORMAT_VERSION_V1: u32 = 1;
pub const JOURNAL_FORMAT_VERSION_V2: u32 = 2;
pub const FILE_HEADER_BYTES: usize = 8;
pub const MAX_USER_ID_BYTES: usize = 32;
pub const MAX_OPUS_PAYLOAD_BYTES: usize = 4000;
pub const MIN_RECORD_BYTES_V1: usize = 4 + 2 + 4 + 2 + 4 + 8 + 2 + 4;
pub const MIN_RECORD_BYTES_V2: usize = MIN_RECORD_BYTES_V1 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRecord {
    pub discord_user_id: String,
    pub ssrc_epoch: u32,
    pub sequence: u16,
    pub rtp_timestamp: u32,
    pub arrived_at_ns: u64,
    pub session_capture_offset_samples: Option<u64>,
    pub opus_payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum JournalFramingError {
    #[error("journal framing fatal: {0}")]
    Fatal(String),
    #[error("journal packet recoverable: {0}")]
    Recoverable(String),
}

pub fn is_supported_journal_format_version(version: u32) -> bool {
    version == JOURNAL_FORMAT_VERSION_V1 || version == JOURNAL_FORMAT_VERSION_V2
}

pub fn read_file_header(buf: &[u8]) -> Result<u32, JournalFramingError> {
    if buf.len() < FILE_HEADER_BYTES {
        return Err(JournalFramingError::Fatal(
            "journal header truncated".into(),
        ));
    }
    if buf[0..4] != JOURNAL_MAGIC {
        return Err(JournalFramingError::Fatal("journal magic mismatch".into()));
    }
    let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if !is_supported_journal_format_version(version) {
        return Err(JournalFramingError::Fatal(format!(
            "unsupported journal version {}",
            version
        )));
    }
    Ok(version)
}

pub fn decode_journal_record(
    buf: &[u8],
    offset: usize,
    format_version: u32,
) -> Result<(JournalRecord, usize), JournalFramingError> {
    let min_record_bytes = if format_version == JOURNAL_FORMAT_VERSION_V2 {
        MIN_RECORD_BYTES_V2
    } else {
        MIN_RECORD_BYTES_V1
    };
    if offset + 4 > buf.len() {
        return Err(JournalFramingError::Fatal(
            "journal record truncated at size".into(),
        ));
    }
    let record_size = u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]) as usize;
    if record_size < min_record_bytes {
        return Err(JournalFramingError::Recoverable(
            "journal record size invalid".into(),
        ));
    }
    if offset + record_size > buf.len() {
        return Err(JournalFramingError::Fatal(
            "journal record truncated".into(),
        ));
    }
    let slice = &buf[offset..offset + record_size];
    let crc_offset = record_size - 4;
    let expected_crc = u32::from_be_bytes([
        slice[crc_offset],
        slice[crc_offset + 1],
        slice[crc_offset + 2],
        slice[crc_offset + 3],
    ]);
    let actual_crc = crc32_buffer(&slice[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(JournalFramingError::Recoverable(
            "journal record checksum mismatch".into(),
        ));
    }

    let mut o = 4usize;
    let user_id_len = u16::from_be_bytes([slice[o], slice[o + 1]]) as usize;
    o += 2;
    if user_id_len == 0 || user_id_len > MAX_USER_ID_BYTES || o + user_id_len > crc_offset {
        return Err(JournalFramingError::Recoverable(
            "journal userId length invalid".into(),
        ));
    }
    let discord_user_id = std::str::from_utf8(&slice[o..o + user_id_len])
        .map_err(|_| JournalFramingError::Recoverable("journal userId utf8".into()))?
        .to_string();
    o += user_id_len;
    let ssrc_epoch = u32::from_be_bytes([slice[o], slice[o + 1], slice[o + 2], slice[o + 3]]);
    o += 4;
    let sequence = u16::from_be_bytes([slice[o], slice[o + 1]]);
    o += 2;
    let rtp_timestamp = u32::from_be_bytes([slice[o], slice[o + 1], slice[o + 2], slice[o + 3]]);
    o += 4;
    let arrived_at_ns = u64::from_be_bytes([
        slice[o],
        slice[o + 1],
        slice[o + 2],
        slice[o + 3],
        slice[o + 4],
        slice[o + 5],
        slice[o + 6],
        slice[o + 7],
    ]);
    o += 8;
    let session_capture_offset_samples = if format_version == JOURNAL_FORMAT_VERSION_V2 {
        let capture = u64::from_be_bytes([
            slice[o],
            slice[o + 1],
            slice[o + 2],
            slice[o + 3],
            slice[o + 4],
            slice[o + 5],
            slice[o + 6],
            slice[o + 7],
        ]);
        o += 8;
        Some(capture)
    } else {
        None
    };
    let payload_len = u16::from_be_bytes([slice[o], slice[o + 1]]) as usize;
    o += 2;
    if payload_len > MAX_OPUS_PAYLOAD_BYTES || o + payload_len > crc_offset {
        return Err(JournalFramingError::Recoverable(
            "journal payload length invalid".into(),
        ));
    }
    let opus_payload = slice[o..o + payload_len].to_vec();
    Ok((
        JournalRecord {
            discord_user_id,
            ssrc_epoch,
            sequence,
            rtp_timestamp,
            arrived_at_ns,
            session_capture_offset_samples,
            opus_payload,
        },
        offset + record_size,
    ))
}

/// Encode a single journal record (including size prefix and CRC) for tests and fixtures.
pub fn encode_journal_record(format_version: u32, record: &JournalRecord) -> Vec<u8> {
    let user_bytes = record.discord_user_id.as_bytes();
    assert!(!user_bytes.is_empty() && user_bytes.len() <= MAX_USER_ID_BYTES);
    let payload_len = record.opus_payload.len();
    assert!(payload_len <= MAX_OPUS_PAYLOAD_BYTES);
    let body_len = 2
        + user_bytes.len()
        + 4
        + 2
        + 4
        + 8
        + if format_version == JOURNAL_FORMAT_VERSION_V2 {
            8
        } else {
            0
        }
        + 2
        + payload_len
        + 4;
    let record_size = 4 + body_len;
    let mut out = Vec::with_capacity(record_size);
    out.extend_from_slice(&(record_size as u32).to_be_bytes());
    out.extend_from_slice(&(user_bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(user_bytes);
    out.extend_from_slice(&record.ssrc_epoch.to_be_bytes());
    out.extend_from_slice(&record.sequence.to_be_bytes());
    out.extend_from_slice(&record.rtp_timestamp.to_be_bytes());
    out.extend_from_slice(&record.arrived_at_ns.to_be_bytes());
    if format_version == JOURNAL_FORMAT_VERSION_V2 {
        let capture = record.session_capture_offset_samples.unwrap_or(0);
        out.extend_from_slice(&capture.to_be_bytes());
    }
    out.extend_from_slice(&(payload_len as u16).to_be_bytes());
    out.extend_from_slice(&record.opus_payload);
    let crc = crc32_buffer(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

/// Minimal `.synj` file bytes containing zero or more encoded records.
pub fn encode_journal_file(format_version: u32, records: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&JOURNAL_MAGIC);
    out.extend_from_slice(&format_version.to_be_bytes());
    for record in records {
        out.extend_from_slice(record);
    }
    out
}
