//! Manifest stem validation and canonical digest (explicit stem order preserved).

use crate::voice_delivery::bounds::{RIFF_MAX_CHUNK_BYTES, STEREO_PCM_FRAME_BYTES};
use crate::voice_delivery::fs::validate_single_component;
use crate::voice_delivery::hash::{hex_digest, sha256_hex_reader};
use crate::voice_delivery::wav::minimal_wav_header;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Cursor;
use thiserror::Error;

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const STEM_NAME_MAX_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StemManifestEntry {
    pub file_name: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedManifest {
    pub schema_version: u32,
    pub stems: Vec<StemManifestEntry>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("invalid stem manifest: {reason}")]
    Invalid { reason: String },
    #[error("manifest digest mismatch")]
    DigestMismatch,
    #[error("hash error: {0}")]
    Hash(String),
}

fn is_portable_stem_byte(b: u8) -> bool {
    matches!(
        b,
        b'a'..=b'z'
            | b'A'..=b'Z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
    )
}

fn validate_sha256_hex(s: &str) -> Result<(), ManifestError> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ManifestError::Invalid {
            reason: "stem sha256 must be 64 lowercase hex digits".into(),
        });
    }
    Ok(())
}

fn is_windows_reserved_device_stem(file_name: &str) -> bool {
    let upper = file_name.to_ascii_uppercase();
    let stem = upper.strip_suffix(".WAV").unwrap_or(&upper);
    let stem = stem.split('.').next().unwrap_or(stem);
    matches!(
        stem,
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn validate_stem_byte_count(byte_count: u64) -> Result<(), ManifestError> {
    if byte_count < 44 {
        return Err(ManifestError::Invalid {
            reason: "byte_count below canonical WAV minimum".into(),
        });
    }
    let data_bytes = byte_count - 44;
    if !data_bytes.is_multiple_of(STEREO_PCM_FRAME_BYTES) {
        return Err(ManifestError::Invalid {
            reason: "byte_count data region not frame aligned".into(),
        });
    }
    if data_bytes > RIFF_MAX_CHUNK_BYTES {
        return Err(ManifestError::Invalid {
            reason: "byte_count exceeds RIFF limit".into(),
        });
    }
    minimal_wav_header(data_bytes).map_err(|e| ManifestError::Invalid {
        reason: e.to_string(),
    })?;
    Ok(())
}

fn validate_stem_file_name(file_name: &str) -> Result<(), ManifestError> {
    if file_name.is_empty() || file_name.len() > STEM_NAME_MAX_LEN {
        return Err(ManifestError::Invalid {
            reason: "stem file name length out of range".into(),
        });
    }
    if file_name.contains('/') || file_name.contains('\\') {
        return Err(ManifestError::Invalid {
            reason: "path separators not allowed".into(),
        });
    }
    if file_name == "." || file_name == ".." {
        return Err(ManifestError::Invalid {
            reason: "reserved file name".into(),
        });
    }
    if !file_name.ends_with(".wav") {
        return Err(ManifestError::Invalid {
            reason: "stem must be a .wav file".into(),
        });
    }
    if !file_name.bytes().all(is_portable_stem_byte) {
        return Err(ManifestError::Invalid {
            reason: "non-portable stem file name".into(),
        });
    }
    if file_name.starts_with('.') || file_name.ends_with('.') {
        return Err(ManifestError::Invalid {
            reason: "leading or trailing dot".into(),
        });
    }
    if is_windows_reserved_device_stem(file_name) {
        return Err(ManifestError::Invalid {
            reason: "windows reserved device stem name".into(),
        });
    }
    Ok(())
}

fn stem_names_unique(stems: &[StemManifestEntry]) -> Result<(), ManifestError> {
    let mut seen = std::collections::HashSet::new();
    for stem in stems {
        let key = stem.file_name.to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(ManifestError::Invalid {
                reason: format!("duplicate stem {}", stem.file_name),
            });
        }
    }
    Ok(())
}

/// Canonical manifest digest: SHA-256 of JSON bytes for `{schema_version, stems}` in **caller order**.
pub fn manifest_digest_from_stems(stems: &[StemManifestEntry]) -> String {
    let body = serde_json::json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "stems": stems,
    });
    let bytes = serde_json::to_vec(&body).expect("manifest json");
    hex_digest(&Sha256::digest(&bytes))
}

impl ValidatedManifest {
    pub fn validate(stems: Vec<StemManifestEntry>) -> Result<Self, ManifestError> {
        if stems.is_empty() {
            return Err(ManifestError::Invalid {
                reason: "empty stem list".into(),
            });
        }
        for stem in &stems {
            validate_stem_file_name(&stem.file_name)?;
            validate_sha256_hex(&stem.sha256)?;
            validate_stem_byte_count(stem.byte_count)?;
        }
        stem_names_unique(&stems)?;
        Ok(Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            stems,
        })
    }

    pub fn digest(&self) -> String {
        manifest_digest_from_stems(&self.stems)
    }
}

/// Stream-hash manifest JSON bytes (for tests and verification helpers).
pub fn digest_json_bytes(json: &[u8], chunk_size: usize) -> Result<String, ManifestError> {
    sha256_hex_reader(Cursor::new(json), chunk_size).map_err(|e| ManifestError::Hash(e.to_string()))
}

#[cfg(test)]
mod stem_manifest_rejects_empty_duplicate_and_traversal {
    use super::*;

    #[test]
    fn stem_manifest_rejects_empty_duplicate_and_traversal() {
        assert!(matches!(
            ValidatedManifest::validate(vec![]),
            Err(ManifestError::Invalid { .. })
        ));
        let dup = vec![
            StemManifestEntry {
                file_name: "a.wav".into(),
                byte_count: 44,
                sha256: "a".repeat(64),
            },
            StemManifestEntry {
                file_name: "a.wav".into(),
                byte_count: 44,
                sha256: "b".repeat(64),
            },
        ];
        assert!(ValidatedManifest::validate(dup).is_err());
        let traversal = vec![StemManifestEntry {
            file_name: "../x.wav".into(),
            byte_count: 44,
            sha256: "c".repeat(64),
        }];
        assert!(ValidatedManifest::validate(traversal).is_err());
    }

    #[test]
    fn rejects_windows_reserved_stem_names() {
        assert!(ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "CON.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .is_err());
    }

    #[test]
    fn manifest_digest_preserves_explicit_order() {
        let a = StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "d".repeat(64),
        };
        let b = StemManifestEntry {
            file_name: "b.wav".into(),
            byte_count: 44,
            sha256: "e".repeat(64),
        };
        let d1 = manifest_digest_from_stems(&[a.clone(), b.clone()]);
        let d2 = manifest_digest_from_stems(&[b, a]);
        assert_ne!(d1, d2);
    }

    #[test]
    fn case_insensitive_duplicate_stems_rejected() {
        let dup = vec![
            StemManifestEntry {
                file_name: "Track.wav".into(),
                byte_count: 44,
                sha256: "a".repeat(64),
            },
            StemManifestEntry {
                file_name: "track.wav".into(),
                byte_count: 44,
                sha256: "b".repeat(64),
            },
        ];
        assert!(ValidatedManifest::validate(dup).is_err());
    }
}
