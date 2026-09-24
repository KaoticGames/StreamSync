//! Opaque tokens and stable lock path identity (no lossy sanitization).

use crate::voice_delivery::fs::FsError;
use sha2::{Digest, Sha256};

const LOCK_DIR: &str = ".streamsync-control";
const LOCKS_SEGMENT: &str = "locks";
const LEDGERS_SEGMENT: &str = "ledgers";
const CHECKPOINTS_SEGMENT: &str = "checkpoints";
const STAGE_PREFIX: &str = ".streamsync-stage-";
const STAGE_HEX_LEN: usize = 32;

/// Opaque directory name for ledger/checkpoint generations (hash of canonical delivery id).
pub fn delivery_opaque_dir_id(canonical_delivery_id: &str) -> String {
    let digest = Sha256::digest(canonical_delivery_id.as_bytes());
    format!("{:x}", digest)
}

/// Relative components from `DEST_ROOT` to ledger generation directory for a delivery.
pub fn ledger_dir_relative_components(opaque_delivery_id: &str) -> [String; 3] {
    [
        LOCK_DIR.to_string(),
        LEDGERS_SEGMENT.to_string(),
        opaque_delivery_id.to_string(),
    ]
}

/// Relative components from `DEST_ROOT` to checkpoint generation directory for a delivery.
pub fn checkpoint_dir_relative_components(opaque_delivery_id: &str) -> [String; 3] {
    [
        LOCK_DIR.to_string(),
        CHECKPOINTS_SEGMENT.to_string(),
        opaque_delivery_id.to_string(),
    ]
}

/// SHA-256 hex digest of the full canonical delivery id (lock filename suffix).
pub fn stable_lock_file_basename(canonical_delivery_id: &str) -> String {
    let digest = Sha256::digest(canonical_delivery_id.as_bytes());
    format!("{:x}.lock", digest)
}

/// Relative path components from `DEST_ROOT` to the stable lock file.
pub fn lock_file_relative_components(canonical_delivery_id: &str) -> [String; 3] {
    [
        LOCK_DIR.to_string(),
        LOCKS_SEGMENT.to_string(),
        stable_lock_file_basename(canonical_delivery_id),
    ]
}

/// Opaque staging directory basename: `.streamsync-stage-<32 lowercase hex>` (128-bit UUID simple form).
pub fn new_opaque_stage_basename() -> String {
    format!(".streamsync-stage-{}", uuid::Uuid::new_v4().simple())
}

/// Validate an existing stage directory basename (exact prefix + 32 lowercase hex chars).
pub fn validate_stage_basename(name: &str) -> Result<(), FsError> {
    if !name.starts_with(STAGE_PREFIX) {
        return Err(FsError::InvalidStageBasename);
    }
    let hex = name.strip_prefix(STAGE_PREFIX).expect("checked prefix");
    if hex.len() != STAGE_HEX_LEN {
        return Err(FsError::InvalidStageBasename);
    }
    if !hex
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(FsError::InvalidStageBasename);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_hash_uses_full_canonical_id() {
        let id = "guild:channel:delivery/with/slashes?and=unicode:🎙";
        let name = stable_lock_file_basename(id);
        assert!(name.ends_with(".lock"));
        assert_eq!(name.len(), 64 + ".lock".len());
        let again = stable_lock_file_basename(id);
        assert_eq!(name, again);
        assert_ne!(name, stable_lock_file_basename(&format!("{id}x")));
    }

    #[test]
    fn stage_basename_validation_table() {
        let valid = new_opaque_stage_basename();
        assert!(validate_stage_basename(&valid).is_ok());

        let fixed = ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef";
        assert!(validate_stage_basename(fixed).is_ok());

        let invalid = [
            ".streamsync-stage-abc",
            ".streamsync-stage-DEADBEEFdeadbeefdeadbeefdeadbeef",
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeeg",
            "streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            ".streamsync-stage-",
        ];
        for name in invalid {
            assert!(
                matches!(
                    validate_stage_basename(name),
                    Err(FsError::InvalidStageBasename)
                ),
                "expected reject for {:?}",
                name
            );
        }
    }
}
