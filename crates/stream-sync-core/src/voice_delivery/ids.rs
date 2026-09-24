//! Opaque tokens and stable lock path identity (no lossy sanitization).

use sha2::{Digest, Sha256};

const LOCK_DIR: &str = ".streamsync-control";
const LOCKS_SEGMENT: &str = "locks";

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

/// Opaque staging directory basename: `.streamsync-stage-<32-byte-hex>`.
pub fn new_opaque_stage_basename() -> String {
    format!(".streamsync-stage-{}", uuid::Uuid::new_v4().simple())
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
}
