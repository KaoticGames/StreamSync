//! Shared fixtures for publication tests.

use crate::voice_delivery::fs::DirHandle;
use crate::voice_delivery::fs::{DestRoot, PortableParentComponent};
use crate::voice_delivery::hash::SyntheticByteSource;
use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
use crate::voice_delivery::marker::verify_exact_staging_membership;
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::wav::minimal_wav_header;

pub fn stage_token(hex32: &str) -> String {
    assert_eq!(hex32.len(), 32);
    format!(".streamsync-stage-{hex32}")
}

pub fn write_minimal_stem(staging: &DirHandle, name: &str, data_bytes: u64) -> StemManifestEntry {
    let header = minimal_wav_header(data_bytes).unwrap();
    let mut body = Vec::new();
    let mut pos = 0u64;
    while pos < data_bytes {
        body.push(SyntheticByteSource::byte_at(pos));
        pos += 1;
    }
    let mut file = staging.create_new_file(name).unwrap();
    file.write_all_at(0, &header).unwrap();
    file.write_all_at(header.len() as u64, &body).unwrap();
    file.std_file().sync_all().unwrap();
    let sha = crate::voice_delivery::hash::sha256_hex_reader(
        crate::voice_delivery::fs::file::open_existing_file_at(staging, name)
            .unwrap()
            .std_file(),
        crate::voice_delivery::hash::DEFAULT_STREAM_CHUNK,
    )
    .unwrap();
    StemManifestEntry {
        file_name: name.to_string(),
        byte_count: header.len() as u64 + data_bytes,
        sha256: sha,
    }
}

pub struct SealedFixture {
    pub tmp: tempfile::TempDir,
    pub guard: DeliverySessionGuard,
    pub stage: String,
}

pub fn sealed_publish_intent_fixture(
    delivery_id: &str,
    final_name: &str,
    parent: &[&str],
) -> SealedFixture {
    let tmp = tempfile::tempdir().unwrap();
    let root = DestRoot::open(tmp.path()).unwrap();
    let stage = stage_token("deadbeefdeadbeefdeadbeefdeadbeef");
    let mut current = root.handle().clone_handle().unwrap();
    for comp in parent {
        current = current.create_or_open_child_dir(comp).unwrap();
    }
    current.create_child_dir(&stage).unwrap();
    let staging = current.open_child_dir(&stage).unwrap();
    let stem = write_minimal_stem(&staging, "a.wav", 8);
    let manifest = ValidatedManifest::validate(vec![stem]).unwrap();
    let parent_comps: Vec<PortableParentComponent> = parent
        .iter()
        .map(|c| PortableParentComponent::validate(c).unwrap())
        .collect();
    let guard = DeliverySessionGuard::begin(
        root,
        delivery_id,
        manifest,
        &stage,
        parent_comps,
        final_name,
        true,
    )
    .unwrap();
    let ledger =
        crate::voice_delivery::records::ledger_generation::LedgerStore::open_for_guard(&guard)
            .unwrap();
    ledger
        .commit(&guard, crate::voice_delivery::state::LedgerState::Receiving)
        .unwrap();
    guard.seal_and_write_publish_intent().unwrap();
    SealedFixture { tmp, guard, stage }
}

pub fn ensure_stems_without_marker(guard: &DeliverySessionGuard) {
    verify_exact_staging_membership(guard.staging_dir().unwrap(), guard.manifest()).unwrap();
}
