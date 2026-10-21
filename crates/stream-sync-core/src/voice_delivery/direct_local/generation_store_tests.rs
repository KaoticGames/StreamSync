//! Generation-scoped on-disk layout: non-colliding paths and create-new segment files.

use super::test_support::{minimal_synj_segment_bytes, sha256_lower_hex, SESSION_ID};
use crate::voice_delivery::direct_local::{
    DirectGenerationStore, DirectGenerationStoreError, DirectSegmentFileName,
};
use crate::voice_delivery::fs::DestRoot;
use std::fs;
use std::io::Write;

#[test]
fn generation_scoped_paths_do_not_collide() {
    let path_a = DirectSegmentFileName::for_identity(SESSION_ID, 1, 0);
    let path_b = DirectSegmentFileName::for_identity(SESSION_ID, 2, 0);
    let path_c = DirectSegmentFileName::for_identity(SESSION_ID, 1, 1);
    assert_ne!(path_a.relative_path(), path_b.relative_path());
    assert_ne!(path_a.relative_path(), path_c.relative_path());
    assert_ne!(path_b.relative_path(), path_c.relative_path());
}

#[test]
fn complete_segment_uses_create_new_not_append() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("dest root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("open store");
    let bytes = minimal_synj_segment_bytes();
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    store
        .write_complete_segment_create_new(&identity, &bytes)
        .expect("first write");
    let err = store
        .write_complete_segment_create_new(&identity, b"append-attempt")
        .expect_err("create-new collision");
    assert!(matches!(
        err,
        DirectGenerationStoreError::SegmentAlreadyExists { .. }
    ));

    let seg_path = tmp.path().join(store.segment_relative_path(&identity));
    let on_disk = fs::read(&seg_path).expect("read segment");
    assert_eq!(on_disk, bytes);
}

#[test]
fn open_segment_file_is_not_opened_for_append() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("dest root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("open store");
    let bytes = minimal_synj_segment_bytes();
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    store
        .write_complete_segment_create_new(&identity, &bytes)
        .expect("write");
    let on_disk = store
        .read_segment_bytes(&identity)
        .expect("reopen via dir handle");
    assert_eq!(on_disk, bytes);
    let seg_path = tmp.path().join(store.segment_relative_path(&identity));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&seg_path)
        .expect("external writer");
    file.write_all(b"tail").expect("external append attempt");
    let still = store
        .read_segment_bytes(&identity)
        .expect("read after external write");
    assert_ne!(
        still, bytes,
        "store reads durable file via reopen, not write handle"
    );
}
