//! Platform filesystem backend for voice delivery publication geometry.

mod dir;
mod durability;
mod error;
mod geometry;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub use dir::{DestRoot, DirHandle, ValidatedFinalName};
pub use error::FsError;
pub use geometry::FinalParentPublication;

use dir::validate_single_component;

/// Same final-parent handle for source and destination directory names.
pub(crate) fn rename_no_replace_same_parent(
    final_parent: &DirHandle,
    src_name: &str,
    dst_name: &str,
) -> Result<(), FsError> {
    validate_single_component(src_name)?;
    validate_single_component(dst_name)?;
    #[cfg(unix)]
    {
        unix::rename_no_replace_same_parent(final_parent, src_name, dst_name)
    }
    #[cfg(windows)]
    {
        windows::rename_no_replace_same_parent(final_parent, src_name, dst_name)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (final_parent, src_name, dst_name);
        Err(FsError::Unsupported)
    }
}

pub(crate) fn open_lock_file_at_root(
    root: &DestRoot,
    rel_components: &[String],
) -> Result<std::fs::File, FsError> {
    #[cfg(unix)]
    {
        unix::open_lock_file(root.handle(), rel_components)
    }
    #[cfg(windows)]
    {
        windows::open_lock_file(root.handle(), rel_components)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (root, rel_components);
        Err(FsError::Unsupported)
    }
}

#[cfg(test)]
mod linux_rename_no_replace_same_parent {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn temp_final_parent() -> (tempfile::TempDir, DirHandle) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_path = tmp.path().join("final-parent");
        fs::create_dir_all(&parent_path).expect("mkdir");
        let root = DestRoot::open(tmp.path()).expect("open root");
        let parent = root
            .open_dir_relative(&["final-parent"])
            .expect("open parent");
        (tmp, parent)
    }

    #[test]
    fn sibling_directory_rename_succeeds() {
        let (_tmp, parent) = temp_final_parent();
        parent
            .create_child_dir(".streamsync-stage-abc")
            .expect("stage");
        rename_no_replace_same_parent(&parent, ".streamsync-stage-abc", "published-session")
            .expect("rename");
        assert!(parent.open_child_dir("published-session").is_ok());
        assert!(parent.open_child_dir(".streamsync-stage-abc").is_err());
    }

    #[test]
    fn pre_existing_file_collision_preserves_both() {
        let (_tmp, parent) = temp_final_parent();
        parent
            .create_child_dir(".streamsync-stage-src")
            .expect("stage");
        let dest_path = _tmp.path().join("final-parent").join("existing-final");
        fs::write(&dest_path, b"DEST_BYTES").expect("dest");
        let result =
            rename_no_replace_same_parent(&parent, ".streamsync-stage-src", "existing-final");
        assert!(matches!(result, Err(FsError::AlreadyExists)));
        assert_eq!(fs::read(&dest_path).expect("read dest"), b"DEST_BYTES");
        assert!(parent.open_child_dir(".streamsync-stage-src").is_ok());
    }

    #[test]
    fn pre_existing_nonempty_dir_collision_untouched() {
        let (_tmp, parent) = temp_final_parent();
        parent
            .create_child_dir(".streamsync-stage-src")
            .expect("stage");
        parent.create_child_dir("existing-final").expect("dest dir");
        let marker = _tmp.path().join("final-parent/existing-final/marker.txt");
        fs::write(&marker, b"INNER").expect("inner");
        let result =
            rename_no_replace_same_parent(&parent, ".streamsync-stage-src", "existing-final");
        assert!(matches!(result, Err(FsError::AlreadyExists)));
        assert_eq!(fs::read(marker).expect("read"), b"INNER");
        assert!(parent.open_child_dir(".streamsync-stage-src").is_ok());
    }

    #[test]
    fn concurrent_sibling_renames_exactly_one_wins() {
        let (_tmp, parent) = temp_final_parent();
        parent.create_child_dir(".streamsync-stage-a").expect("a");
        parent.create_child_dir(".streamsync-stage-b").expect("b");
        let p1 = parent.clone_handle().expect("clone1");
        let p2 = parent.clone_handle().expect("clone2");
        let barrier = Arc::new(Barrier::new(2));
        let t1 = thread::spawn({
            let b = barrier.clone();
            move || {
                b.wait();
                rename_no_replace_same_parent(&p1, ".streamsync-stage-a", "winner-name")
            }
        });
        let t2 = thread::spawn({
            let b = barrier.clone();
            move || {
                b.wait();
                rename_no_replace_same_parent(&p2, ".streamsync-stage-b", "winner-name")
            }
        });
        let r1 = t1.join().expect("j1");
        let r2 = t2.join().expect("j2");
        let wins = [r1.is_ok(), r2.is_ok()];
        assert_eq!(wins.iter().filter(|w| **w).count(), 1);
        let exists_a = parent.open_child_dir(".streamsync-stage-a").is_ok();
        let exists_b = parent.open_child_dir(".streamsync-stage-b").is_ok();
        assert!(exists_a ^ exists_b);
        assert!(parent.open_child_dir("winner-name").is_ok());
    }

    #[test]
    fn sync_dir_exact_targets_opened_directory_fd() {
        let (_tmp, parent) = temp_final_parent();
        durability::sync_dir_exact(&parent).expect("sync");
    }

    #[test]
    #[cfg(all(unix, not(target_os = "linux")))]
    fn unsupported_unix_fails_closed() {
        let (_tmp, parent) = temp_final_parent();
        parent.create_child_dir("stage-x").expect("stage");
        let err = rename_no_replace_same_parent(&parent, "stage-x", "final-y");
        assert!(matches!(err, Err(FsError::Unsupported)));
    }
}

#[cfg(test)]
mod dest_capability_reparse {
    use super::*;
    use std::fs;
    #[test]
    #[cfg(target_os = "linux")]
    fn symlink_dest_root_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let real = tmp.path().join("real-root");
        fs::create_dir(&real).expect("real");
        let link = tmp.path().join("link-root");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        let err = DestRoot::open(&link);
        assert!(matches!(err, Err(FsError::SymlinkOrReparseRoot)));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn symlink_final_parent_component_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        let real = tmp.path().join("real-parent");
        fs::create_dir(&real).expect("real");
        let link_name = "link-parent";
        std::os::unix::fs::symlink(&real, tmp.path().join(link_name)).expect("symlink");
        let err = root.open_dir_relative(&[link_name]);
        assert!(matches!(err, Err(FsError::SymlinkOrReparseComponent(_))));
    }

    #[test]
    fn dotdot_component_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        let err = root.open_dir_relative(&[".."]);
        assert!(matches!(err, Err(FsError::InvalidComponent(_))));
    }
}

#[cfg(test)]
mod stage_final_sibling_final_parent {
    use super::*;
    use crate::voice_delivery::ids::new_opaque_stage_basename;
    use std::fs;

    #[test]
    fn stage_and_final_are_siblings_under_one_parent_handle() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        fs::create_dir(tmp.path().join("guild")).expect("guild");
        let final_parent = root.open_dir_relative(&["guild"]).expect("parent");
        let publication = FinalParentPublication::new(final_parent);
        let stage_name = publication.create_stage_dir().expect("stage");
        let final_name = ValidatedFinalName::validate("my-session").expect("final");
        publication
            .rename_stage_to_final(&stage_name, &final_name)
            .expect("publish rename");
        let parent_path = tmp.path().join("guild");
        assert!(parent_path.join(final_name.as_str()).is_dir());
        assert!(!parent_path.join(&stage_name).exists());
    }

    #[test]
    fn wrong_parent_rename_not_expressible_via_api() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = DestRoot::open(tmp.path()).expect("root");
        fs::create_dir(tmp.path().join("a")).expect("a");
        fs::create_dir(tmp.path().join("b")).expect("b");
        let parent_a = root.open_dir_relative(&["a"]).expect("pa");
        let parent_b = root.open_dir_relative(&["b"]).expect("pb");
        let stage = new_opaque_stage_basename();
        parent_a.create_child_dir(&stage).expect("stage in a");
        let final_name = ValidatedFinalName::validate("sess").expect("final");
        let err = rename_no_replace_same_parent(&parent_b, &stage, final_name.as_str());
        assert!(matches!(err, Err(FsError::Io(_))));
        assert!(tmp.path().join("a").join(&stage).is_dir());
    }

    #[test]
    fn reserved_streamsync_prefix_rejected_for_final_name() {
        let err = ValidatedFinalName::validate(".streamsync-stage-deadbeef");
        assert!(matches!(err, Err(FsError::InvalidFinalName(_))));
    }

    /// Nested mount/junction sibling proof requires elevation on Windows and bind mounts on Linux.
    #[test]
    #[ignore = "manual/CI-qualified: nested mount or junction under final-parent"]
    fn nested_mount_junction_sibling_geometry() {
        // Documented limit: run in qualified CI/VM with prepared mount layout.
    }
}

#[cfg(all(test, windows))]
mod windows_handle_rename_no_replace {
    use super::*;
    use std::fs;

    fn temp_final_parent() -> (tempfile::TempDir, DirHandle) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_path = tmp.path().join("final-parent");
        fs::create_dir_all(&parent_path).expect("mkdir");
        let root = DestRoot::open(tmp.path()).expect("open root");
        let parent = root
            .open_dir_relative(&["final-parent"])
            .expect("open parent");
        (tmp, parent)
    }

    #[test]
    fn sibling_directory_rename_succeeds() {
        let (_tmp, parent) = temp_final_parent();
        parent
            .create_child_dir(".streamsync-stage-win")
            .expect("stage");
        rename_no_replace_same_parent(&parent, ".streamsync-stage-win", "published-win")
            .expect("rename");
        assert!(parent.open_child_dir("published-win").is_ok());
    }

    #[test]
    fn pre_existing_final_collision_preserves_both() {
        let (_tmp, parent) = temp_final_parent();
        parent
            .create_child_dir(".streamsync-stage-win2")
            .expect("stage");
        let dest_path = _tmp.path().join("final-parent").join("taken");
        fs::write(&dest_path, b"KEEP").expect("dest");
        let result = rename_no_replace_same_parent(&parent, ".streamsync-stage-win2", "taken");
        assert!(matches!(result, Err(FsError::AlreadyExists)));
        assert_eq!(fs::read(&dest_path).expect("read"), b"KEEP");
        assert!(parent.open_child_dir(".streamsync-stage-win2").is_ok());
    }

    #[test]
    fn invalid_empty_destination_fails_closed() {
        let (_tmp, parent) = temp_final_parent();
        parent.create_child_dir("stage-z").expect("stage");
        let err = rename_no_replace_same_parent(&parent, "stage-z", "");
        assert!(matches!(err, Err(FsError::InvalidComponent(_))));
    }
}
