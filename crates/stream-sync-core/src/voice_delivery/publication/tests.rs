#[cfg(test)]
mod recover_rename_outcome_matrix {
    use super::super::error::PublicationError;
    use super::super::publish::{
        prepare_publication, publish_prepared, PublicationOperation, PublicationOperationRecorder,
        PublishOptions,
    };
    use super::super::recover::{recover_delivery, RecoveryOutcome, QUARANTINE_AMBIGUOUS};
    use super::super::test_support::{
        sealed_publish_intent_fixture, stage_token, write_minimal_stem,
    };
    use crate::voice_delivery::fs::durability::NamespaceDurability;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::session::DeliverySessionGuard;
    use crate::voice_delivery::state::LedgerState;
    use std::fs;

    #[test]
    fn publish_intent_stage_only_publishes() {
        let fx = sealed_publish_intent_fixture("pub-stage-only", "final-session", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        let published = publish_prepared(prepared, None, PublishOptions::default()).unwrap();
        assert_eq!(published.state, LedgerState::Published);
        #[cfg(target_os = "linux")]
        assert_eq!(
            published.namespace_durability,
            Some(NamespaceDurability::Proven)
        );
        #[cfg(windows)]
        assert_eq!(
            published.namespace_durability,
            Some(NamespaceDurability::Unavailable)
        );
        assert!(fx.tmp.path().join("final-session").is_dir());
        assert!(!fx.tmp.path().join(&fx.stage).exists());
    }

    #[test]
    fn publish_intent_final_only_recovery_writes_published() {
        let fx = sealed_publish_intent_fixture("pub-final-only", "final-only", &[]);
        let parent = fx.guard.final_parent_dir();
        let publication =
            crate::voice_delivery::fs::FinalParentPublication::new(parent.clone_handle().unwrap());
        let final_name =
            crate::voice_delivery::fs::ValidatedFinalName::validate("final-only").unwrap();
        publication
            .rename_stage_to_final(&fx.stage, &final_name)
            .unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        let outcome = recover_delivery(&fx.guard, Some(&mut recorder)).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Published(_)));
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::ParentNamespaceDurability,
                PublicationOperation::FinalReopenVerify,
                PublicationOperation::PublishedCommitted,
            ]
        );
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::Published
        );
    }

    #[test]
    fn publish_intent_both_stage_and_final_quarantines() {
        let fx = sealed_publish_intent_fixture("pub-ambiguous", "ambig", &[]);
        fs::create_dir(fx.tmp.path().join("ambig")).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        let gen = match outcome {
            RecoveryOutcome::Quarantined(g) => g,
            other => panic!("expected quarantine, got {other:?}"),
        };
        assert_eq!(gen.state, LedgerState::Quarantined);
        assert_eq!(gen.quarantine_reason.as_deref(), Some(QUARANTINE_AMBIGUOUS));
        let again = recover_delivery(&fx.guard, None).unwrap();
        let gen2 = match again {
            RecoveryOutcome::Quarantined(g) => g,
            other => panic!("{other:?}"),
        };
        assert_eq!(gen.generation, gen2.generation);
    }

    #[test]
    fn publish_intent_neither_quarantines_missing() {
        let fx = sealed_publish_intent_fixture("pub-missing", "gone", &[]);
        fs::remove_dir_all(fx.tmp.path().join(&fx.stage)).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }

    #[test]
    fn published_final_only_idempotent() {
        let fx = sealed_publish_intent_fixture("pub-idem", "idem-final", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        publish_prepared(prepared, None, PublishOptions::default()).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::IdempotentPublished));
    }

    #[test]
    fn published_with_staging_present_quarantines() {
        let fx = sealed_publish_intent_fixture("pub-stg-after", "after-pub", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        publish_prepared(prepared, None, PublishOptions::default()).unwrap();
        fs::create_dir(fx.tmp.path().join(&fx.stage)).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }

    #[test]
    fn receiving_recovery_does_not_publish() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = stage_token("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        root.create_child_dir(&stage).unwrap();
        let staging = root.open_child_dir(&stage).unwrap();
        let stem = write_minimal_stem(&staging, "a.wav", 4);
        let manifest =
            crate::voice_delivery::manifest::ValidatedManifest::validate(vec![stem]).unwrap();
        let guard =
            DeliverySessionGuard::begin(root, "recv-only", manifest, &stage, vec![], "final", true)
                .unwrap();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        store.commit(&guard, LedgerState::Receiving).unwrap();
        let outcome = recover_delivery(&guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::ResumeIngest));
        assert!(!tmp.path().join("final").exists());
    }

    #[test]
    fn sealed_advances_to_publish_intent_without_rename() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = stage_token("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        root.create_child_dir(&stage).unwrap();
        let staging = root.open_child_dir(&stage).unwrap();
        let stem = write_minimal_stem(&staging, "a.wav", 4);
        let manifest =
            crate::voice_delivery::manifest::ValidatedManifest::validate(vec![stem]).unwrap();
        let guard = DeliverySessionGuard::begin(
            root,
            "sealed-adv",
            manifest,
            &stage,
            vec![],
            "final",
            true,
        )
        .unwrap();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        store.commit(&guard, LedgerState::Receiving).unwrap();
        let (_sealed, _intent, _) = guard.seal_and_write_publish_intent().unwrap();
        let gen_dir = tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(guard.identity().opaque_delivery_id.clone());
        for name in fs::read_dir(&gen_dir).unwrap() {
            let path = name.unwrap().path();
            if path.file_name().unwrap().to_string_lossy().contains("gen-") {
                let data = fs::read_to_string(&path).unwrap();
                if data.contains("\"publish_intent\"") {
                    fs::remove_file(&path).unwrap();
                }
            }
        }
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::Sealed
        );
        let outcome = recover_delivery(&guard, None).unwrap();
        assert!(matches!(
            outcome,
            RecoveryOutcome::AdvancedToPublishIntent(_)
        ));
        assert!(tmp.path().join(&stage).is_dir());
        assert!(!tmp.path().join("final").exists());
    }

    #[test]
    fn unrelated_destination_file_blocks_publish() {
        let fx = sealed_publish_intent_fixture("pub-collision", "taken", &[]);
        fs::write(fx.tmp.path().join("taken"), b"foreign").unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let err = publish_prepared(prepared, None, PublishOptions::default()).unwrap_err();
        assert!(matches!(
            err,
            PublicationError::UnrelatedDestination
                | PublicationError::OtherArtifact { role: "final" }
        ));
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
        assert_eq!(fs::read(fx.tmp.path().join("taken")).unwrap(), b"foreign");
    }

    #[test]
    fn foreign_final_dir_without_marker_blocks() {
        let fx = sealed_publish_intent_fixture("pub-foreign-dir", "foreign-dir", &[]);
        fs::create_dir(fx.tmp.path().join("foreign-dir")).unwrap();
        fs::write(fx.tmp.path().join("foreign-dir/note.txt"), b"x").unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }
}

#[cfg(test)]
mod publication_durability_order {
    use super::super::publish::{
        prepare_publication, publish_prepared, PublicationOperation, PublicationOperationRecorder,
        PublishOptions,
    };
    use super::super::test_support::sealed_publish_intent_fixture;

    #[test]
    fn publication_operation_order() {
        let fx = sealed_publish_intent_fixture("pub-order", "ordered-final", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        publish_prepared(prepared, Some(&mut recorder), PublishOptions::default()).unwrap();
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
                PublicationOperation::RenameNoReplace,
                PublicationOperation::ParentNamespaceDurability,
                PublicationOperation::FinalReopenVerify,
                PublicationOperation::PublishedCommitted,
            ]
        );
        let rename_idx = recorder
            .ops
            .iter()
            .position(|o| *o == PublicationOperation::RenameNoReplace)
            .unwrap();
        let published_idx = recorder
            .ops
            .iter()
            .position(|o| *o == PublicationOperation::PublishedCommitted)
            .unwrap();
        let verify_idx = recorder
            .ops
            .iter()
            .position(|o| *o == PublicationOperation::FinalReopenVerify)
            .unwrap();
        assert!(rename_idx < verify_idx);
        assert!(verify_idx < published_idx);
    }

    #[test]
    fn publication_operation_recorder_appends_every_invocation() {
        let mut recorder = PublicationOperationRecorder::default();
        recorder.record(PublicationOperation::PublishIntentObserved);
        recorder.record(PublicationOperation::PublishIntentObserved);
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::PublishIntentObserved,
            ]
        );
    }

    #[test]
    fn publication_operation_order_final_only() {
        let fx = sealed_publish_intent_fixture("pub-order-final", "final-only-order", &[]);
        let parent = fx.guard.final_parent_dir();
        let publication =
            crate::voice_delivery::fs::FinalParentPublication::new(parent.clone_handle().unwrap());
        let final_name =
            crate::voice_delivery::fs::ValidatedFinalName::validate("final-only-order").unwrap();
        publication
            .rename_stage_to_final(&fx.stage, &final_name)
            .unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        publish_prepared(prepared, Some(&mut recorder), PublishOptions::default()).unwrap();
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::ParentNamespaceDurability,
                PublicationOperation::FinalReopenVerify,
                PublicationOperation::PublishedCommitted,
            ]
        );
    }
}

#[cfg(test)]
mod injectable_rename_outcome_matrix {
    use super::super::error::PublicationError;
    use super::super::publish::{
        prepare_publication, publish_prepared, PublicationOperation, PublicationOperationRecorder,
        PublishOptions,
    };
    use super::super::recover::{recover_delivery, RecoveryOutcome};
    use super::super::test_support::sealed_publish_intent_fixture;
    use crate::voice_delivery::fs::{DirHandle, FsError, ValidatedFinalName};
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::state::LedgerState;

    fn inject_io() -> crate::voice_delivery::publication::publish::RenameInjectFn {
        fn wrap(
            _parent: &DirHandle,
            _stage: &str,
            _final: &ValidatedFinalName,
        ) -> Result<(), FsError> {
            Err(FsError::Io(std::io::Error::other("injected")))
        }
        wrap
    }

    fn inject_already_exists() -> crate::voice_delivery::publication::publish::RenameInjectFn {
        fn wrap(
            _parent: &DirHandle,
            _stage: &str,
            _final: &ValidatedFinalName,
        ) -> Result<(), FsError> {
            Err(FsError::AlreadyExists)
        }
        wrap
    }

    fn inject_rename_then_already_exists(
    ) -> crate::voice_delivery::publication::publish::RenameInjectFn {
        fn wrap(
            parent: &DirHandle,
            stage: &str,
            final_name: &ValidatedFinalName,
        ) -> Result<(), FsError> {
            let publication =
                crate::voice_delivery::fs::FinalParentPublication::new(parent.clone_handle()?);
            publication.rename_stage_to_final(stage, final_name)?;
            Err(FsError::AlreadyExists)
        }
        wrap
    }

    fn inject_foreign_final_reconcile(
    ) -> crate::voice_delivery::publication::publish::RenameInjectFn {
        fn wrap(
            _parent: &DirHandle,
            stage: &str,
            final_name: &ValidatedFinalName,
        ) -> Result<(), FsError> {
            let root = std::env::var("STREAMSYNC_INJECT_RECONCILE_ROOT")
                .map(std::path::PathBuf::from)
                .map_err(|_| FsError::Io(std::io::Error::other("missing root")))?;
            let stage_path = root.join(stage);
            let _ = std::fs::remove_dir_all(&stage_path);
            let final_path = root.join(final_name.as_str());
            std::fs::create_dir(&final_path).map_err(FsError::Io)?;
            std::fs::write(final_path.join("foreign.txt"), b"x").map_err(FsError::Io)?;
            Err(FsError::AlreadyExists)
        }
        wrap
    }

    #[test]
    fn rename_error_stage_only_leaves_publish_intent() {
        let fx = sealed_publish_intent_fixture("inj-stage", "inj-final", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        let err = publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_io()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(matches!(err, PublicationError::RenameFailed { .. }));
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
            ]
        );
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::PublishIntent
        );
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
    }

    #[test]
    fn rename_already_exists_stage_only_truthful() {
        let fx = sealed_publish_intent_fixture("inj-ae-stage", "inj-ae-final", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        let err = publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_already_exists()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PublicationError::RenameFailed { detail } if detail == "already_exists"
        ));
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
            ]
        );
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::PublishIntent
        );
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
        assert!(!fx.tmp.path().join("inj-ae-final").exists());
    }

    #[test]
    fn rename_error_final_only_commits_via_observation() {
        let fx = sealed_publish_intent_fixture("inj-final-only", "inj-committed", &[]);
        let parent = fx.guard.final_parent_dir();
        let publication =
            crate::voice_delivery::fs::FinalParentPublication::new(parent.clone_handle().unwrap());
        let final_name =
            crate::voice_delivery::fs::ValidatedFinalName::validate("inj-committed").unwrap();
        publication
            .rename_stage_to_final(&fx.stage, &final_name)
            .unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let published = publish_prepared(
            prepared,
            None,
            PublishOptions {
                rename_inject: Some(inject_already_exists()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(published.state, LedgerState::Published);
    }

    #[test]
    fn rename_error_both_present_quarantines_on_recovery() {
        let fx = sealed_publish_intent_fixture("inj-both", "inj-both-final", &[]);
        std::fs::create_dir(fx.tmp.path().join("inj-both-final")).unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let err = publish_prepared(
            prepared,
            None,
            PublishOptions {
                rename_inject: Some(inject_already_exists()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(matches!(err, PublicationError::AmbiguousPublication));
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }

    #[test]
    fn rename_unknown_outcome_operation_order_final_only() {
        let fx = sealed_publish_intent_fixture("inj-order-final", "inj-order-commit", &[]);
        let parent = fx.guard.final_parent_dir();
        let publication =
            crate::voice_delivery::fs::FinalParentPublication::new(parent.clone_handle().unwrap());
        let final_name =
            crate::voice_delivery::fs::ValidatedFinalName::validate("inj-order-commit").unwrap();
        publication
            .rename_stage_to_final(&fx.stage, &final_name)
            .unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_already_exists()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::ParentNamespaceDurability,
                PublicationOperation::FinalReopenVerify,
                PublicationOperation::PublishedCommitted,
            ]
        );
    }

    #[test]
    fn rename_error_reconciled_final_only_operation_order() {
        let fx = sealed_publish_intent_fixture("inj-reconcile-final", "inj-reconcile-name", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_rename_then_already_exists()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
                PublicationOperation::ParentNamespaceDurability,
                PublicationOperation::FinalReopenVerify,
                PublicationOperation::PublishedCommitted,
            ]
        );
    }

    #[test]
    fn rename_already_exists_reconcile_foreign_final_unrelated() {
        let fx = sealed_publish_intent_fixture("inj-foreign-recon", "inj-foreign-final", &[]);
        std::env::set_var(
            "STREAMSYNC_INJECT_RECONCILE_ROOT",
            fx.tmp.path().to_string_lossy().as_ref(),
        );
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        let err = publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_foreign_final_reconcile()),
                ..Default::default()
            },
        )
        .unwrap_err();
        std::env::remove_var("STREAMSYNC_INJECT_RECONCILE_ROOT");
        assert!(matches!(err, PublicationError::UnrelatedDestination));
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
            ]
        );
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::PublishIntent
        );
        assert!(fx.tmp.path().join("inj-foreign-final").is_dir());
        assert!(!fx.tmp.path().join(&fx.stage).exists());
    }

    #[test]
    fn rename_unknown_neither_operation_order() {
        let fx = sealed_publish_intent_fixture("inj-neither-order", "inj-neither-final", &[]);
        std::env::set_var(
            "STREAMSYNC_INJECT_DROP_STAGE_ROOT",
            fx.tmp.path().to_string_lossy().as_ref(),
        );
        let prepared = prepare_publication(&fx.guard).unwrap();
        let mut recorder = PublicationOperationRecorder::default();
        let err = publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions {
                rename_inject: Some(inject_drop_stage_and_fail()),
                ..Default::default()
            },
        )
        .unwrap_err();
        std::env::remove_var("STREAMSYNC_INJECT_DROP_STAGE_ROOT");
        assert!(matches!(err, PublicationError::RenameIndeterminate { .. }));
        assert_eq!(
            recorder.ops,
            vec![
                PublicationOperation::PublishIntentObserved,
                PublicationOperation::StagingReverified,
            ]
        );
    }

    fn inject_drop_stage_and_fail() -> crate::voice_delivery::publication::publish::RenameInjectFn {
        fn wrap(
            _parent: &DirHandle,
            stage: &str,
            _final: &ValidatedFinalName,
        ) -> Result<(), FsError> {
            if let Ok(root) = std::env::var("STREAMSYNC_INJECT_DROP_STAGE_ROOT") {
                let path = std::path::Path::new(&root).join(stage);
                let _ = std::fs::remove_dir_all(path);
            }
            Err(FsError::Io(std::io::Error::other("injected")))
        }
        wrap
    }

    #[test]
    fn rename_error_neither_indeterminate() {
        let fx = sealed_publish_intent_fixture("inj-neither", "inj-neither-final", &[]);
        std::env::set_var(
            "STREAMSYNC_INJECT_DROP_STAGE_ROOT",
            fx.tmp.path().to_string_lossy().as_ref(),
        );
        let prepared = prepare_publication(&fx.guard).unwrap();
        let err = publish_prepared(
            prepared,
            None,
            PublishOptions {
                rename_inject: Some(inject_drop_stage_and_fail()),
                ..Default::default()
            },
        )
        .unwrap_err();
        std::env::remove_var("STREAMSYNC_INJECT_DROP_STAGE_ROOT");
        assert!(matches!(err, PublicationError::RenameIndeterminate { .. }));
    }
}

#[cfg(test)]
mod publication_child_probe_matrix {
    use super::super::error::PublicationError;
    use super::super::publish::{prepare_publication, publish_prepared, PublishOptions};
    use super::super::recover::{recover_delivery, RecoveryOutcome};
    use super::super::test_support::sealed_publish_intent_fixture;

    #[test]
    fn stage_regular_file_blocks_publish() {
        let fx = sealed_publish_intent_fixture("probe-stage-file", "final-a", &[]);
        std::fs::remove_dir_all(fx.tmp.path().join(&fx.stage)).unwrap();
        std::fs::write(fx.tmp.path().join(&fx.stage), b"not-a-dir").unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }

    #[test]
    fn final_regular_file_blocks_with_stage_intact() {
        let fx = sealed_publish_intent_fixture("probe-final-file", "final-file", &[]);
        std::fs::write(fx.tmp.path().join("final-file"), b"not-a-dir").unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        let err = publish_prepared(prepared, None, PublishOptions::default()).unwrap_err();
        assert!(matches!(
            err,
            PublicationError::OtherArtifact { role: "final" }
        ));
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn stage_symlink_blocks_publish() {
        let fx = sealed_publish_intent_fixture("probe-stage-symlink", "final-sym", &[]);
        let stage_path = fx.tmp.path().join(&fx.stage);
        std::fs::remove_dir_all(&stage_path).unwrap();
        std::os::unix::fs::symlink("final-sym", &stage_path).unwrap();
        let err = recover_delivery(&fx.guard, None).unwrap_err();
        assert!(matches!(err, PublicationError::Fs(_)));
    }

    #[test]
    fn foreign_nonempty_final_dir_quarantines() {
        let fx = sealed_publish_intent_fixture("probe-foreign-dir", "foreign-final", &[]);
        std::fs::create_dir(fx.tmp.path().join("foreign-final")).unwrap();
        std::fs::write(fx.tmp.path().join("foreign-final/note.txt"), b"x").unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }
}

#[cfg(test)]
mod post_seal_corruption {
    use super::super::publish::{prepare_publication, publish_prepared, PublishOptions};
    use super::super::recover::{recover_delivery, RecoveryOutcome};
    use super::super::test_support::sealed_publish_intent_fixture;
    use crate::voice_delivery::fs::file::open_existing_file_at;
    use crate::voice_delivery::marker::MARKER_FILENAME;
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::state::LedgerState;

    fn corrupt_stem_byte(fx: &super::super::test_support::SealedFixture) {
        let staging = fx.guard.staging_dir().unwrap();
        let mut file = open_existing_file_at(staging, "a.wav").unwrap();
        let len = file.len().unwrap();
        file.write_all_at(len - 1, &[0xFF]).unwrap();
    }

    #[test]
    fn corrupt_stem_bytes_blocks_publish_intent_publish() {
        let fx = sealed_publish_intent_fixture("corrupt-bytes", "corrupt-final", &[]);
        corrupt_stem_byte(&fx);
        let prepared = prepare_publication(&fx.guard).unwrap();
        assert!(publish_prepared(prepared, None, PublishOptions::default()).is_err());
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::PublishIntent
        );
    }

    #[test]
    fn corrupt_final_under_published_quarantines() {
        let fx = sealed_publish_intent_fixture("corrupt-published", "pub-corrupt", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        publish_prepared(prepared, None, PublishOptions::default()).unwrap();
        let final_dir = fx.tmp.path().join("pub-corrupt");
        let wav = final_dir.join("a.wav");
        let mut bytes = std::fs::read(&wav).unwrap();
        bytes[40] ^= 0x01;
        std::fs::write(&wav, bytes).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
        assert_ne!(
            LedgerStore::open_for_guard(&fx.guard)
                .unwrap()
                .read_highest_valid()
                .unwrap()
                .unwrap()
                .state,
            LedgerState::Published
        );
    }

    #[test]
    fn truncated_stem_blocks_sealed_recovery() {
        let fx = sealed_publish_intent_fixture("corrupt-trunc", "trunc-final", &[]);
        let store = LedgerStore::open_for_guard(&fx.guard).unwrap();
        let intent = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(intent.state, LedgerState::PublishIntent);
        let gen_dir = fx
            .tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(fx.guard.identity().opaque_delivery_id.clone());
        std::fs::remove_file(gen_dir.join(format!("gen-{}.json", intent.generation))).unwrap();
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().state,
            LedgerState::Sealed
        );
        let wav = fx.tmp.path().join(&fx.stage).join("a.wav");
        let bytes = std::fs::read(&wav).unwrap();
        std::fs::write(&wav, &bytes[..bytes.len() - 4]).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Quarantined(_)));
    }

    #[test]
    fn fake_marker_bytes_without_valid_digest_blocks() {
        let fx = sealed_publish_intent_fixture("corrupt-marker", "marker-final", &[]);
        std::fs::write(
            fx.tmp.path().join(&fx.stage).join(MARKER_FILENAME),
            br#"{"schema_version":1}"#,
        )
        .unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        assert!(publish_prepared(prepared, None, PublishOptions::default()).is_err());
    }
}

#[cfg(test)]
mod publication_reparse_collision {
    use super::super::publish::{prepare_publication, publish_prepared, PublishOptions};
    use super::super::test_support::sealed_publish_intent_fixture;

    #[cfg(unix)]
    #[test]
    fn unix_final_symlink_collision_fail_closed() {
        let fx = sealed_publish_intent_fixture("unix-final-symlink", "real-final", &[]);
        let real = fx.tmp.path().join("real-final");
        std::fs::create_dir(&real).unwrap();
        std::os::unix::fs::symlink("real-final", fx.tmp.path().join("symlink-final")).unwrap();
        let prepared = prepare_publication(&fx.guard).unwrap();
        assert!(publish_prepared(prepared, None, PublishOptions::default()).is_err());
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
    }

    #[cfg(windows)]
    #[test]
    fn windows_publication_collision_requires_junction_or_fails_gate() {
        use super::super::recover::recover_delivery;
        let fx = sealed_publish_intent_fixture("win-junction", "junction-final", &[]);
        let target = fx.tmp.path().join("junction-target");
        std::fs::create_dir(&target).unwrap();
        let junction = fx.tmp.path().join("junction-final");
        let output = std::process::Command::new("cmd")
            .args([
                "/C",
                "mklink",
                "/J",
                &junction.to_string_lossy(),
                &target.to_string_lossy(),
            ])
            .output()
            .expect("mklink");
        if !output.status.success() {
            panic!(
                "WINDOWS_JUNCTION_ACCEPTANCE_REQUIRED: mklink /J failed (stdout={:?} stderr={:?})",
                output.stdout, output.stderr
            );
        }
        let prepared = prepare_publication(&fx.guard).unwrap();
        assert!(publish_prepared(prepared, None, PublishOptions::default()).is_err());
        assert!(fx.tmp.path().join(&fx.stage).is_dir());
        let _ = recover_delivery(&fx.guard, None);
    }
}

#[cfg(test)]
mod crash_child_process_publication {
    use super::super::publish::PublicationCrashPoint;
    use super::super::publish::{prepare_publication, publish_prepared, PublishOptions};
    use super::super::recover::recover_delivery;
    use super::super::test_support::sealed_publish_intent_fixture;
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::session::DeliverySessionGuard;
    use crate::voice_delivery::state::LedgerState;
    use crate::voice_delivery::subprocess_env::{
        PUB_CRASH_ABORT_MARKER, PUB_CRASH_CHILD, PUB_CRASH_GO, PUB_CRASH_OUTCOME, PUB_CRASH_POINT,
        PUB_CRASH_READY, PUB_CRASH_TMP,
    };
    use std::fs;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    fn wait_for_file(path: &std::path::Path, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if path.is_file() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("timeout waiting for {}", path.display());
    }

    fn crash_point_name(point: PublicationCrashPoint) -> &'static str {
        match point {
            PublicationCrashPoint::AfterPublishIntentBeforeRename => "before_rename",
            PublicationCrashPoint::AfterRenameBeforeParentSync => "after_rename",
            PublicationCrashPoint::AfterParentSyncBeforeFinalVerify => "after_sync",
            PublicationCrashPoint::AfterFinalVerifyBeforePublished => "after_verify",
            PublicationCrashPoint::DuringPublishedLedgerWrite => "during_published_write",
        }
    }

    fn child_main() {
        if std::env::var(PUB_CRASH_CHILD).ok().as_deref() != Some("1") {
            return;
        }
        let tmp = std::env::var(PUB_CRASH_TMP).expect("tmp");
        let ready = std::env::var(PUB_CRASH_READY).expect("ready");
        let go = std::env::var(PUB_CRASH_GO).expect("go");
        let point = std::env::var(PUB_CRASH_POINT).expect("point");
        let delivery = std::env::var("STREAMSYNC_PUB_DELIVERY_ID").expect("id");
        let final_name = std::env::var("STREAMSYNC_PUB_FINAL_NAME").expect("final");
        let stage = std::env::var("STREAMSYNC_PUB_STAGE").expect("stage");
        let manifest_digest = std::env::var("STREAMSYNC_PUB_MANIFEST_DIGEST").expect("digest");
        let root = crate::voice_delivery::fs::DestRoot::open(std::path::Path::new(&tmp)).unwrap();
        let staging = root.open_child_dir(&stage).unwrap();
        let bytes = staging.read_file_all("a.wav").unwrap();
        let byte_count = bytes.len() as u64;
        let sha = crate::voice_delivery::hash::sha256_hex_reader(
            std::io::Cursor::new(&bytes),
            crate::voice_delivery::hash::DEFAULT_STREAM_CHUNK,
        )
        .unwrap();
        let manifest = crate::voice_delivery::manifest::ValidatedManifest::validate(vec![
            crate::voice_delivery::manifest::StemManifestEntry {
                file_name: "a.wav".into(),
                byte_count,
                sha256: sha,
            },
        ])
        .unwrap();
        assert_eq!(manifest.digest(), manifest_digest);
        let guard = DeliverySessionGuard::begin(
            root,
            &delivery,
            manifest,
            &stage,
            vec![],
            &final_name,
            true,
        )
        .unwrap();
        fs::write(&ready, b"ready\n").unwrap();
        wait_for_file(std::path::Path::new(&go), Duration::from_secs(30));
        let crash = match point.as_str() {
            "before_rename" => PublicationCrashPoint::AfterPublishIntentBeforeRename,
            "after_rename" => PublicationCrashPoint::AfterRenameBeforeParentSync,
            "after_sync" => PublicationCrashPoint::AfterParentSyncBeforeFinalVerify,
            "after_verify" => PublicationCrashPoint::AfterFinalVerifyBeforePublished,
            "during_published_write" => PublicationCrashPoint::DuringPublishedLedgerWrite,
            other => panic!("unknown crash point {other}"),
        };
        let prepared = prepare_publication(&guard).unwrap();
        let _ = publish_prepared(
            prepared,
            None,
            PublishOptions {
                crash: Some(crash),
                ..Default::default()
            },
        );
        panic!("child should have aborted");
    }

    #[test]
    fn crash_child_process_publication_matrix() {
        child_main();
        let exe_src = std::env::current_exe().expect("exe");
        let stable_dir = tempfile::tempdir().expect("stable dir");
        let stable_exe = stable_dir.path().join("pub-crash-test-bin");
        fs::copy(&exe_src, &stable_exe).expect("copy exe");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&stable_exe, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let test_name =
            "voice_delivery::publication::tests::crash_child_process_publication::crash_child_process_publication_matrix";
        let points = [
            PublicationCrashPoint::AfterPublishIntentBeforeRename,
            PublicationCrashPoint::AfterRenameBeforeParentSync,
            PublicationCrashPoint::AfterParentSyncBeforeFinalVerify,
            PublicationCrashPoint::AfterFinalVerifyBeforePublished,
            PublicationCrashPoint::DuringPublishedLedgerWrite,
        ];
        for point in points {
            for repeat in 0..2 {
                let delivery_id = format!("crash-{}-{}-{}", crash_point_name(point), repeat, "x");
                let final_name = format!("final-{}-{}", crash_point_name(point), repeat);
                let fx = sealed_publish_intent_fixture(&delivery_id, &final_name, &[]);
                let tmp_path = fx.tmp.path().to_path_buf();
                let stage = fx.stage.clone();
                let manifest = fx.guard.manifest().clone();
                let manifest_digest = fx.guard.manifest().digest();
                let _keep_tmp = fx.tmp;
                drop(fx.guard);
                let ready = tmp_path.join("ready.signal");
                let go = tmp_path.join("go.signal");
                let abort_marker = tmp_path.join("abort.marker");
                let _ = fs::remove_file(&ready);
                let _ = fs::remove_file(&go);
                let _ = fs::remove_file(&abort_marker);
                let child = Command::new(&stable_exe)
                    .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
                    .env(PUB_CRASH_CHILD, "1")
                    .env(PUB_CRASH_TMP, &tmp_path)
                    .env(PUB_CRASH_READY, &ready)
                    .env(PUB_CRASH_GO, &go)
                    .env(PUB_CRASH_ABORT_MARKER, &abort_marker)
                    .env(PUB_CRASH_POINT, crash_point_name(point))
                    .env("STREAMSYNC_PUB_DELIVERY_ID", &delivery_id)
                    .env("STREAMSYNC_PUB_FINAL_NAME", &final_name)
                    .env("STREAMSYNC_PUB_STAGE", &stage)
                    .env("STREAMSYNC_PUB_MANIFEST_DIGEST", manifest_digest)
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("spawn");
                wait_for_file(&ready, Duration::from_secs(30));
                fs::write(&go, b"go\n").unwrap();
                let output = child.wait_with_output().expect("wait");
                assert!(!output.status.success(), "child should abort");
                assert!(
                    abort_marker.is_file(),
                    "pre-abort marker missing for {point:?}"
                );
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(
                    !stderr.contains("panicked at"),
                    "child exited via panic, not abort: {stderr}"
                );
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(
                        output.status.signal(),
                        Some(6),
                        "expected SIGABRT, stderr={stderr}"
                    );
                }
                #[cfg(windows)]
                {
                    assert_ne!(
                        output.status.code(),
                        Some(101),
                        "unexpected rust panic exit code"
                    );
                }
                let root = crate::voice_delivery::fs::DestRoot::open(&tmp_path).unwrap();
                let guard = DeliverySessionGuard::begin_for_recovery(
                    root,
                    &delivery_id,
                    manifest.clone(),
                    &stage,
                    vec![],
                    &final_name,
                    true,
                )
                .unwrap();
                let store = LedgerStore::open_for_guard(&guard).unwrap();
                let highest = store.read_highest_valid().unwrap().unwrap();
                let expect_final = tmp_path.join(&final_name).is_dir();
                match point {
                    PublicationCrashPoint::AfterPublishIntentBeforeRename => {
                        assert!(tmp_path.join(&stage).is_dir());
                        assert_eq!(highest.state, LedgerState::PublishIntent);
                    }
                    PublicationCrashPoint::AfterRenameBeforeParentSync
                    | PublicationCrashPoint::AfterParentSyncBeforeFinalVerify
                    | PublicationCrashPoint::AfterFinalVerifyBeforePublished
                    | PublicationCrashPoint::DuringPublishedLedgerWrite => {
                        assert!(expect_final);
                    }
                }
                if point == PublicationCrashPoint::DuringPublishedLedgerWrite {
                    let gen_dir = tmp_path
                        .join(".streamsync-control/ledgers")
                        .join(guard.identity().opaque_delivery_id.clone());
                    let mut malformed_gen = None;
                    let intent_gen = highest.generation;
                    for entry in fs::read_dir(&gen_dir).unwrap() {
                        let path = entry.unwrap().path();
                        let name = path.file_name().unwrap().to_string_lossy();
                        if !name.starts_with("gen-") {
                            continue;
                        }
                        let gen = name
                            .trim_start_matches("gen-")
                            .trim_end_matches(".json")
                            .parse::<u64>()
                            .unwrap();
                        let data = fs::read_to_string(&path).unwrap();
                        if data.contains("incomplete") && gen > intent_gen {
                            malformed_gen = Some(gen);
                        }
                    }
                    assert!(
                        malformed_gen.is_some(),
                        "malformed higher generation expected before recovery"
                    );
                }
                let recovery = recover_delivery(&guard, None).unwrap();
                let after = store.read_highest_valid().unwrap().unwrap();
                assert_eq!(
                    after.state,
                    LedgerState::Published,
                    "point={point:?} recovery={recovery:?} stage_exists={} final_exists={}",
                    tmp_path.join(&stage).exists(),
                    expect_final
                );
                if point == PublicationCrashPoint::DuringPublishedLedgerWrite {
                    let gen_dir = tmp_path
                        .join(".streamsync-control/ledgers")
                        .join(guard.identity().opaque_delivery_id.clone());
                    let mut saw_malformed = false;
                    for entry in fs::read_dir(&gen_dir).unwrap() {
                        let path = entry.unwrap().path();
                        let name = path.file_name().unwrap().to_string_lossy();
                        if !name.starts_with("gen-") {
                            continue;
                        }
                        let data = fs::read_to_string(&path).unwrap();
                        if data.contains("incomplete") {
                            saw_malformed = true;
                            let malformed_gen = name
                                .trim_start_matches("gen-")
                                .trim_end_matches(".json")
                                .parse::<u64>()
                                .unwrap();
                            assert!(after.generation > malformed_gen);
                        }
                    }
                    assert!(saw_malformed);
                }
                let outcome_path = tmp_path.join(PUB_CRASH_OUTCOME);
                fs::write(&outcome_path, b"ok").unwrap();
            }
        }
    }
}
