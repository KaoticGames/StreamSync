#[cfg(test)]
mod recover_rename_outcome_matrix {
    use super::super::error::PublicationError;
    use super::super::publish::{prepare_publication, publish_prepared, PublishOptions};
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
        let published = publish_prepared(prepared, None, PublishOptions { crash: None }).unwrap();
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
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Published(_)));
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
        publish_prepared(prepared, None, PublishOptions { crash: None }).unwrap();
        let outcome = recover_delivery(&fx.guard, None).unwrap();
        assert!(matches!(outcome, RecoveryOutcome::IdempotentPublished));
    }

    #[test]
    fn published_with_staging_present_quarantines() {
        let fx = sealed_publish_intent_fixture("pub-stg-after", "after-pub", &[]);
        let prepared = prepare_publication(&fx.guard).unwrap();
        publish_prepared(prepared, None, PublishOptions { crash: None }).unwrap();
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
        let err = publish_prepared(prepared, None, PublishOptions { crash: None }).unwrap_err();
        assert!(matches!(err, PublicationError::UnrelatedDestination));
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
        publish_prepared(
            prepared,
            Some(&mut recorder),
            PublishOptions { crash: None },
        )
        .unwrap();
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
        PUB_CRASH_CHILD, PUB_CRASH_GO, PUB_CRASH_OUTCOME, PUB_CRASH_POINT, PUB_CRASH_READY,
        PUB_CRASH_TMP,
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
        let _ = publish_prepared(prepared, None, PublishOptions { crash: Some(crash) });
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
                let _ = fs::remove_file(&ready);
                let _ = fs::remove_file(&go);
                let mut child = Command::new(&stable_exe)
                    .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
                    .env(PUB_CRASH_CHILD, "1")
                    .env(PUB_CRASH_TMP, &tmp_path)
                    .env(PUB_CRASH_READY, &ready)
                    .env(PUB_CRASH_GO, &go)
                    .env(PUB_CRASH_POINT, crash_point_name(point))
                    .env("STREAMSYNC_PUB_DELIVERY_ID", &delivery_id)
                    .env("STREAMSYNC_PUB_FINAL_NAME", &final_name)
                    .env("STREAMSYNC_PUB_STAGE", &stage)
                    .env("STREAMSYNC_PUB_MANIFEST_DIGEST", manifest_digest)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("spawn");
                wait_for_file(&ready, Duration::from_secs(30));
                fs::write(&go, b"go\n").unwrap();
                let status = child.wait().expect("wait");
                assert!(!status.success(), "child should abort");
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
                let recovery = recover_delivery(&guard, None).unwrap();
                let after = store.read_highest_valid().unwrap().unwrap();
                assert_eq!(
                    after.state,
                    LedgerState::Published,
                    "point={point:?} recovery={recovery:?} stage_exists={} final_exists={}",
                    tmp_path.join(&stage).exists(),
                    expect_final
                );
                let outcome_path = tmp_path.join(PUB_CRASH_OUTCOME);
                fs::write(&outcome_path, b"ok").unwrap();
            }
        }
    }
}
