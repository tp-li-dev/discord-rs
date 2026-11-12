use src::{
    drain::DrainTracker,
    switch::{Phase, Switch, SwitchError},
    update_rollback::{self, Action, Component, Recovery, Settlement, UpdateControlRequest},
};
use std::time::Duration;

fn manifest() -> update_rollback::Manifest {
    serde_json::from_value(serde_json::json!({
        "bot_id": "bot-one", "bot_name": "first", "total_shards": 4,
        "shards": [
            {"shard_id": 3, "core": {"current": 10,"candidate": 11}, "gateway": {"current": 20,"candidate": 22}},
            {"shard_id": 1, "core": {"current": 4,"candidate": 6}, "gateway": {"current": 7,"candidate": 9}}
        ],
        "artifacts": {"core": {"path": "core", "sha256": "a".repeat(64)}, "gateway": {"path": "gateway", "sha256": "b".repeat(64)}},
        "identify": {"limits": {"max_concurrency": 2,"remaining": 2,"total": 10,"reset_after": 1000}, "observed_age_ms": 500}
    })).unwrap()
}

#[test]
fn serial_stages_are_sorted_and_do_not_confuse_component_generations() {
    let plan = update_rollback::build_plan(
        &manifest(),
        "bot-o",
        Action::Update,
        Component::All,
        Duration::ZERO,
    )
    .unwrap();
    assert!(plan.executable());
    assert_eq!(plan.additional_identifies, 2);
    let stages = plan
        .stages
        .iter()
        .map(|s| {
            (
                s.shard_id,
                s.component,
                s.from_generation,
                s.to_generation,
                s.identify_bucket,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        stages,
        vec![
            (1, Component::Core, 4, 6, None),
            (1, Component::Gateway, 7, 9, Some(1)),
            (3, Component::Core, 10, 11, None),
            (3, Component::Gateway, 20, 22, Some(1)),
        ]
    );
    for stage in plan.stages {
        assert!(
            stage
                .steps
                .contains(&update_rollback::Step::WaitTasksRestStateAndIpc)
        );
        assert!(
            stage
                .on_failure_after_commit
                .contains(&update_rollback::Step::RestoreRetainedOldIfSafe)
        );
    }
}

#[test]
fn budget_is_for_the_whole_plan_and_accounts_for_validation_time() {
    let mut manifest = manifest();
    let expired = update_rollback::build_plan(
        &manifest,
        "first",
        Action::Update,
        Component::Gateway,
        Duration::from_millis(500),
    )
    .unwrap();
    assert_eq!(expired.gate.status, "defer");
    manifest.identify.as_mut().unwrap().limits.remaining = 1;
    let deferred = update_rollback::build_plan(
        &manifest,
        "first",
        Action::Update,
        Component::Gateway,
        Duration::ZERO,
    )
    .unwrap();
    assert!(!deferred.executable());
    assert_eq!(deferred.gate.status, "defer");
    manifest.identify = None;
    let core = update_rollback::build_plan(
        &manifest,
        "first",
        Action::Rollback,
        Component::Core,
        Duration::MAX,
    )
    .unwrap();
    assert!(core.executable());
    assert_eq!(core.additional_identifies, 0);
    assert!(
        update_rollback::build_plan(
            &manifest,
            "first",
            Action::Update,
            Component::Gateway,
            Duration::ZERO
        )
        .is_err()
    );
    // Even nonsensical metadata and a maximum elapsed clock cannot block Core.
    let core_with_metadata = update_rollback::build_plan(
        &self::manifest(),
        "first",
        Action::Update,
        Component::Core,
        Duration::MAX,
    )
    .unwrap();
    assert!(core_with_metadata.executable());
}

#[test]
fn malformed_binary_and_nested_unknown_metadata_are_rejected() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("update-invalid-native-{}", std::process::id()));
    std::fs::write(&path, b"abc").unwrap();
    let artifact = update_rollback::Artifact {
        path: path.clone(),
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
    };
    assert!(update_rollback::verify_artifact(&artifact, std::path::Path::new(".")).is_err());
    std::fs::remove_file(&path).unwrap();
    let mut value = serde_json::to_value(manifest()).unwrap();
    value["identify"]["limits"]["unexpected"] = serde_json::json!(1);
    assert!(serde_json::from_value::<update_rollback::Manifest>(value).is_err());
}

#[test]
fn failure_before_commit_keeps_old_and_generation_cannot_be_reused() {
    let mut switch = Switch::new(5);
    switch.prepare(6).unwrap();
    assert_eq!(
        update_rollback::plan_failure(&mut switch, true),
        Ok(Recovery::KeepOld)
    );
    assert_eq!(switch.active(), 5);
    assert_eq!(switch.phase(), Phase::Active);
    assert_eq!(switch.prepare(6), Err(SwitchError::DuplicateGeneration));
}

#[test]
fn failure_after_commit_requires_safe_retained_previous_process() {
    let mut switch = Switch::new(5);
    switch.prepare(6).unwrap();
    switch.commit().unwrap();
    assert_eq!(
        update_rollback::plan_failure(&mut switch, false),
        Ok(Recovery::HoldForReconciliation)
    );
    assert_eq!(switch.active(), 6);
    assert_eq!(
        update_rollback::plan_failure(&mut switch, true),
        Ok(Recovery::RestoredOld { active: 5 })
    );
    assert_eq!(switch.phase(), Phase::RollingBack { abandoned: 6 });
    assert_eq!(switch.finish(false), Err(SwitchError::Unsettled));
    switch.finish(true).unwrap();
    assert_eq!(
        update_rollback::plan_failure(&mut switch, true),
        Ok(Recovery::HoldForReconciliation)
    );
}

fn settled() -> Settlement {
    Settlement {
        rest_confirmed: true,
        state_saved: true,
        ipc_flushed: true,
    }
}

#[tokio::test]
async fn retirement_waits_for_children_and_checks_other_channels_after_tasks() {
    let mut switch = Switch::new(1);
    switch.prepare(2).unwrap();
    switch.commit().unwrap();
    let tasks = DrainTracker::default();
    let parent = tasks.try_start().unwrap();
    tasks.start_drain();
    let child = parent.child();
    drop(parent);
    assert_eq!(
        update_rollback::finish_when_settled(
            &mut switch,
            &tasks,
            || panic!("tasks are not drained"),
            Duration::from_millis(10)
        )
        .await,
        Err(SwitchError::Unsettled)
    );
    assert_eq!(switch.phase(), Phase::Draining { previous: 1 });
    drop(child);
    for bad in [
        Settlement {
            rest_confirmed: false,
            ..settled()
        },
        Settlement {
            state_saved: false,
            ..settled()
        },
        Settlement {
            ipc_flushed: false,
            ..settled()
        },
    ] {
        assert_eq!(
            update_rollback::finish_when_settled(
                &mut switch,
                &tasks,
                || bad,
                Duration::from_secs(1)
            )
            .await,
            Err(SwitchError::Unsettled)
        );
        assert_eq!(switch.phase(), Phase::Draining { previous: 1 });
    }
    assert_eq!(
        update_rollback::finish_when_settled(&mut switch, &tasks, settled, Duration::from_secs(1))
            .await,
        Ok(1)
    );
}

#[tokio::test]
async fn rollback_also_keeps_abandoned_process_until_results_are_settled() {
    let mut switch = Switch::new(1);
    switch.prepare(2).unwrap();
    switch.commit().unwrap();
    update_rollback::plan_failure(&mut switch, true).unwrap();
    let tasks = DrainTracker::default();
    tasks.start_drain();
    assert_eq!(
        update_rollback::finish_when_settled(
            &mut switch,
            &tasks,
            || Settlement {
                rest_confirmed: false,
                ..settled()
            },
            Duration::from_secs(1)
        )
        .await,
        Err(SwitchError::Unsettled)
    );
    assert_eq!(switch.phase(), Phase::RollingBack { abandoned: 2 });
    assert_eq!(
        update_rollback::finish_when_settled(&mut switch, &tasks, settled, Duration::from_secs(1))
            .await,
        Ok(2)
    );
}

#[test]
fn proposed_control_contract_requires_yes_canonical_bot_and_operation_id() {
    let mut request = UpdateControlRequest {
        command: Action::Update,
        bot: "bot-one".into(),
        operation_id: "0123456789abcdef0123456789abcdef".into(),
        yes: false,
        component: Component::Core,
        manifest: manifest(),
    };
    assert!(request.validate().is_err());
    request.yes = true;
    request.validate().unwrap();
    let value = serde_json::to_value(&request).unwrap();
    assert!(value.get("secret").is_none());
    let mut extra = value.clone();
    extra["secret"] = serde_json::json!([0]);
    assert!(serde_json::from_value::<UpdateControlRequest>(extra).is_err());
    request.bot = "first".into();
    assert!(request.validate().is_err());
    request.bot = "bot-one".into();
    request.operation_id = "BAD-ID".into();
    assert!(request.validate().is_err());
}

#[cfg(unix)]
#[test]
fn unix_rejects_nonexecutables_and_invalid_native_format_without_spawning() {
    use std::os::unix::fs::PermissionsExt;
    let path = std::env::temp_dir().join(format!("update-invalid-{}", std::process::id()));
    std::fs::write(&path, b"abc").unwrap();
    let artifact = update_rollback::Artifact {
        path: path.clone(),
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
    };
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        update_rollback::verify_artifact(&artifact, std::path::Path::new(".")),
        Err("artifact is not executable")
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    #[cfg(target_os = "linux")]
    assert_eq!(
        update_rollback::verify_artifact(&artifact, std::path::Path::new(".")),
        Err("artifact is not a native Linux ELF executable")
    );
    std::fs::remove_file(path).unwrap();
}
