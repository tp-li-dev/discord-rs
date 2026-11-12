use serde_json::{Value, json};
use src::{
    local_control::DaemonControlEnvelope,
    rbac_audit::{AuditConfig, AuditError, AuditedExecutor, Denial, ExecuteError, Principal},
    update_rollback::{Action, UpdateControlRequest},
};
use std::{cell::Cell, fs, path::PathBuf};

struct AuditFixture(PathBuf);

impl AuditFixture {
    fn new() -> Self {
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let name = format!("update-control-rbac-{:032x}", u128::from_ne_bytes(nonce));
        let directory = std::env::temp_dir().join(name);
        fs::create_dir(&directory).unwrap();
        Self(directory)
    }

    fn log(&self) -> PathBuf {
        self.0.join("audit.jsonl")
    }

    fn records(&self) -> Vec<Value> {
        fs::read_to_string(self.log())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for AuditFixture {
    fn drop(&mut self) {
        let target = fs::canonicalize(&self.0).unwrap();
        let temporary_root = fs::canonicalize(std::env::temp_dir()).unwrap();
        assert_eq!(target.parent(), Some(temporary_root.as_path()));
        assert!(
            target
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("update-control-rbac-"))
        );
        fs::remove_dir_all(target).unwrap();
    }
}

fn request(action: Action) -> UpdateControlRequest {
    serde_json::from_value(json!({
        "command": action,
        "bot": "bot-one",
        "operation_id": "0123456789abcdef0123456789abcdef",
        "yes": true,
        "component": "core",
        "manifest": {
            "bot_id": "bot-one",
            "bot_name": "first",
            "total_shards": 1,
            "shards": [{"shard_id": 0, "core": {"current": 1, "candidate": 2}}],
            "artifacts": {"core": {"path": "fixture-core", "sha256": "a".repeat(64)}}
        }
    }))
    .unwrap()
}

#[test]
fn update_and_rollback_enforce_server_principals_and_record_denials() {
    for action in [Action::Update, Action::Rollback] {
        for (principal, expected, subject, role) in [
            (
                Principal::unauthenticated(),
                Denial::Unauthenticated,
                "anonymous",
                None,
            ),
            (
                Principal::authenticated("viewer-one", "viewer"),
                Denial::InsufficientPermission,
                "viewer-one",
                Some("viewer"),
            ),
            (
                Principal::authenticated("operator-one", "root"),
                Denial::UnknownRole,
                "operator-one",
                Some("root"),
            ),
        ] {
            let fixture = AuditFixture::new();
            let mut executor =
                AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
            let update = request(action);
            update.validate().unwrap();
            let called = Cell::new(false);
            assert_eq!(
                executor.execute_update(&update, principal, true, || {
                    called.set(true);
                    Ok(())
                }),
                Err(ExecuteError::Denied(expected))
            );
            assert!(!called.get());
            drop(executor);
            let records = fixture.records();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["result"], "denied");
            assert_eq!(records[0]["operation"], update.command_name());
            assert_eq!(records[0]["operation_id"], update.operation_id);
            assert_eq!(records[0]["subject"], subject);
            assert_eq!(records[0]["role"], json!(role));
            assert_eq!(records[0]["bot_id"], "bot-one");
            assert_eq!(records[0]["remote"], true);
            assert_eq!(
                records[0]["reason"],
                match expected {
                    Denial::Unauthenticated => "unauthenticated",
                    Denial::InsufficientPermission => "insufficient_permission",
                    Denial::UnknownRole => "unknown_role",
                    _ => unreachable!(),
                }
            );
        }
    }
}

#[test]
fn updater_and_admin_run_once_after_audit_and_record_completion() {
    for action in [Action::Update, Action::Rollback] {
        for role in ["updater", "admin"] {
            for remote in [false, true] {
                let fixture = AuditFixture::new();
                let mut executor =
                    AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
                let update = request(action);
                let calls = Cell::new(0);
                assert_eq!(
                    executor.execute_update(
                        &update,
                        Principal::authenticated("operator-one", role),
                        remote,
                        || {
                            calls.set(calls.get() + 1);
                            Ok(42)
                        }
                    ),
                    Ok(42)
                );
                assert_eq!(calls.get(), 1);
                drop(executor);
                let records = fixture.records();
                assert_eq!(records.len(), 2);
                assert_eq!(records[0]["result"], "started");
                assert_eq!(records[1]["result"], "succeeded");
                assert_eq!(records[0]["request_id"], records[1]["request_id"]);
                for record in records {
                    assert_eq!(record["operation_id"], update.operation_id);
                    assert_eq!(record["operation"], update.command_name());
                    assert_eq!(record["role"], role);
                    assert_eq!(record["remote"], remote);
                    assert!(record.get("secret").is_none());
                    assert!(record.get("manifest").is_none());
                }
            }
        }
    }
}

#[test]
fn server_scope_denial_is_audited_and_never_executes_for_updater_or_admin() {
    for action in [Action::Update, Action::Rollback] {
        for role in ["updater", "admin"] {
            for scope_allowed in [false, true] {
                let fixture = AuditFixture::new();
                let mut executor =
                    AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
                let update = request(action);
                update.validate().unwrap();
                let calls = Cell::new(0);
                let outcome = executor.execute_update_scoped(
                    &update,
                    Principal::authenticated("scoped-operator", role),
                    false,
                    scope_allowed,
                    || {
                        calls.set(calls.get() + 1);
                        Ok(42)
                    },
                );
                assert_eq!(calls.get(), usize::from(scope_allowed));
                assert_eq!(
                    outcome,
                    if scope_allowed {
                        Ok(42)
                    } else {
                        Err(ExecuteError::Denied(Denial::InvalidTarget))
                    }
                );
                drop(executor);
                let records = fixture.records();
                assert_eq!(records.len(), if scope_allowed { 2 } else { 1 });
                assert_eq!(
                    records[0]["result"],
                    if scope_allowed { "started" } else { "denied" }
                );
                if !scope_allowed {
                    assert_eq!(records[0]["reason"], "invalid_target");
                }
                for record in records {
                    assert_eq!(record["operation_id"], update.operation_id);
                    assert_eq!(record["operation"], update.command_name());
                    assert_eq!(record["subject"], "scoped-operator");
                    assert_eq!(record["role"], role);
                    assert_eq!(record["bot_id"], "bot-one");
                }
            }
        }
    }
}

#[test]
fn missing_yes_is_denied_and_audited_for_both_actions() {
    for action in [Action::Update, Action::Rollback] {
        let fixture = AuditFixture::new();
        let mut executor = AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
        let mut update = request(action);
        update.yes = false;
        assert!(update.validate().is_err());
        let called = Cell::new(false);
        assert_eq!(
            executor.execute_update(
                &update,
                Principal::authenticated("operator-one", "admin"),
                true,
                || {
                    called.set(true);
                    Ok(())
                }
            ),
            Err(ExecuteError::Denied(Denial::ConfirmationRequired))
        );
        assert!(!called.get());
        drop(executor);
        let records = fixture.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], "denied");
        assert_eq!(records[0]["reason"], "confirmation_required");
    }
}

#[test]
fn execution_failure_is_audited_without_retry() {
    for action in [Action::Update, Action::Rollback] {
        let fixture = AuditFixture::new();
        let mut executor = AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
        let calls = Cell::new(0);
        let result: Result<(), _> = executor.execute_update(
            &request(action),
            Principal::authenticated("operator-one", "updater"),
            true,
            || {
                calls.set(calls.get() + 1);
                Err("fixture failure".into())
            },
        );
        assert_eq!(result, Err(ExecuteError::OperationFailed));
        assert_eq!(calls.get(), 1);
        drop(executor);
        let records = fixture.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["result"], "started");
        assert_eq!(records[1]["result"], "failed");
        assert_eq!(records[0]["request_id"], records[1]["request_id"]);
    }
}

#[test]
fn audit_capacity_failure_prevents_any_update_side_effect() {
    for action in [Action::Update, Action::Rollback] {
        let fixture = AuditFixture::new();
        let mut executor = AuditedExecutor::open(
            fixture.log(),
            AuditConfig {
                max_file_bytes: 1024,
                max_record_bytes: 1024,
                max_archives: 0,
            },
        )
        .unwrap();
        let called = Cell::new(false);
        assert_eq!(
            executor.execute_update(
                &request(action),
                Principal::authenticated("operator-one", "admin"),
                true,
                || {
                    called.set(true);
                    Ok(())
                }
            ),
            Err(ExecuteError::Audit(AuditError::CapacityExhausted))
        );
        assert!(!called.get());
        drop(executor);
        assert!(fixture.records().is_empty());
    }
}

#[test]
fn malformed_manifest_is_denied_before_the_execution_closure() {
    let fixture = AuditFixture::new();
    let mut executor = AuditedExecutor::open(fixture.log(), AuditConfig::default()).unwrap();
    let mut update = request(Action::Update);
    update.manifest.shards[0].core.as_mut().unwrap().candidate = 1;
    let called = Cell::new(false);
    assert_eq!(
        executor.execute_update(
            &update,
            Principal::authenticated("operator-one", "admin"),
            false,
            || {
                called.set(true);
                Ok(())
            }
        ),
        Err(ExecuteError::Denied(Denial::InvalidTarget))
    );
    assert!(!called.get());
    drop(executor);
    assert_eq!(fixture.records()[0]["reason"], "invalid_target");
}

#[test]
fn strict_wire_contract_rejects_injected_identity_unknown_and_duplicate_fields() {
    for action in [Action::Update, Action::Rollback] {
        let mut wire = serde_json::to_value(request(action)).unwrap();
        // A fixed synthetic control capability, never used with a live daemon.
        wire["secret"] = json!(vec![7_u8; 32]);
        let valid = serde_json::to_string(&wire).unwrap();
        let decoded: DaemonControlEnvelope<UpdateControlRequest> =
            serde_json::from_str(&valid).unwrap();
        assert_eq!(decoded.secret, [7; 32]);
        decoded.request.validate().unwrap();

        for (key, value) in [
            ("role", json!("admin")),
            ("subject", json!("operator-one")),
            ("authenticated", json!(true)),
            ("unknown", json!(true)),
        ] {
            let mut injected = wire.clone();
            injected[key] = value;
            assert!(
                serde_json::from_value::<DaemonControlEnvelope<UpdateControlRequest>>(injected)
                    .is_err(),
                "injected field {key} must be rejected"
            );
        }

        let mut nested_unknown = wire.clone();
        nested_unknown["manifest"]["role"] = json!("admin");
        assert!(
            serde_json::from_value::<DaemonControlEnvelope<UpdateControlRequest>>(nested_unknown)
                .is_err()
        );

        for (key, value) in [
            ("secret", json!(vec![7_u8; 32])),
            ("command", json!(action)),
            ("yes", json!(true)),
            ("manifest", wire["manifest"].clone()),
        ] {
            let duplicate = format!("{{\"{key}\":{value},{}", &valid[1..]);
            assert!(
                serde_json::from_str::<DaemonControlEnvelope<UpdateControlRequest>>(&duplicate)
                    .is_err(),
                "duplicate field {key} must be rejected"
            );
        }

        for key in ["secret", "yes", "operation_id", "manifest"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<DaemonControlEnvelope<UpdateControlRequest>>(missing)
                    .is_err(),
                "missing field {key} must be rejected"
            );
        }
    }
}
