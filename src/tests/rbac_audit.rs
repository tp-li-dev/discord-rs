use serde_json::Value;
use src::rbac_audit::{
    AuditConfig, AuditError, AuditedExecutor, Confirmation, Denial, ExecuteError, Operation,
    Principal, Request, Role,
};
use std::{
    cell::Cell,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!(
            "rbac-audit-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&base).unwrap();
        Self(base)
    }

    fn log(&self) -> PathBuf {
        self.0.join("audit.jsonl")
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let target = fs::canonicalize(&self.0).unwrap();
        let temporary_root = fs::canonicalize(std::env::temp_dir()).unwrap();
        assert_eq!(target.parent(), Some(temporary_root.as_path()));
        assert!(
            target
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(
                    |name| name.starts_with(&format!("rbac-audit-test-{}-", std::process::id()))
                )
        );
        fs::remove_dir_all(target).unwrap();
    }
}

fn request(role: &str, operation: &str) -> Request {
    Request {
        principal: Principal::authenticated("operator-one", role),
        operation: operation.into(),
        bot_id: Some("bot-one".into()),
        shard_id: Some(2),
        remote: true,
        confirmation: Confirmation::None,
    }
}

fn anonymous(operation: &str) -> Request {
    Request {
        principal: Principal::unauthenticated(),
        ..request("admin", operation)
    }
}

fn segment(path: &Path, index: u32) -> PathBuf {
    if index == 0 {
        path.to_path_buf()
    } else {
        let mut name = path.as_os_str().to_os_string();
        name.push(format!(".{index}"));
        name.into()
    }
}

fn records(path: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    for index in 0..100 {
        let path = segment(path, index);
        if !path.exists() {
            break;
        }
        let bytes = fs::read(path).unwrap();
        if !bytes.is_empty() {
            assert_eq!(bytes.last(), Some(&b'\n'));
        }
        for line in bytes.split_inclusive(|b| *b == b'\n') {
            out.push(serde_json::from_slice(line).unwrap());
        }
    }
    out
}

fn deny(executor: &mut AuditedExecutor, request: &Request, expected: Denial) {
    let called = Cell::new(false);
    let result = executor.execute(request, || {
        called.set(true);
        Ok(())
    });
    assert_eq!(result, Err(ExecuteError::Denied(expected)));
    assert!(!called.get());
}

#[test]
fn permission_table_covers_every_role_and_operation() {
    let operations = [
        Operation::Status,
        Operation::Inspect,
        Operation::Logs,
        Operation::Update,
        Operation::Rollback,
        Operation::Stop,
        Operation::Force,
        Operation::OwnershipSwitch,
        Operation::Shutdown,
    ];
    let permissions = [
        (
            Role::Viewer,
            [true, true, true, false, false, false, false, false, false],
        ),
        (
            Role::Updater,
            [true, true, true, true, true, false, false, false, false],
        ),
        (Role::Admin, [true; 9]),
    ];
    for (role, expected) in permissions {
        for (operation, allowed) in operations.into_iter().zip(expected) {
            assert_eq!(role.allows(operation), allowed, "{role:?} {operation:?}");
        }
    }
}

#[test]
fn parsing_is_exact_and_does_not_promote_unknown_roles_or_actions() {
    for (name, expected) in [
        ("viewer", Role::Viewer),
        ("updater", Role::Updater),
        ("admin", Role::Admin),
    ] {
        assert_eq!(Role::parse(name), Some(expected));
    }
    for name in ["", "root", "Admin", " admin", "admin\n", "administrator"] {
        assert_eq!(Role::parse(name), None);
    }
    for (name, expected) in [
        ("status", Operation::Status),
        ("inspect", Operation::Inspect),
        ("logs", Operation::Logs),
        ("update", Operation::Update),
        ("rollback", Operation::Rollback),
        ("stop", Operation::Stop),
        ("force", Operation::Force),
        ("ownership-switch", Operation::OwnershipSwitch),
        ("shutdown", Operation::Shutdown),
    ] {
        assert_eq!(Operation::parse(name), Some(expected));
        assert_eq!(
            expected.requires_confirmation(),
            matches!(expected, Operation::Force | Operation::OwnershipSwitch)
        );
    }
    for name in ["", "restart", "STATUS", "force-stop", "status\n", " status"] {
        assert_eq!(Operation::parse(name), None);
    }
}

#[test]
fn t20_every_unauthenticated_remote_operation_is_denied_and_audited() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let operations = [
        "status",
        "inspect",
        "logs",
        "update",
        "rollback",
        "stop",
        "force",
        "ownership-switch",
        "shutdown",
    ];
    for operation in operations {
        deny(
            &mut executor,
            &anonymous(operation),
            Denial::Unauthenticated,
        );
    }
    drop(executor);
    let rows = records(&dir.log());
    assert_eq!(rows.len(), operations.len());
    for (row, operation) in rows.iter().zip(operations) {
        assert_eq!(row["operation"], operation);
        assert_eq!(row["remote"], true);
        assert_eq!(row["result"], "denied");
        assert!(row["reason"].as_str().is_some_and(|s| !s.is_empty()));
        assert!(row["subject"].is_string());
        assert!(row["timestamp_unix_ms"].is_number());
        assert_eq!(row["bot_id"], "bot-one");
        assert_eq!(row["shard_id"], 2);
    }
}

#[test]
fn local_requests_do_not_bypass_authentication_or_rbac() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let mut req = anonymous("status");
    req.remote = false;
    deny(&mut executor, &req, Denial::Unauthenticated);
    req = request("viewer", "update");
    req.remote = false;
    deny(&mut executor, &req, Denial::InsufficientPermission);
}

#[test]
fn unknown_role_unknown_operation_and_insufficient_permission_are_audited() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    deny(
        &mut executor,
        &request("root", "status"),
        Denial::UnknownRole,
    );
    deny(
        &mut executor,
        &request("admin", "erase"),
        Denial::UnknownOperation,
    );
    deny(
        &mut executor,
        &request("viewer", "rollback"),
        Denial::InsufficientPermission,
    );
    deny(
        &mut executor,
        &request("updater", "stop"),
        Denial::InsufficientPermission,
    );
    drop(executor);
    assert!(
        records(&dir.log())
            .iter()
            .all(|row| row["result"] == "denied")
    );
    assert_eq!(records(&dir.log()).len(), 4);
}

#[test]
fn every_permission_is_enforced_by_execution_gate() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    for (role, allowed) in [("viewer", 3), ("updater", 5), ("admin", 9)] {
        for (index, operation) in [
            "status",
            "inspect",
            "logs",
            "update",
            "rollback",
            "stop",
            "force",
            "ownership-switch",
            "shutdown",
        ]
        .into_iter()
        .enumerate()
        {
            let mut req = request(role, operation);
            req.confirmation = Confirmation::NonInteractive {
                explicit: true,
                yes: true,
            };
            let called = Cell::new(false);
            let result = executor.execute(&req, || {
                called.set(true);
                Ok(42)
            });
            if index < allowed {
                assert_eq!(result, Ok(42));
                assert!(called.get());
            } else {
                assert_eq!(
                    result,
                    Err(ExecuteError::Denied(Denial::InsufficientPermission))
                );
                assert!(!called.get());
            }
        }
    }
}

#[test]
fn dangerous_actions_require_explicit_option_and_positive_confirmation() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    for operation in ["force", "ownership-switch"] {
        for confirmation in [
            Confirmation::None,
            Confirmation::Interactive {
                explicit: false,
                accepted: false,
            },
            Confirmation::Interactive {
                explicit: false,
                accepted: true,
            },
            Confirmation::Interactive {
                explicit: true,
                accepted: false,
            },
            Confirmation::NonInteractive {
                explicit: false,
                yes: false,
            },
            Confirmation::NonInteractive {
                explicit: false,
                yes: true,
            },
            Confirmation::NonInteractive {
                explicit: true,
                yes: false,
            },
        ] {
            let mut req = request("admin", operation);
            req.confirmation = confirmation;
            deny(&mut executor, &req, Denial::ConfirmationRequired);
        }
        for confirmation in [
            Confirmation::Interactive {
                explicit: true,
                accepted: true,
            },
            Confirmation::NonInteractive {
                explicit: true,
                yes: true,
            },
        ] {
            let mut req = request("admin", operation);
            req.confirmation = confirmation;
            assert_eq!(executor.execute(&req, || Ok("executed")), Ok("executed"));
        }
    }
}

#[test]
fn confirmation_never_grants_missing_permission() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let mut req = request("updater", "force");
    req.confirmation = Confirmation::NonInteractive {
        explicit: true,
        yes: true,
    };
    deny(&mut executor, &req, Denial::InsufficientPermission);
}

#[test]
fn success_records_start_and_outcome_with_same_request_identity() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    assert_eq!(
        executor.execute(&request("updater", "update"), || Ok(17)),
        Ok(17)
    );
    drop(executor);
    let rows = records(&dir.log());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["result"], "started");
    assert_eq!(rows[1]["result"], "succeeded");
    assert_eq!(rows[0]["request_id"], rows[1]["request_id"]);
    for row in rows {
        assert_eq!(row["subject"], "operator-one");
        assert_eq!(row["role"], "updater");
        assert_eq!(row["operation"], "update");
        assert_eq!(row["bot_id"], "bot-one");
        assert_eq!(row["shard_id"], 2);
    }
}

#[test]
fn failed_operation_is_not_reported_successful_and_reason_is_masked() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let result: Result<(), _> = executor.execute(&request("updater", "update"), || {
        Err("Authorization: Bearer synthetic-failure-only".into())
    });
    assert_eq!(result, Err(ExecuteError::OperationFailed));
    drop(executor);
    let text = fs::read_to_string(dir.log()).unwrap();
    assert!(!text.contains("synthetic-failure-only"));
    let rows = records(&dir.log());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["result"], "failed");
    assert!(rows[1]["reason"].as_str().unwrap().contains("[REDACTED]"));
}

#[test]
fn masks_untrusted_subject_role_operation_bot_and_reason_before_json_encoding() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let req = Request {
        principal: Principal::authenticated(
            "token=synthetic-subject-only",
            "secret=synthetic-role-only",
        ),
        operation: "custom_id=synthetic-operation-only".into(),
        bot_id: Some("interaction_token=synthetic-bot-only".into()),
        ..request("admin", "status")
    };
    let called = Cell::new(false);
    assert!(
        executor
            .execute(&req, || {
                called.set(true);
                Ok(())
            })
            .is_err()
    );
    assert!(!called.get());
    drop(executor);
    let text = fs::read_to_string(dir.log()).unwrap();
    for value in [
        "synthetic-subject-only",
        "synthetic-role-only",
        "synthetic-operation-only",
        "synthetic-bot-only",
    ] {
        assert!(!text.contains(value));
    }
    assert!(text.contains("[REDACTED]"));
    assert_eq!(records(&dir.log()).len(), 1);
}

#[test]
fn newline_and_json_injection_never_create_extra_audit_records() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let injected = "attacker\r\n{\"result\":\"succeeded\"}\n\t\u{2028}🦀";
    let req = Request {
        principal: Principal::authenticated(injected, "admin"),
        operation: injected.into(),
        bot_id: Some(injected.into()),
        ..request("admin", "status")
    };
    let called = Cell::new(false);
    assert!(
        executor
            .execute(&req, || {
                called.set(true);
                Ok(())
            })
            .is_err()
    );
    assert!(!called.get());
    drop(executor);
    let bytes = fs::read(dir.log()).unwrap();
    assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
    let rows = records(&dir.log());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["result"], "denied");
}

#[test]
fn empty_authenticated_subject_is_rejected() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let req = Request {
        principal: Principal::authenticated("", "admin"),
        ..request("admin", "status")
    };
    deny(&mut executor, &req, Denial::InvalidSubject);
}

#[test]
fn shard_without_bot_and_empty_bot_are_rejected() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let mut req = request("admin", "stop");
    req.bot_id = None;
    deny(&mut executor, &req, Denial::InvalidTarget);
    req.bot_id = Some(String::new());
    deny(&mut executor, &req, Denial::InvalidTarget);
}

#[test]
fn panic_is_audited_without_logging_panic_payload_and_is_rethrown() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), _> = executor.execute(&request("admin", "stop"), || {
            std::panic::panic_any(String::from("token=synthetic-panic-only"));
        });
    }));
    assert!(result.is_err());
    drop(executor);
    let text = fs::read_to_string(dir.log()).unwrap();
    assert!(!text.contains("synthetic-panic-only"));
    let rows = records(&dir.log());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["result"], "panicked");
}

#[test]
fn reopening_appends_without_reusing_request_ids() {
    let dir = TestDir::new();
    {
        let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
        deny(&mut executor, &anonymous("status"), Denial::Unauthenticated);
    }
    let before = fs::read(dir.log()).unwrap();
    {
        let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
        deny(&mut executor, &anonymous("logs"), Denial::Unauthenticated);
    }
    assert!(fs::read(dir.log()).unwrap().starts_with(&before));
    let rows = records(&dir.log());
    assert_eq!(rows.len(), 2);
    assert!(rows[1]["request_id"].as_u64().unwrap() > rows[0]["request_id"].as_u64().unwrap());
}

#[test]
fn rotation_keeps_old_segments_unchanged_and_respects_byte_limit() {
    let dir = TestDir::new();
    let config = AuditConfig {
        max_file_bytes: 1024,
        max_record_bytes: 1024,
        max_archives: 32,
    };
    let mut executor = AuditedExecutor::open(dir.log(), config).unwrap();
    let mut saved = Vec::new();
    for _ in 0..20 {
        deny(&mut executor, &anonymous("update"), Denial::Unauthenticated);
    }
    drop(executor);
    assert!(segment(&dir.log(), 1).exists());
    for index in 0..33 {
        let path = segment(&dir.log(), index);
        if !path.exists() {
            break;
        }
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.len() <= 1024);
        saved.push((path, bytes));
    }
    assert_eq!(records(&dir.log()).len(), 20);
    let mut executor = AuditedExecutor::open(
        dir.log(),
        AuditConfig {
            max_file_bytes: 1024,
            max_record_bytes: 1024,
            max_archives: 32,
        },
    )
    .unwrap();
    for _ in 0..5 {
        deny(
            &mut executor,
            &anonymous("shutdown"),
            Denial::Unauthenticated,
        );
    }
    drop(executor);
    for (index, (path, bytes)) in saved.iter().enumerate() {
        let after = fs::read(path).unwrap();
        if index + 1 < saved.len() {
            assert_eq!(&after, bytes);
        } else {
            assert!(after.starts_with(bytes));
        }
    }
    assert_eq!(records(&dir.log()).len(), 25);
}

#[test]
fn archive_capacity_exhaustion_fails_closed_without_deleting_history() {
    let dir = TestDir::new();
    let config = AuditConfig {
        max_file_bytes: 1024,
        max_record_bytes: 1024,
        max_archives: 1,
    };
    let mut executor = AuditedExecutor::open(dir.log(), config).unwrap();
    let mut exhausted = false;
    for _ in 0..100 {
        let result = executor.execute(&anonymous("status"), || -> Result<(), String> {
            panic!("must not execute")
        });
        match result {
            Err(ExecuteError::Denied(Denial::Unauthenticated)) => {}
            Err(ExecuteError::Audit(AuditError::CapacityExhausted)) => {
                exhausted = true;
                break;
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
    assert!(exhausted);
    let called = Cell::new(false);
    assert!(
        executor
            .execute(&request("admin", "stop"), || {
                called.set(true);
                Ok(())
            })
            .is_err()
    );
    assert!(!called.get());
    drop(executor);
    assert!(segment(&dir.log(), 1).exists());
    assert!(!segment(&dir.log(), 2).exists());
    assert!(!records(&dir.log()).is_empty());
    for index in 0..2 {
        assert!(fs::metadata(segment(&dir.log(), index)).unwrap().len() <= 1024);
    }
}

#[test]
fn oversized_audit_record_does_not_execute_operation() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(
        dir.log(),
        AuditConfig {
            max_file_bytes: 4096,
            max_record_bytes: 512,
            max_archives: 2,
        },
    )
    .unwrap();
    let req = Request {
        principal: Principal::authenticated("x".repeat(8192), "admin"),
        ..request("admin", "stop")
    };
    let called = Cell::new(false);
    assert_eq!(
        executor.execute(&req, || {
            called.set(true);
            Ok(())
        }),
        Err(ExecuteError::Audit(AuditError::RecordTooLarge))
    );
    assert!(!called.get());
    drop(executor);
    assert!(records(&dir.log()).is_empty());
}

#[test]
fn json_escaping_expansion_is_counted_in_record_size_limit() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(
        dir.log(),
        AuditConfig {
            max_file_bytes: 4096,
            max_record_bytes: 512,
            max_archives: 2,
        },
    )
    .unwrap();
    let req = Request {
        principal: Principal::authenticated("\"".repeat(512), "admin"),
        ..request("admin", "stop")
    };
    let called = Cell::new(false);
    assert_eq!(
        executor.execute(&req, || {
            called.set(true);
            Ok(())
        }),
        Err(ExecuteError::Audit(AuditError::RecordTooLarge))
    );
    assert!(!called.get());
}

#[test]
fn simultaneous_writer_is_refused_and_lock_is_released_on_drop() {
    let dir = TestDir::new();
    let executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
    assert!(matches!(
        AuditedExecutor::open(dir.log(), AuditConfig::default()),
        Err(AuditError::Busy)
    ));
    drop(executor);
    assert!(AuditedExecutor::open(dir.log(), AuditConfig::default()).is_ok());
}

#[test]
fn invalid_configuration_and_missing_parent_are_rejected() {
    let dir = TestDir::new();
    for config in [
        AuditConfig {
            max_file_bytes: 0,
            max_record_bytes: 512,
            max_archives: 1,
        },
        AuditConfig {
            max_file_bytes: 4096,
            max_record_bytes: 0,
            max_archives: 1,
        },
        AuditConfig {
            max_file_bytes: 512,
            max_record_bytes: 1024,
            max_archives: 1,
        },
    ] {
        assert!(matches!(
            AuditedExecutor::open(dir.log(), config),
            Err(AuditError::InvalidConfig)
        ));
    }
    assert!(matches!(
        AuditedExecutor::open(dir.0.join("missing/audit.jsonl"), AuditConfig::default()),
        Err(AuditError::Io)
    ));
}

#[test]
fn malformed_or_unterminated_existing_log_is_rejected_without_repair() {
    for bytes in [
        b"not json\n".as_slice(),
        b"{\"result\":\"denied\"}".as_slice(),
        b"\n".as_slice(),
    ] {
        let dir = TestDir::new();
        fs::write(dir.log(), bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.log(), fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(matches!(
            AuditedExecutor::open(dir.log(), AuditConfig::default()),
            Err(AuditError::CorruptLog)
        ));
        assert_eq!(fs::read(dir.log()).unwrap(), bytes);
    }
}

#[test]
fn directory_cannot_be_used_as_audit_log() {
    let dir = TestDir::new();
    assert!(AuditedExecutor::open(&dir.0, AuditConfig::default()).is_err());
}

#[test]
fn completion_capacity_is_reserved_before_operation_starts() {
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(
        dir.log(),
        AuditConfig {
            max_file_bytes: 1024,
            max_record_bytes: 1024,
            max_archives: 0,
        },
    )
    .unwrap();
    let called = Cell::new(false);
    assert_eq!(
        executor.execute(&request("admin", "stop"), || {
            called.set(true);
            Ok(())
        }),
        Err(ExecuteError::Audit(AuditError::CapacityExhausted))
    );
    assert!(!called.get());
    drop(executor);
    assert!(fs::read(dir.log()).unwrap().is_empty());
}

#[test]
fn escaped_multibyte_failure_reasons_fit_the_reserved_completion_record() {
    for (record_limit, expected_reason) in [(512, "omitted"), (4096, "truncated")] {
        let dir = TestDir::new();
        let mut executor = AuditedExecutor::open(
            dir.log(),
            AuditConfig {
                max_file_bytes: 4096,
                max_record_bytes: record_limit,
                max_archives: 2,
            },
        )
        .unwrap();
        let reason = "\"\n🦀日本語".repeat(1000);
        let result: Result<(), _> = executor.execute(&request("updater", "update"), || Err(reason));
        assert_eq!(result, Err(ExecuteError::OperationFailed));
        drop(executor);
        let rows = records(&dir.log());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["result"], "failed");
        let reason = rows[1]["reason"].as_str().unwrap();
        if expected_reason == "omitted" {
            assert_eq!(reason, "omitted");
        } else {
            assert!(reason.ends_with("[truncated]"));
            assert!(reason.len() <= 512 + "[truncated]".len());
        }
        for index in 0..3 {
            let path = segment(&dir.log(), index);
            if !path.exists() {
                break;
            }
            let bytes = fs::read(path).unwrap();
            for line in bytes.split_inclusive(|b| *b == b'\n') {
                assert!(line.len() <= record_limit);
            }
        }
    }
}

#[test]
fn rollover_collision_preserves_existing_entry_and_poison_blocks_operations() {
    for collision_is_directory in [false, true] {
        let dir = TestDir::new();
        let mut executor = AuditedExecutor::open(
            dir.log(),
            AuditConfig {
                max_file_bytes: 512,
                max_record_bytes: 512,
                max_archives: 4,
            },
        )
        .unwrap();
        let collision = segment(&dir.log(), 1);
        if collision_is_directory {
            fs::create_dir(&collision).unwrap();
        } else {
            fs::write(&collision, b"existing history must remain unchanged").unwrap();
        }
        deny(&mut executor, &anonymous("status"), Denial::Unauthenticated);
        let denied_bytes = fs::metadata(dir.log()).unwrap().len();
        while 512 - fs::metadata(dir.log()).unwrap().len() >= denied_bytes {
            deny(&mut executor, &anonymous("status"), Denial::Unauthenticated);
        }
        // This authenticated stop's started record has the same encoded length
        // as the denied status above and must roll over before any side effect.
        let called = Cell::new(false);
        assert_eq!(
            executor.execute(&request("admin", "stop"), || {
                called.set(true);
                Ok(())
            }),
            Err(ExecuteError::Audit(AuditError::Io))
        );
        assert!(!called.get());
        let called = Cell::new(false);
        assert_eq!(
            executor.execute(&request("admin", "stop"), || {
                called.set(true);
                Ok(())
            }),
            Err(ExecuteError::Audit(AuditError::Poisoned))
        );
        assert!(!called.get());
        drop(executor);
        assert!(fs::metadata(dir.log()).unwrap().len() <= 512);
        if collision_is_directory {
            assert!(collision.is_dir());
        } else {
            assert_eq!(
                fs::read(collision).unwrap(),
                b"existing history must remain unchanged"
            );
        }
    }
}

#[test]
fn missing_segment_in_existing_sequence_is_rejected_without_creating_gap() {
    let dir = TestDir::new();
    drop(AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap());
    let unexpected = segment(&dir.log(), 2);
    fs::write(&unexpected, b"").unwrap();
    assert!(matches!(
        AuditedExecutor::open(dir.log(), AuditConfig::default()),
        Err(AuditError::CorruptLog)
    ));
    assert!(!segment(&dir.log(), 1).exists());
    assert!(fs::read(unexpected).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn unix_segments_are_created_with_0600_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TestDir::new();
    let mut executor = AuditedExecutor::open(
        dir.log(),
        AuditConfig {
            max_file_bytes: 1024,
            max_record_bytes: 1024,
            max_archives: 16,
        },
    )
    .unwrap();
    for _ in 0..10 {
        deny(&mut executor, &anonymous("status"), Denial::Unauthenticated);
    }
    drop(executor);
    assert!(segment(&dir.log(), 1).exists());
    for index in 0..17 {
        let path = segment(&dir.log(), index);
        if !path.exists() {
            break;
        }
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[cfg(unix)]
#[test]
fn unix_symlink_and_hardlink_log_paths_are_rejected() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = TestDir::new();
    let target = dir.0.join("target");
    fs::write(&target, b"").unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&target, dir.log()).unwrap();
    assert!(AuditedExecutor::open(dir.log(), AuditConfig::default()).is_err());
    fs::remove_file(dir.log()).unwrap();
    fs::hard_link(&target, dir.log()).unwrap();
    assert!(AuditedExecutor::open(dir.log(), AuditConfig::default()).is_err());
}
