//! Real Core subprocess checks for the Gateway's result-delivery and stop contract.
use serde_json::{Value, json};
use src::worker::{ApiOutcome, DeploymentIdentity, Manager};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(20);

fn binary() -> PathBuf {
    env!("CARGO_BIN_EXE_src").into()
}

fn deployment() -> DeploymentIdentity {
    DeploymentIdentity::parse(|name| {
        Ok(match name {
            "BOT_ID" => Some("101".into()),
            "SHARD_ID" => Some("1".into()),
            "TOTAL_SHARDS" => Some("2".into()),
            "GENERATION_ID" => Some("700".into()),
            _ => None,
        })
    })
    .unwrap()
}

async fn isolated(count: usize) -> std::sync::Arc<Manager> {
    Manager::start_pool_isolated(binary(), count, deployment())
        .await
        .unwrap()
}

#[tokio::test]
async fn real_worker_receives_hybrid_results_on_the_issuing_pipe() {
    timeout(LIMIT, async {
        let manager = isolated(2).await;
        let workers = manager.worker_identities().await;
        assert_eq!(workers.len(), 2);
        assert_ne!(workers[0].pid, workers[1].pid);
        let mut operation_ids = std::collections::HashSet::new();
        for worker in &workers {
            let identity = worker.identity.as_ref().expect("bidirectional Hello");
            assert_eq!(identity.bot_id.0, 101);
            assert_eq!(identity.shard.id(), 1);
            assert_eq!(identity.shard.total(), 2);
            assert_eq!(identity.generation_id.0, 700 + worker.index as u64);
            assert!(worker.credential_free);
            for slash in [false, true] {
                let key = worker.index as u64;
                let mut response = manager.begin_response_delivery(key).await.unwrap();
                let content = if slash {
                    manager
                        .dispatch_slash_with_response(&mut response, key, "ping".into(), vec![])
                        .await
                } else {
                    manager
                        .dispatch_key_with_response(&mut response, key, "!ping".into())
                        .await
                }
                .unwrap();
                assert_eq!(content.as_deref(), Some("Pong! 🦀"));
                let receipt = manager
                    .finish_response(response, ApiOutcome::Confirmed { status: 200 })
                    .await
                    .unwrap();
                assert_eq!(receipt.worker_index, worker.index);
                assert_eq!(receipt.pid, worker.pid);
                assert_eq!(&receipt.identity, identity);
                assert_eq!(receipt.outcome, ApiOutcome::Confirmed { status: 200 });
                assert!(operation_ids.insert(receipt.operation_id));
            }
        }
        assert!(manager.unknown_responses().1.is_empty());
        let pids = manager.shutdown().await.unwrap();
        assert_eq!(pids, workers.iter().map(|w| w.pid).collect::<Vec<_>>());
        assert!(manager.dispatch("!ping".into()).await.is_err());
        assert!(manager.begin_response(0).await.is_err());
        assert!(manager.begin_response_delivery(0).await.is_err());
        assert!(manager.replace(binary(), true).await.is_err());
        // Already-reaped children make shutdown idempotent; no new process starts.
        assert_eq!(manager.shutdown().await.unwrap(), pids);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rest_result_after_core_switch_reaches_old_worker_before_drain_finishes() {
    timeout(LIMIT, async {
        let manager = isolated(1).await;
        let before = manager.worker_identities().await.remove(0);
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        assert!(
            manager
                .dispatch_key_with_response(&mut response, 0, "!ping".into())
                .await
                .unwrap()
                .is_some()
        );
        let update = tokio::spawn({
            let manager = manager.clone();
            async move { manager.replace(binary(), true).await }
        });
        let next = loop {
            let current = manager.worker_identities().await.remove(0);
            if current.pid != before.pid {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(!update.is_finished());
        let mut new_response = manager.begin_response_delivery(0).await.unwrap();
        manager
            .dispatch_slash_with_response(&mut new_response, 0, "ping".into(), vec![])
            .await
            .unwrap();
        let new_receipt = manager
            .finish_response(new_response, ApiOutcome::Confirmed { status: 201 })
            .await
            .unwrap();
        assert_eq!(new_receipt.pid, next.pid);
        let old_receipt = manager
            .finish_response(response, ApiOutcome::Confirmed { status: 200 })
            .await
            .unwrap();
        assert_eq!(old_receipt.pid, before.pid);
        assert_eq!(Some(old_receipt.identity), before.identity);
        assert_ne!(old_receipt.operation_id, new_receipt.operation_id);
        update.await.unwrap().unwrap();
        assert_eq!(manager.shutdown().await.unwrap(), vec![next.pid]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_ack_of_unknown_http_outcome_does_not_claim_graceful_success() {
    timeout(LIMIT, async {
        let manager = isolated(1).await;
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        manager
            .dispatch_key_with_response(&mut response, 0, "!ping".into())
            .await
            .unwrap();
        let receipt = manager
            .finish_response(response, ApiOutcome::Unknown)
            .await
            .unwrap();
        assert_eq!(receipt.outcome, ApiOutcome::Unknown);
        let (instance, unknown) = manager.unknown_responses();
        assert_eq!(unknown, vec![(0, receipt.operation_id)]);
        assert!(manager.shutdown().await.is_err());
        assert_eq!(manager.unknown_responses().1, unknown);
        // Reconciliation is deliberately explicit and uses no application data.
        manager
            .reconcile_response(
                &instance,
                0,
                receipt.operation_id,
                "test fixture reconciliation",
            )
            .unwrap();
        assert_eq!(manager.shutdown().await.unwrap(), vec![receipt.pid]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_the_result_receipt_and_stops_background_admission() {
    timeout(LIMIT, async {
        let manager = isolated(1).await;
        manager.start_jobs();
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        manager
            .dispatch_key_with_response(&mut response, 0, "!ping".into())
            .await
            .unwrap();
        let stopping = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown().await }
        });
        loop {
            match manager.begin_response(0).await {
                Ok(response) => response.confirm(),
                Err(_) => break,
            }
            tokio::task::yield_now().await;
        }
        assert!(!stopping.is_finished());
        let receipt = manager
            .finish_response(response, ApiOutcome::Confirmed { status: 200 })
            .await
            .unwrap();
        assert_eq!(stopping.await.unwrap().unwrap(), vec![receipt.pid]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn no_reply_or_handler_error_does_not_leave_a_worker_result_pending() {
    timeout(LIMIT, async {
        let manager = isolated(1).await;
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        assert!(
            manager
                .dispatch_key_with_response(&mut response, 0, "not-a-command".into())
                .await
                .unwrap()
                .is_none()
        );
        response.confirm();
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        assert!(
            manager
                .dispatch_slash_with_response(&mut response, 0, "greet".into(), vec![])
                .await
                .is_err()
        );
        response.confirm();
        manager.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

// Run the assertion in a child test process to avoid mutating global environment
// while other Tokio tests are spawning children. Values are synthetic, not logged.
#[test]
fn isolated_worker_environment_excludes_all_gateway_credentials() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "isolated_environment_probe", "--nocapture"])
        .env("E2E_SHARD_ISOLATION_PROBE", "1")
        .env("TOKEN_ALPHA", "synthetic-alpha-only")
        .env("TOKEN_BETA", "synthetic-beta-only")
        .env("DISCORD_TOKEN", "synthetic-discord-only")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated Worker environment probe failed"
    );
    for value in [
        b"synthetic-alpha-only".as_slice(),
        b"synthetic-beta-only",
        b"synthetic-discord-only",
    ] {
        assert!(!output.stdout.windows(value.len()).any(|v| v == value));
        assert!(!output.stderr.windows(value.len()).any(|v| v == value));
    }
}

#[test]
fn isolated_environment_probe() {
    if std::env::var_os("E2E_SHARD_ISOLATION_PROBE").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        timeout(LIMIT, async {
            let inherited = Manager::start_pool_with_identity(binary(), 1, deployment())
                .await
                .unwrap();
            assert!(!inherited.worker_identities().await[0].credential_free);
            inherited.shutdown().await.unwrap();
            let manager = isolated(2).await;
            assert!(
                manager
                    .worker_identities()
                    .await
                    .iter()
                    .all(|worker| worker.credential_free)
            );
            manager.replace(binary(), true).await.unwrap();
            assert!(
                manager
                    .worker_identities()
                    .await
                    .iter()
                    .all(|worker| worker.credential_free)
            );
            manager.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    });
}

struct WireWorker {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    identity: Value,
}

impl WireWorker {
    fn new() -> Self {
        let identity = json!({"bot_id":101,"shard":{"id":1,"total":2},"generation_id":700,"ownership_epoch":0});
        let mut child = Command::new(binary())
            .arg("--worker")
            .env_clear()
            .env("BOT_WORKER_IPC_IDENTITY", identity.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
            identity,
        }
    }

    async fn request(&mut self, request: Value) -> Value {
        let mut frame = serde_json::to_vec(&request).unwrap();
        frame.push(b'\n');
        self.input
            .as_mut()
            .unwrap()
            .write_all(&frame)
            .await
            .unwrap();
        let mut line = String::new();
        assert_ne!(self.output.read_line(&mut line).await.unwrap(), 0);
        serde_json::from_str(&line).unwrap()
    }

    async fn hello(&mut self) {
        let ready = self.request(json!({"type":"health"})).await;
        assert_eq!(ready["credential_free"], true);
        assert!(
            ready["supported_capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!("api-result-v1"))
        );
        let response = self
            .request(json!({"type":"hello","hello":{
                "protocol_min":5,"protocol_max":5,"identity":self.identity,
                "required_capabilities":["snapshot-v3"],"optional_capabilities":["api-result-v1"]
            }}))
            .await;
        assert_eq!(response["type"], "hello");
        assert_eq!(response["hello"]["identity"], self.identity);
    }

    async fn eof(mut self) -> std::process::ExitStatus {
        self.input.take();
        self.child.wait().await.unwrap()
    }
}

#[tokio::test]
async fn raw_worker_rejects_unmatched_invalid_and_duplicate_rest_results() {
    timeout(LIMIT, async {
        let mut worker = WireWorker::new();
        worker.hello().await;
        let reply = worker
            .request(json!({"type":"response_dispatch","operation_id":9,
            "request":{"type":"dispatch","key":0,"content":"!ping"}}))
            .await;
        assert_eq!(reply["content"], "Pong! 🦀");
        for (operation_id, status) in [(10, 200), (9, 199)] {
            let result = worker
                .request(json!({"type":"api_result","operation_id":operation_id,
                "outcome":{"kind":"confirmed","status":status}}))
                .await;
            assert_eq!(result["type"], "rejected");
        }
        let result = worker
            .request(json!({"type":"api_result","operation_id":9,
            "outcome":{"kind":"confirmed","status":200}}))
            .await;
        assert_eq!(result["type"], "api_result_accepted");
        assert_eq!(result["identity"], worker.identity);
        assert_eq!(result["pid"], worker.child.id().unwrap());
        let duplicate = worker
            .request(json!({"type":"api_result","operation_id":9,
            "outcome":{"kind":"confirmed","status":200}}))
            .await;
        assert_eq!(duplicate["type"], "rejected");
        assert!(worker.eof().await.success());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn eof_with_unconfirmed_result_exits_but_does_not_claim_success() {
    timeout(LIMIT, async {
        let mut worker = WireWorker::new();
        worker.hello().await;
        worker
            .request(json!({"type":"response_dispatch","operation_id":9,
            "request":{"type":"command","name":"ping"}}))
            .await;
        assert!(!worker.eof().await.success());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn legacy_worker_without_advertised_api_result_capability_is_refused() {
    let root = std::env::temp_dir().join(format!(
        "e2e-shard-legacy-{}-{}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let fixture = root.join(format!("legacy{}", std::env::consts::EXE_SUFFIX));
    assert!(
        std::process::Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/handshake_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    timeout(LIMIT, async {
        let manager = Manager::start_pool_isolated(fixture, 1, deployment())
            .await
            .unwrap();
        let metadata = manager.worker_identities().await;
        assert!(metadata[0].identity.is_none());
        assert!(!metadata[0].credential_free);
        assert!(manager.begin_response_delivery(0).await.is_err());
        manager.begin_response(0).await.unwrap().confirm();
        manager.shutdown().await.unwrap();
    })
    .await
    .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn gateway_policy_rejects_legacy_start_and_update_before_old_admission_changes() {
    let root = std::env::temp_dir().join(format!(
        "e2e-shard-required-cap-{}-{}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let fixture = root.join(format!("legacy{}", std::env::consts::EXE_SUFFIX));
    assert!(
        std::process::Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/handshake_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    timeout(LIMIT, async {
        assert!(
            Manager::start_gateway_pool(fixture.clone(), 1, deployment())
                .await
                .is_err()
        );
        let manager = Manager::start_gateway_pool(binary(), 1, deployment())
            .await
            .unwrap();
        let before = manager.worker_identities().await.remove(0);
        assert!(manager.replace(fixture.clone(), true).await.is_err());
        let after = manager.worker_identities().await.remove(0);
        assert_eq!(after.pid, before.pid);
        assert_eq!(after.identity, before.identity);
        assert!(after.credential_free);
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        assert_eq!(
            manager
                .dispatch_slash_with_response(&mut response, 0, "ping".into(), vec![])
                .await
                .unwrap()
                .as_deref(),
            Some("Pong! 🦀")
        );
        let receipt = manager
            .finish_response(response, ApiOutcome::Confirmed { status: 200 })
            .await
            .unwrap();
        assert_eq!(receipt.pid, before.pid);
        assert_eq!(Some(receipt.identity), before.identity);
        assert!(manager.unknown_responses().1.is_empty());
        manager.shutdown().await.unwrap();
    })
    .await
    .unwrap();
    timeout(LIMIT, async {
        loop {
            match std::fs::remove_dir_all(&root) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("fixture cleanup failed: {:?}", error.kind()),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_or_missing_result_receipts_remain_unknown_and_never_retarget() {
    let root = std::env::temp_dir().join(format!(
        "e2e-shard-receipt-{}-{}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let fixture = root.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    assert!(
        std::process::Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/e2e_shard_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    for mode in [
        "bad-operation",
        "bad-identity",
        "bad-result-pid",
        "bad-outcome",
        "missing-ack",
        "unadvertised",
        "bad-ready-pid",
    ] {
        let path = root.join(format!("{mode}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(&fixture, &path).unwrap();
        timeout(LIMIT, async {
            let started = Manager::start_pool_isolated(path, 1, deployment()).await;
            if mode == "bad-ready-pid" {
                assert!(started.is_err());
                return;
            }
            let manager = started.unwrap();
            let reserved = manager.begin_response_delivery(0).await;
            if mode == "unadvertised" {
                assert!(reserved.is_err());
                manager.shutdown().await.unwrap();
                return;
            }
            let mut response = reserved.unwrap();
            manager
                .dispatch_key_with_response(&mut response, 0, "!ping".into())
                .await
                .unwrap();
            let before = manager.worker_identities().await;
            assert!(
                manager
                    .finish_response(response, ApiOutcome::Confirmed { status: 200 })
                    .await
                    .is_err()
            );
            assert_eq!(manager.worker_identities().await[0].pid, before[0].pid);
            assert_eq!(manager.unknown_responses().1.len(), 1);
            // Even a poisoned result pipe must be closed and its actual child
            // reaped; an unknown receipt keeps the overall operation unsuccessful.
            assert!(manager.shutdown().await.is_err());
        })
        .await
        .unwrap();
    }
    // A failed startup may have sent kill_on_drop without receiving exit yet.
    // Retry only the test-owned temporary directory, never any deployment path.
    timeout(LIMIT, async {
        loop {
            match std::fs::remove_dir_all(&root) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("fixture cleanup failed: {:?}", error.kind()),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn result_delivery_requires_negotiation_and_bounds_unconfirmed_operations() {
    timeout(LIMIT, async {
        let mut worker = WireWorker::new();
        let response = worker
            .request(json!({"type":"api_result","operation_id":1,
            "outcome":{"kind":"confirmed","status":200}}))
            .await;
        assert_eq!(response["type"], "rejected");
        assert!(worker.eof().await.success());

        let mut worker = WireWorker::new();
        worker.hello().await;
        for operation_id in 0..1024 {
            let response = worker
                .request(
                    json!({"type":"response_dispatch","operation_id":operation_id,
                "request":{"type":"command","name":"ping"}}),
                )
                .await;
            assert_eq!(response["content"], "Pong! 🦀");
        }
        let mut overflow =
            serde_json::to_vec(&json!({"type":"response_dispatch","operation_id":1024,
            "request":{"type":"command","name":"ping"}}))
            .unwrap();
        overflow.push(b'\n');
        worker
            .input
            .as_mut()
            .unwrap()
            .write_all(&overflow)
            .await
            .unwrap();
        let mut line = String::new();
        assert_eq!(worker.output.read_line(&mut line).await.unwrap(), 0);
        assert!(!worker.eof().await.success());
    })
    .await
    .unwrap();
}
