//! Client/contract tests using a mock daemon, never a deployment or Discord.
use serde_json::{Value, json};
use src::{local_control::DaemonControlEnvelope, update_rollback::UpdateControlRequest};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;

const CAPABILITY: [u8; 32] = [41; 32];
const ID: &str = "0123456789abcdef0123456789abcdef";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "update-control-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        // Read-only artifact validation, with no candidate ever executed.
        let executable = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&executable).unwrap();
        let hash = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .unwrap()
            .common
            .hash_provider
            .hash(&bytes);
        let digest: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        let artifact = json!({"path":executable, "sha256":digest});
        let manifest = json!({"bot_id":"bot-one", "bot_name":"first", "total_shards":2,
            "shards":[{"shard_id":0,"core":{"current":1,"candidate":2},"gateway":{"current":4,"candidate":5}},
                      {"shard_id":1,"core":{"current":1,"candidate":2},"gateway":{"current":4,"candidate":5}}],
            "artifacts":{"core":artifact.clone(),"gateway":artifact},
            "identify":{"limits":{"max_concurrency":2,"remaining":2,"total":10,"reset_after":60000},"observed_age_ms":0}});
        std::fs::write(
            path.join("plan.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        Self(path)
    }
    fn cli(&self, command: &str, args: &[&str], bootstrap: Value) -> Value {
        self.run(command, args, format!("{bootstrap}\n").as_bytes())
    }
    fn run(&self, command: &str, args: &[&str], input: &[u8]) -> Value {
        let mut child = Command::new(env!("CARGO_BIN_EXE_botctl"))
            .args([command, "first", "--plan"])
            .arg(self.0.join("plan.json"))
            .arg("--json")
            .args(args)
            .env("DISCORD_TOKEN", "CLIENT_ENV_SENTINEL_DO_NOT_ECHO")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut out = child.stdout.take().unwrap();
        let mut err = child.stderr.take().unwrap();
        let stdout = thread::spawn(move || {
            let mut v = Vec::new();
            out.read_to_end(&mut v).unwrap();
            v
        });
        let stderr = thread::spawn(move || {
            let mut v = Vec::new();
            err.read_to_end(&mut v).unwrap();
            v
        });
        child.stdin.take().unwrap().write_all(input).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("CLI deadline exceeded");
            }
            thread::sleep(Duration::from_millis(5));
        }
        let output = Output {
            status: child.wait().unwrap(),
            stdout: stdout.join().unwrap(),
            stderr: stderr.join().unwrap(),
        };
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("CLIENT_ENV_SENTINEL_DO_NOT_ECHO"));
        assert!(!text.contains(&"29".repeat(32)));
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(output.status.success(), value["ok"] == true);
        assert!(value.get("secret").is_none());
        value
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let target = std::fs::canonicalize(&self.0).unwrap();
        let root = std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))
            .unwrap();
        assert_eq!(target.parent(), Some(root.as_path()));
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("update-control-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }
}

fn listener() -> TcpListener {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    listener
}
fn accept(listener: &TcpListener) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                return stream;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5))
            }
            Err(e) => panic!("mock accept: {e}"),
        }
    }
}
fn read(stream: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    BufReader::new(stream)
        .take(4097)
        .read_until(b'\n', &mut bytes)
        .unwrap();
    assert!(bytes.len() <= 4096 && bytes.last() == Some(&b'\n'));
    serde_json::from_slice(&bytes).unwrap()
}
fn reply(stream: &mut TcpStream, value: &Value) {
    writeln!(stream, "{value}").unwrap();
}
fn bootstrap(listener: &TcpListener) -> Value {
    json!({"port":listener.local_addr().unwrap().port(),"secret":CAPABILITY})
}
fn accept_update(listener: &TcpListener) -> TcpStream {
    let mut stream = accept(listener);
    let inspect = read(&mut stream);
    assert_eq!(inspect["command"], "inspect");
    assert_eq!(inspect["bot"], "first");
    assert_eq!(inspect["secret"], json!(CAPABILITY));
    reply(
        &mut stream,
        &json!({"ok":true,"offline":false,"bot":{"bot_id":"bot-one","total_shards":2}}),
    );
    drop(stream);
    accept(listener)
}
fn operation(command: &str, state: &str, id: &str, component: &str) -> Value {
    let generations: Vec<_> = (0..2)
        .map(|shard| match component {
            "core" => json!({"shard_id":shard,"core":2}),
            "gateway" => json!({"shard_id":shard,"gateway":5}),
            _ => json!({"shard_id":shard,"core":2,"gateway":5}),
        })
        .collect();
    json!({"ok":state != "failed", "offline":false, "operation":{
        "operation_id":id,"command":command,"bot_id":"bot-one","state":state,
        "elapsed_ms":12,"shards":[{"key":{"bot_id":"bot-one","shard_id":0}},{"key":{"bot_id":"bot-one","shard_id":1}}],
        "failed_shards":[],"final_generations":generations}})
}
const APPLY: &[&str] = &["--yes", "--bootstrap-stdin", "--operation-id", ID];

#[test]
fn update_and_rollback_use_authenticated_contract_and_poll_without_replay() {
    let fixture = Fixture::new();
    for command in ["update", "rollback"] {
        for component in ["core", "gateway", "all"] {
            let listener = listener();
            let input = bootstrap(&listener);
            let server = thread::spawn(move || {
                let mut stream = accept_update(&listener);
                let request = read(&mut stream);
                let wire: DaemonControlEnvelope<UpdateControlRequest> =
                    serde_json::from_value(request.clone()).unwrap();
                assert!(bool::from(wire.secret.ct_eq(&CAPABILITY)));
                wire.request.validate().unwrap();
                assert_eq!(wire.request.command_name(), command);
                assert_eq!(wire.request.bot, "bot-one");
                assert_eq!(wire.request.operation_id, ID);
                assert!(
                    wire.request
                        .manifest
                        .artifacts
                        .core
                        .as_ref()
                        .unwrap()
                        .path
                        .is_absolute()
                );
                assert!(request.get("role").is_none());
                reply(&mut stream, &operation(command, "running", ID, component));
                drop(stream);
                let mut stream = accept(&listener);
                let query = read(&mut stream);
                assert_eq!(query["command"], "operation");
                assert_eq!(query["operation_id"], ID);
                assert_eq!(query["secret"], json!(CAPABILITY));
                assert_eq!(query["yes"], false);
                assert!(query.get("manifest").is_none());
                assert!(query.get("bot").is_none());
                reply(&mut stream, &operation(command, "succeeded", ID, component));
            });
            let mut args = APPLY.to_vec();
            args.extend(["--component", component]);
            let value = fixture.cli(command, &args, input);
            server.join().unwrap();
            assert_eq!(value["ok"], true);
            assert_eq!(value["offline"], false);
            assert_eq!(value["operation_id"], ID);
        }
    }
}

#[test]
fn confirmation_missing_or_invalid_bootstrap_never_connects() {
    let fixture = Fixture::new();
    let listener = listener();
    for command in ["update", "rollback"] {
        let dry_run = fixture.cli(command, &[], bootstrap(&listener));
        assert_eq!(dry_run["offline"], true);
        assert_eq!(dry_run["applied"], false);
        for args in [
            &["--yes"][..],
            &["--bootstrap-stdin"][..],
            &["--yes", "--dry-run", "--bootstrap-stdin"][..],
        ] {
            assert_eq!(
                fixture.cli(command, args, bootstrap(&listener))["ok"],
                false
            );
        }
        for input in [
            b"{}\n".as_slice(),
            b"not json\n",
            b"{\"port\":0,\"secret\":[0]}\n",
        ] {
            assert_eq!(fixture.run(command, APPLY, input)["ok"], false);
        }
        assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    }
}

#[test]
fn rejected_authentication_and_unsupported_daemon_are_fail_closed() {
    let fixture = Fixture::new();
    for reject_auth in [false, true] {
        let listener = listener();
        let mut input = bootstrap(&listener);
        if reject_auth {
            input["secret"] = json!(vec![42; 32]);
        }
        let server = thread::spawn(move || {
            if reject_auth {
                let mut stream = accept(&listener);
                let request = read(&mut stream);
                assert_ne!(request["secret"], json!(CAPABILITY));
                return;
            }
            let mut stream = accept_update(&listener);
            let request = read(&mut stream);
            let wire: DaemonControlEnvelope<UpdateControlRequest> =
                serde_json::from_value(request).unwrap();
            if !bool::from(wire.secret.ct_eq(&CAPABILITY)) {
                return;
            }
            reply(
                &mut stream,
                &json!({"ok":false,"offline":false,"error":"unsupported command"}),
            );
        });
        let value = fixture.cli("update", APPLY, input);
        server.join().unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["operation_id"], ID);
    }
}

#[test]
fn malformed_success_failed_shards_and_wrong_generations_cannot_succeed() {
    let fixture = Fixture::new();
    for variant in 0..9 {
        let listener = listener();
        let input = bootstrap(&listener);
        let server = thread::spawn(move || {
            let mut stream = accept_update(&listener);
            read(&mut stream);
            let mut response = operation("rollback", "succeeded", ID, "core");
            match variant {
                0 => {
                    response["operation"]
                        .as_object_mut()
                        .unwrap()
                        .remove("final_generations");
                }
                1 => response["operation"]["final_generations"][0]["core"] = json!(99),
                2 => response["operation"]["bot_id"] = json!("another-bot"),
                3 => response["operation"]["operation_id"] = json!("f".repeat(32)),
                4 => response["operation"]["failed_shards"] = json!([{"shard_id":1}]),
                5 => response["operation"]["final_generations"] = json!([{ "shard_id":0,"core":2}]),
                6 => response["operation"]["shards"] = json!([]),
                7 => response["operation"]["state"] = json!("failed"),
                _ => response["operation"]["command"] = json!("update"),
            }
            reply(&mut stream, &response);
        });
        assert_eq!(fixture.cli("rollback", APPLY, input)["ok"], false);
        server.join().unwrap();
    }
}

#[test]
fn lost_reply_is_not_retried_and_credentials_are_not_echoed() {
    let fixture = Fixture::new();
    for echo in [false, true] {
        let listener = listener();
        let input = bootstrap(&listener);
        let server = thread::spawn(move || {
            let mut stream = accept_update(&listener);
            read(&mut stream);
            if echo {
                reply(&mut stream, &json!({"ok":false,"error":CAPABILITY}));
            }
            drop(stream);
            thread::sleep(Duration::from_millis(200));
            assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
        });
        let value = fixture.cli("update", APPLY, input);
        server.join().unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["operation_id"], ID);
    }
}

#[test]
fn oversize_authenticated_envelope_is_rejected_before_connecting() {
    let fixture = Fixture::new();
    let file = fixture.0.join("plan.json");
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    manifest["total_shards"] = json!(64);
    manifest["shards"] = json!(
        (0..64)
            .map(|shard| json!({"shard_id":shard,"core":{"current":1,"candidate":2}}))
            .collect::<Vec<_>>()
    );
    std::fs::write(&file, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let listener = listener();
    let value = fixture.cli("update", APPLY, bootstrap(&listener));
    assert_eq!(value["ok"], false);
    assert_eq!(value["error"], "control request exceeds limit");
    assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
}

#[test]
fn selector_ambiguity_or_changed_live_target_prevents_mutation() {
    let fixture = Fixture::new();
    for variant in 0..3 {
        let listener = listener();
        let input = bootstrap(&listener);
        let server = thread::spawn(move || {
            let mut stream = accept(&listener);
            assert_eq!(read(&mut stream)["command"], "inspect");
            let response = match variant {
                0 => json!({"ok":false,"offline":false,"error":"ambiguous Bot selector"}),
                1 => {
                    json!({"ok":true,"offline":false,"bot":{"bot_id":"changed-bot","total_shards":2}})
                }
                _ => json!({"ok":true,"offline":false,"bot":{"bot_id":"bot-one","total_shards":3}}),
            };
            reply(&mut stream, &response);
            drop(stream);
            thread::sleep(Duration::from_millis(200));
            assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
        });
        assert_eq!(fixture.cli("update", APPLY, input)["ok"], false);
        server.join().unwrap();
    }
}

#[test]
fn mock_daemon_binds_server_roles_and_audits_update_and_rollback() {
    use src::rbac_audit::{AuditConfig, AuditedExecutor, Principal};
    let fixture = Fixture::new();
    for command in ["update", "rollback"] {
        for role in ["viewer", "updater", "admin", "unknown", "anonymous"] {
            let listener = listener();
            let input = bootstrap(&listener);
            let log = fixture.0.join(format!("{command}-{role}.jsonl"));
            let log_for_server = log.clone();
            let server = thread::spawn(move || {
                let mut stream = accept_update(&listener);
                let wire: DaemonControlEnvelope<UpdateControlRequest> =
                    serde_json::from_value(read(&mut stream)).unwrap();
                let principal =
                    if !bool::from(wire.secret.ct_eq(&CAPABILITY)) || role == "anonymous" {
                        Principal::unauthenticated()
                    } else {
                        // Server configuration supplies the role, never the JSON body.
                        Principal::authenticated("mock-operator", role)
                    };
                let mut audit =
                    AuditedExecutor::open(log_for_server, AuditConfig::default()).unwrap();
                let mut calls = 0;
                let result = audit.execute_update(&wire.request, principal, false, || {
                    calls += 1;
                    Ok(operation(command, "succeeded", ID, "core"))
                });
                let response =
                    result.unwrap_or_else(|_| json!({"ok":false,"offline":false,"error":"denied"}));
                reply(&mut stream, &response);
                calls
            });
            let value = fixture.cli(command, APPLY, input);
            let calls = server.join().unwrap();
            let allowed = matches!(role, "updater" | "admin");
            assert_eq!(value["ok"], allowed);
            assert_eq!(calls, usize::from(allowed));
            let text = std::fs::read_to_string(log).unwrap();
            assert!(!text.contains("secret"));
            assert!(!text.contains("manifest"));
            let records: Vec<Value> = text
                .lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
            assert_eq!(records.len(), if allowed { 2 } else { 1 });
            assert_eq!(
                records[0]["result"],
                if allowed { "started" } else { "denied" }
            );
            for record in records {
                assert_eq!(record["operation_id"], ID);
            }
        }
    }
}

#[test]
fn operation_requery_validates_affected_shards_without_original_manifest() {
    for valid in [true, false] {
        let listener = listener();
        let input = bootstrap(&listener);
        let server = thread::spawn(move || {
            let mut stream = accept(&listener);
            let request = read(&mut stream);
            assert_eq!(request["command"], "operation");
            assert_eq!(request["secret"], json!(CAPABILITY));
            let mut response = operation("update", "succeeded", ID, "core");
            if !valid {
                response["operation"]["shards"] = json!([]);
            }
            reply(&mut stream, &response);
        });
        let mut child = Command::new(env!("CARGO_BIN_EXE_botctl"))
            .args([
                "operation",
                ID,
                "--bootstrap-stdin",
                "--timeout",
                "2s",
                "--json",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        writeln!(child.stdin.take().unwrap(), "{input}").unwrap();
        let output = child.wait_with_output().unwrap();
        server.join().unwrap();
        assert_eq!(output.status.success(), valid);
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["ok"], valid);
    }
}
