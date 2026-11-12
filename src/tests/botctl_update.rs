//! CLI planning and authenticated loopback contract tests. The listener below
//! is a mock daemon; no Gateway, Discord connection, or deployment is exercised.
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    io::{BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const TEST_CAPABILITY: [u8; 32] = [29; 32];
const PRIVATE_SENTINEL: &str = "UPDATE_TEST_PRIVATE_VALUE_DO_NOT_ECHO";

fn executable() -> &'static (PathBuf, String) {
    static EXECUTABLE: OnceLock<(PathBuf, String)> = OnceLock::new();
    EXECUTABLE.get_or_init(|| {
        let path = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let hash = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .unwrap()
            .common
            .hash_provider
            .hash(&bytes);
        let digest = hash
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        (path, digest)
    })
}

fn manifest() -> Value {
    let (program, sha256) = executable();
    let artifact = json!({"path": program, "sha256": sha256});
    json!({
        "bot_id": "bot-one", "bot_name": "first", "total_shards": 2,
        "shards": [
            {"shard_id": 1, "core": {"current": 1, "candidate": 2},
             "gateway": {"current": 4, "candidate": 5}},
            {"shard_id": 0, "core": {"current": 1, "candidate": 2},
             "gateway": {"current": 4, "candidate": 5}}
        ],
        "artifacts": {"core": artifact.clone(), "gateway": artifact},
        "identify": {
            "limits": {"max_concurrency": 2, "remaining": 2,
                       "reset_after": 60000, "total": 10},
            "observed_age_ms": 0
        }
    })
}

struct Fixture {
    root: PathBuf,
    file: PathBuf,
}

impl Fixture {
    fn new(value: &Value) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("botctl-update-tests")
            .join(format!(
                "plan-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("計画.json");
        std::fs::write(&file, serde_json::to_vec(value).unwrap()).unwrap();
        Self { root, file }
    }

    fn run(&self, args: &[&str], input: &[u8]) -> Output {
        let mut arguments: Vec<OsString> = args.iter().map(OsString::from).collect();
        arguments.extend([OsString::from("--plan"), self.file.as_os_str().to_owned()]);
        arguments.push(OsString::from("--json"));
        cli(&arguments, input)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn cli(args: &[OsString], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botctl"))
        .args(args)
        .env("DISCORD_TOKEN", PRIVATE_SENTINEL)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Read while the child runs: an all-component plan can exceed the small
    // anonymous-pipe buffer on Windows and must not block process completion.
    let mut stdout = child.stdout.take().unwrap();
    let stdout = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let mut stderr = child.stderr.take().unwrap();
    let stderr = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    if !input.is_empty() {
        let _ = child.stdin.take().unwrap().write_all(input);
    } else {
        drop(child.stdin.take());
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("update CLI did not terminate within the test deadline");
        }
        thread::sleep(Duration::from_millis(5));
    }
    Output {
        status: child.wait().unwrap(),
        stdout: stdout.join().unwrap(),
        stderr: stderr.join().unwrap(),
    }
}

fn result(output: Output, success: bool) -> Value {
    assert_eq!(
        output.status.success(),
        success,
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(!text.contains(PRIVATE_SENTINEL));
    assert!(!text.contains(&"1d".repeat(32)));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["ok"], success);
    assert!(value.get("secret").is_none());
    value
}

fn bootstrap(port: u16) -> Vec<u8> {
    format!("{}\n", json!({"port": port, "secret": TEST_CAPABILITY})).into_bytes()
}

#[test]
fn default_update_is_offline_core_dry_run_without_identify_metadata() {
    let mut input = manifest();
    input.as_object_mut().unwrap().remove("identify");
    let fixture = Fixture::new(&input);
    let value = result(fixture.run(&["update", "first"], b""), true);
    assert_eq!(value["offline"], true);
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["applied"], false);
    let plan = &value["plan"];
    assert_eq!(plan["command"], "update");
    assert_eq!(plan["component"], "core");
    assert_eq!(plan["bot_id"], "bot-one");
    assert_eq!(plan["additional_identifies"], 0);
    assert_eq!(plan["gate"]["status"], "execute");
    let stages = plan["stages"].as_array().unwrap();
    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0]["shard_id"], 0);
    assert_eq!(stages[1]["shard_id"], 1);
    for stage in stages {
        assert_eq!(stage["component"], "core");
        assert_eq!(stage["from_generation"], 1);
        assert_eq!(stage["to_generation"], 2);
    }
}

#[test]
fn gateway_plan_counts_every_shard_and_respects_the_identify_gate() {
    for (remaining, age, expected, success) in [
        (2, 0, "execute", true),
        (1, 0, "defer", false),
        (2, 60000, "defer", false),
    ] {
        let mut input = manifest();
        input["identify"]["limits"]["remaining"] = json!(remaining);
        input["identify"]["observed_age_ms"] = json!(age);
        let fixture = Fixture::new(&input);
        let value = result(
            fixture.run(
                &["update", "bot-o", "--component", "gateway", "--rolling"],
                b"",
            ),
            success,
        );
        assert_eq!(value["applied"], false);
        assert_eq!(value["plan"]["additional_identifies"], 2);
        assert_eq!(value["plan"]["gate"]["status"], expected);
        assert!(value["plan"]["gate"]["diagnostic"].is_string());
    }
    let mut input = manifest();
    input["identify"]["limits"]["total"] = json!(1);
    input["identify"]["limits"]["remaining"] = json!(1);
    let fixture = Fixture::new(&input);
    let value = result(
        fixture.run(&["update", "first", "--component", "gateway"], b""),
        false,
    );
    assert_eq!(value["plan"]["gate"]["status"], "reject");
}

#[test]
fn all_component_plan_contains_separate_core_and_gateway_stages() {
    let fixture = Fixture::new(&manifest());
    let value = result(
        fixture.run(&["update", "first", "--component", "all", "--dry-run"], b""),
        true,
    );
    assert_eq!(value["plan"]["component"], "all");
    assert_eq!(value["plan"]["additional_identifies"], 2);
    let stages = value["plan"]["stages"].as_array().unwrap();
    assert_eq!(stages.len(), 4);
    for shard in [0, 1] {
        for component in ["core", "gateway"] {
            assert_eq!(
                stages
                    .iter()
                    .filter(|stage| {
                        stage["shard_id"] == shard && stage["component"] == component
                    })
                    .count(),
                1
            );
        }
    }
}

#[test]
fn manual_rollback_is_a_dry_run_with_new_launch_generations() {
    let fixture = Fixture::new(&manifest());
    let value = result(fixture.run(&["rollback", "bot-one"], b""), true);
    assert_eq!(value["plan"]["command"], "rollback");
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["applied"], false);
    for stage in value["plan"]["stages"].as_array().unwrap() {
        assert!(
            stage["to_generation"].as_u64().unwrap() > stage["from_generation"].as_u64().unwrap()
        );
    }
}

#[test]
fn invalid_confirmation_and_connection_options_fail_before_authentication() {
    let fixture = Fixture::new(&manifest());
    for args in [
        vec!["update", "first", "--yes"],
        vec!["rollback", "first", "--yes"],
        vec!["update", "first", "--yes", "--dry-run", "--bootstrap-stdin"],
        vec!["update", "first", "--bootstrap-stdin"],
        vec!["update", "first", "--dry-run", "--bootstrap-stdin"],
        vec!["update", "first", "--force", "--yes", "--bootstrap-stdin"],
        vec!["update", "first", "--component", "unknown"],
        vec!["update", "first", "--dry-run", "--dry-run"],
        vec!["update", "first", "--host", "198.51.100.1"],
        vec![
            "update",
            "first",
            "--yes",
            "--bootstrap-stdin",
            "--secret",
            PRIVATE_SENTINEL,
        ],
    ] {
        result(fixture.run(&args, b""), false);
    }
}

#[test]
fn malformed_plans_hash_mismatches_missing_artifacts_and_wrong_bots_are_rejected() {
    let original = manifest();
    let mut variants = Vec::new();
    let mut input = original.clone();
    input["artifacts"]["core"]["sha256"] = json!("00".repeat(32));
    variants.push(input);
    let mut input = original.clone();
    input["artifacts"]["core"]["path"] = json!("missing-candidate.exe");
    variants.push(input);
    let mut input = original.clone();
    input["shards"][0]["core"]["candidate"] = json!(1);
    variants.push(input);
    let mut input = original.clone();
    input["shards"][0]["shard_id"] = json!(2);
    variants.push(input);
    let mut input = original.clone();
    input["shards"][0]["shard_id"] = json!(0);
    variants.push(input);
    let mut input = original.clone();
    input["secret"] = json!(PRIVATE_SENTINEL);
    variants.push(input);
    for input in variants {
        let fixture = Fixture::new(&input);
        result(fixture.run(&["update", "first"], b""), false);
    }
    let fixture = Fixture::new(&original);
    result(fixture.run(&["update", "other-bot"], b""), false);
}

#[test]
fn apply_authenticates_inspection_and_sends_canonical_request_once() {
    let fixture = Fixture::new(&manifest());
    for command in ["update", "rollback"] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let accept = || {
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
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            thread::sleep(Duration::from_millis(5))
                        }
                        Err(error) => panic!("mock daemon accept failed: {error}"),
                    }
                }
            };
            let read = |stream: &mut TcpStream| {
                let mut frame = Vec::new();
                BufReader::new(stream)
                    .take(4097)
                    .read_until(b'\n', &mut frame)
                    .unwrap();
                assert!(frame.len() <= 4096 && frame.last() == Some(&b'\n'));
                serde_json::from_slice::<Value>(&frame).unwrap()
            };
            let mut stream = accept();
            let inspection = read(&mut stream);
            assert_eq!(inspection["command"], "inspect");
            assert_eq!(inspection["bot"], "first");
            assert_eq!(inspection["secret"], json!(TEST_CAPABILITY));
            assert!(inspection.get("manifest").is_none());
            writeln!(
                stream,
                "{}",
                json!({"ok":true,"offline":false,
                "bot":{"bot_id":"bot-one","total_shards":2}})
            )
            .unwrap();
            drop(stream);
            let mut stream = accept();
            let request = read(&mut stream);
            let envelope: src::local_control::DaemonControlEnvelope<
                src::update_rollback::UpdateControlRequest,
            > = serde_json::from_value(request.clone()).unwrap();
            assert_eq!(envelope.secret, TEST_CAPABILITY);
            envelope.request.validate().unwrap();
            assert_eq!(envelope.request.command_name(), command);
            assert_eq!(envelope.request.bot, "bot-one");
            assert!(envelope.request.yes);
            assert_eq!(
                envelope.request.component,
                src::update_rollback::Component::Core
            );
            assert!(request.get("role").is_none());
            assert!(request.get("principal").is_none());
            assert_eq!(envelope.request.manifest.shards.len(), 2);
            assert!(
                envelope
                    .request
                    .manifest
                    .artifacts
                    .core
                    .unwrap()
                    .path
                    .is_absolute()
            );
            let id = envelope.request.operation_id;
            writeln!(
                stream,
                "{}",
                json!({"ok":false,"offline":false,
                "error":"update admission denied"})
            )
            .unwrap();
            drop(stream);
            // A rejection must terminate the command, never replay the mutation.
            thread::sleep(Duration::from_millis(200));
            assert!(
                matches!(listener.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
            );
            id
        });
        let value = result(
            fixture.run(
                &[command, "first", "--yes", "--bootstrap-stdin"],
                &bootstrap(port),
            ),
            false,
        );
        assert_eq!(value["offline"], false);
        assert_eq!(value["operation_id"], server.join().unwrap());
        assert_eq!(value["error"], "update admission denied");
        assert_ne!(value["applied"], true);
    }
}

#[test]
fn apply_rejects_malformed_bootstrap_before_connecting() {
    let fixture = Fixture::new(&manifest());
    for command in ["update", "rollback"] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        for input in [
            Vec::new(),
            b"{}\n".to_vec(),
            b"not-json\n".to_vec(),
            format!("{}\n", json!({"port": port, "secret": vec![0; 32]})).into_bytes(),
            format!(
                "{}\n",
                json!({"port": port, "secret": TEST_CAPABILITY,
                       "host": "198.51.100.1", "extra": PRIVATE_SENTINEL})
            )
            .into_bytes(),
        ] {
            let value = result(
                fixture.run(&[command, "first", "--yes", "--bootstrap-stdin"], &input),
                false,
            );
            assert_eq!(value["offline"], false);
            assert_ne!(value["applied"], true);
            let id = value["operation_id"].as_str().unwrap();
            assert_eq!(id.len(), 32);
            assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(
                value["error"],
                if input.is_empty() {
                    "invalid or oversized local control frame"
                } else {
                    "invalid anonymous control bootstrap"
                }
            );
            match listener.accept() {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                _ => panic!("malformed bootstrap must not allow a daemon connection"),
            }
        }
    }
}
