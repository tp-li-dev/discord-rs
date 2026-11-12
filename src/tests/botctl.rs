use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const ID: &str = "0123456789abcdef0123456789abcdef";
const CONFIG: &str = r#"
[cluster]
id = "c"
mode = "standalone"
[node]
id = "a"
[[bots]]
name = "音楽"
bot_id = "abc-one"
application_id = "1"
token_env = "FIRST_TOKEN"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "a"
[[bots.shard_placement]]
shard_id = 1
primary = "b"
[[bots]]
name = "second"
bot_id = "abd-two"
application_id = "2"
token_env = "SECOND_TOKEN"
total_shards = 1
"#;

struct Fixture(PathBuf);
impl Fixture {
    fn new(text: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "botctl-{}-{}.toml",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, text).unwrap();
        Self(path)
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_botctl"))
            .args(args)
            .arg("--file")
            .arg(&self.0)
            .env("FIRST_TOKEN", "SENTINEL_CREDENTIAL_VALUE")
            .output()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn result(output: &Output, success: bool) -> Value {
    assert_eq!(
        output.status.success(),
        success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!text.contains("SENTINEL_CREDENTIAL_VALUE"));
    assert!(!text.contains("PASTED_SECRET_SENTINEL"));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["ok"], success);
    assert!(value.get("secret").is_none());
    value
}

fn cli(args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botctl"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FIRST_TOKEN", "SENTINEL_CREDENTIAL_VALUE")
        .spawn()
        .unwrap();
    if !input.is_empty() {
        let _ = child.stdin.take().unwrap().write_all(input);
    } else {
        drop(child.stdin.take());
    }
    let deadline = Instant::now() + Duration::from_secs(8);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("CLI failed to terminate within test deadline");
        }
        thread::sleep(Duration::from_millis(5));
    }
    child.wait_with_output().unwrap()
}

fn bootstrap(port: u16) -> Vec<u8> {
    let mut frame = serde_json::to_vec(&json!({"port": port, "secret": vec![7; 32]})).unwrap();
    frame.push(b'\n');
    frame
}

enum Reply {
    Json(Value),
    Operation {
        command: &'static str,
        state: &'static str,
        ok: bool,
    },
    Raw(Vec<u8>),
    Silence(Duration),
}

fn read_request(stream: &mut TcpStream) -> Value {
    // Accepted sockets inherit FIONBIO on Windows.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut frame = Vec::new();
    BufReader::new(stream)
        .take(4097)
        .read_until(b'\n', &mut frame)
        .unwrap();
    assert!(frame.len() <= 4096);
    assert_eq!(frame.last(), Some(&b'\n'));
    let request: Value = serde_json::from_slice(&frame).unwrap();
    assert!(request["secret"] == json!(vec![7; 32]));
    assert!(request.get("port").is_none());
    assert!(!String::from_utf8_lossy(&frame).contains("SENTINEL_CREDENTIAL_VALUE"));
    request
}

fn mock(replies: Vec<Reply>) -> (u16, JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for reply in replies {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, peer)) => {
                        assert!(peer.ip().is_loopback());
                        break stream;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "expected control request did not arrive"
                        );
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock accept failed: {error}"),
                }
            };
            let request = read_request(&mut stream);
            let bytes = match reply {
                Reply::Json(value) => format!("{value}\n").into_bytes(),
                Reply::Operation { command, state, ok } => format!("{}\n", json!({
                    "ok": ok, "offline": false,
                    "operation": {
                        "operation_id": request["operation_id"], "command": command,
                        "bot_id": "abc-one", "state": state, "elapsed_ms": 1,
                        "shards": [{"key": {"bot_id": "abc-one", "shard_id": 0}, "generation": 2}],
                        "failed_shards": if state == "failed" { json!([0]) } else { json!([]) }
                    }
                })).into_bytes(),
                Reply::Raw(bytes) => bytes,
                Reply::Silence(duration) => {
                    thread::sleep(duration);
                    Vec::new()
                }
            };
            if !bytes.is_empty() {
                let _ = stream.write_all(&bytes);
            }
            requests.push(request);
        }
        requests
    });
    (port, handle)
}

fn live(args: &[&str], port: u16) -> Output {
    let mut all = args.to_vec();
    all.extend(["--json", "--bootstrap-stdin"]);
    cli(&all, &bootstrap(port))
}

#[test]
fn only_configuration_validation_is_offline() {
    let fixture = Fixture::new(CONFIG);
    let value = result(&fixture.run(&["config", "validate", "--json"]), true);
    assert_eq!(value["bot_count"], 2);
    assert_eq!(value["offline"], true);
    for args in [
        &["ps", "--json"][..],
        &["status", "音楽", "--json"],
        &["shards", "abc", "--json"],
    ] {
        result(&fixture.run(args), false);
    }
}

#[test]
fn config_validation_rejects_unresolved_malformed_missing_and_shared_credentials() {
    let auto = Fixture::new(&CONFIG.replace("total_shards = 2", "total_shards = \"auto\""));
    assert!(
        result(&auto.run(&["config", "validate", "--json"]), false)["error"]
            .as_str()
            .unwrap()
            .contains("unresolved")
    );
    let bad = Fixture::new("token = 'PASTED_SECRET_SENTINEL'");
    result(&bad.run(&["config", "validate", "--json"]), false);
    let shared = Fixture::new(&CONFIG.replace("SECOND_TOKEN", "first_token"));
    result(&shared.run(&["config", "validate", "--json"]), false);
    let missing = Fixture::new(CONFIG);
    std::fs::remove_file(&missing.0).unwrap();
    result(&missing.run(&["config", "validate", "--json"]), false);
}

#[test]
fn live_commands_show_only_daemon_responses_and_send_selectors_unchanged() {
    for (args, expected_command, expected_bot, response) in [
        (
            vec!["ps"],
            "ps",
            None,
            json!({"ok":true,"bots":[{"status":"Running","pid":1234}]}),
        ),
        (
            vec!["status"],
            "status",
            None,
            json!({"ok":true,"bots":[{"status":"Exited"}]}),
        ),
        (
            vec!["status", "音楽"],
            "status",
            Some("音楽"),
            json!({"ok":true,"bot":{"status":"Running"}}),
        ),
        (
            vec!["inspect", "abc-one"],
            "inspect",
            Some("abc-one"),
            json!({"ok":true,"bot":{"status":"Failed"}}),
        ),
        (
            vec!["shards", "abc"],
            "shards",
            Some("abc"),
            json!({"ok":true,"shards":[{"shard_id":0,"state":"Running"}]}),
        ),
    ] {
        let (port, server) = mock(vec![Reply::Json(response.clone())]);
        assert_eq!(result(&live(&args, port), true), response);
        let requests = server.join().unwrap();
        assert_eq!(requests[0]["command"], expected_command);
        assert_eq!(requests[0]["bot"].as_str(), expected_bot);
        assert!(requests[0].get("operation_id").is_none());
    }
}

#[test]
fn unknown_and_ambiguous_selectors_remain_daemon_errors_in_json_and_text() {
    for message in ["bot not found", "ambiguous bot reference"] {
        let (port, server) = mock(vec![Reply::Json(json!({"ok":false,"error":message}))]);
        assert_eq!(
            result(&live(&["shards", "ab"], port), false)["error"],
            message
        );
        server.join().unwrap();
        let (port, server) = mock(vec![Reply::Json(json!({"ok":false,"error":message}))]);
        let output = cli(&["inspect", "ab", "--bootstrap-stdin"], &bootstrap(port));
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains(message));
        server.join().unwrap();
    }
}

#[test]
fn lifecycle_and_aliases_wait_for_completion_by_querying_the_same_operation() {
    for (command, canonical) in [
        ("start", "start"),
        ("stop", "stop"),
        ("shutdown", "stop"),
        ("restart", "restart"),
        ("reboot", "restart"),
    ] {
        let (port, server) = mock(vec![
            Reply::Operation {
                command: canonical,
                state: "running",
                ok: true,
            },
            Reply::Operation {
                command: canonical,
                state: "succeeded",
                ok: true,
            },
        ]);
        let value = result(&live(&[command, "音楽", "--operation-id", ID], port), true);
        assert_eq!(value["operation_id"], ID);
        assert_eq!(value["operation"]["state"], "succeeded");
        assert_eq!(value["operation"]["failed_shards"], json!([]));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["command"], canonical);
        assert_eq!(requests[1]["command"], "operation");
        assert_eq!(requests[0]["operation_id"], requests[1]["operation_id"]);
        assert!(requests[1].get("bot").is_none());
    }
}

#[test]
fn generated_operation_ids_are_unique_and_returned() {
    let mut ids = Vec::new();
    for _ in 0..2 {
        let (port, server) = mock(vec![Reply::Operation {
            command: "start",
            state: "succeeded",
            ok: true,
        }]);
        let value = result(&live(&["start", "abc"], port), true);
        let id = value["operation_id"].as_str().unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(server.join().unwrap()[0]["operation_id"], id);
        ids.push(id.to_owned());
    }
    assert_ne!(ids[0], ids[1]);
}

#[test]
fn failed_or_inconsistent_operations_are_never_reported_as_success() {
    for (state, ok) in [("failed", false), ("failed", true), ("succeeded", false)] {
        let (port, server) = mock(vec![Reply::Operation {
            command: "start",
            state,
            ok,
        }]);
        let value = result(&live(&["start", "abc", "--operation-id", ID], port), false);
        assert_eq!(value["operation_id"], ID);
        server.join().unwrap();
    }
    for operation in [
        json!({"operation_id":"ffffffffffffffffffffffffffffffff","command":"start","state":"succeeded"}),
        json!({"operation_id":ID,"command":"stop","state":"succeeded"}),
        json!({"operation_id":ID,"command":"start","state":"mystery"}),
    ] {
        let (port, server) = mock(vec![Reply::Json(json!({"ok":true,"operation":operation}))]);
        result(&live(&["start", "abc", "--operation-id", ID], port), false);
        server.join().unwrap();
    }
}

#[test]
fn completed_operations_require_consistent_result_metadata() {
    let valid = json!({
        "operation_id": ID, "command": "start", "state": "succeeded",
        "bot_id": "abc-one", "elapsed_ms": 1, "shards": [], "failed_shards": []
    });
    let mut malformed = Vec::new();
    for field in ["bot_id", "elapsed_ms", "shards", "failed_shards"] {
        let mut operation = valid.clone();
        operation.as_object_mut().unwrap().remove(field);
        malformed.push(operation);
    }
    for (field, value) in [
        ("elapsed_ms", json!(-1)),
        ("shards", json!({})),
        ("failed_shards", json!([0])),
        ("bot_id", json!(7)),
    ] {
        let mut operation = valid.clone();
        operation[field] = value;
        malformed.push(operation);
    }
    for operation in malformed {
        let (port, server) = mock(vec![Reply::Json(
            json!({"ok": true, "operation": operation}),
        )]);
        let value = result(&live(&["start", "abc", "--operation-id", ID], port), false);
        assert_eq!(value["operation_id"], ID);
        server.join().unwrap();
    }
}

#[test]
fn control_capability_echoes_are_rejected_before_output() {
    let capability = json!(vec![7; 32]);
    let embedded = format!(
        "request contained {}",
        serde_json::to_string_pretty(&capability).unwrap()
    );
    let hex = "07".repeat(32);
    for response in [
        json!({"ok": false, "error": embedded}),
        json!({"ok": false, "error": hex}),
        json!({"ok": true, "bots": [], "unknown": {"data": capability}}),
        json!({"ok": true, "bots": [], "unknown": {"secret": "PASTED_SECRET_SENTINEL"}}),
    ] {
        let (port, server) = mock(vec![Reply::Json(response)]);
        let value = result(&live(&["ps"], port), false);
        assert_eq!(
            value["error"],
            "daemon response contains control credentials"
        );
        assert!(value.get("unknown").is_none());
        server.join().unwrap();
    }
}

#[test]
fn operation_requery_reports_running_failed_and_completed_without_resubmitting() {
    for alias in ["operation", "op"] {
        for (state, success) in [("running", false), ("failed", false), ("succeeded", true)] {
            let (port, server) = mock(vec![Reply::Operation {
                command: "stop",
                state,
                ok: state != "failed",
            }]);
            let value = result(&live(&[alias, ID], port), success);
            assert_eq!(value["operation"]["state"], state);
            if state == "running" {
                assert_eq!(value["pending"], true);
            }
            let requests = server.join().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0]["command"], "operation");
        }
    }
}

#[test]
fn operation_timeout_retains_id_and_does_not_retry_mutation() {
    let (port, server) = mock(vec![
        Reply::Operation {
            command: "stop",
            state: "running",
            ok: true,
        },
        Reply::Silence(Duration::from_millis(500)),
    ]);
    let value = result(
        &live(
            &["stop", "abc", "--operation-id", ID, "--timeout", "200ms"],
            port,
        ),
        false,
    );
    assert_eq!(value["operation_id"], ID);
    assert_eq!(value["operation"]["state"], "running");
    assert!(value["error"].as_str().unwrap().contains("timed out"));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["command"], "stop");
    assert_eq!(requests[1]["command"], "operation");
}

#[test]
fn lost_initial_reply_is_an_error_with_requery_id() {
    let (port, server) = mock(vec![Reply::Raw(Vec::new())]);
    let value = result(
        &live(&["restart", "abc", "--operation-id", ID], port),
        false,
    );
    assert_eq!(value["operation_id"], ID);
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn force_stop_requires_explicit_confirmation_and_forwards_both_flags() {
    let value = result(&cli(&["stop", "abc", "--force", "--json"], b""), false);
    assert!(value["error"].as_str().unwrap().contains("--yes"));
    let (port, server) = mock(vec![Reply::Operation {
        command: "stop",
        state: "succeeded",
        ok: true,
    }]);
    result(&live(&["shutdown", "abc", "--force", "--yes"], port), true);
    let requests = server.join().unwrap();
    assert_eq!(requests[0]["force"], true);
    assert_eq!(requests[0]["yes"], true);
}

#[test]
fn invalid_options_and_unimplemented_commands_fail_before_connecting() {
    for args in [
        vec!["ps", "extra"],
        vec!["ps", "--json"],
        vec!["ps", "--bad"],
        vec!["start"],
        vec!["stop", ""],
        vec!["restart", "abc", "--force", "--yes"],
        vec!["stop", "abc", "--yes"],
        vec!["ps", "--operation-id", ID],
        vec!["operation", "bad"],
        vec![
            "start",
            "abc",
            "--operation-id",
            "ABCDEF0123456789ABCDEF0123456789",
        ],
        vec!["ps", "--bootstrap-stdin", "--bootstrap-stdin"],
        vec!["ps", "--timeout", "1s", "--timeout", "1s"],
        vec!["ps", "--host", "198.51.100.1"],
        vec!["ps", "--secret", "PASTED_SECRET_SENTINEL"],
    ] {
        let mut args = args;
        args.push("--json");
        result(&cli(&args, b""), false);
    }
    for timeout in [
        "0",
        "-1",
        "NaN",
        "inf",
        "3601",
        "3600001ms",
        "999999999999999999999999",
        "0.00000000001s",
        "bad",
    ] {
        result(
            &cli(
                &["ps", "--timeout", timeout, "--bootstrap-stdin", "--json"],
                b"",
            ),
            false,
        );
    }
    for args in [
        vec!["update", "abc", "--component", "core"],
        vec!["rollback", "abc"],
        vec!["logs", "abc"],
        vec!["nodes"],
    ] {
        let mut args = args;
        args.push("--json");
        let value = result(&cli(&args, b""), false);
        assert!(value["error"].as_str().unwrap().contains("未実装"));
    }
}

#[test]
fn bootstrap_rejects_remote_zero_malformed_unknown_and_oversized_inputs() {
    let mut zero = serde_json::to_vec(&json!({"port":1,"secret":vec![0;32]})).unwrap();
    zero.push(b'\n');
    for frame in [
        b"{}\n".to_vec(),
        b"bad\n".to_vec(),
        b"{}".to_vec(),
        zero,
        bootstrap(0),
        vec![b'x'; 4097],
        format!(
            "{}\n",
            json!({"port":1,"secret":vec![7;32],"host":"198.51.100.1"})
        )
        .into_bytes(),
        format!(
            "{}\n",
            json!({"port":1,"secret":vec![7;32],"extra":"PASTED_SECRET_SENTINEL"})
        )
        .into_bytes(),
    ] {
        result(&cli(&["ps", "--bootstrap-stdin", "--json"], &frame), false);
    }
}

#[test]
fn absent_daemon_and_invalid_responses_fail_instead_of_showing_configured_bots() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    result(&live(&["ps", "--timeout", "1s"], port), false);
    for bytes in [
        b"{\"ok\":true}\n".to_vec(),
        b"{}\n".to_vec(),
        b"bad\n".to_vec(),
        b"{\"ok\":true,\"bots\":[]}".to_vec(),
        vec![b'x'; 65537],
        b"{\"ok\":true,\"offline\":true,\"bots\":[]}\n".to_vec(),
        b"{\"ok\":true,\"secret\":\"PASTED_SECRET_SENTINEL\",\"bots\":[]}\n".to_vec(),
    ] {
        let (port, server) = mock(vec![Reply::Raw(bytes)]);
        result(&live(&["ps"], port), false);
        server.join().unwrap();
    }
}

#[test]
fn idle_bootstrap_has_a_real_process_deadline() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botctl"))
        .args(["ps", "--bootstrap-stdin", "--timeout", "100ms", "--json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > Duration::from_secs(3) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("idle stdin kept the CLI alive past timeout");
        }
        thread::sleep(Duration::from_millis(5));
    }
    result(&child.wait_with_output().unwrap(), false);
}

#[test]
fn oversized_bot_reference_is_rejected_without_sending_credentials() {
    let reference = "x".repeat(4096);
    let value = result(
        &cli(
            &["status", &reference, "--bootstrap-stdin", "--json"],
            &bootstrap(1),
        ),
        false,
    );
    assert!(value["error"].as_str().unwrap().contains("exceeds limit"));
}
