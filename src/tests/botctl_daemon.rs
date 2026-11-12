//! Authenticated botctl ↔ botd process integration, using a test-only Gateway.
//! This proves local process control, not Discord readiness or distributed leases.
use serde_json::{Value, json};
use src::{
    placement::Config,
    shard_runtime::{self, ShardRuntimeSpec},
};
use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::Child,
};

const SECRET: [u8; 32] = [19; 32];
const ALPHA: &str = "ab100-alpha";
const BETA: &str = "ab200-beta";
const CONFIG: &str = r#"
[cluster]
id = "local-tests"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "音楽"
bot_id = "ab100-alpha"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 3
[[bots.shard_placement]]
shard_id = 0
primary = "one"
[[bots.shard_placement]]
shard_id = 1
primary = "one"
[[bots.shard_placement]]
shard_id = 2
primary = "other"
[[bots]]
name = "second"
bot_id = "ab200-beta"
application_id = "102"
token_env = "TOKEN_BETA"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "one"
"#;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("local control integration deadline")
}

fn directory(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("botctl-daemon-tests");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn gateway_binary() -> &'static Path {
    static PROGRAM: OnceLock<PathBuf> = OnceLock::new();
    PROGRAM.get_or_init(|| {
        let program =
            directory("fixture-build").join(format!("gateway{}", std::env::consts::EXE_SUFFIX));
        let output = Command::new("rustc")
            .args(["--edition=2024", "-Dwarnings"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/daemon_gateway.rs"))
            .arg("-o")
            .arg(&program)
            .env_remove("DISCORD_TOKEN")
            .output()
            .expect("compile local control fixture");
        assert!(
            output.status.success(),
            "fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        program
    })
}

struct DaemonFixture {
    root: PathBuf,
    plan: Vec<ShardRuntimeSpec>,
    child: Child,
    port: u16,
}

impl DaemonFixture {
    async fn new() -> Self {
        Self::configured(CONFIG, false).await
    }

    async fn configured(config: &str, missing_beta_credential: bool) -> Self {
        let root = directory("session");
        let file = root.join("placement.toml");
        std::fs::write(&file, config).unwrap();
        let config = Config::load(&file).unwrap();
        let plan = shard_runtime::plan(
            &config.local_primary_intents(&HashMap::new()).unwrap(),
            &root,
        )
        .unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(file)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(gateway_binary())
            .args(["--bootstrap-stdin", "--no-autostart"])
            .env("TOKEN_ALPHA", "dummy-alpha")
            .env("DISCORD_TOKEN", "must-not-reach-fixture")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if missing_beta_credential {
            command.env_remove("TOKEN_BETA");
        } else {
            command.env("TOKEN_BETA", "dummy-beta");
        }
        let mut child = command.spawn().unwrap();
        let mut bootstrap = json!({"secret": SECRET.to_vec()}).to_string();
        bootstrap.push('\n');
        bounded(child.stdin.take().unwrap().write_all(bootstrap.as_bytes()))
            .await
            .unwrap();
        let mut announcement = String::new();
        bounded(BufReader::new(child.stdout.take().unwrap()).read_line(&mut announcement))
            .await
            .unwrap();
        let value: Value = serde_json::from_str(&announcement).expect("daemon announcement");
        assert_eq!(value["local_only"], true);
        assert_eq!(value["startup_ok"], true);
        assert!(value.get("secret").is_none());
        let port = u16::try_from(value["port"].as_u64().unwrap()).unwrap();
        Self {
            root,
            plan,
            child,
            port,
        }
    }

    async fn cli(&self, args: &[&str]) -> Output {
        cli(self.port, SECRET, args).await
    }

    async fn json(&self, args: &[&str], success: bool) -> Value {
        parse_output(self.cli(args).await, success)
    }

    async fn status(&self) -> Value {
        self.json(&["status"], true).await
    }

    fn spec(&self, bot: &str, shard: u32) -> &ShardRuntimeSpec {
        self.plan
            .iter()
            .find(|spec| spec.bot_id == bot && spec.shard_id == shard)
            .unwrap()
    }

    fn mode(&self, bot: &str, shard: u32, mode: &str) {
        let management = &self.spec(bot, shard).gateway.management_path;
        std::fs::create_dir_all(management.parent().unwrap()).unwrap();
        std::fs::write(management.with_extension("fixture-mode"), mode).unwrap();
    }

    async fn ready(&self, status: &Value) {
        for process in processes(status) {
            if process["state"]["state"] != "Running" {
                continue;
            }
            let spec = self.spec(
                process["key"]["bot_id"].as_str().unwrap(),
                process["key"]["shard_id"].as_u64().unwrap() as u32,
            );
            let pid = process["identity"]["pid"].as_u64().unwrap();
            let ready = spec
                .gateway
                .management_path
                .with_extension(format!("started-{pid}"));
            bounded(async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
        }
    }

    async fn raw(&self, mut request: Value) -> Value {
        request["secret"] = json!(SECRET.to_vec());
        let reply = raw(self.port, request).await;
        serde_json::from_slice(&reply).expect("authenticated response")
    }

    async fn cleanup(mut self) {
        // A separate file only understood by the dummy children makes assertion
        // cleanup independent of whether the production control channel works.
        for spec in &self.plan {
            let path = spec.gateway.management_path.with_extension("fixture-exit");
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(path, "");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = self.child.kill().await;
        assert_no_persisted_credentials(&self.root);
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        for spec in &self.plan {
            let path = spec.gateway.management_path.with_extension("fixture-exit");
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(path, "");
        }
        let _ = self.child.start_kill();
        // Preserve a failed scenario's directory as bounded, credential-free
        // test artifacts; a still-running fixture must see its cleanup marker.
    }
}

fn assert_no_persisted_credentials(directory: &Path) {
    for item in std::fs::read_dir(directory).unwrap() {
        let item = item.unwrap();
        if item.file_type().unwrap().is_dir() {
            assert_no_persisted_credentials(&item.path());
        } else {
            let bytes = std::fs::read(item.path()).unwrap();
            let text = String::from_utf8_lossy(&bytes);
            for forbidden in [
                "dummy-alpha",
                "dummy-beta",
                "must-not-reach-fixture",
                "\"secret\"",
            ] {
                assert!(!text.contains(forbidden), "test credential persisted");
            }
        }
    }
}

async fn cli(port: u16, secret: [u8; 32], args: &[&str]) -> Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botctl"));
    command
        .args(args)
        .args(["--json", "--bootstrap-stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if !args.contains(&"--timeout") {
        command.args(["--timeout", "5s"]);
    }
    let mut child = command.spawn().unwrap();
    let mut bootstrap = json!({"port": port, "secret": secret.to_vec()}).to_string();
    bootstrap.push('\n');
    // Rejected arguments may cause the CLI to close stdin before bootstrap.
    let _ = child
        .stdin
        .take()
        .unwrap()
        .write_all(bootstrap.as_bytes())
        .await;
    bounded(child.wait_with_output()).await.unwrap()
}

fn parse_output(output: Output, success: bool) -> Value {
    assert_eq!(
        output.status.success(),
        success,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "JSON errors belong on stdout");
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(!text.contains("dummy-alpha"));
    assert!(!text.contains("dummy-beta"));
    assert!(!text.contains("must-not-reach-fixture"));
    assert!(!text.contains("\"secret\""));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["ok"], success);
    value
}

fn processes(value: &Value) -> &[Value] {
    value["status"]["processes"].as_array().unwrap()
}

fn process<'a>(status: &'a Value, bot: &str, shard: u32) -> &'a Value {
    processes(status)
        .iter()
        .find(|p| p["key"]["bot_id"] == bot && p["key"]["shard_id"] == shard)
        .unwrap()
}

fn operation_id(value: &Value) -> &str {
    value["operation"]["operation_id"]
        .as_str()
        .or_else(|| value["operation_id"].as_str())
        .expect("operation id remains queryable")
}

fn succeeded(value: &Value) {
    assert_eq!(value["operation"]["state"], "succeeded");
    assert!(value["operation"]["elapsed_ms"].is_number());
    assert!(
        value["operation"]["failed_shards"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!value["operation"]["shards"].as_array().unwrap().is_empty());
}

async fn raw(port: u16, value: Value) -> Vec<u8> {
    bounded(async {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut request = value.to_string();
        request.push('\n');
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        response
    })
    .await
}

#[tokio::test]
async fn multiple_bots_use_real_states_and_graceful_lifecycle_by_name_id_and_prefix() {
    let fixture = DaemonFixture::new().await;
    let initial = fixture.json(&["ps"], true).await;
    assert_eq!(initial["offline"], false);
    assert_eq!(initial["bots"].as_array().unwrap().len(), 2);
    assert_eq!(processes(&initial).len(), 3);
    assert!(processes(&initial).iter().all(|p| p["identity"].is_null()));

    succeeded(&fixture.json(&["start", "音楽"], true).await);
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    assert!(
        processes(&before)
            .iter()
            .all(|p| p["state"]["state"] == "Running")
    );
    assert_eq!(
        fixture.json(&["status", "ab1"], true).await["bot"]["bot_id"],
        ALPHA
    );
    assert_eq!(
        fixture.json(&["inspect", "音楽"], true).await["bot"]["bot_id"],
        ALPHA
    );
    let shards = fixture.json(&["shards", ALPHA], true).await;
    assert_eq!(shards["shards"].as_array().unwrap().len(), 3);
    assert_eq!(
        shards["shards"][0]["process"]["identity"],
        process(&before, ALPHA, 0)["identity"]
    );
    assert_eq!(shards["shards"][2]["local"], false);
    assert!(shards["shards"][2]["process"].is_null());

    let stop = fixture.json(&["stop", "ab1"], true).await;
    succeeded(&stop);
    let after_stop = fixture.status().await;
    for shard in 0..2 {
        let old = process(&before, ALPHA, shard);
        let pid = old["identity"]["pid"].as_u64().unwrap();
        let path = fixture
            .spec(ALPHA, shard)
            .gateway
            .management_path
            .with_extension(format!("drained-{pid}"));
        assert!(path.exists(), "graceful completion must precede success");
        assert_eq!(
            process(&after_stop, ALPHA, shard)["state"]["state"],
            "Exited"
        );
        assert_eq!(process(&after_stop, ALPHA, shard)["desired_running"], false);
    }
    assert_eq!(
        process(&before, BETA, 0)["identity"],
        process(&after_stop, BETA, 0)["identity"]
    );
    succeeded(
        &fixture
            .json(&["operation", operation_id(&stop)], true)
            .await,
    );

    succeeded(&fixture.json(&["start", ALPHA], true).await);
    succeeded(&fixture.json(&["reboot", "ab2"], true).await);
    let restarted = fixture.status().await;
    fixture.ready(&restarted).await;
    for (bot, shard) in [(ALPHA, 0), (ALPHA, 1), (BETA, 0)] {
        assert_ne!(
            process(&before, bot, shard)["identity"]["generation"],
            process(&restarted, bot, shard)["identity"]["generation"]
        );
    }
    succeeded(&fixture.json(&["shutdown", "音楽"], true).await);
    succeeded(&fixture.json(&["stop", "second"], true).await);
    fixture.cleanup().await;
}

#[tokio::test]
async fn unknown_ambiguous_and_cross_namespace_selectors_never_change_children() {
    let fixture = DaemonFixture::configured(
        &CONFIG.replace("name = \"second\"", "name = \"ab1\""),
        false,
    )
    .await;
    for command in ["status", "inspect", "shards", "start", "stop", "restart"] {
        for selector in ["absent", "ab", "ab1"] {
            let response = fixture.json(&[command, selector], false).await;
            assert!(response["error"].is_string());
        }
    }
    assert!(
        processes(&fixture.status().await)
            .iter()
            .all(|p| p["identity"].is_null())
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn force_stop_requires_confirmation_in_cli_and_authenticated_server() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    fixture.json(&["stop", "second", "--force"], false).await;
    let rejected = fixture.raw(json!({"command":"stop", "bot":BETA, "force":true, "yes":false, "operation_id":"11111111111111111111111111111111"})).await;
    assert_eq!(rejected["ok"], false);
    let after = fixture.status().await;
    assert_eq!(
        process(&before, BETA, 0)["identity"],
        process(&after, BETA, 0)["identity"]
    );
    assert_eq!(process(&after, BETA, 0)["state"]["state"], "Running");
    succeeded(
        &fixture
            .json(&["stop", "second", "--force", "--yes"], true)
            .await,
    );
    let pid = process(&before, BETA, 0)["identity"]["pid"]
        .as_u64()
        .unwrap();
    assert!(
        !fixture
            .spec(BETA, 0)
            .gateway
            .management_path
            .with_extension(format!("shutdown-{pid}"))
            .exists()
    );
    assert_eq!(
        process(&fixture.status().await, BETA, 0)["state"]["state"],
        "Exited"
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn client_timeout_keeps_operation_queryable_and_other_bot_control_available() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", ALPHA], true).await);
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    fixture.mode(ALPHA, 0, "slow");
    fixture.mode(ALPHA, 1, "slow");
    let id = "22222222222222222222222222222222";
    let timed_out = fixture
        .json(
            &["stop", "音楽", "--timeout", "100ms", "--operation-id", id],
            false,
        )
        .await;
    assert_eq!(operation_id(&timed_out), id);
    // A wait deadline does not grant permission to force-kill a draining child.
    assert_eq!(
        process(&fixture.status().await, ALPHA, 0)["state"]["state"],
        "Running"
    );
    succeeded(&fixture.json(&["restart", "second"], true).await);
    bounded(async {
        loop {
            let response = fixture
                .raw(json!({"command":"operation", "operation_id":id}))
                .await;
            if response["operation"]["state"] != "running" {
                assert_eq!(response["ok"], true);
                succeeded(&response);
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    succeeded(&fixture.json(&["operation", id], true).await);
    let after = fixture.status().await;
    assert_eq!(process(&after, ALPHA, 0)["state"]["state"], "Exited");
    assert_eq!(process(&after, BETA, 0)["state"]["state"], "Running");
    fixture.cleanup().await;
}

#[tokio::test]
async fn operation_replay_is_idempotent_and_conflicting_reuse_is_rejected() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    let id = "33333333333333333333333333333333";
    let first = fixture
        .json(&["restart", "second", "--operation-id", id], true)
        .await;
    succeeded(&first);
    let after_first = fixture.status().await;
    let repeat = fixture
        .json(&["restart", BETA, "--operation-id", id], true)
        .await;
    assert_eq!(repeat["operation"], first["operation"]);
    assert_eq!(
        process(&fixture.status().await, BETA, 0)["identity"],
        process(&after_first, BETA, 0)["identity"]
    );
    fixture
        .json(&["stop", BETA, "--operation-id", id], false)
        .await;
    assert_eq!(
        process(&fixture.status().await, BETA, 0)["state"]["state"],
        "Running"
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn failed_spawn_and_abnormal_graceful_exit_are_not_success() {
    let fixture = DaemonFixture::configured(CONFIG, true).await;
    let failure = fixture.json(&["start", BETA], false).await;
    assert_eq!(failure["operation"]["state"], "failed");
    assert!(
        !failure["operation"]["failed_shards"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let repeated = fixture
        .json(&["operation", operation_id(&failure)], false)
        .await;
    assert_eq!(repeated["operation"], failure["operation"]);
    succeeded(&fixture.json(&["start", ALPHA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    fixture.mode(ALPHA, 1, "fail-stop");
    let stopped = fixture.json(&["stop", ALPHA], false).await;
    assert_eq!(stopped["operation"]["state"], "failed");
    assert!(
        !stopped["operation"]["failed_shards"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let after = fixture.status().await;
    assert_eq!(process(&after, ALPHA, 1)["state"]["code"], 23);
    assert_eq!(process(&after, ALPHA, 1)["desired_running"], false);
    fixture.cleanup().await;
}

#[tokio::test]
async fn wrong_authentication_cannot_read_or_mutate_daemon() {
    let fixture = DaemonFixture::new().await;
    parse_output(cli(fixture.port, [20; 32], &["status"]).await, false);
    let reply = raw(fixture.port, json!({"secret":vec![20;32], "command":"start", "bot":BETA, "operation_id":"44444444444444444444444444444444"})).await;
    assert!(reply.is_empty());
    assert!(
        processes(&fixture.status().await)
            .iter()
            .all(|p| p["identity"].is_null())
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn unavailable_daemon_never_falls_back_to_configured_success() {
    let fixture = DaemonFixture::new().await;
    let port = fixture.port;
    fixture.cleanup().await;
    for args in [
        vec!["ps"],
        vec!["status"],
        vec!["inspect", ALPHA],
        vec!["shards", ALPHA],
        vec!["start", ALPHA],
    ] {
        let response = parse_output(cli(port, SECRET, &args).await, false);
        assert!(response["error"].is_string());
    }
}

#[tokio::test]
async fn in_flight_operation_rejects_conflicts_until_explicit_force_supersedes_it() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    fixture.mode(BETA, 0, "ignore");
    let waiting_id = "55555555555555555555555555555555";
    let pending = fixture
        .raw(json!({
            "command":"restart", "bot":BETA, "operation_id":waiting_id
        }))
        .await;
    assert_eq!(pending["operation"]["state"], "running");
    fixture.json(&["restart", BETA], false).await;
    assert_eq!(
        process(&fixture.status().await, BETA, 0)["identity"],
        process(&before, BETA, 0)["identity"]
    );
    succeeded(
        &fixture
            .json(&["stop", BETA, "--force", "--yes"], true)
            .await,
    );
    let superseded = fixture.json(&["operation", waiting_id], false).await;
    assert_eq!(superseded["operation"]["state"], "failed");
    assert!(
        !superseded["operation"]["failed_shards"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_force = fixture.status().await;
    assert_eq!(process(&after_force, BETA, 0)["state"]["state"], "Exited");
    assert_eq!(process(&after_force, BETA, 0)["desired_running"], false);
    assert_eq!(
        process(&after_force, BETA, 0)["identity"],
        process(&before, BETA, 0)["identity"]
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn restart_does_not_replace_a_shard_that_failed_graceful_shutdown() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    fixture.mode(BETA, 0, "fail-stop");
    let response = fixture.json(&["restart", BETA], false).await;
    assert_eq!(response["operation"]["state"], "failed");
    let after = fixture.status().await;
    assert_eq!(
        process(&after, BETA, 0)["identity"],
        process(&before, BETA, 0)["identity"]
    );
    assert_eq!(process(&after, BETA, 0)["state"]["code"], 23);
    assert_eq!(process(&after, BETA, 0)["desired_running"], false);
    fixture.cleanup().await;
}

#[tokio::test]
async fn daemon_operation_deadline_preserves_live_child_and_prevents_late_restart() {
    let fixture = DaemonFixture::new().await;
    succeeded(&fixture.json(&["start", BETA], true).await);
    let before = fixture.status().await;
    fixture.ready(&before).await;
    let original = process(&before, BETA, 0)["identity"].clone();
    let pid = original["pid"].as_u64().unwrap();
    fixture.mode(BETA, 0, "ignore");
    let id = "66666666666666666666666666666666";
    let admitted = fixture
        .raw(json!({"command":"restart", "bot":BETA, "operation_id":id}))
        .await;
    assert_eq!(admitted["operation"]["state"], "running");

    // Reusing the accepted ID waits on that operation; the CLI deadline neither
    // cancels it nor grants permission to force a shutdown.
    let short_wait = fixture
        .json(
            &["restart", BETA, "--operation-id", id, "--timeout", "100ms"],
            false,
        )
        .await;
    assert_eq!(operation_id(&short_wait), id);
    let pending = fixture
        .json(&["operation", id, "--timeout", "1s"], false)
        .await;
    assert_eq!(pending["operation"]["state"], "running");
    assert_eq!(pending["pending"], true);
    assert_eq!(
        process(&fixture.status().await, BETA, 0)["identity"],
        original
    );

    // A stuck shutdown on one Bot does not block independent Bot operations.
    succeeded(&fixture.json(&["start", ALPHA], true).await);
    succeeded(&fixture.json(&["restart", ALPHA], true).await);

    let failed = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let response = fixture
                .raw(json!({"command":"operation", "operation_id":id}))
                .await;
            if response["operation"]["state"] != "running" {
                break response;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("daemon must report its 30-second operation deadline");
    assert_eq!(failed["ok"], false);
    assert_eq!(failed["operation"]["state"], "failed");
    assert!(failed["operation"]["elapsed_ms"].as_u64().unwrap() >= 30_000);
    assert_eq!(failed["operation"]["shards"][0]["error"], "StopTimedOut");
    assert_eq!(
        failed["operation"]["failed_shards"],
        json!([{"bot_id":BETA,"shard_id":0}])
    );
    let requery = fixture.json(&["operation", id], false).await;
    assert_eq!(requery["operation"], failed["operation"]);
    let after_deadline = fixture.status().await;
    assert_eq!(process(&after_deadline, BETA, 0)["identity"], original);
    assert_eq!(
        process(&after_deadline, BETA, 0)["state"]["state"],
        "Running"
    );
    assert_eq!(process(&after_deadline, BETA, 0)["desired_running"], false);
    succeeded(&fixture.json(&["stop", ALPHA], true).await);

    // Have the exact old process acknowledge an independent test-only exit after
    // the deadline. Its later exit must not revive the timed-out restart.
    let management = &fixture.spec(BETA, 0).gateway.management_path;
    assert!(
        management
            .with_extension(format!("shutdown-{pid}"))
            .exists()
    );
    std::fs::write(management.with_extension("fixture-exit"), "").unwrap();
    bounded(async {
        while !management.with_extension(format!("exited-{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            if process(&fixture.status().await, BETA, 0)["state"]["state"] == "Exited" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_exit = fixture.status().await;
    assert_eq!(process(&after_exit, BETA, 0)["identity"], original);
    assert_eq!(process(&after_exit, BETA, 0)["state"]["state"], "Exited");
    assert_eq!(process(&after_exit, BETA, 0)["desired_running"], false);
    assert_eq!(process(&after_exit, BETA, 0)["restarts"], 0);
    fixture.cleanup().await;
}
