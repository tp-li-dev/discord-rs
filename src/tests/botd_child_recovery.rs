//! Owned-process fault injection through real botd/Gateway/Core executables.
//! Discord is the explicit loopback simulator: these tests do not establish
//! live Discord, persistent Identify budgets, or distributed lease guarantees.
use serde_json::{Value, json};
use src::testkit::{MockDiscord, MockReply};
use std::{
    collections::BTreeSet,
    future::Future,
    io::{BufRead, Write},
    net::{SocketAddr, TcpStream as BlockingStream},
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    process::Child,
};

const WAIT: Duration = Duration::from_secs(25);
const SECRET: [u8; 32] = [83; 32];
const KEYS: [&str; 3] = ["101/0", "101/1", "202/0"];
const SENTINELS: [&str; 3] = [
    "recovery-test-alpha-credential",
    "recovery-test-beta-credential",
    "recovery-test-parent-credential",
];
static SERIAL: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
const CONFIG: &str = r#"
[cluster]
id = "child-recovery-process-test"
mode = "standalone"
[node]
id = "local"
[[bots]]
name = "alpha"
bot_id = "101"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "local"
[[bots.shard_placement]]
shard_id = 1
primary = "local"
[[bots]]
name = "beta"
bot_id = "202"
application_id = "202"
token_env = "TOKEN_BETA"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "local"
"#;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("botd child recovery deadline")
}

fn next_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!("{:032x}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn no_secrets(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    for sentinel in SENTINELS {
        assert!(
            !text.contains(sentinel),
            "credential reached observable output"
        );
    }
    assert!(
        !text.contains("\"secret\""),
        "capability reached observable output"
    );
}

fn no_persisted_secrets(directory: &Path) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            no_persisted_secrets(&entry.path());
        } else if entry.metadata().unwrap().len() != 0 {
            // Empty lock files hold no payload and may be exclusively locked
            // on Windows while the daemon owns the audit writer.
            no_secrets(&std::fs::read(entry.path()).unwrap());
        }
    }
}

struct Gateway {
    ready: Value,
    input: OwnedWriteHalf,
    output: BufReader<OwnedReadHalf>,
}

impl Gateway {
    async fn receive(&mut self) -> Value {
        let mut bytes = Vec::new();
        bounded(
            (&mut self.output)
                .take(64 * 1024 + 1)
                .read_until(b'\n', &mut bytes),
        )
        .await
        .unwrap();
        assert!(!bytes.is_empty(), "existing Gateway connection was lost");
        assert!(bytes.len() <= 64 * 1024);
        assert_eq!(bytes.last(), Some(&b'\n'));
        no_secrets(&bytes);
        serde_json::from_slice(&bytes).expect("collector JSON")
    }

    async fn exchange(&mut self, value: Value) -> Value {
        let id = value["id"].clone();
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bounded(self.input.write_all(&bytes)).await.unwrap();
        let reply = self.receive().await;
        assert_eq!(reply["id"], id);
        reply
    }

    fn pid(&self) -> u32 {
        self.ready["pid"].as_u64().unwrap().try_into().unwrap()
    }

    fn core_pid(&self) -> u32 {
        self.ready["workers"][0]["pid"]
            .as_u64()
            .unwrap()
            .try_into()
            .unwrap()
    }

    async fn ping(&mut self) {
        let mock = MockDiscord::new(vec![MockReply::json(200, b"{}").unwrap()]);
        let server = mock.spawn_tcp().await.unwrap();
        let reply = self
            .exchange(json!({"type":"prefix","id":1,"key":0,"content":"!ping",
                "rest_addr":server.addr().to_string()}))
            .await;
        assert_eq!(reply["outcome"], "completed");
        assert_eq!(reply["receipt"]["pid"], self.core_pid());
        assert_eq!(
            reply["receipt"]["identity"],
            self.ready["workers"][0]["identity"]
        );
        assert_eq!(reply["receipt"]["outcome"]["kind"], "confirmed");
        assert_eq!(reply["unknown_responses"], 0);
        assert_eq!(mock.stats().requests, 1);
        server.shutdown().await.unwrap();
    }
}

struct Fixture {
    root: PathBuf,
    daemon: Child,
    port: u16,
    collector: TcpListener,
    gateways: Vec<Gateway>,
    observed_pids: BTreeSet<u32>,
}

impl Fixture {
    async fn new(alpha_identifies: u32) -> Self {
        Self::configured(alpha_identifies, None).await
    }

    async fn configured(alpha_identifies: u32, max_restarts: Option<u32>) -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("botd-child-recovery-tests")
            .join(format!("{}-{}", std::process::id(), next_id()));
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("placement.toml");
        let budget = root.join("identify.json");
        std::fs::write(&config, CONFIG).unwrap();
        std::fs::write(
            &budget,
            serde_json::to_vec(&json!([
                {"bot_id":101,"total":alpha_identifies,"remaining":alpha_identifies,
                    "reset_after_ms":60000,"max_concurrency":2,"interval_ms":25},
                {"bot_id":202,"total":10,"remaining":10,
                    "reset_after_ms":60000,"max_concurrency":1,"interval_ms":25}
            ]))
            .unwrap(),
        )
        .unwrap();
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(config)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .args([
                "--bootstrap-stdin",
                "--allow-test-gateway-switch",
                "--mock-discord",
            ])
            .arg(collector.local_addr().unwrap().to_string())
            .arg("--mock-identify-budget")
            .arg(budget)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(max_restarts) = max_restarts {
            command
                .arg("--mock-recovery-max-restarts")
                .arg(max_restarts.to_string());
        }
        for (name, value) in ["TOKEN_ALPHA", "TOKEN_BETA", "DISCORD_TOKEN"]
            .into_iter()
            .zip(SENTINELS)
        {
            command.env(name, value);
        }
        let mut daemon = command.spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret":SECRET})).unwrap();
        bootstrap.push(b'\n');
        bounded(daemon.stdin.take().unwrap().write_all(&bootstrap))
            .await
            .unwrap();
        let mut bytes = Vec::new();
        bounded(BufReader::new(daemon.stdout.take().unwrap()).read_until(b'\n', &mut bytes))
            .await
            .unwrap();
        no_secrets(&bytes);
        if bytes.is_empty() {
            let mut diagnostic = Vec::new();
            if let Some(stderr) = daemon.stderr.take() {
                bounded(stderr.take(4096).read_to_end(&mut diagnostic))
                    .await
                    .unwrap();
            }
            no_secrets(&diagnostic);
            let status = bounded(daemon.wait()).await.unwrap();
            panic!(
                "botd exited before announcement ({status}): {}",
                String::from_utf8_lossy(&diagnostic)
            );
        }
        let announced: Value = serde_json::from_slice(&bytes).expect("botd announcement");
        assert_eq!(announced["mock_discord"], true);
        assert_eq!(announced["test_gateway_switch"], true);
        assert_eq!(announced["production_ready"], false);
        let mut fixture = Self {
            root,
            daemon,
            port: announced["port"].as_u64().unwrap().try_into().unwrap(),
            collector,
            gateways: Vec::new(),
            observed_pids: BTreeSet::new(),
        };
        for _ in KEYS {
            fixture.accept().await;
        }
        fixture
            .wait_status(|status| {
                status["shards"].as_array().is_some_and(|rows| {
                    rows.len() == 3 && rows.iter().all(|row| row["phase"] == "Stable")
                })
            })
            .await;
        fixture
    }

    async fn accept(&mut self) -> usize {
        let (stream, peer) = bounded(self.collector.accept()).await.unwrap();
        assert!(peer.ip().is_loopback());
        let (output, input) = stream.into_split();
        let mut gateway = Gateway {
            ready: Value::Null,
            input,
            output: BufReader::new(output),
        };
        gateway.ready = gateway.receive().await;
        assert_eq!(gateway.ready["type"], "ready");
        assert_eq!(gateway.ready["credential_present"], false);
        assert_eq!(gateway.ready["workers"].as_array().unwrap().len(), 1);
        assert_eq!(gateway.ready["workers"][0]["credential_free"], true);
        self.observed_pids
            .extend([gateway.pid(), gateway.core_pid()]);
        let index = self.gateways.len();
        self.gateways.push(gateway);
        index
    }

    fn gateway(&self, bot: u64, shard: u32) -> usize {
        self.gateways
            .iter()
            .rposition(|g| g.ready["bot_id"] == bot && g.ready["shard_id"] == shard)
            .unwrap()
    }

    async fn status(&self) -> Value {
        request(self.port, json!({"command":"status"})).await
    }

    async fn wait_status(&self, check: impl Fn(&Value) -> bool) -> Value {
        bounded(async {
            loop {
                let status = self.status().await;
                assert_eq!(status["ok"], true);
                single_active(&status);
                if check(&status) {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
    }

    async fn finish(mut self) {
        for key in KEYS {
            let id = next_id();
            let admitted = request(
                self.port,
                json!({"command":"stop","bot":key,"operation_id":id}),
            )
            .await;
            assert_eq!(admitted["ok"], true, "stop admission: {admitted}");
            let result = bounded(async {
                loop {
                    let result =
                        request(self.port, json!({"command":"operation","operation_id":id})).await;
                    if result["operation"]["state"] != "running" {
                        return result;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await;
            assert_eq!(
                result["operation"]["state"], "succeeded",
                "stop result: {result}"
            );
        }
        assert_exited(self.observed_pids.iter().copied().collect()).await;
        no_persisted_secrets(&self.root);
        self.daemon.kill().await.unwrap();
        if let Some(mut stderr) = self.daemon.stderr.take() {
            let mut bytes = Vec::new();
            bounded(stderr.read_to_end(&mut bytes)).await.unwrap();
            no_secrets(&bytes);
            assert!(bytes.is_empty(), "botd wrote unexpected diagnostics");
        }
        self.port = 0;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.port != 0 {
            for key in KEYS {
                let _ = blocking_request(
                    self.port,
                    json!({"command":"stop","bot":key,"operation_id":next_id()}),
                );
            }
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if blocking_request(self.port, json!({"command":"status"})).is_some_and(|status| {
                    status["shards"].as_array().is_some_and(|rows| {
                        rows.iter()
                            .all(|row| row["processes"].as_array().is_some_and(Vec::is_empty))
                    })
                }) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        let _ = self.daemon.start_kill();
        // Panic cleanup addresses only fixture-owned collector sockets. No PID
        // enumeration or broad name-based termination is used during cleanup.
        for gateway in std::mem::take(&mut self.gateways) {
            let Ok(stream) = gateway.input.reunite(gateway.output.into_inner()) else {
                continue;
            };
            let Ok(mut stream) = stream.into_std() else {
                continue;
            };
            if stream.set_nonblocking(false).is_ok()
                && stream
                    .set_write_timeout(Some(Duration::from_millis(500)))
                    .is_ok()
            {
                let _ = stream.write_all(b"{\"type\":\"crash\",\"id\":0}\n");
            }
        }
    }
}

async fn request(port: u16, mut value: Value) -> Value {
    value["secret"] = json!(SECRET);
    let mut stream = bounded(TcpStream::connect(("127.0.0.1", port)))
        .await
        .unwrap();
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bounded(stream.write_all(&bytes)).await.unwrap();
    let mut bytes = Vec::new();
    bounded(
        BufReader::new(stream)
            .take(64 * 1024 + 1)
            .read_until(b'\n', &mut bytes),
    )
    .await
    .unwrap();
    assert!(bytes.len() <= 64 * 1024);
    no_secrets(&bytes);
    serde_json::from_slice(&bytes).expect("authenticated daemon response")
}

fn blocking_request(port: u16, mut value: Value) -> Option<Value> {
    let address: SocketAddr = ([127, 0, 0, 1], port).into();
    let mut stream = BlockingStream::connect_timeout(&address, Duration::from_millis(300)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(500)))
        .ok()?;
    value["secret"] = json!(SECRET);
    let mut bytes = serde_json::to_vec(&value).ok()?;
    bytes.push(b'\n');
    stream.write_all(&bytes).ok()?;
    let mut bytes = Vec::new();
    std::io::BufReader::new(stream)
        .read_until(b'\n', &mut bytes)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn row<'a>(status: &'a Value, bot: &str, shard: u32) -> &'a Value {
    status["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["bot_id"] == bot && row["shard_id"] == shard)
        .unwrap()
}

fn single_active(status: &Value) {
    for row in status["shards"].as_array().unwrap() {
        assert!(
            row["processes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|process| process["state"] == "active")
                .count()
                <= 1
        );
    }
    for event in status["transitions"].as_array().unwrap() {
        assert!(event["active_count"].as_u64().unwrap() <= 1);
    }
}

/// The PID must have just been observed on this fixture's authenticated status
/// or owned mock connection. Verify its executable before injecting OS death.
async fn kill_observed(pid: u32, expected_executable: &'static str) {
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            let output = std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command",
                    "$target = Get-Process -Id ([int]$env:RECOVERY_TEST_PID) -ErrorAction Stop; if (-not [StringComparer]::OrdinalIgnoreCase.Equals($target.MainModule.FileName, $env:RECOVERY_TEST_EXE)) { throw 'unexpected fixture executable' }; $target.Kill(); if (-not $target.WaitForExit(5000)) { throw 'fixture did not exit' }"])
                .env("RECOVERY_TEST_PID", pid.to_string())
                .env("RECOVERY_TEST_EXE", expected_executable)
                .creation_flags(0x08000000)
                .output().expect("terminate the observed fixture process");
            assert!(output.status.success(), "fixture termination failed");
        }
        #[cfg(unix)]
        {
            #[cfg(target_os = "linux")]
            assert_eq!(std::fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
                std::fs::canonicalize(expected_executable).unwrap());
            #[cfg(not(target_os = "linux"))]
            let _ = expected_executable;
            assert!(std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap().success());
        }
    }).await.unwrap();
}

async fn assert_exited(pids: Vec<u32>) {
    bounded(async {
        loop {
            let observed = pids.clone();
            let exited = tokio::task::spawn_blocking(move || {
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    let ids = observed.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
                    let output = std::process::Command::new("powershell.exe")
                        .args(["-NoProfile", "-NonInteractive", "-Command"])
                        .arg(format!("Get-Process -Id @({ids}) -ErrorAction SilentlyContinue | ForEach-Object {{ $_.Id }}; 'probe-complete'"))
                        .creation_flags(0x08000000).output().unwrap();
                    assert!(output.status.success(), "read-only fixture process probe failed");
                    let text = String::from_utf8_lossy(&output.stdout);
                    assert_eq!(text.lines().last(), Some("probe-complete"));
                    text.lines().count() == 1
                }
                #[cfg(target_os = "linux")]
                { observed.iter().all(|pid| !Path::new(&format!("/proc/{pid}")).exists()) }
                #[cfg(all(unix, not(target_os = "linux")))]
                { observed.iter().all(|pid| !std::process::Command::new("kill")
                    .args(["-0", &pid.to_string()]).stdout(Stdio::null()).stderr(Stdio::null())
                    .status().unwrap().success()) }
            }).await.unwrap();
            if exited { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await;
}

#[tokio::test]
async fn core_os_death_recovers_without_gateway_epoch_connection_or_identify_change() {
    let _serial = SERIAL.acquire().await.unwrap();
    // Both Bot-wide Identify permits are consumed by its initial two Shards.
    let mut fixture = Fixture::new(2).await;
    let alpha = fixture.gateway(101, 0);
    let sibling = fixture.gateway(101, 1);
    let beta = fixture.gateway(202, 0);
    let before = fixture.status().await;
    let gateway_pid = fixture.gateways[alpha].pid();
    let old_core = fixture.gateways[alpha].ready["workers"][0].clone();
    let old_core_pid = fixture.gateways[alpha].core_pid();
    kill_observed(old_core_pid, env!("CARGO_BIN_EXE_botd")).await;

    // Status does not dispatch work to Core: recovery must run even while idle.
    // Every exchange uses the original connected socket; a reconnect cannot
    // masquerade as preservation of the Gateway's Discord connection.
    let recovered = bounded(async {
        loop {
            let status = fixture.gateways[alpha]
                .exchange(json!({"type":"status","id":2}))
                .await;
            assert_eq!(status["pid"], gateway_pid);
            if status["workers"][0]["pid"] != old_core["pid"] {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let core = &recovered["workers"][0];
    assert_eq!(core["credential_free"], true);
    assert_eq!(core["identity"]["bot_id"], old_core["identity"]["bot_id"]);
    assert_eq!(core["identity"]["shard"], old_core["identity"]["shard"]);
    assert_eq!(
        core["identity"]["ownership_epoch"],
        old_core["identity"]["ownership_epoch"]
    );
    assert!(
        core["identity"]["generation_id"].as_u64().unwrap()
            > old_core["identity"]["generation_id"].as_u64().unwrap()
    );
    assert_eq!(core["recovery"]["state"], "Running");
    assert_eq!(core["recovery"]["restarts"], 1);
    assert_eq!(core["recovery"]["ready"], true);
    let observations = core["recovery"]["recent_events"].as_array().unwrap();
    let core_events = [
        observations
            .iter()
            .map(|observation| &observation["event"])
            .find(|event| {
                event["key"]["bot_id"] == 101
                    && event["key"]["shard_id"] == 0
                    && event["key"]["role"] == "Core"
                    && event["generation"] == old_core["recovery"]["generation"]
                    && event["kind"]["Exited"]["success"] == false
            })
            .expect("Core recovery observed the killed generation's exit")
            .clone(),
        observations
            .iter()
            .map(|observation| &observation["event"])
            .find(|event| {
                event["key"]["bot_id"] == 101
                    && event["key"]["shard_id"] == 0
                    && event["key"]["role"] == "Core"
                    && event["generation"] == core["recovery"]["generation"]
                    && event["kind"]["Started"]["mode"] == "Core"
            })
            .expect("Core recovery observed the replacement start")
            .clone(),
    ];
    fixture
        .observed_pids
        .insert(core["pid"].as_u64().unwrap() as u32);
    fixture.gateways[alpha].ready["workers"] = recovered["workers"].clone();
    // The source's fixed events must reach both status consumers (including a
    // metrics adapter) and the daemon audit sink through the Gateway relay.
    let after = fixture
        .wait_status(|status| {
            core_events.iter().all(|event| {
                status["recovery_events"]
                    .as_array()
                    .is_some_and(|events| events.contains(event))
                    && status["transitions"].as_array().is_some_and(|transitions| {
                        transitions.iter().any(|transition| {
                            transition["event"] == "child_recovery"
                                && transition["recovery"] == *event
                        })
                    })
            })
        })
        .await;
    assert_eq!(after["recovery_audit"]["ok"], true);
    let audit = fixture.root.join("recovery-audit.jsonl");
    bounded(async {
        loop {
            if let Ok(bytes) = tokio::fs::read(&audit).await {
                no_secrets(&bytes);
                let records: Vec<Value> = bytes
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .filter_map(|line| serde_json::from_slice(line).ok())
                    .collect();
                if core_events.iter().all(|event| records.contains(event)) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert_eq!(row(&after, "101", 0)["phase"], "Stable");
    assert_eq!(
        row(&after, "101", 0)["active_generation"],
        row(&before, "101", 0)["active_generation"]
    );
    assert_eq!(
        row(&after, "101", 0)["processes"][0]["epoch"],
        row(&before, "101", 0)["processes"][0]["epoch"]
    );
    assert_eq!(row(&after, "101", 0)["processes"][0]["pid"], gateway_pid);
    for index in [alpha, sibling, beta] {
        fixture.gateways[index].ping().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.collector.accept())
            .await
            .is_err(),
        "Core restart unexpectedly created another Gateway connection"
    );
    assert_exited(vec![old_core_pid]).await;
    fixture.finish().await;
}

#[tokio::test]
async fn restart_budget_degraded_is_latched_and_emitted_to_status_events_and_audit() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut fixture = Fixture::configured(10, Some(0)).await;
    let alpha = fixture.gateway(101, 0);
    let sibling = fixture.gateway(101, 1);
    let beta = fixture.gateway(202, 0);
    let before = fixture.status().await;
    let pid = fixture.gateways[alpha].pid();
    kill_observed(pid, env!("CARGO_BIN_EXE_src")).await;
    let degraded = fixture
        .wait_status(|status| row(status, "101", 0)["recovery"]["state"] == "Degraded")
        .await;
    let shard = row(&degraded, "101", 0);
    assert_eq!(shard["phase"], "Degraded");
    assert_eq!(shard["recovery"]["reason"], "RestartBudget");
    assert_eq!(shard["recovery"]["restarts"], 0);
    assert!(shard["recovery"]["retry_at_ms"].is_null());
    assert!(shard["processes"].as_array().unwrap().is_empty());
    assert_eq!(
        shard["generation_high_water"],
        row(&before, "101", 0)["generation_high_water"]
    );

    // The identical fixed event can be consumed by a metrics adapter and by the
    // audit writer without exposing a command, token, process output or payload.
    let event = degraded["transitions"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|transition| {
            let event = &transition["recovery"];
            (event["key"]["bot_id"] == 101
                && event["key"]["shard_id"] == 0
                && event["key"]["role"] == "Gateway"
                && event["kind"]["Degraded"]["reason"] == "RestartBudget")
                .then(|| event.clone())
        })
        .expect("fixed Degraded event is observable");
    assert_eq!(event["generation"], shard["recovery"]["generation"]);
    let audit = fixture.root.join("recovery-audit.jsonl");
    bounded(async {
        loop {
            if let Ok(bytes) = tokio::fs::read(&audit).await {
                no_secrets(&bytes);
                let records: Vec<Value> = bytes
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .filter_map(|line| serde_json::from_slice(line).ok())
                    .collect();
                if records.iter().any(|record| record == &event) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fixture.collector.accept())
            .await
            .is_err(),
        "Degraded spawned a replacement despite the exhausted restart budget"
    );
    for index in [sibling, beta] {
        fixture.gateways[index].ping().await;
    }
    let later = fixture.status().await;
    assert_eq!(row(&later, "101", 0)["recovery"], shard["recovery"]);
    let bytes = tokio::fs::read(audit).await.unwrap();
    let records: Vec<Value> = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(
        records.iter().filter(|record| *record == &event).count(),
        1,
        "polling the latched state must not duplicate its Degraded audit event"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn gateway_os_death_recovers_then_waits_when_the_shared_bot_identify_budget_is_spent() {
    let _serial = SERIAL.acquire().await.unwrap();
    // Two starts plus one replacement. A fresh per-process budget would wrongly
    // allow a second replacement below, despite the shared Bot budget being 0.
    let mut fixture = Fixture::new(3).await;
    let old = fixture.gateway(101, 0);
    let sibling = fixture.gateway(101, 1);
    let beta = fixture.gateway(202, 0);
    let before = fixture.status().await;
    let old_pid = fixture.gateways[old].pid();
    let old_core = fixture.gateways[old].core_pid();
    kill_observed(old_pid, env!("CARGO_BIN_EXE_src")).await;
    let new = fixture.accept().await;
    assert_eq!(fixture.gateways[new].ready["bot_id"], 101);
    assert_eq!(fixture.gateways[new].ready["shard_id"], 0);
    assert_ne!(fixture.gateways[new].pid(), old_pid);
    assert_ne!(fixture.gateways[new].core_pid(), old_core);
    let restored = fixture
        .wait_status(|status| {
            row(status, "101", 0)["phase"] == "Stable"
                && row(status, "101", 0)["processes"][0]["pid"] == fixture.gateways[new].pid()
        })
        .await;
    assert!(
        row(&restored, "101", 0)["active_generation"]
            .as_u64()
            .unwrap()
            > row(&before, "101", 0)["active_generation"]
                .as_u64()
                .unwrap()
    );
    assert!(
        row(&restored, "101", 0)["processes"][0]["epoch"]
            .as_u64()
            .unwrap()
            > row(&before, "101", 0)["processes"][0]["epoch"]
                .as_u64()
                .unwrap()
    );
    assert_eq!(row(&restored, "101", 0)["recovery"]["restarts"], 1);
    for (bot, shard) in [("101", 1), ("202", 0)] {
        assert_eq!(
            row(&restored, bot, shard)["active_generation"],
            row(&before, bot, shard)["active_generation"]
        );
        assert_eq!(
            row(&restored, bot, shard)["processes"][0]["pid"],
            row(&before, bot, shard)["processes"][0]["pid"]
        );
    }
    for index in [new, sibling, beta] {
        fixture.gateways[index].ping().await;
    }
    assert_exited(vec![old_pid, old_core]).await;

    let replaced_pid = fixture.gateways[new].pid();
    kill_observed(replaced_pid, env!("CARGO_BIN_EXE_src")).await;
    let deferred = fixture
        .wait_status(|status| row(status, "101", 0)["identify"]["reason"] == "budget_exhausted")
        .await;
    assert_eq!(row(&deferred, "101", 0)["recovery"]["state"], "Waiting");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fixture.collector.accept())
            .await
            .is_err(),
        "Gateway spawned without a shared Identify permit"
    );
    for index in [sibling, beta] {
        fixture.gateways[index].ping().await;
    }
    fixture.finish().await;
}
