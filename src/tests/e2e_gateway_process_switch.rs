//! Real botd -> real Gateway -> shard Core, using explicit loopback simulation.
//! These assertions cover the observed local transitions, not live Discord,
//! distributed leases, persistent budgets, OS suspend, or exactly-once effects.
use serde_json::{Value, json};
use src::{
    placement::Config,
    shard_runtime,
    testkit::{MockDiscord, MockReply},
};
use std::{
    collections::{BTreeSet, HashMap},
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
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    process::Child,
};

const WAIT: Duration = Duration::from_secs(90);
const SECRET: [u8; 32] = [79; 32];
const SENTINELS: [&str; 3] = [
    "test-only-switch-alpha-credential",
    "test-only-switch-beta-credential",
    "test-only-switch-parent-credential",
];
static SERIAL: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
const CONFIG: &str = r#"
[cluster]
id = "gateway-process-switch"
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

async fn bounded<T>(line: u32, future: impl Future<Output = T>) -> T {
    match tokio::time::timeout(WAIT, future).await {
        Ok(value) => value,
        Err(_) => panic!("Gateway switch process deadline at line {line}"),
    }
}

fn directory() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("gateway-switch-process-tests")
        .join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn next_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!("{:032x}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn no_secrets(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    for sentinel in SENTINELS {
        assert!(!text.contains(sentinel), "credential reached output");
    }
    assert!(!text.contains("\"secret\""), "capability reached output");
}

fn budget(remaining: u64, concurrency: u64, interval_ms: u64) -> Value {
    json!([
        {"bot_id":101,"total":remaining,"remaining":remaining,"reset_after_ms":60000,
         "max_concurrency":concurrency,"interval_ms":interval_ms},
        {"bot_id":202,"total":10,"remaining":10,"reset_after_ms":60000,
         "max_concurrency":1,"interval_ms":50},
    ])
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
            line!(),
            (&mut self.output)
                .take(64 * 1024 + 1)
                .read_until(b'\n', &mut bytes),
        )
        .await
        .unwrap();
        assert!(!bytes.is_empty(), "Gateway collector ended unexpectedly");
        assert!(bytes.len() <= 64 * 1024);
        assert_eq!(bytes.last(), Some(&b'\n'));
        no_secrets(&bytes);
        serde_json::from_slice(&bytes).expect("Gateway collector JSON")
    }

    async fn send(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bounded(line!(), self.input.write_all(&bytes))
            .await
            .unwrap();
    }

    async fn exchange(&mut self, value: Value) -> Value {
        let id = value["id"].clone();
        self.send(value).await;
        let reply = self.receive().await;
        assert_eq!(reply["id"], id);
        reply
    }

    fn generation(&self) -> u64 {
        self.ready["gateway_generation"]
            .as_u64()
            .expect("Gateway generation")
    }

    fn pids(&self) -> Vec<u32> {
        vec![
            self.ready["pid"].as_u64().unwrap() as u32,
            self.ready["workers"][0]["pid"].as_u64().unwrap() as u32,
        ]
    }

    async fn command(&mut self, slash: bool, name: &str) -> Value {
        let replies = if slash {
            vec![
                MockReply::interaction_ack(),
                MockReply::json(200, b"{}").unwrap(),
            ]
        } else {
            vec![MockReply::json(200, b"{}").unwrap()]
        };
        let mock = MockDiscord::new(replies);
        let server = mock.spawn_tcp().await.unwrap();
        let mut request = json!({"type":if slash {"slash"} else {"prefix"},
            "id":1,"key":0,"rest_addr":server.addr().to_string()});
        request[if slash { "name" } else { "content" }] = json!(name);
        let reply = self.exchange(request).await;
        assert_eq!(reply["outcome"], "completed");
        assert_receipt(&reply, &self.ready);
        assert_eq!(mock.stats().requests, if slash { 2 } else { 1 });
        server.shutdown().await.unwrap();
        reply
    }

    async fn stopped(&mut self) {
        let reply = self.receive().await;
        assert_eq!(reply["type"], "stopped");
        assert_eq!(reply["pid"], self.ready["pid"]);
        let mut byte = [0];
        assert_eq!(
            bounded(line!(), self.output.read(&mut byte)).await.unwrap(),
            0
        );
    }
}

fn assert_receipt(reply: &Value, ready: &Value) {
    assert_eq!(reply["receipt"]["worker_index"], 0);
    assert_eq!(reply["receipt"]["pid"], ready["workers"][0]["pid"]);
    assert_eq!(
        reply["receipt"]["identity"],
        ready["workers"][0]["identity"]
    );
    assert_eq!(reply["receipt"]["outcome"]["kind"], "confirmed");
    assert_eq!(reply["receipt"]["outcome"]["status"], 200);
    assert_eq!(reply["unknown_responses"], 0);
}

struct Fixture {
    root: PathBuf,
    child: Child,
    port: u16,
    collector: TcpListener,
    gateways: Vec<Gateway>,
}

impl Fixture {
    async fn new(limits: Value) -> Self {
        let root = directory();
        let file = root.join("placement.toml");
        let budget = root.join("identify.json");
        std::fs::write(&file, CONFIG).unwrap();
        std::fs::write(&budget, serde_json::to_vec(&limits).unwrap()).unwrap();
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(file)
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
        for (name, value) in ["TOKEN_ALPHA", "TOKEN_BETA", "DISCORD_TOKEN"]
            .into_iter()
            .zip(SENTINELS)
        {
            command.env(name, value);
        }
        let mut child = command.spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret":SECRET})).unwrap();
        bootstrap.push(b'\n');
        bounded(line!(), child.stdin.take().unwrap().write_all(&bootstrap))
            .await
            .unwrap();
        let mut bytes = Vec::new();
        bounded(
            line!(),
            BufReader::new(child.stdout.take().unwrap()).read_until(b'\n', &mut bytes),
        )
        .await
        .unwrap();
        no_secrets(&bytes);
        let announced: Value = serde_json::from_slice(&bytes).expect("botd test announcement");
        assert_eq!(announced["mock_discord"], true);
        assert_eq!(announced["production_ready"], false);
        let mut fixture = Self {
            root,
            child,
            port: announced["port"].as_u64().unwrap() as u16,
            collector,
            gateways: Vec::new(),
        };
        for _ in 0..3 {
            fixture.accept().await;
        }
        fixture
            .wait_status(|status| {
                status["shards"].as_array().is_some_and(|rows| {
                    rows.len() == 3 && rows.iter().all(|row| row["phase"] == "Stable")
                })
            })
            .await;
        fixture.verify_layout();
        fixture
    }

    async fn request(&self, value: Value) -> Value {
        request(self.port, value).await
    }

    async fn accept(&mut self) -> usize {
        let accepted = tokio::time::timeout(WAIT, self.collector.accept()).await;
        let (stream, peer) = match accepted {
            Ok(connection) => connection.unwrap(),
            Err(_) => {
                // No Gateway connected in time: report what botd believes about its
                // shards so the cause (spawn failure, Degraded, waiting budget) is visible.
                let status = tokio::time::timeout(
                    Duration::from_secs(10),
                    self.request(json!({"command":"status"})),
                )
                .await
                .map(|value| value.to_string())
                .unwrap_or_else(|_| "botd status did not answer within 10s".into());
                panic!(
                    "no Gateway connected within {WAIT:?} ({} accepted so far); botd status: {status}",
                    self.gateways.len()
                );
            }
        };
        assert!(peer.ip().is_loopback());
        let (output, input) = stream.into_split();
        let mut gateway = Gateway {
            ready: Value::Null,
            input,
            output: BufReader::new(output),
        };
        gateway.ready = gateway.receive().await;
        assert_eq!(gateway.ready["type"], "ready");
        assert_eq!(gateway.ready["mode"], "mock_discord");
        assert_eq!(gateway.ready["credential_present"], false);
        assert_eq!(gateway.ready["workers"].as_array().unwrap().len(), 1);
        assert_eq!(gateway.ready["workers"][0]["credential_free"], true);
        let index = self.gateways.len();
        self.gateways.push(gateway);
        index
    }

    fn gateway(&self, bot: u64, shard: u32) -> usize {
        self.gateways
            .iter()
            .position(|gateway| {
                gateway.ready["bot_id"] == bot && gateway.ready["shard_id"] == shard
            })
            .unwrap()
    }

    fn verify_layout(&self) {
        let config = Config::load(&self.root.join("placement.toml")).unwrap();
        let plans = shard_runtime::plan(
            &config.local_primary_intents(&HashMap::new()).unwrap(),
            &self.root,
        )
        .unwrap();
        let mut pids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for gateway in &self.gateways {
            let plan = plans
                .iter()
                .find(|plan| {
                    gateway.ready["bot_id"] == plan.bot_id.parse::<u64>().unwrap()
                        && gateway.ready["shard_id"] == plan.shard_id
                })
                .unwrap();
            for pid in gateway.pids() {
                assert!(pids.insert(pid), "process shared across Shards");
            }
            assert_eq!(gateway.ready["total_shards"], plan.total_shards);
            let core = &gateway.ready["workers"][0];
            assert_eq!(core["identity"]["bot_id"], gateway.ready["bot_id"]);
            assert_eq!(
                core["identity"]["shard"],
                json!({"id":plan.shard_id,"total":plan.total_shards})
            );
            for (field, base) in [
                ("ipc_path", &plan.ipc_path),
                ("state_path", &plan.gateway.state_path),
                ("management_path", &plan.gateway.management_path),
            ] {
                let actual = PathBuf::from(gateway.ready[field].as_str().unwrap());
                assert!(actual.starts_with(base));
                assert!(
                    paths.insert(actual),
                    "Gateway generations share a runtime path"
                );
            }
        }
    }

    async fn begin(&self, command: &str, key: &str) -> String {
        let id = next_id();
        let reply = self
            .request(json!({"command":command,"bot":key,"operation_id":id}))
            .await;
        assert_eq!(reply["ok"], true, "safe operation response: {reply}");
        id
    }

    async fn operation(&self, id: &str) -> Value {
        let mut last = Value::Null;
        let waited = tokio::time::timeout(WAIT, async {
            loop {
                let reply = self
                    .request(json!({"command":"operation","operation_id":id}))
                    .await;
                assert_eq!(reply["ok"], true, "safe operation status: {reply}");
                if reply["operation"]["state"] != "running" {
                    return reply;
                }
                last = reply;
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await;
        match waited {
            Ok(reply) => reply,
            Err(_) => panic!("operation {id} still running at deadline; last reply: {last}"),
        }
    }

    async fn wait_status(&self, check: impl Fn(&Value) -> bool) -> Value {
        let mut last = Value::Null;
        let waited = tokio::time::timeout(WAIT, async {
            loop {
                let status = self.request(json!({"command":"status"})).await;
                assert_eq!(status["ok"], true);
                single_active(&status);
                if check(&status) {
                    return status;
                }
                last = status;
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await;
        match waited {
            Ok(status) => status,
            Err(_) => panic!("status condition not met at deadline; last status: {last}"),
        }
    }

    async fn finish(mut self) {
        for key in ["101/0", "101/1", "202/0"] {
            let status = self.request(json!({"command":"status"})).await;
            let (bot, shard) = key.split_once('/').unwrap();
            if row(&status, bot, shard.parse().unwrap())["phase"] == "Stopped" {
                continue;
            }
            let id = self.begin("stop", key).await;
            let result = self.operation(&id).await;
            assert_eq!(
                result["operation"]["state"], "succeeded",
                "stop result: {result}"
            );
        }
        let pids: Vec<_> = self.gateways.iter().flat_map(Gateway::pids).collect();
        assert_exited(&pids).await;
        self.child.kill().await.unwrap();
        if let Some(mut stderr) = self.child.stderr.take() {
            let mut bytes = Vec::new();
            bounded(line!(), stderr.read_to_end(&mut bytes))
                .await
                .unwrap();
            no_secrets(&bytes);
            assert!(bytes.is_empty(), "botd wrote unexpected diagnostics");
        }
        self.port = 0;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.port != 0 {
            for key in ["101/0", "101/1", "202/0"] {
                let _ = blocking_request(
                    self.port,
                    json!({"command":"stop","bot":key,"operation_id":next_id()}),
                );
            }
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if blocking_request(self.port, json!({"command":"status"})).is_some_and(|value| {
                    value["shards"].as_array().is_some_and(|rows| {
                        rows.iter()
                            .all(|row| row["processes"].as_array().is_some_and(Vec::is_empty))
                    })
                }) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.child.start_kill();
        // Only these owned collector sockets can receive this test-only crash.
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
    let mut stream = bounded(line!(), TcpStream::connect(("127.0.0.1", port)))
        .await
        .unwrap();
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bounded(line!(), stream.write_all(&bytes)).await.unwrap();
    let mut bytes = Vec::new();
    bounded(
        line!(),
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
                <= 1,
            "observed two Active processes: {row}"
        );
    }
    for event in status["transitions"].as_array().unwrap() {
        assert!(
            event["active_count"].as_u64().unwrap() <= 1,
            "acknowledged two Active processes: {event}"
        );
    }
}

async fn live_pids(pids: &[u32]) -> BTreeSet<u32> {
    let pids = pids.to_vec();
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            let ids = pids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
            let output = std::process::Command::new("powershell.exe")
                .args(["-NoProfile","-NonInteractive","-Command"])
                .arg(format!("Get-Process -ErrorAction Stop | Where-Object {{ $_.Id -in @({ids}) }} | ForEach-Object {{ $_.Id }}; 'probe-complete'"))
                .creation_flags(0x08000000).output().expect("read-only process probe");
            assert!(output.status.success() && output.stderr.is_empty(), "process probe failed");
            let text = String::from_utf8_lossy(&output.stdout);
            assert_eq!(text.lines().last(), Some("probe-complete"));
            text.lines().filter_map(|line| line.parse().ok()).collect()
        }
        #[cfg(target_os="linux")]
        { pids.into_iter().filter(|pid| Path::new(&format!("/proc/{pid}")).exists()).collect() }
        #[cfg(all(unix,not(target_os="linux")))]
        { pids.into_iter().filter(|pid| std::process::Command::new("kill").args(["-0",&pid.to_string()])
            .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap().success()).collect() }
    }).await.expect("process probe task")
}

async fn assert_exited(pids: &[u32]) {
    bounded(line!(), async {
        while !live_pids(pids).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
}

async fn update_plan(root: &Path, label: &str, current: u64, candidate: u64) -> PathBuf {
    let program = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let digest = tokio::task::spawn_blocking({
        let program = program.clone();
        move || {
            let bytes = std::fs::read(program).unwrap();
            let digest = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
                .tls13()
                .unwrap()
                .common
                .hash_provider
                .hash(&bytes);
            digest
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        }
    })
    .await
    .unwrap();
    let plan = root.join(format!("{label}-plan.json"));
    std::fs::write(&plan, serde_json::to_vec(&json!({"bot_id":"101","bot_name":"alpha","total_shards":2,
        "shards":[{"shard_id":0,"gateway":{"current":current,"candidate":candidate}}],
        "artifacts":{"gateway":{"path":program,"sha256":digest},
            "core":{"path":"unselected-relative-core","sha256":"a".repeat(64)}},
        "identify":{"limits":{"max_concurrency":1,"remaining":8,"total":10,"reset_after":60000},"observed_age_ms":0}})).unwrap()).unwrap();
    plan
}

async fn update_cli(port: u16, command: &str, id: &str, plan: &Path, success: bool) -> Value {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_botctl"))
        .args([
            command,
            "alpha",
            "--component",
            "gateway",
            "--yes",
            "--bootstrap-stdin",
            "--json",
            "--timeout",
            "15s",
            "--operation-id",
            id,
            "--plan",
        ])
        .arg(plan)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("DISCORD_TOKEN", SENTINELS[2])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let bootstrap = format!("{}\n", json!({"port":port,"secret":SECRET}));
    bounded(
        line!(),
        child.stdin.take().unwrap().write_all(bootstrap.as_bytes()),
    )
    .await
    .unwrap();
    let output = bounded(line!(), child.wait_with_output()).await.unwrap();
    no_secrets(&output.stdout);
    no_secrets(&output.stderr);
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.success(), success, "CLI result: {value}");
    assert_eq!(value["ok"], success);
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_botctl_update_and_rollback_execute_real_gateways_and_audit_final_generations() {
    let _serial = SERIAL.acquire().await.unwrap();
    // A single Identify bucket deliberately leaves the admitted update waiting
    // while the daemon and the other Bot must remain responsive.
    let mut fixture = Fixture::new(budget(10, 1, 800)).await;
    for command in ["status", "shards"] {
        let selected = fixture
            .request(json!({"command":command,"bot":"alpha"}))
            .await;
        assert_eq!(selected["shards"].as_array().unwrap().len(), 2);
        assert!(
            selected["shards"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["bot_id"] == "101")
        );
        assert!(
            selected["transitions"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["bot_id"] == "101")
        );
    }
    let sibling = fixture.gateway(101, 1);
    let beta = fixture.gateway(202, 0);
    let sibling_pid = fixture.gateways[sibling].ready["pid"].clone();
    let beta_pid = fixture.gateways[beta].ready["pid"].clone();
    let mut old = fixture.gateway(101, 0);
    let mut ids = Vec::new();
    for (command, current, candidate) in [("update", 1, 2), ("rollback", 2, 3)] {
        let plan = update_plan(&fixture.root, command, current, candidate).await;
        let id = next_id();
        let port = fixture.port;
        let task_id = id.clone();
        let task_plan = plan.clone();
        let task =
            tokio::spawn(
                async move { update_cli(port, command, &task_id, &task_plan, true).await },
            );
        bounded(line!(), async {
            loop {
                let status = fixture
                    .request(json!({"command":"operation","operation_id":id}))
                    .await;
                if status["operation"]["state"] == "running" {
                    break;
                }
                assert!(
                    !task.is_finished(),
                    "typed operation never exposed running state"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let status = tokio::time::timeout(
            Duration::from_secs(1),
            fixture.request(json!({"command":"status"})),
        )
        .await
        .expect("update blocked daemon monitoring");
        single_active(&status);
        assert_eq!(row(&status, "101", 1)["processes"][0]["pid"], sibling_pid);
        assert_eq!(row(&status, "202", 0)["processes"][0]["pid"], beta_pid);
        assert_eq!(
            fixture.gateways[beta].command(false, "!ping").await["content"],
            "Pong! 🦀"
        );
        let new = fixture.accept().await;
        assert_eq!(fixture.gateways[new].generation(), candidate);
        let result = bounded(line!(), task).await.unwrap();
        assert_eq!(result["operation"]["command"], command);
        assert_eq!(result["operation"]["bot_id"], "101");
        assert_eq!(result["operation"]["state"], "succeeded");
        assert_eq!(result["operation"]["failed_shards"], json!([]));
        assert_eq!(
            result["operation"]["final_generations"],
            json!([{"shard_id":0,"gateway":candidate}])
        );
        assert_eq!(
            result["operation"]["shards"][0]["key"],
            json!({"bot_id":"101","shard_id":0})
        );
        assert!(result["operation"]["elapsed_ms"].is_u64());
        fixture.gateways[old].stopped().await;
        assert_exited(&fixture.gateways[old].pids()).await;
        assert_eq!(
            fixture.gateways[new].command(true, "ping").await["content"],
            "Pong! 🦀"
        );
        let replay = update_cli(fixture.port, command, &id, &plan, true).await;
        assert_eq!(
            replay["operation"], result["operation"],
            "replay changed terminal execution result"
        );
        ids.push(id);
        old = new;
    }
    // The stale manifest is valid offline but must be rejected against the
    // server's live generation before another candidate is launched.
    let stale = update_plan(&fixture.root, "stale", 1, 2).await;
    let stale_id = next_id();
    let rejected = update_cli(fixture.port, "update", &stale_id, &stale, false).await;
    assert_eq!(rejected["operation"]["state"], "failed");
    assert_eq!(
        rejected["operation"]["failed_shards"],
        json!([{"bot_id":"101","shard_id":0}])
    );
    let status = fixture.request(json!({"command":"status"})).await;
    single_active(&status);
    assert_eq!(row(&status, "101", 0)["active_generation"], 3);
    assert_eq!(
        row(&status, "101", 0)["processes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fixture.collector.accept())
            .await
            .is_err(),
        "stale admission launched a process"
    );
    let audit_path = fixture.root.join("update-audit.jsonl");
    // The exclusive audit writer intentionally holds a mandatory file lock on
    // Windows. Stop/reap the owner before inspecting its persisted records.
    fixture.finish().await;
    let audit = std::fs::read(audit_path).unwrap();
    no_secrets(&audit);
    let records: Vec<Value> = String::from_utf8(audit)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    for (id, command) in ids.iter().zip(["update", "rollback"]) {
        let correlated: Vec<_> = records
            .iter()
            .filter(|r| r["operation_id"] == *id)
            .collect();
        assert_eq!(correlated.len(), 2, "replay produced extra audit execution");
        assert_eq!(correlated[0]["result"], "started");
        assert_eq!(correlated[1]["result"], "succeeded");
        assert_eq!(correlated[0]["request_id"], correlated[1]["request_id"]);
        assert_eq!(correlated[1]["operation"], command);
        assert_eq!(correlated[1]["role"], "admin");
        assert_eq!(correlated[1]["bot_id"], "101");
    }
    let failed: Vec<_> = records
        .iter()
        .filter(|r| r["operation_id"] == stale_id)
        .collect();
    assert_eq!(failed.len(), 2);
    assert_eq!(failed[0]["result"], "started");
    assert_eq!(failed[1]["result"], "failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_real_gateway_is_standby_then_switch_preserves_old_rest_origin() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut fixture = Fixture::new(budget(10, 2, 50)).await;
    assert!(
        live_pids(&[std::process::id()])
            .await
            .contains(&std::process::id()),
        "process probe must positively detect this test"
    );
    let old = fixture.gateway(101, 0);
    let sibling = fixture.gateway(101, 1);
    let other_bot = fixture.gateway(202, 0);
    for index in [old, sibling, other_bot] {
        let prefix = fixture.gateways[index].command(false, "!ping").await;
        let slash = fixture.gateways[index].command(true, "ping").await;
        assert_eq!(prefix["content"], "Pong! 🦀");
        assert_eq!(prefix["content"], slash["content"]);
        assert_eq!(
            fixture.gateways[index]
                .command(false, "!state_counter")
                .await["content"],
            "State counter: 1"
        );
    }

    // A result can arrive while the relay has consumed only part of the next
    // input frame. Cancelling a select branch must not discard those bytes.
    let fragmented = MockDiscord::new(vec![
        MockReply::json(200, b"{}")
            .unwrap()
            .with_delay(Duration::from_millis(150)),
    ]);
    let endpoint = fragmented.spawn_tcp().await.unwrap();
    fixture.gateways[old]
        .send(json!({"type":"prefix","id":25,"key":0,"content":"!ping",
        "rest_addr":endpoint.addr().to_string()}))
        .await;
    bounded(line!(), async {
        while fragmented.stats().rest_requests != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    fixture.gateways[old]
        .input
        .write_all(b"{\"type\":\"sta")
        .await
        .unwrap();
    let result = fixture.gateways[old].receive().await;
    assert_eq!(result["id"], 25);
    assert_receipt(&result, &fixture.gateways[old].ready);
    fixture.gateways[old]
        .input
        .write_all(b"tus\",\"id\":26}\n")
        .await
        .unwrap();
    let status = fixture.gateways[old].receive().await;
    assert_eq!(status["type"], "status");
    assert_eq!(status["id"], 26);
    endpoint.shutdown().await.unwrap();

    let prepare = fixture.begin("gateway-prepare", "101/0").await;
    let candidate = fixture.accept().await;
    assert_eq!(fixture.gateways[candidate].ready["bot_id"], 101);
    assert_eq!(fixture.gateways[candidate].ready["shard_id"], 0);
    assert_eq!(
        fixture.operation(&prepare).await["operation"]["state"],
        "succeeded"
    );
    let new_generation = fixture.gateways[candidate].generation();
    let old_generation = fixture.gateways[old].generation();
    assert!(new_generation > old_generation);
    let status = fixture
        .wait_status(|status| row(status, "101", 0)["phase"] == "Warming")
        .await;
    let processes = row(&status, "101", 0)["processes"].as_array().unwrap();
    assert_eq!(processes.len(), 2);
    assert!(
        processes
            .iter()
            .any(|p| p["generation"] == old_generation && p["state"] == "active")
    );
    assert!(
        processes
            .iter()
            .any(|p| p["generation"] == new_generation && p["state"] == "standby")
    );
    let both: Vec<_> = fixture.gateways[old]
        .pids()
        .into_iter()
        .chain(fixture.gateways[candidate].pids())
        .collect();
    assert_eq!(
        live_pids(&both).await,
        both.iter().copied().collect(),
        "old and Standby Gateway/Core must coexist as real processes"
    );
    fixture.verify_layout();

    // A warm collector connection does not authorize command execution or
    // Interaction ACK. Both input forms are suppressed before any HTTP side effect.
    let suppressed = MockDiscord::new(vec![MockReply::json(200, b"{}").unwrap()]);
    let endpoint = suppressed.spawn_tcp().await.unwrap();
    for slash in [false, true] {
        let mut input = json!({"type":if slash {"slash"} else {"prefix"},"id":10,
            "key":0,"rest_addr":endpoint.addr().to_string()});
        input[if slash { "name" } else { "content" }] = json!(if slash { "ping" } else { "!ping" });
        assert_eq!(
            fixture.gateways[candidate].exchange(input).await["type"],
            "suppressed"
        );
    }
    assert_eq!(suppressed.stats().requests, 0);
    endpoint.shutdown().await.unwrap();

    // The candidate is already warm before the old REST starts. This keeps the
    // overlap within the real Gateway's existing two-second mock HTTP deadline.
    let pending = MockDiscord::new(vec![
        MockReply::json(200, b"{}")
            .unwrap()
            .with_delay(Duration::from_millis(1300)),
    ]);
    let endpoint = pending.spawn_tcp().await.unwrap();
    fixture.gateways[old]
        .send(json!({"type":"prefix","id":20,"key":0,"content":"!ping",
        "rest_addr":endpoint.addr().to_string()}))
        .await;
    bounded(line!(), async {
        while pending.stats().rest_requests != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let update = fixture.begin("gateway-update", "101/0").await;
    let status = fixture
        .wait_status(|status| {
            let shard = row(status, "101", 0);
            shard["active_generation"] == new_generation && shard["phase"] == "DrainingOld"
        })
        .await;
    let history = status["transitions"].as_array().unwrap();
    let paused = history
        .iter()
        .position(|e| {
            e["event"] == "paused"
                && e["generation"] == old_generation
                && e["bot_id"] == "101"
                && e["shard_id"] == 0
        })
        .unwrap();
    let activated = history
        .iter()
        .position(|e| {
            e["event"] == "active"
                && e["generation"] == new_generation
                && e["bot_id"] == "101"
                && e["shard_id"] == 0
        })
        .unwrap();
    assert!(
        paused < activated,
        "Activate preceded the real Pause acknowledgement"
    );
    assert_eq!(history[paused]["active_count"], 0);
    assert_eq!(history[activated]["active_count"], 1);
    assert_eq!(
        row(&status, "101", 0)["processes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        fixture.gateways[candidate].command(true, "ping").await["content"],
        "Pong! 🦀"
    );
    let result = fixture.gateways[old].receive().await;
    assert_eq!(result["id"], 20);
    assert_eq!(result["outcome"], "completed");
    assert_receipt(&result, &fixture.gateways[old].ready);
    fixture.gateways[old].stopped().await;
    assert_eq!(
        fixture.operation(&update).await["operation"]["state"],
        "succeeded"
    );
    assert_exited(&fixture.gateways[old].pids()).await;
    endpoint.shutdown().await.unwrap();
    for index in [sibling, other_bot] {
        assert_eq!(
            fixture.gateways[index]
                .command(false, "!state_counter")
                .await["content"],
            "State counter: 2"
        );
    }
    assert_eq!(
        fixture.gateways[candidate]
            .command(false, "!state_counter")
            .await["content"],
        "State counter: 1"
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bot_budget_is_shared_by_shards_and_does_not_block_another_bot() {
    let _serial = SERIAL.acquire().await.unwrap();
    // The two initial Alpha Shards consume its entire Bot-wide budget.
    let mut fixture = Fixture::new(budget(2, 2, 50)).await;
    let first = fixture.begin("gateway-update", "101/0").await;
    let second = fixture.begin("gateway-update", "101/1").await;
    fixture
        .wait_status(|status| {
            [0, 1].into_iter().all(|shard| {
                row(status, "101", shard)["phase"] == "Deferred"
                    && row(status, "101", shard)["identify"]["reason"] == "budget_exhausted"
            })
        })
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), fixture.collector.accept())
            .await
            .is_err(),
        "a Gateway started without an Identify reservation"
    );
    for shard in [0, 1] {
        let index = fixture.gateway(101, shard);
        assert_eq!(
            fixture.gateways[index].command(false, "!ping").await["content"],
            "Pong! 🦀"
        );
    }
    for id in [&first, &second] {
        assert_eq!(
            fixture
                .request(json!({"command":"operation","operation_id":id}))
                .await["operation"]["state"],
            "running"
        );
    }
    let beta = fixture.gateway(202, 0);
    let update = fixture.begin("gateway-update", "202/0").await;
    let candidate = fixture.accept().await;
    assert_eq!(fixture.gateways[candidate].ready["bot_id"], 202);
    assert_eq!(
        fixture.operation(&update).await["operation"]["state"],
        "succeeded"
    );
    fixture.gateways[beta].stopped().await;
    assert_eq!(
        fixture.gateways[candidate].command(false, "!ping").await["content"],
        "Pong! 🦀"
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identify_bucket_defers_a_second_shard_without_pausing_the_old_gateway() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut fixture = Fixture::new(budget(10, 1, 1500)).await;
    // Both Shards hash to the same Identify bucket. Starting either of them
    // reserves that bucket for the other, including a later rolling update.
    let update = fixture.begin("gateway-prepare", "101/0").await;
    let status = fixture
        .wait_status(|status| {
            row(status, "101", 0)["phase"] == "Deferred"
                && row(status, "101", 0)["identify"]["reason"] == "rate_limited"
        })
        .await;
    assert!(
        row(&status, "101", 0)["identify"]["retry_after_ms"]
            .as_u64()
            .unwrap()
            > 0
    );
    let original = fixture.gateway(101, 0);
    assert_eq!(
        row(&status, "101", 0)["active_generation"],
        fixture.gateways[original].generation()
    );
    assert_eq!(
        fixture.gateways[original].command(false, "!ping").await["content"],
        "Pong! 🦀"
    );
    let candidate = fixture.accept().await;
    assert_eq!(fixture.gateways[candidate].ready["bot_id"], 101);
    assert_eq!(fixture.gateways[candidate].ready["shard_id"], 0);
    assert_eq!(
        fixture.operation(&update).await["operation"]["state"],
        "succeeded"
    );
    fixture
        .wait_status(|status| row(status, "101", 0)["phase"] == "Warming")
        .await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standby_candidate_crash_reaps_it_and_preserves_the_old_active_core() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut fixture = Fixture::new(budget(10, 2, 50)).await;
    let old = fixture.gateway(101, 0);
    let old_generation = fixture.gateways[old].generation();
    assert_eq!(
        fixture.gateways[old].command(false, "!state_counter").await["content"],
        "State counter: 1"
    );
    let prepare = fixture.begin("gateway-prepare", "101/0").await;
    let candidate = fixture.accept().await;
    assert_eq!(
        fixture.operation(&prepare).await["operation"]["state"],
        "succeeded"
    );
    fixture
        .wait_status(|status| row(status, "101", 0)["phase"] == "Warming")
        .await;
    fixture.gateways[candidate]
        .send(json!({"type":"crash","id":41}))
        .await;
    let status = fixture
        .wait_status(|status| {
            let shard = row(status, "101", 0);
            shard["phase"] == "Stable"
                && shard["active_generation"] == old_generation
                && shard["processes"].as_array().unwrap().len() == 1
        })
        .await;
    assert_eq!(
        row(&status, "101", 0)["processes"][0]["pid"],
        fixture.gateways[old].ready["pid"]
    );
    assert_exited(&fixture.gateways[candidate].pids()).await;
    assert_eq!(
        fixture.gateways[old].command(false, "!state_counter").await["content"],
        "State counter: 2"
    );
    assert_eq!(
        fixture.gateways[old].command(true, "ping").await["content"],
        "Pong! 🦀"
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_drain_keeps_unknown_outcome_and_stop_reaps_every_owned_generation() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut fixture = Fixture::new(budget(10, 2, 50)).await;
    let old = fixture.gateway(101, 0);
    let prepare = fixture.begin("gateway-prepare", "101/0").await;
    let candidate = fixture.accept().await;
    assert_eq!(
        fixture.operation(&prepare).await["operation"]["state"],
        "succeeded"
    );

    let lost = MockDiscord::new(vec![
        MockReply::json(200, b"{}").unwrap().with_lost_response(),
    ]);
    let endpoint = lost.spawn_tcp().await.unwrap();
    let response = fixture.gateways[old]
        .exchange(json!({"type":"prefix","id":51,"key":0,"content":"!ping",
            "rest_addr":endpoint.addr().to_string()}))
        .await;
    assert_eq!(response["outcome"], "unknown");
    assert_eq!(response["unknown_responses"], 1);
    assert_eq!(
        response["receipt"]["identity"],
        fixture.gateways[old].ready["workers"][0]["identity"]
    );
    assert_eq!(response["receipt"]["outcome"]["kind"], "unknown");
    assert_eq!(lost.stats().rest_requests, 1, "unknown POST was retried");

    // The old Manager closes/reaps Core but rejects successful drain while an
    // external result is unknown. Its control connection then closes. Stop must
    // still send the owned Shutdown and collect both generations; an error from
    // the first dead control channel must not skip the remaining live child.
    let update = fixture.begin("gateway-update", "101/0").await;
    assert_eq!(
        fixture.operation(&update).await["operation"]["state"],
        "failed"
    );
    fixture
        .wait_status(|status| row(status, "101", 0)["phase"] == "RecoveryRequired")
        .await;
    let stop = fixture.begin("stop", "101/0").await;
    let result = fixture.operation(&stop).await;
    assert_eq!(result["operation"]["state"], "failed");
    assert!(result["operation"]["error"].is_string());
    fixture
        .wait_status(|status| {
            row(status, "101", 0)["phase"] == "Stopped"
                && row(status, "101", 0)["processes"]
                    .as_array()
                    .unwrap()
                    .is_empty()
        })
        .await;
    let pids: Vec<_> = fixture.gateways[old]
        .pids()
        .into_iter()
        .chain(fixture.gateways[candidate].pids())
        .collect();
    assert_exited(&pids).await;
    endpoint.shutdown().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_entry_rejects_missing_mock_or_budget_and_quorum_configuration() {
    let _serial = SERIAL.acquire().await.unwrap();
    let root = directory();
    let file = root.join("placement.toml");
    let limits = root.join("identify.json");
    std::fs::write(&file, CONFIG).unwrap();
    std::fs::write(&limits, serde_json::to_vec(&budget(10, 2, 50)).unwrap()).unwrap();
    // Every case must reject before a Gateway can use a production connection.
    for case in 0..4 {
        let expected = [
            "test Gateway switch requires --mock-discord",
            "test Gateway switch requires --mock-identify-budget",
            "mock Identify budget requires --allow-test-gateway-switch",
            "test Gateway switch requires standalone configuration",
        ][case];
        std::fs::write(
            &file,
            if case == 3 {
                CONFIG.replace("standalone", "quorum")
            } else {
                CONFIG.to_owned()
            },
        )
        .unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(&file)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .arg("--bootstrap-stdin")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if case != 2 {
            command.arg("--allow-test-gateway-switch");
        }
        if case != 0 {
            command.args(["--mock-discord", "127.0.0.1:1"]);
        }
        if case != 1 {
            command.arg("--mock-identify-budget").arg(&limits);
        }
        let output = bounded(line!(), command.output()).await.unwrap();
        assert!(
            !output.status.success(),
            "incomplete test-only opt-in accepted"
        );
        assert!(
            output.stdout.is_empty(),
            "rejected configuration announced a daemon"
        );
        no_secrets(&output.stderr);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "configuration was rejected for a different reason than the required opt-in guard"
        );
    }
}
