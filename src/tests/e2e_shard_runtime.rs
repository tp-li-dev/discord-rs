//! Real botd -> real src Gateway -> real Worker processes, without Discord.
//! The collector and REST endpoint are loopback-only testkit fixtures. These
//! tests do not establish distributed ownership, live Discord or TLS guarantees.
use serde_json::{Value, json};
use src::{
    placement::Config,
    shard_runtime::{self, ShardRuntimeSpec},
    testkit::{MockDiscord, MockReply},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
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

const SECRET: [u8; 32] = [61; 32];
const CONFIG: &str = r#"
[cluster]
id = "shard-e2e"
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
const SENTINELS: &[&str] = &[
    "synthetic-alpha-credential",
    "synthetic-beta-credential",
    "synthetic-parent-credential",
];

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("real shard integration deadline")
}

fn directory() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("e2e-shard-tests");
    std::fs::create_dir_all(&root).unwrap();
    let root = root.join(format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root
}

fn next_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!("{:032x}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn check_no_credentials(bytes: &[u8]) {
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

fn check_files(directory: &Path) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            check_files(&entry.path());
        } else {
            check_no_credentials(&std::fs::read(entry.path()).unwrap());
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
        assert!(
            !bytes.is_empty(),
            "Gateway closed the collector unexpectedly"
        );
        assert!(bytes.len() <= 64 * 1024);
        assert_eq!(bytes.last(), Some(&b'\n'));
        check_no_credentials(&bytes);
        serde_json::from_slice(&bytes).expect("bounded collector JSON")
    }

    async fn send(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bounded(self.input.write_all(&bytes)).await.unwrap();
    }

    async fn exchange(&mut self, value: Value) -> Value {
        let id = value["id"].clone();
        self.send(value).await;
        let response = self.receive().await;
        assert_eq!(response["id"], id);
        response
    }

    fn pid(&self) -> u32 {
        self.ready["pid"].as_u64().unwrap().try_into().unwrap()
    }

    fn worker_pids(&self) -> Vec<u32> {
        self.ready["workers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|worker| worker["pid"].as_u64().unwrap().try_into().unwrap())
            .collect()
    }

    async fn command(&mut self, slash: bool, key: u64, text: &str) -> Value {
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
        let mut request = json!({
            "type": if slash { "slash" } else { "prefix" },
            "id": 1,
            "key": key,
            "rest_addr": server.addr().to_string(),
        });
        request[if slash { "name" } else { "content" }] = json!(text);
        let response = self.exchange(request).await;
        assert_eq!(response["type"], "result");
        assert_eq!(response["outcome"], "completed");
        assert_eq!(response["unknown_responses"], 0);
        assert_receipt(&response, &self.ready, key, "confirmed");
        assert_eq!(response["receipt"]["outcome"]["status"], 200);
        let stats = mock.stats();
        assert_eq!(stats.requests, if slash { 2 } else { 1 });
        assert_eq!(stats.interaction_requests, u64::from(slash));
        assert_eq!(stats.rest_requests, 1);
        assert_eq!(stats.rejected_requests, 0);
        assert_eq!(stats.script_exhausted, 0);
        assert_eq!(mock.remaining(), 0);
        server.shutdown().await.unwrap();
        response
    }

    async fn stopped(&mut self) {
        let stopped = self.receive().await;
        assert_eq!(stopped["type"], "stopped");
        assert_eq!(stopped["pid"], self.pid());
        let actual: HashSet<_> = stopped["worker_pids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pid| pid.as_u64().unwrap() as u32)
            .collect();
        assert_eq!(actual, self.worker_pids().into_iter().collect());
        let mut byte = [0u8; 1];
        assert_eq!(bounded(self.output.read(&mut byte)).await.unwrap(), 0);
    }
}

fn assert_receipt(response: &Value, ready: &Value, key: u64, outcome: &str) {
    let receipt = &response["receipt"];
    let workers = ready["workers"].as_array().unwrap();
    let index = ((key ^ (key >> 22)) % workers.len() as u64) as usize;
    assert_eq!(receipt["worker_index"], index);
    assert_eq!(receipt["pid"], workers[index]["pid"]);
    assert_eq!(receipt["identity"], workers[index]["identity"]);
    assert_eq!(receipt["outcome"]["kind"], outcome);
    assert!(receipt["operation_id"].is_u64());
}

struct Fixture {
    root: PathBuf,
    plan: Vec<ShardRuntimeSpec>,
    child: Child,
    stderr: Option<tokio::process::ChildStderr>,
    port: u16,
    collector: TcpListener,
    gateways: BTreeMap<(u64, u32), Gateway>,
}

impl Fixture {
    async fn new() -> Self {
        Self::configured(true).await
    }

    async fn configured(poison_credentials: bool) -> Self {
        let root = directory();
        let file = root.join("placement.toml");
        std::fs::write(&file, CONFIG).unwrap();
        let config = Config::load(&file).unwrap();
        let plan = shard_runtime::plan(
            &config.local_primary_intents(&HashMap::new()).unwrap(),
            &root,
        )
        .unwrap();
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(file)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .args(["--bootstrap-stdin", "--no-autostart", "--mock-discord"])
            .arg(collector.local_addr().unwrap().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (name, sentinel) in ["TOKEN_ALPHA", "TOKEN_BETA", "DISCORD_TOKEN"]
            .into_iter()
            .zip(SENTINELS)
        {
            if poison_credentials {
                // Deliberately poison the daemon's inherited credentials. Mock
                // Gateways and all Core children must receive none of these.
                command.env(name, sentinel);
            } else {
                command.env_remove(name);
            }
        }
        let mut child = command.spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret": SECRET.to_vec()})).unwrap();
        bootstrap.push(b'\n');
        bounded(child.stdin.take().unwrap().write_all(&bootstrap))
            .await
            .unwrap();
        let mut bytes = Vec::new();
        bounded(BufReader::new(child.stdout.take().unwrap()).read_until(b'\n', &mut bytes))
            .await
            .unwrap();
        check_no_credentials(&bytes);
        let value: Value = serde_json::from_slice(&bytes).expect("botd announcement");
        assert_eq!(value["startup_ok"], true);
        assert_eq!(value["local_only"], true);
        let stderr = child.stderr.take();
        Self {
            root,
            plan,
            child,
            stderr,
            port: value["port"].as_u64().unwrap().try_into().unwrap(),
            collector,
            gateways: BTreeMap::new(),
        }
    }

    async fn request(&self, mut request: Value) -> Value {
        request["secret"] = json!(SECRET.to_vec());
        let mut stream = bounded(TcpStream::connect(("127.0.0.1", self.port)))
            .await
            .unwrap();
        let mut bytes = serde_json::to_vec(&request).unwrap();
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
        check_no_credentials(&bytes);
        serde_json::from_slice(&bytes).expect("authenticated botd response")
    }

    async fn operation(&self, command: &str, bot: &str, force: bool) -> Value {
        let id = next_id();
        let admitted = self
            .request(
                json!({"command":command,"bot":bot,"operation_id":id,"force":force,"yes":force}),
            )
            .await;
        assert_eq!(admitted["ok"], true);
        self.wait_operation(&id).await
    }

    async fn wait_operation(&self, id: &str) -> Value {
        bounded(async {
            loop {
                let result = self
                    .request(json!({"command":"operation","operation_id":id}))
                    .await;
                if result["operation"]["state"] != "running" {
                    return result;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }

    async fn accept(&self) -> Gateway {
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
        assert_eq!(gateway.ready["mode"], "mock_discord");
        assert_eq!(gateway.ready["credential_present"], false);
        gateway
    }

    async fn start_all(&mut self) {
        succeeded(&self.operation("start", "alpha", false).await);
        succeeded(&self.operation("start", "beta", false).await);
        for _ in 0..3 {
            let gateway = self.accept().await;
            let key = (
                gateway.ready["bot_id"].as_u64().unwrap(),
                gateway.ready["shard_id"].as_u64().unwrap() as u32,
            );
            assert!(self.gateways.insert(key, gateway).is_none());
        }
        self.verify_layout().await;
    }

    async fn verify_layout(&self) {
        let status = self.request(json!({"command":"status"})).await;
        let mut pids = HashSet::new();
        let mut paths = HashSet::new();
        for spec in &self.plan {
            let bot = spec.bot_id.parse().unwrap();
            let gateway = &self.gateways[&(bot, spec.shard_id)];
            assert!(pids.insert(gateway.pid()));
            assert_eq!(gateway.ready["total_shards"], spec.total_shards);
            for (field, expected) in [
                ("ipc_path", &spec.ipc_path),
                ("state_path", &spec.gateway.state_path),
                ("management_path", &spec.gateway.management_path),
            ] {
                let actual = PathBuf::from(gateway.ready[field].as_str().unwrap());
                assert_eq!(&actual, expected);
                assert!(paths.insert(actual));
            }
            let workers = gateway.ready["workers"].as_array().unwrap();
            assert_eq!(workers.len(), 2);
            let mut generations = HashSet::new();
            for (index, worker) in workers.iter().enumerate() {
                assert_eq!(worker["index"], index);
                assert_eq!(worker["credential_free"], true);
                assert!(pids.insert(worker["pid"].as_u64().unwrap() as u32));
                let identity = &worker["identity"];
                assert_eq!(identity["bot_id"], bot);
                assert_eq!(
                    identity["shard"],
                    json!({"id":spec.shard_id,"total":spec.total_shards})
                );
                assert!(generations.insert(identity["generation_id"].as_u64().unwrap()));
                assert_eq!(identity["ownership_epoch"], 0);
            }
            let process = status["status"]["processes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| {
                    p["key"]["bot_id"] == spec.bot_id && p["key"]["shard_id"] == spec.shard_id
                })
                .unwrap();
            assert_eq!(process["identity"]["pid"], gateway.pid());
            assert_eq!(process["state"]["state"], "Running");
        }
        assert_eq!(
            pids.len(),
            9,
            "three Gateways and six independent Worker processes"
        );
        assert_eq!(paths.len(), 9);
    }

    async fn finish(mut self) {
        check_files(&self.root);
        let _ = self.child.kill().await;
        let mut stderr = Vec::new();
        if let Some(mut pipe) = self.stderr.take() {
            bounded(pipe.read_to_end(&mut stderr)).await.unwrap();
        }
        check_no_credentials(&stderr);
        assert!(stderr.is_empty(), "botd emitted unexpected diagnostics");
        self.port = 0;
    }
}

// Panic cleanup talks to the still-running daemon independently of the Tokio
// runtime in this test. Force is confined to this fixture's authenticated Bot
// processes. After daemon death, use the already-owned mock collector sockets
// instead. No global PID enumeration or unrelated process termination is used.
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.port != 0 {
            for bot in ["101", "202"] {
                let _ = blocking_request(
                    self.port,
                    json!({
                        "command":"stop","bot":bot,"force":true,"yes":true,"operation_id":next_id()
                    }),
                );
            }
            let deadline = Instant::now() + Duration::from_secs(4);
            while Instant::now() < deadline {
                let stopped = blocking_request(self.port, json!({"command":"status"}))
                    .and_then(|value| value["status"]["processes"].as_array().cloned())
                    .is_some_and(|processes| {
                        processes.iter().all(|p| p["state"]["state"] != "Running")
                    });
                if stopped {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.child.start_kill();
        for gateway in std::mem::take(&mut self.gateways).into_values() {
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

fn blocking_request(port: u16, mut request: Value) -> Option<Value> {
    let address: SocketAddr = ([127, 0, 0, 1], port).into();
    let mut stream = BlockingStream::connect_timeout(&address, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .ok()?;
    request["secret"] = json!(SECRET.to_vec());
    let mut bytes = serde_json::to_vec(&request).ok()?;
    bytes.push(b'\n');
    stream.write_all(&bytes).ok()?;
    let mut response = Vec::new();
    std::io::BufReader::new(stream)
        .read_until(b'\n', &mut response)
        .ok()?;
    serde_json::from_slice(&response).ok()
}

fn succeeded(result: &Value) {
    assert_eq!(
        result["ok"], true,
        "safe daemon operation response: {result}"
    );
    assert_eq!(result["operation"]["state"], "succeeded");
    assert_eq!(result["operation"]["failed_shards"], json!([]));
}

/// Query OS process existence, not merely a readiness file or socket EOF. Only
/// observed fixture PIDs are used, and the command never changes a process.
async fn assert_exited(pids: &[u32]) {
    #[cfg(windows)]
    let check = || {
        use std::os::windows::process::CommandExt;
        let ids = pids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut command = std::process::Command::new("powershell.exe");
        command
            .args(["-NoProfile", "-NonInteractive", "-Command"])
            .arg(format!(
                "Get-Process -Id {ids} -ErrorAction SilentlyContinue | ForEach-Object {{ $_.Id }}"
            ))
            .creation_flags(0x08000000);
        let output = command.output().unwrap();
        output.stdout.is_empty()
    };
    #[cfg(target_os = "linux")]
    let check = || {
        pids.iter()
            .all(|pid| !Path::new(&format!("/proc/{pid}")).exists())
    };
    #[cfg(all(unix, not(target_os = "linux")))]
    let check = || {
        pids.iter().all(|pid| {
            !std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        })
    };
    bounded(async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain() {
    let mut fixture = Fixture::new().await;
    fixture.start_all().await;
    let keys = [(101, 0), (101, 1), (202, 0)];
    for key in keys {
        let gateway = fixture.gateways.get_mut(&key).unwrap();
        // Both channel partitions run the same attribute-registered handler,
        // through independent Gateway/Core pipes and actual mock HTTP writes.
        for worker_key in [0, 1] {
            let prefix = gateway.command(false, worker_key, "!ping").await;
            let slash = gateway.command(true, worker_key, "ping").await;
            assert_eq!(prefix["content"], "Pong! 🦀");
            assert_eq!(slash["content"], prefix["content"]);
            assert_eq!(prefix["receipt"]["pid"], slash["receipt"]["pid"]);
            assert_ne!(
                prefix["receipt"]["operation_id"],
                slash["receipt"]["operation_id"]
            );
        }
        assert_eq!(
            gateway.command(false, 0, "!state_counter").await["content"],
            "State counter: 1"
        );
        assert_eq!(
            gateway.command(true, 0, "state_counter").await["content"],
            "State counter: 2"
        );
        assert_eq!(
            gateway.command(false, 1, "!state_counter").await["content"],
            "State counter: 1"
        );
    }

    let mut crashed = fixture.gateways.remove(&(101, 0)).unwrap();
    let mut old_pids = crashed.worker_pids();
    old_pids.push(crashed.pid());
    crashed.send(json!({"type":"crash","id":90})).await;
    // Continue both unaffected execution units while botd observes the exit.
    for key in [(101, 1), (202, 0)] {
        let gateway = fixture.gateways.get_mut(&key).unwrap();
        let reply = gateway.command(false, 0, "!state_counter").await;
        assert_eq!(reply["content"], "State counter: 3");
    }
    let replacement = fixture.accept().await;
    assert_eq!(replacement.ready["bot_id"], 101);
    assert_eq!(replacement.ready["shard_id"], 0);
    assert_ne!(replacement.pid(), crashed.pid());
    assert_ne!(replacement.ready["workers"], crashed.ready["workers"]);
    fixture.gateways.insert((101, 0), replacement);
    assert_exited(&old_pids).await;
    assert_eq!(
        fixture
            .gateways
            .get_mut(&(101, 0))
            .unwrap()
            .command(false, 0, "!state_counter")
            .await["content"],
        "State counter: 1"
    );
    fixture.verify_layout().await;

    // A graceful stop races an already admitted, delayed HTTP response. It must
    // return that result to its Worker before acknowledging all child exits.
    let mock = MockDiscord::new([MockReply::json(200, b"{}")
        .unwrap()
        .with_delay(Duration::from_millis(300))]);
    let server = mock.spawn_tcp().await.unwrap();
    fixture
        .gateways
        .get_mut(&(101, 1))
        .unwrap()
        .send(json!({
            "type":"prefix","id":100,"key":1,"content":"!ping","rest_addr":server.addr().to_string()
        }))
        .await;
    bounded(async {
        while mock.stats().requests != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let operation = next_id();
    assert_eq!(
        fixture
            .request(json!({"command":"stop","bot":"alpha","operation_id":operation}))
            .await["ok"],
        true
    );
    let gateway = fixture.gateways.get_mut(&(101, 1)).unwrap();
    let pending = gateway.receive().await;
    assert_eq!(pending["id"], 100);
    assert_eq!(pending["outcome"], "completed");
    assert_receipt(&pending, &gateway.ready, 1, "confirmed");
    succeeded(&fixture.wait_operation(&operation).await);
    let mut ended = Vec::new();
    for key in [(101, 0), (101, 1)] {
        let gateway = fixture.gateways.get_mut(&key).unwrap();
        gateway.stopped().await;
        ended.push(gateway.pid());
        ended.extend(gateway.worker_pids());
    }
    assert_exited(&ended).await;
    server.shutdown().await.unwrap();
    assert_eq!(
        fixture
            .gateways
            .get_mut(&(202, 0))
            .unwrap()
            .command(true, 0, "ping")
            .await["content"],
        "Pong! 🦀"
    );
    succeeded(&fixture.operation("stop", "beta", false).await);
    let gateway = fixture.gateways.get_mut(&(202, 0)).unwrap();
    gateway.stopped().await;
    let mut ended = gateway.worker_pids();
    ended.push(gateway.pid());
    assert_exited(&ended).await;
    fixture.finish().await;
}

#[tokio::test]
async fn mock_unknown_outcomes_return_to_origin_and_cannot_claim_graceful_success() {
    // No token variables exist at all: explicit mock mode must not even consult
    // a CredentialSource. Real child/handshake/HTTP paths are still exercised.
    let mut fixture = Fixture::configured(false).await;
    fixture.start_all().await;
    let mock = MockDiscord::new([MockReply::json(200, b"{}").unwrap().with_lost_response()]);
    let server = mock.spawn_tcp().await.unwrap();
    let gateway = fixture.gateways.get_mut(&(101, 0)).unwrap();
    let response = gateway
        .exchange(json!({
            "type":"prefix","id":11,"key":1,"content":"!ping","rest_addr":server.addr().to_string()
        }))
        .await;
    assert_eq!(response["outcome"], "unknown");
    assert_eq!(response["unknown_responses"], 1);
    assert_receipt(&response, &gateway.ready, 1, "unknown");
    assert_eq!(
        mock.stats().requests,
        1,
        "a lost POST response must not be retried"
    );
    assert_eq!(mock.stats().applied, 1);
    assert_eq!(mock.stats().lost_responses, 1);
    server.shutdown().await.unwrap();

    // Losing the initial Interaction ACK must leave the handler unexecuted.
    let mock = MockDiscord::new([MockReply::interaction_ack().with_lost_response()]);
    let server = mock.spawn_tcp().await.unwrap();
    let gateway = fixture.gateways.get_mut(&(101, 1)).unwrap();
    let response = gateway.exchange(json!({
        "type":"slash","id":12,"key":0,"name":"state_counter","rest_addr":server.addr().to_string()
    })).await;
    assert!(matches!(
        response["outcome"].as_str(),
        Some("unknown" | "not_dispatched")
    ));
    assert!(response["receipt"].is_null());
    assert_eq!(response["unknown_responses"], 1);
    assert_eq!(mock.stats().requests, 1);
    assert_eq!(mock.stats().interaction_requests, 1);
    assert_eq!(mock.stats().rest_requests, 0);
    server.shutdown().await.unwrap();
    let mock = MockDiscord::new([MockReply::json(200, b"{}").unwrap()]);
    let server = mock.spawn_tcp().await.unwrap();
    let response = gateway.exchange(json!({
        "type":"prefix","id":13,"key":0,"content":"!state_counter","rest_addr":server.addr().to_string()
    })).await;
    assert_eq!(response["content"], "State counter: 1");
    assert_eq!(response["outcome"], "completed");
    assert_eq!(response["unknown_responses"], 1);
    assert_receipt(&response, &gateway.ready, 0, "confirmed");
    server.shutdown().await.unwrap();

    // A complete rate-limit response is a known failure and is relayed once to
    // the originating Worker. Mock mode makes no unsafe automatic POST retry.
    let mock = MockDiscord::new([MockReply::rate_limited(Duration::from_millis(50))]);
    let server = mock.spawn_tcp().await.unwrap();
    let gateway = fixture.gateways.get_mut(&(202, 0)).unwrap();
    let response = gateway
        .exchange(json!({
            "type":"prefix","id":14,"key":0,"content":"!ping","rest_addr":server.addr().to_string()
        }))
        .await;
    assert_eq!(response["outcome"], "failed");
    assert_eq!(response["unknown_responses"], 0);
    assert_receipt(&response, &gateway.ready, 0, "confirmed");
    assert_eq!(response["receipt"]["outcome"]["status"], 429);
    assert_eq!(mock.stats().requests, 1);
    assert_eq!(mock.stats().applied, 0);
    server.shutdown().await.unwrap();

    // The slash edit also distinguishes a complete HTTP failure from a lost
    // response; the originating Worker receives the exact confirmed status.
    for status in [429, 503] {
        let reply = if status == 429 {
            MockReply::rate_limited(Duration::from_millis(50))
        } else {
            MockReply::server_error(status).unwrap()
        };
        let mock = MockDiscord::new([MockReply::interaction_ack(), reply]);
        let server = mock.spawn_tcp().await.unwrap();
        let response = gateway
            .exchange(json!({
                "type":"slash","id":15,"key":1,"name":"ping","rest_addr":server.addr().to_string()
            }))
            .await;
        assert_eq!(response["outcome"], "failed");
        assert_eq!(response["unknown_responses"], 0);
        assert_receipt(&response, &gateway.ready, 1, "confirmed");
        assert_eq!(response["receipt"]["outcome"]["status"], status);
        assert_eq!(mock.stats().requests, 2);
        assert_eq!(mock.stats().interaction_requests, 1);
        assert_eq!(mock.stats().rest_requests, 1);
        server.shutdown().await.unwrap();
    }

    let mock = MockDiscord::new([]);
    let server = mock.spawn_tcp().await.unwrap();
    for (content, expected) in [("not-a-command", "no_reply"), ("!greet", "dispatch_failed")] {
        let response = gateway.exchange(json!({
            "type":"prefix","id":16,"key":0,"content":content,"rest_addr":server.addr().to_string()
        })).await;
        assert_eq!(response["outcome"], expected);
        assert!(response["receipt"].is_null());
        assert_eq!(response["unknown_responses"], 0);
    }
    assert_eq!(mock.stats().requests, 0);
    server.shutdown().await.unwrap();

    let failed = fixture.operation("stop", "alpha", false).await;
    assert_eq!(failed["ok"], false);
    assert_eq!(failed["operation"]["state"], "failed");
    assert_eq!(
        failed["operation"]["failed_shards"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let mut ended = Vec::new();
    for key in [(101, 0), (101, 1)] {
        let gateway = &fixture.gateways[&key];
        ended.push(gateway.pid());
        ended.extend(gateway.worker_pids());
    }
    assert_exited(&ended).await;
    assert_eq!(
        fixture
            .gateways
            .get_mut(&(202, 0))
            .unwrap()
            .command(false, 1, "!ping")
            .await["content"],
        "Pong! 🦀"
    );
    succeeded(&fixture.operation("stop", "beta", false).await);
    let gateway = fixture.gateways.get_mut(&(202, 0)).unwrap();
    gateway.stopped().await;
    let mut ended = gateway.worker_pids();
    ended.push(gateway.pid());
    assert_exited(&ended).await;
    fixture.finish().await;
}

#[tokio::test]
async fn malformed_mock_inputs_restart_only_the_target_and_collector_eof_is_not_shutdown() {
    let mut fixture = Fixture::configured(false).await;
    fixture.start_all().await;
    for (target, frame) in [
        ((101, 0), b"{\"type\":\"private-untrusted-input\"}\n".to_vec()),
        ((101, 0), vec![b'x'; 64 * 1024 + 1]),
        ((101, 1), b"{\"type\":\"prefix\",\"id\":20,\"key\":0,\"content\":\"!ping\",\"rest_addr\":\"203.0.113.1:8000\"}\n".to_vec()),
    ] {
        let mut old = fixture.gateways.remove(&target).unwrap();
        let mut pids = old.worker_pids();
        pids.push(old.pid());
        let _ = bounded(old.input.write_all(&frame)).await;
        let _ = old.input.shutdown().await;
        let mut output = Vec::new();
        bounded(old.output.read_to_end(&mut output)).await.unwrap();
        assert!(!String::from_utf8_lossy(&output).contains("private-untrusted-input"));
        assert!(
            !String::from_utf8_lossy(&output).contains("stopped"),
            "invalid input cannot masquerade as graceful stop"
        );
        let next = fixture.accept().await;
        assert_eq!(next.ready["bot_id"], target.0);
        assert_eq!(next.ready["shard_id"], target.1);
        assert_ne!(next.pid(), old.pid());
        fixture.gateways.insert(target, next);
        assert_exited(&pids).await;
        for key in [(101, 0), (101, 1), (202, 0)].into_iter().filter(|key| *key != target) {
            assert_eq!(
                fixture
                    .gateways
                    .get_mut(&key)
                    .unwrap()
                    .command(false, 0, "!ping")
                    .await["content"],
                "Pong! 🦀"
            );
        }
    }
    let beta = fixture.gateways.get_mut(&(202, 0)).unwrap();
    beta.input.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture.verify_layout().await;
    for bot in ["alpha", "beta"] {
        succeeded(&fixture.operation("stop", bot, false).await);
    }
    let mut ended = Vec::new();
    for gateway in fixture.gateways.values_mut() {
        gateway.stopped().await;
        ended.push(gateway.pid());
        ended.extend(gateway.worker_pids());
    }
    assert_exited(&ended).await;
    fixture.finish().await;
}

#[tokio::test]
async fn daemon_death_closes_shutdown_pipes_without_stopping_gateways_or_workers() {
    let mut fixture = Fixture::configured(false).await;
    fixture.start_all().await;
    let mut pids = vec![fixture.child.id().unwrap()];
    for gateway in fixture.gateways.values_mut() {
        pids.push(gateway.pid());
        pids.extend(gateway.worker_pids());
        assert_eq!(
            gateway.command(false, 0, "!state_counter").await["content"],
            "State counter: 1"
        );
        assert_eq!(
            gateway.command(true, 1, "state_counter").await["content"],
            "State counter: 1"
        );
    }

    // This is an ungraceful death of the exact daemon owned by the test. Its
    // anonymous Gateway control handles close without a Shutdown frame.
    fixture.child.kill().await.unwrap();
    fixture.port = 0;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for gateway in fixture.gateways.values_mut() {
        let status = gateway.exchange(json!({"type":"status","id":200})).await;
        assert_eq!(status["pid"], gateway.pid());
        assert_eq!(status["workers"], gateway.ready["workers"]);
        // Retained state plus a receipt from each original Worker proves that
        // stdin EOF was not interpreted as a shutdown or silent replacement.
        assert_eq!(
            gateway.command(false, 0, "!state_counter").await["content"],
            "State counter: 2"
        );
        assert_eq!(
            gateway.command(true, 1, "state_counter").await["content"],
            "State counter: 2"
        );
    }

    // There is no surviving daemon to accept stop. The explicitly selected
    // mock fault channel terminates only these three fixture Gateways; Core
    // children then drain their closed parent pipes and leave no live orphan.
    for gateway in fixture.gateways.values_mut() {
        gateway.send(json!({"type":"crash","id":201})).await;
        let mut bytes = Vec::new();
        // An abrupt process exit may close TCP with FIN or with a reset (in
        // particular Windows WSAECONNRESET). Neither is a graceful-stop ACK.
        if let Err(error) = bounded(gateway.output.read_to_end(&mut bytes)).await {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert!(bytes.is_empty());
    }
    assert_exited(&pids).await;
    fixture.finish().await;
}

fn bare_gateway() -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_src"));
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

#[tokio::test]
async fn mock_mode_is_explicit_and_rejects_non_loopback_or_invalid_endpoints() {
    let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = bounded(
        bare_gateway()
            .env("MOCK_DISCORD", collector.local_addr().unwrap().to_string())
            .env("DISCORD_MOCK", "1")
            .output(),
    )
    .await
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("token_missing"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), collector.accept())
            .await
            .is_err()
    );
    for endpoint in [
        "203.0.113.1:8000",
        "0.0.0.0:8000",
        "127.0.0.1:0",
        "localhost:80",
        "not-an-endpoint",
    ] {
        let output = bounded(bare_gateway().args(["--mock-discord", endpoint]).output())
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            !error.contains(endpoint),
            "untrusted option must not enter diagnostics"
        );
        assert!(
            !error.contains("token_missing"),
            "mock endpoint must be rejected during argument parsing"
        );
    }
}
