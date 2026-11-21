//! Process-level two-shard separation with the loopback Discord simulator.
#![cfg(unix)]

#[path = "fixtures/control_channel.rs"]
mod control_channel;

use serde_json::{Value, json};
use std::{
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    process::Child,
};

const WAIT: Duration = Duration::from_secs(40);
const SECRET: [u8; 32] = [83; 32];
const CONFIG: &str = r#"
[cluster]
id = "separate-cores-test"
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
"#;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("separate cores deadline")
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
        assert!(!bytes.is_empty(), "Gateway connection closed");
        serde_json::from_slice(&bytes).expect("collector JSON")
    }

    async fn status(&mut self) -> Value {
        self.input
            .write_all(b"{\"type\":\"status\",\"id\":7}\n")
            .await
            .unwrap();
        self.receive().await
    }

    fn pid(&self) -> u32 {
        self.ready["pid"].as_u64().unwrap().try_into().unwrap()
    }
}

fn worker_pids(value: &Value) -> Vec<u32> {
    value["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|worker| worker["pid"].as_u64().unwrap().try_into().unwrap())
        .collect()
}

fn alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
        && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| stat.contains(") Z "))
            .unwrap_or(true)
}

fn signal(pid: u32, name: &str) {
    let status = std::process::Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

struct Fixture {
    root: PathBuf,
    daemon: Child,
    collector: TcpListener,
    port: u16,
}

impl Fixture {
    async fn start() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Short: Unix socket paths are limited to about 100 bytes.
        let root = std::env::temp_dir().join(format!(
            "bscs{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let config = root.join("placement.toml");
        std::fs::write(&config, CONFIG).unwrap();
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
                "--stop-children-on-exit",
                "--core-mode",
                "separate",
                "--mock-discord",
            ])
            .arg(collector.local_addr().unwrap().to_string())
            .env("TOKEN_ALPHA", "separate-test-credential")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut daemon = command.spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret": SECRET})).unwrap();
        bootstrap.push(b'\n');
        bounded(daemon.stdin.take().unwrap().write_all(&bootstrap))
            .await
            .unwrap();
        let mut line = Vec::new();
        bounded(BufReader::new(daemon.stdout.take().unwrap()).read_until(b'\n', &mut line))
            .await
            .unwrap();
        if line.is_empty() {
            let mut diagnostic = Vec::new();
            if let Some(stderr) = daemon.stderr.take() {
                let _ = bounded(stderr.take(4096).read_to_end(&mut diagnostic)).await;
            }
            panic!(
                "botd exited before announcement: {}",
                String::from_utf8_lossy(&diagnostic)
            );
        }
        let announced: Value = serde_json::from_slice(&line).expect("botd announcement");
        let port = announced["port"].as_u64().unwrap().try_into().unwrap();
        Self {
            root,
            daemon,
            collector,
            port,
        }
    }

    async fn accept(&self) -> Gateway {
        let (stream, _) = bounded(self.collector.accept()).await.unwrap();
        let (output, input) = stream.into_split();
        let mut gateway = Gateway {
            ready: Value::Null,
            input,
            output: BufReader::new(output),
        };
        gateway.ready = gateway.receive().await;
        assert_eq!(gateway.ready["type"], "ready");
        assert_eq!(gateway.ready["workers"].as_array().unwrap().len(), 2);
        gateway
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// botd answers `audit_replay_pending` (nothing started) while it replays its audit journal
/// at startup; repeat the request until it is ready.
async fn control(port: u16, value: Value) -> Value {
    for _ in 0..200 {
        let reply = control_once(port, value.clone()).await;
        if reply["error"] != "audit_replay_pending" {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("audit replay did not finish");
}

async fn control_once(port: u16, value: Value) -> Value {
    let reply = bounded(control_channel::request(port, &SECRET, &value))
        .await
        .expect("authenticated control reply");
    serde_json::from_slice(&reply).expect("daemon response")
}

/// Like `control`, but an invalid request (rejected before any handler runs) is answered by
/// closing the connection without a response, which is returned as `None`.
async fn control_or_closed(port: u16, value: Value) -> Option<Value> {
    match bounded(control_channel::request(port, &SECRET, &value)).await {
        Ok(reply) => Some(serde_json::from_slice(&reply).expect("daemon response")),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            None
        }
        Err(error) => panic!("authenticated request failed: {error}"),
    }
}

#[tokio::test]
async fn each_shard_has_its_own_cores_and_updates_independently() {
    let fixture = Fixture::start().await;
    let first = fixture.accept().await;
    let second = fixture.accept().await;
    let (shard0, mut shard1) = match (
        first.ready["shard_id"].as_u64(),
        second.ready["shard_id"].as_u64(),
    ) {
        (Some(0), Some(1)) => (first, second),
        (Some(1), Some(0)) => (second, first),
        ids => panic!("unexpected Gateway shards: {ids:?}"),
    };
    let old0 = shard0.pid();
    let old1 = shard1.pid();
    assert_ne!(old0, old1);
    let cores0 = worker_pids(&shard0.ready);
    let cores1 = worker_pids(&shard1.ready);
    let cores: Vec<u32> = cores0.iter().chain(&cores1).copied().collect();
    assert_eq!(
        cores
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        4
    );
    assert!(cores.iter().all(|pid| alive(*pid)));
    assert!(alive(old0) && alive(old1));

    signal(old0, "KILL");
    let restarted = fixture.accept().await;
    assert_eq!(restarted.ready["shard_id"], 0);
    let restarted_pid = restarted.pid();
    assert_ne!(restarted_pid, old0);
    assert_eq!(worker_pids(&restarted.ready), cores0);
    let status1 = shard1.status().await;
    assert_eq!(status1["pid"], old1);
    assert_eq!(worker_pids(&status1), cores1);
    assert!(alive(old1) && cores.iter().all(|pid| alive(*pid)));

    let id = "0123456789abcdef0123456789abcdef";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id}),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");
    // Updates are sequential; accept each active replacement as it appears.
    let update_a = fixture.accept().await;
    let update_b = fixture.accept().await;
    let (updated0, updated1) = match (
        update_a.ready["shard_id"].as_u64(),
        update_b.ready["shard_id"].as_u64(),
    ) {
        (Some(0), Some(1)) => (update_a, update_b),
        (Some(1), Some(0)) => (update_b, update_a),
        ids => panic!("unexpected updated Gateway shards: {ids:?}"),
    };
    let finished = bounded(async {
        loop {
            let view = control(
                fixture.port,
                json!({"command":"operation","operation_id":id}),
            )
            .await;
            if view["operation"]["state"] != "running" {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert_eq!(finished["operation"]["state"], "succeeded", "{finished}");
    let new0 = updated0.pid();
    let new1 = updated1.pid();
    assert_ne!(new0, restarted_pid);
    assert_ne!(new0, old0);
    assert_ne!(new1, old1);
    assert_ne!(new0, new1);
    assert_eq!(worker_pids(&updated0.ready), cores0);
    assert_eq!(worker_pids(&updated1.ready), cores1);
    assert!(alive(new0) && alive(new1) && cores.iter().all(|pid| alive(*pid)));
    bounded(async {
        while [old0, restarted_pid, old1].into_iter().any(alive) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    signal(fixture.daemon.id().unwrap(), "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [old0, restarted_pid, old1, new0, new1]
            .into_iter()
            .chain(cores.iter().copied())
            .any(alive)
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let ipc: Vec<_> = walk(&fixture.root)
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "sock")
                || p.file_name().is_some_and(|n| n == "core.secret")
        })
        .collect();
    assert!(
        ipc.is_empty(),
        "sockets or core.secret left behind: {ipc:?}"
    );
    std::fs::remove_dir_all(&fixture.root).unwrap();
}

#[tokio::test]
async fn gateway_update_with_a_shard_core_crash_recovers_both_shards() {
    let fixture = Fixture::start().await;
    let first = fixture.accept().await;
    let second = fixture.accept().await;
    let mut gateways = vec![first, second];
    let victim = worker_pids(&gateways[0].ready)[0];
    let daemon_pid = fixture.daemon.id().unwrap();
    let id = "fedcba9876543210fedcba9876543210";
    let admitted = control(
        fixture.port,
        json!({
            "command": "gateway-update", "bot": "101", "operation_id": id
        }),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");
    let running = control(
        fixture.port,
        json!({
            "command": "operation", "operation_id": id
        }),
    )
    .await;
    assert_eq!(running["operation"]["state"], "running", "{running}");
    signal(victim, "KILL");

    let finished = bounded(async {
        loop {
            let view = control(
                fixture.port,
                json!({
                    "command": "operation", "operation_id": id
                }),
            )
            .await;
            if matches!(
                view["operation"]["state"].as_str(),
                Some("succeeded" | "failed")
            ) {
                break view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(matches!(
        finished["operation"]["state"].as_str(),
        Some("succeeded" | "failed")
    ));

    let runtime_root = fixture.root.clone();
    let processes = || {
        let mut found = Vec::new();
        for entry in std::fs::read_dir("/proc").unwrap().flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            if pid == daemon_pid || !alive(pid) {
                continue;
            }
            let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
                continue;
            };
            let args: Vec<_> = cmdline.split(|b| *b == 0).collect();
            if !args
                .iter()
                .any(|arg| arg.starts_with(runtime_root.to_string_lossy().as_bytes()))
            {
                continue;
            }
            let status = std::fs::read_to_string(entry.path().join("status")).unwrap_or_default();
            let parent = status
                .lines()
                .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok());
            found.push((pid, parent, args.contains(&b"--core-listen".as_slice())));
        }
        found
    };
    let settled = bounded(async {
        loop {
            let found = processes();
            if found.len() == 6
                && found
                    .iter()
                    .all(|(_, parent, _)| *parent == Some(daemon_pid))
                && found.iter().filter(|(_, _, core)| *core).count() == 4
                && !found.iter().any(|(pid, _, _)| *pid == victim)
            {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let active: Vec<u32> = settled
        .iter()
        .filter(|(_, _, core)| !core)
        .map(|(pid, _, _)| *pid)
        .collect();
    bounded(async {
        while !active
            .iter()
            .all(|pid| gateways.iter().any(|gateway| gateway.pid() == *pid))
        {
            gateways.push(fixture.accept().await);
        }
    })
    .await;
    let mut seen = std::collections::HashSet::new();
    let mut seen_cores = std::collections::HashSet::new();
    for gateway in gateways
        .iter_mut()
        .filter(|gateway| active.contains(&gateway.pid()))
    {
        let shard = gateway.ready["shard_id"].as_u64().unwrap();
        assert!(shard < 2 && seen.insert(shard));
        let workers = bounded(async {
            loop {
                let status = gateway.status().await;
                let workers = worker_pids(&status);
                if status["pid"] == gateway.pid()
                    && workers.len() == 2
                    && workers[0] != workers[1]
                    && workers.iter().all(|pid| {
                        settled.iter().any(|(p, _, core)| p == pid && *core) && alive(*pid)
                    })
                {
                    break workers;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        assert!(workers.iter().all(|pid| seen_cores.insert(*pid)));
        let flag_value = |pid: u32, flag: &[u8]| -> u64 {
            let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
            let args: Vec<_> = cmdline.split(|byte| *byte == 0).collect();
            args.windows(2)
                .find(|pair| pair[0] == flag)
                .map(|pair| std::str::from_utf8(pair[1]).unwrap().parse().unwrap())
                .unwrap_or(0)
        };
        let floor_before = flag_value(workers[0], b"--epoch-floor");
        let gateway_epoch = flag_value(gateway.pid(), b"--core-epoch");
        signal(workers[0], "KILL");
        let replacement = bounded(async {
            loop {
                let now = worker_pids(&gateway.status().await);
                if now.len() == 2 && now[0] != workers[0] && now[1] == workers[1] && alive(now[0]) {
                    break now[0];
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        let floor_after = flag_value(replacement, b"--epoch-floor");
        assert!(
            floor_after >= floor_before && floor_after >= gateway_epoch,
            "shard {shard}: floor {floor_before} -> {floor_after}, gateway epoch {gateway_epoch}"
        );
    }
    assert_eq!(seen.len(), 2);
    bounded(async {
        loop {
            let found = processes();
            if found.len() == 6
                && found
                    .iter()
                    .all(|(_, parent, _)| *parent == Some(daemon_pid))
                && found.iter().filter(|(_, _, core)| *core).count() == 4
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    signal(daemon_pid, "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while !processes().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let ipc: Vec<_> = walk(&fixture.root)
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "sock")
                || p.file_name().is_some_and(|n| n == "core.secret")
        })
        .collect();
    assert!(
        ipc.is_empty(),
        "sockets or core.secret left behind: {ipc:?}"
    );
    std::fs::remove_dir_all(&fixture.root).unwrap();
}

/// `shards: [1]` restricts a `gateway-update` to that Shard only: Shard 0's Gateway and every
/// Core stay untouched. A Shard that is not local is refused outright (nothing happens), and
/// repeating the operation ID with a different filter is a conflict.
#[tokio::test]
async fn gateway_update_can_be_restricted_to_one_shard() {
    let fixture = Fixture::start().await;
    let first = fixture.accept().await;
    let second = fixture.accept().await;
    let (mut shard0, shard1) = match (
        first.ready["shard_id"].as_u64(),
        second.ready["shard_id"].as_u64(),
    ) {
        (Some(0), Some(1)) => (first, second),
        (Some(1), Some(0)) => (second, first),
        ids => panic!("unexpected Gateway shards: {ids:?}"),
    };
    let old0 = shard0.pid();
    let old1 = shard1.pid();
    let cores0 = worker_pids(&shard0.ready);
    let cores1 = worker_pids(&shard1.ready);
    let cores: Vec<u32> = cores0.iter().chain(&cores1).copied().collect();

    // An unknown Shard is an error and starts nothing.
    let unknown = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":"00000000000000000000000000000009","shards":[9]}),
    )
    .await;
    assert_eq!(unknown["ok"], false, "{unknown}");
    // A stale guild resolution must fail on the wire before starting a candidate.
    let stale = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":"00000000000000000000000000000007","shards":[1],"expect_total_shards":1}),
    )
    .await;
    assert_eq!(stale["ok"], false, "{stale}");
    assert_eq!(
        stale["error"],
        "bot total_shards changed; resolve guilds again before retrying"
    );
    assert!(stale.get("operation").is_none());
    let changed_identity = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":"00000000000000000000000000000006","shards":[1],"expect_bot_id":"removed-bot"}),
    )
    .await;
    assert_eq!(changed_identity["ok"], false, "{changed_identity}");
    assert_eq!(
        changed_identity["error"],
        "bot identity changed; refresh the bot list before retrying"
    );
    assert!(changed_identity.get("operation").is_none());
    // Empty and duplicate filters are invalid requests.
    for bad in [json!([]), json!([1, 1])] {
        let refused = control_or_closed(
            fixture.port,
            json!({"command":"gateway-update","bot":"101","operation_id":"00000000000000000000000000000008","shards":bad}),
        )
        .await;
        assert!(
            refused.is_none(),
            "invalid filter must get no response: {refused:?}"
        );
    }
    // Filters are meaningless for read commands.
    let read = control_or_closed(
        fixture.port,
        json!({"command":"status","bot":"101","shards":[0]}),
    )
    .await;
    assert!(
        read.is_none(),
        "read command with a filter must get no response: {read:?}"
    );
    let read = control_or_closed(
        fixture.port,
        json!({"command":"status","bot":"101","expect_total_shards":2}),
    )
    .await;
    assert!(
        read.is_none(),
        "read command must reject a mutation precondition"
    );

    let id = "1111111111111111aaaaaaaaaaaaaaaa";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id,"shards":[1],"expect_total_shards":2,"expect_bot_id":"101"}),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");
    let replaced = fixture.accept().await;
    assert_eq!(replaced.ready["shard_id"], 1);
    let finished = bounded(async {
        loop {
            let view = control(
                fixture.port,
                json!({"command":"operation","operation_id":id}),
            )
            .await;
            if view["operation"]["state"] != "running" {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert_eq!(finished["operation"]["state"], "succeeded", "{finished}");
    assert_eq!(
        finished["operation"]["shards"].as_array().map(Vec::len),
        Some(1),
        "{finished}"
    );
    let new1 = replaced.pid();
    assert_ne!(new1, old1);
    bounded(async {
        while alive(old1) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    // Shard 0 was never touched.
    let status0 = shard0.status().await;
    assert_eq!(status0["pid"], old0);
    assert!(alive(old0));
    assert!(cores.iter().all(|pid| alive(*pid)));
    assert_eq!(worker_pids(&replaced.ready), cores1);
    assert_eq!(worker_pids(&status0), cores0);

    // The same ID with a different filter is a conflict, not a replay.
    let conflict = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id,"shards":[0]}),
    )
    .await;
    assert_eq!(conflict["ok"], false, "{conflict}");
    let replay = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id,"shards":[1]}),
    )
    .await;
    assert_eq!(replay["operation"]["state"], "succeeded", "{replay}");

    signal(fixture.daemon.id().unwrap(), "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    std::fs::remove_dir_all(&fixture.root).unwrap();
}
