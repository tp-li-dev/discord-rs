//! Test-only process harness. TLS is terminated by botd, never by these proxies.
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    process::{Child, ChildStderr},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub const HEARTBEAT_MS: u64 = 100;
pub const FAIL_MS: u64 = 600;
pub const STABLE_MS: u64 = 1200;
pub const LEASE_MS: u64 = 1500;
pub const STOP_MARGIN_MS: u64 = 500;
pub const WAIT: Duration = Duration::from_secs(15);
const FRAME_LIMIT: u64 = 64 * 1024;
const SECRET: [u8; 32] = [73; 32];
const POISON: &str = "synthetic-cluster-credential-must-not-propagate";
static NEXT: AtomicU64 = AtomicU64::new(1);

pub static PROCESS_TESTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

pub fn no_secrets(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    assert!(
        !text.contains(POISON),
        "credential escaped the process boundary"
    );
    assert!(
        !text.contains("\"secret\""),
        "bootstrap capability was logged"
    );
    assert!(!text.contains("PRIVATE KEY"), "private key was logged");
}

fn directory() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("e2e-cluster-tests");
    std::fs::create_dir_all(&root).unwrap();
    let root = root.join(format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root
}

fn write_private(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap().write_all(bytes).unwrap();
}

fn certificates(root: &Path) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "ephemeral cluster E2E CA");
    let ca: Certificate = params.self_signed(&key).unwrap();
    std::fs::write(root.join("ca.der"), ca.der()).unwrap();
    for id in 1..=3 {
        let leaf_key = KeyPair::generate().unwrap();
        let name = format!("node-{id}.cluster");
        let mut params = CertificateParams::new(vec![name.clone()]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let certificate = params.signed_by(&leaf_key, &ca, &key).unwrap();
        std::fs::write(root.join(format!("node-{id}.der")), certificate.der()).unwrap();
        // Ephemeral generated fixture keys are never committed or included in a diagnostic.
        write_private(
            &root.join(format!("node-{id}.key.der")),
            &leaf_key.serialize_der(),
        );
    }
}

struct Proxy {
    address: SocketAddr,
    commands: mpsc::Sender<(bool, oneshot::Sender<()>)>,
    task: JoinHandle<()>,
}
impl Proxy {
    async fn new(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (commands, mut receiver) = mpsc::channel::<(bool, oneshot::Sender<()>)>(4);
        let task = tokio::spawn(async move {
            let mut enabled = true;
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    command = receiver.recv() => {
                        let Some((next, ack)) = command else { break };
                        enabled = next;
                        if !enabled {
                            connections.abort_all();
                            while connections.join_next().await.is_some() {}
                        }
                        let _ = ack.send(());
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                    accepted = listener.accept() => {
                        let Ok((mut incoming, _)) = accepted else { break };
                        if !enabled { continue; }
                        connections.spawn(async move {
                            if let Ok(Ok(mut outgoing)) = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(target)).await {
                                let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                            }
                        });
                    }
                }
            }
        });
        Self {
            address,
            commands,
            task,
        }
    }
    async fn set(&self, enabled: bool) {
        let (sender, receiver) = oneshot::channel();
        self.commands.send((enabled, sender)).await.unwrap();
        tokio::time::timeout(WAIT, receiver).await.unwrap().unwrap();
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
pub struct Gateway {
    pub session: usize,
    pub node: u64,
    pub bot: u64,
    pub shard: u32,
    pub pid: u32,
    pub epoch: u64,
    pub ready: Value,
    pub ready_at: Instant,
    pub stopped_at: Option<Instant>,
    pub disconnected_at: Option<Instant>,
    input: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
}
impl Gateway {
    pub fn active(&self) -> bool {
        self.stopped_at.is_none() && self.disconnected_at.is_none()
    }
    pub fn pids(&self) -> Vec<u32> {
        let mut pids = vec![
            self.pid,
            self.ready["guardian_pid"].as_u64().unwrap() as u32,
        ];
        pids.extend(
            self.ready["workers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|worker| worker["pid"].as_u64().unwrap() as u32),
        );
        pids
    }
}

#[derive(Default)]
struct Observations {
    gateways: Vec<Gateway>,
    replies: BTreeMap<(usize, u64), Value>,
    failures: Vec<&'static str>,
}

/// Every ready/stopped/EOF is evaluated as it arrives, including while tests are
/// waiting on process exit, TLS partitions, status requests or stable periods.
#[derive(Clone, Default)]
pub struct Monitor(Arc<Mutex<Observations>>);
impl Monitor {
    pub fn check(&self) {
        let state = self.0.lock().unwrap();
        assert!(
            state.failures.is_empty(),
            "continuous process monitor failed: {:?}",
            state.failures
        );
    }
    fn failure(&self, error: &'static str) {
        self.0.lock().unwrap().failures.push(error);
    }
    pub fn all(&self) -> Vec<Gateway> {
        self.check();
        self.0.lock().unwrap().gateways.clone()
    }
    pub fn active(&self) -> Vec<Gateway> {
        self.all().into_iter().filter(Gateway::active).collect()
    }
    pub fn current(&self, shard: u32) -> Option<Gateway> {
        self.active()
            .into_iter()
            .find(|gateway| gateway.bot == 101 && gateway.shard == shard)
    }
    pub async fn wait_for<T>(&self, label: &str, mut condition: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + WAIT;
        loop {
            self.check();
            if let Some(value) = condition() {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {label}; active: {:?}",
                self.active()
                    .iter()
                    .map(|g| (g.node, g.shard, g.pid, g.epoch))
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }
    pub async fn owner(&self, shard: u32, node: u64) -> Gateway {
        self.wait_for("expected shard owner", || {
            self.current(shard).filter(|gateway| gateway.node == node)
        })
        .await
    }
    pub async fn ended(&self, session: usize) -> Gateway {
        self.wait_for("Gateway collector EOF", || {
            self.all()
                .into_iter()
                .find(|g| g.session == session && g.disconnected_at.is_some())
        })
        .await
    }
    pub async fn stable_for(&self, duration: Duration, predicate: impl Fn(&[Gateway]) -> bool) {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            self.check();
            assert!(
                predicate(&self.active()),
                "unexpected ownership during observation window"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        self.check();
        assert!(predicate(&self.active()));
    }
    pub async fn request(&self, gateway: &Gateway, mut value: Value) -> Value {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        value["id"] = json!(id);
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        tokio::time::timeout(WAIT, gateway.input.lock().await.write_all(&bytes))
            .await
            .unwrap()
            .unwrap();
        self.wait_for("mock Gateway response", || {
            self.0
                .lock()
                .unwrap()
                .replies
                .remove(&(gateway.session, id))
        })
        .await
    }

    async fn connection(&self, stream: TcpStream, node: u64) {
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut session = None;
        let mut writer = Some(writer);
        loop {
            let mut bytes = Vec::new();
            let result = (&mut reader)
                .take(FRAME_LIMIT + 1)
                .read_until(b'\n', &mut bytes)
                .await;
            match result {
                Ok(0) => break,
                Ok(_) if bytes.len() as u64 <= FRAME_LIMIT && bytes.last() == Some(&b'\n') => {}
                _ => {
                    self.failure("invalid or failed collector frame");
                    break;
                }
            }
            let text = String::from_utf8_lossy(&bytes);
            if text.contains(POISON) || text.contains("\"secret\"") {
                self.failure("credential reached mock collector");
                break;
            }
            let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                self.failure("invalid collector JSON");
                break;
            };
            let observed_at = Instant::now();
            if value["type"] == "ready" && session.is_none() {
                // EOF is not proof that the Gateway died: a guardian relay can
                // close independently. Check the actual old Gateway processes
                // at every replacement readiness, in addition to monitoring
                // the ready/stopped/EOF ordering continuously.
                let previous = {
                    let mut state = self.0.lock().unwrap();
                    let previous: Vec<_> = state
                        .gateways
                        .iter()
                        .filter(|g| {
                            Some(g.bot) == value["bot_id"].as_u64()
                                && Some(u64::from(g.shard)) == value["shard_id"].as_u64()
                                && g.stopped_at.is_none()
                        })
                        .cloned()
                        .collect();
                    if previous.iter().any(Gateway::active) {
                        state
                            .failures
                            .push("two Active collector sessions overlapped at new readiness");
                    }
                    previous.into_iter().map(|g| g.pid).collect::<Vec<_>>()
                };
                if !previous.is_empty() && !gateway_pids_gone(previous).await {
                    self.failure(
                        "previous Gateway process survived collector EOF at replacement readiness",
                    );
                }
            }
            let mut state = self.0.lock().unwrap();
            match (value["type"].as_str(), session) {
                (Some("ready"), None) => {
                    let numbers = [
                        "pid",
                        "bot_id",
                        "shard_id",
                        "ownership_epoch",
                        "test_cluster_node_id",
                        "guardian_pid",
                    ];
                    if numbers.iter().any(|key| !value[key].is_u64())
                        || value["test_cluster_node_id"] != node
                        || value["total_shards"] != 3
                        || value["mode"] != "mock_discord"
                        || value["credential_present"] != false
                        || !value["workers"].as_array().is_some_and(|workers| {
                            workers.len() == 2
                                && workers
                                    .iter()
                                    .all(|w| w["credential_free"] == true && w["pid"].is_u64())
                        })
                    {
                        state
                            .failures
                            .push("invalid Gateway identity or credential isolation");
                        break;
                    }
                    let bot = value["bot_id"].as_u64().unwrap();
                    let shard = value["shard_id"].as_u64().unwrap() as u32;
                    if state
                        .gateways
                        .iter()
                        .any(|g| g.bot == bot && g.shard == shard && g.active())
                    {
                        state.failures.push("two real Gateway processes were simultaneously Active for one Bot/Shard");
                    }
                    let id = state.gateways.len();
                    state.gateways.push(Gateway {
                        session: id,
                        node,
                        bot,
                        shard,
                        pid: value["pid"].as_u64().unwrap() as u32,
                        epoch: value["ownership_epoch"].as_u64().unwrap(),
                        ready: value,
                        ready_at: observed_at,
                        stopped_at: None,
                        disconnected_at: None,
                        input: Arc::new(tokio::sync::Mutex::new(writer.take().unwrap())),
                    });
                    session = Some(id);
                }
                (Some("stopped"), Some(id)) => {
                    let gateway = &mut state.gateways[id];
                    let workers: BTreeSet<_> = gateway.ready["workers"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|w| w["pid"].as_u64().unwrap())
                        .collect();
                    let stopped: Option<BTreeSet<u64>> = value["worker_pids"]
                        .as_array()
                        .and_then(|v| v.iter().map(Value::as_u64).collect());
                    if value["pid"] != gateway.pid
                        || stopped.as_ref() != Some(&workers)
                        || gateway.stopped_at.is_some()
                    {
                        state
                            .failures
                            .push("invalid or duplicate drained Gateway acknowledgement");
                    } else {
                        gateway.stopped_at = Some(Instant::now());
                    }
                }
                (Some("status" | "result"), Some(id)) if value["id"].is_u64() => {
                    let request_id = value["id"].as_u64().unwrap();
                    if state.replies.insert((id, request_id), value).is_some() {
                        state.failures.push("duplicate Gateway reply");
                    }
                }
                _ => state.failures.push("unexpected mock Gateway message"),
            }
        }
        if let Some(id) = session {
            self.0.lock().unwrap().gateways[id].disconnected_at = Some(Instant::now());
        }
    }
}

struct Collector {
    address: SocketAddr,
    task: JoinHandle<()>,
}
impl Collector {
    async fn new(node: u64, monitor: Monitor) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let monitor = monitor.clone();
                        connections.spawn(async move { monitor.connection(stream, node).await; });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        if matches!(joined, Some(Err(_))) { monitor.failure("collector task failed"); }
                    }
                }
            }
        });
        Self { address, task }
    }
}
impl Drop for Collector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Node {
    child: Child,
    stderr: Option<ChildStderr>,
    port: u16,
}
pub struct Cluster {
    pub monitor: Monitor,
    pub root: PathBuf,
    nodes: BTreeMap<u64, Node>,
    collectors: Vec<Collector>,
    proxies: BTreeMap<(u64, u64), Proxy>,
    reservations: BTreeMap<u64, std::net::TcpListener>,
}

impl Cluster {
    pub async fn prepared() -> Self {
        let root = directory();
        certificates(&root);
        let monitor = Monitor::default();
        let mut collectors = Vec::new();
        let mut reservations = BTreeMap::new();
        let mut addresses = BTreeMap::new();
        for id in 1..=3 {
            collectors.push(Collector::new(id, monitor.clone()).await);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addresses.insert(id, listener.local_addr().unwrap());
            reservations.insert(id, listener);
        }
        let mut proxies = BTreeMap::new();
        for source in 1..=3 {
            for target in 1..=3 {
                if source != target {
                    proxies.insert((source, target), Proxy::new(addresses[&target]).await);
                }
            }
        }
        for id in 1..=3 {
            let name = node_name(id);
            let path = |name: &str| root.join(name).to_string_lossy().replace('\\', "/");
            let mut config = format!(
                r#"[cluster]
id = "cluster-e2e"
mode = "quorum"
[node]
id = "{name}"
[failover]
heartbeat_ms = {HEARTBEAT_MS}
suspect_after_ms = 300
unhealthy_after_ms = {FAIL_MS}
failback_stable_ms = {STABLE_MS}
[test_cluster]
node_id = {id}
listen = "{}"
cert_path = "{}"
key_path = "{}"
ca_path = "{}"
lease_ms = {LEASE_MS}
stop_margin_ms = {STOP_MARGIN_MS}
"#,
                addresses[&id],
                path(&format!("node-{id}.der")),
                path(&format!("node-{id}.key.der")),
                path("ca.der")
            );
            for peer in (1..=3).filter(|peer| *peer != id) {
                config.push_str(&format!(
                    "\n[[test_cluster.peers]]\nid = {peer}\nname = \"{}\"\naddress = \"{}\"\n",
                    node_name(peer),
                    proxies[&(id, peer)].address
                ));
            }
            config.push_str("\n[[bots]]\nname = \"alpha\"\nbot_id = \"101\"\napplication_id = \"101\"\ntoken_env = \"TOKEN_ALPHA\"\ntotal_shards = 3\n");
            for shard in 0..3 {
                config.push_str(&format!("\n[[bots.shard_placement]]\nshard_id = {shard}\nprimary = \"{}\"\nfailover = [\"{}\", \"{}\"]\n", node_name(shard + 1), node_name((shard + 1) % 3 + 1), node_name((shard + 2) % 3 + 1)));
            }
            std::fs::write(root.join(format!("node-{id}.toml")), config).unwrap();
        }
        Self {
            monitor,
            root,
            nodes: BTreeMap::new(),
            collectors,
            proxies,
            reservations,
        }
    }
    pub async fn new() -> Self {
        let mut fixture = Self::prepared().await;
        for id in 1..=3 {
            fixture.start(id).await;
        }
        for id in 1..=3 {
            fixture.monitor.owner((id - 1) as u32, id).await;
        }
        assert_eq!(fixture.monitor.active().len(), 3);
        fixture
    }
    pub fn command(&self, id: u64, allow: bool, mock: bool) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(self.root.join(format!("node-{id}.toml")))
            .arg("--runtime-root")
            .arg(self.root.join(format!("runtime-{id}")))
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .arg("--bootstrap-stdin")
            .env("DISCORD_TOKEN", POISON)
            .env("TOKEN_ALPHA", POISON)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if allow {
            command.arg("--allow-test-quorum");
        }
        if mock {
            command
                .arg("--mock-discord")
                .arg(self.collectors[id as usize - 1].address.to_string());
        }
        command
    }
    pub async fn start(&mut self, id: u64) {
        assert!(!self.nodes.contains_key(&id));
        self.release_listener(id);
        let mut child = self.command(id, true, true).spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret": SECRET.to_vec()})).unwrap();
        bootstrap.push(b'\n');
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&bootstrap)
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let read = tokio::time::timeout(
            WAIT,
            BufReader::new(child.stdout.take().unwrap())
                .take(FRAME_LIMIT)
                .read_until(b'\n', &mut bytes),
        )
        .await;
        if !matches!(read, Ok(Ok(n)) if n > 0) {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let mut stderr = Vec::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_end(&mut stderr)
                .await
                .unwrap();
            no_secrets(&stderr);
            panic!(
                "botd failed to announce: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        no_secrets(&bytes);
        let value: Value = serde_json::from_slice(&bytes).expect("bounded botd announcement");
        assert_eq!(value["startup_ok"], true);
        assert_eq!(value["local_only"], true);
        let port = value["port"].as_u64().unwrap() as u16;
        let stderr = child.stderr.take();
        self.nodes.insert(
            id,
            Node {
                child,
                stderr,
                port,
            },
        );
    }
    pub fn release_listener(&mut self, id: u64) {
        self.reservations.remove(&id);
    }
    pub async fn kill(&mut self, id: u64) {
        let mut node = self.nodes.remove(&id).unwrap();
        node.child.start_kill().unwrap();
        tokio::time::timeout(WAIT, node.child.wait())
            .await
            .unwrap()
            .unwrap();
        if let Some(mut stderr) = node.stderr.take() {
            let mut bytes = Vec::new();
            tokio::time::timeout(WAIT, stderr.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            no_secrets(&bytes);
        }
    }
    pub async fn status(&self, id: u64) -> Value {
        self.request(id, json!({"command":"status"})).await
    }
    pub async fn request(&self, id: u64, mut value: Value) -> Value {
        self.monitor.check();
        value["secret"] = json!(SECRET.to_vec());
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        let value = tokio::time::timeout(WAIT, async {
            let mut stream = TcpStream::connect(("127.0.0.1", self.nodes[&id].port))
                .await
                .unwrap();
            stream.write_all(&bytes).await.unwrap();
            let mut bytes = Vec::new();
            BufReader::new(stream)
                .take(FRAME_LIMIT + 1)
                .read_until(b'\n', &mut bytes)
                .await
                .unwrap();
            assert!(bytes.len() as u64 <= FRAME_LIMIT);
            no_secrets(&bytes);
            serde_json::from_slice(&bytes).expect("authenticated botd JSON response")
        })
        .await
        .unwrap();
        self.monitor.check();
        value
    }
    pub async fn isolate(&self, id: u64, isolated: bool) {
        for (&(from, to), proxy) in &self.proxies {
            if from == id || to == id {
                proxy.set(!isolated).await;
            }
        }
        self.monitor.check();
    }
    pub async fn finish(mut self) {
        // Killing the owned botd handles is also a watchdog regression. The
        // independent guardians must still stop/reap their Gateway/Worker tree.
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.kill(id).await;
        }
        self.monitor
            .wait_for("all cluster Gateways disconnected", || {
                let all = self.monitor.all();
                all.iter()
                    .all(|g| g.disconnected_at.is_some())
                    .then_some(())
            })
            .await;
        let pids: BTreeSet<_> = self.monitor.all().iter().flat_map(Gateway::pids).collect();
        assert_exited(pids.into_iter().collect()).await;
        self.monitor.check();
        // Only this fixture's freshly allocated directory is ever removed.
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn node_name(id: u64) -> &'static str {
    match id {
        1 => "node-a",
        2 => "node-b",
        3 => "node-c",
        _ => unreachable!(),
    }
}

pub async fn assert_exited(pids: Vec<u32>) {
    if pids.is_empty() {
        return;
    }
    let deadline = Instant::now() + WAIT;
    loop {
        let gone = gateway_pids_gone(pids.clone()).await;
        if gone {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "observed fixture processes did not exit"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

async fn gateway_pids_gone(pids: Vec<u32>) -> bool {
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            let ids = pids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
            // Only numeric IDs observed from our children enter this expression.
            // A completion marker and exit status distinguish query failure from
            // absence. No process names, command lines or environments are read.
            let output = std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command"])
                .arg(format!("Get-Process -ErrorAction Stop | Where-Object {{ $_.Id -in @({ids}) }} | ForEach-Object {{ $_.Id }}; 'probe-complete'"))
                .creation_flags(0x08000000)
                .output()
                .expect("read-only process inspection");
            assert!(output.status.success() && output.stderr.is_empty(), "process inspection failed");
            let text = String::from_utf8_lossy(&output.stdout);
            assert_eq!(text.lines().last(), Some("probe-complete"), "incomplete process inspection");
            text.lines().count() == 1
        }
        #[cfg(target_os = "linux")]
        {
            pids.iter()
                .all(|pid| !Path::new(&format!("/proc/{pid}")).exists())
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            pids.iter().all(|pid| {
                !std::process::Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .unwrap()
                    .success()
            })
        }
    })
    .await
    .expect("process inspection task")
}

#[tokio::test]
async fn process_probe_detects_the_current_real_process() {
    assert!(!gateway_pids_gone(vec![std::process::id()]).await);
}
