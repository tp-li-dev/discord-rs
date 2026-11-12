//! Three independent Raft runtimes on real TCP/mTLS in ONE OS process.
//! Proxies never terminate TLS. Mock Gateway admission uses its own clock and
//! survives runtime shutdown, so lack of a runtime tick cannot immortalize it.
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use src::{
    cluster::NodeId,
    cluster_runtime::{
        ClusterAction, ClusterRuntime, LoadedClusterConfig, RaftRuntimeOptions, TestClusterConfig,
        TestClusterPeer,
    },
    daemon::ShardKey,
    placement,
    raft_quorum::StorageMode,
    testkit::{MockDiscord, MockReply},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub const HEARTBEAT_MS: u64 = 100;
pub const FAIL_MS: u64 = 600;
pub const STABLE_MS: u64 = 1200;
pub const LEASE_MS: u64 = 2000;
pub const STOP_MARGIN_MS: u64 = 500;
pub const WAIT: Duration = Duration::from_secs(25);
pub static SERIAL: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

pub fn key(bot: u64) -> ShardKey {
    ShardKey {
        bot_id: bot.to_string(),
        shard_id: 0,
    }
}

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let root = Self(std::env::temp_dir().join(format!("raft-runtime-{suffix}")));
        std::fs::create_dir(&root.0).unwrap();
        root
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if let (Ok(path), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && path.parent() == Some(parent.as_path())
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("raft-runtime-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn certificates(root: &Path) {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "ephemeral runtime test CA");
    let ca = params.self_signed(&ca_key).unwrap();
    std::fs::write(root.join("ca.der"), ca.der()).unwrap();
    for id in 1..=3 {
        let node_key = KeyPair::generate().unwrap();
        let name = format!("node-{id}.cluster");
        let mut params = CertificateParams::new(vec![name.clone()]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let certificate = params.signed_by(&node_key, &ca, &ca_key).unwrap();
        std::fs::write(root.join(format!("node-{id}.der")), certificate.der()).unwrap();
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(root.join(format!("node-{id}.key.der")))
            .unwrap()
            .write_all(&node_key.serialize_der())
            .unwrap();
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
                            if let Ok(Ok(mut outgoing)) = tokio::time::timeout(
                                Duration::from_secs(1), TcpStream::connect(target),
                            ).await {
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

#[derive(Clone, Debug)]
pub struct Gateway {
    pub node: u64,
    pub key: ShardKey,
    pub epoch: u64,
    pub started: Instant,
    pub expires: Instant,
    pub ended: Option<Instant>,
    pub reaped: Option<Instant>,
    pub stop_reason: Option<&'static str>,
    stop_at: Option<Instant>,
    notify_runtime: bool,
}

#[derive(Default)]
struct Observations {
    gateways: Vec<Gateway>,
    failures: Vec<&'static str>,
    withheld_exits: BTreeSet<(u64, ShardKey)>,
}
impl Observations {
    fn poll(&mut self) {
        let now = Instant::now();
        let mut active = BTreeSet::new();
        for gate in &mut self.gateways {
            let deadline = gate
                .stop_at
                .map_or(gate.expires, |stop| stop.min(gate.expires));
            if gate.ended.is_none() && now >= deadline {
                gate.ended = Some(deadline);
            }
            if gate.ended.is_none() && !active.insert(gate.key.clone()) {
                self.failures
                    .push("two simultaneously admitted mock Gateways");
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct Monitor(Arc<Mutex<Observations>>);
impl Monitor {
    pub fn check(&self) {
        let mut state = self.0.lock().unwrap();
        state.poll();
        assert!(
            state.failures.is_empty(),
            "admission violations: {:?}",
            state.failures
        );
    }

    fn apply(&self, node: u64, action: &ClusterAction) {
        let mut state = self.0.lock().unwrap();
        state.poll();
        match action {
            ClusterAction::Start {
                key,
                epoch,
                expires_at,
            } => {
                if *expires_at <= Instant::now() {
                    state.failures.push("Start used an expired lease");
                    return;
                }
                state.gateways.push(Gateway {
                    node,
                    key: key.clone(),
                    epoch: *epoch,
                    started: Instant::now(),
                    expires: *expires_at,
                    ended: None,
                    reaped: None,
                    stop_reason: None,
                    stop_at: None,
                    notify_runtime: true,
                });
            }
            ClusterAction::Renew {
                key,
                epoch,
                expires_at,
            } => {
                let gate = state
                    .gateways
                    .iter_mut()
                    .rev()
                    .find(|gate| gate.node == node && gate.key == *key && gate.ended.is_none());
                match gate {
                    Some(gate)
                        if gate.epoch == *epoch
                            && gate.stop_at.is_none()
                            && *expires_at >= gate.expires =>
                    {
                        gate.expires = *expires_at;
                    }
                    _ => state.failures.push("late, stale or shortened renewal"),
                }
            }
            ClusterAction::Stop { key, reason } => {
                if let Some(gate) = state
                    .gateways
                    .iter_mut()
                    .rev()
                    .find(|gate| gate.node == node && gate.key == *key && gate.ended.is_none())
                {
                    // A successful shutdown write is deliberately NOT exit.
                    // Keep the delegated gate alive while simulated reaping is pending.
                    gate.stop_at
                        .get_or_insert(Instant::now() + Duration::from_millis(250));
                    gate.stop_reason = Some(reason);
                }
            }
        }
        state.poll();
    }

    fn detach_runtime(&self, node: u64) {
        for gate in &mut self.0.lock().unwrap().gateways {
            if gate.node == node {
                gate.notify_runtime = false;
            }
        }
    }

    fn exited(&self, paused: Option<u64>) -> Vec<(u64, ShardKey, u64)> {
        let mut state = self.0.lock().unwrap();
        state.poll();
        let withheld = state.withheld_exits.clone();
        state
            .gateways
            .iter_mut()
            .filter(|gate| {
                gate.notify_runtime
                    && gate.ended.is_some()
                    && Some(gate.node) != paused
                    && !withheld.contains(&(gate.node, gate.key.clone()))
            })
            .map(|gate| {
                gate.notify_runtime = false;
                gate.reaped = Some(Instant::now());
                (gate.node, gate.key.clone(), gate.epoch)
            })
            .collect()
    }

    pub fn withhold_exit(&self, node: u64, key: &ShardKey, withhold: bool) {
        let mut state = self.0.lock().unwrap();
        if withhold {
            state.withheld_exits.insert((node, key.clone()));
        } else {
            state.withheld_exits.remove(&(node, key.clone()));
        }
    }

    pub fn active(&self) -> Vec<Gateway> {
        self.check();
        self.0
            .lock()
            .unwrap()
            .gateways
            .iter()
            .filter(|gate| gate.ended.is_none())
            .cloned()
            .collect()
    }

    pub fn owner(&self, key: &ShardKey, node: u64) -> Option<Gateway> {
        self.active()
            .into_iter()
            .find(|gate| gate.key == *key && gate.node == node)
    }

    pub fn history(&self) -> Vec<Gateway> {
        self.check();
        self.0.lock().unwrap().gateways.clone()
    }

    pub fn admit(&self, node: u64, key: &ShardKey, epoch: u64) -> bool {
        self.owner(key, node)
            .is_some_and(|gate| gate.epoch == epoch)
    }

    pub async fn mock_command(&self, gateway: &Gateway) {
        assert!(self.admit(gateway.node, &gateway.key, gateway.epoch));
        let mock = MockDiscord::new(vec![MockReply::json(200, b"{}").unwrap()]);
        let server = mock.spawn_tcp().await.unwrap();
        let mut socket = TcpStream::connect(server.addr()).await.unwrap();
        socket
            .write_all(b"POST /channels/1/messages HTTP/1.1\r\nHost: mock\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(WAIT, socket.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert_eq!(mock.stats().rest_requests, 1);
        server.shutdown().await.unwrap();
    }
}

pub struct Cluster {
    root: Root,
    pub nodes: BTreeMap<u64, ClusterRuntime>,
    configs: BTreeMap<u64, LoadedClusterConfig>,
    rpc_addresses: BTreeMap<u64, SocketAddr>,
    reservations: BTreeMap<(u64, bool), TcpListener>,
    proxies: BTreeMap<(u64, u64, bool), Proxy>,
    pub monitor: Monitor,
    watcher: JoinHandle<()>,
}
impl Cluster {
    pub async fn prepared() -> Self {
        let root = Root::new();
        certificates(&root.0);
        let mut reservations = BTreeMap::new();
        let mut addresses = BTreeMap::new();
        for node in 1..=3 {
            for rpc in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                addresses.insert((node, rpc), listener.local_addr().unwrap());
                reservations.insert((node, rpc), listener);
            }
        }
        let mut proxies = BTreeMap::new();
        for source in 1..=3 {
            for target in 1..=3 {
                if source == target {
                    continue;
                }
                for rpc in [false, true] {
                    proxies.insert(
                        (source, target, rpc),
                        Proxy::new(addresses[&(target, rpc)]).await,
                    );
                }
            }
        }
        let configs = (1..=3)
            .map(|node| {
                let loaded = LoadedClusterConfig {
                    placement: placement::Config {
                        cluster: placement::Cluster {
                            id: "raft-runtime-fixture".into(),
                            mode: placement::ClusterMode::Quorum,
                        },
                        node: placement::Node {
                            id: format!("node-{node}"),
                            remote_bind: None,
                        },
                        failover: placement::Failover {
                            heartbeat_ms: HEARTBEAT_MS,
                            suspect_after_ms: 300,
                            unhealthy_after_ms: FAIL_MS,
                            failback_stable_ms: STABLE_MS,
                        },
                        bots: [(101, 1, vec![2, 3]), (202, 3, vec![2, 1])]
                            .into_iter()
                            .map(|(bot, primary, fallback)| placement::BotPlacement {
                                name: format!("bot-{bot}"),
                                bot_id: bot.to_string(),
                                application_id: (bot + 1000).to_string(),
                                token_env: format!("UNREAD_FIXTURE_TOKEN_{bot}"),
                                total_shards: placement::ShardCount::Fixed(1),
                                prefix: None,
                                shard_placement: vec![placement::ShardPlacement {
                                    shard_id: 0,
                                    primary: format!("node-{primary}"),
                                    failover: fallback
                                        .into_iter()
                                        .map(|id| format!("node-{id}"))
                                        .collect(),
                                }],
                            })
                            .collect(),
                    },
                    test_cluster: Some(TestClusterConfig {
                        node_id: node,
                        listen: addresses[&(node, false)],
                        cert_path: root.0.join(format!("node-{node}.der")),
                        key_path: root.0.join(format!("node-{node}.key.der")),
                        ca_path: root.0.join("ca.der"),
                        lease_ms: LEASE_MS,
                        stop_margin_ms: STOP_MARGIN_MS,
                        peers: (1..=3)
                            .filter(|peer| *peer != node)
                            .map(|peer| TestClusterPeer {
                                id: peer,
                                name: format!("node-{peer}"),
                                address: proxies[&(node, peer, false)].address,
                            })
                            .collect(),
                    }),
                };
                (node, loaded)
            })
            .collect();
        let monitor = Monitor::default();
        let observe = monitor.clone();
        let watcher = tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_millis(5));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                observe.0.lock().unwrap().poll();
            }
        });
        Self {
            root,
            nodes: BTreeMap::new(),
            configs,
            rpc_addresses: (1..=3).map(|id| (id, addresses[&(id, true)])).collect(),
            reservations,
            proxies,
            monitor,
            watcher,
        }
    }

    pub fn config(&self, node: u64) -> &LoadedClusterConfig {
        &self.configs[&node]
    }

    pub fn options(&self, node: u64, mode: StorageMode) -> RaftRuntimeOptions {
        RaftRuntimeOptions {
            allow_raft: true,
            enable_production: false,
            storage_path: self.root.0.join(format!("store-{node}")),
            storage_mode: mode,
            bootstrap: node == 1 && mode == StorageMode::Create,
            rpc_listen: self.rpc_addresses[&node],
            rpc_peers: (1..=3)
                .filter(|peer| *peer != node)
                .map(|peer| (NodeId(peer), self.proxies[&(node, peer, true)].address))
                .collect(),
        }
    }

    pub async fn new() -> Self {
        let mut cluster = Self::prepared().await;
        // Only the final node explicitly bootstraps this freshly created fleet.
        for node in [3, 2, 1] {
            cluster.start(node, StorageMode::Create).await;
        }
        cluster
    }

    pub async fn start(&mut self, node: u64, mode: StorageMode) {
        self.reservations.remove(&(node, false));
        self.reservations.remove(&(node, true));
        let runtime = ClusterRuntime::bind_raft(self.config(node), self.options(node, mode), true)
            .await
            .unwrap();
        assert!(self.nodes.insert(node, runtime).is_none());
    }

    pub async fn stop(&mut self, node: u64) {
        self.monitor.detach_runtime(node);
        let mut runtime = self.nodes.remove(&node).unwrap();
        runtime.shutdown().await;
        // Delegated mock Gateways intentionally remain alive until their own
        // deadlines. Shutdown is not a fabricated process-exit acknowledgement.
    }

    pub async fn isolate(&self, node: u64, partitioned: bool) {
        for (&(source, target, _), proxy) in &self.proxies {
            if source == node || target == node {
                proxy.set(!partitioned).await;
            }
        }
    }

    pub async fn rpc_partition(&self, partitioned: bool) {
        for (&(_, _, rpc), proxy) in &self.proxies {
            if rpc {
                proxy.set(!partitioned).await;
            }
        }
    }

    pub async fn step(&mut self) {
        self.step_maybe_pausing(None).await;
    }

    pub async fn step_pausing(&mut self, node: u64) {
        self.step_maybe_pausing(Some(node)).await;
    }

    async fn step_maybe_pausing(&mut self, paused: Option<u64>) {
        self.monitor.check();
        for (node, key, epoch) in self.monitor.exited(paused) {
            if let Some(runtime) = self.nodes.get_mut(&node) {
                runtime.process_exited_epoch(&key, epoch).unwrap();
            }
        }
        let nodes = self.nodes.keys().copied().collect::<Vec<_>>();
        for node in nodes {
            if Some(node) != paused {
                self.tick_one(node);
            }
        }
        self.monitor.check();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    pub fn tick_one(&mut self, node: u64) -> Vec<ClusterAction> {
        let runtime = self.nodes.get_mut(&node).unwrap();
        let actions = runtime.tick().unwrap();
        for action in &actions {
            self.monitor.apply(node, action);
            if let ClusterAction::Start { key, epoch, .. } = action {
                // This is mock Gateway/Core readiness, not an OS process or
                // Discord READY assertion. The receipt remains epoch-bound.
                runtime.startup_complete_epoch(key, *epoch).unwrap();
            }
        }
        actions
    }

    pub async fn owner(&mut self, key: &ShardKey, node: u64) -> Gateway {
        let deadline = Instant::now() + WAIT;
        loop {
            self.step().await;
            if let Some(owner) = self.monitor.owner(key, node) {
                return owner;
            }
            assert!(
                Instant::now() < deadline,
                "owner did not appear: node={node}, key={key:?}, status={:?}",
                self.nodes
                    .iter()
                    .map(|(id, node)| (id, node.status()))
                    .collect::<Vec<_>>()
            );
        }
    }

    pub async fn stopping(&mut self, key: &ShardKey, node: u64, epoch: u64) -> Gateway {
        let deadline = Instant::now() + WAIT;
        loop {
            self.step().await;
            if let Some(gate) = self.monitor.history().into_iter().find(|gate| {
                gate.node == node
                    && gate.key == *key
                    && gate.epoch == epoch
                    && gate.stop_reason.is_some()
            }) {
                return gate;
            }
            assert!(
                Instant::now() < deadline,
                "old owner did not begin draining"
            );
        }
    }

    pub async fn hold(&mut self, duration: Duration, predicate: impl Fn(&Monitor) -> bool) {
        let until = Instant::now() + duration;
        while Instant::now() < until {
            self.step().await;
            assert!(
                predicate(&self.monitor),
                "condition failed during observation interval"
            );
        }
    }

    pub async fn no_active(&mut self) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.step().await;
            if self.monitor.active().is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "mock Gateways kept admission after quorum loss"
            );
        }
    }

    pub async fn finish(mut self) {
        for (_, mut runtime) in std::mem::take(&mut self.nodes) {
            runtime.shutdown().await;
        }
        self.monitor.check();
        self.watcher.abort();
        let _ = (&mut self.watcher).await;
        self.proxies.clear();
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}
