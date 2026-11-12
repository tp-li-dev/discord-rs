//! Real botd/Raft process fixture. Both health and Raft RPC proxies preserve TLS.
// Reuse the established collector, credential checks and continuous PID monitor;
// its ACK-only process launcher is deliberately unused by this fixture.
#[allow(dead_code)]
#[path = "e2e_cluster_support.rs"]
mod process_support;

use process_support::{Cluster as ProcessFixture, Gateway, Monitor, no_secrets};
pub use process_support::{FAIL_MS, HEARTBEAT_MS, PROCESS_TESTS, STABLE_MS, assert_exited};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStderr},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub const LEASE_MS: u64 = 2000;
pub const WAIT: Duration = Duration::from_secs(25);
const FRAME_LIMIT: u64 = 64 * 1024;
const SECRET: [u8; 32] = [73; 32];

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

struct Node {
    child: Child,
    stderr: Option<ChildStderr>,
    port: u16,
}

pub struct Cluster {
    pub monitor: Monitor,
    processes: ProcessFixture,
    nodes: BTreeMap<u64, Node>,
    rpc_addresses: BTreeMap<u64, SocketAddr>,
    rpc_reservations: BTreeMap<u64, std::net::TcpListener>,
    rpc_proxies: BTreeMap<(u64, u64), Proxy>,
}

impl Cluster {
    pub async fn new() -> Self {
        let processes = ProcessFixture::prepared().await;
        let monitor = processes.monitor.clone();
        let mut rpc_addresses = BTreeMap::new();
        let mut rpc_reservations = BTreeMap::new();
        for id in 1..=3 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            rpc_addresses.insert(id, listener.local_addr().unwrap());
            rpc_reservations.insert(id, listener);
            let path = processes.root.join(format!("node-{id}.toml"));
            let configuration = std::fs::read_to_string(&path).unwrap().replace(
                &format!("lease_ms = {}", process_support::LEASE_MS),
                &format!("lease_ms = {LEASE_MS}"),
            );
            std::fs::write(path, configuration).unwrap();
        }
        let mut rpc_proxies = BTreeMap::new();
        for from in 1..=3 {
            for to in 1..=3 {
                if from != to {
                    rpc_proxies.insert((from, to), Proxy::new(rpc_addresses[&to]).await);
                }
            }
        }
        let mut cluster = Self {
            monitor,
            processes,
            nodes: BTreeMap::new(),
            rpc_addresses,
            rpc_reservations,
            rpc_proxies,
        };
        for id in 1..=3 {
            cluster.start(id, true).await;
        }
        for id in 1..=3 {
            cluster.monitor.owner((id - 1) as u32, id).await;
        }
        assert_eq!(cluster.monitor.active().len(), 3);
        cluster
    }

    async fn start(&mut self, id: u64, create: bool) {
        assert!(!self.nodes.contains_key(&id));
        self.processes.release_listener(id);
        self.rpc_reservations.remove(&id);
        // The reusable fixture supplies only the mock Gateway, bootstrap and
        // health configuration. Never opt into its majority-ACK runtime.
        let mut command = self.processes.command(id, false, true);
        command
            .arg("--allow-raft-quorum")
            .arg("--raft-rpc-listen")
            .arg(self.rpc_addresses[&id].to_string())
            .arg("--raft-store")
            .arg(self.processes.root.join(format!("raft-store-{id}")))
            .stderr(Stdio::piped());
        for peer in (1..=3).filter(|peer| *peer != id) {
            command
                .arg("--raft-rpc-peer")
                .arg(format!("{peer}={}", self.rpc_proxies[&(id, peer)].address,));
        }
        if create {
            command.arg("--raft-create");
            if id == 1 {
                command.arg("--raft-bootstrap");
            }
        }
        let mut child = command.spawn().unwrap();
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
                .take(FRAME_LIMIT + 1)
                .read_until(b'\n', &mut bytes),
        )
        .await;
        if !matches!(read, Ok(Ok(n)) if n > 0 && n as u64 <= FRAME_LIMIT) {
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
                "Raft botd failed to announce: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        no_secrets(&bytes);
        let value: Value = serde_json::from_slice(&bytes).expect("bounded Raft botd announcement");
        assert_eq!(value["startup_ok"], true);
        assert_eq!(value["local_only"], true);
        assert_eq!(value["mock_discord"], true);
        assert_eq!(value["raft_quorum"], true);
        assert_eq!(value["test_quorum"], false);
        assert_eq!(value["production_ready"], false);
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

    /// Same identity and persistent store: explicitly Open, never bootstrap.
    pub async fn restart(&mut self, id: u64) {
        self.start(id, false).await;
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

    pub async fn wait_status(&self, id: u64, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let status = self.status(id).await;
            if predicate(&status) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "Raft botd status did not converge"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn crash_gateway(&self, gateway: &Gateway) {
        let monitor = self.monitor.clone();
        let target = gateway.clone();
        // The mock-only crash input has no reply. Keep its sender alive until
        // EOF proves delivery, then cancel only the request's reply waiter.
        let sender =
            tokio::spawn(async move { monitor.request(&target, json!({"type":"crash"})).await });
        self.monitor.ended(gateway.session).await;
        sender.abort();
        let _ = sender.await;
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
            serde_json::from_slice(&bytes).expect("authenticated Raft botd JSON response")
        })
        .await
        .unwrap();
        self.monitor.check();
        value
    }

    pub async fn isolate_rpc(&self, id: u64, isolated: bool) {
        for (&(from, to), proxy) in &self.rpc_proxies {
            if from == id || to == id {
                proxy.set(!isolated).await;
            }
        }
        self.monitor.check();
    }

    pub async fn isolate(&self, id: u64, isolated: bool) {
        self.isolate_rpc(id, isolated).await;
        self.processes.isolate(id, isolated).await;
    }

    pub async fn finish(mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.kill(id).await;
        }
        // This also checks guardian/Gateway/Core PID disappearance and removes
        // only the fresh fixture directory after every observed tree has exited.
        self.processes.finish().await;
    }
}
