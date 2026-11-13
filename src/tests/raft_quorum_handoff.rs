//! OpenRaft/file-store handoff regression tests with in-memory RPC delivery.
//! The shared injected clock advances the full 300-second quarantine logically;
//! these are not elapsed-time, separate-host, OS-suspend or process-fencing tests.
use openraft::{Config, ServerState, SnapshotPolicy};
use src::{
    ownership::{Clock, OwnershipCoordinator, OwnershipError, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    raft_quorum::{
        Command, NodeOptions, Outcome, Ownership, OwnershipData, QuorumCoordinator, RaftNode,
        ShardKey, StorageMode, TAKEOVER_QUARANTINE, transport::MemoryTransport,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, sleep};

const WAIT: Duration = Duration::from_secs(60);
const TTL: Duration = Duration::from_secs(10);
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "raftq-handoff-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create a new owned fixture directory");
        Self(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if let (Ok(path), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && path.parent() == Some(parent.as_path())
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("raftq-handoff-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn members() -> BTreeSet<u64> {
    BTreeSet::from([1, 2, 3])
}

fn options() -> NodeOptions {
    NodeOptions {
        allow_test_mode: true,
        enable_production: false,
        write_timeout: Duration::from_secs(2),
        // Exercise the global maximum, not only a node's shorter default TTL.
        max_lease_ttl: Duration::from_secs(300),
        raft_config: Config {
            cluster_name: "raftq-handoff-fixture".into(),
            heartbeat_interval: 50,
            election_timeout_min: 350,
            election_timeout_max: 700,
            snapshot_policy: SnapshotPolicy::Never,
            ..Config::default()
        },
    }
}

struct Cluster {
    root: Arc<TestRoot>,
    nodes: BTreeMap<u64, Arc<RaftNode>>,
    transport: MemoryTransport,
}

impl Cluster {
    async fn new() -> Self {
        let mut cluster = Self {
            root: Arc::new(TestRoot::new()),
            nodes: BTreeMap::new(),
            transport: MemoryTransport::default(),
        };
        for id in members() {
            cluster.start(id, StorageMode::Create).await;
        }
        cluster.nodes[&1].initialize().await.unwrap();
        cluster.leader().await;
        cluster
    }

    async fn start(&mut self, id: u64, mode: StorageMode) {
        let node = Arc::new(
            RaftNode::start(
                self.root.0.join(format!("node-{id}")),
                id,
                members(),
                mode,
                options(),
                Arc::new(self.transport.clone()),
            )
            .await
            .unwrap(),
        );
        self.transport.register(id, node.raft.clone()).unwrap();
        assert!(self.nodes.insert(id, node).is_none());
    }

    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            for (&id, node) in &self.nodes {
                let metrics = node.raft.metrics().borrow().clone();
                if metrics.state == ServerState::Leader
                    && metrics.current_leader == Some(id)
                    && self
                        .nodes
                        .values()
                        .all(|peer| peer.raft.metrics().borrow().current_leader == Some(id))
                {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "leader election did not finish");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn data(&self, id: u64) -> OwnershipData {
        self.nodes[&id].machine.data().await.unwrap()
    }

    async fn converged(&self, expected: &OwnershipData) {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut same = true;
            for id in self.nodes.keys() {
                same &= self.data(*id).await == *expected;
            }
            if same {
                return;
            }
            assert!(Instant::now() < deadline, "state did not converge");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn epoch(&self, id: u64, key: OwnershipKey, expected: u64) {
        let deadline = Instant::now() + WAIT;
        loop {
            if self
                .data(id)
                .await
                .0
                .get(&key.into())
                .is_some_and(|record| record.epoch == expected && record.held)
            {
                return;
            }
            assert!(Instant::now() < deadline, "pending command did not commit");
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn freeze_elections(&self) {
        for node in self.nodes.values() {
            node.raft.runtime_config().elect(false);
        }
    }

    fn isolate(&self, id: u64) {
        for peer in members().into_iter().filter(|peer| *peer != id) {
            self.transport.block(id, peer);
            self.transport.block(peer, id);
        }
    }

    async fn stop_all(&mut self) {
        for id in self.nodes.keys() {
            self.transport.remove(*id);
        }
        for node in std::mem::take(&mut self.nodes).into_values() {
            node.shutdown().await.unwrap();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let nodes = std::mem::take(&mut self.nodes);
        for id in nodes.keys() {
            self.transport.remove(*id);
        }
        if !nodes.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let root = self.root.clone();
            runtime.spawn(async move {
                for node in nodes.into_values() {
                    let _ = node.shutdown().await;
                }
                drop(root);
            });
        }
    }
}

#[derive(Clone, Default)]
struct TestClock(Arc<Mutex<Duration>>);

impl TestClock {
    fn set(&self, now: Duration) {
        *self.0.lock().unwrap() = now;
    }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}

fn key(shard_id: u32) -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(400),
        shard_id,
    }
}

fn old_renew(key: OwnershipKey, old: Ownership) -> Command {
    Command::Renew {
        key: key.into(),
        epoch: old.epoch,
        owner: old.owner,
        session: old.session,
    }
}

fn old_release(key: OwnershipKey, old: Ownership) -> Command {
    Command::Release {
        key: key.into(),
        epoch: old.epoch,
        owner: old.owner,
        session: old.session,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_waits_for_maximum_lease_and_blocks_early_release() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    let follower = members().into_iter().find(|id| *id != leader).unwrap();
    let clock = TestClock::default();
    let k = key(0);
    let mut old = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let lease = old
        .acquire_quorum(k, OwnershipEpoch(0), Duration::from_secs(300))
        .await
        .unwrap();
    let old_record = cluster.data(leader).await.0[&ShardKey::from(k)];
    let mut next = QuorumCoordinator::new(cluster.nodes[&follower].clone(), clock.clone()).unwrap();
    let epoch = next.begin_takeover(k, lease.epoch).await.unwrap();
    assert_eq!(epoch, OwnershipEpoch(2));
    assert_eq!(next.takeover_ready_at(k), Some(TAKEOVER_QUARANTINE));
    assert!(next.check(k, epoch).is_err());
    assert!(next.acquire(k, TTL).is_err());
    assert!(next.release_quorum(k, epoch).await.is_err());
    next.release(k, epoch).unwrap();
    assert_eq!(next.takeover_ready_at(k), Some(TAKEOVER_QUARANTINE));
    assert!(next.release_quorum(k, epoch).await.is_err());
    // Fencing the replicated reservation rejects delayed operations from the
    // previous session, but its existing local lease still needs to expire.
    for command in [old_renew(k, old_record), old_release(k, old_record)] {
        assert_eq!(
            cluster.nodes[&leader].submit(command).await.unwrap(),
            Outcome::Stale { current_epoch: 2 }
        );
    }
    clock.set(TAKEOVER_QUARANTINE - Duration::from_nanos(1));
    old.check(k, lease.epoch).unwrap();
    assert!(next.finish_takeover(k, epoch, TTL).await.is_err());
    assert!(next.release_quorum(k, epoch).await.is_err());
    clock.set(TAKEOVER_QUARANTINE);
    assert_eq!(old.check(k, lease.epoch), Err(OwnershipError::Expired));
    assert_eq!(old.watchdog().unwrap(), vec![k]);
    let next_lease = next.finish_takeover(k, epoch, TTL).await.unwrap();
    assert_eq!(next_lease.expires_at, TAKEOVER_QUARANTINE + TTL);
    next.check(k, epoch).unwrap();
    assert_eq!(
        cluster.data(leader).await.0[&ShardKey::from(k)].owner,
        follower
    );
    next.release_quorum(k, epoch).await.unwrap();
    assert!(!cluster.data(leader).await.0[&ShardKey::from(k)].held);
    drop(old);
    drop(next);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_keeps_fence_epoch_but_requires_a_new_full_quarantine() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    let clock = TestClock::default();
    let k = key(1);
    let mut old = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    old.acquire_quorum(k, OwnershipEpoch(0), TTL).await.unwrap();
    let mut pending =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    assert_eq!(
        pending.begin_takeover(k, OwnershipEpoch(1)).await.unwrap(),
        OwnershipEpoch(2)
    );
    let durable = cluster.data(leader).await;
    cluster.converged(&durable).await;
    drop(old);
    drop(pending);
    cluster.stop_all().await;
    clock.set(Duration::from_secs(150));
    for id in members() {
        cluster.start(id, StorageMode::Open).await;
    }
    let leader = cluster.leader().await;
    cluster.converged(&durable).await;
    let mut replacement =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    assert!(replacement.check(k, OwnershipEpoch(2)).is_err());
    assert_eq!(replacement.takeover_ready_at(k), None);
    assert!(
        replacement
            .finish_takeover(k, OwnershipEpoch(2), TTL)
            .await
            .is_err()
    );
    assert!(
        replacement
            .release_quorum(k, OwnershipEpoch(2))
            .await
            .is_err()
    );
    let epoch = replacement
        .begin_takeover(k, OwnershipEpoch(2))
        .await
        .unwrap();
    assert_eq!(epoch, OwnershipEpoch(3));
    let ready_at = clock.now() + TAKEOVER_QUARANTINE;
    assert_eq!(replacement.takeover_ready_at(k), Some(ready_at));
    clock.set(TAKEOVER_QUARANTINE);
    assert!(replacement.finish_takeover(k, epoch, TTL).await.is_err());
    clock.set(ready_at);
    replacement.finish_takeover(k, epoch, TTL).await.unwrap();
    replacement.check(k, epoch).unwrap();
    drop(replacement);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_fence_that_commits_later_cannot_start_a_wait_or_be_released() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    cluster.freeze_elections();
    let clock = TestClock::default();
    let k = key(2);
    let mut old = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    old.acquire_quorum(k, OwnershipEpoch(0), TTL).await.unwrap();
    let durable = cluster.data(leader).await;
    cluster.converged(&durable).await;
    let mut pending =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    cluster.isolate(leader);
    let previous_log = cluster.nodes[&leader]
        .raft
        .metrics()
        .borrow()
        .last_log_index;
    let mut begin = Box::pin(pending.begin_takeover(k, OwnershipEpoch(1)));
    tokio::select! {
        result = &mut begin => panic!("isolated fence unexpectedly finished: {result:?}"),
        _ = async {
            let deadline = Instant::now() + WAIT;
            while cluster.nodes[&leader].raft.metrics().borrow().last_log_index <= previous_log {
                assert!(Instant::now() < deadline, "fence was not appended locally");
                sleep(Duration::from_millis(10)).await;
            }
        } => {}
    }
    drop(begin);
    assert_eq!(pending.takeover_ready_at(k), None);
    assert!(pending.release_quorum(k, OwnershipEpoch(2)).await.is_err());
    assert_eq!(cluster.data(leader).await, durable);
    // The uncommitted fence can commit much later; counting from submission or
    // client cancellation would be unsafe if old renewals precede that commit.
    clock.set(TAKEOVER_QUARANTINE + Duration::from_secs(100));
    cluster.transport.heal();
    cluster.epoch(leader, k, 2).await;
    assert_eq!(pending.takeover_ready_at(k), None);
    assert!(
        pending
            .finish_takeover(k, OwnershipEpoch(2), TTL)
            .await
            .is_err()
    );
    assert!(pending.release_quorum(k, OwnershipEpoch(2)).await.is_err());
    assert!(pending.check(k, OwnershipEpoch(2)).is_err());
    let mut recovery =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let epoch = recovery.begin_takeover(k, OwnershipEpoch(2)).await.unwrap();
    assert_eq!(epoch, OwnershipEpoch(3));
    let ready_at = clock.now() + TAKEOVER_QUARANTINE;
    assert_eq!(recovery.takeover_ready_at(k), Some(ready_at));
    clock.set(ready_at - Duration::from_nanos(1));
    assert!(recovery.finish_takeover(k, epoch, TTL).await.is_err());
    clock.set(ready_at);
    recovery.finish_takeover(k, epoch, TTL).await.unwrap();
    drop(old);
    drop(pending);
    drop(recovery);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_finish_cannot_extend_the_deadline_and_retry_needs_quorum() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    cluster.freeze_elections();
    let clock = TestClock::default();
    let k = key(3);
    let mut old = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    old.acquire_quorum(k, OwnershipEpoch(0), TTL).await.unwrap();
    let mut pending =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let epoch = pending.begin_takeover(k, OwnershipEpoch(1)).await.unwrap();
    clock.set(TAKEOVER_QUARANTINE);
    cluster.isolate(leader);
    let previous_log = cluster.nodes[&leader]
        .raft
        .metrics()
        .borrow()
        .last_log_index;
    let mut finish = Box::pin(pending.finish_takeover(k, epoch, TTL));
    tokio::select! {
        result = &mut finish => panic!("isolated finish unexpectedly finished: {result:?}"),
        _ = async {
            let deadline = Instant::now() + WAIT;
            while cluster.nodes[&leader].raft.metrics().borrow().last_log_index <= previous_log {
                assert!(Instant::now() < deadline, "finish was not appended locally");
                sleep(Duration::from_millis(10)).await;
            }
        } => {}
    }
    clock.set(TAKEOVER_QUARANTINE + TTL);
    cluster.transport.heal();
    assert_eq!(finish.await, Err(OwnershipError::Expired));
    assert!(pending.check(k, epoch).is_err());
    assert!(pending.release_quorum(k, epoch).await.is_err());
    cluster.isolate(leader);
    assert!(pending.finish_takeover(k, epoch, TTL).await.is_err());
    assert!(pending.check(k, epoch).is_err());
    cluster.transport.heal();
    let granted = pending.finish_takeover(k, epoch, TTL).await.unwrap();
    assert_eq!(granted.expires_at, clock.now() + TTL);
    pending.check(k, epoch).unwrap();
    drop(old);
    drop(pending);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_competing_fence_invalidates_the_pending_candidate_and_restarts_the_wait() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    let clock = TestClock::default();
    let k = key(4);
    let mut old = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    old.acquire_quorum(k, OwnershipEpoch(0), TTL).await.unwrap();
    let mut first = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let first_epoch = first.begin_takeover(k, OwnershipEpoch(1)).await.unwrap();
    clock.set(Duration::from_secs(50));
    let mut second = QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let second_epoch = second.begin_takeover(k, first_epoch).await.unwrap();
    assert_eq!(second_epoch, OwnershipEpoch(3));
    let ready_at = clock.now() + TAKEOVER_QUARANTINE;
    clock.set(TAKEOVER_QUARANTINE);
    assert_eq!(
        first.finish_takeover(k, first_epoch, TTL).await,
        Err(OwnershipError::Fenced)
    );
    assert!(first.check(k, first_epoch).is_err());
    assert!(first.release_quorum(k, first_epoch).await.is_err());
    assert!(second.finish_takeover(k, second_epoch, TTL).await.is_err());
    clock.set(ready_at);
    second.finish_takeover(k, second_epoch, TTL).await.unwrap();
    assert!(second.release_quorum(k, first_epoch).await.is_err());
    second.check(k, second_epoch).unwrap();
    drop(old);
    drop(first);
    drop(second);
    cluster.stop_all().await;
}
