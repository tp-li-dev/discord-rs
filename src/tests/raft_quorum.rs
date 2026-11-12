//! Real OpenRaft tasks and durable files, with only the RPC delivery in memory.
//! These tests do not exercise mTLS, separate hosts, OS suspend or power loss.
use openraft::{Config, ServerState, SnapshotPolicy, storage::RaftStateMachine};
use src::{
    ownership::{Clock, OwnershipCoordinator, OwnershipError, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    raft_quorum::{
        Command, NodeOptions, Outcome, Ownership, OwnershipData, QuorumCoordinator, RaftNode,
        ShardKey, StorageMode, transport::MemoryTransport,
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

const WAIT: Duration = Duration::from_secs(15);
const TTL: Duration = Duration::from_secs(10);
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "raftq-raft-integration-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).expect("create a fresh owned test directory");
        Self(root)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        // Only an immediate child created by this fixture may be removed. A
        // failed test may leave files behind if Windows still holds a handle.
        if let (Ok(root), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && root.parent() == Some(parent.as_path())
            && root
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("raftq-raft-integration-"))
        {
            let _ = std::fs::remove_dir_all(root);
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
        max_lease_ttl: Duration::from_secs(30),
        raft_config: Config {
            cluster_name: "raftq-durable-test".into(),
            heartbeat_interval: 50,
            election_timeout_min: 300,
            election_timeout_max: 600,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 8,
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
        cluster.leader(&[1, 2, 3]).await;
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

    async fn leader(&self, candidates: &[u64]) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            for id in candidates {
                if let Some(node) = self.nodes.get(id) {
                    let metrics = node.raft.metrics().borrow().clone();
                    if metrics.state == ServerState::Leader && metrics.current_leader == Some(*id) {
                        return *id;
                    }
                }
            }
            assert!(Instant::now() < deadline, "leader election did not finish");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn data(&self, id: u64) -> OwnershipData {
        self.nodes[&id].machine.data().await.unwrap()
    }

    async fn converged(&self, ids: &[u64], expected: &OwnershipData) {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut matches = true;
            for id in ids {
                matches &= self.data(*id).await == *expected;
            }
            if matches {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "ownership state did not converge"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn isolate(&self, id: u64) {
        for peer in members().into_iter().filter(|peer| *peer != id) {
            self.transport.block(id, peer);
            self.transport.block(peer, id);
        }
    }

    async fn stop(&mut self, id: u64) {
        self.transport.remove(id);
        if let Some(node) = self.nodes.remove(&id) {
            node.shutdown().await.unwrap();
        }
    }

    async fn stop_all(&mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        // Remove all endpoints first: no other node can acquire a new handle
        // to a stopped peer during restart or fixture cleanup.
        for id in &ids {
            self.transport.remove(*id);
        }
        for id in ids {
            self.stop(id).await;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Assertion failures must also break the MemoryTransport/Raft handle
        // cycle. Normal success awaits stop_all explicitly.
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

fn key(bot_id: u64, shard_id: u32) -> ShardKey {
    ShardKey { bot_id, shard_id }
}

fn local_key(bot_id: u64, shard_id: u32) -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(bot_id),
        shard_id,
    }
}

fn acquire(key: ShardKey, expected_epoch: u64, owner: u64) -> Command {
    Command::Acquire {
        key,
        expected_epoch,
        owner,
        session: [owner as u8; 16],
    }
}

fn release(key: ShardKey, epoch: u64, owner: u64) -> Command {
    Command::Release {
        key,
        epoch,
        owner,
        session: [owner as u8; 16],
    }
}

fn applied(epoch: u64, owner: u64, held: bool) -> Outcome {
    Outcome::Applied(Ownership {
        epoch,
        owner,
        session: [owner as u8; 16],
        held,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_and_invalid_configuration_do_not_create_storage() {
    let root = TestRoot::new();
    let path = root.0.join("must-not-exist");
    assert!(
        RaftNode::start(
            &path,
            1,
            members(),
            StorageMode::Create,
            NodeOptions::default(),
            Arc::new(MemoryTransport::default()),
        )
        .await
        .is_err()
    );
    assert!(!path.exists());
    for (id, membership) in [(1, BTreeSet::from([1, 2])), (4, members())] {
        assert!(
            RaftNode::start(
                &path,
                id,
                membership,
                StorageMode::Create,
                options(),
                Arc::new(MemoryTransport::default()),
            )
            .await
            .is_err()
        );
        assert!(!path.exists());
    }
    let production = NodeOptions {
        allow_test_mode: false,
        enable_production: true,
        ..options()
    };
    assert!(
        RaftNode::start(
            &path,
            1,
            members(),
            StorageMode::Create,
            production,
            Arc::new(MemoryTransport::default()),
        )
        .await
        .is_err(),
        "memory RPC delivery must never enable production"
    );
    assert!(!path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_bot_and_shard_cas_replicates_without_stealing_a_held_owner() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    for (k, owner) in [(key(10, 0), 1), (key(10, 1), 2), (key(20, 0), 3)] {
        assert_eq!(
            cluster.nodes[&leader]
                .submit(acquire(k, 0, owner))
                .await
                .unwrap(),
            applied(1, owner, true)
        );
    }
    let k = key(10, 0);
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 1, 2))
            .await
            .unwrap(),
        Outcome::Held
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(release(k, 1, 2))
            .await
            .unwrap(),
        Outcome::NotOwned
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(release(k, 1, 1))
            .await
            .unwrap(),
        applied(1, 1, false)
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 1, 2))
            .await
            .unwrap(),
        applied(2, 2, true)
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 1, 3))
            .await
            .unwrap(),
        Outcome::Stale { current_epoch: 2 }
    );
    let committed = cluster.data(leader).await;
    assert_eq!(committed.0[&k].epoch, 2);
    assert_eq!(committed.0[&key(10, 1)].epoch, 1);
    assert_eq!(committed.0[&key(20, 0)].epoch, 1);
    cluster.converged(&[1, 2, 3], &committed).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_cas_has_one_winner_and_one_durable_epoch_increment() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let node = &cluster.nodes[&leader];
    let k = key(30, 0);
    let (a, b) = tokio::join!(node.submit(acquire(k, 0, 1)), node.submit(acquire(k, 0, 2)));
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|outcome| matches!(outcome, Outcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|outcome| matches!(outcome, Outcome::Stale { current_epoch: 1 }))
            .count(),
        1
    );
    let committed = cluster.data(leader).await;
    assert_eq!(committed.0[&k].epoch, 1);
    cluster.converged(&[1, 2, 3], &committed).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_loss_keeps_majority_working_and_majority_loss_never_grants() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let followers: Vec<_> = members().into_iter().filter(|id| *id != leader).collect();
    cluster.stop(followers[0]).await;
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(key(40, 0), 0, leader))
            .await
            .unwrap(),
        applied(1, leader, true)
    );
    let before = cluster.data(leader).await;
    cluster.converged(&[leader, followers[1]], &before).await;
    cluster.stop(followers[1]).await;
    for shard in 1..=2 {
        assert!(
            cluster.nodes[&leader]
                .submit(acquire(key(40, shard), 0, leader))
                .await
                .is_err()
        );
        assert_eq!(cluster.data(leader).await, before);
    }
    assert_eq!(
        cluster.nodes[&leader]
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .count(),
        3
    );
    // Timeout is an unknown outcome, not cancellation: no claim is made about
    // a timed-out request after connectivity is restored.
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_old_leader_cannot_advance_epoch_but_majority_can_and_heals() {
    let mut cluster = Cluster::new().await;
    let old = cluster.leader(&[1, 2, 3]).await;
    let k = key(50, 0);
    assert_eq!(
        cluster.nodes[&old]
            .submit(acquire(k, 0, old))
            .await
            .unwrap(),
        applied(1, old, true)
    );
    assert_eq!(
        cluster.nodes[&old]
            .submit(release(k, 1, old))
            .await
            .unwrap(),
        applied(1, old, false)
    );
    let before = cluster.data(old).await;
    cluster.converged(&[1, 2, 3], &before).await;
    cluster.isolate(old);
    let stale_leader = cluster.nodes[&old].clone();
    let pending = tokio::spawn(async move { stale_leader.submit(acquire(k, 1, old)).await });
    let majority: Vec<_> = members().into_iter().filter(|id| *id != old).collect();
    let new = cluster.leader(&majority).await;
    assert_ne!(new, old);
    assert_eq!(
        cluster.nodes[&new]
            .submit(acquire(k, 1, new))
            .await
            .unwrap(),
        applied(2, new, true)
    );
    assert!(pending.await.unwrap().is_err());
    assert_eq!(cluster.data(old).await, before);
    let committed = cluster.data(new).await;
    cluster.converged(&majority, &committed).await;
    cluster.transport.heal();
    cluster.converged(&[1, 2, 3], &committed).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_restart_preserves_epoch_and_held_reservation_without_restoring_local_lease() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let k = key(60, 0);
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 0, 1))
            .await
            .unwrap(),
        applied(1, 1, true)
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(release(k, 1, 1))
            .await
            .unwrap(),
        applied(1, 1, false)
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 1, 2))
            .await
            .unwrap(),
        applied(2, 2, true)
    );
    let before = cluster.data(leader).await;
    cluster.converged(&[1, 2, 3], &before).await;
    cluster.stop_all().await;
    for id in members() {
        cluster.start(id, StorageMode::Open).await;
    }
    let leader = cluster.leader(&[1, 2, 3]).await;
    cluster.converged(&[1, 2, 3], &before).await;
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 0, leader))
            .await
            .unwrap(),
        Outcome::Stale { current_epoch: 2 }
    );
    let mut coordinator =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), TestClock::default()).unwrap();
    assert!(
        coordinator
            .check(local_key(60, 0), OwnershipEpoch(2))
            .is_err()
    );
    assert!(
        coordinator
            .acquire_quorum(local_key(60, 0), OwnershipEpoch(2), TTL)
            .await
            .is_err()
    );
    assert_eq!(cluster.data(leader).await, before);
    // The trusted owner supplies release only after its old admission gate has
    // stopped. Restart itself is deliberately not such evidence.
    assert_eq!(
        cluster.nodes[&leader]
            .submit(release(k, 2, 2))
            .await
            .unwrap(),
        applied(2, 2, false)
    );
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 2, leader))
            .await
            .unwrap(),
        applied(3, leader, true)
    );
    drop(coordinator);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_shutdown_elects_successor_without_reusing_epoch() {
    let mut cluster = Cluster::new().await;
    let old = cluster.leader(&[1, 2, 3]).await;
    let k = key(70, 0);
    assert_eq!(
        cluster.nodes[&old]
            .submit(acquire(k, 0, old))
            .await
            .unwrap(),
        applied(1, old, true)
    );
    assert_eq!(
        cluster.nodes[&old]
            .submit(release(k, 1, old))
            .await
            .unwrap(),
        applied(1, old, false)
    );
    let before = cluster.data(old).await;
    cluster.converged(&[1, 2, 3], &before).await;
    cluster.stop(old).await;
    let remaining: Vec<_> = cluster.nodes.keys().copied().collect();
    let leader = cluster.leader(&remaining).await;
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 1, leader))
            .await
            .unwrap(),
        applied(2, leader, true)
    );
    let after = cluster.data(leader).await;
    cluster.converged(&remaining, &after).await;
    cluster.start(old, StorageMode::Open).await;
    cluster.converged(&[1, 2, 3], &after).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_catchup_and_restart_preserve_epoch_after_log_purge() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let lagging = members().into_iter().find(|id| *id != leader).unwrap();
    let majority: Vec<_> = members().into_iter().filter(|id| *id != lagging).collect();
    cluster.nodes[&lagging].raft.runtime_config().elect(false);
    for id in &majority {
        cluster.transport.block(*id, lagging);
    }
    let k = key(80, 0);
    for epoch in 0..8 {
        assert_eq!(
            cluster.nodes[&leader]
                .submit(acquire(k, epoch, leader))
                .await
                .unwrap(),
            applied(epoch + 1, leader, true)
        );
        assert_eq!(
            cluster.nodes[&leader]
                .submit(release(k, epoch + 1, leader))
                .await
                .unwrap(),
            applied(epoch + 1, leader, false)
        );
    }
    let committed = cluster.data(leader).await;
    cluster.converged(&majority, &committed).await;
    let last = cluster.nodes[&leader]
        .machine
        .clone()
        .applied_state()
        .await
        .unwrap()
        .0
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .trigger()
        .snapshot()
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .wait(Some(WAIT))
        .snapshot(last, "durable snapshot built")
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .trigger()
        .purge_log(last.index)
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .wait(Some(WAIT))
        .purged(Some(last), "snapshot-covered log purged")
        .await
        .unwrap();
    cluster.transport.heal();
    cluster.converged(&[1, 2, 3], &committed).await;
    assert!(
        cluster.transport.snapshot_count() > 0,
        "catchup must use a real OpenRaft snapshot RPC"
    );
    cluster.stop_all().await;
    for id in members() {
        cluster.start(id, StorageMode::Open).await;
    }
    let leader = cluster.leader(&[1, 2, 3]).await;
    cluster.converged(&[1, 2, 3], &committed).await;
    assert_eq!(
        cluster.nodes[&leader]
            .submit(acquire(k, 8, leader))
            .await
            .unwrap(),
        applied(9, leader, true)
    );
    cluster.stop_all().await;
}

#[derive(Clone, Default)]
struct TestClock(Arc<Mutex<Duration>>);

impl TestClock {
    fn set(&self, seconds: u64) {
        *self.0.lock().unwrap() = Duration::from_secs(seconds);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_ownership_requires_one_time_approval_and_checks_expiry_locally() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let clock = TestClock::default();
    let mut coordinator =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let k = local_key(90, 0);
    assert_eq!(
        coordinator.acquire(k, TTL),
        Err(OwnershipError::QuorumUnavailable)
    );
    coordinator
        .prepare_acquire(k, OwnershipEpoch(0), TTL)
        .await
        .unwrap();
    assert!(coordinator.check(k, OwnershipEpoch(1)).is_err());
    let lease = coordinator.acquire(k, TTL).unwrap();
    assert_eq!(lease.epoch, OwnershipEpoch(1));
    assert!(
        coordinator.acquire(k, TTL).is_err(),
        "approval cannot be replayed"
    );
    coordinator.check(k, lease.epoch).unwrap();
    clock.set(5);
    coordinator
        .prepare_renew(k, lease.epoch, TTL)
        .await
        .unwrap();
    let renewed = coordinator.renew(k, lease.epoch, TTL).unwrap();
    assert_eq!(renewed.epoch, lease.epoch);
    assert_eq!(renewed.expires_at, Duration::from_secs(15));
    assert!(coordinator.renew(k, lease.epoch, TTL).is_err());
    cluster.isolate(leader);
    clock.set(14);
    coordinator.check(k, lease.epoch).unwrap();
    clock.set(15);
    assert!(coordinator.check(k, lease.epoch).is_err());
    assert!(coordinator.renew_quorum(k, lease.epoch, TTL).await.is_err());
    assert_eq!(coordinator.watchdog().unwrap(), vec![k]);
    assert!(coordinator.watchdog().unwrap().is_empty());
    drop(coordinator);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quorum_loss_cannot_renew_or_grant_and_clock_regression_is_sticky() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let clock = TestClock::default();
    let mut coordinator =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let k = local_key(100, 0);
    let lease = coordinator
        .acquire_quorum(k, OwnershipEpoch(0), TTL)
        .await
        .unwrap();
    let before = cluster.data(leader).await;
    cluster.converged(&[1, 2, 3], &before).await;
    cluster.isolate(leader);
    clock.set(2);
    assert!(coordinator.renew_quorum(k, lease.epoch, TTL).await.is_err());
    assert!(
        coordinator
            .acquire_quorum(local_key(100, 1), OwnershipEpoch(0), TTL)
            .await
            .is_err()
    );
    assert_eq!(cluster.data(leader).await, before);
    coordinator.check(k, lease.epoch).unwrap();
    clock.set(1);
    assert_eq!(
        coordinator.check(k, lease.epoch),
        Err(OwnershipError::ClockRegressed)
    );
    clock.set(3);
    assert_eq!(
        coordinator.check(k, lease.epoch),
        Err(OwnershipError::ClockRegressed)
    );
    assert_eq!(coordinator.watchdog(), Err(OwnershipError::ClockRegressed));
    drop(coordinator);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_approval_never_moves_deadline_and_release_requires_committed_handoff() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let clock = TestClock::default();
    let mut coordinator =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), clock.clone()).unwrap();
    let k = local_key(110, 0);
    coordinator
        .prepare_acquire(k, OwnershipEpoch(0), TTL)
        .await
        .unwrap();
    clock.set(10);
    assert!(coordinator.acquire(k, TTL).is_err());
    assert!(coordinator.check(k, OwnershipEpoch(1)).is_err());
    let k2 = local_key(110, 1);
    let lease = coordinator
        .acquire_quorum(k2, OwnershipEpoch(0), TTL)
        .await
        .unwrap();
    coordinator.release_quorum(k2, lease.epoch).await.unwrap();
    assert!(coordinator.check(k2, lease.epoch).is_err());
    let next = coordinator
        .acquire_quorum(k2, lease.epoch, TTL)
        .await
        .unwrap();
    assert_eq!(next.epoch, OwnershipEpoch(2));
    assert!(coordinator.check(k2, lease.epoch).is_err());
    cluster.isolate(leader);
    assert!(coordinator.release_quorum(k2, next.epoch).await.is_err());
    assert!(
        coordinator.check(k2, next.epoch).is_err(),
        "failed remote release must not reopen local admissions"
    );
    assert!(cluster.data(leader).await.0[&key(110, 1)].held);
    drop(coordinator);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_acquire_that_commits_later_never_grants_and_can_be_relinquished() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    // Keep this test focused on ambiguous client cancellation. Election and
    // stale-leader replacement have dedicated tests above.
    for node in cluster.nodes.values() {
        node.raft.runtime_config().elect(false);
    }
    let mut coordinator =
        QuorumCoordinator::new(cluster.nodes[&leader].clone(), TestClock::default()).unwrap();
    let k = local_key(120, 0);
    cluster.isolate(leader);
    let previous_log = cluster.nodes[&leader]
        .raft
        .metrics()
        .borrow()
        .last_log_index;
    let mut preparing = Box::pin(coordinator.prepare_acquire(k, OwnershipEpoch(0), TTL));
    tokio::select! {
        result = &mut preparing => panic!("isolated acquisition unexpectedly completed: {result:?}"),
        _ = async {
            let deadline = Instant::now() + WAIT;
            loop {
                if cluster.nodes[&leader].raft.metrics().borrow().last_log_index > previous_log {
                    break;
                }
                assert!(Instant::now() < deadline, "request was not appended locally");
                sleep(Duration::from_millis(10)).await;
            }
        } => {}
    }
    // The client future is gone after its request entered the Raft log. Local
    // append is not a commit and must not create an admission receipt.
    drop(preparing);
    assert!(coordinator.check(k, OwnershipEpoch(1)).is_err());
    assert!(coordinator.acquire(k, TTL).is_err());
    assert!(!cluster.data(leader).await.0.contains_key(&key(120, 0)));
    cluster.transport.heal();
    let deadline = Instant::now() + WAIT;
    loop {
        if cluster
            .data(leader)
            .await
            .0
            .get(&key(120, 0))
            .is_some_and(|record| record.epoch == 1 && record.held)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ambiguous request did not commit after repair"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert!(coordinator.check(k, OwnershipEpoch(1)).is_err());
    assert!(coordinator.acquire(k, TTL).is_err());
    coordinator
        .release_quorum(k, OwnershipEpoch(1))
        .await
        .unwrap();
    assert!(!cluster.data(leader).await.0[&key(120, 0)].held);
    drop(coordinator);
    cluster.stop_all().await;
}
