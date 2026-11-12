//! Production-path ownership decisions use real OpenRaft over TCP/mTLS.
//! The workload and Gateway/Core admission are mock-only, in one OS process.
//! These tests do not establish real-host, OS suspend, Linux or Discord safety.
#[path = "fixtures/raft_runtime_support.rs"]
mod support;

use src::{
    cluster_runtime::{ClusterAction, ClusterRuntime, RaftRuntimeOptions},
    ownership::OwnershipError,
    raft_quorum::StorageMode,
};
use std::time::{Duration, Instant};
use support::{Cluster, FAIL_MS, HEARTBEAT_MS, LEASE_MS, SERIAL, STABLE_MS, key};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_owner_shutdown_reopen_and_stable_automatic_failback() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let original = cluster.owner(&key(101), 1).await;
    let other_bot = cluster.owner(&key(202), 3).await;
    assert_eq!(
        cluster.nodes[&1].check_active(&key(101), original.epoch),
        Ok(())
    );
    assert_eq!(
        cluster.nodes[&1].check_active(&key(101), original.epoch + 1),
        Err(OwnershipError::Fenced)
    );
    cluster.monitor.mock_command(&original).await;
    cluster.monitor.mock_command(&other_bot).await;
    for runtime in cluster.nodes.values() {
        assert_eq!(runtime.status()["implementation"], "OpenRaft");
        assert_eq!(runtime.status()["production_ready"], false);
    }

    let failed_at = Instant::now();
    cluster.stop(1).await;
    let detection_deadline = Instant::now() + support::WAIT;
    loop {
        cluster.step().await;
        let status = cluster.nodes[&2].status();
        let shard = status["shards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|shard| shard["bot_id"] == "101" && shard["shard_id"] == 0)
            .unwrap();
        if shard["policy"] == "FailoverPending" {
            assert!(failed_at.elapsed() >= Duration::from_millis(FAIL_MS - 2 * HEARTBEAT_MS));
            assert!(cluster.monitor.owner(&key(101), 2).is_none());
            break;
        }
        assert!(
            Instant::now() < detection_deadline,
            "failure detection did not become pending"
        );
    }
    let proxy = cluster.owner(&key(101), 2).await;
    let stopped = cluster
        .monitor
        .history()
        .into_iter()
        .find(|gate| gate.node == 1 && gate.key == key(101) && gate.epoch == original.epoch)
        .unwrap();
    assert!(stopped.ended.unwrap() <= proxy.started);
    assert!(proxy.epoch > original.epoch);
    assert!(
        proxy.started.duration_since(failed_at)
            >= Duration::from_millis(FAIL_MS - 2 * HEARTBEAT_MS)
    );
    assert!(!cluster.monitor.admit(1, &key(101), original.epoch));
    // Losing the Raft leader may safely expire another shard's lease while
    // election completes. Verify the other Bot keeps its intended primary and
    // monotonically fenced identity; uninterrupted lease renewal is not promised.
    let other_after_election = cluster.owner(&key(202), 3).await;
    assert!(other_after_election.epoch >= other_bot.epoch);
    if other_after_election.epoch != other_bot.epoch {
        eprintln!(
            "other Bot remained on primary 3 after leader election; epoch {} -> {}",
            other_bot.epoch, other_after_election.epoch
        );
        assert!(
            cluster.nodes[&3]
                .check_active(&key(202), other_bot.epoch)
                .is_err()
        );
    }
    cluster.monitor.mock_command(&proxy).await;

    // Withhold the supervisor's reap acknowledgement even after the mock
    // process stopped. A Stop action or elapsed shutdown delay cannot release
    // the reservation early and let the recovered primary Acquire it.
    cluster.monitor.withhold_exit(2, &key(101), true);
    let recovered_at = Instant::now();
    cluster.start(1, StorageMode::Open).await;
    cluster
        .hold(
            Duration::from_millis(STABLE_MS - 2 * HEARTBEAT_MS),
            |monitor| {
                monitor.owner(&key(101), 2).is_some() && monitor.owner(&key(101), 1).is_none()
            },
        )
        .await;
    let stopping = cluster.stopping(&key(101), 2, proxy.epoch).await;
    assert_eq!(stopping.stop_reason, Some("stable_primary_returned"));
    assert!(
        cluster.nodes[&2]
            .check_active(&key(101), proxy.epoch)
            .is_err()
    );
    cluster
        .hold(Duration::from_millis(400), |monitor| {
            monitor.owner(&key(101), 1).is_none()
        })
        .await;
    assert!(cluster.monitor.history().into_iter().any(|gate| {
        gate.node == 2
            && gate.key == key(101)
            && gate.epoch == proxy.epoch
            && gate.ended.is_some()
            && gate.reaped.is_none()
    }));
    cluster.monitor.withhold_exit(2, &key(101), false);
    let returned = cluster.owner(&key(101), 1).await;
    let retired_proxy = cluster
        .monitor
        .history()
        .into_iter()
        .find(|gate| gate.node == 2 && gate.key == key(101) && gate.epoch == proxy.epoch)
        .unwrap();
    assert!(returned.epoch > proxy.epoch);
    assert!(returned.started.duration_since(recovered_at) >= Duration::from_millis(STABLE_MS));
    assert_eq!(retired_proxy.stop_reason, Some("stable_primary_returned"));
    assert!(retired_proxy.ended.unwrap() <= returned.started);
    assert!(retired_proxy.reaped.unwrap() <= returned.started);
    assert!(!cluster.monitor.admit(2, &key(101), proxy.epoch));
    assert_eq!(
        cluster.nodes[&1].check_active(&key(101), original.epoch),
        Err(OwnershipError::Fenced)
    );
    assert_eq!(
        cluster.nodes[&1].check_active(&key(101), returned.epoch),
        Ok(())
    );
    let runtime = cluster.nodes.get_mut(&1).unwrap();
    assert_eq!(
        runtime.startup_complete_epoch(&key(101), original.epoch),
        Ok(())
    );
    assert_eq!(
        runtime.process_exited_epoch(&key(101), original.epoch),
        Ok(())
    );
    assert!(matches!(
        runtime.process_failed_epoch(&key(101), original.epoch),
        Ok(None)
    ));
    assert!(runtime.process_exited(&key(101)).is_err());
    assert_eq!(runtime.check_active(&key(101), returned.epoch), Ok(()));
    cluster.monitor.mock_command(&returned).await;
    cluster.monitor.check();
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_partition_and_total_quorum_loss_never_allow_two_mock_actives() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let original = cluster.owner(&key(101), 1).await;
    cluster.owner(&key(202), 3).await;
    cluster.isolate(1, true).await;
    // Discord is outside the partition. Old admission remains usable only for
    // its already-committed local lease, without any new Raft/ACK permission.
    cluster.monitor.mock_command(&original).await;
    let proxy = cluster.owner(&key(101), 2).await;
    assert!(proxy.epoch > original.epoch);
    assert!(!cluster.monitor.admit(1, &key(101), original.epoch));
    assert!(
        cluster.nodes[&1]
            .check_active(&key(101), original.epoch)
            .is_err()
    );
    let stopped = cluster
        .monitor
        .history()
        .into_iter()
        .find(|gate| gate.node == 1 && gate.key == key(101) && gate.epoch == original.epoch)
        .unwrap();
    assert!(stopped.ended.unwrap() <= proxy.started);
    assert_eq!(cluster.nodes[&1].status()["quorum"], false);

    cluster.isolate(2, true).await;
    let starts = cluster.monitor.history().len();
    cluster.no_active().await;
    cluster
        .hold(Duration::from_millis(LEASE_MS + FAIL_MS), |monitor| {
            monitor.active().is_empty() && monitor.history().len() == starts
        })
        .await;
    for runtime in cluster.nodes.values() {
        assert_eq!(runtime.status()["quorum"], false);
    }

    cluster.isolate(1, false).await;
    cluster.isolate(2, false).await;
    let recovered = cluster.owner(&key(101), 1).await;
    cluster.owner(&key(202), 3).await;
    assert!(recovered.epoch > proxy.epoch);
    cluster.monitor.mock_command(&recovered).await;
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_control_heartbeats_cannot_replace_raft_majority_commits() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let first = cluster.owner(&key(101), 1).await;
    let second = cluster.owner(&key(202), 3).await;
    cluster.rpc_partition(true).await;
    let starts = cluster.monitor.history().len();
    cluster.no_active().await;
    cluster
        .hold(Duration::from_millis(LEASE_MS + FAIL_MS), |monitor| {
            monitor.active().is_empty() && monitor.history().len() == starts
        })
        .await;
    for runtime in cluster.nodes.values() {
        let status = runtime.status();
        assert_eq!(
            status["quorum"], true,
            "control-plane health should remain available"
        );
        assert!(
            status["peers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|peer| peer["healthy"] == true)
        );
    }
    assert!(
        cluster.nodes[&1]
            .check_active(&key(101), first.epoch)
            .is_err()
    );
    assert!(
        cluster.nodes[&3]
            .check_active(&key(202), second.epoch)
            .is_err()
    );

    cluster.rpc_partition(false).await;
    let recovered = cluster.owner(&key(101), 1).await;
    cluster.owner(&key(202), 3).await;
    assert!(recovered.epoch > first.epoch);
    cluster.monitor.mock_command(&recovered).await;
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_poll_pause_cannot_revive_an_old_admission_receipt() {
    let _serial = SERIAL.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let original = cluster.owner(&key(101), 1).await;
    cluster.owner(&key(202), 3).await;
    let pending_deadline = Instant::now() + support::WAIT;
    loop {
        cluster.step().await;
        if cluster.nodes[&1].status()["raft"]["pending_requests"]
            .as_u64()
            .unwrap()
            > 0
        {
            break;
        }
        assert!(
            Instant::now() < pending_deadline,
            "owner did not submit a renewal"
        );
    }
    // This pauses only application polling, not Tokio, TLS, Raft, the mock
    // Gateway's deadline clock or the other two nodes. It is NOT OS suspend.
    let until = Instant::now() + Duration::from_millis(LEASE_MS + 300);
    while Instant::now() < until {
        cluster.step_pausing(1).await;
    }
    assert!(!cluster.monitor.admit(1, &key(101), original.epoch));
    assert!(
        cluster.nodes[&1]
            .check_active(&key(101), original.epoch)
            .is_err()
    );
    let resumed = cluster.tick_one(1);
    assert!(resumed.iter().any(|action| {
        matches!(action, ClusterAction::Stop { key: stopped, .. } if *stopped == key(101))
    }));
    assert!(!resumed.iter().any(|action| {
        matches!(action, ClusterAction::Start { key: started, .. }
            | ClusterAction::Renew { key: started, .. } if *started == key(101))
    }));
    assert!(
        cluster.nodes[&1]
            .check_active(&key(101), original.epoch)
            .is_err()
    );
    let recovered = cluster.owner(&key(101), 1).await;
    assert!(recovered.epoch > original.epoch);
    assert!(
        cluster.nodes[&1]
            .check_active(&key(101), original.epoch)
            .is_err()
    );
    cluster.monitor.mock_command(&recovered).await;
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_runtime_is_disabled_by_default_and_refuses_non_mock_or_production_startup() {
    let _serial = SERIAL.acquire().await.unwrap();
    let cluster = Cluster::prepared().await;
    assert!(!RaftRuntimeOptions::default().allow_raft);
    for (allow, production, mock) in [
        (false, false, true),
        (true, false, false),
        (true, true, true),
    ] {
        let mut options = cluster.options(1, StorageMode::Create);
        options.allow_raft = allow;
        options.enable_production = production;
        let store = options.storage_path.clone();
        // The control/RPC ports are still occupied by the fixture reservations.
        // Rejection must precede binding and creation of the persistent store.
        assert!(
            ClusterRuntime::bind_raft(cluster.config(1), options, mock)
                .await
                .is_err()
        );
        assert!(!store.exists());
    }
    let defaults = src::placement::Failover::default();
    assert_eq!(defaults.unhealthy_after_ms, 3000);
    assert_eq!(defaults.failback_stable_ms, 30000);
    assert_eq!(
        src::failover::Config::default().fail_after,
        Duration::from_secs(3)
    );
    assert_eq!(
        src::failover::Config::default().stable_for,
        Duration::from_secs(30)
    );
    cluster.finish().await;
}
