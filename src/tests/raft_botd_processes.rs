//! Three real botd processes with OpenRaft/mTLS and token-free mock Gateways.
//! Loopback and normal-clock observations are not Linux, suspend, real-host or
//! real Discord evidence. Each new Gateway Ready is checked for overlap/PIDs.
#[path = "fixtures/raft_botd_support.rs"]
mod support;

use serde_json::json;
use src::testkit::{MockDiscord, MockReply};
use std::time::{Duration, Instant};
use support::{Cluster, FAIL_MS, HEARTBEAT_MS, LEASE_MS, PROCESS_TESTS, STABLE_MS};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_botd_death_persistent_rejoin_and_automatic_failback() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let monitor = cluster.monitor.clone();

    let mut pids = std::collections::BTreeSet::new();
    for shard in 0..3 {
        let gateway = monitor.owner(shard, u64::from(shard) + 1).await;
        assert!(gateway.epoch > 0);
        for pid in gateway.pids() {
            assert!(pids.insert(pid), "process shared across Shards");
        }
        for worker in gateway.ready["workers"].as_array().unwrap() {
            assert_eq!(worker["identity"]["bot_id"], 101);
            assert_eq!(
                worker["identity"]["shard"],
                json!({"id": shard, "total": 3})
            );
        }
        let mock = MockDiscord::new(vec![MockReply::json(200, b"{}").unwrap()]);
        let rest = mock.spawn_tcp().await.unwrap();
        let reply = monitor.request(&gateway, json!({
            "type":"prefix", "key":0, "content":"!ping", "rest_addr":rest.addr().to_string(),
        })).await;
        assert_eq!(reply["outcome"], "completed");
        assert_eq!(reply["content"], "Pong! 🦀");
        assert_eq!(reply["receipt"]["identity"]["shard"]["id"], shard);
        assert_eq!(mock.stats().rest_requests, 1);
        rest.shutdown().await.unwrap();
    }
    let status = cluster.status(1).await;
    assert_eq!(status["cluster"]["implementation"], "OpenRaft");
    assert_eq!(status["cluster"]["production_ready"], false);
    let denied = cluster
        .request(
            1,
            json!({
                "command":"restart", "bot":"alpha", "operation_id":"0123456789abcdef0123456789abcdef",
            }),
        )
        .await;
    assert_eq!(
        denied["ok"], false,
        "local recovery must not bypass Raft ownership"
    );

    // Only botd dies. Its already launched guardian and Gateway stay alive until
    // their last bounded lease; failure detection alone must not authorize B.
    let original = monitor.owner(0, 1).await;
    let failed_at = Instant::now();
    cluster.kill(1).await;
    let proxy = monitor.owner(0, 2).await;
    let retired = monitor.ended(original.session).await;
    assert!(proxy.epoch > original.epoch);
    assert!(retired.disconnected_at.unwrap() < proxy.ready_at);
    assert!(
        proxy.ready_at.duration_since(failed_at) >= Duration::from_millis(FAIL_MS - HEARTBEAT_MS)
    );
    support::assert_exited(original.pids()).await;
    eprintln!(
        "Raft botd failover={} ms, lease ceiling={LEASE_MS} ms",
        proxy.ready_at.duration_since(failed_at).as_millis()
    );

    // Reopen the same durable Raft store without create/bootstrap. A new process
    // incarnation cannot reuse the old epoch or skip the stable-primary wait.
    let recovered_at = Instant::now();
    cluster.restart(1).await;
    monitor
        .stable_for(
            Duration::from_millis(STABLE_MS - 2 * HEARTBEAT_MS),
            |active| !active.iter().any(|g| g.shard == 0 && g.node == 1),
        )
        .await;
    let returned = monitor.owner(0, 1).await;
    let retired_proxy = monitor.ended(proxy.session).await;
    assert!(returned.epoch > proxy.epoch);
    assert!(
        returned.ready_at.duration_since(recovered_at)
            >= Duration::from_millis(STABLE_MS - HEARTBEAT_MS)
    );
    assert!(retired_proxy.disconnected_at.unwrap() < returned.ready_at);
    let status = cluster.status(2).await;
    assert!(
        status["transitions"]
            .as_array()
            .is_some_and(|events| events.iter().any(|event| {
                event["state"] == "draining"
                    && event["shard_id"] == 0
                    && event["epoch"] == proxy.epoch
            })),
        "proxy did not record draining for the delegated epoch"
    );
    support::assert_exited(proxy.pids()).await;
    for shard in 0..3 {
        monitor.owner(shard, u64::from(shard) + 1).await;
    }
    monitor.check();
    eprintln!(
        "Raft botd failback={} ms, stable wait={STABLE_MS} ms",
        returned.ready_at.duration_since(recovered_at).as_millis()
    );
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_botd_partitions_stop_expired_gateways_without_double_active() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let cluster = Cluster::new().await;
    let monitor = cluster.monitor.clone();
    let original = monitor.owner(0, 1).await;

    // Cut existing TLS streams and subsequent attempts in both directions on
    // both control planes. The mock Discord connection is not partitioned.
    cluster.isolate(1, true).await;
    let connected = monitor.request(&original, json!({"type":"status"})).await;
    assert_eq!(connected["pid"], original.pid);
    let proxy = monitor.owner(0, 2).await;
    let stopped = monitor.ended(original.session).await;
    assert!(proxy.epoch > original.epoch);
    assert!(stopped.disconnected_at.unwrap() < proxy.ready_at);
    assert_eq!(cluster.status(1).await["cluster"]["quorum"], false);
    support::assert_exited(original.pids()).await;

    // Every node is now a singleton. No automatic child recovery may revive an
    // old grant even though its local botd and mock Discord remain reachable.
    cluster.isolate(2, true).await;
    monitor
        .wait_for("all Raft leases expired", || {
            monitor.active().is_empty().then_some(())
        })
        .await;
    let starts = monitor.all().len();
    monitor
        .stable_for(Duration::from_millis(LEASE_MS * 2 + FAIL_MS), |active| {
            active.is_empty()
        })
        .await;
    assert_eq!(
        monitor.all().len(),
        starts,
        "minority component started a Gateway"
    );
    for id in 1..=3 {
        assert_eq!(cluster.status(id).await["cluster"]["quorum"], false);
    }

    cluster.isolate(1, false).await;
    cluster.isolate(2, false).await;
    for shard in 0..3 {
        monitor.owner(shard, u64::from(shard) + 1).await;
    }

    // Health reachability is not a Raft grant: isolate only consensus RPC while
    // all three authenticated heartbeat links continue to report a majority.
    cluster.isolate_rpc(1, true).await;
    cluster.isolate_rpc(2, true).await;
    monitor
        .wait_for("RPC-only partition lease expiry", || {
            monitor.active().is_empty().then_some(())
        })
        .await;
    monitor
        .stable_for(Duration::from_millis(LEASE_MS + FAIL_MS), |active| {
            active.is_empty()
        })
        .await;
    for id in 1..=3 {
        assert_eq!(cluster.status(id).await["cluster"]["quorum"], true);
    }
    monitor.check();
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_gateway_crash_recovers_only_after_exit_and_new_epoch() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let cluster = Cluster::new().await;
    let monitor = cluster.monitor.clone();
    let original = monitor.owner(0, 1).await;
    cluster.crash_gateway(&original).await;
    let replacement = monitor.owner(0, 1).await;
    let retired = monitor.ended(original.session).await;
    assert!(
        replacement.epoch > original.epoch,
        "child recovery reused a stale grant"
    );
    assert_ne!(replacement.pid, original.pid);
    assert!(retired.disconnected_at.unwrap() < replacement.ready_at);
    support::assert_exited(original.pids()).await;
    let status = cluster
        .wait_status(1, |status| {
            status["processes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|process| {
                    process["shard_id"] == 0
                        && process["epoch"] == replacement.epoch
                        && process["state"] == "active"
                        && process["active_admission"] == true
                })
        })
        .await;
    assert!(
        status["transitions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["shard_id"] == 0
                    && event["epoch"] == original.epoch
                    && event["state"] == "stopped"
                    && event["reason"] == "gateway_and_guardian_reaped"
            })
    );
    monitor
        .stable_for(Duration::from_millis(LEASE_MS), |active| {
            active.iter().filter(|g| g.shard == 0).count() == 1
                && active
                    .iter()
                    .any(|g| g.shard == 0 && g.epoch == replacement.epoch)
        })
        .await;
    // This observes epoch monotonicity and no legacy recovery bypass. Direct
    // injection of an old authenticated pipe notification belongs to host unit
    // tests; the process test does not impersonate a guardian.
    cluster.finish().await;
}
