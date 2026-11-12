//! T10-T14 with three real botd processes, mTLS and real Gateway/Worker trees.
//! The quorum is explicitly TEST ONLY. These loopback, normal-clock experiments
//! do not establish production consensus, OS suspend safety or Discord behavior.
#[path = "fixtures/e2e_cluster_support.rs"]
mod support;

use serde_json::json;
use src::testkit::{MockDiscord, MockReply};
use std::time::{Duration, Instant};
use support::{Cluster, FAIL_MS, HEARTBEAT_MS, LEASE_MS, PROCESS_TESTS, STABLE_MS, WAIT};
use tokio::io::AsyncWriteExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t10_t11_t12_placement_daemon_death_and_stable_failback() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let mut cluster = Cluster::new().await;
    let monitor = cluster.monitor.clone();

    // T10: Actual processes on their assigned nodes, and actual Core/REST work.
    let mut seen = std::collections::BTreeSet::new();
    for shard in 0..3 {
        let gateway = monitor.owner(shard, u64::from(shard) + 1).await;
        assert!(gateway.epoch > 0);
        for pid in gateway.pids() {
            assert!(seen.insert(pid), "process shared across Shards");
        }
        for worker in gateway.ready["workers"].as_array().unwrap() {
            assert_eq!(worker["identity"]["bot_id"], 101);
            assert_eq!(worker["identity"]["shard"], json!({"id": shard, "total":3}));
        }
        let mock = MockDiscord::new(vec![MockReply::json(200, b"{}").unwrap()]);
        let rest = mock.spawn_tcp().await.unwrap();
        let result = monitor.request(&gateway, json!({
            "type":"prefix", "key":0, "content":"!ping", "rest_addr":rest.addr().to_string(),
        })).await;
        assert_eq!(result["outcome"], "completed");
        assert_eq!(result["content"], "Pong! 🦀");
        assert_eq!(result["receipt"]["identity"]["bot_id"], 101);
        assert_eq!(result["receipt"]["identity"]["shard"]["id"], shard);
        assert_eq!(mock.stats().rest_requests, 1);
        rest.shutdown().await.unwrap();
    }
    monitor
        .stable_for(Duration::from_millis(3 * HEARTBEAT_MS), |active| {
            active.len() == 3 && active.iter().all(|g| g.node == u64::from(g.shard) + 1)
        })
        .await;

    // T11: Only the daemon dies. Its Gateway retains a Discord collector
    // connection, so safety depends on the independently running lease guardian.
    let original = monitor.owner(0, 1).await;
    let failed_at = Instant::now();
    cluster.kill(1).await;
    let proxy = monitor.owner(0, 2).await;
    let old = monitor.ended(original.session).await;
    assert!(
        proxy.epoch > original.epoch,
        "failover must fence the original epoch"
    );
    assert!(
        old.disconnected_at.unwrap() < proxy.ready_at,
        "replacement started before old Gateway disconnected"
    );
    assert!(
        proxy.ready_at.duration_since(failed_at) >= Duration::from_millis(FAIL_MS - HEARTBEAT_MS)
    );
    assert!(
        proxy.ready_at.duration_since(failed_at) < Duration::from_secs(8),
        "short-threshold failover exceeded integration budget"
    );
    eprintln!(
        "T11 observed failover={} ms (failure threshold={FAIL_MS} ms, vote hold={LEASE_MS} ms)",
        proxy.ready_at.duration_since(failed_at).as_millis()
    );
    support::assert_exited(original.pids()).await;

    // T12: Reuse the same node identity, certificate, address and runtime root.
    // A restart cannot clear the old lease or skip the configured stable wait.
    let recovered_at = Instant::now();
    cluster.start(1).await;
    monitor
        .stable_for(
            Duration::from_millis(STABLE_MS - 2 * HEARTBEAT_MS),
            |active| {
                active.iter().any(|g| g.shard == 0 && g.node == 2)
                    && !active.iter().any(|g| g.shard == 0 && g.node == 1)
            },
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
    eprintln!(
        "T12 observed failback={} ms (stable wait={STABLE_MS} ms)",
        returned.ready_at.duration_since(recovered_at).as_millis()
    );
    let status = cluster.status(2).await;
    assert_eq!(status["ok"], true);
    assert!(
        status["transitions"]
            .as_array()
            .is_some_and(|events| events.iter().any(|event| {
                event["state"] == "draining"
                    && event["shard_id"] == 0
                    && event["epoch"] == proxy.epoch
            })),
        "proxy must record Draining before exit: {status}"
    );
    support::assert_exited(proxy.pids()).await;
    for shard in 0..3 {
        monitor.owner(shard, u64::from(shard) + 1).await;
    }
    monitor.check();
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t13_t14_mtls_partition_expires_old_lease_and_majority_loss_blocks_promotion() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let cluster = Cluster::new().await;
    let monitor = cluster.monitor.clone();
    let original = monitor.owner(0, 1).await;

    // T13: Cut both directions of all A peer links, including in-flight TLS
    // streams. The Discord collector is a different, unpartitioned connection.
    let partitioned_at = Instant::now();
    cluster.isolate(1, true).await;
    let still_connected = monitor.request(&original, json!({"type":"status"})).await;
    assert_eq!(still_connected["pid"], original.pid);
    assert_eq!(still_connected["type"], "status");
    let proxy = monitor.owner(0, 2).await;
    let stopped = monitor.ended(original.session).await;
    assert!(proxy.epoch > original.epoch);
    assert!(stopped.disconnected_at.unwrap() < proxy.ready_at);
    assert!(
        stopped
            .disconnected_at
            .unwrap()
            .duration_since(partitioned_at)
            < Duration::from_millis(LEASE_MS * 2)
    );
    let isolated_status = cluster.status(1).await;
    assert_eq!(isolated_status["cluster"]["quorum"], false);
    monitor
        .stable_for(Duration::from_millis(LEASE_MS), |active| {
            active.len() == 3 && active.iter().all(|g| g.node != 1)
        })
        .await;

    // T14: B and C now lose each other too. Membership is still three, so none
    // of the isolated one-node components may treat itself as a new majority.
    let count_before = monitor.all().len();
    cluster.isolate(2, true).await;
    monitor
        .wait_for("lease expiry after majority loss", || {
            monitor.active().is_empty().then_some(())
        })
        .await;
    monitor
        .stable_for(Duration::from_millis(LEASE_MS * 2 + FAIL_MS), |active| {
            active.is_empty()
        })
        .await;
    assert_eq!(
        monitor.all().len(),
        count_before,
        "minority component started a replacement Gateway"
    );
    for node in 1..=3 {
        let status = cluster.status(node).await;
        assert_eq!(status["cluster"]["quorum"], false);
    }

    // Reconnection is included to detect stale ACKs, immortal failed votes, and
    // sticky Degraded states that would otherwise escape a stop-only scenario.
    cluster.isolate(1, false).await;
    cluster.isolate(2, false).await;
    for shard in 0..3 {
        monitor.owner(shard, u64::from(shard) + 1).await;
    }
    monitor
        .stable_for(Duration::from_millis(3 * HEARTBEAT_MS), |active| {
            active.len() == 3
        })
        .await;
    cluster.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_startup_requires_explicit_test_quorum_and_mock_discord_guards() {
    let _serial = PROCESS_TESTS.acquire().await.unwrap();
    let mut cluster = Cluster::prepared().await;
    cluster.release_listener(1);
    for (allow, mock) in [(false, true), (true, false), (false, false)] {
        let mut child = cluster.command(1, allow, mock).spawn().unwrap();
        // Startup rejection must not depend on a missing bootstrap frame.
        let frame = serde_json::to_vec(&json!({"secret": vec![73; 32]})).unwrap();
        let mut input = child.stdin.take().unwrap();
        let _ = input.write_all(&frame).await;
        let _ = input.write_all(b"\n").await;
        drop(input);
        let output = tokio::time::timeout(WAIT, child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        support::no_secrets(&output.stdout);
        support::no_secrets(&output.stderr);
        assert!(!output.status.success());
        assert!(
            output.stdout.is_empty(),
            "unguarded mode announced itself ready"
        );
        let expected = if !allow {
            "test quorum requires --allow-test-quorum"
        } else {
            "test quorum requires --mock-discord"
        };
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "startup must fail at the test-mode guard, not certificate, bind or bootstrap I/O"
        );
    }
    assert!(cluster.monitor.all().is_empty());
    cluster.finish().await;
}
