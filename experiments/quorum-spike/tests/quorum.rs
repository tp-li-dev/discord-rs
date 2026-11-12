use openraft::storage::RaftStateMachine;
use quorum_spike::{
    Outcome, Ownership, OwnershipData, ShardKey, Transfer,
    cluster::{Cluster, WRITE_DEADLINE},
};
use std::time::Duration;
use tokio::time::sleep;

fn key(bot_id: u64, shard_id: u32) -> ShardKey {
    ShardKey { bot_id, shard_id }
}
fn transfer(key: ShardKey, expected_epoch: u64, new_owner: u64) -> Transfer {
    Transfer {
        key,
        expected_epoch,
        new_owner,
    }
}
fn applied(epoch: u64, owner: u64) -> Outcome {
    Outcome::Applied(Ownership {
        ownership_epoch: epoch,
        owner,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn election_replicates_independent_bot_and_shard_epochs_and_rejects_stale_cas() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let a = key(10, 0);
    let b = key(10, 1);
    let other_bot = key(20, 0);
    assert_eq!(
        c.write(leader, transfer(a, 0, 1)).await.unwrap(),
        applied(1, 1)
    );
    assert_eq!(
        c.write(leader, transfer(a, 1, 2)).await.unwrap(),
        applied(2, 2)
    );
    assert_eq!(
        c.write(leader, transfer(b, 0, 3)).await.unwrap(),
        applied(1, 3)
    );
    assert_eq!(
        c.write(leader, transfer(other_bot, 0, 1)).await.unwrap(),
        applied(1, 1)
    );
    let before = c.data(leader).await;
    assert_eq!(
        c.write(leader, transfer(a, 1, 3)).await.unwrap(),
        Outcome::Stale { current_epoch: 2 }
    );
    c.wait_data(&[1, 2, 3], &before).await;
    assert_eq!(before.0[&a].ownership_epoch, 2);
    assert_eq!(before.0[&b].ownership_epoch, 1);
    assert_eq!(before.0[&other_bot].ownership_epoch, 1);
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_shutdown_preserves_majority_writes_then_quorum_loss_fails_closed() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let k = key(10, 0);
    assert_eq!(
        c.write(leader, transfer(k, 0, 1)).await.unwrap(),
        applied(1, 1)
    );
    let initial = c.data(leader).await;
    c.wait_data(&[1, 2, 3], &initial).await;
    let followers: Vec<_> = (1..=3).filter(|id| *id != leader).collect();
    c.stop(followers[0]).await;
    assert_eq!(
        c.write(leader, transfer(k, 1, 2)).await.unwrap(),
        applied(2, 2)
    );
    let before = c.data(leader).await;
    c.wait_data(&[leader, followers[1]], &before).await;
    c.stop(followers[1]).await;
    for _ in 0..2 {
        assert!(c.write(leader, transfer(k, 2, 3)).await.is_err());
        assert_eq!(c.data(leader).await, before);
    }
    // Observe beyond one election timeout, not just the instant of API failure.
    sleep(Duration::from_millis(400)).await;
    assert_eq!(c.data(leader).await, before);
    assert_eq!(
        c.nodes[&leader]
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .count(),
        3
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_shutdown_elects_successor_without_epoch_reset() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let k = key(10, 0);
    assert_eq!(
        c.write(leader, transfer(k, 0, 1)).await.unwrap(),
        applied(1, 1)
    );
    let before = c.data(leader).await;
    c.wait_data(&[1, 2, 3], &before).await;
    c.stop(leader).await;
    let survivors: Vec<_> = c.nodes.keys().copied().collect();
    let successor = c.leader(&survivors).await;
    assert_ne!(leader, successor);
    assert_eq!(
        c.write(successor, transfer(k, 1, successor)).await.unwrap(),
        applied(2, successor)
    );
    let after = c.data(successor).await;
    c.wait_data(&survivors, &after).await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_old_leader_cannot_apply_epoch_majority_progresses_and_heals() {
    let mut c = Cluster::new().await;
    let old = c.leader(&[1, 2, 3]).await;
    let k = key(10, 0);
    assert_eq!(
        c.write(old, transfer(k, 0, old)).await.unwrap(),
        applied(1, old)
    );
    let before = c.data(old).await;
    c.wait_data(&[1, 2, 3], &before).await;
    c.isolate(old);
    // Submit to the stale leader immediately, while it still believes it leads.
    let minority = c.nodes[&old].clone();
    let pending = tokio::spawn(async move {
        tokio::time::timeout(WRITE_DEADLINE, minority.client_write(transfer(k, 1, old))).await
    });
    let majority: Vec<_> = (1..=3).filter(|id| *id != old).collect();
    let new = c.leader(&majority).await;
    assert_eq!(
        c.write(new, transfer(k, 1, new)).await.unwrap(),
        applied(2, new)
    );
    assert!(!matches!(pending.await.unwrap(), Ok(Ok(_))));
    let committed = c.data(new).await;
    c.wait_data(&majority, &committed).await;
    assert_eq!(c.data(old).await, before);
    assert!(c.write(old, transfer(k, 1, old)).await.is_err());
    assert_eq!(c.data(old).await, before);
    c.bus.heal();
    c.wait_data(&[1, 2, 3], &committed).await;
    let current = c.leader(&[1, 2, 3]).await;
    assert_eq!(
        c.write(current, transfer(k, 2, current)).await.unwrap(),
        applied(3, current)
    );
    let after = c.data(current).await;
    c.wait_data(&[1, 2, 3], &after).await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_three_isolated_cannot_commit_on_any_node() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let k = key(10, 0);
    assert_eq!(
        c.write(leader, transfer(k, 0, leader)).await.unwrap(),
        applied(1, leader)
    );
    let before = c.data(leader).await;
    c.wait_data(&[1, 2, 3], &before).await;
    for node in 1..=3 {
        c.isolate(node);
    }
    for node in 1..=3 {
        assert!(c.write(node, transfer(k, 1, node)).await.is_err());
    }
    for node in 1..=3 {
        assert_eq!(c.data(node).await, before);
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_follower_installs_snapshot_and_then_accepts_new_logs() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let lagging = (1..=3).find(|id| *id != leader).unwrap();
    let majority: Vec<_> = (1..=3).filter(|id| *id != lagging).collect();
    // Disable only this follower's election timer to focus this test on snapshot
    // recovery, even on slow CI. Other tests exercise automatic elections.
    c.nodes[&lagging].runtime_config().elect(false);
    // Block every inbound RPC to the lagging follower, including vote requests.
    for peer in &majority {
        c.bus.block(*peer, lagging);
    }
    let k = key(10, 0);
    for epoch in 0..12 {
        assert_eq!(
            c.write(leader, transfer(k, epoch, leader)).await.unwrap(),
            applied(epoch + 1, leader)
        );
    }
    let committed = c.data(leader).await;
    c.wait_data(&majority, &committed).await;
    // Metrics are asynchronous: obtain the committed position from the actual
    // state machine before triggering the snapshot, not from a stale metric.
    let last = c.machines[&leader]
        .clone()
        .applied_state()
        .await
        .unwrap()
        .0
        .unwrap();
    c.nodes[&leader].trigger().snapshot().await.unwrap();
    c.nodes[&leader]
        .wait(Some(quorum_spike::cluster::WAIT))
        .snapshot(last, "snapshot built")
        .await
        .unwrap();
    c.nodes[&leader]
        .trigger()
        .purge_log(last.index)
        .await
        .unwrap();
    c.nodes[&leader]
        .wait(Some(quorum_spike::cluster::WAIT))
        .purged(Some(last), "logs purged")
        .await
        .unwrap();
    c.bus.heal();
    c.wait_data(&[1, 2, 3], &committed).await;
    assert!(
        c.bus.snapshot_count() > 0,
        "recovery must actually use snapshot RPC"
    );
    let current = c.leader(&[1, 2, 3]).await;
    assert_eq!(
        c.write(current, transfer(k, 12, current)).await.unwrap(),
        applied(13, current)
    );
    let after = c.data(current).await;
    c.wait_data(&[1, 2, 3], &after).await;
    c.shutdown().await;
}

#[test]
fn epoch_overflow_and_stale_request_preserve_existing_owner() {
    let k = key(10, 0);
    let mut data = OwnershipData::default();
    data.0.insert(
        k,
        Ownership {
            ownership_epoch: u64::MAX,
            owner: 1,
        },
    );
    let before = data.clone();
    assert_eq!(
        data.transfer(&transfer(k, u64::MAX, 2)),
        Outcome::EpochExhausted
    );
    assert_eq!(
        data.transfer(&transfer(k, 0, 2)),
        Outcome::Stale {
            current_epoch: u64::MAX
        }
    );
    assert_eq!(data, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_transfers_with_same_expected_epoch_have_exactly_one_winner() {
    let mut c = Cluster::new().await;
    let leader = c.leader(&[1, 2, 3]).await;
    let k = key(10, 0);
    let (a, b) = tokio::join!(
        c.write(leader, transfer(k, 0, 1)),
        c.write(leader, transfer(k, 0, 2)),
    );
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Outcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Outcome::Stale { current_epoch: 1 }))
            .count(),
        1
    );
    let committed = c.data(leader).await;
    assert_eq!(committed.0[&k].ownership_epoch, 1);
    c.wait_data(&[1, 2, 3], &committed).await;
    c.shutdown().await;
}
