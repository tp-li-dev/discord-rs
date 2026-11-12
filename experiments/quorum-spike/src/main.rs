use quorum_spike::{Outcome, ShardKey, Transfer, cluster::Cluster};

#[tokio::main]
async fn main() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let request = |epoch| Transfer {
        key: ShardKey {
            bot_id: 10,
            shard_id: 0,
        },
        expected_epoch: epoch,
        new_owner: leader,
    };
    let first = cluster
        .write(leader, request(0))
        .await
        .expect("first commit");
    assert!(matches!(first, Outcome::Applied(_)));
    println!("elected node {leader}; committed {first:?}");
    let followers: Vec<_> = (1..=3).filter(|id| *id != leader).collect();
    cluster.stop(followers[0]).await;
    let second = cluster
        .write(leader, request(1))
        .await
        .expect("majority commit");
    assert!(matches!(second, Outcome::Applied(_)));
    println!("one node stopped; committed {second:?}");
    cluster.stop(followers[1]).await;
    let result = cluster.write(leader, request(2)).await;
    assert!(result.is_err());
    println!("majority lost; no grant: {result:?} (timeout may commit after recovery)");
    assert_eq!(
        cluster.data(leader).await.0[&request(0).key].ownership_epoch,
        2
    );
    cluster.shutdown().await;
    println!("partition and recovery checks: cargo test --offline");
}
