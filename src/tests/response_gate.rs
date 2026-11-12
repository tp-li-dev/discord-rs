use src::worker::Manager;
use std::{path::PathBuf, time::Duration};

const DEADLINE: Duration = Duration::from_secs(30);

async fn wait_for_switch(manager: &Manager, before: &str) {
    tokio::time::timeout(DEADLINE, async {
        while manager.describe_workers().await == before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("candidate did not become active while the old response remained pending");
}

#[tokio::test]
async fn update_waits_for_external_response() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path.clone(), 1).await.unwrap();
    let before = manager.describe_workers().await;
    let guard = manager.begin_response(0).await.unwrap();
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
    let update = tokio::spawn({
        let manager = manager.clone();
        async move { manager.replace(path, true).await }
    });
    // Observe the actual generation transition before checking drain. A fixed
    // sleep could pass with an update task that had not even been scheduled.
    wait_for_switch(&manager, &before).await;
    assert!(!update.is_finished());
    guard.confirm();
    tokio::time::timeout(DEADLINE, update)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
}

#[tokio::test]
async fn unknown_response_prevents_switch_but_does_not_stop_old_worker() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path.clone(), 1).await.unwrap();
    let guard = manager.begin_response(0).await.unwrap();
    drop(guard);
    assert!(manager.replace(path, true).await.is_err());
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
}

#[tokio::test]
async fn other_workers_stay_available_while_one_worker_drains() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path.clone(), 2).await.unwrap();
    let before = manager.describe_workers().await;
    let first = manager.begin_response(0).await.unwrap();
    let update = tokio::spawn({
        let manager = manager.clone();
        async move { manager.replace(path, true).await }
    });
    wait_for_switch(&manager, &before).await;
    assert!(!update.is_finished());
    let other = tokio::time::timeout(DEADLINE, manager.begin_response(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        manager
            .dispatch_key(1, "!ping".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Pong! 🦀")
    );
    other.confirm();
    first.confirm();
    tokio::time::timeout(DEADLINE, update)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn later_unknown_preserves_unupdated_worker_after_partial_update() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path.clone(), 2).await.unwrap();
    let before = manager.describe_workers().await;
    drop(manager.begin_response(1).await.unwrap());
    assert!(manager.replace(path, true).await.is_err());
    let after = manager.describe_workers().await;
    let before: Vec<_> = before.lines().collect();
    let after: Vec<_> = after.lines().collect();
    assert_ne!(before[1], after[1]);
    assert_eq!(before[2], after[2]);
    assert_eq!(
        manager
            .dispatch_key(1, "!ping".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Pong! 🦀")
    );
}

#[tokio::test]
async fn explicit_reconciliation_requires_matching_instance_and_evidence() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path.clone(), 1).await.unwrap();
    drop(manager.begin_response(0).await.unwrap());
    let (instance, ids) = manager.unknown_responses();
    assert_eq!(ids.len(), 1);
    let (worker, id) = ids[0];
    assert!(
        manager
            .reconcile_response("stale-instance", worker, id, "ticket 123")
            .is_err()
    );
    assert!(
        manager
            .reconcile_response(&instance, worker, id, "  ")
            .is_err()
    );
    assert!(
        manager
            .reconcile_response(&instance, worker + 1, id, "ticket 123")
            .is_err()
    );
    assert!(manager.replace(path.clone(), true).await.is_err());
    manager
        .reconcile_response(&instance, worker, id, "ticket 123")
        .unwrap();
    assert!(manager.unknown_responses().1.is_empty());
    assert!(
        manager
            .reconcile_response(&instance, worker, id, "ticket 123")
            .is_err()
    );
    manager.replace(path, true).await.unwrap();
}

#[tokio::test]
async fn wrong_worker_cannot_reconcile_another_unknown_response() {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(path, 2).await.unwrap();
    drop(manager.begin_response(0).await.unwrap());
    drop(manager.begin_response(1).await.unwrap());
    let (instance, ids) = manager.unknown_responses();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0].1, ids[1].1);
    assert!(
        manager
            .reconcile_response(&instance, ids[1].0, ids[0].1, "ticket 123")
            .is_err()
    );
    assert_eq!(manager.unknown_responses().1, ids);
}
