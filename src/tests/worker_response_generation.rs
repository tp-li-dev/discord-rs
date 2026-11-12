use src::worker::Manager;
use std::{path::PathBuf, time::Duration};
use tokio::time::timeout;

fn binary() -> PathBuf {
    env!("CARGO_BIN_EXE_src").into()
}

#[tokio::test]
async fn response_reservation_rejects_wrong_manager_worker_and_reuse() {
    let manager = Manager::start_pool(binary(), 2).await.unwrap();
    let other = Manager::start_pool(binary(), 1).await.unwrap();
    let mut response = manager.begin_response(0).await.unwrap();
    for error in [
        other
            .dispatch_key_with_response(&mut response, 0, "!count".into())
            .await
            .unwrap_err(),
        manager
            .dispatch_key_with_response(&mut response, 1, "!count".into())
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.to_string(), "response generation mismatch");
    }
    assert_eq!(
        manager
            .dispatch_key_with_response(&mut response, 0, "!count".into())
            .await
            .unwrap()
            .as_deref(),
        Some("カウント: 1")
    );
    assert_eq!(
        manager
            .dispatch_key_with_response(&mut response, 0, "!count".into())
            .await
            .unwrap_err()
            .to_string(),
        "response dispatch already used"
    );
    response.confirm();
    assert_eq!(
        manager.dispatch("!count".into()).await.unwrap().as_deref(),
        Some("カウント: 2")
    );
    assert!(manager.unknown_responses().1.is_empty());
}

#[tokio::test]
async fn unused_old_reservation_does_not_block_routing_or_start_old_work() {
    timeout(Duration::from_secs(10), async {
        let manager = Manager::start_pool(binary(), 1).await.unwrap();
        let before = manager.describe_workers().await;
        let mut response = manager.begin_response(0).await.unwrap();
        let update = tokio::spawn({
            let manager = manager.clone();
            async move { manager.replace(binary(), true).await }
        });
        // Observe the new PID rather than assuming a sleep wins a scheduling race.
        while manager.describe_workers().await == before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!update.is_finished());
        assert_eq!(
            manager
                .dispatch_key_with_response(&mut response, 0, "!count".into())
                .await
                .unwrap_err()
                .to_string(),
            "response generation is no longer active"
        );
        response.confirm(); // No work was dispatched, so no external effect occurred.
        update.await.unwrap().unwrap();
        assert_eq!(
            manager.dispatch("!count".into()).await.unwrap().as_deref(),
            Some("カウント: 1")
        );
        assert!(manager.unknown_responses().1.is_empty());
    })
    .await
    .unwrap();
}
