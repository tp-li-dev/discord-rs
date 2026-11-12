use src::worker::Manager;
use std::path::PathBuf;

#[tokio::test]
async fn registered_command_is_dispatched_by_a_real_worker() {
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    assert_eq!(
        manager.dispatch(" !ping ".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
    assert_eq!(manager.dispatch("!ping extra".into()).await.unwrap(), None);
    assert_eq!(manager.dispatch("ping".into()).await.unwrap(), None);
    assert_eq!(
        manager
            .dispatch_command(0, "ping".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Pong! 🦀")
    );
    assert_eq!(
        manager.dispatch_command(0, "count".into()).await.unwrap(),
        None
    );
    assert_eq!(
        manager.dispatch("!count".into()).await.unwrap().as_deref(),
        Some("カウント: 1")
    );
}
