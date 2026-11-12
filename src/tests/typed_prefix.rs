use src::worker::Manager;

#[tokio::test]
async fn typed_prefix_commands_run_in_real_worker_and_report_invalid_input() {
    let manager = Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
        .await
        .unwrap();
    assert_eq!(
        manager
            .dispatch("!greet Alice 3".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Hello, Alice! (3)")
    );
    assert_eq!(
        manager
            .dispatch("!greet Alice".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Hello, Alice! (1)")
    );
    assert!(manager.dispatch("!greet Alice nope".into()).await.is_err());
    assert!(manager.dispatch("!greet".into()).await.is_err());
    assert_eq!(manager.dispatch("!ping extra".into()).await.unwrap(), None);
    assert_eq!(
        manager.dispatch("!unknown extra".into()).await.unwrap(),
        None
    );
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
}
