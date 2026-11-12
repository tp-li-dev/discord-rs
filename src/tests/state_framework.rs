use src::worker::Manager;

#[tokio::test]
async fn worker_keeps_typed_state_across_invocations_and_resets_after_replacement() {
    let binary = std::path::PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let manager = Manager::start_pool(binary.clone(), 1).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!state_counter".into())
            .await
            .unwrap()
            .as_deref(),
        Some("State counter: 1")
    );
    assert_eq!(
        manager
            .dispatch_command(0, "state_counter".into())
            .await
            .unwrap()
            .as_deref(),
        Some("State counter: 2")
    );
    manager.replace(binary, true).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!state_counter".into())
            .await
            .unwrap()
            .as_deref(),
        Some("State counter: 1")
    );
}
