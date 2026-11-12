use bot_framework::arguments::SlashValue;
use src::worker::Manager;

#[tokio::test]
async fn typed_slash_reaches_real_worker_and_validates_arguments() {
    let manager = Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
        .await
        .unwrap();
    let args = vec![
        ("name".into(), SlashValue::String("Alice".into())),
        ("count".into(), SlashValue::Integer(3)),
    ];
    assert_eq!(
        manager
            .dispatch_slash(0, "greet".into(), args)
            .await
            .unwrap()
            .as_deref(),
        Some("Hello, Alice! (3)")
    );
    let invalid = vec![("name".into(), SlashValue::Integer(3))];
    assert!(
        manager
            .dispatch_slash(0, "greet".into(), invalid)
            .await
            .is_err()
    );
    let duplicate = vec![
        ("name".into(), SlashValue::String("a".into())),
        ("name".into(), SlashValue::String("b".into())),
    ];
    assert!(
        manager
            .dispatch_slash(0, "greet".into(), duplicate)
            .await
            .is_err()
    );
    assert_eq!(
        manager
            .dispatch_command(0, "ping".into())
            .await
            .unwrap()
            .as_deref(),
        Some("Pong! 🦀")
    );
}
