// 実 example をリンクして、自動収集されたコマンドを呼ぶ。定義を複製しない。
#[path = "../examples/ping_bot.rs"]
mod ping_bot;

use bot_framework::{
    arguments::{ArgumentError, Arguments, SlashValue},
    commands, dispatch,
    prelude::*,
};
use std::sync::Mutex;

#[tokio::test]
async fn ping_demo_routes_both_inputs() -> BotResult<()> {
    assert_eq!(ping_bot::demo().await?, ["Pong!", "Pong!"]);
    let definitions = commands()?;
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "ping");
    assert!(!definitions[0].description.is_empty());
    assert!(definitions[0].arguments.is_empty());
    Ok(())
}

#[tokio::test]
async fn unknown_commands_and_extra_arguments_do_not_reply() -> BotResult<()> {
    let replies = Mutex::new(Vec::new());
    assert!(!dispatch("unknown", Context::new(&replies)).await?);
    let prefix = Mutex::new(Arguments::prefix("unexpected")?);
    let slash = Mutex::new(Arguments::slash([(
        "extra".into(),
        SlashValue::Boolean(true),
    )])?);
    for arguments in [&prefix, &slash] {
        let error = dispatch("ping", Context::new(&replies).with_arguments(arguments))
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ArgumentError>(),
            Some(ArgumentError::Unexpected(_))
        ));
    }
    assert!(replies.lock().unwrap().is_empty());
    // 無効入力の後も同じ sink と handler を使える。
    assert!(dispatch("ping", Context::new(&replies)).await?);
    assert_eq!(*replies.lock().unwrap(), ["Pong!"]);
    Ok(())
}
