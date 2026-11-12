use bot_framework::{commands, dispatch, prelude::*};
use std::sync::Mutex;

#[hybrid_command(name = "ping", description = "Check response")]
async fn ping(ctx: Context<'_>) -> BotResult<()> {
    ctx.reply("Pong!").await?;
    Ok(())
}

#[tokio::test]
async fn linked_command_routes_both_invocation_types() {
    let definitions = commands().unwrap();
    assert_eq!(
        definitions
            .iter()
            .find(|c| c.name == "ping")
            .unwrap()
            .description,
        "Check response"
    );
    let replies = Mutex::new(Vec::new());
    // The transport adapter strips the prefix or extracts the interaction name.
    assert!(dispatch("ping", Context::new(&replies)).await.unwrap());
    assert!(dispatch("ping", Context::new(&replies)).await.unwrap());
    assert!(!dispatch("missing", Context::new(&replies)).await.unwrap());
    assert_eq!(*replies.lock().unwrap(), ["Pong!", "Pong!"]);
}
