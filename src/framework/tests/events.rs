use bot_framework::{dispatch_ready, prelude::*};
use std::sync::Mutex;

#[event]
async fn z_ready(ctx: EventContext<'_>, ready: ReadyEvent) -> BotResult<()> {
    ctx.log_info(&format!("z: {}", ready.bot_name));
    Ok(())
}

#[event]
async fn a_ready(ctx: EventContext<'_>, ready: ReadyEvent) -> BotResult<()> {
    ctx.log_info(&format!("a: {}", ready.bot_name));
    Ok(())
}

#[tokio::test]
async fn all_linked_ready_handlers_run_in_name_order() {
    let messages = Mutex::new(Vec::new());
    let logger = |message: &str| messages.lock().unwrap().push(message.to_owned());
    dispatch_ready(
        EventContext::new(&logger),
        ReadyEvent {
            bot_name: "test".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(*messages.lock().unwrap(), ["a: test", "z: test"]);
}
