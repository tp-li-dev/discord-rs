use bot_framework::{dispatch_ready, prelude::*};
use std::sync::atomic::{AtomicBool, Ordering};

static CALLED: AtomicBool = AtomicBool::new(false);

#[event]
async fn a_fails(_ctx: EventContext<'_>, _ready: ReadyEvent) -> BotResult<()> {
    Err("event failure".into())
}
#[event]
async fn z_not_called(_ctx: EventContext<'_>, _ready: ReadyEvent) -> BotResult<()> {
    CALLED.store(true, Ordering::SeqCst);
    Ok(())
}
#[tokio::test]
async fn handler_error_is_returned_and_stops_dispatch() {
    let logger = |_: &str| {};
    let result = dispatch_ready(
        EventContext::new(&logger),
        ReadyEvent {
            bot_name: "test".into(),
        },
    )
    .await;
    assert_eq!(result.unwrap_err().to_string(), "event failure");
    assert!(!CALLED.load(Ordering::SeqCst));
}
