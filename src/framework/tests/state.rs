use bot_framework::{dispatch, dispatch_ready, prelude::*};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Counter(AtomicUsize);
struct Missing;

#[hybrid_command(name = "increment")]
async fn increment(ctx: Context<'_>) -> BotResult<()> {
    let value = ctx.state::<Counter>()?.0.fetch_add(1, Ordering::SeqCst) + 1;
    ctx.reply(value.to_string()).await
}
#[event]
async fn ready(ctx: EventContext<'_>, _ready: ReadyEvent) -> BotResult<()> {
    ctx.state::<Counter>()?.0.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn handlers_share_typed_services_and_report_missing_types() {
    let mut states = StateMap::new();
    states.insert(Counter(AtomicUsize::new(0)));
    let replies = Mutex::new(Vec::new());
    let logger = |_: &str| {};
    dispatch_ready(
        EventContext::with_state(&logger, &states),
        ReadyEvent {
            bot_name: "test".into(),
        },
    )
    .await
    .unwrap();
    let (a, b) = tokio::join!(
        dispatch("increment", Context::with_state(&replies, &states)),
        dispatch("increment", Context::with_state(&replies, &states))
    );
    assert!(a.unwrap() && b.unwrap());
    assert_eq!(states.get::<Counter>().unwrap().0.load(Ordering::SeqCst), 3);
    assert!(
        Context::new(&replies)
            .state::<Missing>()
            .err()
            .unwrap()
            .to_string()
            .contains("Missing")
    );
    assert!(
        EventContext::with_state(&logger, &states)
            .state::<Missing>()
            .err()
            .unwrap()
            .to_string()
            .contains("Missing")
    );
}
