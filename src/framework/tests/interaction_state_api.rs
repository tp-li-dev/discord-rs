use bot_framework::{BotResult, component, interactions::*};
use std::sync::Mutex;

// A restore-only fixture verifies compatibility, not production authentication.
struct LegacyStore;
impl InteractionStateStore for LegacyStore {
    fn restore<'a>(&'a self, _: InteractionKind, _: &'a str) -> RestoreFuture<'a> {
        Box::pin(async {
            Ok(RestoredState {
                route: route("legacy"),
                data: b"7".to_vec(),
            })
        })
    }
}

fn route(name: &str) -> Route {
    Route {
        namespace: "app".into(),
        name: name.into(),
        version: 1,
    }
}

#[component(namespace = "app", name = "legacy", version = 1)]
async fn legacy(ctx: InteractionContext<'_>) -> BotResult<()> {
    assert_eq!(ctx.state::<u8>()?, 7);
    assert_eq!(
        ctx.load_state::<u8>().await.err(),
        Some(InteractionError::StateOperationsUnsupported)
    );
    assert_eq!(
        ctx.save_state(&8, 1, 100).await,
        Err(InteractionError::StateOperationsUnsupported)
    );
    assert_eq!(
        ctx.create_state(InteractionKind::Modal, "next", 1, &8, 100)
            .await
            .err(),
        Some(InteractionError::StateOperationsUnsupported)
    );
    assert_eq!(
        ctx.delete_state(1).await,
        Err(InteractionError::StateOperationsUnsupported)
    );
    ctx.reply("legacy read preserved").await
}

#[tokio::test]
async fn restore_only_stores_still_dispatch_and_explicitly_reject_new_operations() {
    let replies = Mutex::new(Vec::new());
    Registry::new()
        .unwrap()
        .dispatch(
            &LegacyStore,
            InteractionKind::Component,
            "fixture",
            &[],
            &replies,
        )
        .await
        .unwrap();
    assert_eq!(*replies.lock().unwrap(), ["legacy read preserved"]);
}

struct UncertainStore;
impl InteractionStateStore for UncertainStore {
    fn restore<'a>(&'a self, _: InteractionKind, _: &'a str) -> RestoreFuture<'a> {
        Box::pin(async {
            Ok(RestoredState {
                route: route("uncertain"),
                data: b"7".to_vec(),
            })
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        _: InteractionKind,
        _: &'a str,
        _: u64,
        _: Vec<u8>,
        _: u64,
    ) -> StateFuture<'a, u64> {
        Box::pin(async { Err(InteractionError::UnknownOutcome) })
    }
}

#[component(namespace = "app", name = "uncertain", version = 1)]
async fn uncertain(ctx: InteractionContext<'_>) -> BotResult<()> {
    ctx.save_state(&8, 1, 100).await?;
    ctx.reply("saved").await
}

#[tokio::test]
async fn an_ambiguous_write_propagated_by_a_handler_stays_distinct_at_dispatch() {
    let replies = Mutex::new(Vec::new());
    assert_eq!(
        Registry::new()
            .unwrap()
            .dispatch(
                &UncertainStore,
                InteractionKind::Component,
                "fixture",
                &[],
                &replies
            )
            .await,
        Err(InteractionError::UnknownOutcome)
    );
    assert!(replies.lock().unwrap().is_empty());
}
