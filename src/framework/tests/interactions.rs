use bot_framework::{BotResult, component, interactions::*, modal};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Deserialize)]
struct Counter {
    count: u32,
}
#[component(namespace = "app", name = "counter", version = 1)]
async fn button(ctx: InteractionContext<'_>) -> BotResult<()> {
    ctx.reply(format!(
        "{}:{}",
        ctx.state::<Counter>()?.count,
        ctx.values().join(",")
    ))
    .await
}
#[modal(namespace = "app", name = "counter", version = 1)]
async fn form(ctx: InteractionContext<'_>) -> BotResult<()> {
    ctx.reply(format!("modal:{}", ctx.state::<Counter>()?.count))
        .await
}
// A controlled fixture, not a cryptographic codec or a durable backend.
type Records = HashMap<String, (u16, u64, Vec<u8>)>;
#[derive(Default)]
struct FakeStore {
    records: Mutex<Records>,
    now: AtomicU64,
}
impl InteractionStateStore for FakeStore {
    fn restore<'a>(&'a self, _: InteractionKind, id: &'a str) -> RestoreFuture<'a> {
        Box::pin(async move {
            if id == "tampered" {
                return Err(InteractionError::InvalidId);
            }
            if id == "wire-version" {
                return Err(InteractionError::UnsupportedVersion);
            }
            let records = self.records.lock().unwrap();
            let (version, expires, data) =
                records.get(id).ok_or(InteractionError::ExpiredOrMissing)?;
            if self.now.load(Ordering::SeqCst) >= *expires {
                return Err(InteractionError::ExpiredOrMissing);
            }
            Ok(RestoredState {
                route: Route {
                    namespace: "app".into(),
                    name: "counter".into(),
                    version: *version,
                },
                data: data.clone(),
            })
        })
    }
}
#[tokio::test]
async fn routes_across_independent_registries_and_errors() {
    let store = FakeStore::default();
    store.records.lock().unwrap().extend([
        ("good".into(), (1, 10, br#"{"count":7}"#.to_vec())),
        ("v2".into(), (2, 10, vec![])),
        ("bad-state".into(), (1, 10, b"invalid".to_vec())),
    ]);
    let old = Registry::new().unwrap();
    let new = Registry::new().unwrap();
    let replies = Mutex::new(vec![]);
    old.dispatch(
        &store,
        InteractionKind::Component,
        "good",
        &["a".into()],
        &replies,
    )
    .await
    .unwrap();
    new.dispatch(
        &store,
        InteractionKind::Component,
        "good",
        &["b".into()],
        &replies,
    )
    .await
    .unwrap();
    new.dispatch(&store, InteractionKind::Modal, "good", &[], &replies)
        .await
        .unwrap();
    assert_eq!(*replies.lock().unwrap(), ["7:a", "7:b", "modal:7"]);
    for (id, error) in [
        ("missing", InteractionError::ExpiredOrMissing),
        ("tampered", InteractionError::InvalidId),
        ("wire-version", InteractionError::UnsupportedVersion),
        ("v2", InteractionError::UnsupportedVersion),
        ("bad-state", InteractionError::HandlerFailed),
    ] {
        assert_eq!(
            new.dispatch(&store, InteractionKind::Component, id, &[], &replies)
                .await,
            Err(error)
        );
        assert!(!error.response().is_empty());
    }
    store.now.store(10, Ordering::SeqCst);
    assert_eq!(
        new.dispatch(&store, InteractionKind::Component, "good", &[], &replies)
            .await,
        Err(InteractionError::ExpiredOrMissing)
    );
}
#[tokio::test]
async fn wait_cancelled_on_drain_or_closed_signal() {
    let (tx, rx) = tokio::sync::watch::channel(false);
    assert_eq!(wait_for(async { 3 }, rx.clone()).await, Ok(3));
    tx.send(true).unwrap();
    assert_eq!(
        wait_for(async { 3 }, rx).await,
        Err(WaitCancelled::Draining)
    );
    let (tx, rx) = tokio::sync::watch::channel(false);
    drop(tx);
    assert_eq!(
        wait_for(std::future::pending::<()>(), rx).await,
        Err(WaitCancelled::SignalClosed)
    );
}
