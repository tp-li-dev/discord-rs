use bot_framework::{BotResult, component, interactions::*};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::{Notify, watch};

static STARTED: Notify = Notify::const_new();
static DROPPED: AtomicUsize = AtomicUsize::new(0);

struct LocalWait;
impl Future for LocalWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        STARTED.notify_one();
        Poll::Pending
    }
}
impl Drop for LocalWait {
    fn drop(&mut self) {
        DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

#[component(namespace = "generation", name = "wait", version = 1)]
async fn waiting(ctx: InteractionContext<'_>) -> BotResult<()> {
    match ctx.values().first().map(String::as_str) {
        Some("pending") => {
            assert_eq!(ctx.wait_for(LocalWait).await, Err(WaitCancelled::Draining));
            // Cancellation leaves the handler alive to finish explicit cleanup.
            assert_eq!(ctx.state::<u64>()?, 7);
            ctx.reply("cancelled; cleanup completed").await
        }
        Some("legacy") => {
            assert_eq!(
                ctx.wait_for(async { 8 }).await,
                Err(WaitCancelled::SignalClosed)
            );
            ctx.reply("no generation signal").await
        }
        _ => {
            assert_eq!(ctx.wait_for(async { 8 }).await, Ok(8));
            ctx.reply("completed").await
        }
    }
}

// This restore-only fixture exercises lifecycle wiring, not ID authentication.
#[derive(Default)]
struct Store {
    reads: AtomicUsize,
    drain_during_read: Option<watch::Sender<bool>>,
}
impl InteractionStateStore for Store {
    fn restore<'a>(&'a self, _: InteractionKind, _: &'a str) -> RestoreFuture<'a> {
        Box::pin(async {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if let Some(drain) = &self.drain_during_read {
                drain.send_replace(true);
            }
            Ok(RestoredState {
                route: Route {
                    namespace: "generation".into(),
                    name: "wait".into(),
                    version: 1,
                },
                data: b"7".to_vec(),
            })
        })
    }
}

#[tokio::test]
async fn context_wait_is_cancelled_drops_future_and_allows_handler_cleanup() {
    let store = Store::default();
    let registry = Registry::new().unwrap();
    let replies = Mutex::new(Vec::new());
    let (drain, signal) = watch::channel(false);
    let values = ["pending".into()];
    let dispatch = registry.dispatch_with_drain(
        &store,
        InteractionKind::Component,
        "fixture",
        &values,
        &replies,
        signal,
    );
    let cancel = async {
        STARTED.notified().await;
        drain.send_replace(true);
    };
    let (result, ()) = tokio::join!(dispatch, cancel);
    result.unwrap();
    assert_eq!(DROPPED.load(Ordering::SeqCst), 1);
    assert_eq!(*replies.lock().unwrap(), ["cancelled; cleanup completed"]);
}

#[tokio::test]
async fn preexisting_drain_or_closed_signal_rejects_before_state_lookup() {
    let store = Store::default();
    let registry = Registry::new().unwrap();
    let replies = Mutex::new(Vec::new());
    let (drain, signal) = watch::channel(true);
    assert_eq!(
        registry
            .dispatch_with_drain(
                &store,
                InteractionKind::Component,
                "fixture",
                &[],
                &replies,
                signal,
            )
            .await,
        Err(InteractionError::Draining)
    );
    drop(drain);
    let (drain, signal) = watch::channel(false);
    drop(drain);
    assert_eq!(
        registry
            .dispatch_with_drain(
                &store,
                InteractionKind::Component,
                "fixture",
                &[],
                &replies,
                signal,
            )
            .await,
        Err(InteractionError::Draining)
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert!(replies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn drain_while_restoring_rejects_before_handler_execution() {
    let (drain, signal) = watch::channel(false);
    let store = Store {
        drain_during_read: Some(drain),
        ..Store::default()
    };
    let replies = Mutex::new(Vec::new());
    assert_eq!(
        Registry::new()
            .unwrap()
            .dispatch_with_drain(
                &store,
                InteractionKind::Component,
                "fixture",
                &[],
                &replies,
                signal,
            )
            .await,
        Err(InteractionError::Draining)
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert!(replies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn legacy_dispatch_remains_available_and_fails_closed_only_for_wait() {
    let store = Store::default();
    let registry = Registry::new().unwrap();
    let replies = Mutex::new(Vec::new());
    registry
        .dispatch(
            &store,
            InteractionKind::Component,
            "fixture",
            &["legacy".into()],
            &replies,
        )
        .await
        .unwrap();
    let (_drain, signal) = watch::channel(false);
    registry
        .dispatch_with_drain(
            &store,
            InteractionKind::Component,
            "fixture",
            &[],
            &replies,
            signal,
        )
        .await
        .unwrap();
    assert_eq!(
        *replies.lock().unwrap(),
        ["no generation signal", "completed"]
    );
}

#[test]
fn interaction_kind_has_stable_wire_names() {
    assert_eq!(
        serde_json::to_string(&InteractionKind::Component).unwrap(),
        "\"component\""
    );
    assert_eq!(
        serde_json::from_str::<InteractionKind>("\"modal\"").unwrap(),
        InteractionKind::Modal
    );
    assert!(serde_json::from_str::<InteractionKind>("\"button\"").is_err());
}
