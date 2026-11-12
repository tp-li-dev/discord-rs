//! Local framework waits and saved subscriptions have independent lifetimes.
//! These tests use a volatile backend; they do not establish restart durability.
use bot_framework::interactions::{WaitCancelled, wait_for};
use src::{protocol::BotId, state_store::*};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::sync::watch;

struct FixedClock;
impl StateClock for FixedClock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(1))
    }
}

struct SavedSubscription {
    store: MemoryStateStore,
    scope: StoreScope,
    schema: SchemaVersion,
    subscription: Subscription,
    key: StateKey,
    revision: u64,
}
impl SavedSubscription {
    async fn new() -> Self {
        let store = MemoryStateStore::new(
            StoreLimits {
                max_entries: 4,
                max_bytes: 4096,
                max_value_bytes: 1024,
            },
            Arc::new(FixedClock),
        );
        let scope = StoreScope {
            bot_id: BotId(7),
            shard_id: 2,
        };
        let schema = SchemaVersion::new(1, 1).unwrap();
        let route = CustomId::new(
            PublicLabel::new("app").unwrap(),
            PublicLabel::new("confirm").unwrap(),
            1,
            StateId::from_bytes([1; 16]),
        )
        .unwrap();
        let key = StateKey::new(
            scope,
            route.namespace.clone(),
            route.feature.clone(),
            StateKind::Subscription,
            route.state_id,
        );
        let subscription = Subscription {
            route,
            event: PublicLabel::new("message").unwrap(),
            resume_handler: PublicLabel::new("resume").unwrap(),
            expires_at: Timestamp(100),
            checkpoint: b"fixture checkpoint".to_vec(),
        };
        let revision = subscription
            .save(&store, scope, schema, PutCondition::IfAbsent)
            .await
            .unwrap();
        Self {
            store,
            scope,
            schema,
            subscription,
            key,
            revision,
        }
    }

    async fn assert_unchanged(&self) {
        let record = self
            .store
            .get(&self.key, self.schema)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.revision, self.revision);
        assert_eq!(record.value.expires_at, Some(self.subscription.expires_at));
        assert_eq!(
            Subscription::restore(
                &self.store,
                self.scope,
                &self.subscription.route,
                self.schema,
            )
            .await
            .unwrap(),
            Some(self.subscription.clone()),
        );
    }
}

#[derive(Default)]
struct Probe {
    ready: AtomicBool,
    polls: AtomicUsize,
    drops: AtomicUsize,
}
struct LocalFuture(Arc<Probe>);
impl Future for LocalFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        if self.0.ready.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Drop for LocalFuture {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    // Polling explicitly makes both-ready races deterministic without sleeps.
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn registered_framework_wait_drains_before_ready_work_without_deleting_subscription() {
    let saved = SavedSubscription::new().await;
    let probe = Arc::new(Probe::default());
    let (sender, signal) = watch::channel(false);
    let mut waiting = Box::pin(wait_for(LocalFuture(probe.clone()), signal));
    assert!(poll_once(waiting.as_mut()).is_pending());
    // A non-draining notification must not cancel a registered wait.
    sender.send_replace(false);
    assert!(poll_once(waiting.as_mut()).is_pending());
    assert_eq!(probe.polls.load(Ordering::SeqCst), 2);

    probe.ready.store(true, Ordering::SeqCst);
    sender.send_replace(true);
    assert_eq!(
        poll_once(waiting.as_mut()),
        Poll::Ready(Err(WaitCancelled::Draining)),
    );
    assert_eq!(probe.polls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    saved.assert_unchanged().await;

    assert!(
        Subscription::cancel(
            &saved.store,
            saved.scope,
            &saved.subscription.route,
            Some(saved.revision),
        )
        .await
        .unwrap()
    );
    assert!(
        saved
            .store
            .get(&saved.key, saved.schema)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn dropping_old_wait_keeps_subscription_available_to_an_independent_generation() {
    let saved = SavedSubscription::new().await;
    let probe = Arc::new(Probe::default());
    let (old_sender, old_signal) = watch::channel(false);
    let mut old_wait = Box::pin(wait_for(LocalFuture(probe.clone()), old_signal));
    assert!(poll_once(old_wait.as_mut()).is_pending());
    drop(old_wait);
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    old_sender.send_replace(true);

    let (_new_sender, new_signal) = watch::channel(false);
    assert_eq!(wait_for(async { 42 }, new_signal).await, Ok(42));
    assert_eq!(probe.polls.load(Ordering::SeqCst), 1);
    saved.assert_unchanged().await;
}

#[tokio::test]
async fn drain_or_closed_signal_preempts_even_an_unpolled_ready_future() {
    let saved = SavedSubscription::new().await;
    for (draining, close_sender, expected) in [
        (true, false, WaitCancelled::Draining),
        (true, true, WaitCancelled::Draining),
        (false, true, WaitCancelled::SignalClosed),
    ] {
        let probe = Arc::new(Probe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let (sender, signal) = watch::channel(draining);
        let sender = if close_sender {
            drop(sender);
            None
        } else {
            Some(sender)
        };
        assert_eq!(
            wait_for(LocalFuture(probe.clone()), signal).await,
            Err(expected),
        );
        assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        saved.assert_unchanged().await;
        drop(sender);
    }
}

#[tokio::test]
async fn closed_signal_cancels_a_registered_wait_without_removing_its_subscription() {
    let saved = SavedSubscription::new().await;
    let probe = Arc::new(Probe::default());
    let (sender, signal) = watch::channel(false);
    let mut waiting = Box::pin(wait_for(LocalFuture(probe.clone()), signal));
    assert!(poll_once(waiting.as_mut()).is_pending());
    drop(sender);
    probe.ready.store(true, Ordering::SeqCst);
    assert_eq!(
        poll_once(waiting.as_mut()),
        Poll::Ready(Err(WaitCancelled::SignalClosed)),
    );
    assert_eq!(probe.polls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    saved.assert_unchanged().await;
}

#[tokio::test]
async fn local_wait_cancellation_and_durable_subscription_rejection_remain_distinct() {
    let saved = SavedSubscription::new().await;
    let mut changed = saved.subscription.clone();
    changed.checkpoint = b"replacement fixture".to_vec();
    let (sender, signal) = watch::channel(false);
    assert_eq!(
        wait_for(
            changed.durable_wait(
                &saved.store,
                saved.scope,
                saved.schema,
                PutCondition::IfRevision(saved.revision),
            ),
            signal.clone(),
        )
        .await,
        Ok(Err(StateError::DurabilityRequired)),
    );
    sender.send_replace(true);
    assert_eq!(
        wait_for(
            changed.durable_wait(
                &saved.store,
                saved.scope,
                saved.schema,
                PutCondition::IfRevision(saved.revision),
            ),
            signal,
        )
        .await,
        Err(WaitCancelled::Draining),
    );
    saved.assert_unchanged().await;
}
