//! Local-file contract tests. Payloads are synthetic; no Discord/network access.
use src::{
    protocol::{BotId, GenerationId, OwnershipEpoch},
    state_store::{
        CustomId, Durability, FileStateStore, MemoryStateStore, PublicLabel, PutCondition,
        SchemaVersion, StateClock, StateError, StateId, StateKey, StateKind, StateRecord,
        StateStore, StateValue, StoreFuture, StoreLimits, StoreScope, Subscription, Timestamp,
    },
    subscription::*,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Clone)]
struct Clock(Arc<AtomicU64>);
impl Clock {
    fn new() -> Self {
        Self(Arc::new(AtomicU64::new(100)))
    }
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
impl StateClock for Clock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(self.0.load(Ordering::SeqCst)))
    }
}

struct Fixture {
    path: PathBuf,
    clock: Clock,
}
impl Fixture {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "subscriptions-{}-{}",
                std::process::id(),
                getrandom::u64().unwrap()
            )),
            clock: Clock::new(),
        }
    }
    async fn store(&self) -> Arc<FileStateStore> {
        Arc::new(
            FileStateStore::open(
                &self.path,
                store_limits(),
                4 * 1024 * 1024,
                Arc::new(self.clock.clone()),
            )
            .await
            .unwrap(),
        )
    }
    async fn queue(&self) -> SubscriptionQueue {
        self.queue_with(scope(1, 0), limits()).await
    }
    async fn queue_with(&self, scope: StoreScope, limits: SubscriptionLimits) -> SubscriptionQueue {
        open_queue(self.store().await, self.clock.clone(), scope, limits)
            .await
            .unwrap()
    }
    async fn prepared(&self) -> SubscriptionQueue {
        let queue = self.queue().await;
        prepare(&queue).await;
        queue
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn label(value: &str) -> PublicLabel {
    PublicLabel::new(value).unwrap()
}
fn id(value: u8) -> StateId {
    StateId::from_bytes([value; 16])
}
fn scope(bot: u64, shard: u32) -> StoreScope {
    StoreScope {
        bot_id: BotId(bot),
        shard_id: shard,
    }
}
fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).unwrap()
}
fn owner(generation: u64, epoch: u64) -> DeliveryOwner {
    DeliveryOwner {
        generation: GenerationId(generation),
        epoch: OwnershipEpoch(epoch),
    }
}
fn limits() -> SubscriptionLimits {
    SubscriptionLimits {
        max_subscriptions: 8,
        max_deliveries: 16,
        max_payload_bytes: 128,
        max_checkpoint_bytes: 128,
        max_record_bytes: 64 * 1024,
        visibility_timeout_ms: 100,
        store_timeout_ms: 5_000,
    }
}
fn store_limits() -> StoreLimits {
    StoreLimits {
        max_entries: 16,
        max_bytes: 1024 * 1024,
        max_value_bytes: 128 * 1024,
    }
}
fn subscription(value: u8) -> Subscription {
    Subscription {
        route: CustomId::new(label("app"), label("await_message"), 1, id(value)).unwrap(),
        event: label("message"),
        resume_handler: label("resume_message"),
        expires_at: Timestamp(5_000),
        checkpoint: b"initial".to_vec(),
    }
}
fn event(value: u8) -> SubscriptionEvent {
    SubscriptionEvent {
        route: subscription(1).route,
        event: label("message"),
        event_id: id(value),
        deduplication_key: Some(id(value)),
        payload: b"synthetic-event".to_vec(),
    }
}
fn root_key(scope: StoreScope) -> StateKey {
    StateKey::new(
        scope,
        label("app"),
        label("subscription_queue_v1"),
        StateKind::Application,
        id(9),
    )
}
async fn open_queue(
    store: Arc<dyn StateStore>,
    clock: Clock,
    scope: StoreScope,
    limits: SubscriptionLimits,
) -> Result<SubscriptionQueue, SubscriptionError> {
    SubscriptionQueue::open(store, Arc::new(clock), scope, label("app"), id(9), limits).await
}
async fn prepare(queue: &SubscriptionQueue) {
    queue
        .activate(owner(1, 1), Timestamp(10_000))
        .await
        .unwrap();
    assert!(
        queue
            .register(owner(1, 1), subscription(1), schema())
            .await
            .unwrap()
    );
}
async fn claim(queue: &SubscriptionQueue, owner: DeliveryOwner) -> Delivery {
    queue.claim(owner, schema(), &[1]).await.unwrap().unwrap()
}

/// Failure at the application/store boundary, with a real file behind every put.
struct FaultStore {
    inner: Arc<FileStateStore>,
    clock: Clock,
    next_put: AtomicU8,
    next_get: AtomicU8,
    after_commit_time: AtomicU64,
    puts: AtomicUsize,
}
impl FaultStore {
    fn new(inner: Arc<FileStateStore>, clock: Clock) -> Self {
        Self {
            inner,
            clock,
            next_put: AtomicU8::new(0),
            next_get: AtomicU8::new(0),
            after_commit_time: AtomicU64::new(0),
            puts: AtomicUsize::new(0),
        }
    }
    fn fail_before_put(&self) {
        self.next_put.store(1, Ordering::SeqCst);
    }
    fn lose_commit_response(&self) {
        self.next_put.store(2, Ordering::SeqCst);
    }
    fn advance_after_commit(&self, to: u64) {
        self.after_commit_time.store(to, Ordering::SeqCst);
        self.next_put.store(3, Ordering::SeqCst);
    }
}
impl StateStore for FaultStore {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        Box::pin(async move {
            if self.next_get.swap(0, Ordering::SeqCst) == 1 {
                std::future::pending::<()>().await;
            }
            self.inner.get(key, reader).await
        })
    }
    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let fault = self.next_put.swap(0, Ordering::SeqCst);
            if fault == 1 {
                return Err(StateError::BackendUnavailable);
            }
            if fault == 4 {
                std::future::pending::<()>().await;
            }
            let revision = self.inner.put(key, value, condition).await?;
            if fault == 2 {
                return Err(StateError::UnknownOutcome);
            }
            if fault == 3 {
                self.clock
                    .set(self.after_commit_time.load(Ordering::SeqCst));
            }
            Ok(revision)
        })
    }
    fn delete<'a>(
        &'a self,
        key: &'a StateKey,
        expected_revision: Option<u64>,
    ) -> StoreFuture<'a, bool> {
        self.inner.delete(key, expected_revision)
    }
    fn durability(&self) -> Durability {
        Durability::Durable
    }
}

#[tokio::test]
async fn registration_and_checkpoint_survive_reopen_with_bot_and_shard_isolation() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    assert!(
        !queue
            .register(owner(1, 1), subscription(1), schema())
            .await
            .unwrap()
    );
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    queue
        .ack(&delivery.receipt, b"finished".to_vec())
        .await
        .unwrap();
    drop(queue);

    let reopened = fixture.queue().await;
    let registered = reopened.registrations(schema()).await.unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].checkpoint, b"finished");
    assert!(
        reopened
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    for other in [scope(2, 0), scope(1, 1)] {
        let other_queue = fixture.queue_with(other, limits()).await;
        assert!(
            other_queue
                .registrations(schema())
                .await
                .unwrap()
                .is_empty()
        );
        other_queue
            .activate(owner(1, 1), Timestamp(10_000))
            .await
            .unwrap();
        assert!(other_queue.ack(&delivery.receipt, vec![]).await.is_err());
    }
}

#[tokio::test]
async fn delivery_repeats_only_after_lease_boundary_and_rejects_stale_ack() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let first = claim(&queue, owner(1, 1)).await;
    assert_eq!(first.receipt.attempt(), 1);
    assert_eq!(first.receipt.expires_at(), Timestamp(200));
    fixture.clock.set(199);
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    drop(queue);
    fixture.clock.set(200);
    let reopened = fixture.queue().await;
    let second = claim(&reopened, owner(1, 1)).await;
    assert_eq!(second.receipt.attempt(), 2);
    assert_eq!(first.receipt.sequence(), second.receipt.sequence());
    assert_eq!(first.event_id, second.event_id);
    assert_eq!(first.deduplication_key, second.deduplication_key);
    assert_eq!(first.payload, second.payload);
    assert_eq!(
        reopened.ack(&first.receipt, vec![]).await,
        Err(SubscriptionError::StaleDelivery)
    );
    reopened.ack(&second.receipt, vec![]).await.unwrap();
}

#[tokio::test]
async fn semantic_duplicate_stays_suppressed_after_ack_and_restart() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    let result = queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let sequence = match result {
        EnqueueResult::Queued { sequence } => sequence,
        _ => panic!("first event must enter the queue"),
    };
    let delivery = claim(&queue, owner(1, 1)).await;
    queue.ack(&delivery.receipt, vec![]).await.unwrap();
    drop(queue);

    let reopened = fixture.queue().await;
    let mut duplicate = event(11);
    duplicate.event_id = id(12);
    assert!(matches!(
        reopened.enqueue(owner(1, 1), duplicate).await,
        Ok(EnqueueResult::Duplicate { sequence: found }) if found == sequence
    ));
    let mut conflict = event(11);
    conflict.payload = b"different-event".to_vec();
    assert_eq!(
        reopened.enqueue(owner(1, 1), conflict).await,
        Err(SubscriptionError::DeduplicationConflict)
    );
    assert!(
        reopened
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn events_without_semantic_keys_only_deduplicate_the_same_acceptance_id() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    for value in [11, 12] {
        let mut input = event(value);
        input.deduplication_key = None;
        assert!(matches!(
            queue.enqueue(owner(1, 1), input).await,
            Ok(EnqueueResult::Queued { .. })
        ));
    }
    let mut same_id = event(11);
    same_id.deduplication_key = None;
    assert!(matches!(
        queue.enqueue(owner(1, 1), same_id).await,
        Ok(EnqueueResult::Duplicate { .. })
    ));
    let first = claim(&queue, owner(1, 1)).await;
    queue.ack(&first.receipt, vec![]).await.unwrap();
    let second = claim(&queue, owner(1, 1)).await;
    assert_ne!(first.event_id, second.event_id);
}

#[tokio::test]
async fn generation_switch_preserves_inflight_completion_but_fences_new_old_work() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let old = claim(&queue, owner(1, 1)).await;
    queue
        .activate(owner(2, 1), Timestamp(10_000))
        .await
        .unwrap();
    assert!(matches!(
        queue.claim(owner(1, 1), schema(), &[1]).await,
        Err(SubscriptionError::StaleOwner)
    ));
    assert_eq!(
        queue.enqueue(owner(1, 1), event(12)).await,
        Err(SubscriptionError::StaleOwner)
    );
    assert!(
        queue
            .claim(owner(2, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    queue.ack(&old.receipt, vec![]).await.unwrap();
    queue.enqueue(owner(2, 1), event(12)).await.unwrap();
    let next = claim(&queue, owner(2, 1)).await;
    assert_eq!(next.receipt.owner(), owner(2, 1));
    assert_eq!(
        queue.activate(owner(1, 1), Timestamp(20_000)).await,
        Err(SubscriptionError::StaleOwner)
    );
}

#[tokio::test]
async fn drain_is_durable_and_does_not_cancel_subscriptions_or_release_work() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    queue.begin_drain(owner(1, 1)).await.unwrap();
    drop(queue);
    let reopened = fixture.queue().await;
    assert!(matches!(
        reopened.claim(owner(1, 1), schema(), &[1]).await,
        Err(SubscriptionError::Draining)
    ));
    assert_eq!(reopened.registrations(schema()).await.unwrap().len(), 1);
    reopened
        .ack(&delivery.receipt, b"drained".to_vec())
        .await
        .unwrap();
    reopened
        .activate(owner(2, 1), Timestamp(10_000))
        .await
        .unwrap();
    assert_eq!(
        reopened.registrations(schema()).await.unwrap()[0].checkpoint,
        b"drained"
    );
}

#[tokio::test]
async fn external_intent_survives_expiry_and_never_becomes_automatic_redelivery() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    queue
        .begin_external(&delivery.receipt, id(77))
        .await
        .unwrap();
    assert_eq!(
        queue.ack(&delivery.receipt, vec![]).await,
        Err(SubscriptionError::ExternalOutcomeUnknown)
    );
    assert!(queue.nack(&delivery.receipt).await.is_err());
    drop(queue);
    fixture.clock.set(5_000);
    let reopened = fixture.queue().await;
    reopened
        .activate(owner(2, 2), Timestamp(10_000))
        .await
        .unwrap();
    assert!(
        reopened
            .claim(owner(2, 2), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    let unknown = reopened.unknown_operations().await.unwrap();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].receipt, delivery.receipt);
    assert_eq!(unknown[0].operation_id, id(77));
    assert!(
        reopened
            .cancel(owner(2, 2), &subscription(1).route)
            .await
            .is_err()
    );
    assert!(
        reopened
            .confirm_external(&delivery.receipt, id(77), vec![])
            .await
            .is_err()
    );
    drop(reopened);
    assert_eq!(
        fixture
            .queue()
            .await
            .unknown_operations()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn correlated_external_result_commits_checkpoint_and_ack_together() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    queue
        .begin_external(&delivery.receipt, id(77))
        .await
        .unwrap();
    assert!(
        queue
            .confirm_external(&delivery.receipt, id(78), vec![])
            .await
            .is_err()
    );
    queue
        .confirm_external(&delivery.receipt, id(77), b"confirmed".to_vec())
        .await
        .unwrap();
    assert_eq!(
        queue
            .confirm_external(&delivery.receipt, id(78), b"confirmed".to_vec())
            .await,
        Err(SubscriptionError::StaleDelivery)
    );
    assert_eq!(
        queue.ack(&delivery.receipt, b"confirmed".to_vec()).await,
        Err(SubscriptionError::StaleDelivery)
    );
    queue
        .confirm_external(&delivery.receipt, id(77), b"confirmed".to_vec())
        .await
        .unwrap();
    drop(queue);
    let reopened = fixture.queue().await;
    assert!(reopened.unknown_operations().await.unwrap().is_empty());
    assert_eq!(
        reopened.registrations(schema()).await.unwrap()[0].checkpoint,
        b"confirmed"
    );
    assert!(
        reopened
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn explicit_nack_retries_but_cancellation_never_releases_running_work() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let first = claim(&queue, owner(1, 1)).await;
    assert!(
        queue
            .cancel(owner(1, 1), &subscription(1).route)
            .await
            .is_err()
    );
    queue.nack(&first.receipt).await.unwrap();
    let second = claim(&queue, owner(1, 1)).await;
    assert_eq!(second.receipt.attempt(), 2);
    assert_eq!(first.event_id, second.event_id);
    assert!(queue.ack(&first.receipt, vec![]).await.is_err());
    queue.ack(&second.receipt, vec![]).await.unwrap();
    assert!(
        queue
            .cancel(owner(1, 1), &subscription(1).route)
            .await
            .unwrap()
    );
    assert!(queue.registrations(schema()).await.unwrap().is_empty());
    assert!(
        !queue
            .cancel(owner(1, 1), &subscription(1).route)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn per_subscription_order_protects_checkpoints_without_blocking_other_subscriptions() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    queue.enqueue(owner(1, 1), event(12)).await.unwrap();
    let first = claim(&queue, owner(1, 1)).await;
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    queue
        .register(owner(1, 1), subscription(2), schema())
        .await
        .unwrap();
    let mut other_event = event(21);
    other_event.route = subscription(2).route;
    queue.enqueue(owner(1, 1), other_event).await.unwrap();
    let other = claim(&queue, owner(1, 1)).await;
    assert_eq!(other.subscription.route.state_id, id(2));
    queue.ack(&other.receipt, vec![]).await.unwrap();
    queue
        .ack(&first.receipt, b"after-first".to_vec())
        .await
        .unwrap();
    let second = claim(&queue, owner(1, 1)).await;
    assert_eq!(second.event_id, id(12));
    assert_eq!(second.subscription.checkpoint, b"after-first");
    queue
        .ack(&second.receipt, b"after-second".to_vec())
        .await
        .unwrap();
    // Repeated older ACK cannot roll a newer checkpoint backwards.
    queue
        .ack(&first.receipt, b"after-first".to_vec())
        .await
        .unwrap();
    let current = queue.registrations(schema()).await.unwrap();
    assert_eq!(
        current
            .iter()
            .find(|item| item.route.state_id == id(1))
            .unwrap()
            .checkpoint,
        b"after-second"
    );
}

#[tokio::test]
async fn independent_handles_never_claim_the_same_attempt_twice() {
    let fixture = Fixture::new();
    let first = fixture.prepared().await;
    first.enqueue(owner(1, 1), event(11)).await.unwrap();
    let second = fixture.queue().await;
    let (a, b) = tokio::join!(
        first.claim(owner(1, 1), schema(), &[1]),
        second.claim(owner(1, 1), schema(), &[1])
    );
    let mut claims = Vec::new();
    for result in [a, b] {
        match result {
            Ok(Some(delivery)) => claims.push(delivery),
            Ok(None) | Err(SubscriptionError::State(StateError::Conflict)) => {}
            other => panic!("unexpected claim result: {other:?}"),
        }
    }
    assert_eq!(claims.len(), 1);
    first.ack(&claims[0].receipt, vec![]).await.unwrap();
    assert!(
        second
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn receipt_is_bound_to_scope_even_when_sequence_attempt_and_owner_collide() {
    let fixture = Fixture::new();
    let first = fixture.prepared().await;
    let second = fixture.queue_with(scope(2, 0), limits()).await;
    prepare(&second).await;
    first.enqueue(owner(1, 1), event(11)).await.unwrap();
    second.enqueue(owner(1, 1), event(11)).await.unwrap();
    let a = claim(&first, owner(1, 1)).await;
    let b = claim(&second, owner(1, 1)).await;
    assert_eq!(a.receipt.sequence(), b.receipt.sequence());
    assert_eq!(a.receipt.attempt(), b.receipt.attempt());
    assert!(second.ack(&a.receipt, vec![]).await.is_err());
    assert!(first.nack(&b.receipt).await.is_err());
    first.ack(&a.receipt, vec![]).await.unwrap();
    second.ack(&b.receipt, vec![]).await.unwrap();
}

#[tokio::test]
async fn incompatible_schema_and_route_version_do_not_consume_deliveries() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue
        .activate(owner(1, 1), Timestamp(10_000))
        .await
        .unwrap();
    let new_schema = SchemaVersion::new(2, 2).unwrap();
    let mut descriptor = subscription(1);
    descriptor.route.version = 2;
    queue
        .register(owner(1, 1), descriptor.clone(), new_schema)
        .await
        .unwrap();
    let mut input = event(11);
    input.route = descriptor.route;
    queue.enqueue(owner(1, 1), input).await.unwrap();
    assert!(queue.registrations(schema()).await.is_err());
    assert!(queue.claim(owner(1, 1), schema(), &[2]).await.is_err());
    assert!(queue.claim(owner(1, 1), new_schema, &[1]).await.is_err());
    let delivery = queue
        .claim(owner(1, 1), new_schema, &[2])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.receipt.attempt(), 1);
    assert_eq!(delivery.subscription.route.version, 2);
}

#[tokio::test]
async fn routing_mismatches_and_changed_registration_are_rejected() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    let mut changed = subscription(1);
    changed.resume_handler = label("different_handler");
    assert_eq!(
        queue.register(owner(1, 1), changed, schema()).await,
        Err(SubscriptionError::SubscriptionConflict)
    );
    for mutate in [0, 1, 2, 3] {
        let mut invalid = event(11);
        match mutate {
            0 => invalid.event = label("reaction"),
            1 => invalid.route.namespace = label("other_app"),
            2 => invalid.route.version = 2,
            _ => invalid.route.feature = label("other_feature"),
        }
        assert!(queue.enqueue(owner(1, 1), invalid).await.is_err());
    }
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn capacity_rejection_preserves_ack_dedup_and_checkpoint() {
    let fixture = Fixture::new();
    let queue = fixture
        .queue_with(
            scope(1, 0),
            SubscriptionLimits {
                max_deliveries: 1,
                ..limits()
            },
        )
        .await;
    prepare(&queue).await;
    let mut oversized = event(11);
    oversized.payload = vec![0; 129];
    assert!(queue.enqueue(owner(1, 1), oversized).await.is_err());
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    assert!(queue.ack(&delivery.receipt, vec![0; 129]).await.is_err());
    assert_eq!(
        queue.registrations(schema()).await.unwrap()[0].checkpoint,
        b"initial"
    );
    queue
        .ack(&delivery.receipt, b"saved".to_vec())
        .await
        .unwrap();
    assert_eq!(
        queue.enqueue(owner(1, 1), event(12)).await,
        Err(SubscriptionError::Capacity)
    );
    assert!(matches!(
        queue.enqueue(owner(1, 1), event(11)).await,
        Ok(EnqueueResult::Duplicate { .. })
    ));
    assert_eq!(
        queue.registrations(schema()).await.unwrap()[0].checkpoint,
        b"saved"
    );
}

#[tokio::test]
async fn expiry_bounds_delivery_lease_and_expired_subscription_is_not_resumed() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1, 1), Timestamp(180)).await.unwrap();
    let mut descriptor = subscription(1);
    descriptor.expires_at = Timestamp(150);
    queue
        .register(owner(1, 1), descriptor, schema())
        .await
        .unwrap();
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    assert_eq!(delivery.receipt.expires_at(), Timestamp(150));
    fixture.clock.set(150);
    assert!(queue.ack(&delivery.receipt, vec![]).await.is_err());
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    assert!(queue.registrations(schema()).await.unwrap().is_empty());
    fixture.clock.set(180);
    assert!(queue.claim(owner(1, 1), schema(), &[1]).await.is_err());
}

#[tokio::test]
async fn clock_regression_and_volatile_backend_fail_closed() {
    let fixture = Fixture::new();
    let volatile = Arc::new(MemoryStateStore::new(
        store_limits(),
        Arc::new(fixture.clock.clone()),
    ));
    assert!(matches!(
        open_queue(volatile, fixture.clock.clone(), scope(1, 0), limits()).await,
        Err(SubscriptionError::State(StateError::DurabilityRequired))
    ));
    assert!(matches!(
        open_queue(
            fixture.store().await,
            fixture.clock.clone(),
            scope(1, 0),
            SubscriptionLimits {
                visibility_timeout_ms: 0,
                ..limits()
            }
        )
        .await,
        Err(SubscriptionError::InvalidConfig)
    ));
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    fixture.clock.set(99);
    assert!(matches!(
        queue.claim(owner(1, 1), schema(), &[1]).await,
        Err(SubscriptionError::State(StateError::ClockRegressed))
    ));
}

#[tokio::test]
async fn failed_and_unacknowledged_commits_are_reconciled_without_blind_retry() {
    let fixture = Fixture::new();
    let faulty = Arc::new(FaultStore::new(
        fixture.store().await,
        fixture.clock.clone(),
    ));
    let queue = open_queue(faulty.clone(), fixture.clock.clone(), scope(1, 0), limits())
        .await
        .unwrap();
    prepare(&queue).await;

    faulty.fail_before_put();
    assert_eq!(
        queue.enqueue(owner(1, 1), event(11)).await,
        Err(SubscriptionError::State(StateError::BackendUnavailable))
    );
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
    faulty.lose_commit_response();
    let puts_before = faulty.puts.load(Ordering::SeqCst);
    assert_eq!(
        queue.enqueue(owner(1, 1), event(11)).await,
        Err(SubscriptionError::State(StateError::UnknownOutcome))
    );
    assert_eq!(faulty.puts.load(Ordering::SeqCst), puts_before + 1);
    drop(queue);

    let reopened = fixture.queue().await;
    assert!(matches!(
        reopened.enqueue(owner(1, 1), event(11)).await,
        Ok(EnqueueResult::Duplicate { .. })
    ));
    let delivery = claim(&reopened, owner(1, 1)).await;
    let through_fault = open_queue(faulty.clone(), fixture.clock.clone(), scope(1, 0), limits())
        .await
        .unwrap();
    faulty.lose_commit_response();
    assert_eq!(
        through_fault
            .ack(&delivery.receipt, b"committed-checkpoint".to_vec())
            .await,
        Err(SubscriptionError::State(StateError::UnknownOutcome))
    );
    drop(through_fault);
    drop(reopened);
    let final_queue = fixture.queue().await;
    assert_eq!(
        final_queue.registrations(schema()).await.unwrap()[0].checkpoint,
        b"committed-checkpoint"
    );
    assert!(
        final_queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn lost_external_intent_write_response_blocks_send_and_replay() {
    let fixture = Fixture::new();
    let faulty = Arc::new(FaultStore::new(
        fixture.store().await,
        fixture.clock.clone(),
    ));
    let queue = open_queue(faulty.clone(), fixture.clock.clone(), scope(1, 0), limits())
        .await
        .unwrap();
    prepare(&queue).await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    faulty.lose_commit_response();
    assert_eq!(
        queue.begin_external(&delivery.receipt, id(77)).await,
        Err(SubscriptionError::State(StateError::UnknownOutcome))
    );
    // The caller cannot send after the failure, even though the intent committed.
    drop(queue);
    fixture.clock.set(200);
    let reopened = fixture.queue().await;
    assert_eq!(reopened.unknown_operations().await.unwrap().len(), 1);
    assert!(
        reopened
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn slow_claim_commit_never_returns_an_expired_execution_permit() {
    let fixture = Fixture::new();
    let faulty = Arc::new(FaultStore::new(
        fixture.store().await,
        fixture.clock.clone(),
    ));
    let queue = open_queue(faulty.clone(), fixture.clock.clone(), scope(1, 0), limits())
        .await
        .unwrap();
    prepare(&queue).await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    faulty.advance_after_commit(200);
    assert!(matches!(
        queue.claim(owner(1, 1), schema(), &[1]).await,
        Err(SubscriptionError::StaleDelivery)
    ));
    drop(queue);
    let reopened = fixture.queue().await;
    let retried = claim(&reopened, owner(1, 1)).await;
    assert_eq!(retried.receipt.attempt(), 2);
    assert_eq!(retried.receipt.expires_at(), Timestamp(300));
}

#[tokio::test]
async fn slow_external_intent_commit_preserves_unknown_and_never_allows_send() {
    let fixture = Fixture::new();
    let faulty = Arc::new(FaultStore::new(
        fixture.store().await,
        fixture.clock.clone(),
    ));
    let queue = open_queue(faulty.clone(), fixture.clock.clone(), scope(1, 0), limits())
        .await
        .unwrap();
    prepare(&queue).await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    faulty.advance_after_commit(200);
    assert_eq!(
        queue.begin_external(&delivery.receipt, id(77)).await,
        Err(SubscriptionError::StaleDelivery)
    );
    assert_eq!(queue.unknown_operations().await.unwrap().len(), 1);
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn repeated_external_intent_is_not_a_second_send_permit() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    queue
        .begin_external(&delivery.receipt, id(77))
        .await
        .unwrap();
    assert_eq!(
        queue.begin_external(&delivery.receipt, id(77)).await,
        Err(SubscriptionError::ExternalOutcomeUnknown)
    );
    assert_eq!(queue.unknown_operations().await.unwrap().len(), 1);
}

#[tokio::test]
async fn store_read_timeout_is_failure_and_write_timeout_preserves_unknown_outcome() {
    let fixture = Fixture::new();
    let faulty = Arc::new(FaultStore::new(
        fixture.store().await,
        fixture.clock.clone(),
    ));
    let queue = open_queue(
        faulty.clone(),
        fixture.clock.clone(),
        scope(1, 0),
        SubscriptionLimits {
            store_timeout_ms: 250,
            ..limits()
        },
    )
    .await
    .unwrap();
    prepare(&queue).await;
    faulty.next_get.store(1, Ordering::SeqCst);
    assert!(matches!(
        queue.registrations(schema()).await,
        Err(SubscriptionError::State(StateError::BackendUnavailable))
    ));
    faulty.next_put.store(4, Ordering::SeqCst);
    assert_eq!(
        queue.enqueue(owner(1, 1), event(11)).await,
        Err(SubscriptionError::State(StateError::UnknownOutcome))
    );
    assert!(
        queue
            .claim(owner(1, 1), schema(), &[1])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn torn_ack_snapshot_restores_previous_claim_and_checkpoint_together() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    let before_ack = std::fs::metadata(&fixture.path).unwrap().len() as usize;
    queue
        .ack(&delivery.receipt, b"after-ack".to_vec())
        .await
        .unwrap();
    drop(queue);
    let complete = std::fs::read(&fixture.path).unwrap();
    let cut = before_ack + (complete.len() - before_ack) / 2;
    // Fixture-only simulation of a crash in the last file snapshot write.
    std::fs::write(&fixture.path, &complete[..cut]).unwrap();
    fixture.clock.set(200);
    let reopened = fixture.queue().await;
    assert_eq!(
        reopened.registrations(schema()).await.unwrap()[0].checkpoint,
        b"initial"
    );
    let retried = claim(&reopened, owner(1, 1)).await;
    assert_eq!(retried.receipt.attempt(), 2);
    reopened
        .ack(&retried.receipt, b"recovered".to_vec())
        .await
        .unwrap();
    drop(reopened);
    assert_eq!(
        fixture.queue().await.registrations(schema()).await.unwrap()[0].checkpoint,
        b"recovered"
    );
}

#[tokio::test]
async fn checksum_and_queue_envelope_corruption_are_rejected_without_empty_success() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    drop(queue);
    let original = std::fs::read(&fixture.path).unwrap();
    let mut corrupt = original.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    std::fs::write(&fixture.path, corrupt).unwrap();
    assert!(matches!(
        FileStateStore::open(
            &fixture.path,
            store_limits(),
            4 * 1024 * 1024,
            Arc::new(fixture.clock.clone())
        )
        .await,
        Err(StateError::InvalidState)
    ));
    std::fs::write(&fixture.path, original).unwrap();
    let store = fixture.store().await;
    let mut record = store
        .get(&root_key(scope(1, 0)), schema())
        .await
        .unwrap()
        .unwrap();
    // The file checksum is valid, but the queue schema is not.
    record.value.data = b"not-a-queue-envelope".to_vec();
    store
        .put(
            root_key(scope(1, 0)),
            record.value,
            PutCondition::IfRevision(record.revision),
        )
        .await
        .unwrap();
    assert!(matches!(
        open_queue(store, fixture.clock.clone(), scope(1, 0), limits()).await,
        Err(SubscriptionError::InvalidRecord)
    ));
}

#[tokio::test]
async fn valid_file_record_copied_into_another_bot_is_rejected_by_envelope_identity() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let store = fixture.store().await;
    let record = store
        .get(&root_key(scope(1, 0)), schema())
        .await
        .unwrap()
        .unwrap();
    store
        .put(root_key(scope(2, 0)), record.value, PutCondition::IfAbsent)
        .await
        .unwrap();
    assert!(matches!(
        open_queue(store, fixture.clock.clone(), scope(2, 0), limits()).await,
        Err(SubscriptionError::InvalidRecord)
    ));
}

#[tokio::test]
async fn diagnostics_do_not_expose_payload_or_checkpoint() {
    let fixture = Fixture::new();
    let queue = fixture.prepared().await;
    let marker = b"synthetic-private-marker";
    let mut input = event(11);
    input.payload = marker.to_vec();
    assert!(!format!("{input:?}").contains("synthetic-private-marker"));
    assert!(!format!("{input:?}").contains("115, 121, 110"));
    queue.enqueue(owner(1, 1), input).await.unwrap();
    let delivery = claim(&queue, owner(1, 1)).await;
    let debug = format!("{delivery:?}");
    assert!(!debug.contains("synthetic-private-marker"));
    assert!(!debug.contains("115, 121, 110"));
}

#[tokio::test]
async fn abrupt_process_exit_after_claim_leaves_work_for_restart() {
    let fixture = Fixture::new();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "subscription_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env("SUBSCRIPTION_CRASH_TEST_PATH", &fixture.path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(91));
    fixture.clock.set(200);
    let reopened = fixture.queue().await;
    reopened
        .activate(owner(2, 1), Timestamp(10_000))
        .await
        .unwrap();
    let delivery = claim(&reopened, owner(2, 1)).await;
    assert_eq!(delivery.receipt.attempt(), 2);
    assert_eq!(delivery.event_id, id(11));
    assert_eq!(delivery.subscription.checkpoint, b"initial");
    reopened.ack(&delivery.receipt, vec![]).await.unwrap();
}

#[tokio::test]
#[ignore = "helper invoked by abrupt_process_exit_after_claim_leaves_work_for_restart"]
async fn subscription_crash_child() {
    let path = PathBuf::from(std::env::var_os("SUBSCRIPTION_CRASH_TEST_PATH").unwrap());
    let fixture = Fixture {
        path,
        clock: Clock::new(),
    };
    let queue = fixture.prepared().await;
    queue.enqueue(owner(1, 1), event(11)).await.unwrap();
    let _delivery = claim(&queue, owner(1, 1)).await;
    // No queue/fixture destructors run: the parent reopens the persisted claim.
    std::process::exit(91);
}
