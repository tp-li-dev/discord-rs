//! Local adapter integration: the shared backend is memory, not process persistence.
use bot_framework::{
    BotResult, component,
    interactions::{
        InteractionContext, InteractionError, InteractionKind, InteractionStateStore, Registry,
        Route,
    },
    modal,
};
use serde::{Deserialize, Serialize};
use src::{protocol::BotId, state_store::*};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

// This key is a synthetic fixture. No environment variables or credentials are read.
const FIXTURE_KEY: [u8; 32] = [0x31; 32];

#[derive(Clone, Default)]
struct Clock(Arc<AtomicU64>);

impl Clock {
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl StateClock for Clock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(self.0.load(Ordering::SeqCst)))
    }
}

fn scope(bot: u64, shard_id: u32) -> StoreScope {
    StoreScope {
        bot_id: BotId(bot),
        shard_id,
    }
}

fn label(value: &str) -> PublicLabel {
    PublicLabel::new(value).unwrap()
}

fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).unwrap()
}

fn route(name: &str) -> Route {
    Route {
        namespace: "app".into(),
        name: name.into(),
        version: 1,
    }
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_entries: 32,
        max_bytes: 64 * 1024,
        max_value_bytes: 4096,
    }
}

fn memory(clock: &Clock) -> Arc<MemoryStateStore> {
    Arc::new(MemoryStateStore::new(limits(), Arc::new(clock.clone())))
}

fn adapter(store: Arc<dyn StateStore>, scope: StoreScope, namespace: &str) -> FrameworkStateStore {
    FrameworkStateStore::new(
        store,
        scope,
        label(namespace),
        FIXTURE_KEY,
        vec![1, 2],
        schema(),
    )
    .unwrap()
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Counter {
    count: u32,
}

fn payload(count: u32) -> Vec<u8> {
    serde_json::to_vec(&Counter { count }).unwrap()
}

#[component(namespace = "app", name = "counter", version = 1)]
async fn button(ctx: InteractionContext<'_>) -> BotResult<()> {
    assert_eq!(ctx.kind(), InteractionKind::Component);
    assert_eq!(ctx.route(), &route("counter"));
    let initial = ctx.state::<Counter>()?;
    let loaded = ctx.load_state::<Counter>().await?;
    assert_eq!(initial, loaded.data);
    let next = Counter {
        count: loaded.data.count + 1,
    };
    let revision = ctx.save_state(&next, loaded.revision, 200).await?;
    let stored = ctx.load_state::<Counter>().await?;
    assert_eq!(stored.data, next);
    assert_eq!(stored.revision, revision);
    assert_eq!(stored.expires_at_unix_ms, 200);
    ctx.reply(format!("button:{}", stored.data.count)).await
}

#[component(namespace = "app", name = "pick", version = 1)]
async fn select(ctx: InteractionContext<'_>) -> BotResult<()> {
    let stored = ctx.load_state::<Counter>().await?;
    ctx.reply(format!(
        "select:{}:{}",
        stored.data.count,
        ctx.values().join(",")
    ))
    .await
}

#[component(namespace = "app", name = "start", version = 1)]
async fn open_modal(ctx: InteractionContext<'_>) -> BotResult<()> {
    let created = ctx
        .create_state(
            InteractionKind::Modal,
            "form",
            1,
            &Counter { count: 17 },
            200,
        )
        .await?;
    assert!(created.revision > 0);
    assert_eq!(created.expires_at_unix_ms, 200);
    // Local transport fixture captures the ID for the subsequent modal event.
    ctx.reply(created.custom_id).await
}

#[modal(namespace = "app", name = "form", version = 1)]
async fn form(ctx: InteractionContext<'_>) -> BotResult<()> {
    assert_eq!(ctx.kind(), InteractionKind::Modal);
    let stored = ctx.load_state::<Counter>().await?;
    assert!(ctx.delete_state(stored.revision).await?);
    ctx.reply(format!(
        "modal:{}:{}",
        stored.data.count,
        ctx.values().join(",")
    ))
    .await
}

#[component(namespace = "app", name = "view", version = 1)]
async fn view(ctx: InteractionContext<'_>) -> BotResult<()> {
    ctx.reply(format!("view:{}", ctx.state::<Counter>()?.count))
        .await
}

#[modal(namespace = "app", name = "view", version = 1)]
async fn modal_view(ctx: InteractionContext<'_>) -> BotResult<()> {
    ctx.reply(format!("modal-view:{}", ctx.state::<Counter>()?.count))
        .await
}

#[tokio::test]
async fn context_mutates_state_and_routes_buttons_selects_and_modals_across_registries() {
    let clock = Clock::default();
    let backend = memory(&clock);
    assert_eq!(backend.durability(), Durability::Volatile);
    let old_store = adapter(backend.clone(), scope(1, 0), "app");
    let old_registry = Registry::new().unwrap();
    let replies = Mutex::new(Vec::new());
    let button_id = old_store
        .create_with_id(
            InteractionKind::Component,
            &route("counter"),
            StateId::from_bytes([1; 16]),
            payload(4),
            100,
        )
        .await
        .unwrap();
    old_registry
        .dispatch(
            &old_store,
            InteractionKind::Component,
            &button_id.custom_id,
            &[],
            &replies,
        )
        .await
        .unwrap();
    drop(old_registry);
    drop(old_store);

    // Independent registry/adapter, but deliberately the same in-process backend/key.
    let new_store = adapter(backend.clone(), scope(1, 0), "app");
    let new_registry = Registry::new().unwrap();
    clock.set(100);
    new_registry
        .dispatch(
            &new_store,
            InteractionKind::Component,
            &button_id.custom_id,
            &[],
            &replies,
        )
        .await
        .unwrap();
    let select_id = new_store
        .create_with_id(
            InteractionKind::Component,
            &route("pick"),
            StateId::from_bytes([2; 16]),
            payload(8),
            200,
        )
        .await
        .unwrap();
    new_registry
        .dispatch(
            &new_store,
            InteractionKind::Component,
            &select_id.custom_id,
            &["red".into(), "blue".into()],
            &replies,
        )
        .await
        .unwrap();
    let start = new_store
        .create(InteractionKind::Component, &route("start"), payload(0), 200)
        .await
        .unwrap();
    new_registry
        .dispatch(
            &new_store,
            InteractionKind::Component,
            &start.custom_id,
            &[],
            &replies,
        )
        .await
        .unwrap();
    let modal_id = replies.lock().unwrap().pop().unwrap();
    assert!(modal_id.len() <= CUSTOM_ID_LIMIT);
    new_registry
        .dispatch(
            &new_store,
            InteractionKind::Modal,
            &modal_id,
            &["answer".into()],
            &replies,
        )
        .await
        .unwrap();
    assert_eq!(
        *replies.lock().unwrap(),
        [
            "button:5",
            "button:6",
            "select:8:red,blue",
            "modal:17:answer"
        ]
    );
    assert_eq!(
        new_store
            .load(InteractionKind::Modal, &modal_id)
            .await
            .err(),
        Some(InteractionError::ExpiredOrMissing)
    );
}

struct TrackedStore {
    inner: Arc<MemoryStateStore>,
    reads: AtomicUsize,
    writes: AtomicUsize,
    fail_writes: AtomicBool,
}

impl TrackedStore {
    fn new(inner: Arc<MemoryStateStore>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            fail_writes: AtomicBool::new(false),
        }
    }
}

impl StateStore for TrackedStore {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, reader)
    }

    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(StateError::UnknownOutcome);
            }
            self.inner.put(key, value, condition).await
        })
    }

    fn delete<'a>(&'a self, key: &'a StateKey, revision: Option<u64>) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(StateError::UnknownOutcome);
            }
            self.inner.delete(key, revision).await
        })
    }
}

#[tokio::test]
async fn rejects_tampering_foreign_namespace_bot_shard_and_kind_before_storage_or_handler() {
    let clock = Clock::default();
    let backend = Arc::new(TrackedStore::new(memory(&clock)));
    let target = adapter(backend.clone(), scope(1, 0), "app");
    let registry = Registry::new().unwrap();
    let replies = Mutex::new(Vec::new());
    let valid = target
        .create_with_id(
            InteractionKind::Component,
            &route("view"),
            StateId::from_bytes([1; 16]),
            payload(1),
            100,
        )
        .await
        .unwrap();
    let mut tampered = valid.custom_id.clone().into_bytes();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    let tampered = String::from_utf8(tampered).unwrap();
    let mut rejected = vec![
        (InteractionKind::Component, tampered),
        (InteractionKind::Component, "malformed".into()),
        (InteractionKind::Modal, valid.custom_id.clone()),
    ];
    for (other_scope, namespace) in [
        (scope(2, 0), "app"),
        (scope(1, 1), "app"),
        (scope(1, 0), "other"),
    ] {
        let other = adapter(backend.clone(), other_scope, namespace);
        let mut other_route = route("view");
        other_route.namespace = namespace.into();
        let signed = other
            .create_with_id(
                InteractionKind::Component,
                &other_route,
                StateId::from_bytes([1; 16]),
                payload(2),
                100,
            )
            .await
            .unwrap();
        // Check that the rejected wire ID really has a valid MAC in its own scope.
        assert!(
            other
                .load(InteractionKind::Component, &signed.custom_id)
                .await
                .is_ok()
        );
        rejected.push((InteractionKind::Component, signed.custom_id));
    }
    let reads_before = backend.reads.load(Ordering::SeqCst);
    let writes_before = backend.writes.load(Ordering::SeqCst);
    for (kind, id) in &rejected {
        assert_eq!(
            registry.dispatch(&target, *kind, id, &[], &replies).await,
            Err(InteractionError::InvalidId)
        );
        assert_eq!(
            target.load(*kind, id).await.err(),
            Some(InteractionError::InvalidId)
        );
        assert_eq!(
            target
                .compare_and_swap(*kind, id, valid.revision, payload(9), 200)
                .await,
            Err(InteractionError::InvalidId)
        );
        assert_eq!(
            target.delete(*kind, id, valid.revision).await,
            Err(InteractionError::InvalidId)
        );
    }
    assert!(replies.lock().unwrap().is_empty());
    assert_eq!(backend.reads.load(Ordering::SeqCst), reads_before);
    assert_eq!(backend.writes.load(Ordering::SeqCst), writes_before);
    assert_eq!(backend.inner.usage().await.unwrap().entries, 4);
    registry
        .dispatch(
            &target,
            InteractionKind::Component,
            &valid.custom_id,
            &[],
            &replies,
        )
        .await
        .unwrap();
    assert_eq!(*replies.lock().unwrap(), ["view:1"]);
}

#[tokio::test]
async fn deadline_is_exclusive_for_components_and_modals_and_cas_cannot_resurrect() {
    for kind in [InteractionKind::Component, InteractionKind::Modal] {
        let clock = Clock::default();
        let backend = memory(&clock);
        let store = adapter(backend.clone(), scope(1, 0), "app");
        let created = store
            .create(kind, &route("view"), payload(3), 100)
            .await
            .unwrap();
        let registry = Registry::new().unwrap();
        let replies = Mutex::new(Vec::new());
        clock.set(99);
        registry
            .dispatch(&store, kind, &created.custom_id, &[], &replies)
            .await
            .unwrap();
        assert_eq!(replies.lock().unwrap().len(), 1);
        clock.set(100);
        assert_eq!(
            registry
                .dispatch(&store, kind, &created.custom_id, &[], &replies)
                .await,
            Err(InteractionError::ExpiredOrMissing)
        );
        assert_eq!(
            store.load(kind, &created.custom_id).await.err(),
            Some(InteractionError::ExpiredOrMissing)
        );
        let result = store
            .compare_and_swap(kind, &created.custom_id, created.revision, payload(4), 200)
            .await;
        assert!(matches!(
            result,
            Err(InteractionError::Conflict | InteractionError::ExpiredOrMissing)
        ));
        assert_eq!(backend.usage().await.unwrap().entries, 0);
        assert_eq!(replies.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn concurrent_cas_has_one_winner_and_rejections_preserve_revision_payload_and_ttl() {
    let clock = Clock::default();
    let backend = memory(&clock);
    let store = adapter(backend, scope(1, 0), "app");
    let initial = store
        .create(InteractionKind::Component, &route("view"), payload(1), 100)
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        store.compare_and_swap(
            InteractionKind::Component,
            &initial.custom_id,
            initial.revision,
            payload(2),
            200
        ),
        store.compare_and_swap(
            InteractionKind::Component,
            &initial.custom_id,
            initial.revision,
            payload(3),
            300
        )
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        (&left, &right),
        (Ok(_), Err(InteractionError::Conflict)) | (Err(InteractionError::Conflict), Ok(_))
    ));
    let after = store
        .load(InteractionKind::Component, &initial.custom_id)
        .await
        .unwrap();
    assert_eq!(after.revision, left.or(right).unwrap());
    for (expected, expiry, error) in [
        (initial.revision, 400, InteractionError::Conflict),
        (after.revision, 0, InteractionError::InvalidExpiry),
    ] {
        assert_eq!(
            store
                .compare_and_swap(
                    InteractionKind::Component,
                    &initial.custom_id,
                    expected,
                    payload(99),
                    expiry
                )
                .await,
            Err(error)
        );
    }
    let unchanged = store
        .load(InteractionKind::Component, &initial.custom_id)
        .await
        .unwrap();
    assert_eq!(unchanged.revision, after.revision);
    assert_eq!(unchanged.data, after.data);
    assert_eq!(unchanged.expires_at_unix_ms, after.expires_at_unix_ms);
    assert_eq!(
        store
            .delete(
                InteractionKind::Component,
                &initial.custom_id,
                initial.revision
            )
            .await,
        Err(InteractionError::Conflict)
    );
    assert!(
        store
            .delete(
                InteractionKind::Component,
                &initial.custom_id,
                after.revision
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn old_route_version_cannot_update_or_delete_a_replaced_envelope_even_with_current_revision()
{
    let clock = Clock::default();
    let backend = memory(&clock);
    let store = adapter(backend.clone(), scope(1, 0), "app");
    let id = StateId::from_bytes([9; 16]);
    let old = store
        .create_with_id(
            InteractionKind::Component,
            &route("view"),
            id,
            payload(1),
            100,
        )
        .await
        .unwrap();
    let low_level = InteractionStore::new(
        backend,
        CustomIdCodec::new(scope(1, 0), StateKind::View, FIXTURE_KEY, vec![1, 2]).unwrap(),
        schema(),
    )
    .unwrap();
    let replacement = CustomId::new(label("app"), label("view"), 2, id).unwrap();
    let replacement_id = low_level
        .save(&replacement, payload(2), Timestamp(200), PutCondition::Any)
        .await
        .unwrap();
    let current = store
        .load(InteractionKind::Component, &replacement_id)
        .await
        .unwrap();
    assert_eq!(
        store
            .load(InteractionKind::Component, &old.custom_id)
            .await
            .err(),
        Some(InteractionError::InvalidState)
    );
    assert_eq!(
        store
            .compare_and_swap(
                InteractionKind::Component,
                &old.custom_id,
                current.revision,
                payload(99),
                300
            )
            .await,
        Err(InteractionError::InvalidState)
    );
    assert_eq!(
        store
            .delete(InteractionKind::Component, &old.custom_id, current.revision)
            .await,
        Err(InteractionError::InvalidState)
    );
    let unchanged = store
        .load(InteractionKind::Component, &replacement_id)
        .await
        .unwrap();
    assert_eq!(unchanged.revision, current.revision);
    assert_eq!(unchanged.data, payload(2));
}

#[tokio::test]
async fn create_rejects_wrong_namespace_expiry_capacity_and_oversized_id_without_partial_writes() {
    let clock = Clock::default();
    let backend = Arc::new(MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            ..limits()
        },
        Arc::new(clock),
    ));
    let tracked = Arc::new(TrackedStore::new(backend.clone()));
    let store = adapter(tracked.clone(), scope(1, 0), "app");
    let mut other_route = route("view");
    other_route.namespace = "other".into();
    assert_eq!(
        store
            .create(InteractionKind::Component, &other_route, payload(1), 100)
            .await
            .err(),
        Some(InteractionError::InvalidId)
    );
    assert_eq!(tracked.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .create(InteractionKind::Component, &route("view"), payload(1), 0)
            .await
            .err(),
        Some(InteractionError::InvalidExpiry)
    );
    assert_eq!(backend.usage().await.unwrap().entries, 0);
    let long_namespace = "a".repeat(LABEL_LIMIT);
    let long = adapter(tracked.clone(), scope(1, 0), &long_namespace);
    let long_route = Route {
        namespace: long_namespace,
        name: "b".repeat(LABEL_LIMIT),
        version: 1,
    };
    let writes_before = tracked.writes.load(Ordering::SeqCst);
    assert_eq!(
        long.create(InteractionKind::Component, &long_route, payload(1), 100)
            .await
            .err(),
        Some(InteractionError::InvalidId)
    );
    assert_eq!(tracked.writes.load(Ordering::SeqCst), writes_before);
    let live = store
        .create(InteractionKind::Component, &route("view"), payload(1), 100)
        .await
        .unwrap();
    assert_eq!(
        store
            .create(InteractionKind::Modal, &route("view"), payload(2), 100)
            .await
            .err(),
        Some(InteractionError::CapacityExceeded)
    );
    let retained = store
        .load(InteractionKind::Component, &live.custom_id)
        .await
        .unwrap();
    assert_eq!(retained.revision, live.revision);
    assert_eq!(retained.data, payload(1));
    assert_eq!(backend.usage().await.unwrap().entries, 1);
}

#[tokio::test]
async fn unknown_write_outcomes_stay_distinct_and_are_not_automatically_retried() {
    let clock = Clock::default();
    let backend = Arc::new(TrackedStore::new(memory(&clock)));
    let store = adapter(backend.clone(), scope(1, 0), "app");
    let live = store
        .create(InteractionKind::Component, &route("view"), payload(1), 100)
        .await
        .unwrap();
    backend.fail_writes.store(true, Ordering::SeqCst);
    let before = backend.writes.load(Ordering::SeqCst);
    assert_eq!(
        store
            .create(InteractionKind::Modal, &route("view"), payload(2), 100)
            .await
            .err(),
        Some(InteractionError::UnknownOutcome)
    );
    assert_eq!(
        store
            .compare_and_swap(
                InteractionKind::Component,
                &live.custom_id,
                live.revision,
                payload(2),
                200
            )
            .await,
        Err(InteractionError::UnknownOutcome)
    );
    assert_eq!(
        store
            .delete(InteractionKind::Component, &live.custom_id, live.revision)
            .await,
        Err(InteractionError::UnknownOutcome)
    );
    assert_eq!(backend.writes.load(Ordering::SeqCst) - before, 3);
    assert_ne!(
        InteractionError::UnknownOutcome,
        InteractionError::StoreUnavailable
    );
}

#[tokio::test]
async fn adapter_debug_and_errors_do_not_expose_wire_ids_or_payloads() {
    let clock = Clock::default();
    let store = adapter(memory(&clock), scope(1, 0), "app");
    let synthetic_payload = b"synthetic-private-content".to_vec();
    let created = store
        .create(
            InteractionKind::Component,
            &route("view"),
            synthetic_payload.clone(),
            100,
        )
        .await
        .unwrap();
    let stored = store
        .load(InteractionKind::Component, &created.custom_id)
        .await
        .unwrap();
    let rendered = format!("{created:?} {stored:?}");
    assert!(!rendered.contains(&created.custom_id));
    assert!(!rendered.contains("synthetic-private-content"));
    assert!(!rendered.contains(&format!("{synthetic_payload:?}")));
    let error = store
        .load(InteractionKind::Modal, &created.custom_id)
        .await
        .err()
        .unwrap();
    assert!(!format!("{error} {error:?}").contains(&created.custom_id));
}

enum StorageRace {
    Replace(StateValue),
    ExpireAt(u64),
}

/// Simulates a different writer/clock advancing after the adapter's read but
/// immediately before the backend's atomic conditional mutation. No sleeps or
/// executor scheduling are needed to place the race at this exact boundary.
struct RacingStore {
    inner: Arc<MemoryStateStore>,
    clock: Clock,
    race: Mutex<Option<StorageRace>>,
    reads: AtomicUsize,
    reads_at_arm: AtomicUsize,
    races_applied: AtomicUsize,
}

impl RacingStore {
    fn new(clock: Clock) -> Self {
        Self {
            inner: memory(&clock),
            clock,
            race: Mutex::new(None),
            reads: AtomicUsize::new(0),
            reads_at_arm: AtomicUsize::new(0),
            races_applied: AtomicUsize::new(0),
        }
    }

    fn arm(&self, race: StorageRace) {
        self.reads_at_arm
            .store(self.reads.load(Ordering::SeqCst), Ordering::SeqCst);
        assert!(self.race.lock().unwrap().replace(race).is_none());
    }

    async fn apply_race(&self, key: &StateKey) -> Result<(), StateError> {
        let race = self.race.lock().unwrap().take();
        if let Some(race) = race {
            // The operation must first have validated the existing envelope
            // before it reaches the conditional write/delete.
            assert!(self.reads.load(Ordering::SeqCst) > self.reads_at_arm.load(Ordering::SeqCst));
            match race {
                StorageRace::Replace(value) => {
                    self.inner
                        .put(key.clone(), value, PutCondition::Any)
                        .await?;
                }
                StorageRace::ExpireAt(now) => self.clock.set(now),
            }
            self.races_applied.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl StateStore for RacingStore {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, reader)
    }

    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            self.apply_race(&key).await?;
            self.inner.put(key, value, condition).await
        })
    }

    fn delete<'a>(&'a self, key: &'a StateKey, revision: Option<u64>) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            self.apply_race(key).await?;
            self.inner.delete(key, revision).await
        })
    }
}

fn envelope(route: &CustomId, data: Vec<u8>, expires_at: Option<Timestamp>) -> StateValue {
    StateValue {
        schema: schema(),
        expires_at,
        data: serde_json::to_vec(&serde_json::json!({ "route": route, "data": data })).unwrap(),
    }
}

#[tokio::test]
async fn replacement_between_validated_read_and_cas_or_delete_is_preserved() {
    for deleting in [false, true] {
        let backend = Arc::new(RacingStore::new(Clock::default()));
        let store = adapter(backend.clone(), scope(1, 0), "app");
        let state_id = StateId::from_bytes([42; 16]);
        let old = store
            .create_with_id(
                InteractionKind::Component,
                &route("view"),
                state_id,
                payload(1),
                100,
            )
            .await
            .unwrap();
        let replacement = CustomId::new(label("app"), label("view"), 2, state_id).unwrap();
        let replacement_id =
            CustomIdCodec::new(scope(1, 0), StateKind::View, FIXTURE_KEY, vec![1, 2])
                .unwrap()
                .encode(&replacement)
                .unwrap();
        backend.arm(StorageRace::Replace(envelope(
            &replacement,
            payload(2),
            Some(Timestamp(300)),
        )));
        let result = if deleting {
            store
                .delete(InteractionKind::Component, &old.custom_id, old.revision)
                .await
                .map(|_| ())
        } else {
            store
                .compare_and_swap(
                    InteractionKind::Component,
                    &old.custom_id,
                    old.revision,
                    payload(99),
                    200,
                )
                .await
                .map(|_| ())
        };
        assert_eq!(result, Err(InteractionError::Conflict));
        assert_eq!(backend.races_applied.load(Ordering::SeqCst), 1);
        let retained = store
            .load(InteractionKind::Component, &replacement_id)
            .await
            .unwrap();
        assert_eq!(retained.route.version, 2);
        assert_eq!(retained.data, payload(2));
        assert_eq!(retained.expires_at_unix_ms, 300);
        assert!(retained.revision > old.revision);
        assert_eq!(backend.inner.usage().await.unwrap().entries, 1);
    }
}

#[tokio::test]
async fn expiry_between_validated_read_and_cas_or_delete_cannot_revive_state() {
    for deleting in [false, true] {
        let backend = Arc::new(RacingStore::new(Clock::default()));
        let store = adapter(backend.clone(), scope(1, 0), "app");
        let old = store
            .create(InteractionKind::Modal, &route("view"), payload(1), 100)
            .await
            .unwrap();
        backend.arm(StorageRace::ExpireAt(100));
        let result = if deleting {
            store
                .delete(InteractionKind::Modal, &old.custom_id, old.revision)
                .await
                .map(|_| ())
        } else {
            store
                .compare_and_swap(
                    InteractionKind::Modal,
                    &old.custom_id,
                    old.revision,
                    payload(99),
                    200,
                )
                .await
                .map(|_| ())
        };
        assert_eq!(result, Err(InteractionError::Conflict));
        assert_eq!(backend.races_applied.load(Ordering::SeqCst), 1);
        assert_eq!(backend.inner.usage().await.unwrap().entries, 0);
        assert_eq!(
            store
                .load(InteractionKind::Modal, &old.custom_id)
                .await
                .err(),
            Some(InteractionError::ExpiredOrMissing)
        );
    }
}

#[tokio::test]
async fn schema_and_envelope_failures_reject_restore_cas_and_delete_without_mutating() {
    let clock = Clock::default();
    let backend = memory(&clock);
    let store = adapter(backend.clone(), scope(1, 0), "app");
    let state_id = StateId::from_bytes([55; 16]);
    let authentic = CustomId::new(label("app"), label("view"), 1, state_id).unwrap();
    let signed_id = CustomIdCodec::new(scope(1, 0), StateKind::View, FIXTURE_KEY, vec![1, 2])
        .unwrap()
        .encode(&authentic)
        .unwrap();
    let key = StateKey::new(
        scope(1, 0),
        label("app"),
        label("view"),
        StateKind::View,
        state_id,
    );
    let valid = envelope(&authentic, payload(1), Some(Timestamp(100)));
    let mut incompatible = valid.clone();
    incompatible.schema = SchemaVersion::new(2, 2).unwrap();
    let mut malformed = valid.clone();
    malformed.data = b"{".to_vec();
    let wrong_route = CustomId::new(label("app"), label("view"), 2, state_id).unwrap();
    for invalid in [
        incompatible,
        malformed,
        envelope(&wrong_route, payload(2), Some(Timestamp(100))),
        envelope(&authentic, payload(1), None),
    ] {
        let revision = backend
            .put(key.clone(), invalid.clone(), PutCondition::Any)
            .await
            .unwrap();
        assert_eq!(
            store
                .restore(InteractionKind::Component, &signed_id)
                .await
                .err(),
            Some(InteractionError::InvalidState)
        );
        assert_eq!(
            store
                .load(InteractionKind::Component, &signed_id)
                .await
                .err(),
            Some(InteractionError::InvalidState)
        );
        assert_eq!(
            store
                .compare_and_swap(
                    InteractionKind::Component,
                    &signed_id,
                    revision,
                    payload(99),
                    200,
                )
                .await,
            Err(InteractionError::InvalidState)
        );
        assert_eq!(
            store
                .delete(InteractionKind::Component, &signed_id, revision)
                .await,
            Err(InteractionError::InvalidState)
        );
        let retained = backend
            .get(&key, SchemaVersion::new(2, 1).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.revision, revision);
        assert_eq!(retained.value, invalid);
    }
}
