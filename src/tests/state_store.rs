use src::{protocol::BotId, state_store::*};
use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Default)]
struct FakeClock(Arc<Mutex<u64>>);

impl FakeClock {
    fn set(&self, now: u64) {
        *self.0.lock().unwrap() = now;
    }
}

impl StateClock for FakeClock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(*self.0.lock().unwrap()))
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

fn schema(version: u16, compatible_since: u16) -> SchemaVersion {
    SchemaVersion::new(version, compatible_since).unwrap()
}

fn route(id: u8) -> CustomId {
    CustomId::new(
        label("example"),
        label("confirm"),
        1,
        StateId::from_bytes([id; 16]),
    )
    .unwrap()
}

fn key(id: u8) -> StateKey {
    StateKey::new(
        scope(1, 0),
        label("example"),
        label("confirm"),
        StateKind::Application,
        StateId::from_bytes([id; 16]),
    )
}

fn value(data: &[u8], expires_at: Option<u64>) -> StateValue {
    StateValue {
        schema: schema(1, 1),
        expires_at: expires_at.map(Timestamp),
        data: data.to_vec(),
    }
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_entries: 64,
        max_bytes: 64 * 1024,
        max_value_bytes: 4096,
    }
}

fn memory(clock: &FakeClock) -> MemoryStateStore {
    MemoryStateStore::new(limits(), Arc::new(clock.clone()))
}

fn codec(scope: StoreScope, kind: StateKind) -> CustomIdCodec {
    // Synthetic fixture key; no production credential is loaded by these tests.
    CustomIdCodec::new(scope, kind, [0x42; 32], vec![1, 2]).unwrap()
}

#[tokio::test]
async fn object_safe_store_supports_get_put_delete_and_shared_handles() {
    let clock = FakeClock::default();
    let memory = memory(&clock);
    let second = memory.clone();
    let store: Arc<dyn StateStore> = Arc::new(memory);
    assert_eq!(store.durability(), Durability::Volatile);
    assert!(store.get(&key(1), schema(1, 1)).await.unwrap().is_none());
    assert!(!store.delete(&key(1), None).await.unwrap());
    let revision = store
        .put(key(1), value(b"first", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    assert!(revision > 0);
    let record = second.get(&key(1), schema(1, 1)).await.unwrap().unwrap();
    assert_eq!(record.revision, revision);
    assert_eq!(record.value.data, b"first");
    assert!(second.delete(&key(1), Some(revision)).await.unwrap());
    assert!(store.get(&key(1), schema(1, 1)).await.unwrap().is_none());
    assert_eq!(second.usage().await.unwrap().entries, 0);
    assert_eq!(second.usage().await.unwrap().bytes, 0);
}

#[tokio::test]
async fn conditional_writes_and_deletes_preserve_state_on_conflict() {
    let store = memory(&FakeClock::default());
    let first = store
        .put(key(1), value(b"old", None), PutCondition::Any)
        .await
        .unwrap();
    for condition in [PutCondition::IfAbsent, PutCondition::IfRevision(first + 1)] {
        assert_eq!(
            store.put(key(1), value(b"bad", None), condition).await,
            Err(StateError::Conflict)
        );
    }
    assert_eq!(
        store.delete(&key(1), Some(first + 1)).await,
        Err(StateError::Conflict)
    );
    assert_eq!(
        store
            .get(&key(1), schema(1, 1))
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"old"
    );
    let next = store
        .put(key(1), value(b"new", None), PutCondition::IfRevision(first))
        .await
        .unwrap();
    assert!(next > first);
    assert_eq!(
        store.delete(&key(1), Some(first)).await,
        Err(StateError::Conflict)
    );
    assert!(store.delete(&key(1), Some(next)).await.unwrap());
    assert_eq!(
        store
            .put(
                key(1),
                value(b"stale", None),
                PutCondition::IfRevision(next)
            )
            .await,
        Err(StateError::Conflict)
    );
    let recreated = store
        .put(key(1), value(b"recreated", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    assert!(recreated > next);
    assert_eq!(
        store.delete(&key(1), Some(next)).await,
        Err(StateError::Conflict)
    );
}

#[tokio::test]
async fn scopes_namespaces_features_kinds_and_ids_are_isolated() {
    let store = memory(&FakeClock::default());
    let keys = [
        key(1),
        StateKey::new(
            scope(2, 0),
            label("example"),
            label("confirm"),
            StateKind::Application,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 1),
            label("example"),
            label("confirm"),
            StateKind::Application,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 0),
            label("other"),
            label("confirm"),
            StateKind::Application,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 0),
            label("example"),
            label("other"),
            StateKind::Application,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 0),
            label("example"),
            label("confirm"),
            StateKind::View,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 0),
            label("example"),
            label("confirm"),
            StateKind::Modal,
            StateId::from_bytes([1; 16]),
        ),
        StateKey::new(
            scope(1, 0),
            label("example"),
            label("confirm"),
            StateKind::Subscription,
            StateId::from_bytes([1; 16]),
        ),
        key(2),
    ];
    for (index, key) in keys.iter().enumerate() {
        store
            .put(
                key.clone(),
                value(&[index as u8], None),
                PutCondition::IfAbsent,
            )
            .await
            .unwrap();
    }
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            store
                .get(key, schema(1, 1))
                .await
                .unwrap()
                .unwrap()
                .value
                .data,
            vec![index as u8]
        );
    }
    store.delete(&keys[0], None).await.unwrap();
    assert_eq!(store.usage().await.unwrap().entries, keys.len() - 1);
    for key in &keys[1..] {
        assert!(store.get(key, schema(1, 1)).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn expiry_is_exclusive_at_deadline_and_releases_capacity() {
    let clock = FakeClock::default();
    let store = MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            ..limits()
        },
        Arc::new(clock.clone()),
    );
    store
        .put(key(1), value(b"old", Some(10)), PutCondition::Any)
        .await
        .unwrap();
    clock.set(9);
    assert!(store.get(&key(1), schema(1, 1)).await.unwrap().is_some());
    assert_eq!(
        store
            .put(key(2), value(b"other", None), PutCondition::Any)
            .await,
        Err(StateError::CapacityExceeded)
    );
    clock.set(10);
    assert!(store.get(&key(1), schema(1, 1)).await.unwrap().is_none());
    assert_eq!(store.usage().await.unwrap().bytes, 0);
    store
        .put(key(2), value(b"other", None), PutCondition::Any)
        .await
        .unwrap();
    clock.set(11);
    assert!(store.get(&key(2), schema(1, 1)).await.unwrap().is_some());
    assert!(!store.delete(&key(1), None).await.unwrap());
}

#[tokio::test]
async fn expired_entries_are_reclaimed_on_put_without_prior_get() {
    let clock = FakeClock::default();
    let store = MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            ..limits()
        },
        Arc::new(clock.clone()),
    );
    let old_revision = store
        .put(key(1), value(b"old", Some(10)), PutCondition::Any)
        .await
        .unwrap();
    clock.set(10);
    let next = store
        .put(key(1), value(b"new", Some(20)), PutCondition::IfAbsent)
        .await
        .unwrap();
    assert!(next > old_revision);
    assert_eq!(
        store
            .put(
                key(1),
                value(b"stale", None),
                PutCondition::IfRevision(old_revision)
            )
            .await,
        Err(StateError::Conflict)
    );
    clock.set(20);
    store
        .put(key(2), value(b"next", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    assert_eq!(store.usage().await.unwrap().entries, 1);
}

#[tokio::test]
async fn expiry_at_or_before_now_is_rejected_atomically() {
    let clock = FakeClock::default();
    clock.set(10);
    let store = memory(&clock);
    let first = store
        .put(key(1), value(b"old", None), PutCondition::Any)
        .await
        .unwrap();
    for expires in [0, 9, 10] {
        assert_eq!(
            store
                .put(key(1), value(b"invalid", Some(expires)), PutCondition::Any)
                .await,
            Err(StateError::InvalidExpiry)
        );
    }
    let record = store.get(&key(1), schema(1, 1)).await.unwrap().unwrap();
    assert_eq!(record.revision, first);
    assert_eq!(record.value.data, b"old");
}

#[tokio::test]
async fn clock_regression_fails_closed_and_does_not_resurrect_expired_state() {
    let clock = FakeClock::default();
    let store = memory(&clock);
    store
        .put(key(1), value(b"old", Some(10)), PutCondition::Any)
        .await
        .unwrap();
    clock.set(10);
    assert!(store.get(&key(1), schema(1, 1)).await.unwrap().is_none());
    clock.set(9);
    assert_eq!(
        store.get(&key(1), schema(1, 1)).await.err(),
        Some(StateError::ClockRegressed)
    );
    assert_eq!(
        store
            .put(key(2), value(b"new", None), PutCondition::Any)
            .await,
        Err(StateError::ClockRegressed)
    );
}

#[tokio::test]
async fn failing_clock_prevents_reads_and_mutations() {
    struct FailedClock;
    impl StateClock for FailedClock {
        fn now(&self) -> Result<Timestamp, StateError> {
            Err(StateError::ClockUnavailable)
        }
    }
    let store = MemoryStateStore::new(limits(), Arc::new(FailedClock));
    assert_eq!(
        store.get(&key(1), schema(1, 1)).await.err(),
        Some(StateError::ClockUnavailable)
    );
    assert_eq!(
        store
            .put(key(1), value(b"new", None), PutCondition::Any)
            .await,
        Err(StateError::ClockUnavailable)
    );
    assert_eq!(
        store.delete(&key(1), None).await,
        Err(StateError::ClockUnavailable)
    );
    assert_eq!(
        store.usage().await.err(),
        Some(StateError::ClockUnavailable)
    );
}

#[tokio::test]
async fn exact_byte_capacity_includes_keys_and_overwrite_replaces_charge() {
    let charge = key(1).charged_bytes(3).unwrap();
    let store = MemoryStateStore::new(
        StoreLimits {
            max_entries: 2,
            max_bytes: charge,
            max_value_bytes: 4,
        },
        Arc::new(FakeClock::default()),
    );
    let revision = store
        .put(key(1), value(b"abc", None), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(store.usage().await.unwrap().bytes, charge);
    assert_eq!(
        store
            .put(key(1), value(b"abcd", None), PutCondition::Any)
            .await,
        Err(StateError::CapacityExceeded)
    );
    let record = store.get(&key(1), schema(1, 1)).await.unwrap().unwrap();
    assert_eq!(record.revision, revision);
    assert_eq!(record.value.data, b"abc");
    store
        .put(key(1), value(b"a", None), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        store.usage().await.unwrap().bytes,
        key(1).charged_bytes(1).unwrap()
    );
    assert_eq!(
        store.put(key(2), value(b"", None), PutCondition::Any).await,
        Err(StateError::CapacityExceeded)
    );
    store.delete(&key(1), None).await.unwrap();
    store
        .put(key(2), value(b"abc", None), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(store.usage().await.unwrap().bytes, charge);
    assert!(key(1).charged_bytes(usize::MAX).is_none());
}

#[tokio::test]
async fn entry_and_value_size_limits_accept_boundaries_and_reject_overflow() {
    let store = MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            max_value_bytes: 3,
            ..limits()
        },
        Arc::new(FakeClock::default()),
    );
    store
        .put(key(1), value(b"abc", None), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        store
            .put(key(1), value(b"abcd", None), PutCondition::Any)
            .await,
        Err(StateError::ValueTooLarge)
    );
    assert_eq!(
        store.put(key(2), value(b"", None), PutCondition::Any).await,
        Err(StateError::CapacityExceeded)
    );
    assert_eq!(
        store
            .get(&key(1), schema(1, 1))
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"abc"
    );
    let empty = MemoryStateStore::new(
        StoreLimits {
            max_entries: 0,
            max_bytes: 0,
            max_value_bytes: 0,
        },
        Arc::new(FakeClock::default()),
    );
    assert_eq!(
        empty.put(key(1), value(b"", None), PutCondition::Any).await,
        Err(StateError::CapacityExceeded)
    );
    assert_eq!(empty.usage().await.unwrap().entries, 0);
}

#[test]
fn schema_versions_enforce_compatibility_intervals() {
    for (version, since) in [(0, 0), (1, 0), (0, 1), (1, 2)] {
        assert_eq!(
            SchemaVersion::new(version, since).err(),
            Some(StateError::InvalidVersion)
        );
    }
    assert!(schema(2, 1).can_read(schema(1, 1)));
    assert!(schema(1, 1).can_read(schema(2, 1)));
    assert!(schema(3, 2).can_read(schema(2, 1)));
    assert!(!schema(1, 1).can_read(schema(2, 2)));
    assert!(!schema(3, 3).can_read(schema(2, 1)));
    assert!(schema(u16::MAX, u16::MAX).can_read(schema(u16::MAX, u16::MAX)));
}

#[tokio::test]
async fn schema_mismatch_is_explicit_and_preserves_stored_state() {
    let store = memory(&FakeClock::default());
    let mut old = value(b"old", None);
    old.schema = schema(2, 2);
    store.put(key(1), old, PutCondition::Any).await.unwrap();
    assert_eq!(
        store.get(&key(1), schema(1, 1)).await.err(),
        Some(StateError::IncompatibleSchema)
    );
    assert_eq!(
        store
            .get(&key(1), schema(3, 2))
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"old"
    );
    assert_eq!(store.usage().await.unwrap().entries, 1);
    let mut expanded = value(b"expanded", None);
    expanded.schema = schema(3, 2);
    store
        .put(key(1), expanded, PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&key(1), schema(2, 2))
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"expanded"
    );
}

#[test]
fn public_labels_restrict_syntax_length_and_obvious_secret_markers() {
    for valid in ["a", "a_b-c9", "abcdefghijklmnopqrstuvwx"] {
        assert_eq!(label(valid).as_str(), valid);
    }
    for invalid in [
        "",
        "1abc",
        "UPPER",
        "a:b",
        "a/b",
        "a.b",
        "a b",
        "a\nb",
        "確認",
        "abcdefghijklmnopqrstuvwxy",
    ] {
        assert_eq!(
            PublicLabel::new(invalid).err(),
            Some(StateError::InvalidLabel)
        );
    }
    for sensitive in [
        "token",
        "bot_token",
        "secret",
        "password",
        "authorization",
        "bearer",
        "credential",
        "private_key",
        "api_key",
        "my_token",
    ] {
        assert_eq!(
            PublicLabel::new(sensitive).err(),
            Some(StateError::SensitiveLabel)
        );
    }
}

#[test]
fn custom_id_round_trip_and_exact_discord_length_limit() {
    let codec = codec(scope(1, 0), StateKind::View);
    let route = route(1);
    let encoded = codec.encode(&route).unwrap();
    assert!(encoded.is_ascii());
    assert!(encoded.len() <= 100);
    assert_eq!(codec.decode(&encoded).unwrap(), route);
    let boundary = CustomId::new(
        label("abcdefghijklmn"),
        label("abcdefghijklmno"),
        1,
        StateId::from_bytes([2; 16]),
    )
    .unwrap();
    let encoded = codec.encode(&boundary).unwrap();
    assert_eq!(encoded.len(), 100);
    assert_eq!(codec.decode(&encoded).unwrap(), boundary);
    let over = CustomId::new(
        label("abcdefghijklmno"),
        label("abcdefghijklmno"),
        1,
        StateId::from_bytes([2; 16]),
    )
    .unwrap();
    assert_eq!(codec.encode(&over), Err(StateError::InvalidCustomId));
    assert_eq!(
        codec.decode(&(encoded + "x")).err(),
        Some(StateError::InvalidCustomId)
    );
}

#[test]
fn custom_id_matches_an_independently_computed_hmac_sha256_fixture() {
    // Expected first 128 bits computed independently with Python's standard
    // hmac/hashlib over the documented domain + scope + kind + wire body.
    let codec = codec(scope(1, 0), StateKind::View);
    let expected =
        "1:example:confirm:1:01010101010101010101010101010101:612286cc70e51e67d9a954d93149c44b";
    assert_eq!(codec.encode(&route(1)).unwrap(), expected);
    assert_eq!(codec.decode(expected).unwrap(), route(1));
}

#[test]
fn custom_id_rejects_invalid_syntax_and_unknown_versions() {
    let codec = codec(scope(1, 0), StateKind::View);
    assert_eq!(
        CustomId::new(
            label("example"),
            label("confirm"),
            0,
            StateId::from_bytes([1; 16])
        )
        .err(),
        Some(StateError::InvalidVersion)
    );
    for invalid in [
        "",
        "1",
        "確認",
        "1:example:confirm:1:00:00",
        "1:example:confirm:1:00000000000000000000000000000000:00000000000000000000000000000000:extra",
    ] {
        assert!(codec.decode(invalid).is_err());
    }
    let encoded = codec.encode(&route(1)).unwrap();
    let mut fields: Vec<_> = encoded.split(':').map(str::to_owned).collect();
    for wire in ["0", "2", "65535"] {
        fields[0] = wire.to_owned();
        assert_eq!(
            codec.decode(&fields.join(":")).err(),
            Some(StateError::UnsupportedVersion)
        );
    }
    let unsupported = CustomId::new(
        label("example"),
        label("confirm"),
        3,
        StateId::from_bytes([1; 16]),
    )
    .unwrap();
    assert_eq!(
        codec.encode(&unsupported),
        Err(StateError::UnsupportedVersion)
    );
    let other_versions =
        CustomIdCodec::new(scope(1, 0), StateKind::View, [0x42; 32], vec![3]).unwrap();
    assert_eq!(
        codec
            .decode(&other_versions.encode(&unsupported).unwrap())
            .err(),
        Some(StateError::UnsupportedVersion)
    );
    for versions in [vec![], vec![0], vec![1, 0]] {
        assert!(CustomIdCodec::new(scope(1, 0), StateKind::View, [0x42; 32], versions).is_err());
    }
}

#[test]
fn custom_id_authenticates_every_route_field_scope_kind_and_key() {
    let codec = codec(scope(1, 0), StateKind::View);
    let encoded = codec.encode(&route(1)).unwrap();
    let fields: Vec<_> = encoded.split(':').map(str::to_owned).collect();
    for (index, replacement) in [
        (1, "other"),
        (2, "cancel"),
        (3, "2"),
        (4, "02020202020202020202020202020202"),
        (5, "00000000000000000000000000000000"),
    ] {
        let mut changed = fields.clone();
        changed[index] = replacement.to_owned();
        assert_eq!(
            codec.decode(&changed.join(":")).err(),
            Some(StateError::Tampered)
        );
    }
    for other in [
        self::codec(scope(2, 0), StateKind::View),
        self::codec(scope(1, 1), StateKind::View),
        self::codec(scope(1, 0), StateKind::Modal),
        CustomIdCodec::new(scope(1, 0), StateKind::View, [0x43; 32], vec![1]).unwrap(),
    ] {
        assert_eq!(other.decode(&encoded).err(), Some(StateError::Tampered));
    }
    // Even one changed MAC nibble must fail; avoid depending on a particular fixture tag.
    let mut changed = encoded.into_bytes();
    let last = changed.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    assert_eq!(
        codec.decode(std::str::from_utf8(&changed).unwrap()).err(),
        Some(StateError::Tampered)
    );
}

#[tokio::test]
async fn view_and_modal_states_restore_in_a_fresh_reader_until_expiry() {
    for kind in [StateKind::View, StateKind::Modal] {
        let clock = FakeClock::default();
        let store: Arc<dyn StateStore> = Arc::new(memory(&clock));
        let old =
            InteractionStore::new(store.clone(), codec(scope(1, 0), kind), schema(1, 1)).unwrap();
        let encoded = old
            .save(
                &route(1),
                b"state only in store".to_vec(),
                Timestamp(100),
                PutCondition::IfAbsent,
            )
            .await
            .unwrap();
        assert!(!encoded.contains("state only in store"));
        drop(old);
        let new =
            InteractionStore::new(store.clone(), codec(scope(1, 0), kind), schema(2, 1)).unwrap();
        clock.set(99);
        let record = new.load(&encoded).await.unwrap().unwrap();
        assert_eq!(record.value.data, b"state only in store");
        assert_eq!(record.value.expires_at, Some(Timestamp(100)));
        assert_eq!(
            new.delete(&encoded, Some(record.revision + 1)).await,
            Err(StateError::Conflict)
        );
        clock.set(100);
        assert!(new.load(&encoded).await.unwrap().is_none());
        assert!(!new.delete(&encoded, None).await.unwrap());
    }
}

#[tokio::test]
async fn interaction_load_rejects_other_kind_scope_and_incompatible_schema() {
    let store: Arc<dyn StateStore> = Arc::new(memory(&FakeClock::default()));
    let view = InteractionStore::new(
        store.clone(),
        codec(scope(1, 0), StateKind::View),
        schema(2, 2),
    )
    .unwrap();
    let encoded = view
        .save(
            &route(1),
            b"state".to_vec(),
            Timestamp(100),
            PutCondition::Any,
        )
        .await
        .unwrap();
    for (other_scope, kind) in [
        (scope(1, 0), StateKind::Modal),
        (scope(2, 0), StateKind::View),
        (scope(1, 1), StateKind::View),
    ] {
        let other =
            InteractionStore::new(store.clone(), codec(other_scope, kind), schema(2, 2)).unwrap();
        assert_eq!(other.load(&encoded).await.err(), Some(StateError::Tampered));
    }
    let old = InteractionStore::new(
        store.clone(),
        codec(scope(1, 0), StateKind::View),
        schema(1, 1),
    )
    .unwrap();
    assert_eq!(
        old.load(&encoded).await.err(),
        Some(StateError::IncompatibleSchema)
    );
    for kind in [StateKind::Application, StateKind::Subscription] {
        assert!(
            InteractionStore::new(store.clone(), codec(scope(1, 0), kind), schema(1, 1)).is_err()
        );
    }
    assert!(view.delete(&encoded, None).await.unwrap());
    assert!(view.load(&encoded).await.unwrap().is_none());
}

#[tokio::test]
async fn stale_interaction_route_cannot_read_or_delete_replaced_version() {
    let store: Arc<dyn StateStore> = Arc::new(memory(&FakeClock::default()));
    let interactions =
        InteractionStore::new(store, codec(scope(1, 0), StateKind::View), schema(1, 1)).unwrap();
    let old = route(1);
    let old_wire = interactions
        .save(&old, b"old".to_vec(), Timestamp(100), PutCondition::Any)
        .await
        .unwrap();
    let mut new = old;
    new.version = 2;
    let new_wire = interactions
        .save(&new, b"new".to_vec(), Timestamp(100), PutCondition::Any)
        .await
        .unwrap();
    let current = interactions.load(&new_wire).await.unwrap().unwrap();
    assert_eq!(
        interactions.load(&old_wire).await.err(),
        Some(StateError::InvalidState)
    );
    for expected_revision in [None, Some(current.revision)] {
        assert_eq!(
            interactions.delete(&old_wire, expected_revision).await,
            Err(StateError::InvalidState)
        );
        assert_eq!(
            interactions
                .load(&new_wire)
                .await
                .unwrap()
                .unwrap()
                .value
                .data,
            b"new"
        );
    }
    assert!(
        interactions
            .delete(&new_wire, Some(current.revision))
            .await
            .unwrap()
    );
}

fn subscription() -> Subscription {
    Subscription {
        route: route(3),
        event: label("message_create"),
        resume_handler: label("continue_flow"),
        expires_at: Timestamp(100),
        checkpoint: b"checkpoint".to_vec(),
    }
}

fn route_key(route: &CustomId, kind: StateKind) -> StateKey {
    StateKey::new(
        scope(1, 0),
        route.namespace.clone(),
        route.feature.clone(),
        kind,
        route.state_id,
    )
}

#[test]
fn serde_cannot_bypass_label_schema_or_route_invariants() {
    for invalid in ["", "bot_token", "a:b", "ABC", "確認"] {
        assert!(serde_json::from_value::<PublicLabel>(serde_json::json!(invalid)).is_err());
    }
    for invalid in [
        serde_json::json!({"version": 0, "compatible_since": 0}),
        serde_json::json!({"version": 1, "compatible_since": 2}),
        serde_json::json!({"version": 1, "compatible_since": 0}),
        serde_json::json!({"version": 1, "compatible_since": 1, "unknown": true}),
    ] {
        assert!(serde_json::from_value::<SchemaVersion>(invalid).is_err());
    }
    let original = serde_json::to_value(route(1)).unwrap();
    for (field, invalid) in [
        ("namespace", serde_json::json!("secret")),
        ("feature", serde_json::json!("../path")),
        ("version", serde_json::json!(0)),
        ("version", serde_json::json!(65536)),
        ("state_id", serde_json::json!([1, 2, 3])),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut changed = original.clone();
        changed[field] = invalid;
        assert!(serde_json::from_value::<CustomId>(changed).is_err());
    }
    assert_eq!(
        serde_json::from_value::<CustomId>(original).unwrap(),
        route(1)
    );
    let mut bad_subscription = serde_json::to_value(subscription()).unwrap();
    bad_subscription["resume_handler"] = serde_json::json!("password");
    assert!(serde_json::from_value::<Subscription>(bad_subscription).is_err());
}

#[tokio::test]
async fn subscription_rejects_corruption_and_mismatched_route_or_expiry() {
    let store = memory(&FakeClock::default());
    let subscription = subscription();
    let correct = serde_json::to_value(&subscription).unwrap();
    let mut wrong_route = correct.clone();
    wrong_route["route"] = serde_json::to_value(route(4)).unwrap();
    let mut wrong_version = correct.clone();
    wrong_version["route"]["version"] = serde_json::json!(2);
    let mut wrong_expiry = correct.clone();
    wrong_expiry["expires_at"] = serde_json::json!(99);
    let mut unknown_field = correct.clone();
    unknown_field["unknown"] = serde_json::json!(true);
    for data in [
        b"not-json".to_vec(),
        serde_json::to_vec(&wrong_route).unwrap(),
        serde_json::to_vec(&wrong_version).unwrap(),
        serde_json::to_vec(&wrong_expiry).unwrap(),
        serde_json::to_vec(&unknown_field).unwrap(),
    ] {
        store
            .put(
                route_key(&subscription.route, StateKind::Subscription),
                value(&data, Some(100)),
                PutCondition::Any,
            )
            .await
            .unwrap();
        assert_eq!(
            Subscription::restore(&store, scope(1, 0), &subscription.route, schema(1, 1))
                .await
                .err(),
            Some(StateError::InvalidState)
        );
    }
    let bytes = serde_json::to_vec(&subscription).unwrap();
    store
        .put(
            route_key(&subscription.route, StateKind::Subscription),
            value(&bytes, None),
            PutCondition::Any,
        )
        .await
        .unwrap();
    assert_eq!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(1, 1))
            .await
            .err(),
        Some(StateError::InvalidState)
    );
}

#[tokio::test]
async fn interaction_rejects_corrupt_envelope_wrong_route_and_unbounded_lifetime() {
    let raw = memory(&FakeClock::default());
    let store: Arc<dyn StateStore> = Arc::new(raw.clone());
    let codec = codec(scope(1, 0), StateKind::View);
    let route = route(1);
    let wire = codec.encode(&route).unwrap();
    let interaction = InteractionStore::new(store, codec, schema(1, 1)).unwrap();
    let correct = serde_json::json!({"route": route, "data": [1, 2, 3]});
    let mut wrong_route = correct.clone();
    wrong_route["route"] = serde_json::to_value(self::route(2)).unwrap();
    let mut wrong_version = correct.clone();
    wrong_version["route"]["version"] = serde_json::json!(2);
    let mut unknown_field = correct.clone();
    unknown_field["unknown"] = serde_json::json!(true);
    for data in [
        b"[]".to_vec(),
        b"{".to_vec(),
        serde_json::to_vec(&wrong_route).unwrap(),
        serde_json::to_vec(&wrong_version).unwrap(),
        serde_json::to_vec(&unknown_field).unwrap(),
    ] {
        raw.put(
            route_key(&route, StateKind::View),
            value(&data, Some(100)),
            PutCondition::Any,
        )
        .await
        .unwrap();
        assert_eq!(
            interaction.load(&wire).await.err(),
            Some(StateError::InvalidState)
        );
    }
    raw.put(
        route_key(&route, StateKind::View),
        value(&serde_json::to_vec(&correct).unwrap(), None),
        PutCondition::Any,
    )
    .await
    .unwrap();
    assert_eq!(
        interaction.load(&wire).await.err(),
        Some(StateError::InvalidState)
    );
}

#[test]
fn debug_does_not_expose_state_or_checkpoint_bytes() {
    let marker = b"payload-must-stay-private";
    let value = value(marker, Some(100));
    let mut subscription = subscription();
    subscription.checkpoint = marker.to_vec();
    let formatted = format!(
        "{value:?} {subscription:?} {:?}",
        codec(scope(1, 0), StateKind::View)
    );
    assert!(!formatted.contains(std::str::from_utf8(marker).unwrap()));
    assert!(!formatted.contains(&format!("{:?}", marker.to_vec())));
    assert!(formatted.contains("data_len"));
    assert!(formatted.contains("checkpoint_len"));
    assert!(formatted.contains("REDACTED"));
}

#[tokio::test]
async fn subscription_descriptor_round_trips_and_cancels_explicitly() {
    let clock = FakeClock::default();
    let store = memory(&clock);
    let subscription = subscription();
    let revision = subscription
        .save(&store, scope(1, 0), schema(1, 1), PutCondition::IfAbsent)
        .await
        .unwrap();
    let new_handle = store.clone();
    assert_eq!(
        Subscription::restore(&new_handle, scope(1, 0), &subscription.route, schema(2, 1))
            .await
            .unwrap(),
        Some(subscription.clone())
    );
    for other_scope in [scope(2, 0), scope(1, 1)] {
        assert!(
            Subscription::restore(&new_handle, other_scope, &subscription.route, schema(1, 1))
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        Subscription::cancel(&store, scope(1, 0), &subscription.route, Some(revision + 1)).await,
        Err(StateError::Conflict)
    );
    assert!(
        Subscription::cancel(&store, scope(1, 0), &subscription.route, Some(revision))
            .await
            .unwrap()
    );
    assert!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(1, 1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !Subscription::cancel(&store, scope(1, 0), &subscription.route, None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn stale_subscription_route_cannot_cancel_replaced_version() {
    let store = memory(&FakeClock::default());
    let old = subscription();
    old.save(&store, scope(1, 0), schema(1, 1), PutCondition::Any)
        .await
        .unwrap();
    let mut new = old.clone();
    new.route.version = 2;
    new.checkpoint = b"new checkpoint".to_vec();
    let revision = new
        .save(&store, scope(1, 0), schema(1, 1), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        Subscription::restore(&store, scope(1, 0), &old.route, schema(1, 1))
            .await
            .err(),
        Some(StateError::InvalidState)
    );
    for expected_revision in [None, Some(revision)] {
        assert_eq!(
            Subscription::cancel(&store, scope(1, 0), &old.route, expected_revision).await,
            Err(StateError::InvalidState)
        );
        assert_eq!(
            Subscription::restore(&store, scope(1, 0), &new.route, schema(1, 1))
                .await
                .unwrap(),
            Some(new.clone())
        );
    }
    assert!(
        Subscription::cancel(&store, scope(1, 0), &new.route, Some(revision))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn subscription_expiry_compatibility_and_volatile_durability_are_explicit() {
    let clock = FakeClock::default();
    let store = memory(&clock);
    let subscription = subscription();
    assert_eq!(
        subscription
            .durable_wait(&store, scope(1, 0), schema(1, 1), PutCondition::Any)
            .await,
        Err(StateError::DurabilityRequired)
    );
    assert_eq!(store.usage().await.unwrap().entries, 0);
    subscription
        .save(&store, scope(1, 0), schema(2, 2), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(1, 1))
            .await
            .err(),
        Some(StateError::IncompatibleSchema)
    );
    clock.set(99);
    assert!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(2, 2))
            .await
            .unwrap()
            .is_some()
    );
    clock.set(100);
    assert!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(2, 2))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.usage().await.unwrap().entries, 0);
}

#[tokio::test]
async fn durable_wait_passes_descriptor_and_conditions_to_a_capable_backend() {
    // This spy tests capability dispatch only. It does not persist data and is
    // not evidence of database, process-restart, or host-failure durability.
    #[derive(Default)]
    struct DurableSpy(Mutex<Option<(StateKey, StateValue, PutCondition)>>);
    impl StateStore for DurableSpy {
        fn get<'a>(
            &'a self,
            _: &'a StateKey,
            _: SchemaVersion,
        ) -> StoreFuture<'a, Option<StateRecord>> {
            Box::pin(async { Ok(None) })
        }
        fn put(
            &self,
            key: StateKey,
            value: StateValue,
            condition: PutCondition,
        ) -> StoreFuture<'_, u64> {
            Box::pin(async move {
                *self.0.lock().unwrap() = Some((key, value, condition));
                Ok(73)
            })
        }
        fn delete<'a>(&'a self, _: &'a StateKey, _: Option<u64>) -> StoreFuture<'a, bool> {
            Box::pin(async { Ok(false) })
        }
        fn durability(&self) -> Durability {
            Durability::Durable
        }
    }
    let store = DurableSpy::default();
    let subscription = subscription();
    assert_eq!(
        subscription
            .durable_wait(
                &store,
                scope(1, 0),
                schema(2, 1),
                PutCondition::IfRevision(72)
            )
            .await,
        Ok(73)
    );
    let (key, value, condition) = store.0.lock().unwrap().take().unwrap();
    assert_eq!(key, route_key(&subscription.route, StateKind::Subscription));
    assert_eq!(condition, PutCondition::IfRevision(72));
    assert_eq!(value.schema, schema(2, 1));
    assert_eq!(value.expires_at, Some(subscription.expires_at));
    assert_eq!(
        serde_json::from_slice::<Subscription>(&value.data).unwrap(),
        subscription
    );
}

#[tokio::test]
async fn wait_for_completes_or_cancels_for_drain_and_closed_signal() {
    let (drain, signal) = watch::channel(false);
    assert_eq!(wait_for(async { 42 }, signal.clone()).await, Ok(42));
    drain.send_replace(true);
    assert_eq!(
        wait_for(async { 42 }, signal.clone()).await,
        Err(WaitCancelled::Draining)
    );
    let (sender, signal) = watch::channel(false);
    drop(sender);
    assert_eq!(
        wait_for(pending::<()>(), signal).await,
        Err(WaitCancelled::SignalClosed)
    );
    let (sender, signal) = watch::channel(false);
    drop(sender);
    assert_eq!(
        wait_for(async { 42 }, signal).await,
        Err(WaitCancelled::SignalClosed)
    );
    let (sender, signal) = watch::channel(false);
    let waiting = tokio::spawn(wait_for(pending::<()>(), signal));
    sender.send_replace(false);
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    sender.send_replace(true);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap(),
        Err(WaitCancelled::Draining)
    );
}

#[tokio::test]
async fn cancelled_wait_drops_local_future_and_keeps_subscription() {
    struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let store = memory(&FakeClock::default());
    let subscription = subscription();
    subscription
        .save(&store, scope(1, 0), schema(1, 1), PutCondition::Any)
        .await
        .unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = DropFlag(dropped.clone());
    let waiting = async move {
        let _guard = guard;
        pending::<()>().await;
    };
    let (sender, signal) = watch::channel(false);
    let task = tokio::spawn(wait_for(waiting, signal));
    sender.send_replace(true);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(WaitCancelled::Draining)
    );
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        Subscription::restore(&store, scope(1, 0), &subscription.route, schema(1, 1))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_if_absent_has_one_winner_and_respects_capacity() {
    let store = Arc::new(MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            ..limits()
        },
        Arc::new(FakeClock::default()),
    ));
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = Vec::new();
    for index in 0..16u8 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .put(key(1), value(&[index], None), PutCondition::IfAbsent)
                .await
        }));
    }
    barrier.wait().await;
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => successes += 1,
            Err(StateError::Conflict) => (),
            unexpected => panic!("unexpected result: {unexpected:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(store.usage().await.unwrap().entries, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unique_keys_cannot_oversubscribe_entry_or_byte_budgets() {
    let charge = key(1).charged_bytes(1).unwrap();
    for limits in [
        StoreLimits {
            max_entries: 3,
            ..limits()
        },
        StoreLimits {
            max_entries: 16,
            max_bytes: charge * 3,
            ..limits()
        },
    ] {
        let store = Arc::new(MemoryStateStore::new(
            limits,
            Arc::new(FakeClock::default()),
        ));
        let barrier = Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for id in 0..16u8 {
            let store = store.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                store
                    .put(key(id), value(&[id], None), PutCondition::IfAbsent)
                    .await
            }));
        }
        barrier.wait().await;
        let mut successes = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(_) => successes += 1,
                Err(StateError::CapacityExceeded) => (),
                unexpected => panic!("unexpected result: {unexpected:?}"),
            }
        }
        assert_eq!(successes, 3);
        let usage = store.usage().await.unwrap();
        assert_eq!(usage.entries, 3);
        assert_eq!(usage.bytes, charge * 3);
    }
}
