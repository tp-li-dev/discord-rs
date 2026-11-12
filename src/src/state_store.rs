//! Optional, asynchronous application state. No database or file I/O runs here.
//!
//! The memory backend is volatile: cloning a handle shares state within one
//! process, not across process/host failure. External backends must implement
//! atomic conditions, expiry and capacity checks, and offload synchronous I/O to
//! a dedicated worker. Neither this API nor a custom_id grants Discord permissions
//! or ownership; callers still authorize the user and fence side effects.
use crate::protocol::BotId;
use bot_framework::interactions::{
    CreatedState, InteractionError, InteractionKind, InteractionStateStore, RestoreFuture,
    RestoredState, Route, StateFuture, StoredState,
};
use rustls::crypto::tls13::OkmBlock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, watch};

pub const CUSTOM_ID_LIMIT: usize = 100;
pub const LABEL_LIMIT: usize = 24;

/// Errors never contain input, payloads, custom IDs, or signing keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateError {
    InvalidLabel,
    SensitiveLabel,
    InvalidVersion,
    InvalidExpiry,
    ClockUnavailable,
    ClockRegressed,
    CapacityExceeded,
    ValueTooLarge,
    Conflict,
    RevisionExhausted,
    IncompatibleSchema,
    InvalidCustomId,
    UnsupportedVersion,
    Tampered,
    InvalidState,
    DurabilityRequired,
    BackendUnavailable,
    /// A remote write may have committed; callers must reconcile, not retry blindly.
    UnknownOutcome,
    Encoding,
    RandomUnavailable,
}
impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "state store error: {self:?}")
    }
}
impl std::error::Error for StateError {}

/// Milliseconds since the Unix epoch, suitable for storage across generations.
/// Wall-clock synchronization is an operator responsibility for shared backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

/// A cheap clock query: implementations must not perform blocking I/O.
pub trait StateClock: Send + Sync {
    fn now(&self) -> Result<Timestamp, StateError>;
}
#[derive(Default)]
pub struct SystemClock;
impl StateClock for SystemClock {
    fn now(&self) -> Result<Timestamp, StateError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StateError::ClockUnavailable)?
            .as_millis();
        Ok(Timestamp(
            u64::try_from(millis).map_err(|_| StateError::ClockUnavailable)?,
        ))
    }
}

/// Public routing metadata only. Restricts syntax and rejects common credential
/// labels. This is a guardrail, not a detector for arbitrary unlabelled secrets:
/// applications must use static public labels, never user data or credentials.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PublicLabel(String);
impl PublicLabel {
    pub fn new(value: &str) -> Result<Self, StateError> {
        if value.is_empty()
            || value.len() > LABEL_LIMIT
            || !value.as_bytes()[0].is_ascii_lowercase()
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(StateError::InvalidLabel);
        }
        if [
            "token",
            "secret",
            "password",
            "authorization",
            "bearer",
            "credential",
            "private_key",
            "api_key",
        ]
        .iter()
        .any(|label| value.contains(label))
        {
            return Err(StateError::SensitiveLabel);
        }
        Ok(Self(value.to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for PublicLabel {
    type Error = StateError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}
impl From<PublicLabel> for String {
    fn from(value: PublicLabel) -> Self {
        value.0
    }
}

/// Opaque lookup key, not authorization and never a secret-bearing string.
/// Generate randomly in production; from_bytes supports fixtures/imported IDs.
/// Allocate a fresh ID for each newly created interaction/subscription; an old
/// signed ID can be replayed until its state expires or is explicitly deleted.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct StateId([u8; 16]);
impl StateId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
    /// Uses OS entropy. Prepare IDs outside the Gateway heartbeat/dispatch path;
    /// get/put/delete themselves never call the RNG or perform storage I/O.
    pub fn random() -> Result<Self, StateError> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|_| StateError::RandomUnavailable)?;
        Ok(Self(bytes))
    }
}
impl fmt::Debug for StateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StateId([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct StoreScope {
    pub bot_id: BotId,
    pub shard_id: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum StateKind {
    View,
    Modal,
    Subscription,
    Application,
}
impl StateKind {
    fn tag(self) -> u8 {
        match self {
            Self::View => 1,
            Self::Modal => 2,
            Self::Subscription => 3,
            Self::Application => 4,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateKey {
    pub scope: StoreScope,
    pub namespace: PublicLabel,
    pub feature: PublicLabel,
    pub kind: StateKind,
    pub state_id: StateId,
}
impl StateKey {
    pub fn new(
        scope: StoreScope,
        namespace: PublicLabel,
        feature: PublicLabel,
        kind: StateKind,
        state_id: StateId,
    ) -> Self {
        Self {
            scope,
            namespace,
            feature,
            kind,
            state_id,
        }
    }
    /// Logical storage budget: payload + label bytes + fixed metadata allowance.
    /// Allocator/HashMap overhead and caller-owned buffers are not RSS-accounted.
    pub fn charged_bytes(&self, value_len: usize) -> Option<usize> {
        64usize
            .checked_add(self.namespace.0.len())?
            .checked_add(self.feature.0.len())?
            .checked_add(value_len)
    }
}

/// Writer schema version and oldest reader version compatible with its data.
/// As a reader, compatible_since is the oldest stored schema it understands.
/// Expand/Migrate/Contract and actual payload migration remain app responsibilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SchemaFields", into = "SchemaFields")]
pub struct SchemaVersion {
    version: u16,
    compatible_since: u16,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaFields {
    version: u16,
    compatible_since: u16,
}
impl SchemaVersion {
    pub fn new(version: u16, compatible_since: u16) -> Result<Self, StateError> {
        if compatible_since == 0 || compatible_since > version {
            return Err(StateError::InvalidVersion);
        }
        Ok(Self {
            version,
            compatible_since,
        })
    }
    pub fn version(self) -> u16 {
        self.version
    }
    pub fn compatible_since(self) -> u16 {
        self.compatible_since
    }
    pub fn can_read(self, stored: Self) -> bool {
        stored.compatible_since <= self.version && self.compatible_since <= stored.version
    }
}
impl TryFrom<SchemaFields> for SchemaVersion {
    type Error = StateError;
    fn try_from(value: SchemaFields) -> Result<Self, Self::Error> {
        Self::new(value.version, value.compatible_since)
    }
}
impl From<SchemaVersion> for SchemaFields {
    fn from(value: SchemaVersion) -> Self {
        Self {
            version: value.version,
            compatible_since: value.compatible_since,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateValue {
    pub schema: SchemaVersion,
    pub expires_at: Option<Timestamp>,
    pub data: Vec<u8>,
}
impl fmt::Debug for StateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateValue")
            .field("schema", &self.schema)
            .field("expires_at", &self.expires_at)
            .field("data_len", &self.data.len())
            .finish()
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRecord {
    pub value: StateValue,
    pub revision: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutCondition {
    Any,
    IfAbsent,
    IfRevision(u64),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Acknowledged data may be lost when this process exits.
    Volatile,
    /// The backend acknowledges persistence before returning success and can
    /// reopen it after process restart. The implementation must document its
    /// host/storage failure model. This flag is not quorum or a delivery claim.
    Durable,
}
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StateError>> + Send + 'a>>;

/// Atomic operations per key. Expired means absent at `now >= expires_at`.
/// Revisions must not be reused while clients can hold an earlier revision.
/// Failed conditions/capacity checks preserve live data. Dropped futures must
/// never partially commit; a backend must document cancellation/unknown outcomes.
/// Implementations must not do blocking I/O in these futures, including polling.
pub trait StateStore: Send + Sync {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>>;
    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64>;
    fn delete<'a>(
        &'a self,
        key: &'a StateKey,
        expected_revision: Option<u64>,
    ) -> StoreFuture<'a, bool>;
    fn durability(&self) -> Durability {
        Durability::Volatile
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreLimits {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub max_value_bytes: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreUsage {
    pub entries: usize,
    pub bytes: usize,
}
#[derive(Default)]
struct MemoryInner {
    records: HashMap<StateKey, StateRecord>,
    bytes: usize,
    last_now: Option<Timestamp>,
    revision: u64,
}
impl MemoryInner {
    fn observe(&mut self, clock: &dyn StateClock) -> Result<Timestamp, StateError> {
        let now = clock.now()?;
        if self.last_now.is_some_and(|previous| now < previous) {
            return Err(StateError::ClockRegressed);
        }
        self.last_now = Some(now);
        Ok(now)
    }
    fn purge(&mut self, now: Timestamp) {
        self.records.retain(|key, record| {
            if record.value.expires_at.is_some_and(|expiry| now >= expiry) {
                self.bytes -= key
                    .charged_bytes(record.value.data.len())
                    .expect("stored size validated");
                false
            } else {
                true
            }
        });
    }
}
/// Bounded in-process backend, using an async mutex and no synchronous I/O.
/// Lazy expiration scans at most max_entries on each operation; no background
/// tasks or unbounded tombstones. Explicit limits should suit the local workload.
#[derive(Clone)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<MemoryInner>>,
    limits: StoreLimits,
    clock: Arc<dyn StateClock>,
}
impl MemoryStateStore {
    pub fn new(limits: StoreLimits, clock: Arc<dyn StateClock>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryInner::default())),
            limits,
            clock,
        }
    }
    pub async fn usage(&self) -> Result<StoreUsage, StateError> {
        let mut inner = self.inner.lock().await;
        let now = inner.observe(self.clock.as_ref())?;
        inner.purge(now);
        Ok(StoreUsage {
            entries: inner.records.len(),
            bytes: inner.bytes,
        })
    }
}
impl StateStore for MemoryStateStore {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().await;
            let now = inner.observe(self.clock.as_ref())?;
            inner.purge(now);
            let record = inner.records.get(key);
            if record.is_some_and(|record| !reader.can_read(record.value.schema)) {
                return Err(StateError::IncompatibleSchema);
            }
            Ok(record.cloned())
        })
    }
    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            if value.data.len() > self.limits.max_value_bytes {
                return Err(StateError::ValueTooLarge);
            }
            let size = key
                .charged_bytes(value.data.len())
                .ok_or(StateError::CapacityExceeded)?;
            let mut inner = self.inner.lock().await;
            let now = inner.observe(self.clock.as_ref())?;
            if value.expires_at.is_some_and(|expiry| now >= expiry) {
                return Err(StateError::InvalidExpiry);
            }
            inner.purge(now);
            let old = inner.records.get(&key);
            match condition {
                PutCondition::IfAbsent if old.is_some() => return Err(StateError::Conflict),
                PutCondition::IfRevision(expected)
                    if old.map(|record| record.revision) != Some(expected) =>
                {
                    return Err(StateError::Conflict);
                }
                _ => {}
            }
            let old_size = old.map_or(0, |record| {
                key.charged_bytes(record.value.data.len())
                    .expect("stored size validated")
            });
            let bytes = (inner.bytes - old_size)
                .checked_add(size)
                .ok_or(StateError::CapacityExceeded)?;
            if bytes > self.limits.max_bytes
                || (old.is_none() && inner.records.len() >= self.limits.max_entries)
            {
                return Err(StateError::CapacityExceeded);
            }
            let revision = inner
                .revision
                .checked_add(1)
                .ok_or(StateError::RevisionExhausted)?;
            inner.records.insert(key, StateRecord { value, revision });
            inner.bytes = bytes;
            inner.revision = revision;
            Ok(revision)
        })
    }
    fn delete<'a>(
        &'a self,
        key: &'a StateKey,
        expected_revision: Option<u64>,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let mut inner = self.inner.lock().await;
            let now = inner.observe(self.clock.as_ref())?;
            inner.purge(now);
            if expected_revision.is_some_and(|expected| {
                inner.records.get(key).map(|record| record.revision) != Some(expected)
            }) {
                return Err(StateError::Conflict);
            }
            if let Some(record) = inner.records.remove(key) {
                inner.bytes -= key
                    .charged_bytes(record.value.data.len())
                    .expect("stored size validated");
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CustomIdFields", into = "CustomIdFields")]
pub struct CustomId {
    pub namespace: PublicLabel,
    pub feature: PublicLabel,
    pub version: u16,
    pub state_id: StateId,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CustomIdFields {
    namespace: PublicLabel,
    feature: PublicLabel,
    version: u16,
    state_id: StateId,
}
impl CustomId {
    pub fn new(
        namespace: PublicLabel,
        feature: PublicLabel,
        version: u16,
        state_id: StateId,
    ) -> Result<Self, StateError> {
        if version == 0 {
            return Err(StateError::InvalidVersion);
        }
        Ok(Self {
            namespace,
            feature,
            version,
            state_id,
        })
    }
    fn key(&self, scope: StoreScope, kind: StateKind) -> StateKey {
        StateKey::new(
            scope,
            self.namespace.clone(),
            self.feature.clone(),
            kind,
            self.state_id,
        )
    }
}
impl TryFrom<CustomIdFields> for CustomId {
    type Error = StateError;
    fn try_from(value: CustomIdFields) -> Result<Self, Self::Error> {
        Self::new(
            value.namespace,
            value.feature,
            value.version,
            value.state_id,
        )
    }
}
impl From<CustomId> for CustomIdFields {
    fn from(value: CustomId) -> Self {
        Self {
            namespace: value.namespace,
            feature: value.feature,
            version: value.version,
            state_id: value.state_id,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}
fn unhex16(input: &str) -> Result<[u8; 16], StateError> {
    fn digit(byte: u8) -> Result<u8, StateError> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(StateError::InvalidCustomId),
        }
    }
    if input.len() != 32 {
        return Err(StateError::InvalidCustomId);
    }
    let mut bytes = [0; 16];
    for (output, pair) in bytes.iter_mut().zip(input.as_bytes().as_chunks::<2>().0) {
        *output = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Ok(bytes)
}

/// Canonical wire format: `1:namespace:feature:version:state_key:mac`.
/// HMAC-SHA256 truncated to 128 bits, constant-time tag comparison. Reuses
/// rustls' ring-backed HMAC provider, not a new cryptographic implementation.
/// The signing key must be independently generated (never a Bot token), kept
/// outside the StateStore and retained across Core generations. Rotation needs
/// an explicit overlap strategy; unknown application versions are rejected.
/// MAC protects integrity, not confidentiality/replay/user authorization.
pub struct CustomIdCodec {
    scope: StoreScope,
    kind: StateKind,
    secret: OkmBlock,
    supported_versions: Vec<u16>,
}
impl fmt::Debug for CustomIdCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CustomIdCodec([REDACTED])")
    }
}
impl CustomIdCodec {
    pub fn new(
        scope: StoreScope,
        kind: StateKind,
        secret: [u8; 32],
        mut supported_versions: Vec<u16>,
    ) -> Result<Self, StateError> {
        if supported_versions.is_empty() || supported_versions.contains(&0) {
            return Err(StateError::InvalidVersion);
        }
        supported_versions.sort_unstable();
        supported_versions.dedup();
        Ok(Self {
            scope,
            kind,
            secret: OkmBlock::new(&secret),
            supported_versions,
        })
    }
    fn tag(&self, body: &str) -> [u8; 16] {
        let mut message = Vec::with_capacity(48 + body.len());
        message.extend_from_slice(b"discord-rs/custom-id/v1\0");
        message.extend_from_slice(&self.scope.bot_id.0.to_be_bytes());
        message.extend_from_slice(&self.scope.shard_id.to_be_bytes());
        message.push(self.kind.tag());
        message.extend_from_slice(body.as_bytes());
        let provider = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("TLS 1.3 suite")
            .hkdf_provider;
        let tag = provider.hmac_sign(&self.secret, &message);
        let mut result = [0; 16];
        result.copy_from_slice(&tag.as_ref()[..16]);
        result
    }
    pub fn encode(&self, id: &CustomId) -> Result<String, StateError> {
        if !self.supported_versions.contains(&id.version) {
            return Err(StateError::UnsupportedVersion);
        }
        let body = format!(
            "1:{}:{}:{}:{}",
            id.namespace.as_str(),
            id.feature.as_str(),
            id.version,
            hex(&id.state_id.0)
        );
        if body.len() + 33 > CUSTOM_ID_LIMIT {
            return Err(StateError::InvalidCustomId);
        }
        Ok(format!("{body}:{}", hex(&self.tag(&body))))
    }
    pub fn decode(&self, wire: &str) -> Result<CustomId, StateError> {
        if wire.len() > CUSTOM_ID_LIMIT || !wire.is_ascii() {
            return Err(StateError::InvalidCustomId);
        }
        let parts: Vec<_> = wire.split(':').collect();
        if parts.len() != 6 {
            return Err(StateError::InvalidCustomId);
        }
        if parts[0] != "1" {
            return Err(StateError::UnsupportedVersion);
        }
        let namespace = PublicLabel::new(parts[1])?;
        let feature = PublicLabel::new(parts[2])?;
        let version = parts[3]
            .parse::<u16>()
            .map_err(|_| StateError::InvalidCustomId)?;
        if version.to_string() != parts[3] {
            return Err(StateError::InvalidCustomId);
        }
        if !self.supported_versions.contains(&version) {
            return Err(StateError::UnsupportedVersion);
        }
        let state_id = StateId(unhex16(parts[4])?);
        let supplied = unhex16(parts[5])?;
        let body = &wire[..wire.len() - 33];
        if !bool::from(self.tag(body).ct_eq(&supplied)) {
            return Err(StateError::Tampered);
        }
        CustomId::new(namespace, feature, version, state_id)
    }
}

/// Typed View/Modal envelope. Reuse the backend and signing key in a new Core;
/// payload, version and expiry are checked before exposing stored state.
pub struct InteractionStore {
    store: Arc<dyn StateStore>,
    codec: CustomIdCodec,
    schema: SchemaVersion,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionEnvelope {
    route: CustomId,
    data: Vec<u8>,
}
impl InteractionStore {
    pub fn new(
        store: Arc<dyn StateStore>,
        codec: CustomIdCodec,
        schema: SchemaVersion,
    ) -> Result<Self, StateError> {
        if !matches!(codec.kind, StateKind::View | StateKind::Modal) {
            return Err(StateError::InvalidState);
        }
        Ok(Self {
            store,
            codec,
            schema,
        })
    }
    pub async fn save(
        &self,
        route: &CustomId,
        data: Vec<u8>,
        expires_at: Timestamp,
        condition: PutCondition,
    ) -> Result<String, StateError> {
        self.save_versioned(route, data, expires_at, condition)
            .await
            .map(|(wire, _)| wire)
    }
    /// Return the revision acknowledged by this exact write, without a second
    /// read that could observe a concurrent update. This low-level API accepts
    /// caller-owned routes; use the framework adapter for authenticated updates.
    pub async fn save_versioned(
        &self,
        route: &CustomId,
        data: Vec<u8>,
        expires_at: Timestamp,
        condition: PutCondition,
    ) -> Result<(String, u64), StateError> {
        let wire = self.codec.encode(route)?;
        let data = serde_json::to_vec(&InteractionEnvelope {
            route: route.clone(),
            data,
        })
        .map_err(|_| StateError::Encoding)?;
        let revision = self
            .store
            .put(
                route.key(self.codec.scope, self.codec.kind),
                StateValue {
                    schema: self.schema,
                    expires_at: Some(expires_at),
                    data,
                },
                condition,
            )
            .await?;
        Ok((wire, revision))
    }
    pub async fn load(&self, wire: &str) -> Result<Option<StateRecord>, StateError> {
        let route = self.codec.decode(wire)?;
        self.load_route(&route).await
    }
    async fn load_route(&self, route: &CustomId) -> Result<Option<StateRecord>, StateError> {
        let Some(mut record) = self
            .store
            .get(&route.key(self.codec.scope, self.codec.kind), self.schema)
            .await?
        else {
            return Ok(None);
        };
        if record.value.expires_at.is_none() {
            return Err(StateError::InvalidState);
        }
        let envelope: InteractionEnvelope =
            serde_json::from_slice(&record.value.data).map_err(|_| StateError::InvalidState)?;
        if envelope.route != *route {
            return Err(StateError::InvalidState);
        }
        record.value.data = envelope.data;
        Ok(Some(record))
    }
    /// Authenticate and validate the current envelope, then replace only that
    /// revision. Missing/expired state conflicts; a stale route cannot overwrite
    /// a newer route version that reused the same state ID.
    pub async fn compare_and_swap(
        &self,
        wire: &str,
        expected_revision: u64,
        data: Vec<u8>,
        expires_at: Timestamp,
    ) -> Result<u64, StateError> {
        let route = self.codec.decode(wire)?;
        self.compare_and_swap_route(&route, expected_revision, data, expires_at)
            .await
    }
    async fn compare_and_swap_route(
        &self,
        route: &CustomId,
        expected_revision: u64,
        data: Vec<u8>,
        expires_at: Timestamp,
    ) -> Result<u64, StateError> {
        let record = self.load_route(route).await?.ok_or(StateError::Conflict)?;
        if record.revision != expected_revision {
            return Err(StateError::Conflict);
        }
        self.save_versioned(
            route,
            data,
            expires_at,
            PutCondition::IfRevision(record.revision),
        )
        .await
        .map(|(_, revision)| revision)
    }
    pub async fn delete(
        &self,
        wire: &str,
        expected_revision: Option<u64>,
    ) -> Result<bool, StateError> {
        let route = self.codec.decode(wire)?;
        self.delete_route(&route, expected_revision).await
    }
    async fn delete_route(
        &self,
        route: &CustomId,
        expected_revision: Option<u64>,
    ) -> Result<bool, StateError> {
        let Some(record) = self.load_route(route).await? else {
            return if expected_revision.is_some() {
                Err(StateError::Conflict)
            } else {
                Ok(false)
            };
        };
        if expected_revision.is_some_and(|revision| revision != record.revision) {
            return Err(StateError::Conflict);
        }
        // Bind the validated envelope to the delete, including concurrent
        // replacement with a different route version using the same state ID.
        self.store
            .delete(
                &route.key(self.codec.scope, self.codec.kind),
                Some(record.revision),
            )
            .await
    }
}

/// Framework adapter bound at startup to one trusted Bot/Shard and namespace.
/// Components (buttons/selects) and modals use distinct MAC domains and keys in
/// the backend. Every read/update authenticates the ID and validates its envelope;
/// conditional writes additionally bind the validated revision atomically.
///
/// Retain the backend and independent signing key across Core generations. This
/// adapter does not improve backend durability: MemoryStateStore remains volatile.
/// Worker transport, user authorization, admission/fencing, reply tracking and
/// Subscription delivery are separate host responsibilities and are not connected
/// here. It never needs a Bot token or treats a signed ID as user authorization.
pub struct FrameworkStateStore {
    namespace: PublicLabel,
    components: InteractionStore,
    modals: InteractionStore,
}
impl FrameworkStateStore {
    pub fn new(
        store: Arc<dyn StateStore>,
        scope: StoreScope,
        namespace: PublicLabel,
        secret: [u8; 32],
        supported_versions: Vec<u16>,
        schema: SchemaVersion,
    ) -> Result<Self, StateError> {
        let components = InteractionStore::new(
            store.clone(),
            CustomIdCodec::new(scope, StateKind::View, secret, supported_versions.clone())?,
            schema,
        )?;
        let modals = InteractionStore::new(
            store,
            CustomIdCodec::new(scope, StateKind::Modal, secret, supported_versions)?,
            schema,
        )?;
        Ok(Self {
            namespace,
            components,
            modals,
        })
    }

    fn interactions(&self, kind: InteractionKind) -> &InteractionStore {
        match kind {
            InteractionKind::Component => &self.components,
            InteractionKind::Modal => &self.modals,
        }
    }

    fn authenticated_route(
        &self,
        kind: InteractionKind,
        custom_id: &str,
    ) -> Result<CustomId, InteractionError> {
        let route = self
            .interactions(kind)
            .codec
            .decode(custom_id)
            .map_err(framework_state_error)?;
        if route.namespace != self.namespace {
            return Err(InteractionError::InvalidId);
        }
        Ok(route)
    }

    /// Insert a fresh, caller-prepared ID only if absent. Prepare random IDs
    /// outside latency-sensitive paths; deterministic IDs are useful in tests.
    /// Never reuse a deleted/expired ID: an old signed ID could otherwise replay.
    /// The returned revision is from this commit, not a subsequent read.
    pub async fn create_with_id(
        &self,
        kind: InteractionKind,
        route: &Route,
        state_id: StateId,
        data: Vec<u8>,
        expires_at_unix_ms: u64,
    ) -> Result<CreatedState, InteractionError> {
        if route.namespace != self.namespace.as_str() {
            return Err(InteractionError::InvalidId);
        }
        let route = CustomId::new(
            self.namespace.clone(),
            PublicLabel::new(&route.name).map_err(framework_state_error)?,
            route.version,
            state_id,
        )
        .map_err(framework_state_error)?;
        let (custom_id, revision) = self
            .interactions(kind)
            .save_versioned(
                &route,
                data,
                Timestamp(expires_at_unix_ms),
                PutCondition::IfAbsent,
            )
            .await
            .map_err(framework_state_error)?;
        Ok(CreatedState {
            custom_id,
            revision,
            expires_at_unix_ms,
        })
    }

    async fn load_state(
        &self,
        kind: InteractionKind,
        custom_id: &str,
    ) -> Result<StoredState, InteractionError> {
        let route = self.authenticated_route(kind, custom_id)?;
        let record = self
            .interactions(kind)
            .load_route(&route)
            .await
            .map_err(framework_state_error)?
            .ok_or(InteractionError::ExpiredOrMissing)?;
        Ok(StoredState {
            route: Route {
                namespace: route.namespace.as_str().to_owned(),
                name: route.feature.as_str().to_owned(),
                version: route.version,
            },
            data: record.value.data,
            revision: record.revision,
            expires_at_unix_ms: record
                .value
                .expires_at
                .ok_or(InteractionError::InvalidState)?
                .0,
        })
    }
}

impl InteractionStateStore for FrameworkStateStore {
    fn restore<'a>(&'a self, kind: InteractionKind, custom_id: &'a str) -> RestoreFuture<'a> {
        Box::pin(async move {
            let state = self.load_state(kind, custom_id).await?;
            Ok(RestoredState {
                route: state.route,
                data: state.data,
            })
        })
    }

    fn load<'a>(
        &'a self,
        kind: InteractionKind,
        custom_id: &'a str,
    ) -> StateFuture<'a, StoredState> {
        Box::pin(self.load_state(kind, custom_id))
    }

    fn create<'a>(
        &'a self,
        kind: InteractionKind,
        route: &'a Route,
        data: Vec<u8>,
        expires_at_unix_ms: u64,
    ) -> StateFuture<'a, CreatedState> {
        Box::pin(async move {
            let state_id = StateId::random().map_err(framework_state_error)?;
            self.create_with_id(kind, route, state_id, data, expires_at_unix_ms)
                .await
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        kind: InteractionKind,
        custom_id: &'a str,
        expected_revision: u64,
        data: Vec<u8>,
        expires_at_unix_ms: u64,
    ) -> StateFuture<'a, u64> {
        Box::pin(async move {
            let route = self.authenticated_route(kind, custom_id)?;
            self.interactions(kind)
                .compare_and_swap_route(
                    &route,
                    expected_revision,
                    data,
                    Timestamp(expires_at_unix_ms),
                )
                .await
                .map_err(framework_state_error)
        })
    }

    fn delete<'a>(
        &'a self,
        kind: InteractionKind,
        custom_id: &'a str,
        expected_revision: u64,
    ) -> StateFuture<'a, bool> {
        Box::pin(async move {
            let route = self.authenticated_route(kind, custom_id)?;
            self.interactions(kind)
                .delete_route(&route, Some(expected_revision))
                .await
                .map_err(framework_state_error)
        })
    }
}

fn framework_state_error(error: StateError) -> InteractionError {
    match error {
        StateError::InvalidLabel
        | StateError::SensitiveLabel
        | StateError::InvalidVersion
        | StateError::InvalidCustomId
        | StateError::Tampered => InteractionError::InvalidId,
        StateError::UnsupportedVersion => InteractionError::UnsupportedVersion,
        StateError::InvalidExpiry => InteractionError::InvalidExpiry,
        StateError::Conflict => InteractionError::Conflict,
        StateError::ValueTooLarge | StateError::CapacityExceeded => {
            InteractionError::CapacityExceeded
        }
        StateError::IncompatibleSchema | StateError::InvalidState | StateError::Encoding => {
            InteractionError::InvalidState
        }
        StateError::UnknownOutcome => InteractionError::UnknownOutcome,
        StateError::ClockUnavailable
        | StateError::ClockRegressed
        | StateError::RevisionExhausted
        | StateError::DurabilityRequired
        | StateError::BackendUnavailable
        | StateError::RandomUnavailable => InteractionError::StoreUnavailable,
    }
}

/// Persisted description of a future event handler, never an executable Future.
/// A dispatcher must restore this record, authorize/filter the event and invoke
/// resume_handler. Delivery, claims/fencing and exactly-once execution are not
/// implemented here. Dropping a local wait or draining does not cancel a record;
/// cancel explicitly. Storage schema and route version are independent.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub route: CustomId,
    pub event: PublicLabel,
    pub resume_handler: PublicLabel,
    pub expires_at: Timestamp,
    pub checkpoint: Vec<u8>,
}
impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("route", &self.route)
            .field("event", &self.event)
            .field("resume_handler", &self.resume_handler)
            .field("expires_at", &self.expires_at)
            .field("checkpoint_len", &self.checkpoint.len())
            .finish()
    }
}
impl Subscription {
    /// Also supports volatile backends for local development. This does not
    /// promise persistence; use durable_wait to require a durable backend.
    pub async fn save(
        &self,
        store: &dyn StateStore,
        scope: StoreScope,
        schema: SchemaVersion,
        condition: PutCondition,
    ) -> Result<u64, StateError> {
        if self.route.version == 0 {
            return Err(StateError::InvalidVersion);
        }
        let data = serde_json::to_vec(self).map_err(|_| StateError::Encoding)?;
        store
            .put(
                self.route.key(scope, StateKind::Subscription),
                StateValue {
                    schema,
                    expires_at: Some(self.expires_at),
                    data,
                },
                condition,
            )
            .await
    }
    pub async fn durable_wait(
        &self,
        store: &dyn StateStore,
        scope: StoreScope,
        schema: SchemaVersion,
        condition: PutCondition,
    ) -> Result<u64, StateError> {
        if store.durability() != Durability::Durable {
            return Err(StateError::DurabilityRequired);
        }
        self.save(store, scope, schema, condition).await
    }
    pub async fn restore(
        store: &dyn StateStore,
        scope: StoreScope,
        route: &CustomId,
        reader: SchemaVersion,
    ) -> Result<Option<Self>, StateError> {
        if route.version == 0 {
            return Err(StateError::InvalidVersion);
        }
        let Some(record) = store
            .get(&route.key(scope, StateKind::Subscription), reader)
            .await?
        else {
            return Ok(None);
        };
        Self::decode_record(&record, route).map(Some)
    }
    pub async fn cancel(
        store: &dyn StateStore,
        scope: StoreScope,
        route: &CustomId,
        expected_revision: Option<u64>,
    ) -> Result<bool, StateError> {
        if route.version == 0 {
            return Err(StateError::InvalidVersion);
        }
        // Cancellation validates identity/expiry, not application checkpoint
        // compatibility: it never executes or exposes the checkpoint. Reading
        // for resumption must use restore with the real reader schema.
        let key = route.key(scope, StateKind::Subscription);
        let Some(record) = store
            .get(
                &key,
                SchemaVersion {
                    version: u16::MAX,
                    compatible_since: 1,
                },
            )
            .await?
        else {
            return if expected_revision.is_some() {
                Err(StateError::Conflict)
            } else {
                Ok(false)
            };
        };
        Self::decode_record(&record, route)?;
        if expected_revision.is_some_and(|revision| revision != record.revision) {
            return Err(StateError::Conflict);
        }
        store.delete(&key, Some(record.revision)).await
    }
    fn decode_record(record: &StateRecord, route: &CustomId) -> Result<Self, StateError> {
        let value: Self =
            serde_json::from_slice(&record.value.data).map_err(|_| StateError::InvalidState)?;
        if value.route != *route || record.value.expires_at != Some(value.expires_at) {
            return Err(StateError::InvalidState);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitCancelled {
    Draining,
    SignalClosed,
}

/// Generation-local wait: cancellation wins when both branches are ready.
/// Use a generation-scoped watch channel; stop admission and set true at the
/// beginning of drain. The sender must keep true once drain has started.
/// Closing the signal fails closed. Dropping this future drops the local wait,
/// and never deletes any Subscription. Callers may wrap this in a timeout.
pub async fn wait_for<F: Future>(
    future: F,
    mut drain: watch::Receiver<bool>,
) -> Result<F::Output, WaitCancelled> {
    let cancelled = async move {
        loop {
            if *drain.borrow_and_update() {
                return WaitCancelled::Draining;
            }
            if drain.changed().await.is_err() {
                return WaitCancelled::SignalClosed;
            }
        }
    };
    tokio::select! {
        biased;
        reason = cancelled => Err(reason),
        output = future => Ok(output),
    }
}
