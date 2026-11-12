//! Bounded durable Subscription delivery on an atomic [`StateStore`].
//!
//! Open the same queue ID/scope/namespace/limits after restart, activate a trusted
//! generation, register descriptors, enqueue authenticated events, then claim and
//! ACK them. The queue, descriptors, receipts and deduplication tombstones occupy
//! one CAS record: no separate index can commit without its delivery. This is an
//! opt-in spool, not a write on every Gateway event. FileStateStore's local-file
//! durability, clock and capacity limitations still apply. No background task,
//! transport, Worker wiring, distributed ownership or consensus is implemented.
//!
//! `activate` is called ONLY after the host's existing ownership/quorum admission.
//! Its epoch/deadline are fencing metadata, not proof of authorization. The host
//! must still fence execution and stop work before its lease expires. Expiry does
//! not physically stop an old handler. At-least-once applies to committed internal
//! deliveries until subscription expiry, not exactly-once application effects.
//!
//! Each event needs a stable acceptance ID to reconcile an uncertain enqueue; a
//! semantic deduplication key is optional (not all Discord events have one).
//! Deduplication is per subscription, retained through ACK until expiry. Full
//! queues reject new work. Never store credentials, tokens or executable Futures
//! in event/checkpoint bytes; they are application-owned data, not encrypted here.
//! ACK checkpoints must preserve the registration's application schema. Reader
//! compatibility is checked, but changing that schema/migrating bytes is not an
//! API provided by this queue; route versions also require explicit acceptance.
//!
//! Before any non-idempotent external send, persist `begin_external`. It records
//! an Unknown operation *before* sending; neither timeout, restart, expiry, drain
//! nor cancellation clears it or makes it retryable. Only a live, correlated
//! confirmed response may use `confirm_external`. Administrative reconciliation,
//! authenticated audit and external REST execution are deliberately left to the
//! host (see docs/local-response-management.md). Unknown write outcomes/cancelled
//! futures require rereading; this module never blindly retries a backend write.
use crate::{
    protocol::{GenerationId, OwnershipEpoch},
    state_store::{
        Durability, PublicLabel, PutCondition, SchemaVersion, StateClock, StateError, StateId,
        StateKey, StateKind, StateRecord, StateStore, StateValue, StoreScope, Subscription,
        Timestamp,
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, sync::Arc, sync::Mutex, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionError {
    State(StateError),
    InvalidConfig,
    InvalidRecord,
    StaleOwner,
    Draining,
    MissingSubscription,
    Expired,
    SubscriptionConflict,
    DeduplicationConflict,
    StaleDelivery,
    ExternalOutcomeUnknown,
    Busy,
    Capacity,
}
impl From<StateError> for SubscriptionError {
    fn from(error: StateError) -> Self {
        Self::State(error)
    }
}
impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "subscription error: {self:?}")
    }
}
impl std::error::Error for SubscriptionError {}
type Result<T> = std::result::Result<T, SubscriptionError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionLimits {
    pub max_subscriptions: usize,
    pub max_deliveries: usize,
    pub max_payload_bytes: usize,
    pub max_checkpoint_bytes: usize,
    pub max_record_bytes: usize,
    pub visibility_timeout_ms: u64,
    pub store_timeout_ms: u64,
}
impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_subscriptions: 128,
            max_deliveries: 1024,
            max_payload_bytes: 64 * 1024,
            max_checkpoint_bytes: 64 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
            visibility_timeout_ms: 30_000,
            store_timeout_ms: 5000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryOwner {
    pub generation: GenerationId,
    pub epoch: OwnershipEpoch,
}
impl DeliveryOwner {
    fn valid(self) -> bool {
        self.generation.0 != 0 && self.epoch.0 != 0
    }
    fn newer_than(self, old: Self) -> bool {
        self.epoch.0 > old.epoch.0
            || (self.epoch == old.epoch && self.generation.0 > old.generation.0)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionEvent {
    pub route: crate::state_store::CustomId,
    pub event: PublicLabel,
    pub event_id: StateId,
    pub deduplication_key: Option<StateId>,
    pub payload: Vec<u8>,
}
impl fmt::Debug for SubscriptionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionEvent")
            .field("route", &self.route)
            .field("event", &self.event)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueResult {
    Queued { sequence: u64 },
    Duplicate { sequence: u64 },
}

/// Queue-, attempt-, owner- and deadline-bound receipt. Not authorization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReceipt {
    queue: StateKey,
    sequence: u64,
    attempt: u64,
    owner: DeliveryOwner,
    expires_at: Timestamp,
}
impl DeliveryReceipt {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn attempt(&self) -> u64 {
        self.attempt
    }
    pub fn owner(&self) -> DeliveryOwner {
        self.owner
    }
    pub fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Delivery {
    pub receipt: DeliveryReceipt,
    pub subscription: Subscription,
    pub event_id: StateId,
    pub deduplication_key: Option<StateId>,
    pub payload: Vec<u8>,
}
impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("receipt", &self.receipt)
            .field("subscription", &self.subscription)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownDelivery {
    pub receipt: DeliveryReceipt,
    pub operation_id: StateId,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    descriptor: Subscription,
    schema: SchemaVersion,
    cancelled: bool,
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum DeliveryState {
    Pending,
    InFlight(DeliveryReceipt),
    Acked {
        receipt: DeliveryReceipt,
        operation_id: Option<StateId>,
        checkpoint: Vec<u8>,
    },
    Unknown {
        receipt: DeliveryReceipt,
        operation_id: StateId,
    },
}
impl DeliveryState {
    fn receipt(&self) -> Option<&DeliveryReceipt> {
        match self {
            Self::Pending => None,
            Self::InFlight(receipt)
            | Self::Acked { receipt, .. }
            | Self::Unknown { receipt, .. } => Some(receipt),
        }
    }
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    sequence: u64,
    attempts: u64,
    event: SubscriptionEvent,
    state: DeliveryState,
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    owner: DeliveryOwner,
    until: Timestamp,
    draining: bool,
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueState {
    format: u16,
    key: StateKey,
    limits: SubscriptionLimits,
    last_now: Timestamp,
    next_sequence: u64,
    active: Option<Active>,
    registrations: Vec<Registration>,
    deliveries: Vec<Entry>,
}

/// Handles may overlap across generations; every mutation reloads and CASes.
/// CAS conflicts are returned to the caller for an explicit fresh decision.
#[derive(Clone)]
pub struct SubscriptionQueue {
    store: Arc<dyn StateStore>,
    clock: Arc<dyn StateClock>,
    key: StateKey,
    limits: SubscriptionLimits,
    last_seen: Arc<Mutex<Option<Timestamp>>>,
}
impl fmt::Debug for SubscriptionQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SubscriptionQueue([REDACTED])")
    }
}
impl SubscriptionQueue {
    pub async fn open(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn StateClock>,
        scope: StoreScope,
        namespace: PublicLabel,
        queue_id: StateId,
        limits: SubscriptionLimits,
    ) -> Result<Self> {
        if store.durability() != Durability::Durable {
            return Err(StateError::DurabilityRequired.into());
        }
        if limits.max_subscriptions == 0
            || limits.max_deliveries == 0
            || limits.max_payload_bytes == 0
            || limits.max_checkpoint_bytes == 0
            || limits.max_record_bytes == 0
            || limits.visibility_timeout_ms == 0
            || limits.store_timeout_ms == 0
        {
            return Err(SubscriptionError::InvalidConfig);
        }
        let queue = Self {
            store,
            clock,
            key: StateKey::new(
                scope,
                namespace,
                PublicLabel::new("subscription_queue_v1")?,
                StateKind::Application,
                queue_id,
            ),
            limits,
            last_seen: Arc::new(Mutex::new(None)),
        };
        if queue.read_record().await?.is_none() {
            let state = QueueState {
                format: 1,
                key: queue.key.clone(),
                limits,
                last_now: queue.clock.now()?,
                next_sequence: 0,
                active: None,
                registrations: Vec::new(),
                deliveries: Vec::new(),
            };
            match queue.write(&state, PutCondition::IfAbsent).await {
                Ok(_) | Err(SubscriptionError::State(StateError::Conflict)) => {}
                Err(error) => return Err(error),
            }
        }
        queue.load().await?;
        Ok(queue)
    }

    /// Set/renew a host-approved owner deadline. Renewing cannot undo drain or
    /// revive an expired owner; a restart/switch must use a newer generation.
    pub async fn activate(&self, owner: DeliveryOwner, lease_until: Timestamp) -> Result<()> {
        self.update(|state, now| {
            if !owner.valid() || lease_until <= now {
                return Err(SubscriptionError::StaleOwner);
            }
            if let Some(active) = &state.active {
                if active.owner == owner {
                    if active.draining {
                        return Err(SubscriptionError::Draining);
                    }
                    if active.until <= now || lease_until < active.until {
                        return Err(SubscriptionError::StaleOwner);
                    }
                } else if !owner.newer_than(active.owner) {
                    return Err(SubscriptionError::StaleOwner);
                }
            }
            state.active = Some(Active {
                owner,
                until: lease_until,
                draining: false,
            });
            Ok(())
        })
        .await
    }

    /// Stops new registration/enqueue/claim durably, without deleting work or
    /// releasing executing claims. Existing exact receipts may still finish.
    pub async fn begin_drain(&self, owner: DeliveryOwner) -> Result<()> {
        self.update(|state, _| {
            let active = state.active.as_mut().ok_or(SubscriptionError::StaleOwner)?;
            if active.owner != owner {
                return Err(SubscriptionError::StaleOwner);
            }
            active.draining = true;
            Ok(())
        })
        .await
    }

    /// New descriptors only; a repeated identical registration is idempotent.
    /// This adopts a descriptor, not a legacy per-route save's separate record.
    pub async fn register(
        &self,
        owner: DeliveryOwner,
        subscription: Subscription,
        reader_schema: SchemaVersion,
    ) -> Result<bool> {
        self.update(|state, now| {
            active(state, owner, now, true)?;
            if subscription.route.namespace != self.key.namespace || subscription.route.version == 0
            {
                return Err(SubscriptionError::SubscriptionConflict);
            }
            if subscription.expires_at <= now {
                return Err(SubscriptionError::Expired);
            }
            if subscription.checkpoint.len() > self.limits.max_checkpoint_bytes {
                return Err(SubscriptionError::Capacity);
            }
            if let Some(old) = state.registrations.iter().find(|registration| {
                registration.descriptor.route.state_id == subscription.route.state_id
            }) {
                return if !old.cancelled
                    && old.descriptor == subscription
                    && old.schema == reader_schema
                {
                    Ok(false)
                } else {
                    Err(SubscriptionError::SubscriptionConflict)
                };
            }
            if state.registrations.len() >= self.limits.max_subscriptions {
                return Err(SubscriptionError::Capacity);
            }
            state.registrations.push(Registration {
                descriptor: subscription,
                schema: reader_schema,
                cancelled: false,
            });
            Ok(true)
        })
        .await
    }

    pub async fn registrations(&self, reader: SchemaVersion) -> Result<Vec<Subscription>> {
        let (state, _, now) = self.load().await?;
        state
            .registrations
            .into_iter()
            .filter(|item| !item.cancelled && item.descriptor.expires_at > now)
            .map(|item| {
                if !reader.can_read(item.schema) {
                    return Err(StateError::IncompatibleSchema.into());
                }
                Ok(item.descriptor)
            })
            .collect()
    }

    /// The host has already authenticated and authorized the event. Matching
    /// checks the exact registered route and event label, not Discord permission.
    pub async fn enqueue(
        &self,
        owner: DeliveryOwner,
        event: SubscriptionEvent,
    ) -> Result<EnqueueResult> {
        self.update(|state, now| {
            active(state, owner, now, true)?;
            let registration = registration(state, &event.route)?;
            if registration.cancelled || registration.descriptor.expires_at <= now {
                return Err(SubscriptionError::Expired);
            }
            if registration.descriptor.event != event.event {
                return Err(SubscriptionError::SubscriptionConflict);
            }
            if event.payload.len() > self.limits.max_payload_bytes {
                return Err(SubscriptionError::Capacity);
            }
            for previous in &state.deliveries {
                if previous.event.route == event.route
                    && (previous.event.event_id == event.event_id
                        || (event.deduplication_key.is_some()
                            && previous.event.deduplication_key == event.deduplication_key))
                {
                    if previous.event.payload != event.payload
                        || previous.event.event != event.event
                        || previous.event.deduplication_key != event.deduplication_key
                    {
                        return Err(SubscriptionError::DeduplicationConflict);
                    }
                    return Ok(EnqueueResult::Duplicate {
                        sequence: previous.sequence,
                    });
                }
            }
            if state.deliveries.len() >= self.limits.max_deliveries {
                return Err(SubscriptionError::Capacity);
            }
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or(StateError::RevisionExhausted)?;
            let sequence = state.next_sequence;
            state.deliveries.push(Entry {
                sequence,
                attempts: 0,
                event,
                state: DeliveryState::Pending,
            });
            Ok(EnqueueResult::Queued { sequence })
        })
        .await
    }

    /// At most one unacknowledged attempt per subscription is eligible at a
    /// time, preserving checkpoint order. Other subscriptions can progress.
    pub async fn claim(
        &self,
        owner: DeliveryOwner,
        reader: SchemaVersion,
        supported_route_versions: &[u16],
    ) -> Result<Option<Delivery>> {
        let delivery = self
            .update(|state, now| {
                let until = active(state, owner, now, true)?.until;
                let mut seen = HashSet::new();
                for index in 0..state.deliveries.len() {
                    let entry = &state.deliveries[index];
                    if matches!(entry.state, DeliveryState::Acked { .. }) {
                        continue;
                    }
                    if !seen.insert(entry.event.route.state_id) {
                        continue;
                    }
                    let registration = registration(state, &entry.event.route)?;
                    if registration.cancelled || registration.descriptor.expires_at <= now {
                        continue;
                    }
                    match &entry.state {
                        DeliveryState::Unknown { .. } => continue,
                        DeliveryState::InFlight(receipt) if receipt.expires_at > now => continue,
                        _ => {}
                    }
                    if !reader.can_read(registration.schema) {
                        return Err(StateError::IncompatibleSchema.into());
                    }
                    if !supported_route_versions.contains(&registration.descriptor.route.version) {
                        return Err(StateError::UnsupportedVersion.into());
                    }
                    let subscription = registration.descriptor.clone();
                    let deadline = now
                        .0
                        .checked_add(self.limits.visibility_timeout_ms)
                        .map(Timestamp)
                        .ok_or(StateError::InvalidExpiry)?
                        .min(until)
                        .min(subscription.expires_at);
                    let entry = &mut state.deliveries[index];
                    entry.attempts = entry
                        .attempts
                        .checked_add(1)
                        .ok_or(StateError::RevisionExhausted)?;
                    let receipt = DeliveryReceipt {
                        queue: self.key.clone(),
                        sequence: entry.sequence,
                        attempt: entry.attempts,
                        owner,
                        expires_at: deadline,
                    };
                    entry.state = DeliveryState::InFlight(receipt.clone());
                    return Ok(Some(Delivery {
                        receipt,
                        subscription,
                        event_id: entry.event.event_id,
                        deduplication_key: entry.event.deduplication_key,
                        payload: entry.event.payload.clone(),
                    }));
                }
                Ok(None)
            })
            .await?;
        if let Some(delivery) = &delivery {
            // A slow successful write may have consumed the entire lease.
            self.validate_receipt(&delivery.receipt).await?;
        }
        Ok(delivery)
    }

    /// Check an internal delivery before resuming it. This is a point-in-time
    /// check, not external ownership authorization or permission to resend an
    /// Unknown operation. The host still fences each actual side effect.
    pub async fn validate_receipt(&self, receipt: &DeliveryReceipt) -> Result<()> {
        let (state, _, now) = self.load().await?;
        let index = self.receipt_index(&state, receipt)?;
        live(receipt, now)?;
        match &state.deliveries[index].state {
            DeliveryState::InFlight(current) if current == receipt => Ok(()),
            DeliveryState::Unknown { .. } => Err(SubscriptionError::ExternalOutcomeUnknown),
            _ => Err(SubscriptionError::StaleDelivery),
        }
    }

    /// Atomically acknowledges delivery and advances its checkpoint. Repeating
    /// the same ACK is safe and never rewrites a newer checkpoint.
    pub async fn ack(&self, receipt: &DeliveryReceipt, checkpoint: Vec<u8>) -> Result<()> {
        self.finish(receipt, None, checkpoint).await
    }

    /// Call only after the internal handler has stopped and no uncertain
    /// external operation exists. Dropping a Future alone is not such evidence.
    pub async fn nack(&self, receipt: &DeliveryReceipt) -> Result<()> {
        self.update(|state, now| {
            let index = self.receipt_index(state, receipt)?;
            live(receipt, now)?;
            match &state.deliveries[index].state {
                DeliveryState::InFlight(current) if current == receipt => {
                    state.deliveries[index].state = DeliveryState::Pending;
                    Ok(())
                }
                DeliveryState::Unknown { .. } => Err(SubscriptionError::ExternalOutcomeUnknown),
                _ => Err(SubscriptionError::StaleDelivery),
            }
        })
        .await
    }

    /// Commit uncertainty before sending. A lost response to this write means
    /// do not send: reread and reconcile; Unknown is never a new send permit.
    pub async fn begin_external(
        &self,
        receipt: &DeliveryReceipt,
        operation_id: StateId,
    ) -> Result<()> {
        self.update(|state, now| {
            let index = self.receipt_index(state, receipt)?;
            live(receipt, now)?;
            if !matches!(&state.deliveries[index].state, DeliveryState::InFlight(current) if current == receipt) {
                return Err(SubscriptionError::ExternalOutcomeUnknown);
            }
            state.deliveries[index].state = DeliveryState::Unknown {
                receipt: receipt.clone(),
                operation_id,
            };
            Ok(())
        })
        .await?;
        // Unknown is already durable if this fails: do not send or erase it.
        live(receipt, self.observe(None)?)
    }

    /// For a confirmed result of this exact live operation, not an operator
    /// override. Expired Unknowns need a future authenticated/audited workflow.
    pub async fn confirm_external(
        &self,
        receipt: &DeliveryReceipt,
        operation_id: StateId,
        checkpoint: Vec<u8>,
    ) -> Result<()> {
        self.finish(receipt, Some(operation_id), checkpoint).await
    }

    pub async fn unknown_operations(&self) -> Result<Vec<UnknownDelivery>> {
        let (state, _, _) = self.load().await?;
        Ok(state
            .deliveries
            .into_iter()
            .filter_map(|entry| match entry.state {
                DeliveryState::Unknown {
                    receipt,
                    operation_id,
                } => Some(UnknownDelivery {
                    receipt,
                    operation_id,
                }),
                _ => None,
            })
            .collect())
    }

    pub async fn cancel(
        &self,
        owner: DeliveryOwner,
        route: &crate::state_store::CustomId,
    ) -> Result<bool> {
        self.update(|state, now| {
            active(state, owner, now, false)?;
            for entry in state
                .deliveries
                .iter()
                .filter(|entry| &entry.event.route == route)
            {
                match &entry.state {
                    DeliveryState::Unknown { .. } => {
                        return Err(SubscriptionError::ExternalOutcomeUnknown);
                    }
                    DeliveryState::InFlight(receipt) if receipt.expires_at > now => {
                        return Err(SubscriptionError::Busy);
                    }
                    _ => {}
                }
            }
            let Some(registration) = state
                .registrations
                .iter_mut()
                .find(|item| &item.descriptor.route == route)
            else {
                return Ok(false);
            };
            let was_live = !registration.cancelled;
            registration.cancelled = true;
            Ok(was_live)
        })
        .await
    }

    async fn finish(
        &self,
        receipt: &DeliveryReceipt,
        operation: Option<StateId>,
        checkpoint: Vec<u8>,
    ) -> Result<()> {
        self.update(|state, now| {
            if checkpoint.len() > self.limits.max_checkpoint_bytes {
                return Err(SubscriptionError::Capacity);
            }
            let index = self.receipt_index(state, receipt)?;
            match &state.deliveries[index].state {
                DeliveryState::Acked {
                    receipt: current,
                    operation_id,
                    checkpoint: saved,
                } if current == receipt && *operation_id == operation && saved == &checkpoint => {
                    return Ok(());
                }
                DeliveryState::InFlight(current) if current == receipt && operation.is_none() => {}
                DeliveryState::Unknown {
                    receipt: current,
                    operation_id,
                } if current == receipt && Some(*operation_id) == operation => {}
                DeliveryState::Unknown { .. } => {
                    return Err(SubscriptionError::ExternalOutcomeUnknown);
                }
                _ => return Err(SubscriptionError::StaleDelivery),
            }
            live(receipt, now)?;
            let route = state.deliveries[index].event.route.clone();
            let registration = state
                .registrations
                .iter_mut()
                .find(|item| item.descriptor.route == route)
                .ok_or(SubscriptionError::InvalidRecord)?;
            registration.descriptor.checkpoint = checkpoint.clone();
            state.deliveries[index].state = DeliveryState::Acked {
                receipt: receipt.clone(),
                operation_id: operation,
                checkpoint,
            };
            Ok(())
        })
        .await
    }

    fn receipt_index(&self, state: &QueueState, receipt: &DeliveryReceipt) -> Result<usize> {
        if receipt.queue != self.key {
            return Err(SubscriptionError::StaleDelivery);
        }
        state
            .deliveries
            .iter()
            .position(|entry| entry.sequence == receipt.sequence)
            .ok_or(SubscriptionError::StaleDelivery)
    }

    async fn load(&self) -> Result<(QueueState, u64, Timestamp)> {
        let record = self
            .read_record()
            .await?
            .ok_or(SubscriptionError::InvalidRecord)?;
        if record.value.expires_at.is_some()
            || record.value.schema != schema()
            || record.value.data.len() > self.limits.max_record_bytes
        {
            return Err(SubscriptionError::InvalidRecord);
        }
        let state: QueueState = serde_json::from_slice(&record.value.data)
            .map_err(|_| SubscriptionError::InvalidRecord)?;
        self.validate(&state)?;
        let now = self.observe(Some(state.last_now))?;
        Ok((state, record.revision, now))
    }

    fn observe(&self, persisted: Option<Timestamp>) -> Result<Timestamp> {
        let mut seen = self
            .last_seen
            .lock()
            .map_err(|_| StateError::BackendUnavailable)?;
        let now = self.clock.now()?;
        if persisted.is_some_and(|previous| now < previous)
            || seen.is_some_and(|previous| now < previous)
        {
            return Err(StateError::ClockRegressed.into());
        }
        *seen = Some(now);
        Ok(now)
    }

    async fn read_record(&self) -> Result<Option<StateRecord>> {
        Ok(tokio::time::timeout(
            Duration::from_millis(self.limits.store_timeout_ms),
            self.store.get(&self.key, schema()),
        )
        .await
        .map_err(|_| StateError::BackendUnavailable)??)
    }

    async fn update<T>(
        &self,
        change: impl FnOnce(&mut QueueState, Timestamp) -> Result<T>,
    ) -> Result<T> {
        let (mut state, revision, now) = self.load().await?;
        let before = state.clone();
        // Unknown operation evidence survives both descriptor TTL and drain.
        let unknown: HashSet<_> = state
            .deliveries
            .iter()
            .filter_map(|entry| {
                matches!(entry.state, DeliveryState::Unknown { .. })
                    .then_some(entry.event.route.state_id)
            })
            .collect();
        state.registrations.retain(|item| {
            item.descriptor.expires_at > now || unknown.contains(&item.descriptor.route.state_id)
        });
        let retained: HashSet<_> = state
            .registrations
            .iter()
            .map(|item| item.descriptor.route.state_id)
            .collect();
        state
            .deliveries
            .retain(|entry| retained.contains(&entry.event.route.state_id));
        let output = change(&mut state, now)?;
        if state != before {
            state.last_now = now;
            self.write(&state, PutCondition::IfRevision(revision))
                .await?;
        }
        Ok(output)
    }

    async fn write(&self, state: &QueueState, condition: PutCondition) -> Result<u64> {
        self.validate(state)?;
        let data = serde_json::to_vec(state).map_err(|_| StateError::Encoding)?;
        if data.len() > self.limits.max_record_bytes {
            return Err(SubscriptionError::Capacity);
        }
        Ok(tokio::time::timeout(
            Duration::from_millis(self.limits.store_timeout_ms),
            self.store.put(
                self.key.clone(),
                StateValue {
                    schema: schema(),
                    expires_at: None,
                    data,
                },
                condition,
            ),
        )
        .await
        .map_err(|_| StateError::UnknownOutcome)??)
    }

    fn validate(&self, state: &QueueState) -> Result<()> {
        if state.format != 1
            || state.key != self.key
            || state.limits != self.limits
            || state.registrations.len() > self.limits.max_subscriptions
            || state.deliveries.len() > self.limits.max_deliveries
            || state
                .active
                .as_ref()
                .is_some_and(|item| !item.owner.valid())
            || (state.active.is_none()
                && (!state.registrations.is_empty() || !state.deliveries.is_empty()))
        {
            return Err(SubscriptionError::InvalidRecord);
        }
        let mut ids = HashSet::new();
        for item in &state.registrations {
            let descriptor = &item.descriptor;
            if descriptor.route.namespace != self.key.namespace
                || descriptor.route.version == 0
                || descriptor.checkpoint.len() > self.limits.max_checkpoint_bytes
                || !ids.insert(descriptor.route.state_id)
            {
                return Err(SubscriptionError::InvalidRecord);
            }
        }
        let mut previous = 0;
        let mut events = HashSet::new();
        let mut dedup = HashSet::new();
        for entry in &state.deliveries {
            let registration = registration(state, &entry.event.route)
                .map_err(|_| SubscriptionError::InvalidRecord)?;
            if entry.sequence <= previous
                || entry.sequence > state.next_sequence
                || entry.event.event != registration.descriptor.event
                || entry.event.payload.len() > self.limits.max_payload_bytes
                || !events.insert((entry.event.route.state_id, entry.event.event_id))
                || entry
                    .event
                    .deduplication_key
                    .is_some_and(|key| !dedup.insert((entry.event.route.state_id, key)))
            {
                return Err(SubscriptionError::InvalidRecord);
            }
            previous = entry.sequence;
            if let Some(receipt) = entry.state.receipt()
                && (receipt.queue != self.key
                    || receipt.sequence != entry.sequence
                    || receipt.attempt != entry.attempts
                    || receipt.attempt == 0
                    || !receipt.owner.valid()
                    || receipt.expires_at.0 == 0
                    || state
                        .active
                        .as_ref()
                        .is_none_or(|active| receipt.owner.newer_than(active.owner))
                    || receipt.expires_at > registration.descriptor.expires_at)
            {
                return Err(SubscriptionError::InvalidRecord);
            }
            if matches!(&entry.state, DeliveryState::Acked { checkpoint, .. } if checkpoint.len() > self.limits.max_checkpoint_bytes)
            {
                return Err(SubscriptionError::InvalidRecord);
            }
        }
        Ok(())
    }
}

fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).expect("fixed schema")
}
fn active(
    state: &QueueState,
    owner: DeliveryOwner,
    now: Timestamp,
    admitting: bool,
) -> Result<&Active> {
    let active = state.active.as_ref().ok_or(SubscriptionError::StaleOwner)?;
    if active.owner != owner || active.until <= now {
        return Err(SubscriptionError::StaleOwner);
    }
    if admitting && active.draining {
        return Err(SubscriptionError::Draining);
    }
    Ok(active)
}
fn registration<'a>(
    state: &'a QueueState,
    route: &crate::state_store::CustomId,
) -> Result<&'a Registration> {
    state
        .registrations
        .iter()
        .find(|item| &item.descriptor.route == route)
        .ok_or(SubscriptionError::MissingSubscription)
}
fn live(receipt: &DeliveryReceipt, now: Timestamp) -> Result<()> {
    if receipt.expires_at <= now {
        return Err(SubscriptionError::StaleDelivery);
    }
    Ok(())
}
