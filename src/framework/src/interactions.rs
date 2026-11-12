//! Transport-independent interaction routing and an optional state adapter API.
//! Discord/Worker transports are not connected.
//!
//! The host must authenticate IDs, isolate bot/shard and interaction kind, check
//! expiry and bind the stored envelope to the decoded route before returning it.
//! A route or state key is not user authorization or protection against replay.
use crate::{BotResult, CommandFuture};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt, future::Future, pin::Pin, sync::Mutex};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InteractionKind {
    Component,
    Modal,
}

/// Component covers both buttons and selects; their values are passed by the host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Route {
    pub namespace: String,
    pub name: String,
    pub version: u16,
}

/// Returned only after authenticating the ID and validating its stored envelope.
pub struct RestoredState {
    pub route: Route,
    pub data: Vec<u8>,
}

/// One validated read, including the revision to use for a conditional write.
/// The expiry is an exclusive Unix-millisecond deadline, not a relative duration.
/// Reading state does not reserve it; another handler may update it immediately.
pub struct StoredState<T = Vec<u8>> {
    pub route: Route,
    pub data: T,
    pub revision: u64,
    pub expires_at_unix_ms: u64,
}
impl<T> fmt::Debug for StoredState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredState")
            .field("revision", &self.revision)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish_non_exhaustive()
    }
}

/// A newly saved interaction. Use the ID on the outgoing component or modal;
/// do not log it. A successful save alone does not send anything to Discord.
pub struct CreatedState {
    pub custom_id: String,
    pub revision: u64,
    pub expires_at_unix_ms: u64,
}
impl fmt::Debug for CreatedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatedState")
            .field("revision", &self.revision)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionError {
    InvalidId,
    UnsupportedVersion,
    ExpiredOrMissing,
    InvalidState,
    StoreUnavailable,
    UnknownHandler,
    DuplicateHandler,
    HandlerFailed,
    Conflict,
    InvalidExpiry,
    StateOperationsUnsupported,
    CapacityExceeded,
    /// A write may have committed. Reconcile with storage before retrying.
    UnknownOutcome,
}
impl InteractionError {
    /// Safe, fixed text suitable for an adapter's interaction error response.
    pub fn response(self) -> &'static str {
        match self {
            Self::InvalidId => "Invalid or tampered interaction.",
            Self::UnsupportedVersion => "This interaction version is not supported.",
            Self::ExpiredOrMissing => "This interaction has expired or is no longer available.",
            Self::InvalidState => "This interaction's saved state is invalid.",
            Self::StoreUnavailable => "Interaction state is temporarily unavailable.",
            Self::UnknownHandler => "This interaction is not supported.",
            Self::DuplicateHandler => "Interaction routing is unavailable.",
            Self::HandlerFailed => "Interaction processing failed.",
            Self::Conflict => "Interaction state changed; reload it before updating.",
            Self::InvalidExpiry => "The interaction expiry is invalid.",
            Self::StateOperationsUnsupported => {
                "This interaction store does not support state operations."
            }
            Self::CapacityExceeded => "Interaction state exceeds the storage limit.",
            Self::UnknownOutcome => "The interaction state write outcome is unknown.",
        }
    }
}
impl std::fmt::Display for InteractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.response())
    }
}
impl std::error::Error for InteractionError {}

pub type StateFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, InteractionError>> + Send + 'a>>;
pub type RestoreFuture<'a> = StateFuture<'a, RestoredState>;
/// Adapter boundary for a shared StateStore and authenticated custom ID codec.
/// Implementations must reject malformed/tampered IDs, unsupported wire versions,
/// missing or expired records, incompatible schemas and route-envelope mismatches.
/// Missing and expired records may be indistinguishable. Never expose ID or payload
/// in errors. External storage and its durability are the host's responsibility.
///
/// Bind an adapter to a trusted Bot/Shard and application namespace at startup;
/// never select its scope from an untrusted custom ID. The host supplies the
/// interaction kind and decoded values from its authenticated transport, admits
/// only active-generation work, and tracks handler/reply completion during drain.
/// No Bot token is required by this interface. MACs do not authorize a user.
///
/// State operations default to an explicit error so existing restore-only
/// implementations remain usable. Implementations must verify MAC, scope, kind,
/// namespace, schema, expiry and envelope identity on **every** load or mutation.
/// CAS/deletion must atomically bind the validated envelope's revision to the
/// mutation. Never revive an expired record during CAS. Report ambiguous remote
/// writes as [`InteractionError::UnknownOutcome`], without automatic retries.
/// These futures must not perform blocking storage I/O.
pub trait InteractionStateStore: Send + Sync {
    fn restore<'a>(&'a self, kind: InteractionKind, custom_id: &'a str) -> RestoreFuture<'a>;

    fn load<'a>(
        &'a self,
        _kind: InteractionKind,
        _custom_id: &'a str,
    ) -> StateFuture<'a, StoredState> {
        Box::pin(async { Err(InteractionError::StateOperationsUnsupported) })
    }

    /// Allocate a fresh opaque ID and insert only if absent. Return the revision
    /// acknowledged by this write, never a revision obtained by a subsequent read.
    /// Validate/sign the ID before storing, and reject already expired deadlines.
    fn create<'a>(
        &'a self,
        _kind: InteractionKind,
        _route: &'a Route,
        _data: Vec<u8>,
        _expires_at_unix_ms: u64,
    ) -> StateFuture<'a, CreatedState> {
        Box::pin(async { Err(InteractionError::StateOperationsUnsupported) })
    }

    /// Replace payload and expiry only while the authenticated record has the
    /// expected revision. A failed condition preserves the current record.
    fn compare_and_swap<'a>(
        &'a self,
        _kind: InteractionKind,
        _custom_id: &'a str,
        _expected_revision: u64,
        _data: Vec<u8>,
        _expires_at_unix_ms: u64,
    ) -> StateFuture<'a, u64> {
        Box::pin(async { Err(InteractionError::StateOperationsUnsupported) })
    }

    fn delete<'a>(
        &'a self,
        _kind: InteractionKind,
        _custom_id: &'a str,
        _expected_revision: u64,
    ) -> StateFuture<'a, bool> {
        Box::pin(async { Err(InteractionError::StateOperationsUnsupported) })
    }
}

pub struct InteractionContext<'a> {
    data: &'a [u8],
    values: &'a [String],
    replies: &'a Mutex<Vec<String>>,
    store: &'a dyn InteractionStateStore,
    kind: InteractionKind,
    custom_id: &'a str,
    route: &'a Route,
}
impl InteractionContext<'_> {
    /// Decode the snapshot validated when dispatch began. Use `load_state` for a
    /// fresh value and its CAS revision. Schema migrations are host-owned.
    pub fn state<T: DeserializeOwned>(&self) -> Result<T, InteractionError> {
        serde_json::from_slice(self.data).map_err(|_| InteractionError::InvalidState)
    }
    pub fn route(&self) -> &Route {
        self.route
    }
    pub fn kind(&self) -> InteractionKind {
        self.kind
    }
    /// Reauthenticate the current custom ID and read its live payload and expiry.
    pub async fn load_state<T: DeserializeOwned>(
        &self,
    ) -> Result<StoredState<T>, InteractionError> {
        let state = self.store.load(self.kind, self.custom_id).await?;
        if state.route != *self.route {
            return Err(InteractionError::InvalidState);
        }
        Ok(StoredState {
            data: serde_json::from_slice(&state.data)
                .map_err(|_| InteractionError::InvalidState)?,
            route: state.route,
            revision: state.revision,
            expires_at_unix_ms: state.expires_at_unix_ms,
        })
    }
    /// Save JSON and its TTL deadline using the revision from `load_state` or a
    /// previous successful save. On conflict, explicitly reload and reconsider
    /// the operation. This does not make external side effects exactly once.
    pub async fn save_state<T: Serialize + Sync + ?Sized>(
        &self,
        data: &T,
        expected_revision: u64,
        expires_at_unix_ms: u64,
    ) -> Result<u64, InteractionError> {
        let data = serde_json::to_vec(data).map_err(|_| InteractionError::InvalidState)?;
        self.store
            .compare_and_swap(
                self.kind,
                self.custom_id,
                expected_revision,
                data,
                expires_at_unix_ms,
            )
            .await
    }
    /// Create another button/select or modal state in this handler's namespace.
    /// Route labels are public, static application metadata, never user secrets.
    /// The adapter supplies a fresh ID; it may fail if OS entropy is unavailable.
    pub async fn create_state<T: Serialize + Sync + ?Sized>(
        &self,
        kind: InteractionKind,
        name: &str,
        version: u16,
        data: &T,
        expires_at_unix_ms: u64,
    ) -> Result<CreatedState, InteractionError> {
        let route = Route {
            namespace: self.route.namespace.clone(),
            name: name.to_owned(),
            version,
        };
        let data = serde_json::to_vec(data).map_err(|_| InteractionError::InvalidState)?;
        self.store
            .create(kind, &route, data, expires_at_unix_ms)
            .await
    }
    /// Explicitly remove this live state if it still has the expected revision.
    /// Draining or dropping a handler never invokes this method automatically.
    pub async fn delete_state(&self, expected_revision: u64) -> Result<bool, InteractionError> {
        self.store
            .delete(self.kind, self.custom_id, expected_revision)
            .await
    }
    pub fn values(&self) -> &[String] {
        self.values
    }
    pub async fn reply(&self, message: impl Into<String>) -> BotResult<()> {
        self.replies
            .lock()
            .map_err(|_| "reply sink poisoned")?
            .push(message.into());
        Ok(())
    }
}

#[doc(hidden)]
pub struct InteractionHandler {
    pub namespace: &'static str,
    pub name: &'static str,
    pub version: u16,
    pub kind: InteractionKind,
    pub handler: for<'a> fn(InteractionContext<'a>) -> CommandFuture<'a>,
}
inventory::collect!(InteractionHandler);

/// Each instance independently discovers linked handlers; no in-memory wait is migrated.
pub struct Registry {
    handlers: Vec<&'static InteractionHandler>,
}
impl Registry {
    pub fn new() -> Result<Self, InteractionError> {
        let mut handlers: Vec<_> = inventory::iter::<InteractionHandler>.into_iter().collect();
        handlers.sort_unstable_by_key(|h| (h.kind, h.namespace, h.name, h.version));
        if handlers.windows(2).any(|p| {
            (p[0].kind, p[0].namespace, p[0].name, p[0].version)
                == (p[1].kind, p[1].namespace, p[1].name, p[1].version)
        }) {
            return Err(InteractionError::DuplicateHandler);
        }
        Ok(Self { handlers })
    }
    /// Returns an explicit safe error for the host to acknowledge to the user.
    /// Handler details are hidden, but a propagated UnknownOutcome remains
    /// distinct so the host can reconcile an ambiguous write instead of retrying.
    pub async fn dispatch(
        &self,
        store: &dyn InteractionStateStore,
        kind: InteractionKind,
        custom_id: &str,
        values: &[String],
        replies: &Mutex<Vec<String>>,
    ) -> Result<(), InteractionError> {
        let state = store.restore(kind, custom_id).await?;
        let route = &state.route;
        let handler = self.handlers.iter().find(|h| {
            h.kind == kind
                && h.namespace == route.namespace
                && h.name == route.name
                && h.version == route.version
        });
        let Some(handler) = handler else {
            return Err(
                if self.handlers.iter().any(|h| {
                    h.kind == kind && h.namespace == route.namespace && h.name == route.name
                }) {
                    InteractionError::UnsupportedVersion
                } else {
                    InteractionError::UnknownHandler
                },
            );
        };
        (handler.handler)(InteractionContext {
            data: &state.data,
            values,
            replies,
            store,
            kind,
            custom_id,
            route,
        })
        .await
        .map_err(|error| {
            if error.downcast_ref::<InteractionError>() == Some(&InteractionError::UnknownOutcome) {
                InteractionError::UnknownOutcome
            } else {
                InteractionError::HandlerFailed
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitCancelled {
    Draining,
    SignalClosed,
}
/// A generation-local future, never a stored subscription. Drain wins when both
/// branches are ready; an already active drain or closed signal fails closed.
/// The host must stop admission and permanently set its generation's signal to
/// true at drain start. Cancellation drops the local future, not persisted work.
/// Spawned tasks still need host-owned tracking; dropping a JoinHandle alone does
/// not stop its task. A fresh Core needs a fresh signal, not a migrated Future.
pub async fn wait_for<F: Future>(
    future: F,
    mut drain: tokio::sync::watch::Receiver<bool>,
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
    tokio::select! { biased; reason = cancelled => Err(reason), output = future => Ok(output) }
}
/// A scheduling descriptor only, not a Future or proof of a durable write.
/// This legacy framework type has no storage, delivery or resumption API. The
/// host's StateStore Subscription API must persist/checkpoint work explicitly
/// and require a durable backend when promising survival across process exit.
/// `wait_for` never creates, converts, migrates or cancels such a subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscription {
    pub route: Route,
    pub expires_at_unix_ms: u64,
}
