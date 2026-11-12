//! Transport-independent interaction routing. Discord/Worker adapters are not connected.
//!
//! The host must authenticate IDs, isolate bot/shard and interaction kind, check
//! expiry and bind the stored envelope to the decoded route before returning it.
//! A route or state key is not user authorization or protection against replay.
use crate::{BotResult, CommandFuture};
use serde::de::DeserializeOwned;
use std::{future::Future, pin::Pin, sync::Mutex};

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
        }
    }
}
impl std::fmt::Display for InteractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.response())
    }
}
impl std::error::Error for InteractionError {}

pub type RestoreFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RestoredState, InteractionError>> + Send + 'a>>;
/// Adapter boundary for a shared StateStore and authenticated custom ID codec.
/// Implementations must reject malformed/tampered IDs, unsupported wire versions,
/// missing or expired records, incompatible schemas and route-envelope mismatches.
/// Missing and expired records may be indistinguishable. Never expose ID or payload
/// in errors. External storage and its durability are the host's responsibility.
pub trait InteractionStateStore: Send + Sync {
    fn restore<'a>(&'a self, kind: InteractionKind, custom_id: &'a str) -> RestoreFuture<'a>;
}

pub struct InteractionContext<'a> {
    data: &'a [u8],
    values: &'a [String],
    replies: &'a Mutex<Vec<String>>,
}
impl InteractionContext<'_> {
    /// Decode the validated state payload as JSON. Schema migrations are host-owned.
    pub fn state<T: DeserializeOwned>(&self) -> Result<T, InteractionError> {
        serde_json::from_slice(self.data).map_err(|_| InteractionError::InvalidState)
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
        })
        .await
        .map_err(|_| InteractionError::HandlerFailed)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitCancelled {
    Draining,
    SignalClosed,
}
/// A local future is cancelled on drain by default, including an already active drain.
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
/// Describes persistent work only; storage, delivery and resumption are not implemented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscription {
    pub route: Route,
    pub expires_at_unix_ms: u64,
}
