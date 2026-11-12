//! Experimental command routing. Only linked registrations are discovered.
//! Routing is in-memory; Discord registration and IPC are not implemented.
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

extern crate self as bot_framework;

pub mod arguments;

pub mod lifecycle;

pub mod interactions;

pub use bot_framework_macros::{cog, component, event, hybrid_command, modal};
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

pub type BotResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type CommandFuture<'a> = Pin<Box<dyn Future<Output = BotResult<()>> + Send + 'a>>;

/// Process-local shared services keyed by concrete Rust type.
/// Insert values before sharing the map with handlers; interior mutability is up to each service.
/// This map is not serialized or transferred during a Worker update.
#[derive(Default)]
pub struct StateMap {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}
impl StateMap {
    pub fn new() -> Self {
        Self::default()
    }
    /// Insert or replace a value of the same concrete type.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Box::new(value));
    }
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.values.contains_key(&TypeId::of::<T>())
    }
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.values
            .remove(&TypeId::of::<T>())?
            .downcast::<T>()
            .ok()
            .map(|value| *value)
    }
    pub fn get<T: Send + Sync + 'static>(&self) -> BotResult<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
            .ok_or_else(|| format!("state not registered: {}", type_name::<T>()).into())
    }
}

/// Optional grouping of shared services and lifecycle hooks. Methods have defaults.
/// `#[cog]` discovers command and ReadyEvent methods on linked implementations.
pub trait Cog: Send + Sync + 'static {
    fn start(&self) -> CommandFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Wait for Cog-owned work before stopping. The default has no work to wait for.
    /// The host must stop admission first and track in-flight handlers and tasks.
    /// This hook alone does not establish that a Worker has drained.
    fn drain(&self) -> CommandFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Release resources after drain succeeds. Defaults to no cleanup.
    fn stop(&self) -> CommandFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Explicitly manages one Cog instance per concrete type.
#[derive(Default)]
pub struct CogSet {
    cogs: HashMap<TypeId, Arc<dyn Cog>>,
}
impl CogSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start first; failed starts are never made available to handlers.
    pub async fn add<T: Cog>(&mut self, states: &mut StateMap, cog: T) -> BotResult<Arc<T>> {
        if self.cogs.contains_key(&TypeId::of::<T>()) || states.contains::<Arc<T>>() {
            return Err(format!("cog already registered: {}", type_name::<T>()).into());
        }
        let cog = Arc::new(cog);
        cog.start().await?;
        states.insert(Arc::clone(&cog));
        self.cogs.insert(TypeId::of::<T>(), cog.clone());
        Ok(cog)
    }

    /// Drain then stop before removing; either failure leaves the Cog registered for retry.
    /// The caller must stop admission and wait for in-flight handlers before removal.
    /// Retried hooks must be idempotent; cancellation also leaves registration intact.
    pub async fn remove<T: Cog>(&mut self, states: &mut StateMap) -> BotResult<()> {
        let cog = self
            .cogs
            .get(&TypeId::of::<T>())
            .ok_or_else(|| format!("cog not registered: {}", type_name::<T>()))?;
        let state = states.get::<Arc<T>>()?;
        let state_as_cog: Arc<dyn Cog> = state.clone();
        if !Arc::ptr_eq(cog, &state_as_cog) {
            return Err(format!("cog state replaced: {}", type_name::<T>()).into());
        }
        cog.drain().await?;
        cog.stop().await?;
        self.cogs.remove(&TypeId::of::<T>());
        states.remove::<Arc<T>>();
        Ok(())
    }
}

/// A minimal transport-independent reply sink used by the prototype router.
pub struct Context<'a> {
    replies: &'a Mutex<Vec<String>>,
    states: Option<&'a StateMap>,
    arguments: Option<&'a Mutex<arguments::Arguments<'a>>>,
}
impl<'a> Context<'a> {
    pub fn new(replies: &'a Mutex<Vec<String>>) -> Self {
        Self {
            replies,
            states: None,
            arguments: None,
        }
    }
    pub fn with_state(replies: &'a Mutex<Vec<String>>, states: &'a StateMap) -> Self {
        Self {
            replies,
            states: Some(states),
            arguments: None,
        }
    }
    /// Attach decoded invocation arguments supplied by the transport adapter.
    pub fn with_arguments(mut self, arguments: &'a Mutex<arguments::Arguments<'a>>) -> Self {
        self.arguments = Some(arguments);
        self
    }
    #[doc(hidden)]
    pub fn required_argument<T: arguments::Argument>(&self, name: &str) -> BotResult<T> {
        match self.arguments {
            Some(args) => Ok(args
                .lock()
                .map_err(|_| "argument lock poisoned")?
                .required(name)?),
            None => Err(arguments::ArgumentError::Missing(name.to_owned()).into()),
        }
    }
    #[doc(hidden)]
    pub fn optional_argument<T: arguments::Argument>(&self, name: &str) -> BotResult<Option<T>> {
        match self.arguments {
            Some(args) => Ok(args
                .lock()
                .map_err(|_| "argument lock poisoned")?
                .optional(name)?),
            None => Ok(None),
        }
    }
    #[doc(hidden)]
    pub fn finish_arguments(&self) -> BotResult<()> {
        if let Some(args) = self.arguments {
            args.lock()
                .map_err(|_| "argument lock poisoned")?
                .finish_ref()?;
        }
        Ok(())
    }
    pub fn state<T: Send + Sync + 'static>(&self) -> BotResult<&T> {
        self.states
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                format!("state not registered: {}", type_name::<T>()).into()
            })?
            .get::<T>()
    }
    pub async fn reply(&self, message: impl Into<String>) -> BotResult<()> {
        self.replies
            .lock()
            .map_err(|_| "reply sink poisoned")?
            .push(message.into());
        Ok(())
    }
}

pub struct Command {
    pub name: &'static str,
    pub description: &'static str,
    pub arguments: &'static [arguments::ArgumentSchema],
    pub handler: for<'a> fn(Context<'a>) -> CommandFuture<'a>,
}
inventory::collect!(Command);

/// Collect linked commands in deterministic order and reject duplicate names.
pub fn commands() -> BotResult<Vec<&'static Command>> {
    let mut commands: Vec<_> = inventory::iter::<Command>.into_iter().collect();
    commands.sort_unstable_by_key(|command| command.name);
    for pair in commands.windows(2) {
        if pair[0].name == pair[1].name {
            return Err(format!("duplicate command name: {}", pair[0].name).into());
        }
    }
    Ok(commands)
}

/// Dispatch either invocation type to the same handler. Unknown commands return false.
pub async fn dispatch(name: &str, ctx: Context<'_>) -> BotResult<bool> {
    let commands = commands()?;
    if let Ok(index) = commands.binary_search_by_key(&name, |command| command.name) {
        (commands[index].handler)(ctx).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Data available to a ready handler in the experimental API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyEvent {
    pub bot_name: String,
}

/// Event-specific services. The host controls where log messages go.
#[derive(Clone, Copy)]
pub struct EventContext<'a> {
    logger: &'a (dyn Fn(&str) + Send + Sync),
    states: Option<&'a StateMap>,
}
impl<'a> EventContext<'a> {
    pub fn new(logger: &'a (dyn Fn(&str) + Send + Sync)) -> Self {
        Self {
            logger,
            states: None,
        }
    }
    pub fn with_state(logger: &'a (dyn Fn(&str) + Send + Sync), states: &'a StateMap) -> Self {
        Self {
            logger,
            states: Some(states),
        }
    }
    pub fn state<T: Send + Sync + 'static>(&self) -> BotResult<&T> {
        self.states
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                format!("state not registered: {}", type_name::<T>()).into()
            })?
            .get::<T>()
    }
    pub fn log_info(&self, message: &str) {
        (self.logger)(message);
    }
}

#[doc(hidden)]
pub struct ReadyHandler {
    pub name: &'static str,
    pub handler: for<'a> fn(EventContext<'a>, ReadyEvent) -> CommandFuture<'a>,
}
inventory::collect!(ReadyHandler);

/// Invoke all linked ReadyEvent handlers in name order, stopping at the first error.
/// This is a local router; the Gateway adapter is not yet implemented.
pub async fn dispatch_ready(ctx: EventContext<'_>, event: ReadyEvent) -> BotResult<()> {
    let mut handlers: Vec<_> = inventory::iter::<ReadyHandler>.into_iter().collect();
    handlers.sort_unstable_by_key(|handler| handler.name);
    for handler in handlers {
        (handler.handler)(ctx, event.clone()).await?;
    }
    Ok(())
}

pub mod prelude {
    pub use crate::{
        BotResult, Cog, CogSet, Context, EventContext, ReadyEvent, StateMap, cog, component, event,
        hybrid_command, modal,
    };
}
