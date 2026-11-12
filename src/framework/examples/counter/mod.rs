//! Counter feature kept separate from the host and its other commands.
use bot_framework::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Shared process-local service. This is not persisted or moved on Worker updates.
pub struct CounterState {
    value: AtomicU64,
    ready_events: AtomicU64,
}

impl CounterState {
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            ready_events: AtomicU64::new(0),
        }
    }

    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn ready_events(&self) -> u64 {
        self.ready_events.load(Ordering::Relaxed)
    }

    fn increment(&self) -> BotResult<u64> {
        self.value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| "counter exhausted".into())
    }
}

pub struct CounterCog {
    pub state: Arc<CounterState>,
}

// There is no background work in this example, so the default hooks suffice.
impl Cog for CounterCog {}

#[cog]
impl CounterCog {
    #[hybrid_command(name = "counter", description = "Increment the shared counter")]
    async fn increment(&self, ctx: Context<'_>) -> BotResult<()> {
        let value = self.state.increment()?;
        // reply() appends to the local sink; it does not confirm a Discord REST send.
        ctx.reply(value.to_string()).await
    }

    #[event]
    async fn on_ready(&self, ctx: EventContext<'_>, _event: ReadyEvent) -> BotResult<()> {
        self.state.ready_events.fetch_add(1, Ordering::Relaxed);
        ctx.log_info("counter ready");
        Ok(())
    }
}
