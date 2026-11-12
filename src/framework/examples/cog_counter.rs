//! Run from src/: cargo run --offline -p bot-framework --example cog_counter
//!
//! Token-free local routing: the Cog is in counter/mod.rs, while a plain command
//! below reads the same typed service. StateMap is process-local and resets on
//! Worker update/restart; no database, Gateway, or Discord transport is installed.
use bot_framework::{arguments::Arguments, dispatch, dispatch_ready, prelude::*};
use std::sync::{Arc, Mutex};

pub mod counter;

use counter::{CounterCog, CounterState};

#[hybrid_command(name = "count_value", description = "Read the shared counter")]
async fn count_value(ctx: Context<'_>) -> BotResult<()> {
    let value = ctx.state::<Arc<CounterState>>()?.value();
    ctx.reply(value.to_string()).await
}

pub async fn demo() -> BotResult<Vec<String>> {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let counter = Arc::new(CounterState::new(0));
    states.insert(Arc::clone(&counter));
    cogs.add(&mut states, CounterCog { state: counter }).await?;

    let replies = Mutex::new(Vec::new());
    let logger = |_: &str| {};
    dispatch_ready(
        EventContext::with_state(&logger, &states),
        ReadyEvent {
            bot_name: "counter-demo".into(),
        },
    )
    .await?;

    // The adapter has already removed !counter or extracted /counter's name.
    let prefix = Mutex::new(Arguments::prefix("")?);
    dispatch(
        "counter",
        Context::with_state(&replies, &states).with_arguments(&prefix),
    )
    .await?;
    let slash = Mutex::new(Arguments::slash([])?);
    dispatch(
        "counter",
        Context::with_state(&replies, &states).with_arguments(&slash),
    )
    .await?;
    dispatch("count_value", Context::with_state(&replies, &states)).await?;

    // This sequential host has finished admitting and awaiting all invocations.
    // A concurrent host must stop admission and wait for handlers before remove;
    // the default Cog drain hook alone does not drain a Worker or its REST sends.
    cogs.remove::<CounterCog>(&mut states).await?;
    Ok(replies.into_inner().map_err(|_| "reply sink poisoned")?)
}

#[cfg(not(test))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> BotResult<()> {
    for reply in demo().await? {
        println!("{reply}");
    }
    Ok(())
}
