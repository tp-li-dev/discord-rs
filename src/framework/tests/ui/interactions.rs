use bot_framework::{component, modal, BotResult, interactions::InteractionContext};
#[component(namespace = "app", name = "x")]
async fn missing(_: InteractionContext<'_>) -> BotResult<()> { Ok(()) }
#[modal(namespace = "app", name = "x", version = 0)]
async fn zero(_: InteractionContext<'_>) -> BotResult<()> { Ok(()) }
#[component(namespace = "App", name = "x", version = 1)]
async fn label(_: InteractionContext<'_>) -> BotResult<()> { Ok(()) }
#[modal(namespace = "app", name = "x", name = "y", version = 1)]
async fn duplicate(_: InteractionContext<'_>) -> BotResult<()> { Ok(()) }
#[component(namespace = "app", name = "x", version = 1)]
fn sync(_: InteractionContext<'_>) -> BotResult<()> { Ok(()) }
fn main() {}
