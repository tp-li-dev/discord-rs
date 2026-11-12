use bot_framework::prelude::*;

#[hybrid_command(name = "ping", name = "pong")]
async fn ping(_ctx: Context<'_>) -> BotResult<()> { Ok(()) }

fn main() {}
