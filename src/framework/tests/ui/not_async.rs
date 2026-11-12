use bot_framework::prelude::*;

#[hybrid_command]
fn ping(_ctx: Context<'_>) -> BotResult<()> { Ok(()) }

fn main() {}
