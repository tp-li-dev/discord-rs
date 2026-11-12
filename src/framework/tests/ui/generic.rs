use bot_framework::prelude::*;

#[hybrid_command]
async fn ping<T>(_ctx: Context<'_>) -> BotResult<()> { Ok(()) }

fn main() {}
