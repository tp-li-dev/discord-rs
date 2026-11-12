use bot_framework::prelude::*;
#[hybrid_command]
async fn ping(_ctx: Context<'_>, _value: Option<String>, _required: i64) -> BotResult<()> { Ok(()) }
fn main() {}
