use bot_framework::prelude::*;
struct Counter;
#[cog]
impl Counter {
    #[hybrid_command]
    async fn add(&self, _ctx: Context<'_>, _value: u64) -> BotResult<()> { Ok(()) }
}
fn main() {}
