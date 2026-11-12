use bot_framework::prelude::*;
struct Listener;
#[cog]
impl Listener {
    #[event]
    async fn ready(&self, ctx: EventContext<'_>) -> BotResult<()> { Ok(()) }
}
fn main() {}
