use bot_framework::{commands, dispatch, prelude::*};
use std::sync::Mutex;

struct First;
struct Second;
impl Cog for First {}
impl Cog for Second {}
#[cog]
impl First {
    #[hybrid_command(name = "collision")]
    async fn first(&self, _ctx: Context<'_>) -> BotResult<()> {
        Ok(())
    }
}
#[cog]
impl Second {
    #[hybrid_command(name = "collision")]
    async fn second(&self, _ctx: Context<'_>) -> BotResult<()> {
        Ok(())
    }
}
#[tokio::test]
async fn duplicate_cog_command_names_are_rejected_before_any_dispatch() {
    assert!(
        matches!(commands(), Err(error) if error.to_string().contains("duplicate command name: collision"))
    );
    let replies = Mutex::new(Vec::new());
    assert!(dispatch("collision", Context::new(&replies)).await.is_err());
}
