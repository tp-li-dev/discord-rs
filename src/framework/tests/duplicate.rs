use bot_framework::{commands, prelude::*};
#[hybrid_command(name = "same")]
async fn first(_ctx: Context<'_>) -> BotResult<()> {
    Ok(())
}
#[hybrid_command(name = "same")]
async fn second(_ctx: Context<'_>) -> BotResult<()> {
    Ok(())
}
#[test]
fn duplicate_names_are_rejected() {
    assert!(
        matches!(commands(), Err(error) if error.to_string().contains("duplicate command name: same"))
    );
}
