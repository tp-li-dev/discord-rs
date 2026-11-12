use bot_framework::{BotResult, component, interactions::InteractionContext};
#[component(namespace = "app", name = "same", version = 1)]
async fn first(_: InteractionContext<'_>) -> BotResult<()> {
    Ok(())
}
#[component(namespace = "app", name = "same", version = 1)]
async fn second(_: InteractionContext<'_>) -> BotResult<()> {
    Ok(())
}
#[test]
fn duplicate_routes_are_rejected() {
    assert!(matches!(
        bot_framework::interactions::Registry::new(),
        Err(bot_framework::interactions::InteractionError::DuplicateHandler)
    ));
}
