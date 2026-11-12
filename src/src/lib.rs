mod cogs;
pub mod drain;
pub mod gateway_metadata;
pub mod identify;
pub mod interaction_flow;
pub mod local_control;
pub mod ownership;
pub mod placement;
pub mod protocol;
pub mod response_drain;
pub mod shard_runtime;
pub mod switch;
pub mod worker;

pub type AppError = Box<dyn std::error::Error + Send + Sync>;

/// Command definitions linked into this binary. Startup must reject duplicates.
pub fn registered_commands() -> bot_framework::BotResult<Vec<&'static bot_framework::Command>> {
    // Reference the module to ensure its static registrations are linked.
    let _ = cogs::build_id();
    bot_framework::commands()
}
