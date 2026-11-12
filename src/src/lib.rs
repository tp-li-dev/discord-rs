pub mod botref;
pub mod cert_policy;
pub mod cluster;
pub mod cluster_runtime;
mod cogs;
pub mod daemon;
pub mod drain;
pub mod event_seq;
pub mod failover;
pub mod fs_perms;
pub mod gateway_metadata;
pub mod gateway_switch;
pub mod identify;
pub mod interaction_flow;
pub mod local_control;
pub mod metrics;
pub mod ownership;
pub mod placement;
pub mod protocol;
pub mod raft_quorum;
pub mod rbac_audit;
pub mod redact;
pub mod response_drain;
pub mod rest_budget;
pub mod shard_runtime;
pub mod state_store;
pub mod supervisor;
pub mod switch;
pub mod testkit;
pub mod tls_transport;
pub mod update_gate;
pub mod update_rollback;
pub mod worker;

pub type AppError = Box<dyn std::error::Error + Send + Sync>;

/// Command definitions linked into this binary. Startup must reject duplicates.
pub fn registered_commands() -> bot_framework::BotResult<Vec<&'static bot_framework::Command>> {
    // Reference the module to ensure its static registrations are linked.
    let _ = cogs::build_id();
    bot_framework::commands()
}
