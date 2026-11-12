//! Discord Gateway metadata is an observation, never an Identify reservation or
//! ownership authorization. Consumers must coordinate and refresh it separately.
use crate::placement::{Config, PlacementError, ShardCount};
use std::{collections::HashMap, time::Duration};
use twilight_model::gateway::connection_info::BotConnectionInfo;

/// Fetch only when explicitly called. The client must be scoped to the desired
/// Bot. HTTP and decoding errors are intentionally redacted to avoid logging secrets.
pub async fn fetch(client: &twilight_http::Client) -> Result<BotConnectionInfo, PlacementError> {
    let info = tokio::time::timeout(Duration::from_secs(10), async {
        client
            .gateway()
            .authed()
            .await
            .map_err(|_| ())?
            .model()
            .await
            .map_err(|_| ())
    })
    .await
    .map_err(|_| PlacementError("gateway metadata request timed out".into()))?
    .map_err(|_| PlacementError("gateway metadata request failed".into()))?;
    validate_metadata(&info)?;
    Ok(info)
}

/// Checks only semantic bounds needed by the planner. Exhausted sessions are a
/// valid observation but cannot be used as permission to Identify.
pub fn validate_metadata(info: &BotConnectionInfo) -> Result<(), PlacementError> {
    let limit = &info.session_start_limit;
    if info.shards == 0
        || limit.total == 0
        || limit.max_concurrency == 0
        || limit.remaining > limit.total
    {
        return Err(PlacementError("invalid gateway metadata".into()));
    }
    Ok(())
}

/// Resolve auto-configured Bot counts from observations explicitly keyed by Bot
/// ID. The caller is responsible for associating each response with the correct
/// Bot/token and for fetching fresh metadata. No credential is read here.
pub fn resolve_auto(
    config: &Config,
    observations: &HashMap<String, BotConnectionInfo>,
) -> Result<HashMap<String, u32>, PlacementError> {
    let mut resolved = HashMap::new();
    for bot in &config.bots {
        if matches!(bot.total_shards, ShardCount::Auto(_)) {
            let info = observations.get(&bot.bot_id).ok_or_else(|| {
                PlacementError("missing gateway metadata for auto shard count".into())
            })?;
            validate_metadata(info)?;
            resolved.insert(bot.bot_id.clone(), info.shards);
        }
    }
    config.validate(&resolved)?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use twilight_model::gateway::SessionStartLimit;
    fn info() -> BotConnectionInfo {
        BotConnectionInfo {
            url: "wss://gateway.discord.gg".into(),
            shards: 2,
            session_start_limit: SessionStartLimit {
                max_concurrency: 1,
                remaining: 0,
                reset_after: 1000,
                total: 1000,
            },
        }
    }
    #[test]
    fn validates_limits_without_treating_exhaustion_as_permission() {
        let mut observation = info();
        assert!(validate_metadata(&observation).is_ok());
        observation.shards = 0;
        assert!(validate_metadata(&observation).is_err());
        observation = info();
        observation.session_start_limit.max_concurrency = 0;
        assert!(validate_metadata(&observation).is_err());
        observation = info();
        observation.session_start_limit.remaining = 1001;
        assert!(validate_metadata(&observation).is_err());
        observation = info();
        observation.session_start_limit.total = 0;
        assert!(validate_metadata(&observation).is_err());
    }
    #[test]
    fn resolves_auto_and_rechecks_placement() {
        let config = Config::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml"),
        )
        .unwrap();
        assert!(resolve_auto(&config, &HashMap::new()).is_err());
        let observations = HashMap::from([("bot-two".to_owned(), info())]);
        let resolved = resolve_auto(&config, &observations).unwrap();
        assert_eq!(resolved.get("bot-two"), Some(&2));
        assert_eq!(config.local_primary_intents(&resolved).unwrap().len(), 1);
        let mut invalid = config;
        invalid.bots[1].shard_placement[0].shard_id = 2;
        assert!(resolve_auto(&invalid, &observations).is_err());
    }
}
