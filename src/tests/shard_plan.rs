//! Offline integration of placement, metadata and in-process Identify queues.
//! These tests do not launch a gateway or authorize ownership.
use src::{gateway_metadata, identify::IdentifyRegistry, placement::Config};
use std::{collections::HashMap, num::NonZeroU64, path::Path, time::Duration};
use twilight_gateway_queue::Queue;
use twilight_model::gateway::{SessionStartLimit, connection_info::BotConnectionInfo};

#[tokio::test]
async fn local_plan_keeps_bot_queues_separate_and_shares_each_bot_bucket() {
    let mut config =
        Config::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml"))
            .unwrap();
    // Place both first-bot shards locally; the second bot also has one local shard.
    config.node.id = "server-b".into();
    config.bots[0].shard_placement[0].primary = "server-b".into();
    config.bots[0].shard_placement[0].failover = vec!["server-a".into()];
    let limits = SessionStartLimit {
        max_concurrency: 1,
        remaining: 2,
        reset_after: 60_000,
        total: 2,
    };
    let observation = BotConnectionInfo {
        shards: 1,
        session_start_limit: limits.clone(),
        url: "wss://gateway.discord.gg".into(),
    };
    let counts = gateway_metadata::resolve_auto(
        &config,
        &HashMap::from([("bot-two".to_owned(), observation)]),
    )
    .unwrap();
    let plan = config.local_primary_intents(&counts).unwrap();
    assert_eq!(
        plan.iter()
            .map(|p| (p.bot_id.as_str(), p.shard_id))
            .collect::<Vec<_>>(),
        [("bot-one", 0), ("bot-one", 1), ("bot-two", 0)]
    );
    let mut queues = IdentifyRegistry::default();
    let first = NonZeroU64::new(1).unwrap();
    let second = NonZeroU64::new(2).unwrap();
    let q1 = queues.register(first, &limits).unwrap();
    let q2 = queues.register(second, &limits).unwrap();
    tokio::time::timeout(Duration::from_secs(1), q1.enqueue(plan[0].shard_id))
        .await
        .unwrap()
        .unwrap();
    // Other bot's independent bucket can proceed immediately.
    tokio::time::timeout(Duration::from_secs(1), q2.enqueue(plan[2].shard_id))
        .await
        .unwrap()
        .unwrap();
    // Same bot, same bucket remains delayed.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), q1.enqueue(plan[1].shard_id))
            .await
            .is_err()
    );
}
