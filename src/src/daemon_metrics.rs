//! Daemon-side owner of per-shard metrics handles and their feed channels.
//! Labels are fixed (`bot_id`, `shard_id`, `generation_id = 0` for the stable
//! per-shard series), so a restart never creates a new series. Registration
//! failures (invalid/suspected-secret Bot IDs, cardinality caps) skip that
//! shard's metrics and are counted; they never affect process supervision.
//! Values are advisory observations, not authority or health.
use crate::{
    daemon::ShardKey,
    metrics::{
        GenerationStatus, MetricLabels, MetricsRegistry, RuntimeRole, RuntimeStatus, ShardMetrics,
    },
    metrics_feed::{self, FeedReceiver, FeedSender},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

const FEED_CAPACITY: usize = 64;
const COLLECT_LIMIT: usize = 256;

struct Slot {
    handle: Arc<ShardMetrics>,
    sender: FeedSender,
    receiver: FeedReceiver,
}

pub struct DaemonMetrics {
    registry: MetricsRegistry,
    slots: BTreeMap<ShardKey, Slot>,
    rejected: u64,
}

impl DaemonMetrics {
    pub fn new<'a>(keys: impl IntoIterator<Item = &'a ShardKey>) -> Self {
        let registry = MetricsRegistry::default();
        let mut slots = BTreeMap::new();
        let mut rejected = 0u64;
        for key in keys {
            let registered = MetricLabels::new(key.bot_id.clone(), key.shard_id, 0)
                .and_then(|labels| registry.register(labels, runtime(0, false)));
            match (registered, metrics_feed::channel(FEED_CAPACITY)) {
                (Ok(handle), Some((sender, receiver))) => {
                    slots.insert(
                        key.clone(),
                        Slot {
                            handle,
                            sender,
                            receiver,
                        },
                    );
                }
                _ => rejected = rejected.saturating_add(1),
            }
        }
        Self {
            registry,
            slots,
            rejected,
        }
    }

    /// Non-blocking sender for the shard's owner; None when not registered.
    pub fn sender(&self, key: &ShardKey) -> Option<FeedSender> {
        self.slots.get(key).map(|s| s.sender.clone())
    }

    pub fn handle(&self, key: &ShardKey) -> Option<Arc<ShardMetrics>> {
        self.slots.get(key).map(|s| Arc::clone(&s.handle))
    }

    /// `generation` is the launch sequence of the Gateway child, if any.
    pub fn set_process(&self, key: &ShardKey, running: bool, generation: u64) {
        if let Some(slot) = self.slots.get(key) {
            slot.handle.set_runtime_status(runtime(generation, running));
        }
    }

    /// Bounded work per shard so a noisy shard cannot starve the daemon loop.
    pub fn collect(&self) -> usize {
        self.slots
            .values()
            .map(|s| s.receiver.collect(&s.handle, COLLECT_LIMIT))
            .sum()
    }

    pub fn dropped_total(&self) -> u64 {
        self.slots
            .values()
            .map(|s| s.receiver.dropped())
            .fold(0, u64::saturating_add)
    }

    pub fn render_prometheus(&self) -> String {
        self.collect();
        self.registry.render_prometheus()
    }

    /// Compact status view; `bot` restricts it to one Bot's shards.
    pub fn status(&self, bot: Option<&str>) -> Value {
        self.collect();
        let shards: Vec<_> = self
            .slots
            .iter()
            .filter(|(key, _)| bot.is_none_or(|b| b == key.bot_id))
            .map(|(_, slot)| {
                let snapshot = slot.handle.snapshot();
                let feed = slot
                    .handle
                    .feed
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .to_json();
                json!({
                    "bot_id": snapshot.labels.bot_id(), "shard_id": snapshot.labels.shard_id(),
                    "generation_id": snapshot.labels.generation_id(),
                    "runtime": snapshot.runtime, "feed": feed,
                })
            })
            .collect();
        json!({"advisory": true, "registration_rejected": self.rejected,
            "feed_dropped_total": self.dropped_total(), "shards": shards})
    }
}

fn runtime(generation: u64, running: bool) -> RuntimeStatus {
    let role = if running {
        RuntimeRole::Active
    } else {
        RuntimeRole::Standby
    };
    RuntimeStatus {
        gateway: GenerationStatus {
            generation_id: generation,
            role,
        },
        // This daemon does not supervise Core generations or observe a
        // WebSocket; these stay conservative rather than guessed.
        core: GenerationStatus {
            generation_id: 0,
            role: RuntimeRole::Standby,
        },
        ws_connected: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_feed::Observation;
    use std::time::Duration;

    fn key(bot: &str, shard: u32) -> ShardKey {
        ShardKey {
            bot_id: bot.into(),
            shard_id: shard,
        }
    }

    #[test]
    fn feed_values_reach_prometheus_and_suspect_labels_are_skipped() {
        let (a, bad) = (key("alpha", 0), key("my-token-bot", 0));
        let metrics = DaemonMetrics::new([&a, &bad]);
        assert!(metrics.sender(&bad).is_none());
        assert!(
            metrics
                .sender(&a)
                .unwrap()
                .observe(Observation::Drain(Duration::from_millis(1500)))
        );
        let text = metrics.render_prometheus();
        assert!(text.contains(
            "met_feed_drain_seconds{bot_id=\"alpha\",shard_id=\"0\",generation_id=\"0\"} 1.500000000"
        ));
        assert!(!text.contains("my-token-bot"));
        assert_eq!(metrics.status(None)["registration_rejected"], 1);
    }
}
