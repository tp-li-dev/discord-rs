//! In-process identify queues. This is not a distributed limiter or an ownership lease.
//! Use one registry for all shards of a bot within a single process. Never start a
//! second process with its own registry for the same bot without a shared coordinator.
use std::{collections::HashMap, num::NonZeroU64, time::Duration};
use twilight_gateway_queue::InMemoryQueue;
use twilight_model::gateway::SessionStartLimit;

#[derive(Default)]
pub struct IdentifyRegistry {
    queues: HashMap<NonZeroU64, InMemoryQueue>,
}

impl IdentifyRegistry {
    /// Install the limit obtained from Discord once per bot and share the queue.
    /// A duplicate is an error rather than an accidental quota reset.
    pub fn register(
        &mut self,
        bot: NonZeroU64,
        limits: &SessionStartLimit,
    ) -> Result<InMemoryQueue, &'static str> {
        if self.queues.contains_key(&bot) {
            return Err("identify queue for bot already exists");
        }
        if limits.max_concurrency == 0 || limits.total == 0 || limits.remaining > limits.total {
            return Err("invalid session start limits");
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err("identify queue requires a Tokio runtime");
        }
        let queue = InMemoryQueue::new(
            limits.max_concurrency,
            limits.remaining,
            Duration::from_millis(limits.reset_after),
            limits.total,
        );
        self.queues.insert(bot, queue.clone());
        Ok(queue)
    }

    pub fn get(&self, bot: NonZeroU64) -> Option<InMemoryQueue> {
        self.queues.get(&bot).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use twilight_gateway_queue::Queue;

    fn limits() -> SessionStartLimit {
        SessionStartLimit {
            max_concurrency: 2,
            remaining: 4,
            reset_after: 60_000,
            total: 4,
        }
    }
    #[tokio::test]
    async fn cloned_queue_shares_bucket_and_bot_quotas() {
        let mut registry = IdentifyRegistry::default();
        let bot = NonZeroU64::new(123).unwrap();
        let queue = registry.register(bot, &limits()).unwrap();
        let shared = registry.get(bot).unwrap();
        let a = queue.enqueue(0);
        let b = shared.enqueue(2); // same bucket as shard 0
        let c = shared.enqueue(1); // different bucket
        timeout(Duration::from_secs(1), a).await.unwrap().unwrap();
        timeout(Duration::from_secs(1), c).await.unwrap().unwrap();
        assert!(timeout(Duration::from_millis(100), b).await.is_err());
        assert!(registry.register(bot, &limits()).is_err());
    }
    #[tokio::test]
    async fn exhausted_quota_waits_and_invalid_limits_are_rejected() {
        let mut registry = IdentifyRegistry::default();
        let bot = NonZeroU64::new(1).unwrap();
        let mut current = limits();
        current.max_concurrency = 0;
        assert!(registry.register(bot, &current).is_err());
        current.max_concurrency = 1;
        current.remaining = current.total + 1;
        assert!(registry.register(bot, &current).is_err());
        current.remaining = 0;
        let queue = registry.register(bot, &current).unwrap();
        assert!(
            timeout(Duration::from_millis(100), queue.enqueue(0))
                .await
                .is_err()
        );
    }
}
