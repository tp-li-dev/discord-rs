//! Local ownership authority, separate from peer authentication and process control.
//! Keep exactly one standalone coordinator per daemon. Epochs are in-memory and
//! must not be reused across daemon restarts while old participants can survive.
//! This module does not stop processes: callers must poll `watchdog` and gate each
//! new operation with `check`. Checks authorize admission, not later side effects.
use crate::protocol::{BotId, OwnershipEpoch};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct OwnershipKey {
    pub bot_id: BotId,
    pub shard_id: u32,
}

/// Time elapsed in one local clock domain; never a remote or wall-clock timestamp.
/// Implementations must advance during runtime pauses relevant to lease safety.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

/// Uses `Instant`; OS suspend accounting is platform dependent. This default is
/// only for standalone use, not a clock-safety guarantee for distributed leases.
pub struct MonotonicClock(Instant);

impl Default for MonotonicClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease {
    pub key: OwnershipKey,
    pub epoch: OwnershipEpoch,
    pub expires_at: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipError {
    NotOwned,
    Fenced,
    Expired,
    InvalidDuration,
    TimeOverflow,
    EpochExhausted,
    ClockRegressed,
    QuorumUnavailable,
}

impl std::fmt::Display for OwnershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ownership error: {self:?}")
    }
}
impl std::error::Error for OwnershipError {}

/// Trusted control-plane boundary. Authentication/authorization precedes calls.
/// Distributed implementations must obtain quorum approval before granting or
/// renewing authority; local `check` must never perform network or storage I/O.
pub trait OwnershipCoordinator {
    /// Explicitly transfer authority, fencing any previous epoch immediately.
    fn acquire(&mut self, key: OwnershipKey, ttl: Duration) -> Result<Lease, OwnershipError>;
    fn renew(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError>;
    fn release(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError>;
    fn check(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError>;
    /// Invalidate expired leases and return the keys whose new work must stop.
    /// On clock failure all authority is denied; the caller must stop all work.
    fn watchdog(&mut self) -> Result<Vec<OwnershipKey>, OwnershipError>;
}

struct Entry {
    epoch: OwnershipEpoch,
    deadline: Option<Duration>,
}

/// Single daemon only: no failover, durable epoch allocation, or distributed lease.
/// Serialize access (e.g. a local mutex) when shared by several admission paths.
pub struct StandaloneCoordinator<C = MonotonicClock> {
    clock: C,
    entries: HashMap<OwnershipKey, Entry>,
    last_now: Duration,
    clock_failed: bool,
}

impl Default for StandaloneCoordinator {
    fn default() -> Self {
        Self::new(MonotonicClock::default())
    }
}

impl<C: Clock> StandaloneCoordinator<C> {
    pub fn new(clock: C) -> Self {
        let last_now = clock.now();
        Self {
            clock,
            entries: HashMap::new(),
            last_now,
            clock_failed: false,
        }
    }

    fn now(&mut self) -> Result<Duration, OwnershipError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            return Err(OwnershipError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    fn deadline(now: Duration, ttl: Duration) -> Result<Duration, OwnershipError> {
        if ttl.is_zero() {
            return Err(OwnershipError::InvalidDuration);
        }
        now.checked_add(ttl).ok_or(OwnershipError::TimeOverflow)
    }

    fn live(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        now: Duration,
    ) -> Result<&mut Entry, OwnershipError> {
        let entry = self.entries.get_mut(&key).ok_or(OwnershipError::NotOwned)?;
        if epoch != entry.epoch {
            return Err(OwnershipError::Fenced);
        }
        match entry.deadline {
            Some(deadline) if now < deadline => Ok(entry),
            // Keep expiry visible until watchdog emits its stop notification.
            _ => Err(OwnershipError::Expired),
        }
    }
}

impl<C: Clock> OwnershipCoordinator for StandaloneCoordinator<C> {
    fn acquire(&mut self, key: OwnershipKey, ttl: Duration) -> Result<Lease, OwnershipError> {
        let now = self.now()?;
        let expires_at = Self::deadline(now, ttl)?;
        let previous = self.entries.get(&key).map_or(0, |entry| entry.epoch.0);
        let epoch = OwnershipEpoch(
            previous
                .checked_add(1)
                .ok_or(OwnershipError::EpochExhausted)?,
        );
        self.entries.insert(
            key,
            Entry {
                epoch,
                deadline: Some(expires_at),
            },
        );
        Ok(Lease {
            key,
            epoch,
            expires_at,
        })
    }

    fn renew(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError> {
        let now = self.now()?;
        let expires_at = Self::deadline(now, ttl)?;
        let entry = self.live(key, epoch, now)?;
        // A renewal never shortens an existing grant.
        let expires_at = expires_at.max(entry.deadline.expect("live lease has deadline"));
        entry.deadline = Some(expires_at);
        Ok(Lease {
            key,
            epoch,
            expires_at,
        })
    }

    fn release(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        let now = self.now()?;
        self.live(key, epoch, now)?.deadline = None;
        Ok(())
    }

    fn check(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        let now = self.now()?;
        self.live(key, epoch, now).map(|_| ())
    }

    fn watchdog(&mut self) -> Result<Vec<OwnershipKey>, OwnershipError> {
        let now = self.now()?;
        let mut stopped = Vec::new();
        for (key, entry) in &mut self.entries {
            if entry.deadline.is_some_and(|deadline| now >= deadline) {
                entry.deadline = None;
                stopped.push(*key);
            }
        }
        Ok(stopped)
    }
}

/// Startup refusal until a mature consensus backend is integrated.
/// Evaluate openraft for durable log/snapshot recovery, membership changes,
/// authenticated transport, quorum loss, and safe local lease deadlines before
/// enabling a backend. No consensus or automatic standalone fallback here.
pub struct QuorumCoordinator {
    _private: (),
}

impl QuorumCoordinator {
    pub fn new() -> Result<Self, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
}

impl OwnershipCoordinator for QuorumCoordinator {
    fn acquire(&mut self, _: OwnershipKey, _: Duration) -> Result<Lease, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn renew(
        &mut self,
        _: OwnershipKey,
        _: OwnershipEpoch,
        _: Duration,
    ) -> Result<Lease, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn release(&mut self, _: OwnershipKey, _: OwnershipEpoch) -> Result<(), OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn check(&mut self, _: OwnershipKey, _: OwnershipEpoch) -> Result<(), OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn watchdog(&mut self) -> Result<Vec<OwnershipKey>, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_epoch_never_wraps_or_replaces_lease() {
        let mut c = StandaloneCoordinator::default();
        let key = OwnershipKey {
            bot_id: BotId(1),
            shard_id: 0,
        };
        c.entries.insert(
            key,
            Entry {
                epoch: OwnershipEpoch(u64::MAX),
                deadline: Some(Duration::MAX),
            },
        );
        assert_eq!(
            c.acquire(key, Duration::from_secs(1)),
            Err(OwnershipError::EpochExhausted)
        );
        c.check(key, OwnershipEpoch(u64::MAX)).unwrap();
    }

    #[test]
    fn quorum_boundary_denies_every_operation() {
        let mut c = QuorumCoordinator { _private: () };
        let key = OwnershipKey {
            bot_id: BotId(1),
            shard_id: 0,
        };
        assert_eq!(
            c.acquire(key, Duration::from_secs(1)),
            Err(OwnershipError::QuorumUnavailable)
        );
        assert_eq!(
            c.renew(key, OwnershipEpoch(1), Duration::from_secs(1)),
            Err(OwnershipError::QuorumUnavailable)
        );
        assert_eq!(
            c.release(key, OwnershipEpoch(1)),
            Err(OwnershipError::QuorumUnavailable)
        );
        assert_eq!(
            c.check(key, OwnershipEpoch(1)),
            Err(OwnershipError::QuorumUnavailable)
        );
        assert_eq!(c.watchdog(), Err(OwnershipError::QuorumUnavailable));
    }
}
