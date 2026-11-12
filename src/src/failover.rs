//! Per-shard detection policy, with no I/O or ownership authority of its own.
//! Trusted adapters supply authenticated heartbeats, quorum status and completion
//! events only after coordinator approval / process preparation. State alone is
//! never permission for side effects; callers must enforce leases and fencing.
pub use crate::ownership::Clock;
use crate::ownership::{Lease, OwnershipCoordinator, OwnershipError, OwnershipKey};
use crate::protocol::OwnershipEpoch;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Assigned,
    Suspected,
    FailoverPending,
    StandbyReady,
    TemporaryActive,
    FailbackWaiting,
    Degraded,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub heartbeat_interval: Duration,
    pub suspect_after: Duration,
    pub fail_after: Duration,
    pub stable_for: Duration,
    pub cooldown: Duration,
    pub max_backoff: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            suspect_after: Duration::from_millis(1500),
            fail_after: Duration::from_secs(3),
            stable_for: Duration::from_secs(30),
            cooldown: Duration::from_secs(3),
            max_backoff: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    ClockRegressed,
    AttemptExhausted,
    Ownership(OwnershipError),
}

#[derive(Clone, Copy, Debug)]
pub enum Event {
    /// Heartbeat from the original assigned host, not the temporary host.
    Heartbeat,
    /// Poll for missing heartbeats; callers schedule this at heartbeat_interval.
    Tick,
    Quorum(bool),
    CandidateEligible(bool),
    /// Approval of new ownership for the current failover attempt.
    OwnershipApproved {
        attempt: u64,
    },
    /// Both Gateway and Core are ready for this attempt.
    StartupComplete {
        attempt: u64,
    },
    /// After stability: original host prepared, ownership approved and handoff done.
    FailbackComplete {
        attempt: u64,
    },
    StartupFailed {
        attempt: u64,
    },
}

pub struct Failover<C> {
    clock: C,
    config: Config,
    state: State,
    last_now: Duration,
    last_heartbeat: Duration,
    stable_since: Option<Duration>,
    retry_at: Duration,
    backoff: Duration,
    attempt: u64,
    quorum: bool,
    eligible: bool,
    clock_failed: bool,
}

/// Ownership-enforcing adapter for the policy-only `Failover` model.
/// Call `handle` on every watchdog poll and `check_active` before new work.
/// Neither this adapter nor the standalone authority stops OS processes or
/// implements distributed consensus. Coordinator checks must be local.
pub struct CoordinatedFailover<C> {
    policy: Failover<C>,
    key: OwnershipKey,
    ttl: Duration,
    lease: Option<Lease>,
}

impl<C: Clock> CoordinatedFailover<C> {
    pub fn new(clock: C, config: Config, key: OwnershipKey, ttl: Duration) -> Result<Self, Error> {
        if ttl.is_zero() {
            return Err(Error::Ownership(OwnershipError::InvalidDuration));
        }
        Ok(Self {
            policy: Failover::new(clock, config)?,
            key,
            ttl,
            lease: None,
        })
    }

    pub fn state(&self) -> State {
        self.policy.state()
    }
    pub fn attempt(&self) -> u64 {
        self.policy.attempt()
    }
    /// Last approved grant, including after expiry/fencing, for diagnostics.
    pub fn lease(&self) -> Option<Lease> {
        self.lease
    }

    fn deny(&mut self, error: OwnershipError) -> Error {
        self.policy.state = State::Degraded;
        self.policy.stable_since = None;
        self.policy.dampen(self.policy.last_now);
        Error::Ownership(error)
    }

    fn validate(&mut self, coordinator: &mut impl OwnershipCoordinator) -> Result<(), Error> {
        if let Some(lease) = self.lease {
            // Local expiry is mandatory even if a backend incorrectly accepts it.
            if self.policy.clock.now() >= lease.expires_at {
                return Err(self.deny(OwnershipError::Expired));
            }
            coordinator
                .check(self.key, lease.epoch)
                .map_err(|e| self.deny(e))?;
        }
        Ok(())
    }

    fn acquire(&mut self, coordinator: &mut impl OwnershipCoordinator) -> Result<(), Error> {
        let lease = coordinator
            .acquire(self.key, self.ttl)
            .map_err(|e| self.deny(e))?;
        if lease.key != self.key || self.lease.is_some_and(|old| lease.epoch.0 <= old.epoch.0) {
            return Err(self.deny(OwnershipError::Fenced));
        }
        if lease.expires_at <= self.policy.clock.now() {
            return Err(self.deny(OwnershipError::Expired));
        }
        self.lease = Some(lease);
        self.validate(coordinator)
    }

    /// `OwnershipApproved` is a request to the coordinator, never proof of approval.
    /// `FailbackComplete` requests the final ownership transfer after preparation.
    /// Stale attempts cannot acquire authority. Retry needs a fresh approval.
    pub fn handle(
        &mut self,
        event: Event,
        coordinator: &mut impl OwnershipCoordinator,
    ) -> Result<State, Error> {
        // First poll the model clock, failing closed on regression before any grant.
        let now = self.policy.clock.now();
        if self.policy.clock_failed || now < self.policy.last_now {
            return self.policy.handle(Event::Tick);
        }
        self.policy.last_now = now;
        // Retain safety inputs even when this same poll discovers a dead lease.
        // Otherwise a lost Quorum(false) could allow a later retry to promote.
        match event {
            Event::Quorum(value) => self.policy.quorum = value,
            Event::CandidateEligible(value) => self.policy.eligible = value,
            _ => {}
        }
        if matches!(
            self.state(),
            State::StandbyReady | State::TemporaryActive | State::FailbackWaiting
        ) && self.validate(coordinator).is_err()
        {
            return Ok(State::Degraded);
        }
        let approve = matches!(event, Event::OwnershipApproved { attempt }
            if attempt == self.attempt() && self.state() == State::FailoverPending);
        let failback = matches!(event, Event::FailbackComplete { attempt }
            if attempt == self.attempt() && self.state() == State::FailbackWaiting
            && now.saturating_sub(self.policy.last_heartbeat) < self.policy.config.suspect_after
            && self.policy.stable_since.is_some_and(|t| now.saturating_sub(t) >= self.policy.config.stable_for));
        if (approve || failback) && self.policy.quorum && self.policy.eligible {
            self.acquire(coordinator)?;
        }
        self.policy.handle(event)
    }

    /// Admission gate; an Active-looking cached state is never sufficient.
    pub fn check_active(
        &mut self,
        coordinator: &mut impl OwnershipCoordinator,
    ) -> Result<OwnershipEpoch, Error> {
        self.handle(Event::Tick, coordinator)?;
        if !matches!(
            self.state(),
            State::TemporaryActive | State::FailbackWaiting
        ) {
            return Err(Error::Ownership(OwnershipError::NotOwned));
        }
        self.validate(coordinator)?;
        self.lease
            .map(|lease| lease.epoch)
            .ok_or(Error::Ownership(OwnershipError::NotOwned))
    }

    /// Renewal is explicit control-plane work, never performed by admission checks.
    pub fn renew(&mut self, coordinator: &mut impl OwnershipCoordinator) -> Result<Lease, Error> {
        self.check_active(coordinator)?;
        let old = self.lease.expect("active admission requires a lease");
        let lease = coordinator
            .renew(self.key, old.epoch, self.ttl)
            .map_err(|e| self.deny(e))?;
        if lease.key != self.key || lease.epoch != old.epoch || lease.expires_at < old.expires_at {
            return Err(self.deny(OwnershipError::Fenced));
        }
        self.lease = Some(lease);
        self.validate(coordinator)?;
        Ok(lease)
    }
}

impl<C: Clock> Failover<C> {
    /// Quorum starts unknown/unavailable; no implicit standalone fallback.
    pub fn new(clock: C, config: Config) -> Result<Self, Error> {
        if config.heartbeat_interval.is_zero()
            || config.heartbeat_interval > config.suspect_after
            || config.suspect_after >= config.fail_after
            || config.stable_for.is_zero()
            || config.cooldown.is_zero()
            || config.max_backoff < config.cooldown
        {
            return Err(Error::InvalidConfig);
        }
        let now = clock.now();
        Ok(Self {
            clock,
            config,
            state: State::Assigned,
            last_now: now,
            last_heartbeat: now,
            stable_since: None,
            retry_at: now,
            backoff: config.cooldown,
            attempt: 0,
            quorum: false,
            eligible: true,
            clock_failed: false,
        })
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn attempt(&self) -> u64 {
        self.attempt
    }
    pub fn retry_at(&self) -> Duration {
        self.retry_at
    }
    pub fn config(&self) -> Config {
        self.config
    }
    fn next_attempt(&mut self) -> Result<(), Error> {
        self.attempt = self.attempt.checked_add(1).ok_or(Error::AttemptExhausted)?;
        Ok(())
    }
    fn dampen(&mut self, now: Duration) {
        self.retry_at = now.saturating_add(self.backoff);
        self.backoff = self.backoff.saturating_mul(2).min(self.config.max_backoff);
    }
    pub fn handle(&mut self, event: Event) -> Result<State, Error> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            self.state = State::Degraded;
            return Err(Error::ClockRegressed);
        }
        self.last_now = now;
        // Evaluate continuity BEFORE accepting a heartbeat, including late polls.
        let age = now.saturating_sub(self.last_heartbeat);
        if self.state == State::FailbackWaiting && age >= self.config.suspect_after {
            self.state = State::TemporaryActive;
            self.stable_since = None;
            self.dampen(now);
        }
        match event {
            Event::Quorum(value) => self.quorum = value,
            Event::CandidateEligible(value) => self.eligible = value,
            Event::Heartbeat => {
                self.last_heartbeat = now;
                match self.state {
                    State::Suspected | State::FailoverPending => {
                        self.state = State::Assigned;
                        self.dampen(now);
                    }
                    State::TemporaryActive if now >= self.retry_at => {
                        self.next_attempt()?;
                        self.state = State::FailbackWaiting;
                        self.stable_since = Some(now);
                    }
                    _ => {}
                }
            }
            Event::OwnershipApproved { attempt }
                if attempt == self.attempt
                    && self.state == State::FailoverPending
                    && self.quorum
                    && self.eligible =>
            {
                self.state = State::StandbyReady
            }
            Event::StartupComplete { attempt }
                if attempt == self.attempt
                    && self.state == State::StandbyReady
                    && self.quorum
                    && self.eligible =>
            {
                self.state = State::TemporaryActive;
                self.dampen(now);
            }
            Event::FailbackComplete { attempt }
                if attempt == self.attempt
                    && self.state == State::FailbackWaiting
                    && self.quorum
                    && self.eligible
                    && self
                        .stable_since
                        .is_some_and(|t| now.saturating_sub(t) >= self.config.stable_for) =>
            {
                self.state = State::Assigned;
                self.stable_since = None;
                self.dampen(now);
            }
            Event::StartupFailed { attempt }
                if attempt == self.attempt
                    && matches!(self.state, State::FailoverPending | State::StandbyReady) =>
            {
                self.state = State::Degraded;
                self.dampen(now);
            }
            _ => {}
        }
        if !self.quorum || !self.eligible {
            self.state = State::Degraded;
            self.stable_since = None;
            return Ok(self.state);
        }
        let age = now.saturating_sub(self.last_heartbeat);
        match self.state {
            State::Assigned | State::Suspected
                if age >= self.config.fail_after && now >= self.retry_at =>
            {
                self.next_attempt()?;
                self.state = State::FailoverPending;
            }
            State::Assigned if age >= self.config.suspect_after => self.state = State::Suspected,
            State::Degraded if now >= self.retry_at => {
                self.next_attempt()?;
                self.state = State::FailoverPending;
            }
            _ => {}
        }
        Ok(self.state)
    }
}
