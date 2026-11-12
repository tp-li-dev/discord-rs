//! Per-shard detection policy, with no I/O or ownership authority of its own.
//! Trusted adapters supply authenticated heartbeats, quorum status and completion
//! events only after coordinator approval / process preparation. State alone is
//! never permission for side effects; callers must enforce leases and fencing.
pub use crate::ownership::Clock;
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
