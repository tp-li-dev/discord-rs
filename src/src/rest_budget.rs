//! Shared, in-process REST admission accounting for one Bot (§13.1).
//!
//! Create exactly one budget per Bot and clone it into all of that Bot's HTTP
//! clients. Clones share local state, not copies of the quota. There is no I/O,
//! sleeping, task creation, token handling, or remote RPC in this module.
//! Independent processes/hosts still need coordinated allocations whose sum is
//! within the Bot budget; creating one full budget on every host is unsafe.
//!
//! Route limits are explicit policy, not hard-coded Discord route limits. The
//! HTTP adapter must normalize method/routes and major resources, and map routes
//! with the same Discord bucket AND major resource to the same opaque key.
//! Do not use raw URLs or webhook/interaction tokens as keys. Parse HTTP responses
//! outside this module and report every 429 using the larger of the applicable
//! Retry-After header/body delays. This module does not retry HTTP operations or
//! decide whether a failed/unknown operation is safe to retry.
//! Registered rolling-window policies are fixed; normal response headers do not
//! automatically replace them or replenish quota. This is an admission policy
//! layer, not a replacement for the HTTP adapter's Discord bucket discovery.

pub use crate::ownership::{Clock, MonotonicClock};
use crate::protocol::BotId;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Caller-interned normalized route/bucket plus major-resource identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RouteKey(pub u64);

/// At most `requests` admissions in any half-open rolling `window`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub requests: u32,
    pub window: Duration,
}

impl RateLimit {
    fn validate(self) -> Result<(), BudgetError> {
        if self.requests == 0 || self.window.is_zero() {
            return Err(BudgetError::InvalidLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestBudgetConfig {
    /// Default policy is 50 requests in a rolling second; it is configurable.
    pub global: RateLimit,
    /// Explicit registration bounds bucket cardinality. Buckets are never evicted
    /// automatically, since eviction could discard a live 429/quota restriction.
    pub max_routes: usize,
    /// Additive jitter per 429 observation, sampled once in [0, max_jitter].
    /// Must fit in u64 nanoseconds. Zero disables jitter for deterministic policy.
    pub max_jitter: Duration,
}

impl Default for RestBudgetConfig {
    fn default() -> Self {
        Self {
            global: RateLimit {
                requests: 50,
                window: Duration::from_secs(1),
            },
            max_routes: 1024,
            max_jitter: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetError {
    InvalidBot,
    InvalidLimit,
    InvalidCapacity,
    InvalidJitter,
    UnknownRoute,
    DuplicateRoute,
    RouteCapacity,
    ClockRegressed,
    TimeOverflow,
    Poisoned,
}

impl std::fmt::Display for BudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "REST budget error: {self:?}")
    }
}
impl std::error::Error for BudgetError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReason {
    GlobalLimit,
    RouteLimit,
    Global429,
    Route429,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Consumed both quotas. Dispatch promptly; cancelled admissions are not
    /// refunded. Holding this result does not reserve permission for a later time.
    Granted,
    /// Earliest time to recheck in this budget's local Clock domain. This is not
    /// a reservation: other clients and later responses can postpone admission.
    Wait {
        retry_at: Duration,
        reason: WaitReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimited {
    pub retry_after: Duration,
    pub global: bool,
}

struct Window {
    policy: RateLimit,
    expires: VecDeque<Duration>,
}

impl Window {
    fn new(policy: RateLimit) -> Self {
        Self {
            policy,
            expires: VecDeque::new(),
        }
    }

    fn blocked_until(&mut self, now: Duration) -> Option<Duration> {
        while self.expires.front().is_some_and(|at| *at <= now) {
            self.expires.pop_front();
        }
        if self.expires.len() >= self.policy.requests as usize {
            self.expires.front().copied()
        } else {
            None
        }
    }
}

struct Route {
    window: Window,
    blocked_until: Duration,
}

struct State {
    global: Window,
    routes: HashMap<RouteKey, Route>,
    global_blocked_until: Duration,
    last_now: Duration,
    failure: Option<BudgetError>,
    rng: u64,
}

impl State {
    fn now(&mut self, clock: &impl Clock) -> Result<Duration, BudgetError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let now = clock.now();
        if now < self.last_now {
            self.failure = Some(BudgetError::ClockRegressed);
            return Err(BudgetError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    fn add(&mut self, now: Duration, delay: Duration) -> Result<Duration, BudgetError> {
        now.checked_add(delay).ok_or_else(|| {
            // An unrepresentable restriction must never turn into permission.
            self.failure = Some(BudgetError::TimeOverflow);
            BudgetError::TimeOverflow
        })
    }

    fn jitter(&mut self, max: Duration) -> Duration {
        // Seeded non-cryptographic jitter; no secret/randomness is required for
        // safety. Inject independently chosen seeds to decorrelate separate Bots.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        Duration::from_nanos((u128::from(self.rng) % (max.as_nanos() + 1)) as u64)
    }
}

struct Shared<C> {
    bot: BotId,
    config: RestBudgetConfig,
    clock: C,
    state: Mutex<State>,
}

/// Pure admission/accounting state; thread-safe clones share the same Bot quota.
/// Clock callbacks must be nonblocking and must not call back into this budget.
/// Clock regression, arithmetic overflow and lock poisoning fail closed.
pub struct RestBudget<C = MonotonicClock> {
    shared: Arc<Shared<C>>,
}

impl<C> Clone for RestBudget<C> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<C: Clock> RestBudget<C> {
    /// `seed` makes jitter reproducible; choose it independently for unrelated
    /// budgets. No OS randomness or credentials are accessed by this constructor.
    pub fn new(
        bot: BotId,
        config: RestBudgetConfig,
        clock: C,
        seed: u64,
    ) -> Result<Self, BudgetError> {
        if bot.0 == 0 {
            return Err(BudgetError::InvalidBot);
        }
        config.global.validate()?;
        if config.max_routes == 0 {
            return Err(BudgetError::InvalidCapacity);
        }
        if config.max_jitter.as_nanos() > u128::from(u64::MAX) {
            return Err(BudgetError::InvalidJitter);
        }
        let rng = seed ^ bot.0.rotate_left(23);
        let last_now = clock.now();
        Ok(Self {
            shared: Arc::new(Shared {
                bot,
                config,
                clock,
                state: Mutex::new(State {
                    global: Window::new(config.global),
                    routes: HashMap::new(),
                    global_blocked_until: Duration::ZERO,
                    last_now,
                    failure: None,
                    rng: if rng == 0 { 0x9e3779b97f4a7c15 } else { rng },
                }),
            }),
        })
    }

    pub fn bot_id(&self) -> BotId {
        self.shared.bot
    }

    /// Register one policy per canonical bucket before dispatch. Duplicate
    /// registration never resets spent quota or an outstanding 429 restriction.
    /// Use response-derived route policies in the HTTP adapter where available.
    pub fn register_route(&self, key: RouteKey, policy: RateLimit) -> Result<(), BudgetError> {
        policy.validate()?;
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| BudgetError::Poisoned)?;
        state.now(&self.shared.clock)?;
        if state.routes.contains_key(&key) {
            return Err(BudgetError::DuplicateRoute);
        }
        if state.routes.len() >= self.shared.config.max_routes {
            return Err(BudgetError::RouteCapacity);
        }
        state.routes.insert(
            key,
            Route {
                window: Window::new(policy),
                blocked_until: Duration::ZERO,
            },
        );
        Ok(())
    }

    /// Atomically check and spend both rolling quotas. Waiting spends neither;
    /// call again when due. All requests count toward the configured Bot policy
    /// (including globally exempt endpoints, conservatively).
    pub fn try_acquire(&self, key: RouteKey) -> Result<Admission, BudgetError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| BudgetError::Poisoned)?;
        let now = state.now(&self.shared.clock)?;
        let route = state
            .routes
            .get_mut(&key)
            .ok_or(BudgetError::UnknownRoute)?;
        let mut wait: Option<(Duration, WaitReason)> = None;
        let mut consider = |at: Duration, reason| {
            if at > now && wait.is_none_or(|(previous, _)| at > previous) {
                wait = Some((at, reason));
            }
        };
        consider(route.blocked_until, WaitReason::Route429);
        if let Some(at) = route.window.blocked_until(now) {
            consider(at, WaitReason::RouteLimit);
        }
        let route_window = route.window.policy.window;
        consider(state.global_blocked_until, WaitReason::Global429);
        if let Some(at) = state.global.blocked_until(now) {
            consider(at, WaitReason::GlobalLimit);
        }
        if let Some((retry_at, reason)) = wait {
            return Ok(Admission::Wait { retry_at, reason });
        }
        let global_window = state.global.policy.window;
        let global_expiry = state.add(now, global_window)?;
        let route_expiry = state.add(now, route_window)?;
        state.global.expires.push_back(global_expiry);
        state
            .routes
            .get_mut(&key)
            .ok_or(BudgetError::UnknownRoute)?
            .window
            .expires
            .push_back(route_expiry);
        Ok(Admission::Granted)
    }

    /// Report a decoded 429. The deadline includes additive jitter and can only
    /// extend an existing restriction. `global` affects every registered route
    /// and is honored even if `key` is unknown. No quota is refunded/reset.
    /// A zero delay is permitted; configured rolling limits still apply.
    pub fn rate_limited(
        &self,
        key: RouteKey,
        response: RateLimited,
    ) -> Result<Duration, BudgetError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| BudgetError::Poisoned)?;
        let now = state.now(&self.shared.clock)?;
        if !response.global && !state.routes.contains_key(&key) {
            return Err(BudgetError::UnknownRoute);
        }
        let jitter = state.jitter(self.shared.config.max_jitter);
        let minimum = state.add(now, response.retry_after)?;
        let deadline = state.add(minimum, jitter)?;
        let target = if response.global {
            &mut state.global_blocked_until
        } else {
            &mut state
                .routes
                .get_mut(&key)
                .ok_or(BudgetError::UnknownRoute)?
                .blocked_until
        };
        *target = (*target).max(deadline);
        Ok(*target)
    }
}
