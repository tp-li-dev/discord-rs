//! Per-Bot admission for real Gateway launches in one botd. With send grants
//! disabled the launch history is process-local. Both modes use this policy.

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use crate::daemon::{LaunchGeneration, ShardKey};

/// Minimum spacing between two Identify grants of the same Bot. Discord allows
/// one Identify per 5 seconds per max_concurrency bucket; 30
/// seconds keeps a crash loop far from that limit and leaves time for a
/// previous Gateway to be observed as gone.
pub const MIN_IDENTIFY_INTERVAL: Duration = Duration::from_secs(30);

/// Rolling window for the per-Bot Identify count.
pub const IDENTIFY_WINDOW: Duration = Duration::from_secs(60 * 60);

/// Maximum Identifies per Bot in `IDENTIFY_WINDOW`. Discord's hard limit is
/// 1000 per day (a token reset follows when exceeded); 10 per hour caps a
/// runaway restart loop at 240 per day, on the safe side of that limit, while
/// still allowing several genuine recoveries.
pub const MAX_IDENTIFIES_PER_WINDOW: u32 = 10;

/// Upper bound on the local policy's rolling count.
const MAX_RECORDS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalIdentifyPolicy {
    pub min_interval: Duration,
    pub window: Duration,
    pub max_per_window: u32,
}

impl Default for LocalIdentifyPolicy {
    fn default() -> Self {
        Self {
            min_interval: MIN_IDENTIFY_INTERVAL,
            window: IDENTIFY_WINDOW,
            max_per_window: MAX_IDENTIFIES_PER_WINDOW,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalAdmission {
    /// Permitted and recorded.
    Granted,
    /// Too soon after the previous grant; nothing recorded. Try again later.
    Pending { retry_after: Duration },
    /// The rolling-window count is exhausted; nothing recorded. Automatic
    /// recovery must stop; `retry_after` is when the oldest grant leaves it.
    Exhausted { retry_after: Duration },
}

/// Per-Bot grant history.
#[derive(Clone, Debug)]
pub struct LocalIdentifyAdmission {
    policy: LocalIdentifyPolicy,
    grants: VecDeque<Charge>,
    last_grant: Option<Duration>,
}

#[derive(Clone, Debug)]
struct Charge {
    at: Duration,
    #[cfg(target_os = "linux")]
    launch: Option<LaunchGeneration>,
}

impl LocalIdentifyAdmission {
    pub fn new(policy: LocalIdentifyPolicy) -> Self {
        Self {
            policy,
            grants: VecDeque::new(),
            last_grant: None,
        }
    }

    fn prune(&mut self, now: Duration) {
        while self
            .grants
            .front()
            .is_some_and(|charge| now.saturating_sub(charge.at) >= self.policy.window)
        {
            self.grants.pop_front();
        }
    }

    /// Decide and record a single grant. A standby also needs capacity for a
    /// subsequent fallback, which will request its own grant if needed.
    fn try_acquire_slots(&mut self, now: Duration, required_slots: usize) -> LocalAdmission {
        let admission = self.check_slots(now, required_slots);
        if admission == LocalAdmission::Granted {
            self.push(now);
        }
        admission
    }

    pub(crate) fn check_slots(&mut self, now: Duration, required_slots: usize) -> LocalAdmission {
        self.prune(now);
        let capacity = (self.policy.max_per_window as usize).min(MAX_RECORDS);
        if required_slots > capacity {
            return LocalAdmission::Exhausted {
                retry_after: Duration::MAX,
            };
        }
        if self.grants.len() >= capacity {
            return LocalAdmission::Exhausted {
                retry_after: self.window_retry_after(capacity, required_slots, now),
            };
        }
        let cooldown = self.last_grant.map_or(Duration::ZERO, |last| {
            self.policy
                .min_interval
                .saturating_add(last)
                .saturating_sub(now)
        });
        let window_retry = self.window_retry_after(capacity, required_slots, now);
        if window_retry > cooldown {
            return LocalAdmission::Exhausted {
                retry_after: window_retry,
            };
        }
        if !cooldown.is_zero() {
            return LocalAdmission::Pending {
                retry_after: cooldown,
            };
        }
        LocalAdmission::Granted
    }

    fn window_retry_after(
        &self,
        capacity: usize,
        required_slots: usize,
        now: Duration,
    ) -> Duration {
        let expiring = (self.grants.len() + required_slots).saturating_sub(capacity);
        if expiring == 0 {
            return Duration::ZERO;
        }
        self.policy
            .window
            .saturating_add(self.grants[expiring - 1].at)
            .saturating_sub(now)
    }

    pub fn try_acquire(&mut self, now: Duration) -> LocalAdmission {
        self.try_acquire_slots(now, 1)
    }

    #[cfg(test)]
    fn record(&mut self, now: Duration) {
        self.prune(now);
        self.push(now);
    }

    fn push(&mut self, now: Duration) {
        self.push_charge(Charge {
            at: now,
            #[cfg(target_os = "linux")]
            launch: None,
        });
    }

    fn push_charge(&mut self, charge: Charge) {
        if self.grants.len() == MAX_RECORDS {
            self.grants.pop_front();
        }
        self.last_grant = Some(charge.at);
        self.grants.push_back(charge);
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn record_launch(&mut self, now: Duration, launch: LaunchGeneration) {
        self.prune(now);
        self.push_charge(Charge {
            at: now,
            launch: Some(launch),
        });
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn remove_launch(&mut self, now: Duration, launch: &LaunchGeneration) -> bool {
        self.prune(now);
        let Some(index) = self
            .grants
            .iter()
            .position(|c| c.launch.as_ref() == Some(launch))
        else {
            return false;
        };
        self.grants.remove(index);
        self.last_grant = self.grants.back().map(|charge| charge.at);
        true
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn record_permit(&mut self, expires: Duration) {
        self.push(expires);
    }
}

/// Independent history per Bot ID.
#[derive(Default)]
pub struct LocalIdentifyRegistry {
    policy: LocalIdentifyPolicy,
    bots: HashMap<String, LocalIdentifyAdmission>,
    restart_slots: HashMap<String, usize>,
    #[cfg(target_os = "linux")]
    pub(crate) send_grants: Option<crate::identify_grant::service::Service>,
}

impl LocalIdentifyRegistry {
    pub fn with_policy(policy: LocalIdentifyPolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    pub fn set_policy(&mut self, policy: LocalIdentifyPolicy) {
        self.policy = policy;
        self.bots.clear();
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn policy(&self) -> LocalIdentifyPolicy {
        self.policy
    }

    fn bot(&mut self, bot_id: &str) -> &mut LocalIdentifyAdmission {
        let policy = self.policy;
        self.bots
            .entry(bot_id.to_owned())
            .or_insert_with(|| LocalIdentifyAdmission::new(policy))
    }

    pub fn try_acquire(&mut self, bot_id: &str, now: Duration) -> LocalAdmission {
        self.try_acquire_slots(bot_id, now, 1)
    }

    pub(crate) fn try_acquire_standby(&mut self, bot_id: &str, now: Duration) -> LocalAdmission {
        self.try_acquire_slots(bot_id, now, 2)
    }

    pub(crate) fn acquire_gateway(
        &mut self,
        key: &ShardKey,
        _generation: LaunchGeneration,
        now: Duration,
        standby: bool,
        reserved_restart: bool,
    ) -> LocalAdmission {
        let bot = &key.bot_id;
        #[cfg(target_os = "linux")]
        if let Some(service) = &mut self.send_grants {
            if !reserved_restart && self.restart_slots.contains_key(bot) {
                return LocalAdmission::Pending {
                    retry_after: Duration::ZERO,
                };
            }
            let admission = service.launch(key, _generation, if standby { 2 } else { 1 });
            if admission == LocalAdmission::Granted && reserved_restart {
                self.take_restart_slot(bot);
            }
            return admission;
        }
        if reserved_restart {
            self.consume_restart(bot, now);
            LocalAdmission::Granted
        } else if standby {
            self.try_acquire_standby(bot, now)
        } else {
            self.try_acquire(bot, now)
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn poll_send_grants(&mut self, fallback_bots: &std::collections::HashSet<&str>) {
        if let Some(service) = &mut self.send_grants {
            service.poll(|bot| {
                (
                    self.restart_slots.contains_key(bot),
                    fallback_bots.contains(bot),
                )
            });
        }
    }

    fn try_acquire_slots(
        &mut self,
        bot_id: &str,
        now: Duration,
        required_slots: usize,
    ) -> LocalAdmission {
        if self.restart_slots.contains_key(bot_id) {
            return LocalAdmission::Pending {
                retry_after: Duration::ZERO,
            };
        }
        self.bot(bot_id).try_acquire_slots(now, required_slots)
    }

    /// Reserve every replacement without recording a launch. The daemon is
    /// the single owner, so it can reserve several Bots in one admission.
    pub(crate) fn reserve_restart(
        &mut self,
        bot_id: &str,
        now: Duration,
        slots: usize,
    ) -> LocalAdmission {
        if self.restart_slots.contains_key(bot_id) {
            return LocalAdmission::Pending {
                retry_after: Duration::ZERO,
            };
        }
        #[cfg(target_os = "linux")]
        let admission = match &mut self.send_grants {
            Some(service) => service.check(bot_id, slots),
            None => self.bot(bot_id).check_slots(now, slots),
        };
        #[cfg(not(target_os = "linux"))]
        let admission = self.bot(bot_id).check_slots(now, slots);
        if admission != LocalAdmission::Granted {
            return admission;
        }
        if slots > 1 && !self.policy.min_interval.is_zero() {
            return LocalAdmission::Pending {
                retry_after: self.policy.min_interval,
            };
        }
        self.restart_slots.insert(bot_id.to_owned(), slots);
        LocalAdmission::Granted
    }

    /// Charge at the replacement's launch boundary, even if spawn then fails.
    pub(crate) fn consume_restart(&mut self, bot_id: &str, now: Duration) {
        self.take_restart_slot(bot_id);
        let history = self.bot(bot_id);
        history.prune(now);
        history.push(now);
    }

    fn take_restart_slot(&mut self, bot_id: &str) {
        let slots = self
            .restart_slots
            .get_mut(bot_id)
            .expect("reserved restart");
        *slots -= 1;
        if *slots == 0 {
            self.release_restart(bot_id);
        }
    }

    pub(crate) fn release_restart(&mut self, bot_id: &str) {
        self.restart_slots.remove(bot_id);
    }

    #[cfg(test)]
    pub(crate) fn record(&mut self, bot_id: &str, now: Duration) {
        self.bot(bot_id).record(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn restart_reservation_blocks_other_launches_until_release_or_consumption() {
        let mut registry = LocalIdentifyRegistry::default();
        assert_eq!(registry.try_acquire("A", secs(0)), LocalAdmission::Granted);
        assert_eq!(
            registry.reserve_restart("A", secs(30), 1),
            LocalAdmission::Granted
        );
        assert!(matches!(
            registry.try_acquire("A", secs(30)),
            LocalAdmission::Pending { .. }
        ));
        assert!(matches!(
            registry.try_acquire_standby("A", secs(30)),
            LocalAdmission::Pending { .. }
        ));
        assert_eq!(registry.try_acquire("B", secs(30)), LocalAdmission::Granted);

        registry.release_restart("A");
        assert_eq!(
            registry.reserve_restart("A", secs(30), 1),
            LocalAdmission::Granted
        );
        registry.consume_restart("A", secs(60));
        assert_eq!(
            registry.try_acquire("A", secs(89)),
            LocalAdmission::Pending {
                retry_after: secs(1)
            }
        );
        assert_eq!(registry.try_acquire("A", secs(90)), LocalAdmission::Granted);
    }

    #[test]
    fn first_is_granted_and_next_within_interval_is_pending_with_remaining_time() {
        let t0 = Duration::ZERO;
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        assert_eq!(a.try_acquire(t0), LocalAdmission::Granted);
        assert_eq!(
            a.try_acquire(t0 + secs(10)),
            LocalAdmission::Pending {
                retry_after: secs(20)
            }
        );
        // Pending records nothing: the interval still counts from the grant.
        assert_eq!(a.try_acquire(t0 + secs(30)), LocalAdmission::Granted);
    }

    #[test]
    fn eleventh_identify_in_an_hour_is_exhausted_and_recovers_after_the_window() {
        let t0 = Duration::ZERO;
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        for i in 0..10 {
            assert_eq!(a.try_acquire(t0 + secs(30 * i)), LocalAdmission::Granted);
        }
        // Oldest grant is at t0; the window ends at t0 + 3600.
        assert_eq!(
            a.try_acquire(t0 + secs(300)),
            LocalAdmission::Exhausted {
                retry_after: secs(3300)
            }
        );
        assert!(matches!(
            a.try_acquire(t0 + secs(3599)),
            LocalAdmission::Exhausted { .. }
        ));
        assert_eq!(a.try_acquire(t0 + secs(3600)), LocalAdmission::Granted);
    }

    #[test]
    fn record_counts_and_spaces_without_refusing() {
        let t0 = Duration::ZERO;
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        a.record(t0);
        assert_eq!(
            a.try_acquire(t0 + secs(5)),
            LocalAdmission::Pending {
                retry_after: secs(25)
            }
        );
        for i in 0..20 {
            a.record(t0 + secs(i));
        }
        assert!(matches!(
            a.try_acquire(t0 + secs(100)),
            LocalAdmission::Exhausted { .. }
        ));
    }

    #[test]
    fn bots_are_independent() {
        let t0 = Duration::ZERO;
        let mut r = LocalIdentifyRegistry::default();
        assert_eq!(r.try_acquire("a", t0), LocalAdmission::Granted);
        assert_eq!(r.try_acquire("b", t0), LocalAdmission::Granted);
        assert!(matches!(
            r.try_acquire("a", t0 + secs(1)),
            LocalAdmission::Pending { .. }
        ));
        r.record("b", t0);
        assert!(matches!(
            r.try_acquire("b", t0 + secs(1)),
            LocalAdmission::Pending { .. }
        ));
    }

    #[test]
    fn standby_at_cooldown_boundary_spends_one_of_two_required_slots() {
        let mut registry = LocalIdentifyRegistry::with_policy(LocalIdentifyPolicy {
            max_per_window: 2,
            ..Default::default()
        });
        assert_eq!(registry.try_acquire("a", secs(0)), LocalAdmission::Granted);
        assert_eq!(
            registry.try_acquire_standby("a", secs(30)),
            LocalAdmission::Exhausted {
                retry_after: secs(3570)
            }
        );
        assert_eq!(
            registry.try_acquire_standby("a", secs(3600)),
            LocalAdmission::Granted
        );
        assert_eq!(
            registry.try_acquire("a", secs(3630)),
            LocalAdmission::Granted
        );
        assert!(matches!(
            registry.try_acquire("a", secs(3660)),
            LocalAdmission::Exhausted { .. }
        ));
    }

    #[test]
    fn cooldown_can_outlive_the_rolling_window() {
        let mut admission = LocalIdentifyAdmission::new(LocalIdentifyPolicy {
            min_interval: secs(30),
            window: secs(5),
            max_per_window: 1,
        });
        assert_eq!(admission.try_acquire(secs(0)), LocalAdmission::Granted);
        assert_eq!(
            admission.try_acquire(secs(5)),
            LocalAdmission::Pending {
                retry_after: secs(25)
            }
        );
        assert_eq!(admission.try_acquire(secs(30)), LocalAdmission::Granted);
    }

    #[test]
    fn history_is_bounded() {
        let t0 = Duration::ZERO;
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        for _ in 0..(MAX_RECORDS * 2) {
            a.record(t0);
        }
        assert_eq!(a.grants.len(), MAX_RECORDS);
    }
}
