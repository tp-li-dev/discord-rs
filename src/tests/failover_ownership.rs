use src::{
    failover::{Config, CoordinatedFailover, Error, Event, State},
    ownership::{
        Clock, Lease, OwnershipCoordinator, OwnershipError, OwnershipKey, StandaloneCoordinator,
    },
    protocol::{BotId, OwnershipEpoch},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Default)]
struct Manual(Arc<Mutex<Duration>>);
impl Clock for Manual {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}
impl Manual {
    fn at(&self, ms: u64) {
        *self.0.lock().unwrap() = Duration::from_millis(ms);
    }
}
fn key() -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(7),
        shard_id: 2,
    }
}
fn machine(clock: &Manual) -> CoordinatedFailover<Manual> {
    CoordinatedFailover::new(
        clock.clone(),
        Config::default(),
        key(),
        Duration::from_secs(60),
    )
    .unwrap()
}
fn pending(m: &mut CoordinatedFailover<Manual>, c: &mut impl OwnershipCoordinator) {
    m.handle(Event::Quorum(true), c).unwrap();
    assert_eq!(m.state(), State::FailoverPending);
}
fn approve(m: &mut CoordinatedFailover<Manual>, c: &mut impl OwnershipCoordinator) {
    let attempt = m.attempt();
    assert_eq!(
        m.handle(Event::OwnershipApproved { attempt }, c),
        Ok(State::StandbyReady)
    );
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }, c),
        Ok(State::TemporaryActive)
    );
}

// Scripted control-plane decisions, NOT a quorum/consensus implementation.
// Old Discord connections are represented by a holder continuing to poll and
// request admission independently of control-plane reachability.
struct Fake {
    authority: StandaloneCoordinator<Manual>,
    clock: Manual,
    approved: bool,
    previous_deadline: Duration,
    acquisitions: usize,
}
impl Fake {
    fn new(clock: &Manual) -> Self {
        Self {
            authority: StandaloneCoordinator::new(clock.clone()),
            clock: clock.clone(),
            approved: true,
            previous_deadline: Duration::ZERO,
            acquisitions: 0,
        }
    }
}
impl OwnershipCoordinator for Fake {
    fn acquire(&mut self, key: OwnershipKey, ttl: Duration) -> Result<Lease, OwnershipError> {
        self.acquisitions += 1;
        if !self.approved || self.clock.now() < self.previous_deadline {
            return Err(OwnershipError::QuorumUnavailable);
        }
        let lease = self.authority.acquire(key, ttl)?;
        self.previous_deadline = lease.expires_at;
        Ok(lease)
    }
    fn renew(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError> {
        if !self.approved {
            return Err(OwnershipError::QuorumUnavailable);
        }
        let lease = self.authority.renew(key, epoch, ttl)?;
        self.previous_deadline = lease.expires_at;
        Ok(lease)
    }
    fn release(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        self.authority.release(key, epoch)
    }
    fn check(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        self.authority.check(key, epoch)
    }
    fn watchdog(&mut self) -> Result<Vec<OwnershipKey>, OwnershipError> {
        self.authority.watchdog()
    }
}

#[test]
fn t11_standalone_approval_epoch_and_fencing() {
    let clock = Manual::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let mut m = machine(&clock);
    m.handle(Event::Quorum(true), &mut c).unwrap();
    clock.at(2999);
    assert_eq!(m.handle(Event::Tick, &mut c), Ok(State::Suspected));
    clock.at(3000);
    m.handle(Event::Tick, &mut c).unwrap();
    let attempt = m.attempt();
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }, &mut c),
        Ok(State::FailoverPending)
    );
    approve(&mut m, &mut c);
    let old = m.check_active(&mut c).unwrap();
    let new = c.acquire(key(), Duration::from_secs(60)).unwrap();
    assert!(new.epoch.0 > old.0);
    assert_eq!(c.check(key(), old), Err(OwnershipError::Fenced));
    assert_eq!(m.handle(Event::Tick, &mut c), Ok(State::Degraded));
    assert!(m.check_active(&mut c).is_err());
    assert_eq!(m.lease().unwrap().epoch, old);
}

#[test]
fn lease_deadline_stops_active_and_expired_startup() {
    for startup in [false, true] {
        let clock = Manual::default();
        let mut c = StandaloneCoordinator::new(clock.clone());
        let mut m = machine(&clock);
        clock.at(3000);
        pending(&mut m, &mut c);
        let attempt = m.attempt();
        m.handle(Event::OwnershipApproved { attempt }, &mut c)
            .unwrap();
        if startup {
            m.handle(Event::StartupComplete { attempt }, &mut c)
                .unwrap();
        }
        clock.at(63000);
        assert_eq!(
            m.handle(Event::StartupComplete { attempt }, &mut c),
            Ok(State::Degraded)
        );
        assert!(m.check_active(&mut c).is_err());
        assert_eq!(c.watchdog().unwrap(), vec![key()]);
    }
}

#[test]
fn t12_failback_requires_stability_and_new_coordinator_epoch() {
    let clock = Manual::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let mut m = machine(&clock);
    clock.at(3000);
    pending(&mut m, &mut c);
    approve(&mut m, &mut c);
    let old = m.lease().unwrap().epoch;
    clock.at(6000);
    m.handle(Event::Heartbeat, &mut c).unwrap();
    let attempt = m.attempt();
    for ms in (6500..36000).step_by(500) {
        clock.at(ms);
        m.handle(Event::Heartbeat, &mut c).unwrap();
        assert_eq!(
            m.handle(Event::FailbackComplete { attempt }, &mut c),
            Ok(State::FailbackWaiting)
        );
        assert_eq!(m.lease().unwrap().epoch, old);
    }
    clock.at(36000);
    m.handle(Event::Heartbeat, &mut c).unwrap();
    assert_eq!(
        m.handle(Event::FailbackComplete { attempt }, &mut c),
        Ok(State::Assigned)
    );
    assert!(m.lease().unwrap().epoch.0 > old.0);
    assert_eq!(c.check(key(), old), Err(OwnershipError::Fenced));
    assert!(m.check_active(&mut c).is_err()); // proxy has handed off
}

#[test]
fn t13_partition_keeps_old_discord_alive_until_lease_expires_without_double_active() {
    let clock = Manual::default();
    let mut c = Fake::new(&clock);
    let mut old = machine(&clock);
    let mut new = machine(&clock);
    clock.at(3000);
    pending(&mut old, &mut c);
    approve(&mut old, &mut c);
    let old_epoch = old.lease().unwrap().epoch;
    // New host sees failure, but approval is refused while old local authority lives.
    pending(&mut new, &mut c);
    let attempt = new.attempt();
    assert_eq!(
        new.handle(Event::OwnershipApproved { attempt }, &mut c),
        Err(Error::Ownership(OwnershipError::QuorumUnavailable))
    );
    for ms in [6000, 30000, 62999] {
        clock.at(ms);
        assert!(old.check_active(&mut c).is_ok());
        assert!(new.check_active(&mut c).is_err());
    }
    clock.at(63000);
    // Even before the old watchdog runs, its admission gate rejects at the boundary.
    assert!(old.check_active(&mut c).is_err());
    new.handle(Event::Tick, &mut c).unwrap();
    approve(&mut new, &mut c);
    assert!(new.check_active(&mut c).is_ok());
    assert!(old.check_active(&mut c).is_err());
    assert_eq!(c.check(key(), old_epoch), Err(OwnershipError::Fenced));
}

#[test]
fn t14_quorum_loss_denies_approval_renewal_and_stale_completion() {
    let clock = Manual::default();
    let mut c = Fake::new(&clock);
    let mut m = machine(&clock);
    clock.at(3000);
    pending(&mut m, &mut c);
    c.approved = false;
    let stale = m.attempt();
    assert_eq!(
        m.handle(Event::OwnershipApproved { attempt: stale }, &mut c),
        Err(Error::Ownership(OwnershipError::QuorumUnavailable))
    );
    assert_eq!(m.state(), State::Degraded);
    assert!(m.lease().is_none());
    clock.at(6000);
    m.handle(Event::Tick, &mut c).unwrap();
    let count = c.acquisitions;
    m.handle(Event::OwnershipApproved { attempt: stale }, &mut c)
        .unwrap();
    m.handle(Event::StartupComplete { attempt: stale }, &mut c)
        .unwrap();
    assert_eq!(c.acquisitions, count);
    c.approved = true;
    approve(&mut m, &mut c);
    c.approved = false;
    assert_eq!(
        m.renew(&mut c),
        Err(Error::Ownership(OwnershipError::QuorumUnavailable))
    );
    assert_eq!(m.state(), State::Degraded);
    m.handle(Event::Quorum(false), &mut c).unwrap();
    clock.at(100000);
    assert_eq!(m.handle(Event::Tick, &mut c), Ok(State::Degraded));
    assert!(m.check_active(&mut c).is_err());
}

#[test]
fn renewal_and_clock_regression_fail_closed() {
    let clock = Manual::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let mut m = machine(&clock);
    clock.at(3000);
    pending(&mut m, &mut c);
    approve(&mut m, &mut c);
    clock.at(6000);
    let lease = m.renew(&mut c).unwrap();
    assert_eq!(lease.expires_at, Duration::from_millis(66000));
    clock.at(5999);
    assert_eq!(m.check_active(&mut c), Err(Error::ClockRegressed));
    assert_eq!(m.state(), State::Degraded);
    clock.at(7000);
    assert_eq!(m.handle(Event::Tick, &mut c), Err(Error::ClockRegressed));
}

#[test]
fn unknown_quorum_and_loss_during_startup_cannot_bypass_coordinator() {
    let clock = Manual::default();
    let mut c = Fake::new(&clock);
    let mut m = machine(&clock);
    clock.at(3000);
    assert_eq!(m.handle(Event::Tick, &mut c), Ok(State::Degraded));
    let attempt = m.attempt();
    m.handle(Event::OwnershipApproved { attempt }, &mut c)
        .unwrap();
    assert_eq!(c.acquisitions, 0);
    pending(&mut m, &mut c);
    let attempt = m.attempt();
    m.handle(Event::OwnershipApproved { attempt }, &mut c)
        .unwrap();
    m.handle(Event::Quorum(false), &mut c).unwrap();
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }, &mut c),
        Ok(State::Degraded)
    );
    assert!(m.check_active(&mut c).is_err());
    assert_eq!(c.acquisitions, 1);
}

#[test]
fn failback_approval_refusal_does_not_report_handoff() {
    let clock = Manual::default();
    let mut c = Fake::new(&clock);
    let mut m = machine(&clock);
    clock.at(3000);
    pending(&mut m, &mut c);
    approve(&mut m, &mut c);
    let epoch = m.lease().unwrap().epoch;
    clock.at(6000);
    m.handle(Event::Heartbeat, &mut c).unwrap();
    let attempt = m.attempt();
    for ms in (6500..=36000).step_by(500) {
        clock.at(ms);
        m.handle(Event::Heartbeat, &mut c).unwrap();
    }
    c.approved = false;
    assert_eq!(
        m.handle(Event::FailbackComplete { attempt }, &mut c),
        Err(Error::Ownership(OwnershipError::QuorumUnavailable))
    );
    assert_eq!(m.state(), State::Degraded);
    assert_eq!(m.lease().unwrap().epoch, epoch);
    assert!(m.check_active(&mut c).is_err());
}

#[test]
fn lease_expiry_preserves_simultaneous_quorum_and_candidate_loss() {
    for event in [Event::Quorum(false), Event::CandidateEligible(false)] {
        let clock = Manual::default();
        let mut c = StandaloneCoordinator::new(clock.clone());
        let mut m = machine(&clock);
        clock.at(3000);
        pending(&mut m, &mut c);
        approve(&mut m, &mut c);
        clock.at(63000);
        assert_eq!(m.handle(event, &mut c), Ok(State::Degraded));
        clock.at(100000);
        assert_eq!(m.handle(Event::Tick, &mut c), Ok(State::Degraded));
        assert!(m.check_active(&mut c).is_err());
    }
}
