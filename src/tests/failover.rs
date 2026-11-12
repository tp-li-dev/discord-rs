use src::failover::{Clock, Config, Error, Event, Failover, State};
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
fn machine() -> (Manual, Failover<Manual>) {
    let c = Manual::default();
    let mut m = Failover::new(c.clone(), Config::default()).unwrap();
    m.handle(Event::Quorum(true)).unwrap();
    (c, m)
}
fn activate(c: &Manual, m: &mut Failover<Manual>) {
    c.at(3000);
    assert_eq!(m.handle(Event::Tick), Ok(State::FailoverPending));
    let attempt = m.attempt();
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }),
        Ok(State::FailoverPending)
    );
    assert_eq!(
        m.handle(Event::OwnershipApproved { attempt }),
        Ok(State::StandbyReady)
    );
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }),
        Ok(State::TemporaryActive)
    );
}
#[test]
fn detection_boundaries_and_approval_gate() {
    let (c, mut m) = machine();
    assert_eq!(m.config().heartbeat_interval, Duration::from_millis(500));
    for (ms, state) in [
        (1499, State::Assigned),
        (1500, State::Suspected),
        (2999, State::Suspected),
        (3000, State::FailoverPending),
    ] {
        c.at(ms);
        assert_eq!(m.handle(Event::Tick), Ok(state));
    }
    let a = m.attempt();
    m.handle(Event::OwnershipApproved { attempt: a + 1 })
        .unwrap();
    assert_eq!(m.state(), State::FailoverPending);
    m.handle(Event::OwnershipApproved { attempt: a }).unwrap();
    assert_eq!(m.state(), State::StandbyReady);
    m.handle(Event::StartupComplete { attempt: a }).unwrap();
    assert_eq!(m.state(), State::TemporaryActive);
}
#[test]
fn quorum_loss_and_ineligible_candidate_never_promote() {
    for event in [Event::Quorum(false), Event::CandidateEligible(false)] {
        let (c, mut m) = machine();
        c.at(3000);
        m.handle(Event::Tick).unwrap();
        let old = m.attempt();
        assert_eq!(m.handle(event), Ok(State::Degraded));
        for e in [
            Event::OwnershipApproved { attempt: old },
            Event::StartupComplete { attempt: old },
            Event::Tick,
        ] {
            assert_eq!(m.handle(e), Ok(State::Degraded));
        }
        m.handle(Event::Quorum(true)).unwrap();
        m.handle(Event::CandidateEligible(true)).unwrap();
        assert_eq!(m.state(), State::FailoverPending);
        assert!(m.attempt() > old);
        assert_eq!(
            m.handle(Event::OwnershipApproved { attempt: old }),
            Ok(State::FailoverPending)
        );
    }
}
#[test]
fn stable_recovery_requires_thirty_seconds_and_explicit_handoff() {
    let (c, mut m) = machine();
    activate(&c, &mut m);
    c.at(6000);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::FailbackWaiting));
    let attempt = m.attempt();
    for ms in (6500..36000).step_by(500) {
        c.at(ms);
        m.handle(Event::Heartbeat).unwrap();
        assert_eq!(
            m.handle(Event::FailbackComplete { attempt }),
            Ok(State::FailbackWaiting)
        );
    }
    c.at(36000);
    m.handle(Event::Heartbeat).unwrap();
    assert_eq!(m.handle(Event::Tick), Ok(State::FailbackWaiting));
    assert_eq!(
        m.handle(Event::FailbackComplete { attempt }),
        Ok(State::Assigned)
    );
}
#[test]
fn unstable_recovery_and_late_heartbeat_restart_stability() {
    let (c, mut m) = machine();
    activate(&c, &mut m);
    c.at(6000);
    m.handle(Event::Heartbeat).unwrap();
    let old = m.attempt();
    c.at(7499);
    assert_eq!(m.handle(Event::Tick), Ok(State::FailbackWaiting));
    c.at(7500);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::TemporaryActive));
    assert_eq!(m.retry_at(), Duration::from_millis(13500));
    c.at(13499);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::TemporaryActive));
    c.at(13500);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::FailbackWaiting));
    assert!(m.attempt() > old);
    c.at(43500);
    assert_eq!(
        m.handle(Event::FailbackComplete { attempt: old }),
        Ok(State::TemporaryActive)
    );
}
#[test]
fn startup_failure_backoff_doubles_and_caps() {
    let (c, mut m) = machine();
    c.at(3000);
    m.handle(Event::Tick).unwrap();
    let mut now = 3000;
    for delay in [3000, 6000, 12000, 24000, 48000, 60000, 60000] {
        let attempt = m.attempt();
        assert_eq!(
            m.handle(Event::StartupFailed { attempt }),
            Ok(State::Degraded)
        );
        now += delay;
        assert_eq!(m.retry_at(), Duration::from_millis(now));
        c.at(now - 1);
        assert_eq!(m.handle(Event::Tick), Ok(State::Degraded));
        c.at(now);
        assert_eq!(m.handle(Event::Tick), Ok(State::FailoverPending));
    }
}
#[test]
fn configuration_and_clock_failure() {
    let c = Manual::default();
    assert!(matches!(
        Failover::new(
            c.clone(),
            Config {
                heartbeat_interval: Duration::ZERO,
                ..Config::default()
            }
        ),
        Err(Error::InvalidConfig)
    ));
    let mut m = Failover::new(
        c.clone(),
        Config {
            heartbeat_interval: Duration::from_millis(10),
            suspect_after: Duration::from_millis(20),
            fail_after: Duration::from_millis(40),
            ..Config::default()
        },
    )
    .unwrap();
    m.handle(Event::Quorum(true)).unwrap();
    c.at(20);
    assert_eq!(m.handle(Event::Tick), Ok(State::Suspected));
    c.at(40);
    assert_eq!(m.handle(Event::Tick), Ok(State::FailoverPending));
    c.at(39);
    assert_eq!(m.handle(Event::Tick), Err(Error::ClockRegressed));
    c.at(100);
    assert_eq!(m.handle(Event::Quorum(true)), Err(Error::ClockRegressed));
    assert_eq!(m.state(), State::Degraded);
}
#[test]
fn heartbeat_cancels_suspicion_and_invalidates_old_readiness() {
    let (c, mut m) = machine();
    c.at(1500);
    m.handle(Event::Tick).unwrap();
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::Assigned));
    c.at(4500);
    m.handle(Event::Tick).unwrap();
    let old = m.attempt();
    m.handle(Event::Heartbeat).unwrap();
    c.at(10500);
    m.handle(Event::Tick).unwrap();
    assert!(m.attempt() > old);
    assert_eq!(
        m.handle(Event::OwnershipApproved { attempt: old }),
        Ok(State::FailoverPending)
    );
}

#[test]
fn approval_survives_heartbeat_but_not_quorum_loss() {
    let (c, mut m) = machine();
    c.at(3000);
    m.handle(Event::Tick).unwrap();
    let attempt = m.attempt();
    m.handle(Event::OwnershipApproved { attempt }).unwrap();
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::StandbyReady));
    assert_eq!(m.handle(Event::Quorum(false)), Ok(State::Degraded));
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }),
        Ok(State::Degraded)
    );
    assert_eq!(m.handle(Event::Quorum(true)), Ok(State::FailoverPending));
    assert_eq!(
        m.handle(Event::StartupComplete { attempt }),
        Ok(State::FailoverPending)
    );
}

#[test]
fn unknown_quorum_and_independent_shards() {
    let c = Manual::default();
    let mut unknown = Failover::new(c.clone(), Config::default()).unwrap();
    let mut healthy = Failover::new(c.clone(), Config::default()).unwrap();
    healthy.handle(Event::Quorum(true)).unwrap();
    c.at(3000);
    assert_eq!(unknown.handle(Event::Tick), Ok(State::Degraded));
    assert_eq!(healthy.handle(Event::Heartbeat), Ok(State::Assigned));
}

#[test]
fn customized_recovery_and_cooldown_are_used() {
    let c = Manual::default();
    let mut m = Failover::new(
        c.clone(),
        Config {
            heartbeat_interval: Duration::from_millis(10),
            suspect_after: Duration::from_millis(20),
            fail_after: Duration::from_millis(40),
            stable_for: Duration::from_millis(60),
            cooldown: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
        },
    )
    .unwrap();
    m.handle(Event::Quorum(true)).unwrap();
    c.at(40);
    m.handle(Event::Tick).unwrap();
    let attempt = m.attempt();
    m.handle(Event::OwnershipApproved { attempt }).unwrap();
    m.handle(Event::StartupComplete { attempt }).unwrap();
    c.at(49);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::TemporaryActive));
    c.at(50);
    assert_eq!(m.handle(Event::Heartbeat), Ok(State::FailbackWaiting));
    let attempt = m.attempt();
    for ms in (60..=110).step_by(10) {
        c.at(ms);
        m.handle(Event::Heartbeat).unwrap();
    }
    assert_eq!(
        m.handle(Event::FailbackComplete { attempt }),
        Ok(State::Assigned)
    );
}
