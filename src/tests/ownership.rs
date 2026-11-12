use src::{
    ownership::*,
    protocol::{BotId, OwnershipEpoch},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Default)]
struct FakeClock(Arc<Mutex<Duration>>);
impl FakeClock {
    fn set(&self, seconds: u64) {
        *self.0.lock().unwrap() = Duration::from_secs(seconds);
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}
fn key(bot: u64, shard_id: u32) -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(bot),
        shard_id,
    }
}
const TTL: Duration = Duration::from_secs(10);

#[test]
fn epochs_increase_and_fence_every_old_command() {
    let mut c = StandaloneCoordinator::new(FakeClock::default());
    let k = key(1, 0);
    assert_eq!(c.check(k, OwnershipEpoch(1)), Err(OwnershipError::NotOwned));
    let first = c.acquire(k, TTL).unwrap();
    let second = c.acquire(k, TTL).unwrap();
    assert!(second.epoch.0 > first.epoch.0);
    assert_eq!(c.check(k, first.epoch), Err(OwnershipError::Fenced));
    assert_eq!(c.renew(k, first.epoch, TTL), Err(OwnershipError::Fenced));
    assert_eq!(c.release(k, first.epoch), Err(OwnershipError::Fenced));
    assert_eq!(
        c.check(k, OwnershipEpoch(second.epoch.0 + 1)),
        Err(OwnershipError::Fenced)
    );
    c.check(k, second.epoch).unwrap();
    c.release(k, second.epoch).unwrap();
    assert_eq!(c.check(k, second.epoch), Err(OwnershipError::Expired));
    assert_eq!(c.renew(k, second.epoch, TTL), Err(OwnershipError::Expired));
    assert!(c.acquire(k, TTL).unwrap().epoch.0 > second.epoch.0);
}

#[test]
fn bots_and_shards_have_independent_authority() {
    let mut c = StandaloneCoordinator::new(FakeClock::default());
    let keys = [key(1, 0), key(1, 1), key(2, 0)];
    for k in keys {
        assert_eq!(c.acquire(k, TTL).unwrap().epoch, OwnershipEpoch(1));
    }
    c.acquire(keys[0], TTL).unwrap();
    for k in &keys[1..] {
        c.check(*k, OwnershipEpoch(1)).unwrap();
    }
}

#[test]
fn expiry_boundary_denies_work_without_watchdog_and_cannot_renew() {
    let clock = FakeClock::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let k = key(1, 0);
    let lease = c.acquire(k, TTL).unwrap();
    clock.set(9);
    c.check(k, lease.epoch).unwrap();
    clock.set(10);
    assert_eq!(c.check(k, lease.epoch), Err(OwnershipError::Expired));
    assert_eq!(c.renew(k, lease.epoch, TTL), Err(OwnershipError::Expired));
    clock.set(11);
    assert_eq!(c.release(k, lease.epoch), Err(OwnershipError::Expired));
    assert_eq!(c.watchdog().unwrap(), vec![k]);
    assert_eq!(c.acquire(k, TTL).unwrap().epoch, OwnershipEpoch(2));
}

#[test]
fn watchdog_invalidates_only_expired_leases_once() {
    let clock = FakeClock::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let a = c.acquire(key(1, 0), TTL).unwrap();
    let b = c.acquire(key(1, 1), Duration::from_secs(20)).unwrap();
    clock.set(9);
    assert!(c.watchdog().unwrap().is_empty());
    clock.set(10);
    assert_eq!(c.watchdog().unwrap(), vec![a.key]);
    assert!(c.watchdog().unwrap().is_empty());
    assert_eq!(c.check(a.key, a.epoch), Err(OwnershipError::Expired));
    c.check(b.key, b.epoch).unwrap();
    clock.set(100);
    assert_eq!(c.watchdog().unwrap(), vec![b.key]);
}

#[test]
fn timely_renewal_preserves_epoch_and_extends_deadline() {
    let clock = FakeClock::default();
    let mut c = StandaloneCoordinator::new(clock.clone());
    let lease = c.acquire(key(1, 0), TTL).unwrap();
    clock.set(5);
    let renewed = c.renew(lease.key, lease.epoch, TTL).unwrap();
    assert_eq!(renewed.epoch, lease.epoch);
    assert_eq!(renewed.expires_at, Duration::from_secs(15));
    assert_eq!(
        c.renew(lease.key, lease.epoch, Duration::from_secs(1))
            .unwrap(),
        renewed
    );
    clock.set(10);
    c.check(lease.key, lease.epoch).unwrap();
    clock.set(15);
    assert_eq!(
        c.renew(lease.key, lease.epoch, TTL),
        Err(OwnershipError::Expired)
    );
}

#[test]
fn clock_regression_permanently_fails_closed_for_all_keys() {
    let clock = FakeClock::default();
    clock.set(5);
    let mut c = StandaloneCoordinator::new(clock.clone());
    let a = c.acquire(key(1, 0), TTL).unwrap();
    let b = c.acquire(key(2, 0), TTL).unwrap();
    clock.set(4);
    assert_eq!(c.watchdog(), Err(OwnershipError::ClockRegressed));
    clock.set(6);
    for lease in [a, b] {
        assert_eq!(
            c.check(lease.key, lease.epoch),
            Err(OwnershipError::ClockRegressed)
        );
        assert_eq!(
            c.renew(lease.key, lease.epoch, TTL),
            Err(OwnershipError::ClockRegressed)
        );
        assert_eq!(
            c.release(lease.key, lease.epoch),
            Err(OwnershipError::ClockRegressed)
        );
        assert_eq!(
            c.acquire(lease.key, TTL),
            Err(OwnershipError::ClockRegressed)
        );
    }
}

#[test]
fn invalid_ttl_and_overflow_do_not_replace_authority() {
    let clock = FakeClock::default();
    clock.set(1);
    let mut c = StandaloneCoordinator::new(clock);
    let a = c.acquire(key(1, 0), TTL).unwrap();
    assert_eq!(
        c.acquire(a.key, Duration::ZERO),
        Err(OwnershipError::InvalidDuration)
    );
    assert_eq!(
        c.renew(a.key, a.epoch, Duration::ZERO),
        Err(OwnershipError::InvalidDuration)
    );
    assert_eq!(
        c.acquire(a.key, Duration::MAX),
        Err(OwnershipError::TimeOverflow)
    );
    assert_eq!(
        c.renew(a.key, a.epoch, Duration::MAX),
        Err(OwnershipError::TimeOverflow)
    );
    c.check(a.key, a.epoch).unwrap();
    assert_eq!(c.acquire(a.key, TTL).unwrap().epoch, OwnershipEpoch(2));
}

#[test]
fn quorum_startup_refuses_without_backend() {
    assert!(matches!(
        QuorumCoordinator::new(),
        Err(OwnershipError::QuorumUnavailable)
    ));
}
