use src::{protocol::BotId, rest_budget::*};
use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

const A: RouteKey = RouteKey(1);
const B: RouteKey = RouteKey(2);
const UNKNOWN: RouteKey = RouteKey(999);

#[derive(Clone, Default)]
struct FakeClock(Arc<Mutex<Duration>>);
impl FakeClock {
    fn set(&self, now: Duration) {
        *self.0.lock().unwrap() = now;
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
fn limit(requests: u32, window_ms: u64) -> RateLimit {
    RateLimit {
        requests,
        window: ms(window_ms),
    }
}
fn config(requests: u32, window_ms: u64) -> RestBudgetConfig {
    RestBudgetConfig {
        global: limit(requests, window_ms),
        max_jitter: Duration::ZERO,
        ..RestBudgetConfig::default()
    }
}
fn budget(config: RestBudgetConfig) -> (RestBudget<FakeClock>, FakeClock) {
    let clock = FakeClock::default();
    (
        RestBudget::new(BotId(1), config, clock.clone(), 42).unwrap(),
        clock,
    )
}
fn wait(b: &RestBudget<FakeClock>, key: RouteKey, at: u64, reason: WaitReason) {
    assert_eq!(
        b.try_acquire(key),
        Ok(Admission::Wait {
            retry_at: ms(at),
            reason,
        })
    );
}
fn report(b: &RestBudget<FakeClock>, key: RouteKey, delay_ms: u64, global: bool) -> Duration {
    b.rate_limited(
        key,
        RateLimited {
            retry_after: ms(delay_ms),
            global,
        },
    )
    .unwrap()
}

#[test]
fn default_policy_admits_exactly_fifty_in_one_rolling_second() {
    let (b, clock) = budget(RestBudgetConfig {
        max_jitter: Duration::ZERO,
        ..RestBudgetConfig::default()
    });
    assert_eq!(b.bot_id(), BotId(1));
    b.register_route(A, limit(100, 1000)).unwrap();
    for _ in 0..50 {
        assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    }
    wait(&b, A, 1000, WaitReason::GlobalLimit);
    clock.set(ms(1000) - Duration::from_nanos(1));
    wait(&b, A, 1000, WaitReason::GlobalLimit);
    clock.set(ms(1000));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn rolling_window_does_not_allow_a_fixed_window_boundary_burst() {
    let (b, clock) = budget(config(2, 1000));
    b.register_route(A, limit(100, 1000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    clock.set(ms(500));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    clock.set(ms(999));
    wait(&b, A, 1000, WaitReason::GlobalLimit);
    clock.set(ms(1000));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    wait(&b, A, 1500, WaitReason::GlobalLimit);
    clock.set(ms(1500));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    wait(&b, A, 2000, WaitReason::GlobalLimit);
}

#[test]
fn route_buckets_have_independent_capacity_and_share_global_capacity() {
    let (b, clock) = budget(config(3, 1000));
    b.register_route(A, limit(1, 2000)).unwrap();
    b.register_route(B, limit(2, 2000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    wait(&b, A, 2000, WaitReason::RouteLimit);
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
    wait(&b, B, 2000, WaitReason::RouteLimit);
    clock.set(ms(1000));
    wait(&b, A, 2000, WaitReason::RouteLimit);
    clock.set(ms(2000));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
}

#[test]
fn waiting_on_a_route_does_not_spend_global_budget() {
    let (b, _) = budget(config(2, 1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    b.register_route(B, limit(1, 1000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    for _ in 0..100 {
        wait(&b, A, 1000, WaitReason::RouteLimit);
    }
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
}

#[test]
fn waiting_on_global_budget_does_not_spend_route_budget() {
    let (b, clock) = budget(config(1, 1000));
    b.register_route(A, limit(1, 10_000)).unwrap();
    b.register_route(B, limit(1, 10_000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    for _ in 0..100 {
        wait(&b, B, 1000, WaitReason::GlobalLimit);
    }
    clock.set(ms(1000));
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
}

#[test]
fn aliases_using_the_same_canonical_key_cannot_bypass_route_capacity() {
    let (b, _) = budget(config(100, 1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    let shard_one = b.clone();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    wait(&shard_one, RouteKey(A.0), 1000, WaitReason::RouteLimit);
    assert_eq!(
        shard_one.register_route(A, limit(1000, 1)),
        Err(BudgetError::DuplicateRoute)
    );
    wait(&b, A, 1000, WaitReason::RouteLimit);
}

#[test]
fn cloned_clients_cannot_multiply_global_quota_under_concurrent_admission() {
    let (b, _) = budget(config(50, 1000));
    b.register_route(A, limit(1000, 1000)).unwrap();
    b.register_route(B, limit(1000, 1000)).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let client = b.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..100)
                    .filter(|_| {
                        client.try_acquire(if i % 2 == 0 { A } else { B }).unwrap()
                            == Admission::Granted
                    })
                    .count()
            })
        })
        .collect();
    let granted: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(granted, 50);
    wait(&b, A, 1000, WaitReason::GlobalLimit);
}

#[test]
fn bots_have_independent_quotas_and_429_restrictions() {
    let clock = FakeClock::default();
    let a = RestBudget::new(BotId(1), config(1, 1000), clock.clone(), 7).unwrap();
    let b = RestBudget::new(BotId(2), config(1, 1000), clock, 7).unwrap();
    a.register_route(A, limit(1, 1000)).unwrap();
    b.register_route(A, limit(1, 1000)).unwrap();
    assert_eq!(a.try_acquire(A), Ok(Admission::Granted));
    report(&a, A, 10_000, true);
    assert_eq!(b.bot_id(), BotId(2));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    wait(&a, A, 10_000, WaitReason::Global429);
}

#[test]
fn route_429_waits_for_retry_after_with_nanosecond_boundary() {
    let (b, clock) = budget(config(100, 1000));
    b.register_route(A, limit(100, 1000)).unwrap();
    b.register_route(B, limit(100, 1000)).unwrap();
    clock.set(ms(123));
    assert_eq!(report(&b, A, 1500, false), ms(1623));
    wait(&b, A, 1623, WaitReason::Route429);
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
    clock.set(ms(1623) - Duration::from_nanos(1));
    wait(&b, A, 1623, WaitReason::Route429);
    clock.set(ms(1623));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn global_429_propagates_to_clones_and_later_registered_routes() {
    let (b, clock) = budget(config(100, 1000));
    b.register_route(A, limit(100, 1000)).unwrap();
    let client = b.clone();
    assert_eq!(report(&client, A, 3210, true), ms(3210));
    b.register_route(B, limit(100, 1000)).unwrap();
    wait(&b, A, 3210, WaitReason::Global429);
    wait(&b, B, 3210, WaitReason::Global429);
    clock.set(ms(3210));
    assert_eq!(client.try_acquire(B), Ok(Admission::Granted));
}

#[test]
fn later_shorter_429_reports_never_shorten_either_restriction() {
    for global in [false, true] {
        let (b, clock) = budget(config(100, 1000));
        b.register_route(A, limit(100, 1000)).unwrap();
        assert_eq!(report(&b, A, 4000, global), ms(4000));
        clock.set(ms(1000));
        assert_eq!(report(&b, A, 100, global), ms(4000));
        assert_eq!(report(&b, A, 5000, global), ms(6000));
        wait(
            &b,
            A,
            6000,
            if global {
                WaitReason::Global429
            } else {
                WaitReason::Route429
            },
        );
    }
}

#[test]
fn concurrent_429_responses_preserve_the_longest_deadline() {
    let (b, _) = budget(config(100, 1000));
    b.register_route(A, limit(100, 1000)).unwrap();
    let threads: Vec<_> = (1..=16)
        .map(|delay| {
            let client = b.clone();
            std::thread::spawn(move || report(&client, A, delay * 100, true))
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    wait(&b, A, 1600, WaitReason::Global429);
}

#[test]
fn retry_deadline_is_the_maximum_of_all_restrictions() {
    let (b, clock) = budget(config(1, 2000));
    b.register_route(A, limit(1, 3000)).unwrap();
    b.register_route(B, limit(100, 1000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    report(&b, A, 1000, false);
    report(&b, A, 2500, true);
    wait(&b, A, 3000, WaitReason::RouteLimit);
    wait(&b, B, 2500, WaitReason::Global429);
    report(&b, A, 4000, false);
    wait(&b, A, 4000, WaitReason::Route429);
    clock.set(ms(2500));
    assert_eq!(b.try_acquire(B), Ok(Admission::Granted));
    // That admission extends the shared rolling-window restriction to 4500.
    wait(&b, A, 4500, WaitReason::GlobalLimit);
}

#[test]
fn expiry_of_a_429_does_not_reset_spent_route_or_global_quota() {
    for global in [false, true] {
        let (b, clock) = budget(config(1, 2000));
        b.register_route(A, limit(1, 3000)).unwrap();
        assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
        report(&b, A, 1000, global);
        clock.set(ms(1000));
        wait(&b, A, 3000, WaitReason::RouteLimit);
        clock.set(ms(3000));
        assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    }
}

#[test]
fn jitter_is_additive_bounded_seeded_and_sampled_once_per_response() {
    let cfg = RestBudgetConfig {
        max_jitter: ms(250),
        ..config(100, 1000)
    };
    let (a, clock_a) = budget(cfg);
    let (b, clock_b) = budget(cfg);
    a.register_route(A, limit(100, 1000)).unwrap();
    b.register_route(A, limit(100, 1000)).unwrap();
    let mut sampled = Vec::new();
    for tick in 0..20 {
        let now = ms(tick * 5000);
        clock_a.set(now);
        clock_b.set(now);
        let deadline = report(&a, A, 1000, false);
        assert_eq!(deadline, report(&b, A, 1000, false));
        assert!(deadline >= now + ms(1000));
        assert!(deadline <= now + ms(1250));
        sampled.push(deadline - now);
        for _ in 0..10 {
            assert_eq!(
                a.try_acquire(A),
                Ok(Admission::Wait {
                    retry_at: deadline,
                    reason: WaitReason::Route429
                })
            );
        }
        clock_a.set(deadline);
        assert_eq!(a.try_acquire(A), Ok(Admission::Granted));
    }
    assert!(sampled.windows(2).any(|pair| pair[0] != pair[1]));
}

#[test]
fn zero_jitter_and_zero_delay_have_exact_semantics() {
    let (b, clock) = budget(config(10, 1000));
    b.register_route(A, limit(10, 1000)).unwrap();
    clock.set(ms(123));
    assert_eq!(report(&b, A, 0, false), ms(123));
    assert_eq!(report(&b, A, 0, true), ms(123));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn submillisecond_retry_after_is_not_truncated() {
    let (b, clock) = budget(config(10, 1000));
    b.register_route(A, limit(10, 1000)).unwrap();
    assert_eq!(
        b.rate_limited(
            A,
            RateLimited {
                retry_after: Duration::from_nanos(7),
                global: true
            }
        ),
        Ok(Duration::from_nanos(7))
    );
    clock.set(Duration::from_nanos(6));
    assert_eq!(
        b.try_acquire(A),
        Ok(Admission::Wait {
            retry_at: Duration::from_nanos(7),
            reason: WaitReason::Global429
        })
    );
    clock.set(Duration::from_nanos(7));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn unknown_route_is_denied_without_consuming_quota() {
    let (b, _) = budget(config(1, 1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    assert_eq!(b.try_acquire(UNKNOWN), Err(BudgetError::UnknownRoute));
    assert_eq!(
        b.rate_limited(
            UNKNOWN,
            RateLimited {
                retry_after: ms(1000),
                global: false
            }
        ),
        Err(BudgetError::UnknownRoute)
    );
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn a_global_429_is_honored_even_when_reported_for_an_unknown_route() {
    let (b, _) = budget(config(1, 1000));
    assert_eq!(report(&b, UNKNOWN, 1000, true), ms(1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    wait(&b, A, 1000, WaitReason::Global429);
}

#[test]
fn registration_is_bounded_and_duplicates_preserve_live_429_restrictions() {
    let (b, _) = budget(RestBudgetConfig {
        max_routes: 1,
        ..config(1, 1000)
    });
    assert_eq!(
        b.register_route(A, limit(0, 1000)),
        Err(BudgetError::InvalidLimit)
    );
    assert_eq!(
        b.register_route(A, limit(1, 0)),
        Err(BudgetError::InvalidLimit)
    );
    b.register_route(A, limit(1, 1000)).unwrap();
    report(&b, A, 9999, false);
    assert_eq!(
        b.register_route(A, limit(100, 1)),
        Err(BudgetError::DuplicateRoute)
    );
    assert_eq!(
        b.register_route(B, limit(1, 1000)),
        Err(BudgetError::RouteCapacity)
    );
    wait(&b, A, 9999, WaitReason::Route429);
}

#[test]
fn invalid_constructor_parameters_are_rejected() {
    assert!(matches!(
        RestBudget::new(BotId(0), config(1, 1000), FakeClock::default(), 1),
        Err(BudgetError::InvalidBot)
    ));
    for cfg in [config(0, 1000), config(1, 0)] {
        assert!(matches!(
            RestBudget::new(BotId(1), cfg, FakeClock::default(), 1),
            Err(BudgetError::InvalidLimit)
        ));
    }
    assert!(matches!(
        RestBudget::new(
            BotId(1),
            RestBudgetConfig {
                max_routes: 0,
                ..config(1, 1000)
            },
            FakeClock::default(),
            1
        ),
        Err(BudgetError::InvalidCapacity)
    ));
    assert!(matches!(
        RestBudget::new(
            BotId(1),
            RestBudgetConfig {
                max_jitter: Duration::from_nanos(u64::MAX) + Duration::from_nanos(1),
                ..config(1, 1000)
            },
            FakeClock::default(),
            1
        ),
        Err(BudgetError::InvalidJitter)
    ));
}

#[test]
fn a_regressing_clock_permanently_denies_every_clone_even_after_recovery() {
    let (b, clock) = budget(config(1, 1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    let client = b.clone();
    clock.set(ms(10));
    assert_eq!(client.try_acquire(A), Ok(Admission::Granted));
    clock.set(ms(9));
    assert_eq!(b.try_acquire(A), Err(BudgetError::ClockRegressed));
    clock.set(ms(100_000));
    assert_eq!(client.try_acquire(A), Err(BudgetError::ClockRegressed));
    assert_eq!(
        b.register_route(B, limit(1, 1000)),
        Err(BudgetError::ClockRegressed)
    );
    assert_eq!(
        b.rate_limited(
            A,
            RateLimited {
                retry_after: ms(1),
                global: true
            }
        ),
        Err(BudgetError::ClockRegressed)
    );
}

#[test]
fn overflow_while_computing_retry_after_latches_fail_closed() {
    let (b, clock) = budget(config(1, 1000));
    b.register_route(A, limit(1, 1000)).unwrap();
    clock.set(ms(1));
    assert_eq!(
        b.rate_limited(
            A,
            RateLimited {
                retry_after: Duration::MAX,
                global: true
            }
        ),
        Err(BudgetError::TimeOverflow)
    );
    assert_eq!(b.try_acquire(A), Err(BudgetError::TimeOverflow));
    assert_eq!(
        b.clone().register_route(B, limit(1, 1000)),
        Err(BudgetError::TimeOverflow)
    );
}

#[test]
fn additive_jitter_overflow_cannot_wrap_a_429_into_permission() {
    let cfg = RestBudgetConfig {
        max_jitter: ms(1000),
        ..config(1, 1000)
    };
    let (probe, _) = budget(cfg);
    // The same injected seed produces the same first jitter in each budget.
    assert!(report(&probe, UNKNOWN, 0, true) > Duration::ZERO);
    let (b, _) = budget(cfg);
    b.register_route(A, limit(1, 1000)).unwrap();
    assert_eq!(
        b.rate_limited(
            A,
            RateLimited {
                retry_after: Duration::MAX,
                global: true,
            },
        ),
        Err(BudgetError::TimeOverflow)
    );
    assert_eq!(b.try_acquire(A), Err(BudgetError::TimeOverflow));
}

#[test]
fn maximum_jitter_and_zero_internal_seed_do_not_overflow_or_lock_rng_at_zero() {
    let cfg = RestBudgetConfig {
        max_jitter: Duration::from_nanos(u64::MAX),
        ..config(1, 1000)
    };
    // This cancels the Bot-derived seed; the implementation must handle zero.
    let b = RestBudget::new(BotId(1), cfg, FakeClock::default(), 1_u64.rotate_left(23)).unwrap();
    let deadline = b
        .rate_limited(
            UNKNOWN,
            RateLimited {
                retry_after: Duration::ZERO,
                global: true,
            },
        )
        .unwrap();
    assert!(deadline > Duration::ZERO);
    assert!(deadline <= cfg.max_jitter);
}

#[test]
fn overflow_in_either_window_expiry_denies_admission() {
    for (global, route) in [(limit(1, 1000), limit(1, 1)), (limit(1, 1), limit(1, 1000))] {
        let (b, clock) = budget(RestBudgetConfig {
            global,
            ..config(1, 1000)
        });
        b.register_route(A, route).unwrap();
        clock.set(Duration::MAX - ms(500));
        assert_eq!(b.try_acquire(A), Err(BudgetError::TimeOverflow));
        assert_eq!(b.try_acquire(A), Err(BudgetError::TimeOverflow));
    }
}

#[test]
fn very_long_idle_periods_expire_all_old_admissions_and_blocks() {
    let (b, clock) = budget(config(1, 1000));
    b.register_route(A, limit(1, 2000)).unwrap();
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
    report(&b, A, 100_000, true);
    clock.set(Duration::from_secs(1_000_000));
    assert_eq!(b.try_acquire(A), Ok(Admission::Granted));
}

#[test]
fn poisoned_accounting_lock_is_an_error_instead_of_unchecked_permission() {
    #[derive(Clone)]
    struct PanicClock(Arc<AtomicBool>);
    impl Clock for PanicClock {
        fn now(&self) -> Duration {
            assert!(!self.0.load(Ordering::SeqCst), "injected clock failure");
            Duration::ZERO
        }
    }
    let panic = Arc::new(AtomicBool::new(false));
    let b = RestBudget::new(BotId(1), config(1, 1000), PanicClock(panic.clone()), 1).unwrap();
    b.register_route(A, limit(1, 1000)).unwrap();
    panic.store(true, Ordering::SeqCst);
    assert!(std::panic::catch_unwind(|| b.try_acquire(A)).is_err());
    panic.store(false, Ordering::SeqCst);
    assert_eq!(b.try_acquire(A), Err(BudgetError::Poisoned));
    assert_eq!(
        b.clone().register_route(B, limit(1, 1000)),
        Err(BudgetError::Poisoned)
    );
    assert_eq!(
        b.rate_limited(
            A,
            RateLimited {
                retry_after: ms(1),
                global: true
            }
        ),
        Err(BudgetError::Poisoned)
    );
}
