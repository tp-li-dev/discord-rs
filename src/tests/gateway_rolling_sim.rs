//! In-memory composition only: no real transport, processes, or Discord.
use src::{
    gateway_switch::*,
    metrics::*,
    ownership::{Clock, OwnershipCoordinator, OwnershipError, OwnershipKey, StandaloneCoordinator},
    protocol::{BotId, GenerationId, OperationId, OwnershipEpoch, RequestId},
    rest_budget::{
        Admission, RateLimit, RateLimited, RestBudget, RestBudgetConfig, RouteKey, WaitReason,
    },
    update_gate::{DeferralReason, UpdateDecision, UpdatePlan, evaluate_update},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use twilight_model::gateway::SessionStartLimit;
#[derive(Clone, Default)]
struct FakeClock(Arc<Mutex<Duration>>);
impl Clock for FakeClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}
fn key() -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(7),
        shard_id: 2,
    }
}
fn auth(g: u64, e: OwnershipEpoch) -> Authority {
    Authority {
        generation: GenerationId(g),
        epoch: e,
    }
}
fn origin(n: u8) -> RestOrigin {
    RestOrigin {
        core_generation: GenerationId(10 + u64::from(n)),
        request_id: RequestId([n; 16]),
    }
}
fn op(n: u8) -> OperationId {
    OperationId([n; 16])
}
fn send(m: &mut GatewaySwitch, e: Event) -> Vec<Effect> {
    m.handle(key(), e).unwrap()
}
fn setup() -> (StandaloneCoordinator<FakeClock>, GatewaySwitch, Authority) {
    let mut c = StandaloneCoordinator::new(FakeClock::default());
    let a = auth(1, c.acquire(key(), Duration::from_secs(60)).unwrap().epoch);
    (c, GatewaySwitch::new(key(), a), a)
}
fn begin(m: &mut GatewaySwitch) {
    assert_eq!(
        send(
            m,
            Event::Begin {
                candidate: GenerationId(2),
                identify: IdentifyDecision::Reserved
            }
        ),
        vec![Effect::SpawnStandby(GenerationId(2))]
    );
    send(m, Event::Spawned(GenerationId(2)));
}
fn transfer(
    c: &mut StandaloneCoordinator<FakeClock>,
    m: &mut GatewaySwitch,
    old: Authority,
) -> Authority {
    assert_eq!(
        send(m, Event::WarmReady(GenerationId(2))),
        vec![Effect::PauseOld(old)]
    );
    assert_eq!(
        send(m, Event::OldPaused(old)),
        vec![Effect::RequestAuthority {
            generation: GenerationId(2),
            after_epoch: old.epoch
        }]
    );
    assert_eq!(m.active(), None);
    let new = auth(2, c.acquire(key(), Duration::from_secs(60)).unwrap().epoch);
    assert!(new.epoch.0 > old.epoch.0);
    assert_eq!(c.check(key(), old.epoch), Err(OwnershipError::Fenced));
    assert_eq!(
        send(m, Event::AuthorityGranted(new)),
        vec![Effect::Activate(new)]
    );
    new
}
fn metrics() -> Arc<ShardMetrics> {
    MetricsRegistry::default()
        .register(
            MetricLabels::new("bot", 2, 1).unwrap(),
            RuntimeStatus {
                gateway: GenerationStatus {
                    generation_id: 1,
                    role: RuntimeRole::Active,
                },
                core: GenerationStatus {
                    generation_id: 11,
                    role: RuntimeRole::Active,
                },
                ws_connected: true,
            },
        )
        .unwrap()
}

#[test]
fn rolling_schedules_keep_one_active_and_route_old_result() {
    // Six interleavings of work-finished < drain-ack and result < result-ack.
    for schedule in [
        [0, 1, 2, 3],
        [0, 2, 1, 3],
        [0, 2, 3, 1],
        [2, 0, 1, 3],
        [2, 0, 3, 1],
        [2, 3, 0, 1],
    ] {
        let (mut c, mut m, old) = setup();
        let met = metrics();
        m.admit_work(key(), old, origin(1)).unwrap();
        let route = m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
        begin(&mut m);
        assert_eq!(
            m.admit_work(key(), auth(2, old.epoch), origin(2)),
            Err(Error::AdmissionClosed)
        );
        let new = transfer(&mut c, &mut m, old);
        assert_eq!(
            m.admit_work(key(), old, origin(3)),
            Err(Error::AdmissionClosed)
        );
        assert_eq!(
            send(&mut m, Event::ActiveAck(new)),
            vec![Effect::Drain(auth(1, new.epoch))]
        );
        met.active_changes.inc();
        c.check(key(), new.epoch).unwrap();
        m.admit_work(key(), new, origin(2)).unwrap();
        let mut stopped = false;
        for step in schedule {
            let effects = match step {
                0 => send(
                    &mut m,
                    Event::WorkFinished {
                        generation: GenerationId(1),
                        request_id: origin(1).request_id,
                    },
                ),
                1 => send(&mut m, Event::DrainAck(auth(1, new.epoch))),
                2 => {
                    let effects = send(
                        &mut m,
                        Event::RestResult {
                            generation: GenerationId(1),
                            operation_id: op(1),
                        },
                    );
                    assert_eq!(
                        effects,
                        vec![Effect::RouteRestResult {
                            operation_id: op(1),
                            route
                        }]
                    );
                    assert_eq!(route.gateway_generation, GenerationId(1));
                    assert_eq!(route.origin, origin(1));
                    effects
                }
                3 => send(
                    &mut m,
                    Event::RestResultAck {
                        generation: GenerationId(1),
                        operation_id: op(1),
                    },
                ),
                _ => unreachable!(),
            };
            if effects.contains(&Effect::Stop(GenerationId(1))) {
                assert!(!stopped);
                stopped = true;
            }
            if !stopped {
                assert_eq!(m.phase(), Phase::DrainingOld);
            }
            assert_eq!(m.active(), Some(new));
        }
        assert!(stopped);
        assert_eq!(m.phase(), Phase::Stable);
        assert_eq!(met.snapshot().active_changes_total, 1);
    }
}
#[test]
fn ready_timeout_and_missing_active_ack() {
    let (mut c, mut m, old) = setup();
    begin(&mut m);
    let timer = m.wait().unwrap();
    let candidate = auth(2, old.epoch);
    assert!(send(&mut m, Event::Timeout(timer)).contains(&Effect::Quiesce(candidate)));
    assert_eq!(m.active(), Some(old));
    c.check(key(), old.epoch).unwrap();
    m.admit_work(key(), old, origin(1)).unwrap();
    send(&mut m, Event::CandidateQuiesced(candidate));
    send(&mut m, Event::DrainAck(candidate));
    let (mut c, mut m, old) = setup();
    begin(&mut m);
    let candidate = transfer(&mut c, &mut m, old);
    let timer = m.wait().unwrap();
    assert!(send(&mut m, Event::Timeout(timer)).contains(&Effect::Quiesce(candidate)));
    assert_eq!(
        m.admit_work(key(), candidate, origin(2)),
        Err(Error::AdmissionClosed)
    );
    assert_eq!(
        m.handle(
            key(),
            Event::AuthorityGranted(auth(1, OwnershipEpoch(candidate.epoch.0 + 1)))
        ),
        Err(Error::InvalidPhase)
    );
    assert!(matches!(
        send(&mut m, Event::CandidateQuiesced(candidate)).as_slice(),
        [Effect::RequestAuthority {
            generation: GenerationId(1),
            ..
        }]
    ));
    let restored = auth(1, c.acquire(key(), Duration::from_secs(60)).unwrap().epoch);
    assert!(restored.epoch.0 > candidate.epoch.0);
    assert_eq!(c.check(key(), candidate.epoch), Err(OwnershipError::Fenced));
    send(&mut m, Event::AuthorityGranted(restored));
    send(&mut m, Event::ActiveAck(restored));
    assert_eq!(m.active(), Some(restored));
    assert_eq!(
        m.admit_work(key(), candidate, origin(3)),
        Err(Error::AdmissionClosed)
    );
}
#[test]
fn identify_deferral_reset_and_shared_rest_429() {
    let (_, mut m, old) = setup();
    let mut limits = SessionStartLimit {
        total: 10,
        remaining: 0,
        reset_after: 5000,
        max_concurrency: 1,
    };
    let plan = UpdatePlan {
        additional_identifies: 1,
    };
    let decision = evaluate_update(&limits, Duration::ZERO, Duration::from_secs(1), plan);
    assert!(matches!(
        decision,
        UpdateDecision::Defer {
            reason: DeferralReason::InsufficientBudget {
                required: 1,
                remaining: 0
            },
            ..
        }
    ));
    assert!(
        decision
            .diagnostic()
            .contains("keep the old Gateway active")
    );
    let defer = IdentifyDeferral {
        reason: IdentifyBlock::BudgetExhausted,
        retry_after: Duration::from_secs(4),
    };
    assert_eq!(
        send(
            &mut m,
            Event::Begin {
                candidate: GenerationId(2),
                identify: IdentifyDecision::Defer(defer)
            }
        ),
        vec![Effect::Diagnostic(Diagnostic::IdentifyDeferred(defer))]
    );
    assert_eq!(m.active(), Some(old));
    let expired = evaluate_update(&limits, Duration::ZERO, Duration::from_secs(5), plan);
    assert!(matches!(
        expired,
        UpdateDecision::Defer {
            reason: DeferralReason::MetadataExpired,
            ..
        }
    ));
    assert!(expired.diagnostic().contains("fresh metadata"));
    limits.remaining = 10;
    assert!(matches!(
        evaluate_update(
            &limits,
            Duration::from_secs(5),
            Duration::from_secs(5),
            plan
        ),
        UpdateDecision::Execute { .. }
    ));
    // Preflight is not a reservation; simulate the separate queue reservation.
    assert_eq!(
        send(&mut m, Event::RetryIdentify(IdentifyDecision::Reserved)),
        vec![Effect::SpawnStandby(GenerationId(2))]
    );
    let clock = FakeClock::default();
    let limit = RateLimit {
        requests: 20,
        window: Duration::from_secs(1),
    };
    let budget = RestBudget::new(
        BotId(7),
        RestBudgetConfig {
            global: limit,
            max_routes: 2,
            max_jitter: Duration::ZERO,
        },
        clock.clone(),
        1,
    )
    .unwrap();
    let old_client = budget.clone();
    let new_client = budget.clone();
    let route = RouteKey(1);
    budget.register_route(route, limit).unwrap();
    assert_eq!(old_client.try_acquire(route).unwrap(), Admission::Granted);
    let met = metrics();
    met.http_429.inc();
    assert_eq!(
        old_client
            .rate_limited(
                route,
                RateLimited {
                    retry_after: Duration::from_secs(3),
                    global: false
                }
            )
            .unwrap(),
        Duration::from_secs(3)
    );
    assert_eq!(
        new_client.try_acquire(route).unwrap(),
        Admission::Wait {
            retry_at: Duration::from_secs(3),
            reason: WaitReason::Route429
        }
    );
    *clock.0.lock().unwrap() = Duration::from_secs(3);
    assert_eq!(new_client.try_acquire(route).unwrap(), Admission::Granted);
    assert_eq!(met.snapshot().http_429_total, 1);
}
