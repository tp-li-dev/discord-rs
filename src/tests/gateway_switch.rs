use src::{
    gateway_switch::*,
    ownership::{Clock, OwnershipCoordinator, OwnershipError, OwnershipKey, StandaloneCoordinator},
    protocol::{BotId, GenerationId, OperationId, OwnershipEpoch, RequestId},
};
use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

fn key() -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(7),
        shard_id: 2,
    }
}
fn auth(generation: u64, epoch: u64) -> Authority {
    Authority {
        generation: GenerationId(generation),
        epoch: OwnershipEpoch(epoch),
    }
}
fn machine() -> GatewaySwitch {
    GatewaySwitch::new(key(), auth(1, 1))
}
fn origin(id: u8) -> RestOrigin {
    RestOrigin {
        core_generation: GenerationId(100 + id as u64),
        request_id: RequestId([id; 16]),
    }
}
fn op(id: u8) -> OperationId {
    OperationId([id; 16])
}
fn send(m: &mut GatewaySwitch, event: Event) -> Vec<Effect> {
    m.handle(key(), event).unwrap()
}
fn drain_ack(m: &mut GatewaySwitch, generation: u64) -> Vec<Effect> {
    let epoch = m.epoch().0;
    send(m, Event::DrainAck(auth(generation, epoch)))
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
}
fn warm(m: &mut GatewaySwitch) {
    begin(m);
    assert_eq!(
        send(m, Event::Spawned(GenerationId(2))),
        vec![Effect::IdentifyStandby(GenerationId(2))]
    );
    assert_eq!(
        send(m, Event::WarmReady(GenerationId(2))),
        vec![Effect::PauseOld(auth(1, 1))]
    );
}
fn pause(m: &mut GatewaySwitch) {
    warm(m);
    assert_eq!(
        send(m, Event::OldPaused(auth(1, 1))),
        vec![Effect::RequestAuthority {
            generation: GenerationId(2),
            after_epoch: OwnershipEpoch(1)
        }]
    );
    assert_eq!(m.active(), None);
}
fn activate(m: &mut GatewaySwitch) {
    pause(m);
    assert_eq!(
        send(m, Event::AuthorityGranted(auth(2, 2))),
        vec![Effect::Activate(auth(2, 2))]
    );
}
fn drain(m: &mut GatewaySwitch) {
    activate(m);
    assert_eq!(
        send(m, Event::ActiveAck(auth(2, 2))),
        vec![Effect::Drain(auth(1, 2))]
    );
}
fn reject(m: &mut GatewaySwitch, event: Event, expected: Error) {
    let before = m.clone();
    assert_eq!(m.handle(key(), event), Err(expected));
    assert_eq!(*m, before, "rejected event mutated state: {event:?}");
}
fn fail_candidate(m: &mut GatewaySwitch) -> Vec<Effect> {
    send(
        m,
        Event::Failed {
            generation: GenerationId(2),
            reason: Failure::GatewayFailed,
        },
    )
}
fn restore(m: &mut GatewaySwitch, quiesce_epoch: u64, restore_epoch: u64) {
    assert_eq!(
        send(m, Event::CandidateQuiesced(auth(2, quiesce_epoch))),
        vec![Effect::RequestAuthority {
            generation: GenerationId(1),
            after_epoch: OwnershipEpoch(quiesce_epoch)
        }]
    );
    assert_eq!(m.active(), None);
    assert_eq!(
        send(m, Event::AuthorityGranted(auth(1, restore_epoch))),
        vec![Effect::Activate(auth(1, restore_epoch))]
    );
    assert_eq!(
        send(m, Event::ActiveAck(auth(1, restore_epoch))),
        vec![Effect::Drain(auth(2, m.epoch().0))]
    );
}

#[test]
fn t07_standby_never_admits_commands_and_order_is_strict() {
    let mut m = machine();
    begin(&mut m);
    for event in [None, Some(Event::Spawned(GenerationId(2)))] {
        if let Some(event) = event {
            send(&mut m, event);
        }
        assert_eq!(
            m.admit_work(key(), auth(2, 1), origin(1)),
            Err(Error::AdmissionClosed)
        );
        reject(
            &mut m,
            Event::AuthorityGranted(auth(2, 2)),
            Error::InvalidPhase,
        );
        reject(&mut m, Event::ActiveAck(auth(2, 2)), Error::InvalidPhase);
    }
    send(&mut m, Event::WarmReady(GenerationId(2)));
    assert_eq!(m.active(), Some(auth(1, 1)));
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    reject(
        &mut m,
        Event::AuthorityGranted(auth(2, 2)),
        Error::InvalidPhase,
    );
    send(&mut m, Event::OldPaused(auth(1, 1)));
    assert_eq!(
        m.admit_work(key(), auth(1, 1), origin(2)),
        Err(Error::AdmissionClosed)
    );
    send(&mut m, Event::AuthorityGranted(auth(2, 9)));
    assert_eq!(m.active(), Some(auth(2, 9)));
    m.admit_work(key(), auth(2, 9), origin(2)).unwrap();
    assert_eq!(
        m.admit_work(key(), auth(2, 2), origin(3)),
        Err(Error::AdmissionClosed)
    );
    reject(&mut m, Event::DrainAck(auth(1, 2)), Error::InvalidPhase);
}

#[test]
fn t08_old_work_can_issue_rest_after_pause_and_routes_survive_switch() {
    let mut m = machine();
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    drain(&mut m);
    let route = m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
    assert_eq!(
        route,
        RestRoute {
            gateway_generation: GenerationId(1),
            origin: origin(1)
        }
    );
    reject(&mut m, Event::DrainAck(auth(1, 2)), Error::UnfinishedWork);
    send(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(1).request_id,
        },
    );
    assert_eq!(
        m.register_rest(key(), origin(1).request_id, op(2)),
        Err(Error::WorkFinished)
    );
    assert!(send(&mut m, Event::DrainAck(auth(1, 2))).is_empty());
    assert_eq!(
        send(
            &mut m,
            Event::RestResult {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        ),
        vec![Effect::RouteRestResult {
            operation_id: op(1),
            route
        }]
    );
    assert_eq!(m.phase(), Phase::DrainingOld);
    assert_eq!(m.pending_rest(GenerationId(1)), 1);
    assert_eq!(
        send(
            &mut m,
            Event::RestResultAck {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        ),
        vec![Effect::Stop(GenerationId(1))]
    );
    assert_eq!(m.phase(), Phase::Stable);
    assert_eq!(m.active(), Some(auth(2, 2)));
    assert_eq!(m.rest_route(op(1)), Some(route));
    // Delivery duplicates after retirement are harmless and never rerun REST.
    assert!(
        send(
            &mut m,
            Event::RestResult {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        )
        .is_empty()
    );
    assert!(
        send(
            &mut m,
            Event::RestResultAck {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        )
        .is_empty()
    );
}

#[test]
fn t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old() {
    for reason in [IdentifyBlock::BudgetExhausted, IdentifyBlock::RateLimited] {
        let mut m = machine();
        let deferral = IdentifyDeferral {
            reason,
            retry_after: Duration::from_secs(60),
        };
        let diagnostic = vec![Effect::Diagnostic(Diagnostic::IdentifyDeferred(deferral))];
        assert_eq!(
            send(
                &mut m,
                Event::Begin {
                    candidate: GenerationId(2),
                    identify: IdentifyDecision::Defer(deferral)
                }
            ),
            diagnostic
        );
        assert_eq!(m.phase(), Phase::Deferred);
        assert_eq!(m.active(), Some(auth(1, 1)));
        m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
        reject(&mut m, Event::Spawned(GenerationId(2)), Error::InvalidPhase);
        assert_eq!(
            send(
                &mut m,
                Event::RetryIdentify(IdentifyDecision::Defer(deferral))
            ),
            diagnostic
        );
        assert_eq!(
            send(&mut m, Event::RetryIdentify(IdentifyDecision::Reserved)),
            vec![Effect::SpawnStandby(GenerationId(2))]
        );
        reject(
            &mut m,
            Event::RetryIdentify(IdentifyDecision::Reserved),
            Error::InvalidPhase,
        );
    }
}

#[test]
fn identify_rate_limit_after_spawn_preserves_old_and_cleans_up_candidate() {
    let mut m = machine();
    begin(&mut m);
    send(&mut m, Event::Spawned(GenerationId(2)));
    let deferral = IdentifyDeferral {
        reason: IdentifyBlock::RateLimited,
        retry_after: Duration::from_secs(5),
    };
    assert_eq!(
        send(
            &mut m,
            Event::Failed {
                generation: GenerationId(2),
                reason: Failure::IdentifyLimited(deferral)
            }
        ),
        vec![
            Effect::Diagnostic(Diagnostic::IdentifyDeferred(deferral)),
            Effect::Quiesce(auth(2, 1))
        ]
    );
    assert_eq!(m.active(), Some(auth(1, 1)));
    assert_eq!(
        send(&mut m, Event::CandidateQuiesced(auth(2, 1))),
        vec![Effect::Drain(auth(2, m.epoch().0))]
    );
    assert_eq!(drain_ack(&mut m, 2), vec![Effect::Stop(GenerationId(2))]);
    assert_eq!(m.active(), Some(auth(1, 1)));
}

#[test]
fn ready_not_reached_and_spawn_failure_keep_old_running() {
    for warming in [false, true] {
        let mut m = machine();
        begin(&mut m);
        if warming {
            send(&mut m, Event::Spawned(GenerationId(2)));
        }
        let timeout = m.wait().unwrap();
        let effects = send(&mut m, Event::Timeout(timeout));
        assert!(effects.contains(&Effect::Quiesce(auth(2, 1))));
        assert_eq!(m.active(), Some(auth(1, 1)));
        m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
        send(&mut m, Event::CandidateQuiesced(auth(2, 1)));
        drain_ack(&mut m, 2);
        reject(
            &mut m,
            Event::Begin {
                candidate: GenerationId(2),
                identify: IdentifyDecision::Reserved,
            },
            Error::GenerationReused,
        );
    }
}

#[test]
fn lost_pause_ack_restores_old_at_fresh_epoch_and_rejects_late_pause() {
    let mut m = machine();
    warm(&mut m);
    let timer = m.wait().unwrap();
    send(&mut m, Event::Timeout(timer));
    reject(&mut m, Event::OldPaused(auth(1, 1)), Error::InvalidPhase);
    restore(&mut m, 1, 3);
    reject(&mut m, Event::OldPaused(auth(1, 1)), Error::StaleEpoch);
    assert_eq!(drain_ack(&mut m, 2), vec![Effect::Stop(GenerationId(2))]);
    assert_eq!(m.active(), Some(auth(1, 3)));
}

#[test]
fn lost_active_ack_waits_for_stop_barrier_before_restoring_old() {
    let mut m = machine();
    activate(&mut m);
    let active_timer = m.wait().unwrap();
    assert_eq!(
        send(&mut m, Event::Timeout(active_timer)),
        vec![
            Effect::Diagnostic(Diagnostic::TimedOut(Phase::ActivatingCandidate)),
            Effect::Quiesce(auth(2, 2))
        ]
    );
    assert_eq!(m.active(), Some(auth(2, 2))); // Candidate may be executing.
    assert_eq!(
        m.admit_work(key(), auth(2, 2), origin(1)),
        Err(Error::AdmissionClosed)
    );
    reject(
        &mut m,
        Event::AuthorityGranted(auth(1, 3)),
        Error::InvalidPhase,
    );
    reject(&mut m, Event::ActiveAck(auth(2, 2)), Error::InvalidPhase);
    reject(
        &mut m,
        Event::CandidateQuiesced(auth(2, 1)),
        Error::StaleEpoch,
    );
    let barrier_timer = m.wait().unwrap();
    assert_eq!(
        send(&mut m, Event::Timeout(barrier_timer)),
        vec![Effect::Diagnostic(Diagnostic::TimedOut(
            Phase::QuiescingCandidate
        ))]
    );
    assert_eq!(m.phase(), Phase::QuiescingCandidate);
    restore(&mut m, 2, 3);
    assert_eq!(m.active(), Some(auth(1, 3)));
    reject(&mut m, Event::Timeout(active_timer), Error::StaleTimeout);
    reject(&mut m, Event::ActiveAck(auth(2, 2)), Error::InvalidPhase);
}

#[test]
fn candidate_failure_during_old_drain_rolls_back_and_preserves_both_rest_routes() {
    let mut m = machine();
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
    drain(&mut m);
    m.admit_work(key(), auth(2, 2), origin(2)).unwrap();
    fail_candidate(&mut m);
    restore(&mut m, 2, 5);
    // Already admitted candidate work still owns its REST after rollback.
    let route = m.register_rest(key(), origin(2).request_id, op(2)).unwrap();
    assert_eq!(route.gateway_generation, GenerationId(2));
    m.register_rest(key(), origin(1).request_id, op(3)).unwrap();
    assert_eq!(
        m.rest_route(op(1)).unwrap().gateway_generation,
        GenerationId(1)
    );
    assert_eq!(
        m.admit_work(key(), auth(2, 2), origin(3)),
        Err(Error::AdmissionClosed)
    );
    reject(&mut m, Event::DrainAck(auth(2, 5)), Error::UnfinishedWork);
    send(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(2),
            request_id: origin(2).request_id,
        },
    );
    send(
        &mut m,
        Event::RestResult {
            generation: GenerationId(2),
            operation_id: op(2),
        },
    );
    send(
        &mut m,
        Event::RestResultAck {
            generation: GenerationId(2),
            operation_id: op(2),
        },
    );
    assert_eq!(drain_ack(&mut m, 2), vec![Effect::Stop(GenerationId(2))]);
    assert_eq!(m.pending_rest(GenerationId(1)), 2);
}

#[test]
fn authority_denial_aborts_candidate_but_old_restore_can_wait_for_quorum() {
    let mut m = machine();
    pause(&mut m);
    assert!(
        send(&mut m, Event::AuthorityDenied(GenerationId(2)))
            .contains(&Effect::Quiesce(auth(2, 1)))
    );
    send(&mut m, Event::CandidateQuiesced(auth(2, 1)));
    assert_eq!(
        send(&mut m, Event::AuthorityDenied(GenerationId(1))),
        vec![Effect::Diagnostic(Diagnostic::AuthorityUnavailable)]
    );
    assert_eq!(m.active(), None);
    assert_eq!(
        send(&mut m, Event::RetryAuthority),
        vec![Effect::RequestAuthority {
            generation: GenerationId(1),
            after_epoch: OwnershipEpoch(1)
        }]
    );
    send(&mut m, Event::AuthorityGranted(auth(1, 10)));
    send(&mut m, Event::ActiveAck(auth(1, 10)));
    assert_eq!(m.active(), Some(auth(1, 10)));
}

#[test]
fn old_restore_ack_timeout_requires_recovery_instead_of_claiming_success() {
    let mut m = machine();
    activate(&mut m);
    fail_candidate(&mut m);
    send(&mut m, Event::CandidateQuiesced(auth(2, 2)));
    send(&mut m, Event::AuthorityGranted(auth(1, 3)));
    let wait = m.wait().unwrap();
    assert!(send(&mut m, Event::Timeout(wait)).contains(&Effect::RecoverShard));
    assert_eq!(m.phase(), Phase::RecoveryRequired);
    assert_eq!(m.active(), None);
    reject(&mut m, Event::ActiveAck(auth(1, 3)), Error::InvalidPhase);
}

#[test]
fn unknown_results_and_missing_delivery_ack_block_retirement_indefinitely() {
    let mut m = machine();
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
    send(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(1).request_id,
        },
    );
    drain(&mut m);
    send(&mut m, Event::DrainAck(auth(1, 2)));
    let wait = m.wait().unwrap();
    for _ in 0..3 {
        assert_eq!(
            send(&mut m, Event::Timeout(wait)),
            vec![Effect::Diagnostic(Diagnostic::TimedOut(Phase::DrainingOld))]
        );
    }
    reject(
        &mut m,
        Event::RestResultAck {
            generation: GenerationId(1),
            operation_id: op(1),
        },
        Error::ResultNotReceived,
    );
    let first = send(
        &mut m,
        Event::RestResult {
            generation: GenerationId(1),
            operation_id: op(1),
        },
    );
    assert_eq!(
        send(
            &mut m,
            Event::RestResult {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        ),
        first
    );
    assert!(
        !send(&mut m, Event::Timeout(wait))
            .iter()
            .any(|e| matches!(e, Effect::Stop(_)))
    );
    assert_eq!(m.phase(), Phase::DrainingOld);
}

#[test]
fn rest_delivery_before_drain_ack_does_not_stop_old_early() {
    let mut m = machine();
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
    drain(&mut m);
    send(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(1).request_id,
        },
    );
    send(
        &mut m,
        Event::RestResult {
            generation: GenerationId(1),
            operation_id: op(1),
        },
    );
    assert!(
        send(
            &mut m,
            Event::RestResultAck {
                generation: GenerationId(1),
                operation_id: op(1)
            }
        )
        .is_empty()
    );
    assert_eq!(m.phase(), Phase::DrainingOld);
    assert_eq!(
        send(&mut m, Event::DrainAck(auth(1, 2))),
        vec![Effect::Stop(GenerationId(1))]
    );
}

#[test]
fn identities_unknown_ids_duplicates_and_out_of_order_acks_are_rejected() {
    let mut m = machine();
    let wrong_keys = [
        OwnershipKey {
            bot_id: BotId(8),
            ..key()
        },
        OwnershipKey {
            shard_id: 3,
            ..key()
        },
    ];
    for other in wrong_keys {
        let before = m.clone();
        assert_eq!(
            m.handle(other, Event::AuthorityLost),
            Err(Error::WrongShard)
        );
        assert_eq!(
            m.admit_work(other, auth(1, 1), origin(1)),
            Err(Error::WrongShard)
        );
        assert_eq!(
            m.register_rest(other, origin(1).request_id, op(1)),
            Err(Error::WrongShard)
        );
        assert_eq!(m, before);
    }
    assert_eq!(
        m.register_rest(key(), origin(1).request_id, op(1)),
        Err(Error::UnknownRequest)
    );
    m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
    assert_eq!(
        m.admit_work(key(), auth(1, 1), origin(1)),
        Err(Error::DuplicateRequest)
    );
    m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
    assert_eq!(
        m.register_rest(key(), origin(1).request_id, op(1)),
        Err(Error::DuplicateOperation)
    );
    reject(
        &mut m,
        Event::RestResult {
            generation: GenerationId(2),
            operation_id: op(1),
        },
        Error::WrongGeneration,
    );
    reject(
        &mut m,
        Event::RestResult {
            generation: GenerationId(1),
            operation_id: op(2),
        },
        Error::UnknownOperation,
    );
    reject(
        &mut m,
        Event::RestResultAck {
            generation: GenerationId(1),
            operation_id: op(2),
        },
        Error::UnknownOperation,
    );
    reject(
        &mut m,
        Event::RestResultAck {
            generation: GenerationId(2),
            operation_id: op(1),
        },
        Error::WrongGeneration,
    );
    reject(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(2),
            request_id: origin(1).request_id,
        },
        Error::WrongGeneration,
    );
    reject(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(2).request_id,
        },
        Error::UnknownRequest,
    );
    send(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(1).request_id,
        },
    );
    reject(
        &mut m,
        Event::WorkFinished {
            generation: GenerationId(1),
            request_id: origin(1).request_id,
        },
        Error::WorkFinished,
    );
}

#[test]
fn epochs_never_wrap_and_old_grants_cannot_reactivate_a_gateway() {
    let mut m = machine();
    pause(&mut m);
    for stale in [0, 1] {
        reject(
            &mut m,
            Event::AuthorityGranted(auth(2, stale)),
            Error::StaleEpoch,
        );
    }
    reject(
        &mut m,
        Event::AuthorityGranted(auth(1, 2)),
        Error::WrongGeneration,
    );
    send(&mut m, Event::AuthorityGranted(auth(2, u64::MAX)));
    fail_candidate(&mut m);
    send(&mut m, Event::CandidateQuiesced(auth(2, u64::MAX)));
    for stale in [0, 1, 2, u64::MAX] {
        reject(
            &mut m,
            Event::AuthorityGranted(auth(1, stale)),
            Error::StaleEpoch,
        );
    }
    assert_eq!(m.active(), None);
    assert_eq!(m.phase(), Phase::AwaitingOldAuthority);
}

#[test]
fn cancelled_deferred_update_never_spawns_and_generations_are_not_reused() {
    let mut m = machine();
    let defer = IdentifyDecision::Defer(IdentifyDeferral {
        reason: IdentifyBlock::BudgetExhausted,
        retry_after: Duration::MAX,
    });
    send(
        &mut m,
        Event::Begin {
            candidate: GenerationId(2),
            identify: defer,
        },
    );
    assert_eq!(
        send(
            &mut m,
            Event::Failed {
                generation: GenerationId(2),
                reason: Failure::Cancelled
            }
        ),
        vec![Effect::Diagnostic(Diagnostic::UpdateAborted(
            Failure::Cancelled
        ))]
    );
    for generation in [0, 1, 2] {
        reject(
            &mut m,
            Event::Begin {
                candidate: GenerationId(generation),
                identify: IdentifyDecision::Reserved,
            },
            Error::GenerationReused,
        );
    }
    send(
        &mut m,
        Event::Begin {
            candidate: GenerationId(3),
            identify: IdentifyDecision::Reserved,
        },
    );
}

#[test]
fn next_rolling_update_ignores_previous_generation_and_epoch_messages() {
    let mut m = machine();
    drain(&mut m);
    send(&mut m, Event::DrainAck(auth(1, 2)));
    send(
        &mut m,
        Event::Begin {
            candidate: GenerationId(3),
            identify: IdentifyDecision::Reserved,
        },
    );
    reject(
        &mut m,
        Event::Spawned(GenerationId(2)),
        Error::WrongGeneration,
    );
    reject(
        &mut m,
        Event::CandidateQuiesced(auth(2, 2)),
        Error::WrongGeneration,
    );
    send(&mut m, Event::Spawned(GenerationId(3)));
    assert_eq!(
        send(&mut m, Event::WarmReady(GenerationId(3))),
        vec![Effect::PauseOld(auth(2, 2))]
    );
    reject(&mut m, Event::OldPaused(auth(1, 1)), Error::WrongGeneration);
    send(&mut m, Event::OldPaused(auth(2, 2)));
    send(&mut m, Event::AuthorityGranted(auth(3, 7)));
    send(&mut m, Event::ActiveAck(auth(3, 7)));
    assert_eq!(drain_ack(&mut m, 2), vec![Effect::Stop(GenerationId(2))]);
    assert_eq!(m.active(), Some(auth(3, 7)));
}

#[test]
fn repeated_drain_after_rollback_rejects_previous_ack_and_retains_id_tombstones() {
    for first_drain_acked in [false, true] {
        let mut m = machine();
        m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
        m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
        send(
            &mut m,
            Event::WorkFinished {
                generation: GenerationId(1),
                request_id: origin(1).request_id,
            },
        );
        drain(&mut m);
        if first_drain_acked {
            send(&mut m, Event::DrainAck(auth(1, 2)));
        }
        fail_candidate(&mut m);
        restore(&mut m, 2, 3);
        drain_ack(&mut m, 2);
        assert_eq!(m.phase(), Phase::Stable);
        m.admit_work(key(), auth(1, 3), origin(2)).unwrap();
        m.register_rest(key(), origin(2).request_id, op(2)).unwrap();
        send(
            &mut m,
            Event::RestResult {
                generation: GenerationId(1),
                operation_id: op(1),
            },
        );
        send(
            &mut m,
            Event::RestResultAck {
                generation: GenerationId(1),
                operation_id: op(1),
            },
        );
        send(
            &mut m,
            Event::Begin {
                candidate: GenerationId(3),
                identify: IdentifyDecision::Reserved,
            },
        );
        send(&mut m, Event::Spawned(GenerationId(3)));
        send(&mut m, Event::WarmReady(GenerationId(3)));
        send(&mut m, Event::OldPaused(auth(1, 3)));
        send(&mut m, Event::AuthorityGranted(auth(3, 4)));
        assert_eq!(
            send(&mut m, Event::ActiveAck(auth(3, 4))),
            vec![Effect::Drain(auth(1, 4))]
        );
        send(
            &mut m,
            Event::WorkFinished {
                generation: GenerationId(1),
                request_id: origin(2).request_id,
            },
        );
        reject(&mut m, Event::DrainAck(auth(1, 2)), Error::StaleEpoch);
        send(
            &mut m,
            Event::RestResult {
                generation: GenerationId(1),
                operation_id: op(2),
            },
        );
        assert!(
            send(
                &mut m,
                Event::RestResultAck {
                    generation: GenerationId(1),
                    operation_id: op(2)
                }
            )
            .is_empty()
        );
        assert_eq!(
            send(&mut m, Event::DrainAck(auth(1, 4))),
            vec![Effect::Stop(GenerationId(1))]
        );
        assert_eq!(
            m.admit_work(key(), auth(3, 4), origin(1)),
            Err(Error::DuplicateRequest)
        );
        m.admit_work(key(), auth(3, 4), origin(3)).unwrap();
        assert_eq!(
            m.register_rest(key(), origin(3).request_id, op(1)),
            Err(Error::DuplicateOperation)
        );
    }
}

#[test]
fn ownership_coordinator_grants_fence_switch_and_rollback_admission() {
    struct Fixed;
    impl Clock for Fixed {
        fn now(&self) -> Duration {
            Duration::ZERO
        }
    }
    let mut coordinator = StandaloneCoordinator::new(Fixed);
    let initial = coordinator.acquire(key(), Duration::from_secs(30)).unwrap();
    let mut m = GatewaySwitch::new(
        key(),
        Authority {
            generation: GenerationId(1),
            epoch: initial.epoch,
        },
    );
    pause(&mut m);
    let next = coordinator.acquire(key(), Duration::from_secs(30)).unwrap();
    send(
        &mut m,
        Event::AuthorityGranted(Authority {
            generation: GenerationId(2),
            epoch: next.epoch,
        }),
    );
    assert_eq!(
        coordinator.check(key(), initial.epoch),
        Err(OwnershipError::Fenced)
    );
    coordinator.check(key(), next.epoch).unwrap();
    fail_candidate(&mut m);
    send(&mut m, Event::CandidateQuiesced(auth(2, 2)));
    let rollback = coordinator.acquire(key(), Duration::from_secs(30)).unwrap();
    assert_eq!(
        coordinator.check(key(), next.epoch),
        Err(OwnershipError::Fenced)
    );
    send(
        &mut m,
        Event::AuthorityGranted(Authority {
            generation: GenerationId(1),
            epoch: rollback.epoch,
        }),
    );
    coordinator.check(key(), rollback.epoch).unwrap();
    send(&mut m, Event::ActiveAck(auth(1, 3)));
    m.admit_work(key(), auth(1, 3), origin(1)).unwrap();
    send(&mut m, Event::AuthorityLost);
    assert_eq!(
        m.admit_work(key(), auth(1, 3), origin(2)),
        Err(Error::AdmissionClosed)
    );
}

#[test]
fn old_loss_and_lease_loss_fail_closed_without_stopping_unsettled_rest() {
    for candidate_active in [false, true] {
        for lease_loss in [false, true] {
            let mut m = machine();
            m.admit_work(key(), auth(1, 1), origin(1)).unwrap();
            m.register_rest(key(), origin(1).request_id, op(1)).unwrap();
            if candidate_active {
                activate(&mut m);
            } else {
                begin(&mut m);
            }
            let event = if lease_loss {
                Event::AuthorityLost
            } else {
                Event::Failed {
                    generation: GenerationId(1),
                    reason: Failure::GatewayFailed,
                }
            };
            let effects = send(&mut m, event);
            assert!(effects.contains(&Effect::RecoverShard));
            assert!(
                !effects
                    .iter()
                    .any(|e| matches!(e, Effect::Stop(_) | Effect::Activate(_)))
            );
            assert_eq!(m.active(), None);
            assert_eq!(m.pending_rest(GenerationId(1)), 1);
            reject(
                &mut m,
                Event::AuthorityGranted(auth(1, 3)),
                Error::InvalidPhase,
            );
            reject(&mut m, Event::DrainAck(auth(1, 2)), Error::InvalidPhase);
            assert!(matches!(
                send(
                    &mut m,
                    Event::RestResult {
                        generation: GenerationId(1),
                        operation_id: op(1)
                    }
                )
                .as_slice(),
                [Effect::RouteRestResult { .. }]
            ));
        }
    }
}

// Explore every reachable lifecycle state over a finite event alphabet. A second
// model tracks peers that may execute, independently of the machine's getter:
// a sent Activate executes immediately; Pause/Quiesce stops only on its ACK.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Model {
    machine: GatewaySwitch,
    executing: Vec<GenerationId>,
}

#[test]
fn exhaustive_event_sequences_preserve_single_active_and_control_barriers() {
    let mut events = vec![Event::RetryAuthority, Event::AuthorityLost];
    for candidate in [2, 3] {
        for identify in [
            IdentifyDecision::Reserved,
            IdentifyDecision::Defer(IdentifyDeferral {
                reason: IdentifyBlock::BudgetExhausted,
                retry_after: Duration::ZERO,
            }),
        ] {
            events.push(Event::Begin {
                candidate: GenerationId(candidate),
                identify,
            });
            events.push(Event::RetryIdentify(identify));
        }
    }
    for generation in 1..=3 {
        let g = GenerationId(generation);
        events.extend([
            Event::Spawned(g),
            Event::WarmReady(g),
            Event::AuthorityDenied(g),
            Event::Failed {
                generation: g,
                reason: Failure::GatewayFailed,
            },
        ]);
        for epoch in [0, 1, 2, 3, 4, u64::MAX] {
            let a = auth(generation, epoch);
            events.extend([
                Event::OldPaused(a),
                Event::AuthorityGranted(a),
                Event::ActiveAck(a),
                Event::CandidateQuiesced(a),
                Event::DrainAck(a),
            ]);
        }
    }
    let initial = Model {
        machine: machine(),
        executing: vec![GenerationId(1)],
    };
    let mut queue = VecDeque::from([initial.clone()]);
    let mut seen = HashSet::from([format!("{initial:?}")]);
    let mut checked = 0;
    while let Some(model) = queue.pop_front() {
        let mut inputs = events.clone();
        if let Some(wait) = model.machine.wait() {
            inputs.push(Event::Timeout(wait));
        }
        for event in inputs {
            let mut next = model.clone();
            let result = next.machine.handle(key(), event);
            checked += 1;
            let Ok(effects) = result else {
                assert_eq!(
                    next.machine, model.machine,
                    "error mutated state for {event:?}"
                );
                continue;
            };
            if let Event::OldPaused(a) | Event::CandidateQuiesced(a) = event {
                next.executing.retain(|g| *g != a.generation);
            }
            for effect in effects {
                match effect {
                    Effect::Activate(a) => {
                        assert!(
                            next.executing.iter().all(|g| *g == a.generation),
                            "overlapping execution: {model:?}, {event:?}, {a:?}"
                        );
                        assert!(a.epoch.0 > model.machine.epoch().0);
                        assert!(matches!(
                            model.machine.phase(),
                            Phase::AwaitingCandidateAuthority | Phase::AwaitingOldAuthority
                        ));
                        next.executing = vec![a.generation];
                    }
                    Effect::Stop(g) => {
                        assert!(matches!(
                            model.machine.phase(),
                            Phase::DrainingOld | Phase::DrainingCandidate
                        ));
                        assert_ne!(next.machine.active().map(|a| a.generation), Some(g));
                        next.executing.retain(|executing| *executing != g);
                    }
                    Effect::SpawnStandby(_) | Effect::IdentifyStandby(_) => {
                        assert!(matches!(
                            next.machine.phase(),
                            Phase::Spawning | Phase::Warming
                        ));
                    }
                    _ => {}
                }
            }
            assert!(next.executing.len() <= 1);
            // Try every generation/epoch against the actual command gate.
            let mut admitted = 0;
            for g in 1..=3 {
                for epoch in [1, 2, 3, 4, u64::MAX] {
                    let mut probe = next.machine.clone();
                    if probe.admit_work(key(), auth(g, epoch), origin(99)).is_ok() {
                        admitted += 1;
                    }
                }
            }
            assert!(admitted <= 1, "multiple admissions: {next:?}");
            let fingerprint = format!("{next:?}");
            if seen.insert(fingerprint) {
                queue.push_back(next);
            }
        }
    }
    assert!(seen.len() > 100, "only {} states explored", seen.len());
    assert!(checked > 10_000);
    eprintln!(
        "explored {} reachable states and {checked} transitions",
        seen.len()
    );
}

#[test]
fn mock_identify_scheduler_tolerates_out_of_order_reservation_times() {
    // Two Shards sample their time before locking the shared scheduler, so the one
    // that locks second can carry an earlier `now`. That must not be an error.
    let mut scheduler = MockIdentifyScheduler::new(vec![MockIdentifyLimit {
        bot_id: 7,
        total: 10,
        remaining: 10,
        reset_after_ms: 60_000,
        max_concurrency: 2,
        interval_ms: 5_000,
    }])
    .unwrap();
    assert!(matches!(
        scheduler.reserve(7, 0, Duration::from_millis(10)),
        Ok(IdentifyDecision::Reserved)
    ));
    // Different bucket (shard 1 % 2), earlier timestamp than the previous call.
    assert!(matches!(
        scheduler.reserve(7, 1, Duration::from_millis(5)),
        Ok(IdentifyDecision::Reserved)
    ));
    // Same bucket as shard 0 is still rate limited, using the clamped time.
    match scheduler.reserve(7, 2, Duration::from_millis(6)) {
        Ok(IdentifyDecision::Defer(deferral)) => {
            assert!(deferral.retry_after <= Duration::from_millis(5_000));
            assert!(deferral.retry_after > Duration::ZERO);
        }
        other => panic!("expected a rate limit deferral, got {other:?}"),
    }
}
