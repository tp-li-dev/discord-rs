//! Deterministic control-plane simulations, not distributed-consensus or Discord
//! integration proofs. The finite search bounds below are deliberately explicit.
use src::{
    cluster::{
        ClusterSimulation, Link, NodeId, ProposalKind, ShardPlacement, SimulationConfig, TraceEvent,
    },
    failover::State,
    ownership::{OwnershipError, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
};
use std::time::Duration;

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);
const NODES: [NodeId; 3] = [A, B, C];

fn key(bot: u64, shard: u32) -> OwnershipKey {
    OwnershipKey {
        bot_id: BotId(bot),
        shard_id: shard,
    }
}

fn placements() -> Vec<ShardPlacement> {
    [11, 22]
        .into_iter()
        .flat_map(|bot| {
            NODES
                .into_iter()
                .enumerate()
                .map(move |(shard, primary)| ShardPlacement {
                    key: key(bot, shard as u32),
                    primary,
                    failover: vec![NODES[(shard + 1) % 3], NODES[(shard + 2) % 3]],
                })
        })
        .collect()
}

fn simulation(config: SimulationConfig, seed: u64) -> ClusterSimulation {
    ClusterSimulation::new(NODES.to_vec(), placements(), config, seed).unwrap()
}

fn ready() -> ClusterSimulation {
    let mut sim = simulation(SimulationConfig::default(), 1);
    sim.advance(Duration::from_secs(1)).unwrap();
    assert_placement(&sim);
    sim
}

fn assert_safe(sim: &ClusterSimulation) {
    sim.assert_single_active().unwrap();
    // Independently inspect all Bot/Shard keys, including equal shard IDs in
    // different Bots. A global "one active node" check would miss isolation bugs.
    for placement in placements() {
        let owners = sim.active_owners(placement.key);
        assert!(
            owners.len() <= 1,
            "at {:?}, {:?}: {owners:?}",
            sim.now(),
            placement.key,
        );
        for owner in owners {
            let lease = sim.local_lease(owner, placement.key).unwrap();
            assert_eq!(lease.key, placement.key);
            assert!(lease.expires_at > sim.now());
        }
    }
}

fn assert_placement(sim: &ClusterSimulation) {
    assert_safe(sim);
    for placement in placements() {
        assert_eq!(
            sim.active_owners(placement.key),
            vec![placement.primary],
            "at {:?}, {:?}, record={:?}, local={:?}",
            sim.now(),
            placement.key,
            sim.ownership(placement.key),
            NODES.map(|node| (
                node,
                sim.policy_state(node, placement.key),
                sim.local_lease(node, placement.key),
            )),
        );
    }
}

fn advance(sim: &mut ClusterSimulation, duration: Duration) {
    sim.advance(duration).unwrap();
    assert_safe(sim);
}

fn restore_links(sim: &mut ClusterSimulation) {
    sim.heal();
    for from in NODES {
        for to in NODES.into_iter().filter(|to| *to != from) {
            sim.network_mut()
                .set_link(from, to, Link::default())
                .unwrap();
        }
    }
}

fn until(
    sim: &mut ClusterSimulation,
    timeout: Duration,
    predicate: impl Fn(&ClusterSimulation) -> bool,
) {
    let end = sim.now() + timeout;
    while !predicate(sim) && sim.now() < end {
        let step = Duration::from_millis(100).min(end - sim.now());
        advance(sim, step);
    }
    assert!(
        predicate(sim),
        "condition not reached at {:?}; latest policy transitions: {:?}",
        sim.now(),
        sim.trace()
            .iter()
            .rev()
            .filter(|entry| matches!(entry.event, TraceEvent::Policy { .. }))
            .take(24)
            .collect::<Vec<_>>(),
    );
}

#[test]
fn t10_three_nodes_match_configured_placement_for_multiple_bots() {
    let mut sim = ready();
    for _ in 0..60 {
        advance(&mut sim, Duration::from_millis(500));
        assert_placement(&sim);
    }
    for node in NODES {
        for peer in NODES.into_iter().filter(|peer| *peer != node) {
            let seen = sim.peer(node, peer).unwrap().last_seen.unwrap();
            assert!(sim.now() - seen <= Duration::from_millis(500));
        }
    }
}

#[test]
fn t11_server_stop_is_detected_at_three_seconds_before_proxy_activation() {
    let mut sim = ready();
    let shard = key(11, 0);
    let old = sim.local_lease(A, shard).unwrap();
    let last_seen = sim.peer(B, A).unwrap().last_seen.unwrap();
    sim.stop_node(A).unwrap();
    let detection = last_seen + Duration::from_secs(3);
    let remaining = detection - sim.now() - Duration::from_nanos(1);
    advance(&mut sim, remaining);
    assert!(sim.active_owners(shard).is_empty());
    assert_eq!(sim.policy_state(B, shard), Some(State::Suspected));
    advance(&mut sim, Duration::from_nanos(1));
    assert_eq!(sim.policy_state(B, shard), Some(State::FailoverPending));
    until(&mut sim, Duration::from_secs(6), |sim| {
        sim.active_owners(shard) == vec![B]
    });
    assert!(sim.now() >= old.expires_at);
    assert!(sim.local_lease(B, shard).unwrap().epoch.0 > old.epoch.0);
    assert_eq!(sim.active_owners(key(22, 0)), vec![B]);
    assert_eq!(sim.active_owners(key(11, 1)), vec![B]);
    assert_eq!(sim.active_owners(key(22, 2)), vec![C]);
    assert!(sim.check(A, shard, old.epoch).is_err());
}

#[test]
fn t12_recovered_primary_waits_thirty_stable_seconds_then_receives_new_epoch() {
    let mut sim = ready();
    let shard = key(11, 0);
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(9));
    let proxy = sim.local_lease(B, shard).unwrap();
    assert_eq!(sim.active_owners(shard), vec![B]);
    sim.recover_node(A).unwrap();
    until(&mut sim, Duration::from_secs(70), |sim| {
        sim.policy_state(B, shard) == Some(State::FailbackWaiting)
    });
    let stable_start = sim.now();
    advance(&mut sim, Duration::from_millis(29_999));
    assert_eq!(sim.active_owners(shard), vec![B]);
    assert_eq!(sim.local_lease(B, shard).unwrap().epoch, proxy.epoch);
    until(&mut sim, Duration::from_secs(10), |sim| {
        sim.active_owners(shard) == vec![A]
    });
    assert!(sim.now() >= stable_start + Duration::from_secs(30));
    let returned = sim.local_lease(A, shard).unwrap();
    assert!(returned.epoch.0 > proxy.epoch.0);
    assert!(sim.check(B, shard, proxy.epoch).is_err());
    advance(&mut sim, Duration::from_secs(5));
    assert_placement(&sim);
}

#[test]
fn t12_flapping_primary_restarts_stability_and_does_not_bounce_ownership() {
    let mut sim = ready();
    let shard = key(11, 0);
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(9));
    let proxy_epoch = sim.local_lease(B, shard).unwrap().epoch;
    sim.recover_node(A).unwrap();
    until(&mut sim, Duration::from_secs(70), |sim| {
        sim.policy_state(B, shard) == Some(State::FailbackWaiting)
    });
    advance(&mut sim, Duration::from_secs(20));
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(2));
    assert_ne!(sim.policy_state(B, shard), Some(State::FailbackWaiting));
    assert_eq!(sim.active_owners(shard), vec![B]);
    sim.recover_node(A).unwrap();
    until(&mut sim, Duration::from_secs(70), |sim| {
        sim.policy_state(B, shard) == Some(State::FailbackWaiting)
    });
    advance(&mut sim, Duration::from_millis(29_999));
    assert_eq!(sim.active_owners(shard), vec![B]);
    assert_eq!(sim.local_lease(B, shard).unwrap().epoch, proxy_epoch);
    until(&mut sim, Duration::from_secs(10), |sim| {
        sim.active_owners(shard) == vec![A]
    });
}

#[test]
fn failback_in_flight_is_cancelled_when_recovered_primary_becomes_unstable() {
    let mut sim = simulation(
        SimulationConfig {
            lease_ttl: Duration::from_secs(6),
            proposal_timeout: Duration::from_secs(5),
            ..SimulationConfig::default()
        },
        1,
    );
    advance(&mut sim, Duration::from_secs(1));
    let shard = key(11, 0);
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(12));
    assert_eq!(sim.active_owners(shard), vec![B]);
    sim.recover_node(A).unwrap();
    until(&mut sim, Duration::from_secs(70), |sim| {
        sim.policy_state(B, shard) == Some(State::FailbackWaiting)
    });
    let before = sim.ownership(shard).unwrap();
    until(&mut sim, Duration::from_secs(32), |sim| {
        sim.trace().iter().any(|entry| {
            matches!(entry.event, TraceEvent::Proposed(p)
                if p.key == shard && p.kind == ProposalKind::Failback)
        })
    });
    let outstanding = sim.local_lease(B, shard).unwrap();
    assert!(outstanding.expires_at - sim.now() > Duration::from_millis(1500));
    sim.partition(&[A], &[B, C]).unwrap();
    advance(&mut sim, Duration::from_secs(2));
    assert_ne!(sim.policy_state(B, shard), Some(State::FailbackWaiting));
    assert_eq!(sim.ownership(shard).unwrap().owner, B);
    assert_eq!(
        sim.ownership(shard).unwrap().lease.epoch,
        before.lease.epoch
    );
    advance(&mut sim, Duration::from_secs(5));
    assert!(!sim.active_owners(shard).contains(&A));
    sim.heal();
    until(&mut sim, Duration::from_secs(70), |sim| {
        sim.policy_state(B, shard) == Some(State::FailbackWaiting)
    });
    advance(&mut sim, Duration::from_millis(29_999));
    assert_eq!(sim.active_owners(shard), vec![B]);
    until(&mut sim, Duration::from_secs(12), |sim| {
        sim.active_owners(shard) == vec![A]
    });
}

#[test]
fn t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry() {
    let mut sim = ready();
    let shard = key(11, 0);
    let old = sim.local_lease(A, shard).unwrap();
    sim.partition(&[A], &[B, C]).unwrap();
    let until_expiry = old.expires_at - sim.now();
    advance(&mut sim, until_expiry - Duration::from_nanos(1));
    // The old Discord connection is modeled as alive. Shared test authority
    // must not magically revoke this independent, disconnected local gate.
    assert_eq!(sim.active_owners(shard), vec![A]);
    assert_eq!(sim.check(A, shard, old.epoch), Ok(()));
    advance(&mut sim, Duration::from_nanos(1));
    assert!(!sim.active_owners(shard).contains(&A));
    assert!(sim.check(A, shard, old.epoch).is_err());
    until(&mut sim, Duration::from_secs(6), |sim| {
        sim.active_owners(shard) == vec![B]
    });
    assert!(sim.local_lease(B, shard).unwrap().epoch.0 > old.epoch.0);
    sim.heal();
    advance(&mut sim, Duration::from_secs(45));
    assert_placement(&sim);
}

#[test]
fn t14_minority_never_promotes_or_renews_and_recovers_after_quorum_returns() {
    let mut sim = ready();
    let shard = key(11, 0);
    let before = sim.ownership(shard).unwrap();
    sim.stop_node(A).unwrap();
    sim.stop_node(B).unwrap();
    advance(&mut sim, Duration::from_secs(20));
    assert_eq!(sim.ownership(shard).unwrap(), before);
    assert!(sim.active_owners(shard).is_empty());
    assert_eq!(sim.policy_state(C, shard), Some(State::Degraded));
    for placement in placements() {
        assert!(sim.active_owners(placement.key).is_empty());
    }
    sim.recover_node(B).unwrap();
    until(&mut sim, Duration::from_secs(15), |sim| {
        !sim.active_owners(shard).is_empty()
    });
    sim.recover_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(50));
    assert_placement(&sim);
}

#[test]
fn all_isolated_nodes_expire_and_cannot_count_themselves_as_a_majority() {
    let mut sim = ready();
    sim.partition(&[A], &[B, C]).unwrap();
    sim.partition(&[B], &[C]).unwrap();
    let epochs: Vec<_> = placements()
        .iter()
        .map(|p| sim.ownership(p.key).unwrap().lease.epoch)
        .collect();
    advance(&mut sim, Duration::from_secs(30));
    for (placement, epoch) in placements().into_iter().zip(epochs) {
        assert!(sim.active_owners(placement.key).is_empty());
        assert_eq!(sim.ownership(placement.key).unwrap().lease.epoch, epoch);
    }
    sim.heal();
    advance(&mut sim, Duration::from_secs(15));
    assert_placement(&sim);
}

#[test]
fn two_member_configuration_cannot_shrink_quorum_after_a_stop() {
    let shard = key(11, 0);
    let mut sim = ClusterSimulation::new(
        vec![A, B],
        vec![ShardPlacement {
            key: shard,
            primary: A,
            failover: vec![B],
        }],
        SimulationConfig::default(),
        1,
    )
    .unwrap();
    sim.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(sim.active_owners(shard), vec![A]);
    let before = sim.ownership(shard).unwrap();
    sim.stop_node(A).unwrap();
    sim.advance(Duration::from_secs(20)).unwrap();
    assert!(sim.active_owners(shard).is_empty());
    assert_eq!(sim.ownership(shard).unwrap(), before);
}

#[test]
fn startup_delay_never_exposes_a_prepared_shard_as_active() {
    let mut sim = simulation(
        SimulationConfig {
            startup_delay: Duration::from_secs(2),
            ..SimulationConfig::default()
        },
        1,
    );
    advance(&mut sim, Duration::from_millis(1999));
    for placement in placements() {
        assert!(sim.active_owners(placement.key).is_empty());
    }
    until(&mut sim, Duration::from_secs(3), |sim| {
        placements()
            .iter()
            .all(|p| sim.active_owners(p.key) == vec![p.primary])
    });
}

#[test]
fn one_way_partition_and_delayed_acknowledgements_cannot_extend_old_authority() {
    let mut sim = ready();
    let shard = key(11, 0);
    let old = sim.local_lease(A, shard).unwrap();
    for peer in [B, C] {
        sim.network_mut()
            .set_link(
                peer,
                A,
                Link {
                    delay: Duration::from_secs(8),
                    ..Link::default()
                },
            )
            .unwrap();
    }
    advance(&mut sim, Duration::from_secs(10));
    assert!(sim.check(A, shard, old.epoch).is_err());
    assert!(!sim.active_owners(shard).contains(&A));
    restore_links(&mut sim);
    advance(&mut sim, Duration::from_secs(45));
    assert_placement(&sim);
}

#[test]
fn proxy_stop_during_primary_recovery_does_not_leave_a_permanent_vacancy() {
    let mut sim = ready();
    let shard = key(11, 0);
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(8));
    assert_eq!(sim.active_owners(shard), vec![B]);
    // There are still two voters after A returns and B stops. The old proxy
    // deadline must be respected before one surviving candidate takes over.
    sim.recover_node(A).unwrap();
    sim.stop_node(B).unwrap();
    advance(&mut sim, Duration::from_secs(12));
    assert_eq!(sim.active_owners(shard).len(), 1);
    assert!(!sim.active_owners(shard).contains(&B));
    assert_safe(&sim);
}

#[test]
fn invalid_membership_placement_and_timing_are_rejected() {
    let config = SimulationConfig::default();
    for members in [vec![], vec![A, A, B], vec![A, B]] {
        assert!(ClusterSimulation::new(members, placements(), config, 1).is_err());
    }
    let mut duplicate = placements();
    duplicate.push(duplicate[0].clone());
    assert!(ClusterSimulation::new(NODES.to_vec(), duplicate, config, 1).is_err());
    let mut unknown = placements();
    unknown[0].failover.push(NodeId(999));
    assert!(ClusterSimulation::new(NODES.to_vec(), unknown, config, 1).is_err());
    let mut zero_ttl = config;
    zero_ttl.lease_ttl = Duration::ZERO;
    assert!(ClusterSimulation::new(NODES.to_vec(), placements(), zero_ttl, 1).is_err());
    let mut invalid_timer = config;
    invalid_timer.failover.heartbeat_interval = Duration::ZERO;
    assert!(ClusterSimulation::new(NODES.to_vec(), placements(), invalid_timer, 1).is_err());
}

#[test]
fn complete_packet_loss_denies_bootstrap_until_real_acknowledgements_arrive() {
    let mut sim = simulation(SimulationConfig::default(), 1);
    for from in NODES {
        for to in NODES.into_iter().filter(|to| *to != from) {
            sim.network_mut()
                .set_link(
                    from,
                    to,
                    Link {
                        loss_per_mille: 1000,
                        ..Link::default()
                    },
                )
                .unwrap();
        }
    }
    advance(&mut sim, Duration::from_secs(10));
    for placement in placements() {
        assert!(sim.active_owners(placement.key).is_empty());
        assert!(sim.ownership(placement.key).is_none());
    }
    restore_links(&mut sim);
    advance(&mut sim, Duration::from_secs(10));
    assert_placement(&sim);
}

#[test]
fn restart_preserves_monotonic_epochs_and_rejects_old_generation() {
    let mut sim = ready();
    let shard = key(11, 0);
    let first = sim.local_lease(A, shard).unwrap();
    sim.stop_node(A).unwrap();
    advance(&mut sim, Duration::from_secs(9));
    let second = sim.local_lease(B, shard).unwrap();
    sim.recover_node(A).unwrap();
    until(&mut sim, Duration::from_secs(90), |sim| {
        sim.active_owners(shard) == vec![A]
    });
    let third = sim.local_lease(A, shard).unwrap();
    assert!(first.epoch.0 < second.epoch.0 && second.epoch.0 < third.epoch.0);
    assert_eq!(
        sim.check(A, shard, first.epoch),
        Err(OwnershipError::Fenced)
    );
    assert_eq!(
        sim.check(A, shard, OwnershipEpoch(0)),
        Err(OwnershipError::Fenced)
    );
    assert_eq!(sim.check(A, shard, third.epoch), Ok(()));
}

fn apply_fault(sim: &mut ClusterSimulation, action: u64, running: &mut [bool; 3]) {
    match action {
        0 => sim.partition(&[A], &[B, C]).unwrap(),
        1 => sim.partition(&[B], &[A, C]).unwrap(),
        2 => sim.heal(),
        3 if running[0] => {
            sim.stop_node(A).unwrap();
            running[0] = false;
        }
        4 if !running[0] => {
            sim.recover_node(A).unwrap();
            running[0] = true;
        }
        _ => {}
    }
}

#[test]
fn bounded_exhaustive_fault_words_preserve_single_active() {
    // Exhaust every word of length 0..=4 over six external events:
    // isolate A, isolate B, heal, stop A, recover A, time-only.
    // Each letter is followed by exactly two seconds of simulated time.
    // This is 1 + 6 + 36 + 216 + 1,296 = 1,555 finite words, not an
    // exhaustive proof over unbounded executions or all packet orderings.
    let mut words = 0;
    let mut transfers = 0;
    for len in 0..=4 {
        for word in 0..6_u64.pow(len) {
            let mut sim = ready();
            let mut running = [true; 3];
            let mut suffix = word;
            for step in 0..len {
                let action = suffix % 6;
                suffix /= 6;
                apply_fault(&mut sim, action, &mut running);
                sim.advance(Duration::from_secs(2)).unwrap_or_else(|error| {
                    panic!("word={word}, len={len}, step={step}, action={action}: {error:?}")
                });
                assert_safe(&sim);
            }
            transfers += usize::from(sim.ownership(key(11, 0)).unwrap().lease.epoch.0 > 1);
            words += 1;
        }
    }
    assert_eq!(words, 1555);
    assert!(
        transfers > 0,
        "search must exercise actual ownership changes"
    );
}

fn next(seed: &mut u64) -> u64 {
    // Fixed algorithm independent of OS RNG and dependency versions.
    *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut word = *seed;
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    word ^ (word >> 31)
}

fn random_run(seed: u64) -> ClusterSimulation {
    let mut sim = simulation(SimulationConfig::default(), seed);
    advance(&mut sim, Duration::from_secs(1));
    assert_placement(&sim);
    let mut random = seed;
    let mut running = [true; 3];
    for step in 0..96 {
        let choice = next(&mut random);
        match choice % 12 {
            action @ 0..=5 => apply_fault(&mut sim, action, &mut running),
            6 => {
                let index = (next(&mut random) % 3) as usize;
                if running[index] {
                    sim.stop_node(NODES[index]).unwrap();
                } else {
                    sim.recover_node(NODES[index]).unwrap();
                }
                running[index] = !running[index];
            }
            _ => {
                let from = (next(&mut random) % 3) as usize;
                let to = (from + 1 + (next(&mut random) % 2) as usize) % 3;
                let link = Link {
                    delay: Duration::from_millis(next(&mut random) % 4001),
                    loss_per_mille: (next(&mut random) % 1001) as u16,
                    enabled: true,
                };
                sim.network_mut()
                    .set_link(NODES[from], NODES[to], link)
                    .unwrap();
            }
        }
        let elapsed = Duration::from_millis(next(&mut random) % 2001);
        sim.advance(elapsed).unwrap_or_else(|error| {
            panic!("seed={seed}, step={step}, elapsed={elapsed:?}: {error:?}")
        });
        assert_safe(&sim);
    }
    restore_links(&mut sim);
    for (index, node) in NODES.into_iter().enumerate() {
        if !running[index] {
            sim.recover_node(node).unwrap();
        }
    }
    // The existing failover policy has two separate backoffs: retry after a
    // failure, then cooldown after a genuinely new proxy startup. Include both
    // 60-second caps, 30-second stability, and the old lease's final deadline.
    // Seed 24 exercises expiry followed by fresh acquisition (epoch 7 -> 8).
    advance(&mut sim, Duration::from_secs(180));
    assert!(
        placements()
            .iter()
            .all(|p| sim.active_owners(p.key) == vec![p.primary]),
        "seed={seed}; recovered at {:?}; recent policy transitions={:?}",
        sim.now() - Duration::from_secs(180),
        sim.trace()
            .iter()
            .rev()
            .filter(
                |entry| matches!(entry.event, TraceEvent::Policy { key: k, .. } if k == key(11, 0))
            )
            .take(24)
            .collect::<Vec<_>>(),
    );
    assert_placement(&sim);
    sim
}

#[test]
fn seeded_fault_scan_checks_every_transition_and_eventually_recovers() {
    // 128 seeds x 96 external fault steps; the simulator also checks every
    // internal delivery/timer/activation, rather than sampling only these steps.
    for seed in 0..128 {
        random_run(seed);
    }
}

#[test]
fn identical_seed_and_fault_schedule_reproduce_the_entire_trace() {
    for seed in [0, 7, 42, 20261112] {
        let first = random_run(seed);
        let second = random_run(seed);
        assert_eq!(first.trace(), second.trace(), "seed={seed}");
    }
}
