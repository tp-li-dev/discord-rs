use src::{
    protocol::{BotId, RequestId},
    supervisor::*,
};
use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::{oneshot, watch};

#[derive(Clone)]
struct ManualClock(watch::Sender<Duration>);
impl Default for ManualClock {
    fn default() -> Self {
        Self(watch::channel(Duration::ZERO).0)
    }
}
impl ManualClock {
    fn set(&self, time: Duration) {
        self.0.send_replace(time);
    }
    fn seconds(&self, seconds: u64) {
        self.set(Duration::from_secs(seconds));
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.0.borrow()
    }
    fn sleep_until(&self, deadline: Duration) -> Sleep<'_> {
        Box::pin(async move {
            let mut changed = self.0.subscribe();
            let mut last = *changed.borrow();
            loop {
                let now = *changed.borrow_and_update();
                if now >= deadline || now < last {
                    return;
                }
                last = now;
                changed.changed().await.unwrap();
            }
        })
    }
}
fn manager(clock: &ManualClock) -> TaskManager<ManualClock> {
    TaskManager::new(clock.clone(), BotId(1), 2, 32).unwrap()
}
fn spec(kind: TaskKind) -> TaskSpec {
    TaskSpec {
        owner: TaskOwner {
            bot_id: BotId(1),
            shard_id: 2,
            cog: Some("test".into()),
            request_id: Some(RequestId([1; 16])),
            parent: None,
        },
        kind,
        timeout: None,
    }
}
fn child(kind: TaskKind, parent: TaskId) -> TaskSpec {
    let mut spec = spec(kind);
    spec.owner.parent = Some(parent);
    spec
}
async fn guarded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("test deadlocked")
}
async fn state(manager: &mut TaskManager<ManualClock>, id: TaskId, expected: TaskState) {
    guarded(async {
        loop {
            manager.poll().unwrap();
            if manager.snapshot(id).unwrap().state == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn supervised_state(
    supervisor: &mut Supervisor<ManualClock>,
    id: TaskId,
    expected: TaskState,
) {
    guarded(async {
        loop {
            supervisor.poll().unwrap();
            if supervisor.manager().snapshot(id).unwrap().state == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn policy() -> RestartPolicy {
    RestartPolicy {
        max_restarts: 4,
        max_restarts_in_window: 4,
        window: Duration::from_secs(100),
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(3),
        circuit_breaker_after: 10,
        circuit_open_for: Duration::from_secs(10),
    }
}
fn failure_factory(counter: &Arc<AtomicUsize>) -> TaskFactory {
    let counter = counter.clone();
    Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(TaskFailure::Application) })
    })
}
fn evidence() -> DrainEvidence {
    DrainEvidence {
        api_results_settled: true,
        state_saved: true,
        ipc_flushed: true,
        durable_jobs_safe: false,
    }
}

#[tokio::test]
async fn registration_execution_results_and_metrics_are_retained_until_forget() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let id = manager.register(spec(TaskKind::RequestScoped)).unwrap();
    assert_eq!(manager.snapshot(id).unwrap().state, TaskState::Registered);
    assert_eq!(manager.metrics().registered, 1);
    manager
        .start(id, |ctx| async move {
            assert_eq!(ctx.attempt, 1);
            assert!(!ctx.cancellation.is_cancelled());
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    assert_eq!(manager.metrics().running, 1);
    assert_eq!(manager.metrics().pending_joins, 1);
    assert_eq!(
        manager.start(id, |_| async { Ok(TaskCompletion::Completed) }),
        Err(Error::InvalidState)
    );
    let result = guarded(manager.wait(id)).await.unwrap();
    assert_eq!(result.result, Some(Ok(TaskCompletion::Completed)));
    assert_eq!(result.started_at, Some(Duration::ZERO));
    assert_eq!(result.finished_at, Some(Duration::ZERO));
    assert_eq!(manager.metrics().completed, 1);
    assert_eq!(manager.metrics().pending_joins, 0);
    manager.forget(id).unwrap();
    assert!(matches!(manager.snapshot(id), Err(Error::UnknownTask)));
}

#[test]
fn invalid_configuration_ownership_capacity_and_no_runtime_do_not_start_work() {
    let clock = ManualClock::default();
    assert!(matches!(
        TaskManager::new(clock.clone(), BotId(1), 2, 0),
        Err(Error::InvalidConfig)
    ));
    let mut manager = TaskManager::new(clock, BotId(1), 2, 1).unwrap();
    for wrong in [
        TaskOwner {
            bot_id: BotId(2),
            ..spec(TaskKind::RequestScoped).owner
        },
        TaskOwner {
            shard_id: 1,
            ..spec(TaskKind::RequestScoped).owner
        },
    ] {
        assert_eq!(
            manager.register(TaskSpec {
                owner: wrong,
                ..spec(TaskKind::RequestScoped)
            }),
            Err(Error::WrongOwner)
        );
    }
    let mut invalid = spec(TaskKind::RequestScoped);
    invalid.timeout = Some(Duration::ZERO);
    assert_eq!(manager.register(invalid), Err(Error::InvalidConfig));
    assert_eq!(
        manager.spawn(spec(TaskKind::RequestScoped), |_| async {
            Ok(TaskCompletion::Completed)
        }),
        Err(Error::NoRuntime)
    );
    let id = manager.register(spec(TaskKind::RequestScoped)).unwrap();
    assert_eq!(
        manager.register(spec(TaskKind::RequestScoped)),
        Err(Error::Capacity)
    );
    manager.request_cancel(id).unwrap();
    assert_eq!(manager.snapshot(id).unwrap().state, TaskState::Cancelled);
    assert_eq!(
        manager.register(spec(TaskKind::RequestScoped)),
        Err(Error::Capacity)
    );
    manager.forget(id).unwrap();
    assert!(manager.register(spec(TaskKind::RequestScoped)).is_ok());
}

#[tokio::test]
async fn registered_cancellation_is_idempotent_and_unknown_ids_are_rejected() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let id = manager.register(spec(TaskKind::RequestScoped)).unwrap();
    manager.request_cancel(id).unwrap();
    manager.request_cancel(id).unwrap();
    assert_eq!(
        guarded(manager.wait(id)).await.unwrap().result,
        Some(Ok(TaskCompletion::Cancelled))
    );
    assert_eq!(
        manager.start(id, |_| async { Ok(TaskCompletion::Completed) }),
        Err(Error::InvalidState)
    );
    assert_eq!(manager.request_cancel(TaskId(999)), Err(Error::UnknownTask));
    assert!(matches!(
        manager.wait(TaskId(999)).await,
        Err(Error::UnknownTask)
    ));
    assert!(matches!(
        manager.children(TaskId(999)),
        Err(Error::UnknownTask)
    ));
}

#[tokio::test]
async fn cancellation_is_level_triggered_for_clones_and_all_waiters() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let (started, ready) = oneshot::channel();
    let id = manager
        .spawn(spec(TaskKind::BackgroundCancellable), |ctx| async move {
            let first = ctx.cancellation.clone();
            let second = ctx.cancellation.clone();
            started.send(()).unwrap();
            tokio::join!(first.cancelled(), second.cancelled());
            assert!(ctx.cancellation.is_cancelled());
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    guarded(ready).await.unwrap();
    manager.request_cancel(id).unwrap();
    assert_eq!(
        manager.snapshot(id).unwrap().state,
        TaskState::CancelRequested
    );
    assert_eq!(
        guarded(manager.wait(id)).await.unwrap().state,
        TaskState::Cancelled
    );
}

#[tokio::test]
async fn cancellation_requests_never_abort_uncooperative_futures_or_assume_their_outcome() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let (finish, release) = oneshot::channel();
    let id = manager
        .spawn(spec(TaskKind::RequestScoped), |_| async move {
            release.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    manager.begin_core_drain();
    manager.request_cancel(id).unwrap();
    for _ in 0..10 {
        manager.poll().unwrap();
        tokio::task::yield_now().await;
    }
    assert_eq!(
        manager.snapshot(id).unwrap().state,
        TaskState::CancelRequested
    );
    assert_eq!(manager.metrics().pending_joins, 1);
    assert!(!manager.core_tasks_finished());
    finish.send(()).unwrap();
    assert_eq!(
        guarded(manager.wait(id)).await.unwrap().state,
        TaskState::Completed
    );
    assert!(manager.core_tasks_finished());
}

#[tokio::test]
async fn timeout_boundary_requests_cooperative_stop_and_manual_clock_wakes_wait() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let mut timed = spec(TaskKind::RequestScoped);
    timed.timeout = Some(Duration::from_secs(5));
    let id = manager
        .spawn(timed, |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    clock.seconds(4);
    manager.poll().unwrap();
    assert!(!manager.snapshot(id).unwrap().timed_out);
    assert_eq!(manager.snapshot(id).unwrap().state, TaskState::Running);
    let advance = async {
        tokio::task::yield_now().await;
        clock.seconds(5);
    };
    let (result, ()) = guarded(async { tokio::join!(manager.wait(id), advance) }).await;
    let result = result.unwrap();
    assert_eq!(result.state, TaskState::Cancelled);
    assert!(result.timed_out);
    assert_eq!(manager.metrics().timed_out, 1);
}

#[tokio::test]
async fn ready_completion_wins_deadline_and_unstarted_registration_has_no_timeout() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let mut timed = spec(TaskKind::RequestScoped);
    timed.timeout = Some(Duration::from_secs(1));
    let registered = manager.register(timed.clone()).unwrap();
    let (done, ready) = oneshot::channel();
    let completed = manager
        .spawn(timed, |_| async move {
            done.send(()).unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    guarded(ready).await.unwrap();
    // No suspension follows the signal, so the join is available before expiry.
    tokio::task::yield_now().await;
    clock.seconds(100);
    manager.poll().unwrap();
    assert_eq!(
        manager.snapshot(completed).unwrap().state,
        TaskState::Completed
    );
    assert!(!manager.snapshot(completed).unwrap().timed_out);
    assert_eq!(
        manager.snapshot(registered).unwrap().state,
        TaskState::Registered
    );
    manager.request_cancel(registered).unwrap();
}

#[tokio::test]
async fn time_overflow_preserves_registration_and_clock_regression_fails_closed_but_reaps() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let mut timed = spec(TaskKind::RequestScoped);
    timed.timeout = Some(Duration::MAX);
    let id = manager.register(timed).unwrap();
    clock.seconds(1);
    assert_eq!(
        manager.start(id, |_| async { Ok(TaskCompletion::Completed) }),
        Err(Error::TimeOverflow)
    );
    assert_eq!(manager.snapshot(id).unwrap().state, TaskState::Registered);
    manager.request_cancel(id).unwrap();
    let running = manager
        .spawn(spec(TaskKind::BackgroundCancellable), |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    clock.seconds(0);
    assert_eq!(manager.poll(), Err(Error::ClockRegressed));
    assert!(manager.is_draining());
    assert_eq!(
        manager.register(spec(TaskKind::GatewayInternal)),
        Err(Error::ClockRegressed)
    );
    assert_eq!(
        guarded(manager.wait(running)).await.unwrap().state,
        TaskState::Cancelled
    );
    clock.seconds(2);
    assert_eq!(manager.poll(), Err(Error::ClockRegressed));
    assert_eq!(manager.metrics().pending_joins, 0);
}

#[tokio::test]
async fn panic_and_application_failure_are_collected_without_error_payloads() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let application = manager
        .spawn(spec(TaskKind::RequestScoped), |_| async {
            Err(TaskFailure::Application)
        })
        .unwrap();
    let panic = manager
        .spawn(spec(TaskKind::RequestScoped), |_| -> TaskFuture {
            panic!("synthetic factory panic")
        })
        .unwrap();
    guarded(manager.wait_all()).await.unwrap();
    assert_eq!(
        manager.snapshot(application).unwrap().result,
        Some(Err(TaskFailure::Application))
    );
    assert_eq!(
        manager.snapshot(panic).unwrap().result,
        Some(Err(TaskFailure::Panicked))
    );
    assert_eq!(manager.metrics().failed, 2);
    assert_eq!(manager.metrics().pending_joins, 0);
}

#[tokio::test]
async fn ownership_parent_rules_and_leaf_forget_keep_descendant_links_valid() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    assert_eq!(
        manager.register(child(TaskKind::RequestScoped, TaskId(42))),
        Err(Error::InvalidParent)
    );
    let (finish, release) = oneshot::channel();
    let parent = manager
        .spawn(spec(TaskKind::RequestScoped), |_| async move {
            release.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    for invalid in [
        TaskSpec {
            kind: TaskKind::BackgroundCancellable,
            ..child(TaskKind::RequestScoped, parent)
        },
        TaskSpec {
            owner: TaskOwner {
                cog: None,
                ..child(TaskKind::RequestScoped, parent).owner
            },
            ..spec(TaskKind::RequestScoped)
        },
        TaskSpec {
            owner: TaskOwner {
                request_id: None,
                ..child(TaskKind::RequestScoped, parent).owner
            },
            ..spec(TaskKind::RequestScoped)
        },
    ] {
        assert_eq!(manager.register(invalid), Err(Error::InvalidParent));
    }
    let descendant = manager
        .register(child(TaskKind::RequestScoped, parent))
        .unwrap();
    assert_eq!(manager.children(parent).unwrap(), vec![descendant]);
    finish.send(()).unwrap();
    state(&mut manager, parent, TaskState::Completed).await;
    assert!(matches!(manager.forget(parent), Err(Error::InvalidState)));
    assert_eq!(
        manager.register(child(TaskKind::RequestScoped, parent)),
        Err(Error::InvalidParent)
    );
    manager.request_cancel(descendant).unwrap();
    manager.forget(descendant).unwrap();
    manager.forget(parent).unwrap();
}

#[tokio::test]
async fn parent_wait_tracks_children_and_cancelled_wait_future_keeps_every_join() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let (finish_parent, parent_release) = oneshot::channel();
    let parent = manager
        .spawn(spec(TaskKind::RequestScoped), |_| async move {
            parent_release.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    let (finish_child, child_release) = oneshot::channel();
    let descendant = manager
        .spawn(child(TaskKind::RequestScoped, parent), |_| async move {
            child_release.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    finish_parent.send(()).unwrap();
    state(&mut manager, parent, TaskState::Completed).await;
    {
        let mut waiting = std::pin::pin!(manager.wait(parent));
        poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(manager.metrics().pending_joins, 1);
    assert_eq!(
        manager.snapshot(descendant).unwrap().state,
        TaskState::Running
    );
    finish_child.send(()).unwrap();
    assert_eq!(
        guarded(manager.wait(parent)).await.unwrap().state,
        TaskState::Completed
    );
    assert_eq!(
        manager.snapshot(descendant).unwrap().state,
        TaskState::Completed
    );
    assert_eq!(manager.metrics().pending_joins, 0);
}

#[tokio::test]
async fn parent_failure_and_explicit_cancellation_propagate_to_all_descendants() {
    for failure in [true, false] {
        let clock = ManualClock::default();
        let mut manager = manager(&clock);
        let (finish, release) = oneshot::channel();
        let parent = manager
            .spawn(spec(TaskKind::RequestScoped), move |_| async move {
                release.await.unwrap();
                if failure {
                    Err(TaskFailure::Application)
                } else {
                    Ok(TaskCompletion::Cancelled)
                }
            })
            .unwrap();
        let descendant = manager
            .spawn(child(TaskKind::RequestScoped, parent), |ctx| async move {
                ctx.cancellation.cancelled().await;
                Ok(TaskCompletion::Cancelled)
            })
            .unwrap();
        let grandchild = manager
            .register(child(TaskKind::RequestScoped, descendant))
            .unwrap();
        finish.send(()).unwrap();
        guarded(manager.wait(parent)).await.unwrap();
        assert_eq!(
            manager.snapshot(descendant).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(
            manager.snapshot(grandchild).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(manager.metrics().pending_joins, 0);
    }
}

#[tokio::test]
async fn exponential_backoff_is_capped_and_lifetime_restart_budget_never_resets() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let attempts = Arc::new(AtomicUsize::new(0));
    let id = supervisor
        .spawn(
            spec(TaskKind::BackgroundCancellable),
            policy(),
            failure_factory(&attempts),
        )
        .unwrap();
    for (time, retry) in [(0, 1), (1, 3), (3, 6), (6, 9)] {
        clock.seconds(time);
        supervised_state(&mut supervisor, id, TaskState::Restarting).await;
        assert_eq!(
            supervisor.restart_status(id).unwrap().retry_at,
            Some(Duration::from_secs(retry))
        );
        clock.set(Duration::from_secs(retry) - Duration::from_nanos(1));
        supervisor.poll().unwrap();
        assert_eq!(
            supervisor.manager().snapshot(id).unwrap().state,
            TaskState::Restarting
        );
        clock.seconds(retry);
        supervisor.poll().unwrap();
    }
    supervised_state(&mut supervisor, id, TaskState::Failed).await;
    let status = supervisor.restart_status(id).unwrap();
    assert!(status.exhausted);
    assert_eq!(status.restarts, 4);
    assert_eq!(attempts.load(Ordering::SeqCst), 5);
    clock.seconds(10_000);
    supervisor.poll().unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn restart_rate_window_expires_at_exact_boundary() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut policy = policy();
    policy.max_restarts_in_window = 1;
    policy.window = Duration::from_secs(10);
    let id = supervisor
        .spawn(
            spec(TaskKind::BackgroundCancellable),
            policy,
            failure_factory(&attempts),
        )
        .unwrap();
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    clock.seconds(1);
    supervisor.poll().unwrap();
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    clock.seconds(3);
    supervisor.poll().unwrap();
    assert_eq!(
        supervisor.restart_status(id).unwrap().retry_at,
        Some(Duration::from_secs(11))
    );
    clock.seconds(10);
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 1);
    clock.seconds(11);
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 2);
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    supervisor.manager_mut().request_cancel(id).unwrap();
}

#[tokio::test]
async fn circuit_cooldown_half_open_probe_and_success_close_the_breaker() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let mut policy = policy();
    policy.circuit_breaker_after = 1;
    let (finish, release) = oneshot::channel();
    let release = Arc::new(std::sync::Mutex::new(Some(release)));
    let factory: TaskFactory = Arc::new(move |ctx| {
        let release = release.clone();
        Box::pin(async move {
            if ctx.attempt == 1 {
                return Err(TaskFailure::Application);
            }
            let wait = release.lock().unwrap().take().unwrap();
            wait.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
    });
    let id = supervisor
        .spawn(spec(TaskKind::BackgroundCancellable), policy, factory)
        .unwrap();
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    assert_eq!(
        supervisor.restart_status(id).unwrap().circuit,
        CircuitState::Open {
            until: Duration::from_secs(10)
        }
    );
    clock.seconds(9);
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 0);
    clock.seconds(10);
    supervisor.poll().unwrap();
    assert_eq!(
        supervisor.restart_status(id).unwrap().circuit,
        CircuitState::HalfOpen
    );
    finish.send(()).unwrap();
    supervised_state(&mut supervisor, id, TaskState::Completed).await;
    assert_eq!(
        supervisor.restart_status(id).unwrap().circuit,
        CircuitState::Closed
    );
    assert_eq!(supervisor.restart_status(id).unwrap().failures, 0);
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 1);
}

#[tokio::test]
async fn requests_and_durable_jobs_reject_replay_policy_and_execute_once() {
    for kind in [TaskKind::RequestScoped, TaskKind::DurableJob] {
        let clock = ManualClock::default();
        let mut supervisor = Supervisor::new(manager(&clock));
        let attempts = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            supervisor.spawn(spec(kind), policy(), failure_factory(&attempts)),
            Err(Error::InvalidConfig)
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        let mut once = policy();
        once.max_restarts = 0;
        let id = supervisor
            .spawn(spec(kind), once, failure_factory(&attempts))
            .unwrap();
        supervised_state(&mut supervisor, id, TaskState::Failed).await;
        clock.seconds(1000);
        supervisor.poll().unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(supervisor.restart_status(id).unwrap().exhausted);
    }
}

#[tokio::test]
async fn invalid_restart_policy_and_restart_time_overflow_are_reported() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut invalid = Vec::new();
    let mut p = policy();
    p.window = Duration::ZERO;
    invalid.push(p);
    let mut p = policy();
    p.max_restarts_in_window = 0;
    invalid.push(p);
    let mut p = policy();
    p.initial_backoff = Duration::ZERO;
    invalid.push(p);
    let mut p = policy();
    p.max_backoff = Duration::ZERO;
    invalid.push(p);
    let mut p = policy();
    p.circuit_breaker_after = 0;
    invalid.push(p);
    let mut p = policy();
    p.circuit_open_for = Duration::ZERO;
    invalid.push(p);
    for p in invalid {
        assert_eq!(
            supervisor.spawn(
                spec(TaskKind::BackgroundCancellable),
                p,
                failure_factory(&counter)
            ),
            Err(Error::InvalidConfig)
        );
    }
    let id = supervisor
        .spawn(
            spec(TaskKind::BackgroundCancellable),
            policy(),
            failure_factory(&counter),
        )
        .unwrap();
    state(supervisor.manager_mut(), id, TaskState::Failed).await;
    clock.set(Duration::MAX);
    assert_eq!(supervisor.poll(), Err(Error::TimeOverflow));
    assert_eq!(supervisor.poll(), Err(Error::TimeOverflow));
    assert_eq!(
        supervisor.manager().snapshot(id).unwrap().state,
        TaskState::Failed
    );
}

#[tokio::test]
async fn explicit_cancel_and_core_drain_suppress_pending_restart_but_not_gateway_restart() {
    for cancel in [true, false] {
        let clock = ManualClock::default();
        let mut supervisor = Supervisor::new(manager(&clock));
        let attempts = Arc::new(AtomicUsize::new(0));
        let id = supervisor
            .spawn(
                spec(TaskKind::BackgroundCancellable),
                policy(),
                failure_factory(&attempts),
            )
            .unwrap();
        supervised_state(&mut supervisor, id, TaskState::Restarting).await;
        if cancel {
            supervisor.manager_mut().request_cancel(id).unwrap();
        } else {
            supervisor.begin_core_drain();
        }
        clock.seconds(1000);
        supervisor.poll().unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            supervisor.manager().snapshot(id).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(supervisor.restart_status(id).unwrap().retry_at, None);
    }
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let attempts = Arc::new(AtomicUsize::new(0));
    let id = supervisor
        .spawn(
            spec(TaskKind::GatewayInternal),
            policy(),
            failure_factory(&attempts),
        )
        .unwrap();
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    supervisor.begin_core_drain();
    clock.seconds(1);
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 1);
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    supervisor.manager_mut().request_cancel(id).unwrap();
}

#[tokio::test]
async fn failed_parent_restarts_only_after_its_cancelled_descendant_has_joined() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let attempts = Arc::new(AtomicUsize::new(0));
    let (fail, release) = oneshot::channel();
    let release = Arc::new(std::sync::Mutex::new(Some(release)));
    let factory: TaskFactory = {
        let attempts = attempts.clone();
        Arc::new(move |ctx| {
            attempts.fetch_add(1, Ordering::SeqCst);
            let release = release.clone();
            Box::pin(async move {
                if ctx.attempt == 1 {
                    let release = release.lock().unwrap().take().unwrap();
                    release.await.unwrap();
                }
                Err(TaskFailure::Application)
            })
        })
    };
    let parent = supervisor
        .spawn(spec(TaskKind::BackgroundCancellable), policy(), factory)
        .unwrap();
    let (finish, release) = oneshot::channel();
    let descendant = supervisor
        .manager_mut()
        .spawn(
            child(TaskKind::BackgroundCancellable, parent),
            |_| async move {
                release.await.unwrap();
                Ok(TaskCompletion::Cancelled)
            },
        )
        .unwrap();
    fail.send(()).unwrap();
    supervised_state(&mut supervisor, parent, TaskState::Restarting).await;
    clock.seconds(1);
    supervisor.poll().unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        supervisor.manager().snapshot(descendant).unwrap().state,
        TaskState::CancelRequested
    );
    finish.send(()).unwrap();
    guarded(async {
        while supervisor.restart_status(parent).unwrap().restarts == 0 {
            supervisor.poll().unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await;
    supervised_state(&mut supervisor, parent, TaskState::Restarting).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    supervisor.manager_mut().request_cancel(parent).unwrap();
}

#[tokio::test]
async fn core_drain_is_staged_allows_admitted_request_children_and_preserves_gateway() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    assert_eq!(supervisor.drain_status(evidence()), DrainStatus::Accepting);
    let (finish, release) = oneshot::channel();
    let request = supervisor
        .manager_mut()
        .spawn(spec(TaskKind::RequestScoped), |_| async move {
            release.await.unwrap();
            Ok(TaskCompletion::Completed)
        })
        .unwrap();
    let background = supervisor
        .manager_mut()
        .spawn(spec(TaskKind::BackgroundCancellable), |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    let durable = supervisor
        .manager_mut()
        .spawn(spec(TaskKind::DurableJob), |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Checkpointed)
        })
        .unwrap();
    let gateway = supervisor
        .manager_mut()
        .spawn(spec(TaskKind::GatewayInternal), |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    supervisor.begin_core_drain();
    supervisor.begin_core_drain();
    supervisor.poll().unwrap();
    assert_eq!(
        supervisor.drain_status(evidence()),
        DrainStatus::WaitingRequests
    );
    assert_eq!(
        supervisor.manager().snapshot(background).unwrap().state,
        TaskState::Running
    );
    for kind in [
        TaskKind::RequestScoped,
        TaskKind::BackgroundCancellable,
        TaskKind::DurableJob,
    ] {
        assert_eq!(
            supervisor.manager_mut().register(spec(kind)),
            Err(Error::Draining)
        );
    }
    let descendant = supervisor
        .manager_mut()
        .register(child(TaskKind::RequestScoped, request))
        .unwrap();
    finish.send(()).unwrap();
    state(supervisor.manager_mut(), request, TaskState::Completed).await;
    assert_eq!(
        supervisor.drain_status(evidence()),
        DrainStatus::WaitingRequests
    );
    supervisor
        .manager_mut()
        .start(descendant, |_| async { Ok(TaskCompletion::Completed) })
        .unwrap();
    guarded(async {
        loop {
            supervisor.poll().unwrap();
            if supervisor.manager().core_tasks_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        supervisor.manager().snapshot(background).unwrap().state,
        TaskState::Cancelled
    );
    assert_eq!(
        supervisor.manager().snapshot(durable).unwrap().result,
        Some(Ok(TaskCompletion::Checkpointed))
    );
    assert_eq!(
        supervisor.manager().snapshot(gateway).unwrap().state,
        TaskState::Running
    );
    assert_eq!(supervisor.drain_status(evidence()), DrainStatus::Complete);
    supervisor.manager_mut().request_cancel(gateway).unwrap();
    guarded(supervisor.manager_mut().wait_all()).await.unwrap();
}

#[tokio::test]
async fn each_external_requirement_and_unsafe_durable_result_blocks_drain() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let durable = supervisor
        .manager_mut()
        .spawn(spec(TaskKind::DurableJob), |_| async {
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    guarded(supervisor.manager_mut().wait(durable))
        .await
        .unwrap();
    supervisor.begin_core_drain();
    supervisor.poll().unwrap();
    assert_eq!(
        supervisor.drain_status(evidence()),
        DrainStatus::WaitingExternal
    );
    let all = DrainEvidence {
        durable_jobs_safe: true,
        ..evidence()
    };
    for missing in [
        DrainEvidence {
            api_results_settled: false,
            ..all
        },
        DrainEvidence {
            state_saved: false,
            ..all
        },
        DrainEvidence {
            ipc_flushed: false,
            ..all
        },
    ] {
        assert_eq!(
            supervisor.drain_status(missing),
            DrainStatus::WaitingExternal
        );
    }
    assert_eq!(supervisor.drain_status(all), DrainStatus::Complete);
    assert_eq!(
        supervisor.drain_status(DrainEvidence::default()),
        DrainStatus::WaitingExternal
    );
}

#[tokio::test]
async fn durable_safety_cannot_be_forgotten_or_confirmed_before_the_job_stops() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let registered = manager.register(spec(TaskKind::DurableJob)).unwrap();
    assert!(manager.snapshot(registered).unwrap().durable_safe);
    manager.request_cancel(registered).unwrap();
    assert!(manager.forget(registered).unwrap().durable_safe);
    let (finish, release) = oneshot::channel();
    let running = manager
        .spawn(spec(TaskKind::DurableJob), |_| async move {
            release.await.unwrap();
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    assert!(!manager.snapshot(running).unwrap().durable_safe);
    assert_eq!(
        manager.confirm_durable_safe(running),
        Err(Error::InvalidState)
    );
    finish.send(()).unwrap();
    guarded(manager.wait(running)).await.unwrap();
    assert!(!manager.snapshot(running).unwrap().durable_safe);
    assert!(matches!(manager.forget(running), Err(Error::InvalidState)));
    manager.confirm_durable_safe(running).unwrap();
    assert!(manager.forget(running).unwrap().durable_safe);
    let ordinary = manager.register(spec(TaskKind::RequestScoped)).unwrap();
    manager.request_cancel(ordinary).unwrap();
    assert_eq!(
        manager.confirm_durable_safe(ordinary),
        Err(Error::InvalidState)
    );
    assert_eq!(
        manager.confirm_durable_safe(TaskId(999)),
        Err(Error::UnknownTask)
    );
}

#[tokio::test]
async fn supervisor_wait_and_wait_all_drive_retries_with_injected_time() {
    for wait_all in [true, false] {
        let clock = ManualClock::default();
        let mut supervisor = Supervisor::new(manager(&clock));
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: TaskFactory = {
            let calls = calls.clone();
            Arc::new(move |ctx| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if ctx.attempt == 1 {
                        Err(TaskFailure::Application)
                    } else {
                        Ok(TaskCompletion::Completed)
                    }
                })
            })
        };
        let id = supervisor
            .spawn(spec(TaskKind::BackgroundCancellable), policy(), factory)
            .unwrap();
        supervised_state(&mut supervisor, id, TaskState::Restarting).await;
        {
            let mut waiting = std::pin::pin!(supervisor.wait(id));
            poll_fn(|cx| {
                assert!(waiting.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(supervisor.restart_status(id).unwrap().restarts, 0);
        assert_eq!(
            supervisor
                .manager_mut()
                .start(id, |_| async { Ok(TaskCompletion::Completed) }),
            Err(Error::InvalidState)
        );
        let advance = async {
            tokio::task::yield_now().await;
            clock.seconds(1);
        };
        guarded(async {
            if wait_all {
                let (result, ()) = tokio::join!(supervisor.wait_all(), advance);
                result.unwrap();
            } else {
                let (result, ()) = tokio::join!(supervisor.wait(id), advance);
                assert_eq!(result.unwrap().state, TaskState::Completed);
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(supervisor.manager().metrics().pending_joins, 0);
    }
}

#[tokio::test]
async fn backoff_stays_capped_after_more_than_thirty_two_restarts() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let attempts = Arc::new(AtomicUsize::new(0));
    let policy = RestartPolicy {
        max_restarts: 35,
        max_restarts_in_window: 40,
        max_backoff: Duration::from_secs(1 << 34),
        window: Duration::MAX,
        circuit_breaker_after: 40,
        ..policy()
    };
    let id = supervisor
        .spawn(
            spec(TaskKind::BackgroundCancellable),
            policy,
            failure_factory(&attempts),
        )
        .unwrap();
    let mut now = 0;
    for restart in 0..35 {
        supervised_state(&mut supervisor, id, TaskState::Restarting).await;
        let expected = now + (1u64 << restart).min(1 << 34);
        assert_eq!(
            supervisor.restart_status(id).unwrap().retry_at,
            Some(Duration::from_secs(expected))
        );
        now = expected;
        clock.seconds(now);
        supervisor.poll().unwrap();
    }
    supervised_state(&mut supervisor, id, TaskState::Failed).await;
    assert_eq!(supervisor.restart_status(id).unwrap().restarts, 35);
    assert_eq!(attempts.load(Ordering::SeqCst), 36);
}

#[tokio::test]
async fn timeout_failure_never_replays_background_task() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let calls = Arc::new(AtomicUsize::new(0));
    let factory: TaskFactory = {
        let calls = calls.clone();
        Arc::new(move |ctx| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                ctx.cancellation.cancelled().await;
                Err(TaskFailure::Application)
            })
        })
    };
    let mut timed = spec(TaskKind::BackgroundCancellable);
    timed.timeout = Some(Duration::from_secs(1));
    let id = supervisor.spawn(timed, policy(), factory).unwrap();
    clock.seconds(1);
    supervised_state(&mut supervisor, id, TaskState::Failed).await;
    clock.seconds(1000);
    supervisor.poll().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(supervisor.manager().snapshot(id).unwrap().timed_out);
    assert_eq!(supervisor.restart_status(id).unwrap().retry_at, None);
}

#[tokio::test]
async fn durable_child_requires_recovery_confirmation_before_parent_restarts() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let (fail, release) = oneshot::channel();
    let release = Arc::new(std::sync::Mutex::new(Some(release)));
    let factory: TaskFactory = Arc::new(move |ctx| {
        let release = release.clone();
        Box::pin(async move {
            if ctx.attempt == 1 {
                let release = release.lock().unwrap().take().unwrap();
                release.await.unwrap();
            }
            Err(TaskFailure::Application)
        })
    });
    let parent = supervisor
        .spawn(spec(TaskKind::BackgroundCancellable), policy(), factory)
        .unwrap();
    let durable = supervisor
        .manager_mut()
        .spawn(child(TaskKind::DurableJob, parent), |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    fail.send(()).unwrap();
    supervised_state(&mut supervisor, parent, TaskState::Restarting).await;
    guarded(supervisor.manager_mut().wait(durable))
        .await
        .unwrap();
    assert!(!supervisor.manager().snapshot(durable).unwrap().durable_safe);
    clock.seconds(1);
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(parent).unwrap().restarts, 0);
    supervisor
        .manager_mut()
        .confirm_durable_safe(durable)
        .unwrap();
    supervisor.poll().unwrap();
    assert_eq!(supervisor.restart_status(parent).unwrap().restarts, 1);
    supervised_state(&mut supervisor, parent, TaskState::Restarting).await;
    supervisor.manager_mut().request_cancel(parent).unwrap();
}

#[tokio::test]
async fn explicit_parent_cancel_reaches_grandchildren_without_cancelling_unrelated_work() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    let cancel_task = |ctx: TaskContext| async move {
        ctx.cancellation.cancelled().await;
        Ok(TaskCompletion::Cancelled)
    };
    let parent = manager
        .spawn(spec(TaskKind::RequestScoped), cancel_task)
        .unwrap();
    let descendant = manager
        .spawn(child(TaskKind::RequestScoped, parent), cancel_task)
        .unwrap();
    let grandchild = manager
        .spawn(child(TaskKind::RequestScoped, descendant), cancel_task)
        .unwrap();
    let unrelated = manager
        .spawn(spec(TaskKind::RequestScoped), cancel_task)
        .unwrap();
    manager.request_cancel(parent).unwrap();
    assert_eq!(
        manager.register(child(TaskKind::RequestScoped, parent)),
        Err(Error::InvalidParent)
    );
    guarded(manager.wait(parent)).await.unwrap();
    for id in [parent, descendant, grandchild] {
        assert_eq!(manager.snapshot(id).unwrap().state, TaskState::Cancelled);
    }
    assert_eq!(
        manager.snapshot(unrelated).unwrap().state,
        TaskState::Running
    );
    manager.request_cancel(unrelated).unwrap();
    guarded(manager.wait_all()).await.unwrap();
}

#[tokio::test]
async fn half_open_failure_reopens_circuit_without_resetting_restart_budget() {
    let clock = ManualClock::default();
    let mut supervisor = Supervisor::new(manager(&clock));
    let calls = Arc::new(AtomicUsize::new(0));
    let policy = RestartPolicy {
        circuit_breaker_after: 1,
        ..policy()
    };
    let id = supervisor
        .spawn(
            spec(TaskKind::BackgroundCancellable),
            policy,
            failure_factory(&calls),
        )
        .unwrap();
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    clock.seconds(10);
    supervisor.poll().unwrap();
    assert_eq!(
        supervisor.restart_status(id).unwrap().circuit,
        CircuitState::HalfOpen
    );
    supervised_state(&mut supervisor, id, TaskState::Restarting).await;
    let status = supervisor.restart_status(id).unwrap();
    assert_eq!(
        status.circuit,
        CircuitState::Open {
            until: Duration::from_secs(20)
        }
    );
    assert_eq!(status.restarts, 1);
    assert_eq!(status.failures, 2);
    assert_eq!(status.retry_at, Some(Duration::from_secs(20)));
    supervisor.manager_mut().request_cancel(id).unwrap();
}

#[tokio::test]
async fn cancel_all_includes_gateway_and_snapshots_are_independent_copies() {
    let clock = ManualClock::default();
    let mut manager = manager(&clock);
    for kind in [
        TaskKind::RequestScoped,
        TaskKind::BackgroundCancellable,
        TaskKind::GatewayInternal,
    ] {
        manager
            .spawn(spec(kind), |ctx| async move {
                ctx.cancellation.cancelled().await;
                Ok(TaskCompletion::Cancelled)
            })
            .unwrap();
    }
    let before = manager.snapshots();
    assert_eq!(before.len(), 3);
    assert!(before.iter().all(|s| s.state == TaskState::Running));
    manager.request_cancel_all();
    manager.request_cancel_all();
    guarded(manager.wait_all()).await.unwrap();
    assert!(
        manager
            .snapshots()
            .iter()
            .all(|s| s.state == TaskState::Cancelled)
    );
    assert!(before.iter().all(|s| s.state == TaskState::Running));
    assert_eq!(manager.metrics().pending_joins, 0);
}

#[tokio::test]
async fn regressing_clock_wakes_a_pending_wait_and_cancels_live_tasks() {
    let clock = ManualClock::default();
    clock.seconds(10);
    let mut manager = manager(&clock);
    let mut timed = spec(TaskKind::RequestScoped);
    timed.timeout = Some(Duration::from_secs(5));
    let id = manager
        .spawn(timed, |ctx| async move {
            ctx.cancellation.cancelled().await;
            Ok(TaskCompletion::Cancelled)
        })
        .unwrap();
    let regress = async {
        tokio::task::yield_now().await;
        clock.seconds(9);
    };
    let (result, ()) = guarded(async { tokio::join!(manager.wait(id), regress) }).await;
    assert_eq!(result.unwrap().state, TaskState::Cancelled);
    assert!(manager.is_draining());
    assert_eq!(manager.metrics().pending_joins, 0);
}
