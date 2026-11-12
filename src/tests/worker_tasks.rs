use src::{
    protocol::{BotId, RequestId},
    supervisor::{Error, TaskKind, TaskState, WorkerTasks},
};
use std::time::Duration;
use tokio::sync::oneshot;

fn tasks() -> WorkerTasks {
    WorkerTasks::new(BotId(19), 3).unwrap()
}

#[tokio::test]
async fn handler_is_a_tracked_request_child_with_safe_owner_metadata() {
    let mut tasks = tasks();
    let (release, wait) = oneshot::channel();
    let request = tasks
        .start_request(async move { wait.await.unwrap() })
        .unwrap();
    let snapshots = tasks.snapshots();
    assert_eq!(snapshots.len(), 2);
    let root = snapshots
        .iter()
        .find(|s| s.id == request.root_id())
        .unwrap();
    let child = snapshots
        .iter()
        .find(|s| s.id == request.child_id())
        .unwrap();
    assert_eq!(root.spec.kind, TaskKind::RequestScoped);
    assert_eq!(child.spec.kind, TaskKind::RequestScoped);
    assert_eq!(root.spec.owner.bot_id, BotId(19));
    assert_eq!(root.spec.owner.shard_id, 3);
    assert_eq!(root.spec.owner.cog, None);
    assert_eq!(root.spec.owner.parent, None);
    assert_eq!(child.spec.owner.parent, Some(root.id));
    assert_eq!(root.spec.owner.request_id, child.spec.owner.request_id);
    assert_eq!(
        root.spec.owner.request_id,
        Some(RequestId(1u128.to_be_bytes()))
    );
    assert_eq!(tasks.metrics().pending_joins, 2);
    release.send(42).unwrap();
    assert_eq!(tasks.finish_request(request).await.unwrap(), 42);
    assert!(tasks.snapshots().is_empty());
    assert_eq!(tasks.metrics().pending_joins, 0);
}

#[tokio::test]
async fn drain_closes_root_admission_then_waits_for_the_handler_child() {
    let mut tasks = tasks();
    let (release, wait) = oneshot::channel();
    let request = tasks
        .start_request(async move { wait.await.unwrap() })
        .unwrap();
    tasks.begin_drain();
    assert!(tasks.is_draining());
    assert!(matches!(
        tasks.start_request(async {}),
        Err(Error::Draining)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), tasks.wait_drained())
            .await
            .is_err()
    );
    assert_eq!(tasks.resume(), Err(Error::InvalidState));
    assert!(
        tasks
            .snapshots()
            .iter()
            .all(|s| s.state == TaskState::Running)
    );
    release.send(7).unwrap();
    tokio::time::timeout(Duration::from_secs(2), tasks.wait_drained())
        .await
        .unwrap()
        .unwrap();
    // A live handle cannot be confused with fresh task identifiers after resume.
    assert_eq!(tasks.resume(), Err(Error::InvalidState));
    assert_eq!(tasks.finish_request(request).await.unwrap(), 7);
    tasks.resume().unwrap();
    assert!(!tasks.is_draining());
    let request = tasks.start_request(async { 8 }).unwrap();
    assert_eq!(
        tasks.snapshots()[0].spec.owner.request_id,
        Some(RequestId(2u128.to_be_bytes()))
    );
    assert_eq!(tasks.finish_request(request).await.unwrap(), 8);
}

#[tokio::test]
async fn cancelling_the_callers_wait_keeps_managed_work_for_drain() {
    let mut tasks = tasks();
    let (release, wait) = oneshot::channel::<()>();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            tasks.request(async move {
                wait.await.unwrap();
            })
        )
        .await
        .is_err()
    );
    assert_eq!(tasks.metrics().pending_joins, 2);
    tasks.begin_drain();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), tasks.wait_drained())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), tasks.wait_drained())
        .await
        .unwrap()
        .unwrap();
    tasks.resume().unwrap();
    assert!(tasks.snapshots().is_empty());
    assert_eq!(tasks.request(async { 9 }).await.unwrap(), 9);
}

#[tokio::test]
async fn handler_panic_terminates_the_root_and_returns_a_fixed_failure() {
    let mut tasks = tasks();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        tasks.request(async {
            panic!("fixture panic");
        }),
    )
    .await
    .unwrap();
    assert_eq!(result, Err(Error::TaskFailed));
    assert!(tasks.snapshots().is_empty());
    assert_eq!(tasks.metrics().pending_joins, 0);
    assert!(tasks.request(async { true }).await.unwrap());
}

#[tokio::test]
async fn terminal_records_do_not_exhaust_capacity_across_serial_requests() {
    let mut tasks = tasks();
    for value in 0..260 {
        assert_eq!(tasks.request(async move { value }).await.unwrap(), value);
        assert!(tasks.snapshots().is_empty());
    }
    assert_eq!(tasks.metrics().pending_joins, 0);
}

#[tokio::test]
async fn abandoned_completed_handles_are_reaped_before_new_admission() {
    let mut tasks = tasks();
    for _ in 0..260 {
        let (started, start) = oneshot::channel();
        drop(
            tasks
                .start_request(async move {
                    let _ = started.send(());
                })
                .unwrap(),
        );
        start.await.unwrap();
        // Joining happens on the next admission; yield also lets the root
        // receive the child completion signal.
        tokio::task::yield_now().await;
    }
    tasks.begin_drain();
    tasks.wait_drained().await.unwrap();
    tasks.resume().unwrap();
    assert!(tasks.snapshots().is_empty());
}

#[tokio::test]
async fn handles_cannot_be_finished_by_another_worker_task_manager() {
    let mut first = tasks();
    let mut second = tasks();
    let request = first.start_request(async { 1 }).unwrap();
    let other = second.start_request(async { 2 }).unwrap();
    assert_eq!(second.finish_request(request).await, Err(Error::WrongOwner));
    assert_eq!(second.finish_request(other).await.unwrap(), 2);
    first.begin_drain();
    first.wait_drained().await.unwrap();
    first.resume().unwrap();
}

#[tokio::test]
async fn wait_and_resume_require_an_explicit_drain() {
    let mut tasks = tasks();
    assert_eq!(tasks.wait_drained().await, Err(Error::InvalidState));
    assert_eq!(tasks.resume(), Err(Error::InvalidState));
    tasks.begin_drain();
    tasks.wait_drained().await.unwrap();
    tasks.resume().unwrap();
}
