use src::{durable_jobs::*, protocol::BotId, state_store::*};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[derive(Clone)]
struct Clock(Arc<AtomicU64>);
impl StateClock for Clock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(self.0.load(Ordering::SeqCst)))
    }
}
impl Clock {
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
struct Fixture {
    path: PathBuf,
    clock: Clock,
}
impl Fixture {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "durable-jobs-{}-{}",
                std::process::id(),
                getrandom::u64().unwrap()
            )),
            clock: Clock(Arc::new(AtomicU64::new(100))),
        }
    }
    async fn store(&self) -> Arc<FileStateStore> {
        Arc::new(
            FileStateStore::open(
                &self.path,
                store_limits(),
                8 * 1024 * 1024,
                Arc::new(self.clock.clone()),
            )
            .await
            .unwrap(),
        )
    }
    async fn queue(&self) -> DurableJobs {
        open(
            self.store().await,
            &self.clock,
            scope(1, 0),
            "jobs",
            id(1),
            JobLimits::default(),
        )
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
fn store_limits() -> StoreLimits {
    StoreLimits {
        max_entries: 32,
        max_bytes: 1024 * 1024,
        max_value_bytes: 256 * 1024,
    }
}
fn scope(bot: u64, shard: u32) -> StoreScope {
    StoreScope {
        bot_id: BotId(bot),
        shard_id: shard,
    }
}
fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).unwrap()
}
fn id(value: u8) -> StateId {
    StateId::from_bytes([value; 16])
}
fn owner(generation: u64) -> JobOwner {
    JobOwner {
        generation,
        ownership_epoch: 1,
    }
}
fn spec(value: u8, due: u64) -> JobSpec {
    JobSpec {
        id: id(value),
        handler: PublicLabel::new("resume_work").unwrap(),
        schema: schema(),
        scheduled_at: Timestamp(due),
        expires_at: None,
        checkpoint: b"initial".to_vec(),
        retry: RetryPolicy {
            max_retries: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 25,
        },
        recovery: RecoveryPolicy::RetrySafe,
    }
}
async fn open(
    store: Arc<dyn StateStore>,
    clock: &Clock,
    scope: StoreScope,
    namespace: &str,
    queue_id: StateId,
    limits: JobLimits,
) -> Result<DurableJobs, JobError> {
    DurableJobs::open(
        store,
        Arc::new(clock.clone()),
        scope,
        PublicLabel::new(namespace).unwrap(),
        queue_id,
        limits,
    )
    .await
}

#[tokio::test]
async fn scheduling_order_due_boundary_and_completed_tombstone_survive_restart() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 110)).await.unwrap();
    queue.schedule(owner(1), spec(2, 105)).await.unwrap();
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
    fixture.clock.set(105);
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    assert_eq!(claim.job().spec.id, id(2));
    queue.finish(&claim, JobOutcome::Completed).await.unwrap();
    drop(queue);
    let queue = fixture.queue().await;
    assert_eq!(queue.list().await.unwrap()[1].status, JobStatus::Completed);
    assert_eq!(
        queue.schedule(owner(1), spec(2, 105)).await,
        Err(JobError::DuplicateJob)
    );
    fixture.clock.set(110);
    assert_eq!(
        queue
            .claim(owner(1), schema(), 50)
            .await
            .unwrap()
            .unwrap()
            .job()
            .spec
            .id,
        id(1)
    );
}

#[tokio::test]
async fn checkpoint_and_expired_retry_safe_claim_resume_after_restart_and_backoff() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let first = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    queue
        .checkpoint(&first, b"step-two".to_vec())
        .await
        .unwrap();
    drop(queue);
    let queue = fixture.queue().await;
    queue.activate(owner(2)).await.unwrap();
    fixture.clock.set(149);
    assert!(queue.claim(owner(2), schema(), 50).await.unwrap().is_none());
    fixture.clock.set(150);
    assert_eq!(queue.recover(owner(2)).await.unwrap(), 1);
    assert_eq!(
        queue.validate_claim(&first).await,
        Err(JobError::StaleClaim)
    );
    assert!(queue.claim(owner(2), schema(), 50).await.unwrap().is_none());
    fixture.clock.set(160);
    let resumed = queue.claim(owner(2), schema(), 50).await.unwrap().unwrap();
    assert_eq!(resumed.job().spec.checkpoint, b"step-two");
    assert_eq!(resumed.job().attempts, 2);
    assert_eq!(resumed.job().failures, 1);
    queue.finish(&resumed, JobOutcome::Completed).await.unwrap();
}

#[tokio::test]
async fn known_failures_use_capped_exponential_backoff_and_finite_budget() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    for (now, expected_due) in [
        (100, Some(110)),
        (110, Some(130)),
        (130, Some(155)),
        (155, None),
    ] {
        fixture.clock.set(now);
        let claim = queue.claim(owner(1), schema(), 100).await.unwrap().unwrap();
        queue
            .finish(&claim, JobOutcome::RetryableFailure)
            .await
            .unwrap();
        let expected = expected_due.map_or(JobStatus::Failed, |due| JobStatus::Scheduled {
            due_at: Timestamp(due),
        });
        assert_eq!(queue.list().await.unwrap()[0].status, expected);
        assert!(
            queue
                .claim(owner(1), schema(), 100)
                .await
                .unwrap()
                .is_none()
        );
    }
    fixture.clock.set(10_000);
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
    let view = &queue.list().await.unwrap()[0];
    assert_eq!((view.attempts, view.failures), (4, 4));
}

#[tokio::test]
async fn generation_and_epoch_fence_admission_but_preserve_existing_completion() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let old = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    queue.activate(owner(2)).await.unwrap();
    assert_eq!(
        queue.claim(owner(1), schema(), 50).await,
        Err(JobError::Fenced)
    );
    assert_eq!(
        queue.schedule(owner(1), spec(2, 100)).await,
        Err(JobError::Fenced)
    );
    assert_eq!(queue.renew(&old, 60).await, Err(JobError::Fenced));
    assert!(queue.claim(owner(2), schema(), 50).await.unwrap().is_none());
    queue.finish(&old, JobOutcome::Completed).await.unwrap();
    queue
        .activate(JobOwner {
            generation: 1,
            ownership_epoch: 2,
        })
        .await
        .unwrap();
    assert_eq!(queue.activate(owner(100)).await, Err(JobError::Fenced));
    assert_eq!(
        queue
            .activate(JobOwner {
                generation: 0,
                ownership_epoch: 3
            })
            .await,
        Err(JobError::InvalidConfiguration)
    );
}

#[tokio::test]
async fn drain_persists_admission_closure_and_only_explicit_stopped_release_resumes() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    assert!(!queue.drain_status().await.unwrap().is_complete());
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let old = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    let status = queue.begin_drain(owner(1)).await.unwrap();
    assert_eq!(status.running, 1);
    assert!(!status.is_complete());
    assert_eq!(
        queue.claim(owner(1), schema(), 50).await,
        Err(JobError::Draining)
    );
    assert_eq!(
        queue.schedule(owner(1), spec(2, 100)).await,
        Err(JobError::Draining)
    );
    drop(queue);
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    assert_eq!(
        queue.claim(owner(1), schema(), 50).await,
        Err(JobError::Draining)
    );
    queue
        .release_after_stop(&old, b"joined-checkpoint".to_vec())
        .await
        .unwrap();
    assert!(queue.drain_status().await.unwrap().is_complete());
    queue.activate(owner(2)).await.unwrap();
    let next = queue.claim(owner(2), schema(), 50).await.unwrap().unwrap();
    assert_eq!(next.job().spec.checkpoint, b"joined-checkpoint");
    assert_eq!(next.job().failures, 0);
    assert_eq!(
        queue.finish(&old, JobOutcome::Completed).await,
        Err(JobError::StaleClaim)
    );
    queue.finish(&next, JobOutcome::Completed).await.unwrap();
}

#[tokio::test]
async fn external_intent_blocks_checkpoint_release_and_automatic_replay() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    queue.begin_external_operation(&claim, id(9)).await.unwrap();
    assert_eq!(
        queue.checkpoint(&claim, b"advance".to_vec()).await,
        Err(JobError::ExternalOperationPending)
    );
    assert_eq!(
        queue.release_after_stop(&claim, vec![]).await,
        Err(JobError::ExternalOperationPending)
    );
    assert_eq!(
        queue.finish(&claim, JobOutcome::Completed).await,
        Err(JobError::ExternalOperationPending)
    );
    assert_eq!(
        queue.begin_external_operation(&claim, id(10)).await,
        Err(JobError::ExternalOperationPending)
    );
    fixture.clock.set(150);
    queue.activate(owner(2)).await.unwrap();
    queue.recover(owner(2)).await.unwrap();
    drop(queue);
    let queue = fixture.queue().await;
    let job = &queue.list().await.unwrap()[0];
    assert_eq!(job.status, JobStatus::Unknown);
    assert_eq!(
        job.external,
        Some(ExternalOperation {
            id: id(9),
            owner: owner(1),
            status: ExternalStatus::Unknown
        })
    );
    fixture.clock.set(100_000);
    assert!(queue.claim(owner(2), schema(), 50).await.unwrap().is_none());
    assert!(!queue.begin_drain(owner(2)).await.unwrap().is_complete());
}

#[tokio::test]
async fn explicit_external_outcomes_preserve_operation_identity_and_control_retry() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    assert_eq!(
        queue
            .complete_external_operation(&claim, ExternalOutcome::Succeeded)
            .await,
        Err(JobError::NoExternalOperation)
    );
    queue.begin_external_operation(&claim, id(8)).await.unwrap();
    queue
        .complete_external_operation(&claim, ExternalOutcome::ConfirmedNoEffect)
        .await
        .unwrap();
    fixture.clock.set(110);
    let next = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    assert_eq!(
        queue.begin_external_operation(&next, id(8)).await,
        Err(JobError::ExternalOperationPending)
    );
    queue.begin_external_operation(&next, id(9)).await.unwrap();
    queue
        .complete_external_operation(&next, ExternalOutcome::Succeeded)
        .await
        .unwrap();
    let job = &queue.list().await.unwrap()[0];
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.external.unwrap().status, ExternalStatus::Succeeded);
    assert_eq!(
        queue
            .complete_external_operation(&claim, ExternalOutcome::Unknown)
            .await,
        Err(JobError::StaleClaim)
    );
}

#[tokio::test]
async fn unsafe_crash_and_explicit_unknown_never_retry() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    let mut unsafe_job = spec(1, 100);
    unsafe_job.recovery = RecoveryPolicy::RequiresReconciliation;
    queue.schedule(owner(1), unsafe_job).await.unwrap();
    queue.schedule(owner(1), spec(2, 100)).await.unwrap();
    let first = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    let second = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    assert_ne!(first.job().spec.id, second.job().spec.id);
    queue.finish(&second, JobOutcome::Unknown).await.unwrap();
    fixture.clock.set(150);
    queue.recover(owner(1)).await.unwrap();
    assert!(
        queue
            .list()
            .await
            .unwrap()
            .iter()
            .all(|job| job.status == JobStatus::Unknown)
    );
    fixture.clock.set(10_000);
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
}

#[tokio::test]
async fn bot_shard_namespace_queue_and_claim_isolation() {
    let fixture = Fixture::new();
    let mut queues = Vec::new();
    for (scope, namespace, queue_id) in [
        (scope(1, 0), "jobs", id(1)),
        (scope(2, 0), "jobs", id(1)),
        (scope(1, 1), "jobs", id(1)),
        (scope(1, 0), "other", id(1)),
        (scope(1, 0), "jobs", id(2)),
    ] {
        let queue = open(
            fixture.store().await,
            &fixture.clock,
            scope,
            namespace,
            queue_id,
            JobLimits::default(),
        )
        .await
        .unwrap();
        queue.activate(owner(1)).await.unwrap();
        queue.schedule(owner(1), spec(1, 100)).await.unwrap();
        queues.push(queue);
    }
    let claim = queues[0]
        .claim(owner(1), schema(), 50)
        .await
        .unwrap()
        .unwrap();
    for queue in queues.iter().skip(1) {
        assert_eq!(
            queue.finish(&claim, JobOutcome::Completed).await,
            Err(JobError::StaleClaim)
        );
        let independent = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
        queue
            .finish(&independent, JobOutcome::Completed)
            .await
            .unwrap();
    }
    queues[0].validate_claim(&claim).await.unwrap();
}

#[tokio::test]
async fn competing_independent_file_handles_grant_at_most_one_claim() {
    let fixture = Fixture::new();
    let first = fixture.queue().await;
    let second = fixture.queue().await;
    first.activate(owner(1)).await.unwrap();
    first.schedule(owner(1), spec(1, 100)).await.unwrap();
    let (a, b) = tokio::join!(
        first.claim(owner(1), schema(), 50),
        second.claim(owner(1), schema(), 50)
    );
    assert_eq!(
        usize::from(matches!(a, Ok(Some(_)))) + usize::from(matches!(b, Ok(Some(_)))),
        1
    );
    for result in [a, b] {
        assert!(matches!(
            result,
            Ok(Some(_)) | Ok(None) | Err(JobError::Store(StateError::Conflict))
        ));
    }
    assert_eq!(first.list().await.unwrap()[0].attempts, 1);
}

#[tokio::test]
async fn renewal_fences_old_permit_and_exact_lease_deadline_rejects_completion() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    fixture.clock.set(120);
    let renewed = queue.renew(&claim, 50).await.unwrap();
    assert_eq!(renewed.lease_until(), Timestamp(170));
    assert_eq!(
        queue.finish(&claim, JobOutcome::Completed).await,
        Err(JobError::StaleClaim)
    );
    fixture.clock.set(169);
    queue.validate_claim(&renewed).await.unwrap();
    fixture.clock.set(170);
    assert_eq!(
        queue.finish(&renewed, JobOutcome::Completed).await,
        Err(JobError::StaleClaim)
    );
}

#[tokio::test]
async fn deadlines_capacity_and_overflow_fail_without_dropping_live_jobs() {
    let fixture = Fixture::new();
    let limits = JobLimits {
        max_jobs: 1,
        max_checkpoint_bytes: 8,
        ..JobLimits::default()
    };
    let queue = open(
        fixture.store().await,
        &fixture.clock,
        scope(1, 0),
        "jobs",
        id(1),
        limits,
    )
    .await
    .unwrap();
    queue.activate(owner(1)).await.unwrap();
    let mut job = spec(1, 100);
    job.expires_at = Some(Timestamp(130));
    queue.schedule(owner(1), job).await.unwrap();
    assert_eq!(
        queue.schedule(owner(1), spec(2, 100)).await,
        Err(JobError::CapacityExceeded)
    );
    let claim = queue.claim(owner(1), schema(), 100).await.unwrap().unwrap();
    assert_eq!(claim.lease_until(), Timestamp(130));
    assert_eq!(
        queue.checkpoint(&claim, vec![0; 9]).await,
        Err(JobError::CapacityExceeded)
    );
    fixture.clock.set(130);
    queue.recover(owner(1)).await.unwrap();
    assert_eq!(queue.list().await.unwrap()[0].status, JobStatus::Expired);
    assert_eq!(
        queue.schedule(owner(1), spec(2, 100)).await,
        Err(JobError::CapacityExceeded)
    );

    let separate = open(
        fixture.store().await,
        &fixture.clock,
        scope(1, 1),
        "jobs",
        id(1),
        JobLimits::default(),
    )
    .await
    .unwrap();
    separate.activate(owner(1)).await.unwrap();
    separate.schedule(owner(1), spec(1, 130)).await.unwrap();
    fixture.clock.set(u64::MAX - 10);
    assert_eq!(
        separate.claim(owner(1), schema(), 100).await,
        Err(JobError::TimeOverflow)
    );
    assert_eq!(separate.list().await.unwrap()[0].attempts, 0);
}

#[tokio::test]
async fn clock_regression_and_volatile_backend_fail_closed() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    fixture.clock.set(99);
    assert_eq!(
        queue.list().await,
        Err(JobError::Store(StateError::ClockRegressed))
    );
    let clock = Clock(Arc::new(AtomicU64::new(100)));
    let memory = Arc::new(MemoryStateStore::new(
        store_limits(),
        Arc::new(clock.clone()),
    ));
    assert!(matches!(
        open(
            memory,
            &clock,
            scope(1, 0),
            "jobs",
            id(1),
            JobLimits::default()
        )
        .await,
        Err(JobError::Store(StateError::DurabilityRequired))
    ));
}

fn queue_key() -> StateKey {
    StateKey::new(
        scope(1, 0),
        PublicLabel::new("jobs").unwrap(),
        PublicLabel::new("durable_jobs_v1").unwrap(),
        StateKind::Application,
        id(1),
    )
}

#[tokio::test]
async fn validly_stored_but_malformed_envelopes_are_never_reinitialized() {
    for change in ["identity", "format", "duplicate", "schema", "expiry"] {
        let fixture = Fixture::new();
        let store = fixture.store().await;
        let queue = open(
            store.clone(),
            &fixture.clock,
            scope(1, 0),
            "jobs",
            id(1),
            JobLimits::default(),
        )
        .await
        .unwrap();
        queue.activate(owner(1)).await.unwrap();
        queue.schedule(owner(1), spec(1, 100)).await.unwrap();
        let key = queue_key();
        let record = store
            .get(&key, SchemaVersion::new(1, 1).unwrap())
            .await
            .unwrap()
            .unwrap();
        let mut value = record.value;
        let mut json: serde_json::Value = serde_json::from_slice(&value.data).unwrap();
        match change {
            "identity" => json["key"]["scope"]["bot_id"] = 99.into(),
            "format" => json["format"] = 2.into(),
            "duplicate" => {
                let job = json["jobs"][0].clone();
                json["jobs"].as_array_mut().unwrap().push(job);
            }
            "schema" => value.schema = SchemaVersion::new(2, 1).unwrap(),
            "expiry" => value.expires_at = Some(Timestamp(200)),
            _ => unreachable!(),
        }
        value.data = serde_json::to_vec(&json).unwrap();
        let invalid_revision = store
            .put(
                key.clone(),
                value,
                PutCondition::IfRevision(record.revision),
            )
            .await
            .unwrap();
        assert!(matches!(
            open(
                store.clone(),
                &fixture.clock,
                scope(1, 0),
                "jobs",
                id(1),
                JobLimits::default()
            )
            .await,
            Err(JobError::InvalidRecord)
        ));
        assert_eq!(
            store
                .get(&key, SchemaVersion::new(1, 1).unwrap())
                .await
                .unwrap()
                .unwrap()
                .revision,
            invalid_revision
        );
    }
}

#[tokio::test]
async fn partial_completion_frames_recover_previous_claim_and_complete_corruption_fails_closed() {
    let source = Fixture::new();
    let queue = source.queue().await;
    queue.activate(owner(1)).await.unwrap();
    let mut job = spec(1, 100);
    job.recovery = RecoveryPolicy::RequiresReconciliation;
    queue.schedule(owner(1), job).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    let before = std::fs::metadata(&source.path).unwrap().len() as usize;
    queue.finish(&claim, JobOutcome::Completed).await.unwrap();
    drop(queue);
    let bytes = std::fs::read(&source.path).unwrap();
    for cut in [
        before + 1,
        before + 8,
        before + 16,
        before + (bytes.len() - before) / 2,
        bytes.len() - 1,
    ] {
        let fixture = Fixture::new();
        std::fs::write(&fixture.path, &bytes[..cut]).unwrap();
        let queue = fixture.queue().await;
        assert!(matches!(
            queue.list().await.unwrap()[0].status,
            JobStatus::Running { .. }
        ));
        fixture.clock.set(150);
        queue.activate(owner(2)).await.unwrap();
        queue.recover(owner(2)).await.unwrap();
        assert_eq!(queue.list().await.unwrap()[0].status, JobStatus::Unknown);
    }
    let corrupt = Fixture::new();
    let mut damaged = bytes;
    damaged[before + 20] ^= 1;
    std::fs::write(&corrupt.path, damaged).unwrap();
    assert!(matches!(
        FileStateStore::open(
            &corrupt.path,
            store_limits(),
            8 * 1024 * 1024,
            Arc::new(corrupt.clock.clone())
        )
        .await,
        Err(StateError::InvalidState)
    ));
}

struct UncertainWrite {
    inner: Arc<FileStateStore>,
    fail: AtomicBool,
}
impl StateStore for UncertainWrite {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        self.inner.get(key, reader)
    }
    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            let revision = self.inner.put(key, value, condition).await?;
            if self.fail.swap(false, Ordering::SeqCst) {
                Err(StateError::UnknownOutcome)
            } else {
                Ok(revision)
            }
        })
    }
    fn delete<'a>(
        &'a self,
        key: &'a StateKey,
        expected_revision: Option<u64>,
    ) -> StoreFuture<'a, bool> {
        self.inner.delete(key, expected_revision)
    }
    fn durability(&self) -> Durability {
        Durability::Durable
    }
}

#[tokio::test]
async fn committed_writes_with_lost_ack_are_reconciled_without_external_replay() {
    let fixture = Fixture::new();
    let store = Arc::new(UncertainWrite {
        inner: fixture.store().await,
        fail: AtomicBool::new(false),
    });
    let queue = open(
        store.clone(),
        &fixture.clock,
        scope(1, 0),
        "jobs",
        id(1),
        JobLimits::default(),
    )
    .await
    .unwrap();
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    store.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        queue.claim(owner(1), schema(), 50).await,
        Err(JobError::Store(StateError::UnknownOutcome))
    );
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
    fixture.clock.set(150);
    queue.recover(owner(1)).await.unwrap();
    fixture.clock.set(160);
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    store.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        queue.begin_external_operation(&claim, id(9)).await,
        Err(JobError::Store(StateError::UnknownOutcome))
    );
    assert_eq!(
        queue.list().await.unwrap()[0].external.unwrap().status,
        ExternalStatus::Pending
    );
    fixture.clock.set(210);
    queue.recover(owner(1)).await.unwrap();
    assert_eq!(queue.list().await.unwrap()[0].status, JobStatus::Unknown);
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
}

#[tokio::test]
async fn completion_ack_loss_keeps_success_after_reopen() {
    let fixture = Fixture::new();
    let store = Arc::new(UncertainWrite {
        inner: fixture.store().await,
        fail: AtomicBool::new(false),
    });
    let queue = open(
        store.clone(),
        &fixture.clock,
        scope(1, 0),
        "jobs",
        id(1),
        JobLimits::default(),
    )
    .await
    .unwrap();
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    queue.begin_external_operation(&claim, id(9)).await.unwrap();
    store.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        queue
            .complete_external_operation(&claim, ExternalOutcome::Succeeded)
            .await,
        Err(JobError::Store(StateError::UnknownOutcome))
    );
    drop(queue);
    drop(store);
    let queue = fixture.queue().await;
    assert_eq!(queue.list().await.unwrap()[0].status, JobStatus::Completed);
    fixture.clock.set(1000);
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
}

#[tokio::test]
async fn process_crash_after_external_intent_preserves_unknown_and_origin() {
    const ENV: &str = "DURABLE_JOB_CRASH_FIXTURE_PATH";
    if let Some(path) = std::env::var_os(ENV) {
        let clock = Clock(Arc::new(AtomicU64::new(100)));
        let store = Arc::new(
            FileStateStore::open(
                PathBuf::from(path),
                store_limits(),
                8 * 1024 * 1024,
                Arc::new(clock.clone()),
            )
            .await
            .unwrap(),
        );
        let queue = open(
            store,
            &clock,
            scope(1, 0),
            "jobs",
            id(1),
            JobLimits::default(),
        )
        .await
        .unwrap();
        queue.activate(owner(1)).await.unwrap();
        queue.schedule(owner(1), spec(1, 100)).await.unwrap();
        let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
        queue.begin_external_operation(&claim, id(7)).await.unwrap();
        // Exit without unwinding/drop, immediately after durable send intent.
        std::process::exit(73);
    }
    let fixture = Fixture::new();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_crash_after_external_intent_preserves_unknown_and_origin",
            "--nocapture",
        ])
        .env(ENV, &fixture.path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("job crash fixture exceeded its deadline");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(status.code(), Some(73));
    let queue = fixture.queue().await;
    queue.activate(owner(2)).await.unwrap();
    assert!(queue.claim(owner(2), schema(), 50).await.unwrap().is_none());
    fixture.clock.set(150);
    queue.recover(owner(2)).await.unwrap();
    let job = &queue.list().await.unwrap()[0];
    assert_eq!(job.status, JobStatus::Unknown);
    assert_eq!(job.external.unwrap().id, id(7));
    assert_eq!(job.external.unwrap().owner, owner(1));
}

#[tokio::test]
async fn debug_and_errors_do_not_include_checkpoint_contents() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    let mut job = spec(1, 100);
    job.checkpoint = b"synthetic-sensitive-marker".to_vec();
    assert!(!format!("{job:?}").contains("synthetic-sensitive-marker"));
    queue.schedule(owner(1), job).await.unwrap();
    let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
    let view = queue.list().await.unwrap();
    let debug = format!("{queue:?} {claim:?} {view:?} {}", JobError::InvalidRecord);
    assert!(!debug.contains("synthetic-sensitive-marker"));
    assert!(!debug.contains("115, 121, 110, 116, 104"));
}

#[tokio::test]
async fn checkpoint_schema_is_checked_before_claiming_after_generation_change() {
    let fixture = Fixture::new();
    let queue = fixture.queue().await;
    queue.activate(owner(1)).await.unwrap();
    let mut job = spec(1, 100);
    job.schema = SchemaVersion::new(2, 2).unwrap();
    queue.schedule(owner(1), job).await.unwrap();
    drop(queue);
    let queue = fixture.queue().await;
    queue.activate(owner(2)).await.unwrap();
    assert_eq!(
        queue.claim(owner(2), schema(), 50).await,
        Err(JobError::Store(StateError::IncompatibleSchema))
    );
    assert_eq!(queue.list().await.unwrap()[0].attempts, 0);
    let claim = queue
        .claim(owner(2), SchemaVersion::new(3, 2).unwrap(), 50)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.job().spec.schema.version(), 2);
    queue.finish(&claim, JobOutcome::Completed).await.unwrap();
}

struct LateWrite {
    inner: Arc<FileStateStore>,
    clock: Clock,
    advance_to: AtomicU64,
    stall_read: AtomicBool,
    stall_after_commit: AtomicBool,
}
impl StateStore for LateWrite {
    fn get<'a>(
        &'a self,
        key: &'a StateKey,
        reader: SchemaVersion,
    ) -> StoreFuture<'a, Option<StateRecord>> {
        Box::pin(async move {
            if self.stall_read.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            self.inner.get(key, reader).await
        })
    }
    fn put(
        &self,
        key: StateKey,
        value: StateValue,
        condition: PutCondition,
    ) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            let advance = self.advance_to.swap(0, Ordering::SeqCst);
            if advance != 0 {
                self.clock.set(advance);
            }
            let revision = self.inner.put(key, value, condition).await?;
            if self.stall_after_commit.swap(false, Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(revision)
        })
    }
    fn delete<'a>(
        &'a self,
        key: &'a StateKey,
        expected_revision: Option<u64>,
    ) -> StoreFuture<'a, bool> {
        self.inner.delete(key, expected_revision)
    }
    fn durability(&self) -> Durability {
        Durability::Durable
    }
}
async fn late_store(fixture: &Fixture) -> Arc<LateWrite> {
    Arc::new(LateWrite {
        inner: fixture.store().await,
        clock: fixture.clock.clone(),
        advance_to: AtomicU64::new(0),
        stall_read: AtomicBool::new(false),
        stall_after_commit: AtomicBool::new(false),
    })
}

#[tokio::test]
async fn slow_commit_never_returns_expired_claim_renewal_or_external_send_permission() {
    for phase in ["claim", "renew", "external"] {
        let fixture = Fixture::new();
        let store = late_store(&fixture).await;
        let queue = open(
            store.clone(),
            &fixture.clock,
            scope(1, 0),
            "jobs",
            id(1),
            JobLimits::default(),
        )
        .await
        .unwrap();
        queue.activate(owner(1)).await.unwrap();
        queue.schedule(owner(1), spec(1, 100)).await.unwrap();
        if phase == "claim" {
            store.advance_to.store(150, Ordering::SeqCst);
            assert_eq!(
                queue.claim(owner(1), schema(), 50).await,
                Err(JobError::StaleClaim)
            );
        } else {
            let claim = queue.claim(owner(1), schema(), 50).await.unwrap().unwrap();
            if phase == "renew" {
                store.advance_to.store(200, Ordering::SeqCst);
                assert_eq!(queue.renew(&claim, 100).await, Err(JobError::StaleClaim));
            } else {
                store.advance_to.store(150, Ordering::SeqCst);
                assert_eq!(
                    queue.begin_external_operation(&claim, id(9)).await,
                    Err(JobError::StaleClaim)
                );
            }
        }
        queue.recover(owner(1)).await.unwrap();
        if phase == "external" {
            assert_eq!(queue.list().await.unwrap()[0].status, JobStatus::Unknown);
        } else {
            assert!(matches!(
                queue.list().await.unwrap()[0].status,
                JobStatus::Scheduled { .. }
            ));
        }
    }
}

#[tokio::test]
async fn bounded_store_timeouts_keep_committed_claims_and_do_not_fake_empty_lists() {
    let fixture = Fixture::new();
    let store = late_store(&fixture).await;
    let limits = JobLimits {
        // Ordinary store calls must not time out on a loaded CI runner; only the stalled
        // call after `stall_after_commit` is expected to hit this limit.
        store_timeout_ms: 2_000,
        ..JobLimits::default()
    };
    let queue = open(
        store.clone(),
        &fixture.clock,
        scope(1, 0),
        "jobs",
        id(1),
        limits,
    )
    .await
    .unwrap();
    queue.activate(owner(1)).await.unwrap();
    queue.schedule(owner(1), spec(1, 100)).await.unwrap();
    store.stall_after_commit.store(true, Ordering::SeqCst);
    assert_eq!(
        queue.claim(owner(1), schema(), 50).await,
        Err(JobError::Store(StateError::UnknownOutcome))
    );
    assert!(matches!(
        queue.list().await.unwrap()[0].status,
        JobStatus::Running { .. }
    ));
    assert!(queue.claim(owner(1), schema(), 50).await.unwrap().is_none());
    store.stall_read.store(true, Ordering::SeqCst);
    assert_eq!(
        queue.list().await,
        Err(JobError::Store(StateError::BackendUnavailable))
    );
    store.stall_read.store(false, Ordering::SeqCst);
    assert_eq!(queue.list().await.unwrap().len(), 1);
}
