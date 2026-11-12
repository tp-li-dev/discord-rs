//! Bounded, durable job scheduling on the existing per-key CAS StateStore API.
//!
//! One queue record contains the index, checkpoints, leases and outcomes, so a
//! crash cannot commit an index separately from its job. Open the same scope,
//! namespace and queue ID after restart, then activate an externally authorized
//! owner. This is a trusted local API, not authentication, quorum or a worker
//! executor. The host must stop expired handlers, verify current ownership at
//! every side effect and fence/deduplicate that effect. A lease does not stop a
//! paused process. Existing unexpired claims may finish during generation drain;
//! they cannot renew after a new owner is activated.
//!
//! RetrySafe explicitly permits replay from the last checkpoint. Other crashed
//! work becomes Unknown. Before sending an external operation, persist its ID
//! with begin_external_operation; an unresolved operation ALWAYS becomes Unknown
//! after lease loss, even for RetrySafe jobs. Unknown has no automatic or manual
//! clearing API here: authenticated reconciliation and durable audit belong to
//! the management integration described in docs/local-response-management.md.
//! Checkpoints and public handler labels must contain no credentials or tokens.
//! Bytes supplied to checkpoint/release_after_stop must retain JobSpec.schema's
//! format. The reader compatibility gate does not migrate data; schema migration
//! APIs are not implemented here.
//!
//! StateStore UnknownOutcome or cancellation can mean a mutation committed. Read
//! the queue again; do not blindly repeat an external effect. Completed/failed/
//! Unknown jobs are retained as tombstones and count against capacity. There is
//! no automatic eviction, compaction, full attempt audit or cross-host guarantee.
//! FileStateStore durability and OS/filesystem limitations remain unchanged.

use crate::state_store::{
    Durability, PublicLabel, PutCondition, SchemaVersion, StateClock, StateError, StateId,
    StateKey, StateKind, StateStore, StateValue, StoreScope, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, sync::Arc, sync::Mutex, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobError {
    Store(StateError),
    InvalidConfiguration,
    InvalidRecord,
    CapacityExceeded,
    DuplicateJob,
    Fenced,
    Draining,
    StaleClaim,
    ExternalOperationPending,
    NoExternalOperation,
    TimeOverflow,
}
impl From<StateError> for JobError {
    fn from(value: StateError) -> Self {
        Self::Store(value)
    }
}
impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable job error: {self:?}")
    }
}
impl std::error::Error for JobError {}

/// Supplied only by a host that has already obtained ownership authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobOwner {
    pub generation: u64,
    pub ownership_epoch: u64,
}
impl JobOwner {
    fn valid(self) -> bool {
        self.generation != 0 && self.ownership_epoch != 0
    }
    fn follows(self, previous: Self) -> bool {
        self.ownership_epoch > previous.ownership_epoch
            || (self.ownership_epoch == previous.ownership_epoch
                && self.generation > previous.generation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RecoveryPolicy {
    /// The application promises replay is idempotent or independently fenced.
    RetrySafe,
    /// A crash or cancellation may have performed an external side effect.
    RequiresReconciliation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Number of retries after the first failure; graceful checkpoints do not
    /// consume this budget. Crashed RetrySafe claims do consume it.
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}
impl RetryPolicy {
    fn valid(self) -> bool {
        self.initial_backoff_ms != 0 && self.max_backoff_ms >= self.initial_backoff_ms
    }
    fn delay(self, failures: u32) -> u64 {
        self.initial_backoff_ms
            .saturating_mul(
                1u64.checked_shl(failures.saturating_sub(1))
                    .unwrap_or(u64::MAX),
            )
            .min(self.max_backoff_ms)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobLimits {
    pub max_jobs: usize,
    pub max_checkpoint_bytes: usize,
    pub max_record_bytes: usize,
    pub max_lease_ms: u64,
    pub store_timeout_ms: u64,
}
impl Default for JobLimits {
    fn default() -> Self {
        Self {
            max_jobs: 128,
            max_checkpoint_bytes: 4096,
            max_record_bytes: 1024 * 1024,
            max_lease_ms: 60_000,
            store_timeout_ms: 5000,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub id: StateId,
    pub handler: PublicLabel,
    /// Application checkpoint format, independent of the queue storage schema.
    pub schema: SchemaVersion,
    pub scheduled_at: Timestamp,
    pub expires_at: Option<Timestamp>,
    pub checkpoint: Vec<u8>,
    pub retry: RetryPolicy,
    pub recovery: RecoveryPolicy,
}
impl fmt::Debug for JobSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobSpec")
            .field("id", &self.id)
            .field("handler", &self.handler)
            .field("schema", &self.schema)
            .field("scheduled_at", &self.scheduled_at)
            .field("expires_at", &self.expires_at)
            .field("checkpoint_len", &self.checkpoint.len())
            .field("retry", &self.retry)
            .field("recovery", &self.recovery)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobStatus {
    Scheduled {
        due_at: Timestamp,
    },
    Running {
        owner: JobOwner,
        lease_until: Timestamp,
    },
    Completed,
    Failed,
    Expired,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExternalStatus {
    Pending,
    Succeeded,
    ConfirmedNoEffect,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalOperation {
    pub id: StateId,
    pub owner: JobOwner,
    pub status: ExternalStatus,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobView {
    pub spec: JobSpec,
    pub status: JobStatus,
    pub attempts: u64,
    pub failures: u32,
    pub last_failure_at: Option<Timestamp>,
    pub external: Option<ExternalOperation>,
}
impl fmt::Debug for JobView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobView")
            .field("spec", &self.spec)
            .field("status", &self.status)
            .field("attempts", &self.attempts)
            .field("failures", &self.failures)
            .field("external", &self.external)
            .finish()
    }
}

/// A persisted, finite execution permit. It is not remote authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobClaim {
    key: StateKey,
    job: JobView,
}
impl JobClaim {
    pub fn job(&self) -> &JobView {
        &self.job
    }
    pub fn owner(&self) -> JobOwner {
        match self.job.status {
            JobStatus::Running { owner, .. } => owner,
            _ => unreachable!("claims are created only for running jobs"),
        }
    }
    pub fn lease_until(&self) -> Timestamp {
        match self.job.status {
            JobStatus::Running { lease_until, .. } => lease_until,
            _ => unreachable!("claims are created only for running jobs"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobOutcome {
    Completed,
    /// The handler has stopped and the failure is known safe to retry.
    RetryableFailure,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalOutcome {
    Succeeded,
    /// This is an application assertion of no side effect, not merely HTTP 5xx.
    ConfirmedNoEffect,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainStatus {
    pub draining: bool,
    pub running: usize,
    pub unknown: usize,
}
impl DrainStatus {
    pub fn is_complete(self) -> bool {
        self.draining && self.running == 0 && self.unknown == 0
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Queue {
    format: u16,
    key: StateKey,
    last_now: Timestamp,
    owner: Option<JobOwner>,
    draining: bool,
    jobs: Vec<JobView>,
}

#[derive(Clone)]
pub struct DurableJobs {
    store: Arc<dyn StateStore>,
    clock: Arc<dyn StateClock>,
    key: StateKey,
    limits: JobLimits,
    observed: Arc<Mutex<Option<Timestamp>>>,
}
impl fmt::Debug for DurableJobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableJobs")
            .field("key", &self.key)
            .finish()
    }
}
impl DurableJobs {
    /// Creates an empty queue only when its key is absent. Never delete/reuse a
    /// live queue key. The backing directory/file is trusted application storage.
    pub async fn open(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn StateClock>,
        scope: StoreScope,
        namespace: PublicLabel,
        queue_id: StateId,
        limits: JobLimits,
    ) -> Result<Self, JobError> {
        if store.durability() != Durability::Durable {
            return Err(StateError::DurabilityRequired.into());
        }
        if limits.max_jobs == 0
            || limits.max_record_bytes == 0
            || limits.max_lease_ms == 0
            || limits.store_timeout_ms == 0
        {
            return Err(JobError::InvalidConfiguration);
        }
        let this = Self {
            store,
            clock,
            key: StateKey::new(
                scope,
                namespace,
                PublicLabel::new("durable_jobs_v1")?,
                StateKind::Application,
                queue_id,
            ),
            limits,
            observed: Arc::new(Mutex::new(None)),
        };
        if this.read().await?.is_none() {
            let now = this.now(None)?;
            let queue = Queue {
                format: 1,
                key: this.key.clone(),
                last_now: now,
                owner: None,
                draining: true,
                jobs: Vec::new(),
            };
            match this.save(&queue, PutCondition::IfAbsent).await {
                Ok(()) => {}
                Err(JobError::Store(StateError::Conflict)) => {
                    this.load().await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(this)
    }

    fn schema() -> SchemaVersion {
        SchemaVersion::new(1, 1).expect("fixed schema")
    }
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.limits.store_timeout_ms)
    }
    fn now(&self, persisted: Option<Timestamp>) -> Result<Timestamp, JobError> {
        let mut observed = self
            .observed
            .lock()
            .map_err(|_| StateError::ClockUnavailable)?;
        let now = self.clock.now()?;
        if persisted.is_some_and(|previous| now < previous)
            || observed.is_some_and(|previous| now < previous)
        {
            return Err(StateError::ClockRegressed.into());
        }
        *observed = Some(now);
        Ok(now)
    }
    async fn read(&self) -> Result<Option<(Queue, u64)>, JobError> {
        let record =
            tokio::time::timeout(self.timeout(), self.store.get(&self.key, Self::schema()))
                .await
                .map_err(|_| StateError::BackendUnavailable)??;
        let Some(record) = record else {
            return Ok(None);
        };
        if record.value.expires_at.is_some()
            || record.value.schema != Self::schema()
            || record.value.data.len() > self.limits.max_record_bytes
        {
            return Err(JobError::InvalidRecord);
        }
        let queue: Queue =
            serde_json::from_slice(&record.value.data).map_err(|_| JobError::InvalidRecord)?;
        self.validate(&queue)?;
        self.now(Some(queue.last_now))?;
        Ok(Some((queue, record.revision)))
    }
    async fn load(&self) -> Result<(Queue, u64), JobError> {
        self.read().await?.ok_or(JobError::InvalidRecord)
    }
    fn validate(&self, queue: &Queue) -> Result<(), JobError> {
        if queue.format != 1
            || queue.key != self.key
            || queue.owner.is_some_and(|owner| !owner.valid())
            || (queue.owner.is_none() && (!queue.draining || !queue.jobs.is_empty()))
        {
            return Err(JobError::InvalidRecord);
        }
        if queue.jobs.len() > self.limits.max_jobs {
            return Err(JobError::CapacityExceeded);
        }
        let mut ids = HashSet::new();
        for job in &queue.jobs {
            if !ids.insert(job.spec.id)
                || !job.spec.retry.valid()
                || job
                    .spec
                    .expires_at
                    .is_some_and(|expiry| expiry <= job.spec.scheduled_at)
                || job.failures as u64 > job.attempts
                || job.last_failure_at.is_some_and(|at| at > queue.last_now)
                || (job.failures == 0) != job.last_failure_at.is_none()
            {
                return Err(JobError::InvalidRecord);
            }
            if job.spec.checkpoint.len() > self.limits.max_checkpoint_bytes {
                return Err(JobError::CapacityExceeded);
            }
            if let JobStatus::Running { owner, lease_until } = job.status
                && (!owner.valid()
                    || job.attempts == 0
                    || queue.owner.is_none_or(|current| owner.follows(current))
                    || job
                        .spec
                        .expires_at
                        .is_some_and(|expiry| lease_until > expiry))
            {
                return Err(JobError::InvalidRecord);
            }
            if let Some(operation) = job.external {
                if !operation.owner.valid() || job.attempts == 0 {
                    return Err(JobError::InvalidRecord);
                }
                match operation.status {
                    ExternalStatus::Pending => {
                        if !matches!(job.status, JobStatus::Running { owner, .. } if owner == operation.owner)
                        {
                            return Err(JobError::InvalidRecord);
                        }
                    }
                    ExternalStatus::Unknown if job.status != JobStatus::Unknown => {
                        return Err(JobError::InvalidRecord);
                    }
                    ExternalStatus::Succeeded if job.status != JobStatus::Completed => {
                        return Err(JobError::InvalidRecord);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
    async fn save(&self, queue: &Queue, condition: PutCondition) -> Result<(), JobError> {
        self.validate(queue)?;
        let data = serde_json::to_vec(queue).map_err(|_| StateError::Encoding)?;
        if data.len() > self.limits.max_record_bytes {
            return Err(JobError::CapacityExceeded);
        }
        tokio::time::timeout(
            self.timeout(),
            self.store.put(
                self.key.clone(),
                StateValue {
                    schema: Self::schema(),
                    expires_at: None,
                    data,
                },
                condition,
            ),
        )
        .await
        .map_err(|_| StateError::UnknownOutcome)??;
        Ok(())
    }
    async fn edit<T>(
        &self,
        update: impl FnOnce(&mut Queue, Timestamp) -> Result<(T, bool), JobError>,
    ) -> Result<T, JobError> {
        let (mut queue, revision) = self.load().await?;
        let now = self.now(Some(queue.last_now))?;
        queue.last_now = now;
        let (value, changed) = update(&mut queue, now)?;
        if changed {
            self.save(&queue, PutCondition::IfRevision(revision))
                .await?;
        }
        Ok(value)
    }
    fn active(queue: &Queue, owner: JobOwner, admission: bool) -> Result<(), JobError> {
        if queue.owner != Some(owner) {
            return Err(JobError::Fenced);
        }
        if admission && queue.draining {
            return Err(JobError::Draining);
        }
        Ok(())
    }

    /// Epoch increases may reset the generation; within one epoch generation
    /// must increase. Repeating the same owner is idempotent and never undrains.
    pub async fn activate(&self, owner: JobOwner) -> Result<(), JobError> {
        if !owner.valid() {
            return Err(JobError::InvalidConfiguration);
        }
        self.edit(|queue, _| {
            if queue.owner == Some(owner) {
                return Ok(((), false));
            }
            if queue.owner.is_some_and(|previous| !owner.follows(previous)) {
                return Err(JobError::Fenced);
            }
            queue.owner = Some(owner);
            queue.draining = false;
            Ok(((), true))
        })
        .await
    }

    pub async fn schedule(&self, owner: JobOwner, spec: JobSpec) -> Result<(), JobError> {
        if !spec.retry.valid()
            || spec
                .expires_at
                .is_some_and(|expiry| expiry <= spec.scheduled_at)
        {
            return Err(JobError::InvalidConfiguration);
        }
        if spec.checkpoint.len() > self.limits.max_checkpoint_bytes {
            return Err(JobError::CapacityExceeded);
        }
        self.edit(|queue, now| {
            Self::active(queue, owner, true)?;
            if queue.jobs.iter().any(|job| job.spec.id == spec.id) {
                return Err(JobError::DuplicateJob);
            }
            if queue.jobs.len() >= self.limits.max_jobs {
                return Err(JobError::CapacityExceeded);
            }
            if spec.expires_at.is_some_and(|expiry| now >= expiry) {
                return Err(StateError::InvalidExpiry.into());
            }
            queue.jobs.push(JobView {
                status: JobStatus::Scheduled {
                    due_at: spec.scheduled_at,
                },
                spec,
                attempts: 0,
                failures: 0,
                last_failure_at: None,
                external: None,
            });
            Ok(((), true))
        })
        .await
    }

    pub async fn list(&self) -> Result<Vec<JobView>, JobError> {
        Ok(self.load().await?.0.jobs)
    }

    fn retry(job: &mut JobView, now: Timestamp) -> Result<(), JobError> {
        job.failures = job
            .failures
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?;
        job.last_failure_at = Some(now);
        if job.failures > job.spec.retry.max_retries {
            job.status = JobStatus::Failed;
        } else {
            let due_at = Timestamp(
                now.0
                    .checked_add(job.spec.retry.delay(job.failures))
                    .ok_or(JobError::TimeOverflow)?,
            );
            job.status = if job.spec.expires_at.is_some_and(|expiry| due_at >= expiry) {
                JobStatus::Expired
            } else {
                JobStatus::Scheduled { due_at }
            };
        }
        Ok(())
    }
    fn unknown(job: &mut JobView) {
        job.status = JobStatus::Unknown;
        if let Some(operation) = &mut job.external
            && operation.status == ExternalStatus::Pending
        {
            operation.status = ExternalStatus::Unknown;
        }
    }
    fn recover_jobs(queue: &mut Queue, now: Timestamp) -> Result<usize, JobError> {
        let mut changed = 0;
        for job in &mut queue.jobs {
            match job.status {
                JobStatus::Running { lease_until, .. } if now >= lease_until => {
                    if job.spec.recovery == RecoveryPolicy::RequiresReconciliation
                        || job
                            .external
                            .is_some_and(|operation| operation.status == ExternalStatus::Pending)
                    {
                        Self::unknown(job);
                    } else if job.spec.expires_at.is_some_and(|expiry| now >= expiry) {
                        job.status = JobStatus::Expired;
                    } else {
                        // Backoff starts when recovery observes failure, not in a
                        // past interval that could cause an immediate retry storm.
                        Self::retry(job, now)?;
                    }
                    changed += 1;
                }
                JobStatus::Scheduled { .. }
                    if job.spec.expires_at.is_some_and(|expiry| now >= expiry) =>
                {
                    job.status = JobStatus::Expired;
                    changed += 1;
                }
                _ => {}
            }
        }
        Ok(changed)
    }
    pub async fn recover(&self, owner: JobOwner) -> Result<usize, JobError> {
        self.edit(|queue, now| {
            Self::active(queue, owner, false)?;
            let recovered = Self::recover_jobs(queue, now)?;
            Ok((recovered, recovered != 0))
        })
        .await
    }

    /// Claims the earliest due job (registration order breaks ties). Call
    /// validate_claim and the host ownership gate immediately before dispatch.
    pub async fn claim(
        &self,
        owner: JobOwner,
        reader: SchemaVersion,
        lease_ms: u64,
    ) -> Result<Option<JobClaim>, JobError> {
        if lease_ms == 0 || lease_ms > self.limits.max_lease_ms {
            return Err(JobError::InvalidConfiguration);
        }
        let claim = self
            .edit(|queue, now| {
                Self::active(queue, owner, true)?;
                let recovered = Self::recover_jobs(queue, now)?;
                let index = queue
                    .jobs
                    .iter()
                    .enumerate()
                    .filter_map(|(index, job)| match job.status {
                        JobStatus::Scheduled { due_at } if due_at <= now => Some((index, due_at)),
                        _ => None,
                    })
                    .min_by_key(|(_, due_at)| *due_at)
                    .map(|(index, _)| index);
                let Some(index) = index else {
                    return Ok((None, recovered != 0));
                };
                let job = &mut queue.jobs[index];
                if !reader.can_read(job.spec.schema) {
                    return Err(StateError::IncompatibleSchema.into());
                }
                let mut lease_until =
                    Timestamp(now.0.checked_add(lease_ms).ok_or(JobError::TimeOverflow)?);
                if let Some(expiry) = job.spec.expires_at {
                    lease_until = lease_until.min(expiry);
                }
                job.attempts = job
                    .attempts
                    .checked_add(1)
                    .ok_or(StateError::RevisionExhausted)?;
                job.status = JobStatus::Running { owner, lease_until };
                Ok((
                    Some(JobClaim {
                        key: self.key.clone(),
                        job: job.clone(),
                    }),
                    true,
                ))
            })
            .await?;
        // A slow durable write must not return an already expired permit.
        if let Some(claim) = &claim
            && self.now(None)? >= claim.lease_until()
        {
            return Err(JobError::StaleClaim);
        }
        Ok(claim)
    }
    fn claimed<'a>(
        &self,
        queue: &'a mut Queue,
        claim: &JobClaim,
        now: Timestamp,
    ) -> Result<&'a mut JobView, JobError> {
        if claim.key != self.key {
            return Err(JobError::StaleClaim);
        }
        let job = queue
            .jobs
            .iter_mut()
            .find(|job| job.spec.id == claim.job.spec.id)
            .ok_or(JobError::StaleClaim)?;
        if job.attempts != claim.job.attempts
            || job.status != claim.job.status
            || now >= claim.lease_until()
        {
            return Err(JobError::StaleClaim);
        }
        Ok(job)
    }
    pub async fn validate_claim(&self, claim: &JobClaim) -> Result<(), JobError> {
        let (mut queue, _) = self.load().await?;
        let now = self.now(Some(queue.last_now))?;
        self.claimed(&mut queue, claim, now)?;
        Ok(())
    }
    pub async fn renew(&self, claim: &JobClaim, lease_ms: u64) -> Result<JobClaim, JobError> {
        if lease_ms == 0 || lease_ms > self.limits.max_lease_ms {
            return Err(JobError::InvalidConfiguration);
        }
        let renewed = self
            .edit(|queue, now| {
                Self::active(queue, claim.owner(), false)?;
                let job = self.claimed(queue, claim, now)?;
                let mut until =
                    Timestamp(now.0.checked_add(lease_ms).ok_or(JobError::TimeOverflow)?);
                if let Some(expiry) = job.spec.expires_at {
                    until = until.min(expiry);
                }
                if until <= claim.lease_until() {
                    return Err(JobError::InvalidConfiguration);
                }
                job.status = JobStatus::Running {
                    owner: claim.owner(),
                    lease_until: until,
                };
                Ok((
                    JobClaim {
                        key: self.key.clone(),
                        job: job.clone(),
                    },
                    true,
                ))
            })
            .await?;
        if self.now(None)? >= renewed.lease_until() {
            return Err(JobError::StaleClaim);
        }
        Ok(renewed)
    }
    pub async fn checkpoint(&self, claim: &JobClaim, checkpoint: Vec<u8>) -> Result<(), JobError> {
        if checkpoint.len() > self.limits.max_checkpoint_bytes {
            return Err(JobError::CapacityExceeded);
        }
        self.edit(|queue, now| {
            let job = self.claimed(queue, claim, now)?;
            if job
                .external
                .is_some_and(|operation| operation.status == ExternalStatus::Pending)
            {
                return Err(JobError::ExternalOperationPending);
            }
            job.spec.checkpoint = checkpoint;
            Ok(((), true))
        })
        .await
    }
    /// Persist before external dispatch. Do not send unless this succeeds AND
    /// the current lease/ownership still permit the side effect.
    pub async fn begin_external_operation(
        &self,
        claim: &JobClaim,
        operation_id: StateId,
    ) -> Result<(), JobError> {
        self.edit(|queue, now| {
            let job = self.claimed(queue, claim, now)?;
            if job.external.is_some_and(|operation| {
                operation.status == ExternalStatus::Pending || operation.id == operation_id
            }) {
                return Err(JobError::ExternalOperationPending);
            }
            job.external = Some(ExternalOperation {
                id: operation_id,
                owner: claim.owner(),
                status: ExternalStatus::Pending,
            });
            Ok(((), true))
        })
        .await?;
        if self.now(None)? >= claim.lease_until() {
            return Err(JobError::StaleClaim);
        }
        Ok(())
    }
    pub async fn complete_external_operation(
        &self,
        claim: &JobClaim,
        outcome: ExternalOutcome,
    ) -> Result<(), JobError> {
        self.edit(|queue, now| {
            let job = self.claimed(queue, claim, now)?;
            let operation = job
                .external
                .as_mut()
                .filter(|operation| operation.status == ExternalStatus::Pending)
                .ok_or(JobError::NoExternalOperation)?;
            match outcome {
                ExternalOutcome::Succeeded => {
                    operation.status = ExternalStatus::Succeeded;
                    job.status = JobStatus::Completed;
                }
                ExternalOutcome::ConfirmedNoEffect => {
                    operation.status = ExternalStatus::ConfirmedNoEffect;
                    Self::retry(job, now)?;
                }
                ExternalOutcome::Unknown => Self::unknown(job),
            }
            Ok(((), true))
        })
        .await
    }
    pub async fn finish(&self, claim: &JobClaim, outcome: JobOutcome) -> Result<(), JobError> {
        self.edit(|queue, now| {
            let job = self.claimed(queue, claim, now)?;
            if outcome != JobOutcome::Unknown
                && job
                    .external
                    .is_some_and(|operation| operation.status == ExternalStatus::Pending)
            {
                return Err(JobError::ExternalOperationPending);
            }
            match outcome {
                JobOutcome::Completed => job.status = JobStatus::Completed,
                JobOutcome::RetryableFailure => Self::retry(job, now)?,
                JobOutcome::Unknown => Self::unknown(job),
            }
            Ok(((), true))
        })
        .await
    }

    /// Stops new admission atomically; does NOT abort handlers or release leases.
    /// Reopening or repeating activate with this owner cannot undo drain.
    pub async fn begin_drain(&self, owner: JobOwner) -> Result<DrainStatus, JobError> {
        self.edit(|queue, _| {
            Self::active(queue, owner, false)?;
            queue.draining = true;
            Ok((Self::drain_counts(queue), true))
        })
        .await
    }
    fn drain_counts(queue: &Queue) -> DrainStatus {
        DrainStatus {
            draining: queue.draining,
            running: queue
                .jobs
                .iter()
                .filter(|job| matches!(job.status, JobStatus::Running { .. }))
                .count(),
            unknown: queue
                .jobs
                .iter()
                .filter(|job| job.status == JobStatus::Unknown)
                .count(),
        }
    }
    pub async fn drain_status(&self) -> Result<DrainStatus, JobError> {
        Ok(Self::drain_counts(&self.load().await?.0))
    }
    /// Call ONLY after joining/stopping the handler and confirming there is no
    /// unresolved external effect. Checkpoint and release commit together. A
    /// dropped Future or cancellation request alone is not proof of stoppage.
    pub async fn release_after_stop(
        &self,
        claim: &JobClaim,
        checkpoint: Vec<u8>,
    ) -> Result<(), JobError> {
        if checkpoint.len() > self.limits.max_checkpoint_bytes {
            return Err(JobError::CapacityExceeded);
        }
        self.edit(|queue, now| {
            let job = self.claimed(queue, claim, now)?;
            if job
                .external
                .is_some_and(|operation| operation.status == ExternalStatus::Pending)
            {
                return Err(JobError::ExternalOperationPending);
            }
            job.spec.checkpoint = checkpoint;
            job.status = JobStatus::Scheduled { due_at: now };
            Ok(((), true))
        })
        .await
    }
}
