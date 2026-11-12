//! Managed, cooperative tasks for one Bot/Shard generation. No ownership authority,
//! persistent jobs, or external side-effect guarantees are provided here.
//!
//! The owner must drive `poll` (or TaskManager's wait methods). Supervisor restart
//! decisions require Supervisor::poll. Cancellation requests never drop a future:
//! an uncooperative task remains live and prevents drain. JoinSet retains every
//! spawned handle, including while a wait future is cancelled. Dropping the manager
//! is emergency abort-on-drop, NOT graceful shutdown; use cancel + wait_all first.
//!
//! DrainTracker integration: serialize its root admission with this manager's
//! admission, keep a WorkPermit per admitted task and use WorkPermit::child for
//! descendants. Keep each permit until the corresponding join is collected, not
//! just until cancellation is requested. Start both admission gates together.
//! Its wait_drained is only one input: REST outcomes, persisted state and IPC flush
//! must be confirmed separately. This module does not change the existing tracker
//! or wire itself into Worker/Cog/REST; the generation adapter must do that.

use crate::protocol::{BotId, RequestId};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinSet};

pub type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// One local monotonic time domain, including its wake-up mechanism. Test clocks
/// must wake sleepers when advanced; neither wall time nor lease authority is used.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
    /// May wake early. A clock capable of regressing must also return early on
    /// regression, allowing async waiters to detect the failure and cancel work.
    fn sleep_until(&self, deadline: Duration) -> Sleep<'_>;
}

pub struct MonotonicClock(Instant);

impl Default for MonotonicClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }

    fn sleep_until(&self, deadline: Duration) -> Sleep<'_> {
        Box::pin(async move {
            tokio::time::sleep(deadline.saturating_sub(self.now())).await;
        })
    }
}

#[derive(Clone)]
pub struct CancellationToken(watch::Sender<bool>);

impl CancellationToken {
    fn new() -> Self {
        Self(watch::channel(false).0)
    }

    fn cancel(&self) {
        self.0.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Level-triggered and safe with multiple waiters and cancellation races.
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct TaskId(pub u64);

/// Metadata only: callers must supply identifiers, never tokens or request bodies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskOwner {
    pub bot_id: BotId,
    pub shard_id: u32,
    pub cog: Option<String>,
    pub request_id: Option<RequestId>,
    pub parent: Option<TaskId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskKind {
    RequestScoped,
    BackgroundCancellable,
    DurableJob,
    GatewayInternal,
}

#[derive(Clone, Debug)]
pub struct TaskSpec {
    pub owner: TaskOwner,
    pub kind: TaskKind,
    /// Per-attempt runtime limit. Expiry requests cooperative cancellation, never
    /// implicit abort. Registered but unstarted tasks have no runtime deadline.
    pub timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Registered,
    Running,
    Completed,
    Failed,
    CancelRequested,
    Cancelled,
    Restarting,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Fixed error codes intentionally retain no application error/panic text. The
/// runtime's panic hook is outside this module and may still print panic payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskFailure {
    Application,
    Panicked,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskCompletion {
    Completed,
    Cancelled,
    /// Return only after a durable checkpoint is committed and execution stopped.
    Checkpointed,
}

pub type TaskResult = Result<TaskCompletion, TaskFailure>;
pub type TaskFuture = Pin<Box<dyn Future<Output = TaskResult> + Send + 'static>>;
pub type TaskFactory = Arc<dyn Fn(TaskContext) -> TaskFuture + Send + Sync>;

#[derive(Clone)]
pub struct TaskContext {
    pub id: TaskId,
    pub owner: TaskOwner,
    pub attempt: u32,
    pub cancellation: CancellationToken,
}

#[derive(Clone, Debug)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub spec: TaskSpec,
    pub state: TaskState,
    pub attempt: u32,
    pub result: Option<TaskResult>,
    pub timed_out: bool,
    /// Unstarted, completed, checkpointed, or externally confirmed durable work.
    /// An ordinary cancellation/failure after start is not a safe checkpoint.
    pub durable_safe: bool,
    pub started_at: Option<Duration>,
    pub finished_at: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidConfig,
    Capacity,
    UnknownTask,
    InvalidState,
    InvalidParent,
    WrongOwner,
    Draining,
    NoRuntime,
    TimeOverflow,
    ClockRegressed,
    IdExhausted,
    TaskFailed,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task supervisor error: {self:?}")
    }
}
impl std::error::Error for Error {}

struct Entry {
    snapshot: TaskSnapshot,
    cancellation: CancellationToken,
    deadline: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Metrics {
    pub registered: usize,
    pub running: usize,
    pub cancel_requested: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub restarting: usize,
    pub timed_out: usize,
    pub pending_joins: usize,
}

/// Serialized control-plane owner. Terminal records count against capacity until
/// explicitly forgotten, so both live tasks and retained results are bounded.
pub struct TaskManager<C = MonotonicClock> {
    clock: C,
    bot_id: BotId,
    shard_id: u32,
    capacity: usize,
    next_id: u64,
    entries: BTreeMap<TaskId, Entry>,
    joins: JoinSet<TaskResult>,
    join_ids: HashMap<tokio::task::Id, TaskId>,
    last_now: Duration,
    clock_failed: bool,
    draining: bool,
    stopping_residents: bool,
    closed: bool,
}

impl<C: Clock> TaskManager<C> {
    pub fn new(clock: C, bot_id: BotId, shard_id: u32, capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidConfig);
        }
        let last_now = clock.now();
        Ok(Self {
            clock,
            bot_id,
            shard_id,
            capacity,
            next_id: 1,
            entries: BTreeMap::new(),
            joins: JoinSet::new(),
            join_ids: HashMap::new(),
            last_now,
            clock_failed: false,
            draining: false,
            stopping_residents: false,
            closed: false,
        })
    }

    fn now(&mut self) -> Result<Duration, Error> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            // Stop all new work on a broken clock; retain and reap existing joins.
            self.draining = true;
            let ids: Vec<_> = self.entries.keys().copied().collect();
            for id in ids {
                self.cancel_tree(id);
            }
            return Err(Error::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    pub fn register(&mut self, spec: TaskSpec) -> Result<TaskId, Error> {
        self.now()?;
        if self.closed {
            return Err(Error::Draining);
        }
        if spec.owner.bot_id != self.bot_id || spec.owner.shard_id != self.shard_id {
            return Err(Error::WrongOwner);
        }
        if spec.timeout.is_some_and(|d| d.is_zero()) {
            return Err(Error::InvalidConfig);
        }
        if let Some(parent) = spec.owner.parent {
            let entry = self.entries.get(&parent).ok_or(Error::InvalidParent)?;
            let p = &entry.snapshot;
            if p.state != TaskState::Running {
                return Err(Error::InvalidParent);
            }
            if p.spec.owner.cog != spec.owner.cog
                || p.spec.owner.request_id != spec.owner.request_id
                || (p.spec.kind == TaskKind::GatewayInternal)
                    != (spec.kind == TaskKind::GatewayInternal)
                || (p.spec.kind == TaskKind::RequestScoped && spec.kind != TaskKind::RequestScoped)
            {
                return Err(Error::InvalidParent);
            }
        }
        if self.draining
            && spec.kind != TaskKind::GatewayInternal
            && !(spec.kind == TaskKind::RequestScoped && spec.owner.parent.is_some())
        {
            return Err(Error::Draining);
        }
        if self.entries.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        let id = TaskId(self.next_id);
        self.next_id = self.next_id.checked_add(1).ok_or(Error::IdExhausted)?;
        self.entries.insert(
            id,
            Entry {
                snapshot: TaskSnapshot {
                    id,
                    spec,
                    state: TaskState::Registered,
                    attempt: 0,
                    result: None,
                    timed_out: false,
                    durable_safe: true,
                    started_at: None,
                    finished_at: None,
                },
                cancellation: CancellationToken::new(),
                deadline: None,
            },
        );
        Ok(id)
    }

    pub fn start<F, Fut>(&mut self, id: TaskId, task: F) -> Result<(), Error>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        if self.snapshot(id)?.state != TaskState::Registered {
            return Err(Error::InvalidState);
        }
        self.start_attempt(id, task)
    }

    fn start_attempt<F, Fut>(&mut self, id: TaskId, task: F) -> Result<(), Error>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        let now = self.now()?;
        let entry = self.entries.get(&id).ok_or(Error::UnknownTask)?;
        if !matches!(
            entry.snapshot.state,
            TaskState::Registered | TaskState::Restarting
        ) {
            return Err(Error::InvalidState);
        }
        if self.draining && entry.snapshot.spec.kind != TaskKind::GatewayInternal {
            // A previously registered request is admitted work, including children.
            if entry.snapshot.state != TaskState::Registered
                || entry.snapshot.spec.kind != TaskKind::RequestScoped
            {
                return Err(Error::Draining);
            }
        }
        let deadline = entry
            .snapshot
            .spec
            .timeout
            .map(|t| now.checked_add(t).ok_or(Error::TimeOverflow))
            .transpose()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let entry = self.entries.get_mut(&id).unwrap();
        let attempt = entry
            .snapshot
            .attempt
            .checked_add(1)
            .ok_or(Error::IdExhausted)?;
        let context = TaskContext {
            id,
            owner: entry.snapshot.spec.owner.clone(),
            attempt,
            cancellation: entry.cancellation.clone(),
        };
        // Construct the user's future inside the managed task, so factory panics
        // are join failures too. Never detach a separate monitoring task.
        let handle = self
            .joins
            .spawn_on(async move { task(context).await }, &runtime);
        self.join_ids.insert(handle.id(), id);
        entry.deadline = deadline;
        entry.snapshot.state = TaskState::Running;
        entry.snapshot.attempt = attempt;
        entry.snapshot.started_at = Some(now);
        entry.snapshot.finished_at = None;
        entry.snapshot.result = None;
        entry.snapshot.timed_out = false;
        entry.snapshot.durable_safe = false;
        Ok(())
    }

    pub fn spawn<F, Fut>(&mut self, spec: TaskSpec, task: F) -> Result<TaskId, Error>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        let id = self.register(spec)?;
        if let Err(error) = self.start(id, task) {
            self.entries.remove(&id);
            return Err(error);
        }
        Ok(id)
    }

    pub fn snapshot(&self, id: TaskId) -> Result<TaskSnapshot, Error> {
        self.entries
            .get(&id)
            .map(|e| e.snapshot.clone())
            .ok_or(Error::UnknownTask)
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.entries.values().map(|e| e.snapshot.clone()).collect()
    }

    pub fn children(&self, id: TaskId) -> Result<Vec<TaskId>, Error> {
        if !self.entries.contains_key(&id) {
            return Err(Error::UnknownTask);
        }
        Ok(self
            .entries
            .iter()
            .filter_map(|(child, e)| (e.snapshot.spec.owner.parent == Some(id)).then_some(*child))
            .collect())
    }

    fn in_tree(&self, mut candidate: TaskId, root: TaskId) -> bool {
        loop {
            if candidate == root {
                return true;
            }
            match self
                .entries
                .get(&candidate)
                .and_then(|e| e.snapshot.spec.owner.parent)
            {
                Some(parent) => candidate = parent,
                None => return false,
            }
        }
    }

    fn cancel_tree(&mut self, root: TaskId) {
        let ids: Vec<_> = self
            .entries
            .keys()
            .copied()
            .filter(|id| self.in_tree(*id, root))
            .collect();
        for id in ids {
            let entry = self.entries.get_mut(&id).unwrap();
            entry.cancellation.cancel();
            match entry.snapshot.state {
                TaskState::Registered | TaskState::Restarting => {
                    entry.snapshot.state = TaskState::Cancelled;
                    entry.snapshot.result = Some(Ok(TaskCompletion::Cancelled));
                    entry.snapshot.finished_at = Some(self.last_now);
                }
                TaskState::Running => entry.snapshot.state = TaskState::CancelRequested,
                _ => {}
            }
        }
    }

    pub fn request_cancel(&mut self, id: TaskId) -> Result<(), Error> {
        if !self.entries.contains_key(&id) {
            return Err(Error::UnknownTask);
        }
        self.cancel_tree(id);
        Ok(())
    }

    /// Cooperative full shutdown, including Gateway tasks. Follow with wait_all;
    /// returning from this method alone does not mean execution has stopped.
    pub fn request_cancel_all(&mut self) {
        self.closed = true;
        self.draining = true;
        let ids: Vec<_> = self.entries.keys().copied().collect();
        for id in ids {
            self.cancel_tree(id);
        }
    }

    fn collect(&mut self, joined: Result<(tokio::task::Id, TaskResult), tokio::task::JoinError>) {
        // Completion times are observation times in the injected clock domain.
        // A join may wake a waiter long after its preceding poll.
        let _ = self.now();
        let (join_id, result) = match joined {
            Ok(pair) => pair,
            Err(error) => (
                error.id(),
                Err(if error.is_panic() {
                    TaskFailure::Panicked
                } else {
                    TaskFailure::Aborted
                }),
            ),
        };
        let id = self.join_ids.remove(&join_id).expect("managed join id");
        let entry = self.entries.get_mut(&id).expect("managed task record");
        entry.deadline = None;
        entry.snapshot.state = match result {
            Ok(TaskCompletion::Cancelled) => TaskState::Cancelled,
            Ok(_) => TaskState::Completed,
            Err(_) => TaskState::Failed,
        };
        entry.snapshot.result = Some(result);
        entry.snapshot.durable_safe = matches!(
            result,
            Ok(TaskCompletion::Completed | TaskCompletion::Checkpointed)
        );
        entry.snapshot.finished_at = Some(self.last_now);
        // A failed parent cannot leave an orphan performing side effects while
        // its replacement starts. Successful parents may leave tracked children.
        if result.is_err() || result == Ok(TaskCompletion::Cancelled) {
            let children = self.children(id).expect("managed parent");
            for child in children {
                self.cancel_tree(child);
            }
        }
    }

    /// Reap ready joins, then expire deadlines. Completion already available at
    /// the deadline wins; a cancellation request is never proof of termination.
    pub fn poll(&mut self) -> Result<(), Error> {
        let now = self.now();
        while let Some(joined) = self.joins.try_join_next_with_id() {
            self.collect(joined);
        }
        now?;
        if self.clock_failed {
            return Err(Error::ClockRegressed);
        }
        let now = self.last_now;
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| {
                (e.deadline.is_some_and(|deadline| deadline <= now) && !e.snapshot.timed_out)
                    .then_some(*id)
            })
            .collect();
        for id in expired {
            self.entries.get_mut(&id).unwrap().snapshot.timed_out = true;
            self.cancel_tree(id);
        }
        if self.draining && !self.stopping_residents && self.requests_finished() {
            self.stopping_residents = true;
            let ids: Vec<_> = self
                .entries
                .iter()
                .filter_map(|(id, e)| {
                    matches!(
                        e.snapshot.spec.kind,
                        TaskKind::BackgroundCancellable | TaskKind::DurableJob
                    )
                    .then_some(*id)
                })
                .collect();
            for id in ids {
                self.cancel_tree(id);
            }
        }
        Ok(())
    }

    fn requests_finished(&self) -> bool {
        self.entries.values().all(|e| {
            e.snapshot.spec.kind != TaskKind::RequestScoped || e.snapshot.state.is_terminal()
        })
    }

    pub fn begin_core_drain(&mut self) {
        self.draining = true;
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn core_tasks_finished(&self) -> bool {
        self.draining
            && self.entries.values().all(|e| {
                e.snapshot.spec.kind == TaskKind::GatewayInternal || e.snapshot.state.is_terminal()
            })
    }

    pub fn metrics(&self) -> Metrics {
        let mut metrics = Metrics {
            pending_joins: self.joins.len(),
            ..Metrics::default()
        };
        for entry in self.entries.values() {
            let counter = match entry.snapshot.state {
                TaskState::Registered => &mut metrics.registered,
                TaskState::Running => &mut metrics.running,
                TaskState::CancelRequested => &mut metrics.cancel_requested,
                TaskState::Completed => &mut metrics.completed,
                TaskState::Failed => &mut metrics.failed,
                TaskState::Cancelled => &mut metrics.cancelled,
                TaskState::Restarting => &mut metrics.restarting,
            };
            *counter += 1;
            metrics.timed_out += usize::from(entry.snapshot.timed_out);
        }
        metrics
    }

    /// Forget only a joined leaf. Parents remain to preserve descendant ownership.
    pub fn forget(&mut self, id: TaskId) -> Result<TaskSnapshot, Error> {
        let entry = self.entries.get(&id).ok_or(Error::UnknownTask)?;
        if !entry.snapshot.state.is_terminal()
            || !self.children(id)?.is_empty()
            || (entry.snapshot.spec.kind == TaskKind::DurableJob && !entry.snapshot.durable_safe)
        {
            return Err(Error::InvalidState);
        }
        Ok(self.entries.remove(&id).unwrap().snapshot)
    }

    /// A trusted generation adapter asserts persisted recovery/checkpoint safety
    /// for a stopped durable job. This module does not perform that persistence.
    pub fn confirm_durable_safe(&mut self, id: TaskId) -> Result<(), Error> {
        let entry = self.entries.get_mut(&id).ok_or(Error::UnknownTask)?;
        if entry.snapshot.spec.kind != TaskKind::DurableJob || !entry.snapshot.state.is_terminal() {
            return Err(Error::InvalidState);
        }
        entry.snapshot.durable_safe = true;
        Ok(())
    }

    fn tree_finished(&self, root: TaskId) -> bool {
        self.entries
            .iter()
            .all(|(id, e)| !self.in_tree(*id, root) || e.snapshot.state.is_terminal())
    }

    async fn wait_progress(&mut self, wake_at: Option<Duration>) {
        let deadline = if self.clock_failed {
            None
        } else {
            self.entries
                .values()
                .filter(|e| !e.snapshot.timed_out)
                .filter_map(|e| e.deadline)
                .chain(wake_at)
                .min()
        };
        match deadline {
            Some(deadline) => tokio::select! {
                joined = self.joins.join_next_with_id(), if !self.joins.is_empty() => {
                    if let Some(joined) = joined { self.collect(joined); }
                }
                () = self.clock.sleep_until(deadline) => {}
            },
            None => {
                if let Some(joined) = self.joins.join_next_with_id().await {
                    self.collect(joined);
                } else {
                    std::future::pending::<()>().await;
                }
            }
        }
    }

    /// Wait for the task AND its entire descendant tree. This future is cancel
    /// safe. Unstarted registrations must be started or cancelled by the owner.
    pub async fn wait(&mut self, id: TaskId) -> Result<TaskSnapshot, Error> {
        if !self.entries.contains_key(&id) {
            return Err(Error::UnknownTask);
        }
        loop {
            match self.poll() {
                Err(Error::ClockRegressed) => {}
                other => other?,
            }
            if self.tree_finished(id) {
                return self.snapshot(id);
            }
            self.wait_progress(None).await;
        }
    }

    /// Wait for every task, including GatewayInternal. Does not close admission
    /// or request cancellation; callers choose those policies explicitly.
    pub async fn wait_all(&mut self) -> Result<(), Error> {
        loop {
            match self.poll() {
                Err(Error::ClockRegressed) => {}
                other => other?,
            }
            if self
                .entries
                .values()
                .all(|e| e.snapshot.state.is_terminal())
            {
                return Ok(());
            }
            self.wait_progress(None).await;
        }
    }
}

/// The result receiver and tracked identifiers for a Worker request tree.
/// Dropping this value does not detach or cancel either managed task.
pub struct WorkerRequest<T> {
    root: TaskId,
    child: TaskId,
    output: tokio::sync::oneshot::Receiver<T>,
    _lifetime: Arc<()>,
}

impl<T> WorkerRequest<T> {
    pub fn root_id(&self) -> TaskId {
        self.root
    }

    pub fn child_id(&self) -> TaskId {
        self.child
    }
}

/// Generation adapter for the Worker's serial command execution slot. Every
/// admitted request has a RequestScoped root and a managed handler child. The
/// adapter retains their joins if its caller cancels a wait; begin_drain closes
/// root admission before wait_drained waits for every retained descendant.
///
/// This is task settlement only. The Worker must separately flush IPC, save
/// state and settle Gateway/REST outcomes before retiring its generation.
pub struct WorkerTasks {
    manager: TaskManager,
    bot_id: BotId,
    shard_id: u32,
    next_request: u128,
    requests: BTreeMap<TaskId, (TaskId, std::sync::Weak<()>)>,
}

impl WorkerTasks {
    const CAPACITY: usize = 128;

    pub fn new(bot_id: BotId, shard_id: u32) -> Result<Self, Error> {
        Ok(Self {
            manager: TaskManager::new(MonotonicClock::default(), bot_id, shard_id, Self::CAPACITY)?,
            bot_id,
            shard_id,
            next_request: 1,
            requests: BTreeMap::new(),
        })
    }

    fn reap_abandoned(&mut self) -> Result<(), Error> {
        self.manager.poll()?;
        let abandoned: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(root, (child, lifetime))| {
                (lifetime.upgrade().is_none() && self.manager.tree_finished(*root))
                    .then_some((*root, *child))
            })
            .collect();
        for (root, child) in abandoned {
            self.manager.forget(child)?;
            self.manager.forget(root)?;
            self.requests.remove(&root);
        }
        Ok(())
    }

    /// Register the request and handler atomically with respect to this adapter's
    /// admission gate. Actual future execution starts only after registration.
    pub fn start_request<F, T>(&mut self, future: F) -> Result<WorkerRequest<T>, Error>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.reap_abandoned()?;
        if self.manager.is_draining() {
            return Err(Error::Draining);
        }
        // Reserve two records before spawning the root, avoiding a capacity
        // failure that would otherwise leave it waiting for an absent child.
        if self.manager.entries.len() > Self::CAPACITY - 2 {
            return Err(Error::Capacity);
        }
        let request = self.next_request;
        self.next_request = request.checked_add(1).ok_or(Error::IdExhausted)?;
        let owner = TaskOwner {
            bot_id: self.bot_id,
            shard_id: self.shard_id,
            cog: None,
            request_id: Some(RequestId(request.to_be_bytes())),
            parent: None,
        };
        let spec = TaskSpec {
            owner,
            kind: TaskKind::RequestScoped,
            timeout: None,
        };
        let (settled, settlement) = tokio::sync::oneshot::channel();
        let root = self.manager.spawn(spec.clone(), move |_| async move {
            settlement.await.map_err(|_| TaskFailure::Application)?;
            Ok(TaskCompletion::Completed)
        })?;
        let mut child_spec = spec;
        child_spec.owner.parent = Some(root);
        let (output, received) = tokio::sync::oneshot::channel();
        let child = match self.manager.spawn(child_spec, move |_| async move {
            let result = future.await;
            // The completion signal is sent even when the caller no longer
            // wants the value. A dropped receiver never strands the root.
            let _ = output.send(result);
            let _ = settled.send(());
            Ok(TaskCompletion::Completed)
        }) {
            Ok(child) => child,
            Err(error) => {
                self.manager.request_cancel(root)?;
                return Err(error);
            }
        };
        let lifetime = Arc::new(());
        self.requests
            .insert(root, (child, Arc::downgrade(&lifetime)));
        Ok(WorkerRequest {
            root,
            child,
            output: received,
            _lifetime: lifetime,
        })
    }

    pub async fn finish_request<T>(&mut self, request: WorkerRequest<T>) -> Result<T, Error> {
        if !self
            .requests
            .get(&request.root)
            .is_some_and(|(child, lifetime)| {
                *child == request.child && lifetime.ptr_eq(&Arc::downgrade(&request._lifetime))
            })
        {
            return Err(Error::WrongOwner);
        }
        self.manager.wait(request.root).await?;
        let child = self.manager.forget(request.child)?;
        let root = self.manager.forget(request.root)?;
        self.requests.remove(&request.root);
        if child.result != Some(Ok(TaskCompletion::Completed))
            || root.result != Some(Ok(TaskCompletion::Completed))
        {
            return Err(Error::TaskFailed);
        }
        request.output.await.map_err(|_| Error::TaskFailed)
    }

    pub async fn request<F, T>(&mut self, future: F) -> Result<T, Error>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let request = self.start_request(future)?;
        self.finish_request(request).await
    }

    pub fn begin_drain(&mut self) {
        self.manager.begin_core_drain();
    }

    pub fn is_draining(&self) -> bool {
        self.manager.is_draining()
    }

    pub async fn wait_drained(&mut self) -> Result<(), Error> {
        if !self.manager.is_draining() {
            return Err(Error::InvalidState);
        }
        self.manager.wait_all().await
    }

    /// Reopen only after all retained joins have actually completed. In-flight
    /// request handles must be finished before this resets the task registry.
    /// Logical request identifiers continue monotonically across aborted drains.
    pub fn resume(&mut self) -> Result<(), Error> {
        self.reap_abandoned()?;
        if !self.manager.is_draining()
            || !self.manager.core_tasks_finished()
            || self.manager.metrics().pending_joins != 0
            || !self.requests.is_empty()
        {
            return Err(Error::InvalidState);
        }
        self.manager = TaskManager::new(
            MonotonicClock::default(),
            self.bot_id,
            self.shard_id,
            Self::CAPACITY,
        )?;
        Ok(())
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.manager.snapshots()
    }

    pub fn metrics(&self) -> Metrics {
        self.manager.metrics()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    /// Lifetime cap, never reset by successful attempts or circuit cooldown.
    pub max_restarts: u32,
    /// Sliding interval (now - window, now]; the initial start is not a restart.
    pub max_restarts_in_window: u32,
    pub window: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub circuit_breaker_after: u32,
    pub circuit_open_for: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 8,
            max_restarts_in_window: 3,
            window: Duration::from_secs(60),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            circuit_breaker_after: 3,
            circuit_open_for: Duration::from_secs(60),
        }
    }
}

impl RestartPolicy {
    // Shared with the OS child recovery policy; keep task and child delay
    // arithmetic identical without changing the task supervisor's semantics.
    pub(crate) fn backoff(self, restarts: u32) -> Duration {
        let mut delay = self.initial_backoff;
        // At most the bit width of Duration is needed, even with a huge budget.
        for _ in 0..restarts {
            if delay >= self.max_backoff {
                break;
            }
            delay = delay.saturating_mul(2).min(self.max_backoff);
        }
        delay
    }

    pub(crate) fn validate(self) -> Result<(), Error> {
        if self.max_restarts_in_window == 0
            || self.window.is_zero()
            || self.initial_backoff.is_zero()
            || self.max_backoff < self.initial_backoff
            || self.circuit_breaker_after == 0
            || self.circuit_open_for.is_zero()
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitState {
    Closed,
    Open { until: Duration },
    HalfOpen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestartStatus {
    pub restarts: u32,
    pub failures: u32,
    pub retry_at: Option<Duration>,
    pub circuit: CircuitState,
    pub exhausted: bool,
}

struct Supervised {
    factory: TaskFactory,
    policy: RestartPolicy,
    status: RestartStatus,
    handled_attempt: u32,
    recent: VecDeque<Duration>,
}

/// External evidence is deliberately required on EACH drain check. The adapter
/// must obtain it after closing admission and after the final managed task exits;
/// cached evidence from before drain is not sufficient. Unknown REST outcomes
/// must make api_results_settled false. No boolean here proves external durability.
#[derive(Clone, Copy, Debug, Default)]
pub struct DrainEvidence {
    pub api_results_settled: bool,
    pub state_saved: bool,
    pub ipc_flushed: bool,
    pub durable_jobs_safe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainStatus {
    Accepting,
    WaitingRequests,
    StoppingResidents,
    WaitingExternal,
    Complete,
}

pub struct Supervisor<C = MonotonicClock> {
    manager: TaskManager<C>,
    supervised: BTreeMap<TaskId, Supervised>,
}

impl<C: Clock> Supervisor<C> {
    pub fn new(manager: TaskManager<C>) -> Self {
        Self {
            manager,
            supervised: BTreeMap::new(),
        }
    }
    pub fn manager(&self) -> &TaskManager<C> {
        &self.manager
    }
    /// Register/spawn/cancel through the manager as needed. Drive Supervisor::poll
    /// for retries; manager.wait does not execute supervisor restart policy.
    pub fn manager_mut(&mut self) -> &mut TaskManager<C> {
        &mut self.manager
    }

    /// Only background and Gateway internal tasks may automatically restart.
    /// Requests may have emitted side effects; durable jobs require an external
    /// checkpoint/idempotency/lease protocol and are never replayed here.
    pub fn spawn(
        &mut self,
        spec: TaskSpec,
        policy: RestartPolicy,
        factory: TaskFactory,
    ) -> Result<TaskId, Error> {
        // manager_mut permits collecting/forgetting tasks without driving this
        // supervisor. Release old factories before admitting another task too.
        self.supervised
            .retain(|id, _| self.manager.entries.contains_key(id));
        policy.validate()?;
        if !matches!(
            spec.kind,
            TaskKind::BackgroundCancellable | TaskKind::GatewayInternal
        ) && policy.max_restarts != 0
        {
            return Err(Error::InvalidConfig);
        }
        let task = factory.clone();
        let id = self.manager.spawn(spec, move |context| task(context))?;
        self.supervised.insert(
            id,
            Supervised {
                factory,
                policy,
                handled_attempt: 0,
                recent: VecDeque::new(),
                status: RestartStatus {
                    restarts: 0,
                    failures: 0,
                    retry_at: None,
                    circuit: CircuitState::Closed,
                    exhausted: false,
                },
            },
        );
        Ok(id)
    }

    pub fn restart_status(&self, id: TaskId) -> Result<RestartStatus, Error> {
        self.supervised
            .get(&id)
            .map(|s| s.status)
            .ok_or(Error::UnknownTask)
    }

    pub fn poll(&mut self) -> Result<(), Error> {
        self.manager.poll()?;
        self.supervised
            .retain(|id, _| self.manager.entries.contains_key(id));
        let now = self.manager.last_now;
        for (&id, supervised) in &mut self.supervised {
            let Some(entry) = self.manager.entries.get(&id) else {
                continue;
            };
            let snapshot = &entry.snapshot;
            // Explicit cancellation (also after failure), timeout and Core drain
            // suppress restart. Gateway tasks are independent of Core drain.
            let stopped = entry.cancellation.is_cancelled()
                || snapshot.timed_out
                || (self.manager.draining && snapshot.spec.kind != TaskKind::GatewayInternal);
            if stopped {
                supervised.status.retry_at = None;
                if snapshot.state == TaskState::Restarting {
                    self.manager.cancel_tree(id);
                }
                continue;
            }
            if snapshot.state == TaskState::Completed {
                supervised.status.circuit = CircuitState::Closed;
                supervised.status.failures = 0;
            }
            if snapshot.state == TaskState::Failed && supervised.handled_attempt != snapshot.attempt
            {
                let failures = supervised.status.failures.saturating_add(1);
                if supervised.status.restarts >= supervised.policy.max_restarts {
                    supervised.handled_attempt = snapshot.attempt;
                    supervised.status.failures = failures;
                    supervised.status.exhausted = true;
                    continue;
                }
                let backoff = supervised.policy.backoff(supervised.status.restarts);
                let mut retry_at = now.checked_add(backoff).ok_or(Error::TimeOverflow)?;
                if failures >= supervised.policy.circuit_breaker_after {
                    let until = now
                        .checked_add(supervised.policy.circuit_open_for)
                        .ok_or(Error::TimeOverflow)?;
                    supervised.status.circuit = CircuitState::Open { until };
                    retry_at = retry_at.max(until);
                }
                supervised.handled_attempt = snapshot.attempt;
                supervised.status.failures = failures;
                supervised.status.retry_at = Some(retry_at);
                self.manager.entries.get_mut(&id).unwrap().snapshot.state = TaskState::Restarting;
            }
            let ready = supervised.status.retry_at.is_some_and(|at| now >= at);
            if !ready || !self.manager.tree_finished_except_restart(id) {
                continue;
            }
            while supervised
                .recent
                .front()
                .is_some_and(|at| now.saturating_sub(*at) >= supervised.policy.window)
            {
                supervised.recent.pop_front();
            }
            if supervised.recent.len() >= supervised.policy.max_restarts_in_window as usize {
                supervised.status.retry_at = Some(
                    supervised.recent[0]
                        .checked_add(supervised.policy.window)
                        .ok_or(Error::TimeOverflow)?,
                );
                continue;
            }
            let task = supervised.factory.clone();
            self.manager.entries.get_mut(&id).unwrap().cancellation = CancellationToken::new();
            self.manager
                .start_attempt(id, move |context| task(context))?;
            supervised.recent.push_back(now);
            supervised.status.restarts += 1;
            supervised.status.retry_at = None;
            if matches!(supervised.status.circuit, CircuitState::Open { .. }) {
                supervised.status.circuit = CircuitState::HalfOpen;
            }
        }
        Ok(())
    }

    pub fn begin_core_drain(&mut self) {
        self.manager.begin_core_drain();
    }

    async fn wait_progress(&mut self) {
        // Once a retry is due, an unfinished descendant must be joined first.
        // Avoid repeatedly waking on a past retry deadline in that case.
        let retry_at = self
            .supervised
            .iter()
            .filter(|(id, _)| self.manager.tree_finished_except_restart(**id))
            .filter_map(|(_, s)| s.status.retry_at)
            .min();
        self.manager.wait_progress(retry_at).await;
    }

    /// Drives retries, timeout cancellation and joins until this task's entire
    /// subtree ends. Unlike manager.wait, this includes supervised retry policy.
    pub async fn wait(&mut self, id: TaskId) -> Result<TaskSnapshot, Error> {
        self.manager.snapshot(id)?;
        loop {
            match self.poll() {
                Err(Error::ClockRegressed) => {}
                other => other?,
            }
            if self.manager.tree_finished(id) {
                return self.manager.snapshot(id);
            }
            self.wait_progress().await;
        }
    }

    /// Includes Gateway tasks. For graceful shutdown request cancellation first;
    /// for a Core update use begin_core_drain + drain_status instead.
    pub async fn wait_all(&mut self) -> Result<(), Error> {
        loop {
            match self.poll() {
                Err(Error::ClockRegressed) => {}
                other => other?,
            }
            if self
                .manager
                .entries
                .values()
                .all(|e| e.snapshot.state.is_terminal())
            {
                return Ok(());
            }
            self.wait_progress().await;
        }
    }

    pub fn drain_status(&self, evidence: DrainEvidence) -> DrainStatus {
        if !self.manager.draining {
            return DrainStatus::Accepting;
        }
        if !self.manager.requests_finished() {
            return DrainStatus::WaitingRequests;
        }
        if !self.manager.core_tasks_finished() {
            return DrainStatus::StoppingResidents;
        }
        let durable_safe = self
            .manager
            .entries
            .values()
            .filter(|e| e.snapshot.spec.kind == TaskKind::DurableJob)
            .all(|e| e.snapshot.durable_safe)
            || evidence.durable_jobs_safe;
        if evidence.api_results_settled
            && evidence.state_saved
            && evidence.ipc_flushed
            && durable_safe
        {
            DrainStatus::Complete
        } else {
            DrainStatus::WaitingExternal
        }
    }
}

impl<C: Clock> TaskManager<C> {
    fn tree_finished_except_restart(&self, root: TaskId) -> bool {
        self.entries.iter().all(|(id, e)| {
            *id == root
                || !self.in_tree(*id, root)
                || (e.snapshot.state.is_terminal()
                    && (e.snapshot.spec.kind != TaskKind::DurableJob || e.snapshot.durable_safe))
        })
    }
}
