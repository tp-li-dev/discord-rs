//! Experimental transactional quiesce contract, independent of the legacy Cog API.
//! Hooks must be cancellation-safe and `abort` must undo a partially failed prepare.
//! Until IPC coordination and state transfer exist this must not be treated as Core drain.
use crate::CommandFuture;
use std::sync::Arc;

pub trait Quiesce: Send + Sync + 'static {
    /// Stop starting work while retaining enough state to resume.
    fn prepare(&self) -> CommandFuture<'_>;
    /// Undo prepare, including one that returned an error; it must be idempotent.
    fn abort(&self) -> CommandFuture<'_>;
    /// Finalize after switching; failure leaves the generation unsafe to resume.
    fn commit(&self) -> CommandFuture<'_>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Active,
    Preparing,
    Prepared,
    Aborting,
    Committing,
    Committed,
    Indeterminate,
}
#[derive(Debug, PartialEq, Eq)]
pub enum LifecycleError {
    InvalidPhase,
    PrepareFailed,
    AbortFailed,
    CommitFailed,
}

pub struct Lifecycle {
    hooks: Vec<Arc<dyn Quiesce>>,
    phase: Phase,
    attempted: usize,
}
impl Lifecycle {
    pub fn new(hooks: Vec<Arc<dyn Quiesce>>) -> Self {
        Self {
            hooks,
            phase: Phase::Active,
            attempted: 0,
        }
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// A dropped future leaves the phase non-active; callers must explicitly abort.
    pub async fn prepare(&mut self) -> Result<(), LifecycleError> {
        if self.phase != Phase::Active {
            return Err(LifecycleError::InvalidPhase);
        }
        self.phase = Phase::Preparing;
        for hook in &self.hooks {
            self.attempted += 1;
            if hook.prepare().await.is_err() {
                return match self.abort().await {
                    Ok(()) => Err(LifecycleError::PrepareFailed),
                    Err(_) => Err(LifecycleError::AbortFailed),
                };
            }
        }
        self.phase = Phase::Prepared;
        Ok(())
    }

    /// May be called after a cancelled prepare or abort; retried hooks must be idempotent.
    pub async fn abort(&mut self) -> Result<(), LifecycleError> {
        if !matches!(
            self.phase,
            Phase::Preparing | Phase::Prepared | Phase::Aborting
        ) {
            return Err(LifecycleError::InvalidPhase);
        }
        self.phase = Phase::Aborting;
        while self.attempted > 0 {
            let index = self.attempted - 1;
            if self.hooks[index].abort().await.is_err() {
                self.phase = Phase::Indeterminate;
                return Err(LifecycleError::AbortFailed);
            }
            self.attempted -= 1;
        }
        self.phase = Phase::Active;
        Ok(())
    }

    /// Commit runs in reverse preparation order. It cannot be rolled back.
    /// A cancelled commit remains Committing and is not eligible for resume.
    pub async fn commit(&mut self) -> Result<(), LifecycleError> {
        if self.phase != Phase::Prepared {
            return Err(LifecycleError::InvalidPhase);
        }
        self.phase = Phase::Committing;
        for hook in self.hooks.iter().rev() {
            if hook.commit().await.is_err() {
                self.phase = Phase::Indeterminate;
                return Err(LifecycleError::CommitFailed);
            }
        }
        self.phase = Phase::Committed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Hook {
        name: &'static str,
        fail_prepare: bool,
        fail_abort: bool,
        fail_commit: bool,
        log: Arc<Mutex<Vec<String>>>,
    }
    impl Quiesce for Hook {
        fn prepare(&self) -> CommandFuture<'_> {
            Box::pin(async move {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("prepare {}", self.name));
                if self.fail_prepare {
                    return Err("prepare failed".into());
                }
                Ok(())
            })
        }
        fn abort(&self) -> CommandFuture<'_> {
            Box::pin(async move {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("abort {}", self.name));
                if self.fail_abort {
                    return Err("abort failed".into());
                }
                Ok(())
            })
        }
        fn commit(&self) -> CommandFuture<'_> {
            Box::pin(async move {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("commit {}", self.name));
                if self.fail_commit {
                    return Err("commit failed".into());
                }
                Ok(())
            })
        }
    }
    fn hook(
        name: &'static str,
        log: &Arc<Mutex<Vec<String>>>,
        p: bool,
        a: bool,
        c: bool,
    ) -> Arc<dyn Quiesce> {
        Arc::new(Hook {
            name,
            fail_prepare: p,
            fail_abort: a,
            fail_commit: c,
            log: log.clone(),
        })
    }
    #[tokio::test]
    async fn prepare_failure_aborts_in_reverse_including_failed_hook() {
        let log = Arc::new(Mutex::new(vec![]));
        let mut lifecycle = Lifecycle::new(vec![
            hook("a", &log, false, false, false),
            hook("b", &log, true, false, false),
        ]);
        assert_eq!(
            lifecycle.prepare().await,
            Err(LifecycleError::PrepareFailed)
        );
        assert_eq!(lifecycle.phase(), Phase::Active);
        assert_eq!(
            *log.lock().unwrap(),
            ["prepare a", "prepare b", "abort b", "abort a"]
        );
    }
    #[tokio::test]
    async fn abort_or_commit_failure_is_not_safe_to_resume() {
        let log = Arc::new(Mutex::new(vec![]));
        let mut abort = Lifecycle::new(vec![hook("a", &log, true, true, false)]);
        assert_eq!(abort.prepare().await, Err(LifecycleError::AbortFailed));
        assert_eq!(abort.phase(), Phase::Indeterminate);
        assert_eq!(abort.prepare().await, Err(LifecycleError::InvalidPhase));
        let mut commit = Lifecycle::new(vec![hook("b", &log, false, false, true)]);
        commit.prepare().await.unwrap();
        assert_eq!(commit.commit().await, Err(LifecycleError::CommitFailed));
        assert_eq!(commit.phase(), Phase::Indeterminate);
        assert_eq!(commit.abort().await, Err(LifecycleError::InvalidPhase));
    }
    struct PendingHook;
    impl Quiesce for PendingHook {
        fn prepare(&self) -> CommandFuture<'_> {
            Box::pin(std::future::pending())
        }
        fn abort(&self) -> CommandFuture<'_> {
            Box::pin(async { Ok(()) })
        }
        fn commit(&self) -> CommandFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }
    #[tokio::test]
    async fn cancelled_prepare_requires_explicit_abort() {
        let mut lifecycle = Lifecycle::new(vec![Arc::new(PendingHook)]);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        {
            let mut future = Box::pin(lifecycle.prepare());
            assert!(matches!(
                std::future::Future::poll(future.as_mut(), &mut context),
                std::task::Poll::Pending
            ));
        }
        assert_eq!(lifecycle.phase(), Phase::Preparing);
        assert_eq!(lifecycle.prepare().await, Err(LifecycleError::InvalidPhase));
        lifecycle.abort().await.unwrap();
        assert_eq!(lifecycle.phase(), Phase::Active);
    }
}
