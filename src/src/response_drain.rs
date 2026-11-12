//! Local bookkeeping for response-bearing work during a generation switch.
//! This is not a durable journal and is not yet connected to the Gateway.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Pending,
    Unknown,
}

struct State {
    draining: bool,
    operations: BTreeMap<u64, Status>,
}
struct Inner {
    state: Mutex<State>,
    changes: watch::Sender<u64>,
    capacity: usize,
}

/// Tracks response-bearing requests for a single generation, with bounded memory.
#[derive(Clone)]
pub struct ResponseDrain(Arc<Inner>);

/// Dropping without an explicit result conservatively records an unknown outcome.
#[must_use]
pub struct ResponseOperation {
    inner: Arc<Inner>,
    id: u64,
    finished: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BeginError {
    Draining,
    Full,
    IdExhausted,
}

impl Inner {
    fn signal(&self) {
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
    }
}

impl ResponseDrain {
    pub fn new(capacity: usize) -> Self {
        let (changes, _) = watch::channel(0);
        Self(Arc::new(Inner {
            state: Mutex::new(State {
                draining: false,
                operations: BTreeMap::new(),
            }),
            changes,
            capacity,
        }))
    }

    /// Reserve before admitting work to the generation, not after sending HTTP.
    pub fn begin(&self) -> Result<ResponseOperation, BeginError> {
        let mut state = self.0.state.lock().unwrap();
        if state.draining {
            return Err(BeginError::Draining);
        }
        if state.operations.len() >= self.0.capacity {
            return Err(BeginError::Full);
        }
        // IDs must not collide across workers, otherwise a mistyped worker index
        // could reconcile a different outstanding response with the same ID.
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let mut id = NEXT_ID.load(Ordering::Relaxed);
        loop {
            let next = id.checked_add(1).ok_or(BeginError::IdExhausted)?;
            match NEXT_ID.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => id = observed,
            }
        }
        state.operations.insert(id, Status::Pending);
        self.0.signal();
        Ok(ResponseOperation {
            inner: self.0.clone(),
            id,
            finished: false,
        })
    }

    /// Close admission atomically with respect to begin.
    pub fn start_drain(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.draining = true;
        self.0.signal();
    }

    /// Reopen a retained generation after a failed switch. Routing and this call
    /// must be serialized externally; discard waiters from the earlier drain attempt.
    pub fn resume(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.draining = false;
        self.0.signal();
    }

    /// Wait for all operations to have a known or explicitly unknown outcome.
    /// Unknown IDs must be reconciled; an empty list alone is not full Core drain.
    /// Callers should impose a deadline and must not interpret timeout as success.
    pub async fn wait_settled(&self) -> Vec<u64> {
        let mut changes = self.0.changes.subscribe();
        loop {
            changes.borrow_and_update();
            {
                let state = self.0.state.lock().unwrap();
                if state.draining && !state.operations.values().any(|v| *v == Status::Pending) {
                    return state.operations.keys().copied().collect();
                }
            }
            // `self` keeps the sender alive.
            changes
                .changed()
                .await
                .expect("response tracker sender alive");
        }
    }

    /// Snapshot of unresolved IDs. Pending operations are intentionally omitted.
    pub fn unknown_ids(&self) -> Vec<u64> {
        let state = self.0.state.lock().unwrap();
        state
            .operations
            .iter()
            .filter_map(|(id, status)| (*status == Status::Unknown).then_some(*id))
            .collect()
    }

    /// Forget an unknown outcome only after an external reconciliation has resolved it.
    pub fn resolve_unknown(&self, id: u64) -> bool {
        let mut state = self.0.state.lock().unwrap();
        if state.operations.get(&id) != Some(&Status::Unknown) {
            return false;
        }
        state.operations.remove(&id);
        self.0.signal();
        true
    }
}

impl ResponseOperation {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Use only when the response's outcome has been confirmed or no response was sent.
    pub fn confirm(mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.operations.remove(&self.id);
        self.finished = true;
        self.inner.signal();
    }

    /// Use for a timeout or any other outcome that cannot be established.
    pub fn unknown(self) {
        drop(self);
    }
}

impl Drop for ResponseOperation {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self.inner.state.lock().unwrap();
            if let Some(status) = state.operations.get_mut(&self.id) {
                *status = Status::Unknown;
            }
            self.inner.signal();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn drain_waits_for_http_completion_and_reports_unknown() {
        let tracker = ResponseDrain::new(2);
        let confirmed = tracker.begin().unwrap();
        let unknown = tracker.begin().unwrap();
        let id = unknown.id();
        tracker.start_drain();
        assert!(matches!(tracker.begin(), Err(BeginError::Draining)));
        confirmed.confirm();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tracker.wait_settled())
                .await
                .is_err()
        );
        unknown.unknown();
        assert_eq!(tracker.wait_settled().await, vec![id]);
        assert!(tracker.resolve_unknown(id));
        assert!(tracker.wait_settled().await.is_empty());
        assert!(!tracker.resolve_unknown(id));
    }

    #[tokio::test]
    async fn cancellation_is_unknown_and_capacity_is_bounded() {
        let tracker = ResponseDrain::new(1);
        let op = tracker.begin().unwrap();
        let id = op.id();
        assert!(matches!(tracker.begin(), Err(BeginError::Full)));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _op = op;
            let _ = tx.send(());
            std::future::pending::<()>().await
        });
        rx.await.unwrap();
        task.abort();
        let _ = task.await;
        assert!(matches!(tracker.begin(), Err(BeginError::Full)));
        tracker.start_drain();
        assert_eq!(tracker.wait_settled().await, vec![id]);
    }
    #[tokio::test]
    async fn aborted_switch_can_resume_admission_without_losing_work() {
        let tracker = ResponseDrain::new(2);
        let original = tracker.begin().unwrap();
        tracker.start_drain();
        assert!(matches!(tracker.begin(), Err(BeginError::Draining)));
        tracker.resume();
        let resumed = tracker.begin().unwrap();
        original.confirm();
        tracker.start_drain();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tracker.wait_settled())
                .await
                .is_err()
        );
        resumed.confirm();
        assert!(tracker.wait_settled().await.is_empty());
    }
}
