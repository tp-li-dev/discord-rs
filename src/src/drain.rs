//! Tracks work admitted before draining. This is only the task portion of a Core drain;
//! REST acknowledgements, state persistence, and IPC flushing require separate checks.
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct State {
    draining: bool,
    active: usize,
}

struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<State>,
}

/// Admission and completion tracking for a single generation of work.
#[derive(Clone)]
pub struct DrainTracker(Arc<Inner>);

/// A live unit of work. Dropping it records completion, including on cancellation or panic.
#[must_use]
pub struct WorkPermit(Arc<Inner>);

impl Default for DrainTracker {
    fn default() -> Self {
        let state = State::default();
        let (changed, _) = watch::channel(state);
        Self(Arc::new(Inner {
            state: Mutex::new(state),
            changed,
        }))
    }
}

impl DrainTracker {
    /// Admit a new request unless draining has begun.
    pub fn try_start(&self) -> Option<WorkPermit> {
        let mut state = self.0.state.lock().unwrap();
        if state.draining {
            return None;
        }
        state.active = state.active.checked_add(1).expect("work count overflow");
        self.0.changed.send_replace(*state);
        Some(WorkPermit(self.0.clone()))
    }

    /// Atomically stop admission. Repeated calls are harmless.
    pub fn start_drain(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.draining = true;
        self.0.changed.send_replace(*state);
    }

    /// Wait until draining has begun and every tracked unit has completed.
    /// Callers supply a timeout and decide their own cancellation policy.
    pub async fn wait_drained(&self) {
        let mut receiver = self.0.changed.subscribe();
        loop {
            let state = *receiver.borrow_and_update();
            if state.draining && state.active == 0 {
                return;
            }
            // The sender stays alive while self exists.
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl WorkPermit {
    /// Track a child of admitted work, even after draining begins.
    /// The parent permit must remain live while the child is registered.
    pub fn child(&self) -> WorkPermit {
        let mut state = self.0.state.lock().unwrap();
        state.active = state.active.checked_add(1).expect("work count overflow");
        self.0.changed.send_replace(*state);
        WorkPermit(self.0.clone())
    }
}

impl Drop for WorkPermit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        self.0.changed.send_replace(*state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn drain_rejects_new_work_and_waits_for_children() {
        let tracker = DrainTracker::default();
        let parent = tracker.try_start().unwrap();
        tracker.start_drain();
        assert!(tracker.try_start().is_none());
        let child = parent.child();
        drop(parent);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tracker.wait_drained())
                .await
                .is_err()
        );
        drop(child);
        tokio::time::timeout(Duration::from_secs(1), tracker.wait_drained())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn empty_and_concurrent_drains_complete() {
        let tracker = DrainTracker::default();
        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_drained().await }
        });
        tracker.start_drain();
        tracker.start_drain();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancellation_releases_the_permit() {
        let tracker = DrainTracker::default();
        let permit = tracker.try_start().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _permit = permit;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        tracker.start_drain();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tracker.wait_drained())
                .await
                .is_err()
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), tracker.wait_drained())
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admission_and_drain_account_for_every_accepted_task() {
        let tracker = DrainTracker::default();
        let start = std::sync::Arc::new(tokio::sync::Barrier::new(33));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let tracker = tracker.clone();
            let start = start.clone();
            let release = release.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let permit = tracker.try_start();
                if permit.is_some() {
                    release.notified().await;
                }
                drop(permit);
            }));
        }
        start.wait().await;
        tracker.start_drain();
        assert!(tracker.try_start().is_none());
        // Abort is deliberate: cancellation must release all accepted permits, even
        // if it races with admission or a task has not reached its wait yet.
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        tokio::time::timeout(Duration::from_secs(1), tracker.wait_drained())
            .await
            .unwrap();
    }
}
