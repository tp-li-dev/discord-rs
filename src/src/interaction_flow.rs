//! Interaction acknowledgement and response sequencing, independent of HTTP transport.
use std::{future::Future, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Completed,
    AckFailed,
    AckTimedOut,
    UpdateFailed,
    UpdateTimedOut,
}

/// Execute only after confirmed ACK. Timeouts are ambiguous and never retried.
pub async fn run<I, A, AF, W, WF, U, UF>(
    identity: I,
    ack_timeout: Duration,
    update_timeout: Duration,
    ack: A,
    work: W,
    update: U,
) -> Outcome
where
    I: Clone,
    A: FnOnce(I) -> AF,
    AF: Future<Output = bool>,
    W: FnOnce() -> WF,
    WF: Future<Output = String>,
    U: FnOnce(I, String) -> UF,
    UF: Future<Output = bool>,
{
    match tokio::time::timeout(ack_timeout, ack(identity.clone())).await {
        Err(_) => return Outcome::AckTimedOut,
        Ok(false) => return Outcome::AckFailed,
        Ok(true) => {}
    }
    let message = work().await;
    match tokio::time::timeout(update_timeout, update(identity, message)).await {
        Err(_) => Outcome::UpdateTimedOut,
        Ok(false) => Outcome::UpdateFailed,
        Ok(true) => Outcome::Completed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn success_updates_the_original_interaction() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let ack_events = events.clone();
        let update_events = events.clone();
        let result = run(
            (123, "token-a"),
            Duration::from_secs(1),
            Duration::from_secs(1),
            move |id| async move {
                ack_events.lock().unwrap().push(("ack", id, ""));
                true
            },
            || async { "reply".to_owned() },
            move |id, message| async move {
                assert_eq!(message, "reply");
                update_events.lock().unwrap().push(("update", id, "reply"));
                true
            },
        )
        .await;
        assert_eq!(result, Outcome::Completed);
        assert_eq!(
            *events.lock().unwrap(),
            [
                ("ack", (123, "token-a"), ""),
                ("update", (123, "token-a"), "reply")
            ]
        );
    }
    #[tokio::test]
    async fn failed_and_timed_out_ack_never_execute_work_or_update() {
        for slow in [false, true] {
            let result = run(
                (),
                Duration::from_millis(10),
                Duration::from_secs(1),
                move |()| async move {
                    if slow {
                        std::future::pending::<()>().await;
                    }
                    false
                },
                || async { panic!("work must not run after an unconfirmed ACK") },
                |(), _| async { panic!("update must not run after an unconfirmed ACK") },
            )
            .await;
            assert_eq!(
                result,
                if slow {
                    Outcome::AckTimedOut
                } else {
                    Outcome::AckFailed
                }
            );
        }
    }
    #[tokio::test]
    async fn failed_and_timed_out_updates_are_distinguished() {
        for slow in [false, true] {
            let result = run(
                (),
                Duration::from_secs(1),
                Duration::from_millis(10),
                |()| async { true },
                || async { "done".into() },
                move |(), _| async move {
                    if slow {
                        std::future::pending::<()>().await;
                    }
                    false
                },
            )
            .await;
            assert_eq!(
                result,
                if slow {
                    Outcome::UpdateTimedOut
                } else {
                    Outcome::UpdateFailed
                }
            );
        }
    }
}
