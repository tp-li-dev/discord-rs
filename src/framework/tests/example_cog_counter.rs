#[path = "../examples/cog_counter.rs"]
mod example;

use bot_framework::{arguments::Arguments, dispatch, dispatch_ready, prelude::*};
use example::counter::{CounterCog, CounterState};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn runnable_demo_uses_the_real_example_commands() {
    assert_eq!(example::demo().await.unwrap(), ["1", "2", "2"]);
}

#[tokio::test]
async fn prefix_slash_and_plain_command_share_one_service() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let shared = Arc::new(CounterState::new(10));
    states.insert(Arc::clone(&shared));
    let cog = cogs
        .add(
            &mut states,
            CounterCog {
                state: Arc::clone(&shared),
            },
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &cog.state,
        states.get::<Arc<CounterState>>().unwrap()
    ));
    let replies = Mutex::new(Vec::new());
    let prefix = Mutex::new(Arguments::prefix("").unwrap());
    let slash = Mutex::new(Arguments::slash([]).unwrap());
    for args in [&prefix, &slash] {
        assert!(
            dispatch(
                "counter",
                Context::with_state(&replies, &states).with_arguments(args),
            )
            .await
            .unwrap()
        );
    }
    dispatch("count_value", Context::with_state(&replies, &states))
        .await
        .unwrap();
    assert_eq!(*replies.lock().unwrap(), ["11", "12", "12"]);
    assert_eq!(shared.value(), 12);

    let definitions = bot_framework::commands().unwrap();
    assert_eq!(
        definitions.iter().map(|c| c.name).collect::<Vec<_>>(),
        ["count_value", "counter"]
    );
    assert!(definitions.iter().all(|c| c.arguments.is_empty()));
}

#[tokio::test]
async fn ready_event_uses_the_cog_and_does_not_reset_counter() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let shared = Arc::new(CounterState::new(7));
    cogs.add(
        &mut states,
        CounterCog {
            state: Arc::clone(&shared),
        },
    )
    .await
    .unwrap();
    let logs = Mutex::new(Vec::new());
    let logger = |message: &str| logs.lock().unwrap().push(message.to_owned());
    for _ in 0..2 {
        dispatch_ready(
            EventContext::with_state(&logger, &states),
            ReadyEvent {
                bot_name: "local-demo".into(),
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(shared.ready_events(), 2);
    assert_eq!(shared.value(), 7);
    assert_eq!(*logs.lock().unwrap(), ["counter ready", "counter ready"]);
}

#[tokio::test]
async fn missing_state_and_removed_cog_fail_without_replies() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let replies = Mutex::new(Vec::new());
    for command in ["counter", "count_value"] {
        assert!(
            dispatch(command, Context::with_state(&replies, &states))
                .await
                .is_err()
        );
    }
    let shared = Arc::new(CounterState::new(0));
    states.insert(Arc::clone(&shared));
    cogs.add(&mut states, CounterCog { state: shared })
        .await
        .unwrap();
    cogs.remove::<CounterCog>(&mut states).await.unwrap();
    assert!(
        dispatch("counter", Context::with_state(&replies, &states))
            .await
            .is_err()
    );
    let logger = |_: &str| {};
    assert!(
        dispatch_ready(
            EventContext::with_state(&logger, &states),
            ReadyEvent {
                bot_name: "removed".into()
            }
        )
        .await
        .is_err()
    );
    assert!(replies.lock().unwrap().is_empty());
    // Removing a Cog removes its own registration, not independently shared services.
    dispatch("count_value", Context::with_state(&replies, &states))
        .await
        .unwrap();
    assert_eq!(*replies.lock().unwrap(), ["0"]);
}

#[tokio::test]
async fn invalid_arguments_and_overflow_do_not_mutate_the_counter() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let shared = Arc::new(CounterState::new(u64::MAX));
    cogs.add(
        &mut states,
        CounterCog {
            state: Arc::clone(&shared),
        },
    )
    .await
    .unwrap();
    let replies = Mutex::new(Vec::new());
    let extra = Mutex::new(Arguments::prefix("unexpected").unwrap());
    assert!(
        dispatch(
            "counter",
            Context::with_state(&replies, &states).with_arguments(&extra)
        )
        .await
        .is_err()
    );
    let error = dispatch("counter", Context::with_state(&replies, &states))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "counter exhausted");
    assert_eq!(shared.value(), u64::MAX);
    assert!(replies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn independent_hosts_have_independent_state() {
    for initial in [0, 20] {
        let mut states = StateMap::new();
        let mut cogs = CogSet::new();
        cogs.add(
            &mut states,
            CounterCog {
                state: Arc::new(CounterState::new(initial)),
            },
        )
        .await
        .unwrap();
        let replies = Mutex::new(Vec::new());
        let (one, two) = tokio::join!(
            dispatch("counter", Context::with_state(&replies, &states)),
            dispatch("counter", Context::with_state(&replies, &states)),
        );
        assert!(one.unwrap());
        assert!(two.unwrap());
        let mut values = replies.into_inner().unwrap();
        values.sort();
        let mut expected = vec![(initial + 1).to_string(), (initial + 2).to_string()];
        expected.sort();
        assert_eq!(values, expected);
    }
}
