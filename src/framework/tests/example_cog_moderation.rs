#[path = "../examples/cog_moderation.rs"]
mod sample;

use bot_framework::{
    arguments::{ArgumentKind, ArgumentSchema, Arguments, SlashValue},
    dispatch,
    prelude::*,
};
use sample::moderation::{BanRequest, Moderation, ModerationService, RecordingModeration};
use std::sync::{Arc, Mutex};

enum Input<'a> {
    Prefix(&'a str),
    Slash(Vec<(String, SlashValue)>),
}

async fn invoke(states: &StateMap, input: Input<'_>) -> BotResult<Vec<String>> {
    let replies = Mutex::new(Vec::new());
    let args = Mutex::new(match input {
        Input::Prefix(text) => Arguments::prefix(text)?,
        Input::Slash(values) => Arguments::slash(values)?,
    });
    assert!(
        dispatch(
            "ban",
            Context::with_state(&replies, states).with_arguments(&args)
        )
        .await?
    );
    Ok(replies.into_inner().unwrap())
}

#[tokio::test]
async fn demo_uses_registered_cog_and_reports_only_dry_run() {
    let replies = sample::demo().await.unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0], replies[1]);
    assert!(replies[0].contains("dry-run"));
    assert!(replies[0].contains("no Discord request sent"));
}

#[tokio::test]
async fn prefix_and_slash_share_cog_service_with_optional_reasons() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let service = Arc::new(RecordingModeration::default());
    cogs.add(&mut states, Moderation::new(service.clone()))
        .await
        .unwrap();

    let prefix = invoke(&states, Input::Prefix("123 spam")).await.unwrap();
    let slash = invoke(
        &states,
        Input::Slash(vec![
            ("reason".into(), SlashValue::String("spam".into())),
            ("user".into(), SlashValue::String("123".into())),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(prefix, slash);
    invoke(&states, Input::Prefix("18446744073709551615"))
        .await
        .unwrap();
    invoke(
        &states,
        Input::Slash(vec![
            ("user".into(), SlashValue::String("456".into())),
            (
                "reason".into(),
                SlashValue::String("repeated spam messages".into()),
            ),
        ]),
    )
    .await
    .unwrap();

    let requests = service.requests().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[0].user_id.get(), 123);
    assert_eq!(requests[0].reason.as_deref(), Some("spam"));
    assert_eq!(requests[2].user_id.get(), u64::MAX);
    assert_eq!(requests[2].reason, None);
    assert_eq!(
        requests[3].reason.as_deref(),
        Some("repeated spam messages")
    );

    // 呼出しを await し終え、以降の入力を停止してから Cog を外す。
    cogs.remove::<Moderation>(&mut states).await.unwrap();
    assert!(invoke(&states, Input::Prefix("123")).await.is_err());
    assert_eq!(service.requests().unwrap(), requests);
}

#[tokio::test]
async fn invalid_input_never_records_a_plan_or_a_success_reply() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let service = Arc::new(RecordingModeration::default());
    cogs.add(&mut states, Moderation::new(service.clone()))
        .await
        .unwrap();
    for input in [
        "",
        "0",
        "-1",
        "+1",
        "abc",
        "１",
        "18446744073709551616",
        "123 reason extra",
    ] {
        let replies = Mutex::new(Vec::new());
        let args = Mutex::new(Arguments::prefix(input).unwrap());
        assert!(
            dispatch(
                "ban",
                Context::with_state(&replies, &states).with_arguments(&args)
            )
            .await
            .is_err(),
            "input: {input}"
        );
        assert!(replies.into_inner().unwrap().is_empty());
    }
    for values in [
        vec![("user".into(), SlashValue::Integer(123))],
        vec![
            ("user".into(), SlashValue::String("123".into())),
            ("reason".into(), SlashValue::Boolean(true)),
        ],
        vec![
            ("user".into(), SlashValue::String("123".into())),
            ("typo".into(), SlashValue::String("x".into())),
        ],
        vec![("user".into(), SlashValue::String("".into()))],
    ] {
        let replies = Mutex::new(Vec::new());
        let args = Mutex::new(Arguments::slash(values).unwrap());
        assert!(
            dispatch(
                "ban",
                Context::with_state(&replies, &states).with_arguments(&args)
            )
            .await
            .is_err()
        );
        assert!(replies.into_inner().unwrap().is_empty());
    }
    assert!(Arguments::prefix("123 \"two words\"").is_err());
    assert!(service.requests().unwrap().is_empty());
}

struct UnavailableService;
impl ModerationService for UnavailableService {
    fn plan_ban(&self, _: BanRequest) -> BotResult<()> {
        Err("plan service unavailable".into())
    }
}

#[tokio::test]
async fn missing_cog_and_service_failure_do_not_reply_with_success() {
    let mut states = StateMap::new();
    let replies = Mutex::new(Vec::new());
    let args = Mutex::new(Arguments::prefix("123").unwrap());
    assert!(
        dispatch(
            "ban",
            Context::with_state(&replies, &states).with_arguments(&args)
        )
        .await
        .is_err()
    );
    assert!(replies.lock().unwrap().is_empty());

    let mut cogs = CogSet::new();
    cogs.add(&mut states, Moderation::new(Arc::new(UnavailableService)))
        .await
        .unwrap();
    let args = Mutex::new(Arguments::prefix("123").unwrap());
    let error = dispatch(
        "ban",
        Context::with_state(&replies, &states).with_arguments(&args),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "plan service unavailable");
    assert!(replies.into_inner().unwrap().is_empty());
}

#[test]
fn macro_exposes_supported_string_and_optional_reason_schema() {
    let definitions = bot_framework::commands().unwrap();
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "ban");
    assert_eq!(
        definitions[0].arguments,
        &[
            ArgumentSchema {
                name: "user",
                kind: ArgumentKind::String,
                required: true
            },
            ArgumentSchema {
                name: "reason",
                kind: ArgumentKind::String,
                required: false
            },
        ]
    );
}
