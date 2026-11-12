use bot_framework::{
    arguments::{Arguments, SlashValue},
    dispatch,
    prelude::*,
};
use std::sync::Mutex;
#[hybrid_command(name = "repeat")]
async fn repeat(
    ctx: Context<'_>,
    word: String,
    times: i64,
    suffix: Option<String>,
) -> BotResult<()> {
    ctx.reply(format!("{word}:{times}:{}", suffix.unwrap_or_default()))
        .await
}
#[tokio::test]
async fn typed_command_accepts_both_input_forms() {
    use bot_framework::arguments::{ArgumentKind, ArgumentSchema};
    let metadata = bot_framework::commands()
        .unwrap()
        .into_iter()
        .find(|c| c.name == "repeat")
        .unwrap();
    assert_eq!(
        metadata.arguments,
        &[
            ArgumentSchema {
                name: "word",
                kind: ArgumentKind::String,
                required: true
            },
            ArgumentSchema {
                name: "times",
                kind: ArgumentKind::Integer,
                required: true
            },
            ArgumentSchema {
                name: "suffix",
                kind: ArgumentKind::String,
                required: false
            },
        ]
    );
    let replies = Mutex::new(Vec::new());
    let prefix = Mutex::new(Arguments::prefix("hello 4 end").unwrap());
    assert!(
        dispatch("repeat", Context::new(&replies).with_arguments(&prefix))
            .await
            .unwrap()
    );
    let slash = Mutex::new(
        Arguments::slash([
            ("word".into(), SlashValue::String("hello".into())),
            ("times".into(), SlashValue::Integer(4)),
            ("suffix".into(), SlashValue::String("end".into())),
        ])
        .unwrap(),
    );
    assert!(
        dispatch("repeat", Context::new(&replies).with_arguments(&slash))
            .await
            .unwrap()
    );
    assert_eq!(*replies.lock().unwrap(), ["hello:4:end", "hello:4:end"]);
    let invalid = Mutex::new(Arguments::prefix("hello nope").unwrap());
    assert!(
        dispatch("repeat", Context::new(&replies).with_arguments(&invalid))
            .await
            .is_err()
    );
    let extra = Mutex::new(Arguments::prefix("hello 4 suffix extra").unwrap());
    assert!(
        dispatch("repeat", Context::new(&replies).with_arguments(&extra))
            .await
            .is_err()
    );
}
