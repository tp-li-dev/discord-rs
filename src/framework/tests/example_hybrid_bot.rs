#[path = "../examples/hybrid_bot.rs"]
mod hybrid_bot;

use bot_framework::{
    arguments::{ArgumentError, ArgumentKind, SlashValue},
    commands,
    prelude::*,
};
use hybrid_bot::{on_message, on_slash};
use std::sync::Mutex;

fn options(name: &str, enthusiastic: bool) -> Vec<(String, SlashValue)> {
    vec![
        ("name".into(), SlashValue::String(name.into())),
        ("enthusiastic".into(), SlashValue::Boolean(enthusiastic)),
    ]
}

#[tokio::test]
async fn demo_and_schema_match_the_real_sample() -> BotResult<()> {
    assert_eq!(
        hybrid_bot::demo().await?,
        ["Hello from on_message!", "Hello, Ada!", "Hello, Ada!"]
    );
    let definitions = commands()?;
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "greet");
    assert_eq!(
        definitions[0]
            .arguments
            .iter()
            .map(|argument| (argument.name, argument.kind, argument.required))
            .collect::<Vec<_>>(),
        [
            ("name", ArgumentKind::String, true),
            ("enthusiastic", ArgumentKind::Boolean, true),
            ("salutation", ArgumentKind::String, false),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn message_hook_ignores_bots_and_handles_plain_messages_and_commands() -> BotResult<()> {
    let replies = Mutex::new(Vec::new());
    for (message, bot) in [
        ("hello", true),
        ("!greet Ada true", true),
        ("!greet invalid", true),
        ("ordinary message", false),
        ("/greet Ada true", false),
        ("", false),
        ("!", false),
        ("! greet Ada true", false),
        ("!unknown", false),
    ] {
        assert!(!on_message(message, bot, &replies).await?);
    }
    assert!(replies.lock().unwrap().is_empty());
    assert!(on_message("hello", false, &replies).await?);
    assert!(on_message("!greet\tAda\nfalse\tWelcome", false, &replies).await?);
    assert_eq!(
        *replies.lock().unwrap(),
        ["Hello from on_message!", "Welcome, Ada."]
    );
    Ok(())
}

#[tokio::test]
async fn prefix_and_slash_share_handler_and_optional_arguments() -> BotResult<()> {
    let replies = Mutex::new(Vec::new());
    for (message, enthusiastic, salutation) in [
        ("!greet Ada true", true, None),
        ("!greet Ada false Welcome", false, Some("Welcome")),
    ] {
        assert!(on_message(message, false, &replies).await?);
        let mut slash = options("Ada", enthusiastic);
        if let Some(salutation) = salutation {
            slash.push(("salutation".into(), SlashValue::String(salutation.into())));
        }
        slash.reverse(); // option 順序ではなく名前で抽出する。
        assert!(on_slash("greet", slash, &replies).await?);
    }
    let mut slash = options("Ada Lovelace", false);
    slash.push((
        "salutation".into(),
        SlashValue::String("Good morning".into()),
    ));
    assert!(on_slash("greet", slash, &replies).await?);
    assert!(!on_slash("unknown", [], &replies).await?);
    assert_eq!(
        *replies.lock().unwrap(),
        [
            "Hello, Ada!",
            "Hello, Ada!",
            "Welcome, Ada.",
            "Welcome, Ada.",
            "Good morning, Ada Lovelace."
        ]
    );
    Ok(())
}

#[tokio::test]
async fn malformed_inputs_never_reach_the_handler() -> BotResult<()> {
    let replies = Mutex::new(Vec::new());
    for message in [
        "!greet",
        "!greet Ada",
        "!greet Ada maybe",
        "!greet Ada true Welcome extra",
        "!greet \"Ada Lovelace\" true",
        "!greet Ada\\Lovelace true",
    ] {
        let error = on_message(message, false, &replies).await.unwrap_err();
        assert!(error.downcast_ref::<ArgumentError>().is_some());
        assert!(replies.lock().unwrap().is_empty());
    }
    for slash in [
        vec![],
        vec![("name".into(), SlashValue::String("Ada".into()))],
        vec![
            ("name".into(), SlashValue::String("Ada".into())),
            ("enthusiastic".into(), SlashValue::String("true".into())),
        ],
        vec![
            ("name".into(), SlashValue::Integer(42)),
            ("enthusiastic".into(), SlashValue::Boolean(true)),
        ],
        vec![
            ("name".into(), SlashValue::String("Ada".into())),
            ("enthusiastic".into(), SlashValue::Boolean(true)),
            ("salutation".into(), SlashValue::Boolean(false)),
        ],
        vec![
            ("name".into(), SlashValue::String("Ada".into())),
            ("enthusiastic".into(), SlashValue::Boolean(true)),
            ("typo".into(), SlashValue::String("Welcome".into())),
        ],
        vec![
            ("name".into(), SlashValue::String("Ada".into())),
            ("name".into(), SlashValue::String("Grace".into())),
            ("enthusiastic".into(), SlashValue::Boolean(true)),
        ],
    ] {
        let error = on_slash("greet", slash, &replies).await.unwrap_err();
        assert!(error.downcast_ref::<ArgumentError>().is_some());
        assert!(replies.lock().unwrap().is_empty());
    }
    assert!(on_message("!greet Ada true", false, &replies).await?);
    assert_eq!(*replies.lock().unwrap(), ["Hello, Ada!"]);
    Ok(())
}
