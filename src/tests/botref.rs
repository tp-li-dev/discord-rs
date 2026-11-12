use src::botref::{
    BotIsolation, BotRefError, resolve, valid_name, validate_bots, validate_isolation,
};
use src::placement::{BotPlacement, ShardCount};

fn bot(name: &str, id: &str, env: &str) -> BotPlacement {
    BotPlacement {
        name: name.into(),
        bot_id: id.into(),
        application_id: "unused".into(),
        token_env: env.into(),
        total_shards: ShardCount::Fixed(1),
        prefix: None,
        shard_placement: vec![],
    }
}

#[test]
fn resolves_unicode_name_full_id_and_unique_prefix_without_mutation() {
    let bots = vec![
        bot("音楽猫", "abc-123", "MUSIC_TOKEN"),
        bot("管理", "abd-456", "ADMIN_TOKEN"),
    ];
    let before = bots.clone();
    for query in ["音楽猫", "abc-123", "abc"] {
        assert!(std::ptr::eq(resolve(&bots, query).unwrap(), &bots[0]));
    }
    assert_eq!(bots, before);
    assert_eq!(
        resolve(&bots, "ab"),
        Err(BotRefError::Ambiguous {
            indices: vec![0, 1]
        })
    );
    for query in ["", "unknown", "ABC", " 音楽猫", "abc-123 "] {
        assert_eq!(resolve(&bots, query), Err(BotRefError::NotFound));
    }
    assert_eq!(resolve(&[], "x"), Err(BotRefError::NotFound));
}

#[test]
fn name_and_id_collisions_have_no_silent_precedence() {
    for (name, first_id, second_id, query) in [
        ("abc", "other", "abc-123", "abc"),
        ("abc-123", "other", "abc-123", "abc-123"),
        ("one", "abc", "abc-123", "abc"),
    ] {
        let bots = vec![bot(name, first_id, "A"), bot("two", second_id, "B")];
        assert!(validate_bots(&bots).is_ok());
        assert_eq!(
            resolve(&bots, query),
            Err(BotRefError::Ambiguous {
                indices: vec![0, 1]
            })
        );
        let reversed = vec![bots[1].clone(), bots[0].clone()];
        assert!(matches!(
            resolve(&reversed, query),
            Err(BotRefError::Ambiguous { .. })
        ));
    }
    let bots = vec![bot("abc", "abc-123", "A")];
    assert_eq!(resolve(&bots, "abc"), Ok(&bots[0]));
}

#[test]
fn invalid_or_duplicate_configuration_fails_before_resolution() {
    let first = bot("one", "abc", "A");
    for (second, expected) in [
        (
            bot("one", "def", "B"),
            BotRefError::DuplicateName {
                first: 0,
                second: 1,
            },
        ),
        (
            bot("two", "abc", "B"),
            BotRefError::DuplicateId {
                first: 0,
                second: 1,
            },
        ),
        (
            bot("../bad", "def", "B"),
            BotRefError::InvalidName { index: 1 },
        ),
        (bot("two", "", "B"), BotRefError::InvalidId { index: 1 }),
        (
            bot("two", "bad\ninput", "B"),
            BotRefError::InvalidId { index: 1 },
        ),
        (
            bot("two", &"x".repeat(65), "B"),
            BotRefError::InvalidId { index: 1 },
        ),
    ] {
        assert_eq!(resolve(&[first.clone(), second], "one"), Err(expected));
    }
}

#[test]
fn names_are_portable_components_with_utf8_byte_limit() {
    for name in [
        "猫",
        "音楽-bot_2",
        "Cafe\u{301}",
        "COM10",
        "console",
        "a.b",
        &"x".repeat(64),
    ] {
        assert!(valid_name(name), "{name:?}");
    }
    for name in [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "a:b",
        "a?b",
        "a*b",
        "a|b",
        "a<b",
        "a>b",
        "a\"b",
        "a\0b",
        "a\nb",
        "a\tb",
        " a",
        "a ",
        "a b",
        "a.",
        "con",
        "NUL.txt",
        "COM1",
        "lpt9.log",
        "COM¹",
        "LPT².txt",
        "CONIN$",
        "CONOUT$",
        &"x".repeat(65),
        &"猫".repeat(22),
    ] {
        assert!(!valid_name(name), "{name:?}");
    }
    assert!(valid_name(&"猫".repeat(21)));
}

fn isolation(
    a: &BotPlacement,
    b: &BotPlacement,
    left: &str,
    right: &str,
) -> Result<(), BotRefError> {
    validate_isolation(&[
        BotIsolation {
            bot: a,
            state_dir: left,
        },
        BotIsolation {
            bot: b,
            state_dir: right,
        },
    ])
}

#[test]
fn isolation_rejects_shared_environment_references_on_both_platforms() {
    let a = bot("one", "a", "BOT_A_TOKEN");
    let mut b = bot("two", "b", "BOT_B_TOKEN");
    assert!(isolation(&a, &b, "bots/a", "bots/b").is_ok());
    for env in ["BOT_A_TOKEN", "bot_a_token"] {
        b.token_env = env.into();
        assert_eq!(
            isolation(&a, &b, "a", "b"),
            Err(BotRefError::SharedTokenEnv {
                first: 0,
                second: 1
            })
        );
    }
    for env in ["", "1TOKEN", "A=B", "TOKEN\n", "トークン"] {
        b.token_env = env.into();
        assert_eq!(
            isolation(&a, &b, "a", "b"),
            Err(BotRefError::InvalidTokenEnv { index: 1 })
        );
    }
}

#[test]
fn isolation_rejects_equal_and_nested_paths_in_either_order() {
    let a = bot("one", "a", "A");
    let b = bot("two", "b", "B");
    for (left, right) in [
        ("bots/a", "bots/a"),
        ("bots/a", "BOTS\\A"),
        ("bots/a", "bots/a/state"),
        ("bots/a/state", "bots/a"),
        ("猫/状態", "猫/状態/core"),
    ] {
        assert_eq!(
            isolation(&a, &b, left, right),
            Err(BotRefError::OverlappingStatePath {
                first: 0,
                second: 1
            })
        );
    }
    for right in ["bots/ab", "bots/b", "elsewhere/a"] {
        assert!(isolation(&a, &b, "bots/a", right).is_ok());
    }
    for path in [
        "",
        "/absolute",
        "\\absolute",
        "C:\\state",
        "\\\\server\\share",
        "a/../b",
        "./a",
        "a//b",
        "a/",
        "a\\",
        "NUL/state",
        "a./b",
        "a /b",
    ] {
        assert_eq!(
            isolation(&a, &b, "safe", path),
            Err(BotRefError::InvalidStatePath { index: 1 })
        );
    }
    assert!(validate_isolation(&[]).is_ok());
}

#[test]
fn isolation_checks_identity_and_errors_never_echo_input() {
    let a = bot("one", "a", "A");
    let b = bot("two", "a", "B");
    assert_eq!(
        isolation(&a, &b, "a", "b"),
        Err(BotRefError::DuplicateId {
            first: 0,
            second: 1
        })
    );
    let b = bot("untrusted\ntext", "b", "B");
    let error = resolve(&[a, b], "untrusted\ntext").unwrap_err();
    assert!(!format!("{error} {error:?}").contains("untrusted"));
}
