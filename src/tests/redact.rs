use src::redact::{MASK, Redacted, Secret, redact};

#[test]
fn secret_formatting_does_not_require_or_invoke_inner_formatting() {
    struct Unformattable;
    let secret = Secret::new(Unformattable);
    assert_eq!(
        format!("{secret} {secret:?} {secret:#?} {secret:>30}"),
        [MASK; 4].join(" ")
    );
    let secret = Secret::new("synthetic-only");
    assert_eq!(*secret.expose_secret(), "synthetic-only");
    assert_eq!(secret.into_inner(), "synthetic-only");
}

#[test]
fn masks_labelled_and_scheme_credentials() {
    for input in [
        "Bot synthetic-only",
        "bOt\r\n synthetic-only",
        "BEARER synthetic-only",
        "Authorization: Bot synthetic-only",
        "authorization:\nBearer synthetic-only",
        "PROXY-AUTHORIZATION: Basic synthetic-only",
        "'token=synthetic-only'",
        "TOKEN = synthetic-only",
        "{\"token\":\"synthetic-only\"}",
        "interaction_token='synthetic-only'",
        "BOT_TOKEN: synthetic-only",
        "custom_id=synthetic-only",
        "gateway_secret=synthetic-only",
        "manager_secret=synthetic-only",
        "secret=synthetic-only",
        "token=\n'synthetic-\nonly'",
        "token='synthetic-\\'only'",
        "token='synthetic-only",
        "token=\"synthetic-only",
    ] {
        let output = redact(input);
        assert!(!output.contains("synthetic"), "case failed: {output}");
        assert!(output.contains(MASK));
        assert_eq!(format!("{}", Redacted(input)), output);
        assert_eq!(format!("{:?}", Redacted(input)), output);
    }
}

#[test]
fn masks_urls_and_preserves_routing_information() {
    assert_eq!(
        redact("https://discord.com/api/v10/interactions/123/synthetic%2Fonly/callback?wait=true"),
        "https://discord.com/api/v10/interactions/123/[REDACTED]/callback?wait=true"
    );
    assert_eq!(
        redact("/WEBHOOKS/456/synthetic-only/messages/@original"),
        "/WEBHOOKS/456/[REDACTED]/messages/@original"
    );
    assert_eq!(
        redact("/path?token=synthetic-only&wait=true"),
        "/path?token=[REDACTED]&wait=true"
    );
}

#[test]
fn masks_common_discord_shapes_without_labels() {
    for credential in [
        "AAAAAAAAAAAAAAAAAAAAAAAA.BBBBBB.CCCCCCCCCCCCCCCCCCCCCCCCCCC",
        "mfa.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        assert_eq!(
            redact(&format!("日本語 ({credential}) 続き")),
            "日本語 ([REDACTED]) 続き"
        );
    }
}

#[test]
fn multiple_overlapping_matches_and_nonsecret_text() {
    assert_eq!(
        redact("Authorization: Bot synthetic-only\r\ntoken=second-only&ok=1"),
        "Authorization: [REDACTED]\r\ntoken=[REDACTED]&ok=1"
    );
    for input in [
        "",
        "日本語 🦀",
        "bot_id=123 shard_id=0",
        "token_env=DISCORD_TOKEN",
        "not_token=visible",
        "a.b.c",
        "token=",
        "Bearer",
    ] {
        assert_eq!(redact(input), input);
    }
}
