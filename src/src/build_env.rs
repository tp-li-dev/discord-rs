//! Environment policy for the legacy manage/reload Cargo children.

use std::{env, ffi::OsString, process::Command};

const BUILD_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "LANG",
    "TERM",
    "TMPDIR",
    "CARGO_TARGET_DIR",
    "CARGO_NET_OFFLINE",
    "CARGO_TERM_COLOR",
    "CARGO_INCREMENTAL",
    "CARGO_PROFILE_DEV_DEBUG",
];

fn build_environment<'a>(
    variables: impl IntoIterator<Item = (OsString, OsString)> + 'a,
    extra: &'a [&str],
) -> impl Iterator<Item = (OsString, OsString)> + 'a {
    variables.into_iter().filter(move |(name, _)| {
        name.to_str().is_some_and(|name| {
            // Placement names contain a TOKEN component. Guard against any casing or
            // spelling containing TOKEN, including the private Gateway slot, even
            // when an entry is covered by LC_* or a future allowlist addition.
            !name.to_ascii_uppercase().contains("TOKEN")
                && (BUILD_ENV.contains(&name) || name.starts_with("LC_") || extra.contains(&name))
        })
    })
}

fn command_with_environment(extra: &[&str]) -> Command {
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .env_clear()
        .envs(build_environment(env::vars_os(), extra));
    command
}

/// Prepare a Cargo build command with the toolchain environment only.
pub fn cargo_command() -> Command {
    command_with_environment(&[])
}

/// Run reload through offline Cargo, preserving its explicit executable and worker-file settings.
pub fn cargo_reload_command() -> Command {
    let mut command = command_with_environment(&["CARGO", "DISCORD_WORKER_FILE"]);
    command.args(["run", "--offline", "--bin", "reload", "--", "general"]);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_environment_filters_token_like_names_and_matches_case() {
        let variables = [
            ("PATH", "/usr/bin:/bin"),
            ("Path", "/discard"),
            ("LC_ALL", "C"),
            ("LC_TOKEN_ALPHA", "fixture"),
            ("LC_TokenCache", "fixture"),
            ("DISCORD_TOKEN", "fixture"),
            ("MUSIC_BOT_TOKEN", "fixture"),
            ("DISCORD_TEST_BOT_TOKEN", "fixture"),
            ("TOKEN_ALPHA", "fixture"),
            (crate::placement::GATEWAY_TOKEN_ENV, "fixture"),
            ("DISCORD_WORKER_FILE", "/tmp/worker-path"),
            ("RUSTC_WRAPPER", "/discard"),
        ]
        .map(|(name, value)| (OsString::from(name), OsString::from(value)));
        let extra = [
            "DISCORD_WORKER_FILE",
            "MUSIC_BOT_TOKEN",
            crate::placement::GATEWAY_TOKEN_ENV,
        ];
        let path = (OsString::from("PATH"), OsString::from("/usr/bin:/bin"));
        let locale = (OsString::from("LC_ALL"), OsString::from("C"));

        assert_eq!(
            build_environment(variables.clone(), &[]).collect::<Vec<_>>(),
            [path.clone(), locale.clone()]
        );
        assert_eq!(
            build_environment(variables, &extra).collect::<Vec<_>>(),
            [
                path,
                locale,
                ("DISCORD_WORKER_FILE".into(), "/tmp/worker-path".into()),
            ]
        );
    }
}
