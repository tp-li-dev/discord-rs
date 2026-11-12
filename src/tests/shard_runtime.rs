use src::placement::{Config, LocalShardIntent};
use src::shard_runtime::{Role, plan};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("runtime test root")
}
fn intent(bot: &str, shard: u32, token: &str) -> LocalShardIntent {
    LocalShardIntent {
        bot_id: bot.into(),
        shard_id: shard,
        total_shards: 2,
        token_env: token.into(),
    }
}
fn argument<'a>(args: &'a [OsString], flag: &str) -> &'a std::ffi::OsStr {
    &args.windows(2).find(|pair| pair[0] == flag).unwrap()[1]
}

#[test]
fn placement_output_produces_isolated_descriptions_only() {
    let config =
        Config::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml"))
            .unwrap();
    let intents = config
        .local_primary_intents(&HashMap::from([("bot-two".into(), 1)]))
        .unwrap();
    let specs = plan(&intents, &root()).unwrap();
    assert_eq!(specs.len(), intents.len());
    for spec in specs {
        assert_eq!(spec.gateway.role, Role::Gateway);
        assert_eq!(spec.core.role, Role::Core);
        for process in [&spec.gateway, &spec.core] {
            assert!(process.environment.clear_inherited);
            assert!(process.environment.variables.is_empty());
            assert_eq!(argument(&process.args, "--bot-id"), spec.bot_id.as_str());
            assert_eq!(
                argument(&process.args, "--shard-id"),
                spec.shard_id.to_string().as_str()
            );
            assert_eq!(
                argument(&process.args, "--total-shards"),
                spec.total_shards.to_string().as_str()
            );
            assert_eq!(
                argument(&process.args, "--ipc-path"),
                spec.ipc_path.as_os_str()
            );
            assert_eq!(
                argument(&process.args, "--state-path"),
                process.state_path.as_os_str()
            );
            assert_eq!(
                argument(&process.args, "--management-path"),
                process.management_path.as_os_str()
            );
        }
        assert!(spec.core.environment.credential_refs.is_empty());
        assert!(!spec.core.args.iter().any(|arg| arg == "--token-env"));
        assert_eq!(
            spec.gateway.environment.credential_refs,
            [intents
                .iter()
                .find(|i| i.bot_id == spec.bot_id && i.shard_id == spec.shard_id)
                .unwrap()
                .token_env
                .clone()]
        );
    }
}

#[test]
fn multiple_bots_and_shards_keep_credentials_and_paths_separate() {
    let inputs = [
        intent("B", 0, "B_TOKEN"),
        intent("A", 1, "A_TOKEN"),
        intent("A", 0, "A_TOKEN"),
    ];
    let specs = plan(&inputs, &root()).unwrap();
    assert_eq!(
        specs
            .iter()
            .map(|s| (s.bot_id.as_str(), s.shard_id))
            .collect::<Vec<_>>(),
        [("A", 0), ("A", 1), ("B", 0)]
    );
    let mut paths = HashSet::new();
    for spec in &specs {
        let token = if spec.bot_id == "A" {
            "A_TOKEN"
        } else {
            "B_TOKEN"
        };
        assert_eq!(spec.gateway.environment.credential_refs, [token]);
        assert_eq!(argument(&spec.gateway.args, "--token-env"), token);
        assert!(
            !spec
                .core
                .args
                .iter()
                .any(|arg| arg == "A_TOKEN" || arg == "B_TOKEN")
        );
        for path in [
            &spec.ipc_path,
            &spec.gateway.state_path,
            &spec.core.state_path,
            &spec.gateway.management_path,
            &spec.core.management_path,
        ] {
            assert!(path.starts_with(root()));
            assert!(paths.insert(path.as_os_str().to_ascii_lowercase()));
        }
    }
    let mut reversed = inputs.to_vec();
    reversed.reverse();
    assert_eq!(plan(&reversed, &root()).unwrap(), specs);
}

#[test]
fn portable_encoding_handles_reserved_names_separators_and_case() {
    let ids = [
        "CON",
        "con",
        "NUL.txt",
        "COM1",
        "LPT9",
        "..",
        "a/b",
        "a\\b",
        "C:\\x",
        "a.",
        "a",
        "日本語",
    ];
    let inputs = ids
        .iter()
        .enumerate()
        .map(|(n, id)| intent(id, 0, &format!("TOKEN_{n}")))
        .collect::<Vec<_>>();
    let specs = plan(&inputs, &root()).unwrap();
    let mut paths = HashSet::new();
    for spec in specs {
        let relative = spec.ipc_path.strip_prefix(root()).unwrap();
        assert_eq!(relative.components().count(), 3);
        let component = relative
            .components()
            .next()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap();
        assert!(component.starts_with("bot-"));
        assert!(
            component[4..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        assert!(paths.insert(relative.as_os_str().to_ascii_lowercase()));
    }
    #[cfg(windows)]
    assert!(
        plan(&inputs, Path::new(r"C:\runtime root")).unwrap()[0]
            .ipc_path
            .starts_with(r"C:\runtime root")
    );
    #[cfg(unix)]
    assert!(
        plan(&inputs, Path::new("/var/run/bots")).unwrap()[0]
            .ipc_path
            .starts_with("/var/run/bots")
    );
}

#[test]
fn invalid_and_ambiguous_inputs_fail_without_echoing_input() {
    let valid = intent("A", 0, "A_TOKEN");
    let mut invalid = Vec::new();
    let mut value = valid.clone();
    value.total_shards = 0;
    invalid.push(value);
    let mut value = valid.clone();
    value.shard_id = 2;
    invalid.push(value);
    let mut value = valid.clone();
    value.token_env = "secret-value!".into();
    invalid.push(value);
    let mut value = valid.clone();
    value.bot_id = "secret-value!\n".into();
    invalid.push(value);
    let mut value = valid.clone();
    value.bot_id = "x".repeat(65);
    invalid.push(value);
    for value in invalid {
        assert!(
            !plan(&[value], &root())
                .unwrap_err()
                .to_string()
                .contains("secret-value")
        );
    }
    assert!(plan(&[valid.clone(), valid.clone()], &root()).is_err());
    assert!(plan(&[valid.clone(), intent("B", 0, "a_token")], &root()).is_err());
    assert!(plan(&[valid.clone(), intent("A", 1, "OTHER")], &root()).is_err());
    let mut other = intent("A", 1, "A_TOKEN");
    other.total_shards = 3;
    assert!(plan(&[valid.clone(), other], &root()).is_err());
    assert!(plan(std::slice::from_ref(&valid), Path::new("relative")).is_err());
    assert!(plan(&[valid], &root().join("..")).is_err());
    assert!(plan(&[], &root()).unwrap().is_empty());
}
