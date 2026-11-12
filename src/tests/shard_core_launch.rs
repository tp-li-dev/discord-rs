//! Placement-derived Core argv and the real internal Core process.
use serde_json::{Value, json};
use src::{
    placement::LocalShardIntent,
    protocol::Hello,
    shard_runtime::{plan, test_switch::CoreLaunch},
};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    time::timeout,
};

fn spec() -> src::shard_runtime::ShardRuntimeSpec {
    plan(
        &[LocalShardIntent {
            bot_id: "101".into(),
            shard_id: 1,
            total_shards: 2,
            token_env: "CORE_MUST_NOT_INHERIT_THIS".into(),
        }],
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("unused core paths"),
    )
    .unwrap()
    .pop()
    .unwrap()
}

fn launch() -> CoreLaunch {
    CoreLaunch::from_spec(&spec(), &PathBuf::from(env!("CARGO_BIN_EXE_botd")), 701).unwrap()
}

fn command(core: &CoreLaunch) -> Command {
    let mut command = Command::new(&core.program);
    command
        .args(core.args())
        .env_clear()
        .env(
            "BOT_WORKER_IPC_IDENTITY",
            serde_json::to_string(&core.identity).unwrap(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.env("SystemRoot", std::env::var_os("SystemRoot").unwrap());
    command
}

async fn exchange(
    input: &mut ChildStdin,
    output: &mut BufReader<ChildStdout>,
    request: Value,
) -> Value {
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.push(b'\n');
    input.write_all(&bytes).await.unwrap();
    let mut response = String::new();
    assert!(output.read_line(&mut response).await.unwrap() > 0);
    serde_json::from_str(&response).unwrap()
}

#[test]
fn core_arguments_use_placement_identity_and_reject_tampered_specs() {
    let core = launch();
    let args = core.args();
    assert_eq!(args[0], "--shard-core");
    assert_eq!(core.identity.bot_id.0, 101);
    assert_eq!(core.identity.shard.id(), 1);
    assert_eq!(core.identity.shard.total(), 2);
    assert_eq!(core.identity.generation_id.0, 701);
    assert!(
        !args
            .iter()
            .any(|arg| arg == "--token-env" || arg == "CORE_MUST_NOT_INHERIT_THIS")
    );
    let mut changed = spec();
    changed.core.args[1] = "102".into();
    assert!(CoreLaunch::from_spec(&changed, &core.program, 701).is_err());
    let mut changed = spec();
    changed
        .core
        .environment
        .credential_refs
        .push("TOKEN".into());
    assert!(CoreLaunch::from_spec(&changed, &core.program, 701).is_err());
    assert!(CoreLaunch::from_spec(&spec(), &core.program, 0).is_err());
}

#[tokio::test]
async fn real_shard_core_matches_argv_to_hello_and_remains_credential_free() {
    timeout(Duration::from_secs(15), async {
        let core = launch();
        let mut child = command(&core).spawn().unwrap();
        let pid = child.id().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let ready = exchange(&mut input, &mut output, json!({"type":"health"})).await;
        assert_eq!(ready["type"], "ready");
        assert_eq!(ready["pid"], pid);
        assert_eq!(ready["credential_free"], true);
        let hello = Hello {
            protocol_min: 5,
            protocol_max: 5,
            identity: core.identity.clone(),
            required_capabilities: vec!["snapshot-v3".into()],
            optional_capabilities: vec!["hello-v1".into(), "command-dispatch-v1".into()],
        };
        let reply = exchange(
            &mut input,
            &mut output,
            json!({"type":"hello","hello":hello}),
        )
        .await;
        assert_eq!(
            reply["hello"]["identity"],
            serde_json::to_value(&core.identity).unwrap()
        );
        let reply = exchange(
            &mut input,
            &mut output,
            json!({"type":"command","name":"ping"}),
        )
        .await;
        assert_eq!(reply["content"], "Pong! 🦀");
        drop(input);
        assert!(child.wait().await.unwrap().success());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn core_rejects_mismatched_identity_and_inherited_environment_without_echo() {
    timeout(Duration::from_secs(15), async {
        let core = launch();
        let mut mismatched = command(&core);
        let mut identity = core.identity.clone();
        identity.bot_id.0 = 102;
        mismatched.env(
            "BOT_WORKER_IPC_IDENTITY",
            serde_json::to_string(&identity).unwrap(),
        );
        let output = mismatched.output().await.unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let output = command(&core)
            .env("UNTRUSTED_INHERITED_VALUE", "must-not-echo-core-value")
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("must-not-echo-core-value"));
    })
    .await
    .unwrap();
}
