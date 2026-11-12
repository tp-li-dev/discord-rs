//! Pure launch descriptions. These do not authorize ownership or start processes.
use crate::placement::LocalShardIntent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Component, Path, PathBuf};

/// Launch-owned anonymous stdin control. EOF only closes the control channel:
/// it must not stop an otherwise healthy Gateway when its daemon disappears.
/// Shutdown is complete only after the Gateway drains/reaps its Workers and
/// exits successfully; writing this frame is not an acknowledgement.
pub const GATEWAY_CONTROL_FRAME_LIMIT: usize = 512;
pub const GATEWAY_SHUTDOWN_FRAME: &[u8] = b"{\"command\":\"shutdown\"}\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum GatewayControl {
    Shutdown {},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Gateway,
    Core,
}

/// A launcher must clear the inherited environment and read only these named
/// credential references. Canonical Bot/Shard/generation metadata is supplied by
/// the launcher. No credential values are stored in the description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    pub clear_inherited: bool,
    pub variables: BTreeMap<String, String>,
    pub credential_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    pub role: Role,
    pub program: OsString,
    pub args: Vec<OsString>,
    pub environment: Environment,
    pub state_path: PathBuf,
    /// Local authenticated management endpoint location, not a TCP bind address.
    pub management_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSpec {
    pub bot_id: String,
    pub shard_id: u32,
    pub total_shards: u32,
    /// Shared only by this shard's Gateway and Core; transport is not selected.
    pub ipc_path: PathBuf,
    pub gateway: ProcessSpec,
    pub core: ProcessSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimePlanError(pub &'static str);
impl fmt::Display for RuntimePlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for RuntimePlanError {}

fn token_reference(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        && ![
            "BOT_ID",
            "SHARD_ID",
            "TOTAL_SHARDS",
            "GENERATION_ID",
            "SYSTEMROOT",
            test_switch::LAUNCH_ENV,
        ]
        .contains(&value.to_ascii_uppercase().as_str())
}

/// Generate a deterministic plan beneath a native absolute root (Path::join,
/// never shell strings). IDs are encoded injectively into portable components.
/// The root is trusted deployment configuration; no filesystem is accessed.
/// A plan is not a quorum/lease grant. The daemon launches `gateway` and the
/// Gateway owns its Worker pool; `core` records the reserved Core namespace.
pub fn plan(
    intents: &[LocalShardIntent],
    root: &Path,
) -> Result<Vec<ShardRuntimeSpec>, RuntimePlanError> {
    if !root.is_absolute() || root.components().any(|c| c == Component::ParentDir) {
        return Err(RuntimePlanError(
            "runtime root must be absolute without parent traversal",
        ));
    }
    let mut identities = HashSet::new();
    let mut bots = HashMap::new();
    let mut credentials = HashMap::new();
    let mut paths = HashSet::new();
    let mut sorted = intents.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|a, b| (&a.bot_id, a.shard_id).cmp(&(&b.bot_id, b.shard_id)));
    let mut result = Vec::new();
    for intent in sorted {
        if intent.bot_id.is_empty()
            || intent.bot_id.len() > 64
            || intent.bot_id.trim() != intent.bot_id
            || intent.bot_id.chars().any(char::is_control)
            || intent.total_shards == 0
            || intent.shard_id >= intent.total_shards
            || !token_reference(&intent.token_env)
        {
            return Err(RuntimePlanError("invalid shard intent"));
        }
        if !identities.insert((&intent.bot_id, intent.shard_id)) {
            return Err(RuntimePlanError("duplicate shard intent"));
        }
        let metadata = (intent.total_shards, &intent.token_env);
        if bots
            .insert(&intent.bot_id, metadata)
            .is_some_and(|old| old != metadata)
        {
            return Err(RuntimePlanError("inconsistent bot metadata"));
        }
        // Windows environment names are case insensitive. Reject ambiguous
        // credential sources on both platforms, before any launch is possible.
        if credentials
            .insert(intent.token_env.to_ascii_uppercase(), &intent.bot_id)
            .is_some_and(|old| old != &intent.bot_id)
        {
            return Err(RuntimePlanError("credential reference shared across bots"));
        }
        let mut component = String::from("bot-");
        for byte in intent.bot_id.bytes() {
            use std::fmt::Write;
            write!(component, "{byte:02x}").expect("writing to String cannot fail");
        }
        let directory = root
            .join(component)
            .join(format!("shard-{}", intent.shard_id));
        let ipc_path = directory.join("ipc");
        let mut processes = Vec::new();
        for (role, name) in [(Role::Gateway, "gateway-host"), (Role::Core, "bot-core")] {
            let role_name = if role == Role::Gateway {
                "gateway"
            } else {
                "core"
            };
            let state_path = directory.join(format!("{role_name}-state"));
            let management_path = directory.join(format!("{role_name}-management"));
            let mut args = Vec::new();
            for (flag, value) in [
                ("--bot-id", OsString::from(&intent.bot_id)),
                ("--shard-id", intent.shard_id.to_string().into()),
                ("--total-shards", intent.total_shards.to_string().into()),
                ("--ipc-path", ipc_path.as_os_str().to_owned()),
                ("--state-path", state_path.as_os_str().to_owned()),
                ("--management-path", management_path.as_os_str().to_owned()),
            ] {
                args.extend([OsString::from(flag), value]);
            }
            if role == Role::Gateway {
                args.extend([
                    OsString::from("--token-env"),
                    OsString::from(&intent.token_env),
                ]);
            }
            processes.push(ProcessSpec {
                role,
                program: name.into(),
                args,
                environment: Environment {
                    clear_inherited: true,
                    variables: BTreeMap::new(),
                    credential_refs: if role == Role::Gateway {
                        vec![intent.token_env.clone()]
                    } else {
                        vec![]
                    },
                },
                state_path,
                management_path,
            });
        }
        // Check all allocated locations, including case-insensitive aliases.
        for path in std::iter::once(&ipc_path).chain(
            processes
                .iter()
                .flat_map(|p| [&p.state_path, &p.management_path]),
        ) {
            if !paths.insert(path.as_os_str().to_ascii_lowercase()) {
                return Err(RuntimePlanError("runtime path collision"));
            }
        }
        let core = processes.pop().expect("core specification");
        let gateway = processes.pop().expect("gateway specification");
        result.push(ShardRuntimeSpec {
            bot_id: intent.bot_id.clone(),
            shard_id: intent.shard_id,
            total_shards: intent.total_shards,
            ipc_path,
            gateway,
            core,
        });
    }
    Ok(result)
}

/// Explicit, local mock-Discord switch wiring. This is not a distributed lease,
/// a production control endpoint, or permission to contact Discord.
pub mod test_switch {
    use super::{Role, RuntimePlanError, ShardRuntimeSpec};
    use crate::protocol::{BotId, GenerationId, Identity, OwnershipEpoch, Shard};
    use crate::worker::WorkerIdentity;
    use serde::{Deserialize, Serialize};
    use std::ffi::OsString;
    use std::net::SocketAddr;
    use std::path::{Component, Path, PathBuf};
    use std::time::Duration;

    pub const LAUNCH_ENV: &str = "BOT_TEST_GATEWAY_SWITCH_LAUNCH";
    pub const FRAME_LIMIT: usize = 16 * 1024;
    pub const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

    fn absolute(path: &Path) -> bool {
        path.is_absolute() && !path.components().any(|part| part == Component::ParentDir)
    }

    /// The reserved Core specification becomes an executable, single-Shard
    /// launch contract only in this opt-in path. No credential reference/value
    /// is copied. Paths reserve namespaces; they do not promise durable state.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CoreLaunch {
        pub program: PathBuf,
        pub identity: Identity,
        pub ipc_path: PathBuf,
        pub state_path: PathBuf,
        pub management_path: PathBuf,
    }

    impl CoreLaunch {
        pub fn from_spec(
            spec: &ShardRuntimeSpec,
            program: &Path,
            generation: u64,
        ) -> Result<Self, RuntimePlanError> {
            let bot = spec
                .bot_id
                .parse::<u64>()
                .ok()
                .filter(|id| id.to_string() == spec.bot_id)
                .ok_or(RuntimePlanError("Core requires a canonical numeric Bot ID"))?;
            let core = Self {
                program: program.to_path_buf(),
                identity: Identity {
                    bot_id: BotId(bot),
                    shard: Shard::new(spec.shard_id, spec.total_shards)
                        .map_err(RuntimePlanError)?,
                    generation_id: GenerationId(generation),
                    ownership_epoch: OwnershipEpoch(0),
                },
                ipc_path: spec.ipc_path.clone(),
                state_path: spec.core.state_path.clone(),
                management_path: spec.core.management_path.clone(),
            };
            core.validate()?;
            if spec.core.role != Role::Core
                || !spec.core.environment.clear_inherited
                || !spec.core.environment.credential_refs.is_empty()
                || !spec.core.environment.variables.is_empty()
                || spec.core.args != core.placement_args()
            {
                return Err(RuntimePlanError("Core launch does not match placement"));
            }
            Ok(core)
        }

        pub fn validate(&self) -> Result<(), RuntimePlanError> {
            let mut paths = std::collections::HashSet::new();
            if self.identity.generation_id.0 == 0
                || self.identity.ownership_epoch.0 != 0
                || [
                    &self.program,
                    &self.ipc_path,
                    &self.state_path,
                    &self.management_path,
                ]
                .into_iter()
                .any(|path| !absolute(path))
                || [&self.ipc_path, &self.state_path, &self.management_path]
                    .into_iter()
                    .any(|path| !paths.insert(path.as_os_str().to_ascii_lowercase()))
            {
                return Err(RuntimePlanError("invalid Core launch specification"));
            }
            Ok(())
        }

        fn placement_args(&self) -> Vec<OsString> {
            let mut args = Vec::new();
            for (flag, value) in [
                ("--bot-id", self.identity.bot_id.0.to_string().into()),
                ("--shard-id", self.identity.shard.id().to_string().into()),
                (
                    "--total-shards",
                    self.identity.shard.total().to_string().into(),
                ),
                ("--ipc-path", self.ipc_path.as_os_str().to_owned()),
                ("--state-path", self.state_path.as_os_str().to_owned()),
                (
                    "--management-path",
                    self.management_path.as_os_str().to_owned(),
                ),
            ] {
                args.extend([OsString::from(flag), value]);
            }
            args
        }

        pub fn args(&self) -> Vec<OsString> {
            self.args_for(&self.identity)
        }

        pub(crate) fn args_for(&self, identity: &Identity) -> Vec<OsString> {
            let mut args = vec![OsString::from("--shard-core")];
            args.extend(self.placement_args());
            args.extend([
                OsString::from("--generation-id"),
                identity.generation_id.0.to_string().into(),
                OsString::from("--ownership-epoch"),
                identity.ownership_epoch.0.to_string().into(),
            ]);
            args
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Launch {
        pub address: SocketAddr,
        pub gateway: Identity,
        pub core: CoreLaunch,
    }

    impl Launch {
        pub fn validate(&self) -> Result<(), RuntimePlanError> {
            self.core.validate()?;
            if !self.address.ip().is_loopback()
                || self.address.port() == 0
                || self.gateway != self.core.identity
            {
                return Err(RuntimePlanError("invalid mock Gateway switch launch"));
            }
            Ok(())
        }
    }

    // Capabilities intentionally have no Debug implementation.
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Bootstrap {
        pub secret: [u8; 32],
    }

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Hello {
        pub secret: [u8; 32],
        pub identity: Identity,
        pub pid: u32,
        pub workers: Vec<WorkerIdentity>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Action {
        Pause,
        Activate,
        Quiesce,
        Drain,
        Status,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum State {
        Standby,
        Active,
        Paused,
        Drained,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Control {
        pub request_id: u64,
        pub identity: Identity,
        pub action: Action,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Reply {
        pub request_id: u64,
        pub identity: Identity,
        pub state: State,
        pub workers: Vec<WorkerIdentity>,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_contract_is_bounded_and_rejects_extra_control_fields() {
        assert!(GATEWAY_SHUTDOWN_FRAME.len() <= GATEWAY_CONTROL_FRAME_LIMIT);
        assert_eq!(
            serde_json::from_slice::<GatewayControl>(GATEWAY_SHUTDOWN_FRAME).unwrap(),
            GatewayControl::Shutdown {}
        );
        let mut encoded = serde_json::to_vec(&GatewayControl::Shutdown {}).unwrap();
        encoded.push(b'\n');
        assert_eq!(encoded, GATEWAY_SHUTDOWN_FRAME);
        for frame in [
            r#"{}"#,
            r#"{"command":"Shutdown"}"#,
            r#"{"command":"shutdown","force":true}"#,
            r#"{"command":"shutdown","secret":"must-not-echo"}"#,
            r#"{"command":"start"}"#,
        ] {
            assert!(serde_json::from_str::<GatewayControl>(frame).is_err());
        }
    }

    #[test]
    fn credentials_cannot_overwrite_launch_identity_on_either_platform() {
        for reference in [
            "BOT_ID",
            "shard_id",
            "Total_Shards",
            "GENERATION_ID",
            "SystemRoot",
            "BOT_TEST_GATEWAY_SWITCH_LAUNCH",
        ] {
            let intent = LocalShardIntent {
                bot_id: "101".into(),
                shard_id: 0,
                total_shards: 1,
                token_env: reference.into(),
            };
            assert!(plan(&[intent], &std::env::current_dir().unwrap()).is_err());
        }
    }
}
