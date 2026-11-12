//! Pure launch descriptions. These do not authorize ownership or start processes.
use crate::placement::LocalShardIntent;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Gateway,
    Core,
}

/// A future launcher must clear the inherited environment and copy only these
/// named variables from its credential source. No credential values are stored.
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
}

/// Generate a deterministic plan beneath a native absolute root (Path::join,
/// never shell strings). IDs are encoded injectively into portable components.
/// The root is trusted deployment configuration; no filesystem is accessed.
/// A plan is not a quorum/lease grant and is not wired to existing executables.
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
