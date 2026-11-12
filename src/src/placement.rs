//! Validation of intended Bot/Shard placement. This module does not grant ownership
//! or start processes: a validated placement alone is not a quorum lease.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::Read;
use std::path::Path;

/// Parsed configuration only; it is not an authorization to run a shard.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub cluster: Cluster,
    pub node: Node,
    #[serde(default)]
    pub failover: Failover,
    #[serde(default)]
    pub bots: Vec<BotPlacement>,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub id: String,
    pub mode: ClusterMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    Standalone,
    Quorum,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub id: String,
    pub remote_bind: Option<std::net::SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Failover {
    pub heartbeat_ms: u64,
    pub suspect_after_ms: u64,
    pub unhealthy_after_ms: u64,
    pub failback_stable_ms: u64,
}
impl Default for Failover {
    fn default() -> Self {
        Self {
            heartbeat_ms: 500,
            suspect_after_ms: 1500,
            unhealthy_after_ms: 3000,
            failback_stable_ms: 30000,
        }
    }
}

const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

impl Config {
    /// Reads a bounded UTF-8 TOML file. Parsing errors deliberately omit source text,
    /// so a mistakenly pasted token is not copied into logs.
    pub fn load(path: &Path) -> Result<Self, PlacementError> {
        let file = std::fs::File::open(path)
            .map_err(|_| PlacementError("cannot open configuration".into()))?;
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| PlacementError("cannot read configuration".into()))?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(PlacementError("configuration exceeds size limit".into()));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| PlacementError("configuration is not UTF-8".into()))?;
        toml::from_str(text).map_err(|_| PlacementError("invalid TOML configuration".into()))
    }

    /// Validate only after any automatic shard counts have been obtained from Discord.
    pub fn validate(&self, resolved_auto: &HashMap<String, u32>) -> Result<(), PlacementError> {
        for (label, value) in [("cluster id", &self.cluster.id), ("node id", &self.node.id)] {
            if value.trim().is_empty() || value.trim() != value {
                return Err(PlacementError(format!("invalid {label}")));
            }
        }
        let f = &self.failover;
        if f.heartbeat_ms == 0
            || f.heartbeat_ms > f.suspect_after_ms
            || f.suspect_after_ms >= f.unhealthy_after_ms
            || f.failback_stable_ms == 0
        {
            return Err(PlacementError("invalid failover timing".into()));
        }
        validate(&self.bots, resolved_auto)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ShardCount {
    Fixed(u32),
    Auto(AutoShardCount),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoShardCount {
    Auto,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotPlacement {
    pub name: String,
    pub bot_id: String,
    pub application_id: String,
    pub token_env: String,
    pub total_shards: ShardCount,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub shard_placement: Vec<ShardPlacement>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardPlacement {
    pub shard_id: u32,
    pub primary: String,
    #[serde(default)]
    pub failover: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PlacementError(pub String);
impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::redact::redact(&self.0))
    }
}
impl std::error::Error for PlacementError {}

/// Checks identities and placement after resolving each automatic shard count
/// from Discord. Missing resolution is an error, never permission to start.
/// Unlisted shards may later be assigned by an explicit automatic placement policy.
pub fn validate(
    bots: &[BotPlacement],
    resolved_auto: &HashMap<String, u32>,
) -> Result<(), PlacementError> {
    let mut names = HashSet::new();
    let mut ids = HashSet::new();
    let mut applications = HashSet::new();
    for bot in bots {
        for (label, value) in [
            ("name", &bot.name),
            ("bot_id", &bot.bot_id),
            ("application_id", &bot.application_id),
            ("token_env", &bot.token_env),
        ] {
            if value.trim().is_empty() || value.trim() != value {
                return Err(PlacementError(format!("invalid {label}")));
            }
        }
        if !names.insert(&bot.name)
            || !ids.insert(&bot.bot_id)
            || !applications.insert(&bot.application_id)
        {
            return Err(PlacementError(
                "duplicate bot name, bot_id or application_id".into(),
            ));
        }
        // Restrict references to portable environment variable names, never token values.
        let mut chars = bot.token_env.chars();
        if !chars
            .next()
            .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
            || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        {
            return Err(PlacementError("invalid token_env reference".into()));
        }
        let total = match bot.total_shards {
            ShardCount::Fixed(n) => n,
            ShardCount::Auto(_) => *resolved_auto
                .get(&bot.bot_id)
                .ok_or_else(|| PlacementError("automatic shard count unresolved".into()))?,
        };
        if total == 0 {
            return Err(PlacementError("total_shards must be positive".into()));
        }
        let mut shards = HashSet::new();
        for placement in &bot.shard_placement {
            if placement.shard_id >= total || !shards.insert(placement.shard_id) {
                return Err(PlacementError(format!(
                    "invalid or duplicate shard {}",
                    placement.shard_id
                )));
            }
            let mut nodes = HashSet::new();
            for node in std::iter::once(&placement.primary).chain(&placement.failover) {
                if node.trim().is_empty() || node.trim() != node || !nodes.insert(node) {
                    return Err(PlacementError(format!(
                        "invalid or duplicate node for shard {}",
                        placement.shard_id
                    )));
                }
            }
        }
    }
    Ok(())
}

/// An intent only. It is neither a lease nor permission to launch a process.
#[derive(Clone, PartialEq, Eq)]
pub struct LocalShardIntent {
    pub bot_id: String,
    pub shard_id: u32,
    pub total_shards: u32,
    pub token_env: String,
}

impl Config {
    /// List explicitly assigned primary shards for this node. This method never
    /// reads credentials, assigns unlisted shards, claims ownership or launches.
    pub fn local_primary_intents(
        &self,
        resolved_auto: &HashMap<String, u32>,
    ) -> Result<Vec<LocalShardIntent>, PlacementError> {
        self.validate(resolved_auto)?;
        let mut intents = Vec::new();
        for bot in &self.bots {
            let total_shards = match bot.total_shards {
                ShardCount::Fixed(n) => n,
                // validate() above requires the count to exist.
                ShardCount::Auto(_) => *resolved_auto
                    .get(&bot.bot_id)
                    .ok_or_else(|| PlacementError("automatic shard count unresolved".into()))?,
            };
            for shard in &bot.shard_placement {
                if shard.primary == self.node.id {
                    intents.push(LocalShardIntent {
                        bot_id: bot.bot_id.clone(),
                        shard_id: shard.shard_id,
                        total_shards,
                        token_env: bot.token_env.clone(),
                    });
                }
            }
        }
        intents.sort_unstable_by(|a, b| (&a.bot_id, a.shard_id).cmp(&(&b.bot_id, b.shard_id)));
        Ok(intents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bot() -> BotPlacement {
        serde_json::from_str(r#"{"name":"one","bot_id":"id1","application_id":"123","token_env":"BOT_TOKEN","total_shards":2,"shard_placement":[{"shard_id":0,"primary":"a","failover":["b"]},{"shard_id":1,"primary":"b","failover":["a"]}]}"#).unwrap()
    }
    #[test]
    fn validates_multiple_bots_and_rejects_duplicate_identity_and_shards() {
        let first = bot();
        let mut second = first.clone();
        second.name = "two".into();
        second.bot_id = "id2".into();
        second.application_id = "456".into();
        assert!(validate(&[first.clone(), second.clone()], &HashMap::new()).is_ok());
        second.bot_id = first.bot_id.clone();
        assert!(validate(&[first.clone(), second], &HashMap::new()).is_err());
        let mut bad = first.clone();
        bad.shard_placement.push(bad.shard_placement[0].clone());
        assert!(validate(&[bad], &HashMap::new()).is_err());
        let mut bad = first;
        bad.shard_placement[0].shard_id = 2;
        assert!(validate(&[bad], &HashMap::new()).is_err());
    }
    #[test]
    fn auto_must_be_resolved_and_nodes_must_be_unique() {
        let mut b = bot();
        b.total_shards = ShardCount::Auto(AutoShardCount::Auto);
        assert!(validate(&[b.clone()], &HashMap::new()).is_err());
        let counts = HashMap::from([("id1".to_owned(), 2)]);
        assert!(validate(&[b.clone()], &counts).is_ok());
        b.shard_placement[0].failover.push("a".into());
        assert!(validate(&[b], &counts).is_err());
    }
    #[test]
    fn loads_sample_and_checks_timing_and_auto_resolution() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml");
        let mut config = Config::load(&path).unwrap();
        assert!(config.validate(&HashMap::new()).is_err());
        let counts = HashMap::from([("bot-two".to_owned(), 1)]);
        assert!(config.validate(&counts).is_ok());
        config.failover.unhealthy_after_ms = config.failover.suspect_after_ms;
        assert!(config.validate(&counts).is_err());
    }

    #[test]
    fn load_rejects_invalid_and_oversized_files_without_exposing_source() {
        let path =
            std::env::temp_dir().join(format!("discord-rs-placement-test-{}", std::process::id()));
        let text =
            "[cluster]\nid = 'c'\nmode = 'standalone'\n[node]\nid = 'n'\nsecret = 'DO_NOT_LOG'\n";
        std::fs::write(&path, text).unwrap();
        let error = Config::load(&path).unwrap_err().to_string();
        assert!(!error.contains("DO_NOT_LOG"));
        std::fs::write(&path, vec![b'x'; MAX_CONFIG_BYTES as usize + 1]).unwrap();
        assert!(
            Config::load(&path)
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_intents_are_only_validated_explicit_primaries() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml");
        let mut config = Config::load(&path).unwrap();
        let counts = HashMap::from([("bot-two".to_owned(), 1)]);
        let intents = config.local_primary_intents(&counts).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(
            (
                &*intents[0].bot_id,
                intents[0].shard_id,
                intents[0].total_shards
            ),
            ("bot-one", 0, 2)
        );
        config.node.id = "server-b".into();
        let intents = config.local_primary_intents(&counts).unwrap();
        assert_eq!(
            intents
                .iter()
                .map(|i| (i.bot_id.as_str(), i.shard_id))
                .collect::<Vec<_>>(),
            [("bot-one", 1), ("bot-two", 0)]
        );
        assert!(config.local_primary_intents(&HashMap::new()).is_err());
        let duplicate = config.bots[0].shard_placement[0].clone();
        config.bots[0].shard_placement.push(duplicate);
        assert!(config.local_primary_intents(&counts).is_err());
    }

    #[test]
    fn validation_errors_do_not_echo_bot_identifiers() {
        let mut value = bot();
        value.bot_id = "secret-token\nforged-log-entry".into();
        value.total_shards = ShardCount::Auto(AutoShardCount::Auto);
        let error = validate(&[value.clone()], &HashMap::new())
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-token"));
        value.total_shards = ShardCount::Fixed(1);
        let error = validate(&[value], &HashMap::new()).unwrap_err().to_string();
        assert!(!error.contains("secret-token"));
    }

    #[test]
    fn rejects_unknown_fields_and_non_auto_string() {
        let json = r#"{"name":"x","bot_id":"i","application_id":"1","token_env":"TOKEN","total_shards":"other"}"#;
        assert!(serde_json::from_str::<BotPlacement>(json).is_err());
        let json = r#"{"name":"x","bot_id":"i","application_id":"1","token_env":"TOKEN","total_shards":1,"token":"secret"}"#;
        assert!(serde_json::from_str::<BotPlacement>(json).is_err());
    }
}

impl fmt::Debug for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cluster")
            .field("id", &crate::redact::Secret::new(&self.id))
            .field("mode", &self.mode)
            .finish()
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("id", &crate::redact::Secret::new(&self.id))
            .field("remote_bind", &self.remote_bind)
            .finish()
    }
}

impl fmt::Debug for BotPlacement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BotPlacement")
            .field("name", &crate::redact::Secret::new(&self.name))
            .field("bot_id", &crate::redact::Secret::new(&self.bot_id))
            .field(
                "application_id",
                &crate::redact::Secret::new(&self.application_id),
            )
            .field("token_env", &crate::redact::Secret::new(&self.token_env))
            .field("total_shards", &self.total_shards)
            .field("prefix", &crate::redact::Secret::new(&self.prefix))
            .field("shard_placement", &self.shard_placement)
            .finish()
    }
}

impl fmt::Debug for ShardPlacement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardPlacement")
            .field("shard_id", &self.shard_id)
            .field("primary", &crate::redact::Secret::new(&self.primary))
            .field("failover", &crate::redact::Secret::new(&self.failover))
            .finish()
    }
}

impl fmt::Debug for LocalShardIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalShardIntent")
            .field("bot_id", &crate::redact::Secret::new(&self.bot_id))
            .field("shard_id", &self.shard_id)
            .field("total_shards", &self.total_shards)
            .field("token_env", &crate::redact::Secret::new(&self.token_env))
            .finish()
    }
}

impl fmt::Debug for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
