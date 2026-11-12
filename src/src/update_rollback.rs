//! Shard-by-shard update/rollback planning and authenticated daemon requests.
//!
//! CLI: `botctl update BOT --plan manifest.json [--component core|gateway|all]`.
//! `rollback` uses the same format with the previous software's artifacts, but
//! **new, increasing process generations**. Both commands default to dry-run.
//! Paths are relative to the manifest's directory unless absolute. Example:
//! ```json
//! {"bot_id":"bot-one","bot_name":"first","total_shards":2,
//!  "shards":[{"shard_id":0,"core":{"current":1,"candidate":2}}],
//!  "artifacts":{"core":{"path":"bot-core.exe","sha256":"<64 hex digits>"}}}
//! ```
//! Gateway/all plans also require `identify: {limits: {max_concurrency, remaining,
//! reset_after, total}, observed_age_ms}`. `reset_after` is milliseconds. The
//! supplied age plus time spent planning is only an offline estimate, not proof
//! of freshness, a reservation, ownership, quorum, a lease, or readiness.
//!
//! # Daemon control contract
//! [`UpdateControlRequest`] extends daemon `CommandRequest` on the client side.
//! Carry it in the existing anonymous-bootstrap authenticated loopback envelope
//! (`secret` supplied by the transport, never stored in the plan), newline JSON,
//! 4096-byte request / 64-KiB response limits and existing operation-ID polling.
//! Count the authentication envelope in the request limit: a large manifest may
//! not fit. Reject it before any mutation; generate a smaller, explicitly reviewed
//! plan rather than silently truncating shards or fragmenting an operation.
//! With `--yes --bootstrap-stdin`, botctl sends it once and polls the operation
//! endpoint. The server binds the capability to a trusted launch-time principal,
//! role and optional Shard scope, then uses the audited update executor. Normal
//! runtime and Core/all execution remain unsupported and fail closed. Explicit
//! test Gateway-switch mode supports Gateway updates using mock Discord only.
//! Artifact paths sent by botctl are absolute paths on the same local host.
//! Successful operations add `final_generations` entries of the form
//! `{shard_id, core: generation, gateway: generation}` (selected components only).
//!
//! A production executor must authenticate and authorize update privileges, validate
//! `yes` and operation id, resolve the canonical Bot, compare every current
//! generation and shard count, rehash the pinned artifact immediately before
//! launching it, and refresh/reserve Identify budget through the Bot-wide shared
//! queue (including all hosts). Never infer a refill from elapsed time. Serialize
//! admission/routing with Switch transitions, verify IPC/readiness/state schema,
//! fence ownership and check quorum/lease at each activation. Preserve old
//! processes until health checks and all drain checks pass. No Bot token belongs
//! in this contract or a Core environment. Audit without credentials. Require
//! idempotent operation ids; on a lost reply query rather than replay mutation.
//! A successful response must include final generations, affected/failed shards,
//! elapsed time and final operation state, not merely request acceptance.
//!
//! Automatic recovery is conditional: before commit keep old routing; after
//! commit restore the retained previous process only when it can resume safely.
//! A retired/lost old process needs a separate restart plan and Identify gate.
//! Unknown external results block retirement; never replay external effects.
//! DrainTracker cannot reopen admission: the future routing owner must implement
//! safe resumption separately. Static file checks do not prove successful loading,
//! signatures, runtime dependencies, Windows ACLs, or race-free future execution.

use crate::{
    drain::DrainTracker,
    switch::{Phase, Switch, SwitchError},
    update_gate::{self, UpdateDecision},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use twilight_model::gateway::SessionStartLimit;

pub const MANIFEST_LIMIT: u64 = 64 * 1024;
pub const SHARD_LIMIT: usize = 64;
const BINARY_LIMIT: u64 = 512 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Update,
    Rollback,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Component {
    #[default]
    Core,
    Gateway,
    All,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifacts {
    pub core: Option<Artifact>,
    pub gateway: Option<Artifact>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Generations {
    pub current: u64,
    pub candidate: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub shard_id: u32,
    pub core: Option<Generations>,
    pub gateway: Option<Generations>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentifyObservation {
    pub limits: IdentifyLimits,
    pub observed_age_ms: u64,
}

/// Strict wire representation; reject accidental credentials/unknown fields
/// even inside the metadata supplied by an offline operator.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentifyLimits {
    pub max_concurrency: u16,
    pub remaining: u32,
    pub reset_after: u64,
    pub total: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub bot_id: String,
    pub bot_name: String,
    pub total_shards: u32,
    pub shards: Vec<Shard>,
    pub artifacts: Artifacts,
    pub identify: Option<IdentifyObservation>,
}

/// Authenticated envelope payload, deliberately without credentials or roles.
/// The daemon must resolve/authorize the canonical Bot and revalidate live state.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateControlRequest {
    pub command: Action,
    pub bot: String,
    pub operation_id: String,
    pub yes: bool,
    pub component: Component,
    pub manifest: Manifest,
}

impl UpdateControlRequest {
    pub fn command_name(&self) -> &'static str {
        match self.command {
            Action::Update => "update",
            Action::Rollback => "rollback",
        }
    }

    /// Structural checks only, never authentication or authorization.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.yes {
            return Err("update/rollback requires explicit --yes confirmation");
        }
        if self.bot != self.manifest.bot_id
            || self.operation_id.len() != 32
            || !self
                .operation_id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err("invalid update control identity or operation ID");
        }
        validate_manifest(&self.manifest, &self.bot, self.component)
    }
}

pub fn load_manifest(path: &Path) -> Result<Manifest, &'static str> {
    if !path
        .metadata()
        .map_err(|_| "cannot inspect update manifest")?
        .is_file()
    {
        return Err("update manifest must be a regular file");
    }
    let file = File::open(path).map_err(|_| "cannot open update manifest")?;
    if !file
        .metadata()
        .map_err(|_| "cannot inspect update manifest")?
        .is_file()
    {
        return Err("update manifest must be a regular file");
    }
    let mut bytes = Vec::new();
    file.take(MANIFEST_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read update manifest")?;
    if bytes.len() as u64 > MANIFEST_LIMIT {
        return Err("update manifest exceeds size limit");
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid update manifest JSON")
}

fn selected(component: Component) -> &'static [Component] {
    match component {
        Component::Core => &[Component::Core],
        Component::Gateway => &[Component::Gateway],
        Component::All => &[Component::Core, Component::Gateway],
    }
}

fn generation(shard: &Shard, component: Component) -> Option<Generations> {
    match component {
        Component::Core => shard.core,
        Component::Gateway => shard.gateway,
        Component::All => None,
    }
}

fn valid_digest(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|c| c.is_ascii_hexdigit())
}

fn validate_manifest(
    manifest: &Manifest,
    selector: &str,
    component: Component,
) -> Result<(), &'static str> {
    if manifest.bot_id.is_empty()
        || manifest.bot_id.len() > 64
        || manifest.bot_id.trim() != manifest.bot_id
        || manifest.bot_id.chars().any(char::is_control)
        || !crate::botref::valid_name(&manifest.bot_name)
    {
        return Err("invalid manifest Bot identity");
    }
    // A manifest contains one Bot, so prefixes cannot select a different entry.
    // Live inspect must still resolve the selector against the entire daemon.
    if selector.is_empty()
        || !(selector == manifest.bot_name || manifest.bot_id.starts_with(selector))
    {
        return Err("Bot selector does not match update manifest");
    }
    if manifest.total_shards == 0
        || manifest.shards.is_empty()
        || manifest.shards.len() > SHARD_LIMIT
    {
        return Err("update plan requires 1..64 shards and a positive total_shards");
    }
    let mut seen = HashSet::new();
    for shard in &manifest.shards {
        if shard.shard_id >= manifest.total_shards || !seen.insert(shard.shard_id) {
            return Err("invalid or duplicate update Shard");
        }
        for part in selected(component) {
            let generations = generation(shard, *part).ok_or("missing component generations")?;
            let mut switch = Switch::new(generations.current);
            switch
                .prepare(generations.candidate)
                .map_err(|_| "candidate generation must increase")?;
            switch
                .abort_prepare()
                .map_err(|_| "invalid generation preparation")?;
        }
    }
    for part in selected(component) {
        let artifact = match part {
            Component::Core => &manifest.artifacts.core,
            _ => &manifest.artifacts.gateway,
        }
        .as_ref()
        .ok_or("missing component artifact")?;
        if artifact.path.as_os_str().is_empty() || !valid_digest(&artifact.sha256) {
            return Err("artifact requires a path and a SHA-256 hexadecimal digest");
        }
    }
    if component != Component::Core && manifest.identify.is_none() {
        return Err("Gateway planning requires an Identify observation");
    }
    Ok(())
}

/// Check permissions/format and hash an opened regular file, without spawning it.
/// The returned canonical path is a description, not a pinned execution handle.
pub fn verify_artifact(artifact: &Artifact, base: &Path) -> Result<PathBuf, &'static str> {
    if !valid_digest(&artifact.sha256) {
        return Err("invalid SHA-256 digest");
    }
    let path = if artifact.path.is_absolute() {
        artifact.path.clone()
    } else {
        base.join(&artifact.path)
    };
    let path = path
        .canonicalize()
        .map_err(|_| "artifact does not exist or is inaccessible")?;
    if !path
        .metadata()
        .map_err(|_| "cannot inspect artifact")?
        .is_file()
    {
        return Err("artifact must be a regular file");
    }
    let mut file = File::open(&path).map_err(|_| "artifact is not readable")?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect opened artifact")?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > BINARY_LIMIT {
        return Err("artifact must be a nonempty regular file no larger than 512 MiB");
    }
    executable(&path, &metadata)?;
    // Reuse the already-declared cryptographic provider; no new dependencies or
    // handwritten digest algorithm. This public suite uses SHA-256 on both OSes.
    let hash = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .expect("TLS 1.3 suite")
        .common
        .hash_provider;
    let mut context = hash.start();
    let mut buffer = [0u8; 64 * 1024];
    let mut read = 0u64;
    let mut header = Vec::with_capacity(64);
    loop {
        let n = file.read(&mut buffer).map_err(|_| "cannot hash artifact")?;
        if n == 0 {
            break;
        }
        read = read.checked_add(n as u64).ok_or("artifact size overflow")?;
        if read > BINARY_LIMIT {
            return Err("artifact exceeds size limit");
        }
        let header_bytes = (64 - header.len()).min(n);
        header.extend_from_slice(&buffer[..header_bytes]);
        context.update(&buffer[..n]);
    }
    if read != metadata.len() {
        return Err("artifact changed during validation");
    }
    let digest = context.finish();
    let actual: String = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if !actual.eq_ignore_ascii_case(&artifact.sha256) {
        return Err("artifact SHA-256 mismatch");
    }
    native_format(&header)?;
    Ok(path)
}

#[cfg(target_os = "linux")]
fn native_format(header: &[u8]) -> Result<(), &'static str> {
    // Basic native ELF check, not a loader or dependency check. Hashing includes
    // these bytes from the same open file. Foreign-architecture artifacts cannot
    // be used in a local plan (even if binfmt happens to support them).
    if header.len() < 64 || &header[..4] != b"\x7fELF" || header[6] != 1 {
        return Err("artifact is not a native Linux ELF executable");
    }
    let endian = if cfg!(target_endian = "little") { 1 } else { 2 };
    let word = |offset| {
        if endian == 1 {
            u16::from_le_bytes([header[offset], header[offset + 1]])
        } else {
            u16::from_be_bytes([header[offset], header[offset + 1]])
        }
    };
    let machine = if cfg!(target_arch = "x86_64") {
        62
    } else if cfg!(target_arch = "aarch64") {
        183
    } else if cfg!(target_arch = "x86") {
        3
    } else if cfg!(target_arch = "arm") {
        40
    } else {
        0
    };
    if header[4]
        != if cfg!(target_pointer_width = "64") {
            2
        } else {
            1
        }
        || header[5] != endian
        || !matches!(word(16), 2 | 3)
        || machine == 0
        || word(18) != machine
    {
        return Err("artifact ELF type or architecture is not supported");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn native_format(_: &[u8]) -> Result<(), &'static str> {
    Ok(())
}

#[cfg(unix)]
fn executable(path: &Path, metadata: &std::fs::Metadata) -> Result<(), &'static str> {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err("artifact is not executable");
    }
    let path =
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| "invalid artifact path")?;
    unsafe extern "C" {
        fn access(path: *const std::ffi::c_char, mode: std::ffi::c_int) -> std::ffi::c_int;
    }
    // SAFETY: NUL-terminated native path; POSIX X_OK is 1. No file is executed.
    if unsafe { access(path.as_ptr(), 1) } != 0 {
        return Err("artifact execution permission denied");
    }
    Ok(())
}

#[cfg(windows)]
fn executable(path: &Path, _metadata: &std::fs::Metadata) -> Result<(), &'static str> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetBinaryTypeW(path: *const u16, kind: *mut u32) -> i32;
    }
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut kind = 0u32;
    // SAFETY: NUL-terminated UTF-16 path and valid output pointer. Inspection only.
    if unsafe { GetBinaryTypeW(path.as_ptr(), &mut kind) } == 0 || !matches!(kind, 0 | 6) {
        return Err("artifact is not a native Windows executable");
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn executable(_: &Path, _: &std::fs::Metadata) -> Result<(), &'static str> {
    Err("artifact validation unsupported on this OS")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    VerifyArtifactAgain,
    CheckOwnershipLeaseAndGenerations,
    ReserveIdentifyThroughSharedQueue,
    StartCandidateWithoutBotToken,
    StartShadowGateway,
    VerifyReadinessAndCompatibility,
    PrepareSwitch,
    CommitRoutingAndCloseOldAdmission,
    CheckCandidateHealth,
    WaitTasksRestStateAndIpc,
    RetireOldOnlyWhenSettled,
    AbortPrepareKeepOld,
    CleanupUncommittedCandidateWhenSafe,
    RestoreRetainedOldIfSafe,
    DrainAbandonedKeepUnknownResults,
    StopPlanKeepGenerationsForReconciliation,
}

#[derive(Serialize)]
pub struct Stage {
    pub shard_id: u32,
    pub component: Component,
    pub from_generation: u64,
    pub to_generation: u64,
    pub identify_bucket: Option<u32>,
    pub steps: Vec<Step>,
    pub on_failure_before_commit: Vec<Step>,
    pub on_failure_after_commit: Vec<Step>,
}

#[derive(Serialize)]
pub struct Gate {
    pub status: &'static str,
    pub diagnostic: String,
}

#[derive(Serialize)]
pub struct RollingPlan {
    pub command: Action,
    pub component: Component,
    pub bot_id: String,
    pub total_shards: u32,
    pub additional_identifies: u32,
    /// Selected paths and expected hashes for review. Never put credentials in
    /// manifest paths. Verification is a separate filesystem operation.
    pub artifacts: Artifacts,
    pub gate: Gate,
    pub stages: Vec<Stage>,
    pub note: &'static str,
}

impl RollingPlan {
    pub fn executable(&self) -> bool {
        self.gate.status == "execute"
    }
}

/// Produce strictly sequential stages. Do not advance to another stage until
/// health and full settlement succeed. Abort remaining stages after any failure;
/// already-retired shards are not blindly switched back by this plan.
pub fn build_plan(
    manifest: &Manifest,
    selector: &str,
    action: Action,
    component: Component,
    elapsed: Duration,
) -> Result<RollingPlan, &'static str> {
    validate_manifest(manifest, selector, component)?;
    let required = if component == Component::Core {
        0
    } else {
        manifest.shards.len() as u32
    };
    let mut limits = SessionStartLimit {
        max_concurrency: 0,
        remaining: 0,
        reset_after: 0,
        total: 0,
    };
    let age = match &manifest.identify {
        Some(observation) if required > 0 => {
            limits = SessionStartLimit {
                max_concurrency: observation.limits.max_concurrency,
                remaining: observation.limits.remaining,
                reset_after: observation.limits.reset_after,
                total: observation.limits.total,
            };
            Duration::from_millis(observation.observed_age_ms)
                .checked_add(elapsed)
                .ok_or("Identify observation age overflow")?
        }
        _ => Duration::ZERO,
    };
    let decision = update_gate::evaluate_update(
        &limits,
        Duration::ZERO,
        age,
        update_gate::UpdatePlan {
            additional_identifies: required,
        },
    );
    let gate = Gate {
        status: match decision {
            UpdateDecision::Execute { .. } => "execute",
            UpdateDecision::Defer { .. } => "defer",
            UpdateDecision::Reject { .. } => "reject",
        },
        diagnostic: decision.diagnostic(),
    };
    let mut shards = manifest.shards.iter().collect::<Vec<_>>();
    shards.sort_unstable_by_key(|shard| shard.shard_id);
    let mut stages = Vec::new();
    for shard in shards {
        for part in selected(component) {
            let generations = generation(shard, *part).expect("validated component generations");
            let mut switch = Switch::new(generations.current);
            switch
                .prepare(generations.candidate)
                .map_err(|_| "invalid switch preparation")?;
            switch.commit().map_err(|_| "invalid switch commit")?;
            switch
                .rollback()
                .map_err(|_| "invalid rollback transition")?;
            let mut steps = vec![
                Step::VerifyArtifactAgain,
                Step::CheckOwnershipLeaseAndGenerations,
            ];
            steps.push(if *part == Component::Core {
                Step::StartCandidateWithoutBotToken
            } else {
                Step::ReserveIdentifyThroughSharedQueue
            });
            if *part == Component::Gateway {
                steps.push(Step::StartShadowGateway);
            }
            steps.extend([
                Step::VerifyReadinessAndCompatibility,
                Step::PrepareSwitch,
                Step::CommitRoutingAndCloseOldAdmission,
                Step::CheckCandidateHealth,
                Step::WaitTasksRestStateAndIpc,
                Step::RetireOldOnlyWhenSettled,
            ]);
            stages.push(Stage {
                shard_id: shard.shard_id,
                component: *part,
                from_generation: generations.current,
                to_generation: generations.candidate,
                identify_bucket: if *part == Component::Gateway && limits.max_concurrency > 0 {
                    Some(shard.shard_id % u32::from(limits.max_concurrency))
                } else {
                    None
                },
                steps,
                on_failure_before_commit: vec![
                    Step::AbortPrepareKeepOld,
                    Step::CleanupUncommittedCandidateWhenSafe,
                    Step::StopPlanKeepGenerationsForReconciliation,
                ],
                on_failure_after_commit: vec![
                    Step::RestoreRetainedOldIfSafe,
                    Step::DrainAbandonedKeepUnknownResults,
                    Step::StopPlanKeepGenerationsForReconciliation,
                ],
            });
        }
    }
    Ok(RollingPlan {
        command: action,
        component,
        bot_id: manifest.bot_id.clone(),
        total_shards: manifest.total_shards,
        additional_identifies: required,
        artifacts: Artifacts {
            core: if component != Component::Gateway {
                manifest.artifacts.core.clone()
            } else {
                None
            },
            gateway: if component != Component::Core {
                manifest.artifacts.gateway.clone()
            } else {
                None
            },
        },
        gate,
        stages,
        note: "Offline estimate only; stages are serial and conditional on readiness, ownership/lease, fresh shared Identify reservation and complete drain. Stop on failure; rollback requires the retained old process. No runtime action was performed.",
    })
}

/// Validate all selected artifacts once before displaying a plan. Candidates are
/// never started, even with --yes. Canonical paths are not persisted.
pub fn verify_artifacts(
    manifest: &Manifest,
    component: Component,
    base: &Path,
) -> Result<(), &'static str> {
    for part in selected(component) {
        let artifact = match part {
            Component::Core => &manifest.artifacts.core,
            _ => &manifest.artifacts.gateway,
        }
        .as_ref()
        .ok_or("missing component artifact")?;
        verify_artifact(artifact, base)?;
    }
    Ok(())
}

/// A testable retirement gate for a future executor. Booleans must represent
/// positively confirmed facts under its routing lock, not timeout assumptions.
pub struct Settlement {
    pub rest_confirmed: bool,
    pub state_saved: bool,
    pub ipc_flushed: bool,
}

pub async fn finish_when_settled(
    switch: &mut Switch,
    tasks: &DrainTracker,
    settlement: impl FnOnce() -> Settlement,
    timeout: Duration,
) -> Result<u64, SwitchError> {
    if tokio::time::timeout(timeout, tasks.wait_drained())
        .await
        .is_err()
    {
        return switch.finish(false);
    }
    // Inspect the other channels after tasks stop creating new work/results.
    let settlement = settlement();
    if !settlement.rest_confirmed || !settlement.state_saved || !settlement.ipc_flushed {
        return switch.finish(false);
    }
    switch.finish(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Recovery {
    KeepOld,
    RestoredOld { active: u64 },
    HoldForReconciliation,
}

/// Pure failure handling only. The owner must apply routing/admission changes
/// atomically under its routing lock; this does not reopen a DrainTracker.
pub fn plan_failure(
    switch: &mut Switch,
    previous_can_resume: bool,
) -> Result<Recovery, SwitchError> {
    match switch.phase() {
        Phase::Prepared { .. } => {
            switch.abort_prepare()?;
            Ok(Recovery::KeepOld)
        }
        Phase::Draining { .. } if previous_can_resume => {
            switch.rollback()?;
            Ok(Recovery::RestoredOld {
                active: switch.active(),
            })
        }
        // Active might mean the old process has already been retired. Do not
        // invent an old generation or reuse a previous process identifier.
        _ => Ok(Recovery::HoldForReconciliation),
    }
}
