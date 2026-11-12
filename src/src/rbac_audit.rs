//! Fail-closed management authorization and durable, append-only JSON Lines audit.
//!
//! This is a synchronous management-plane API, not an authentication transport.
//! Only a trusted authentication adapter may construct an authenticated principal;
//! never deserialize that decision or the role from an untrusted command body.
//! Keep the log directory private and stable. Run blocking I/O away from Gateway
//! heartbeat/event tasks. Existing CLI/daemon handlers are not wired to this module.
//!
//! ```no_run
//! use src::rbac_audit::{AuditConfig, AuditedExecutor, Confirmation, Principal, Request};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut executor = AuditedExecutor::open("private/audit.jsonl", AuditConfig::default())?;
//! // Only after transport authentication and server-side role/target resolution:
//! let request = Request {
//!     principal: Principal::authenticated("operator-one", "updater"),
//!     operation: "update".into(), bot_id: Some("bot-one".into()), shard_id: Some(0),
//!     remote: true, confirmation: Confirmation::None,
//! };
//! executor.execute(&request, || Ok(()))?; // Replace with the actual management action.
//! # Ok(())
//! # }
//! ```
use crate::redact::redact;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Viewer,
    Updater,
    Admin,
}

impl Role {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "updater" => Some(Self::Updater),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    pub fn allows(self, operation: Operation) -> bool {
        match operation {
            Operation::Status | Operation::Inspect | Operation::Logs => true,
            Operation::Update | Operation::Rollback => self != Self::Viewer,
            Operation::Stop
            | Operation::Force
            | Operation::OwnershipSwitch
            | Operation::Shutdown => self == Self::Admin,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Status,
    Inspect,
    Logs,
    Update,
    Rollback,
    Stop,
    Force,
    OwnershipSwitch,
    Shutdown,
}

impl Operation {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "status" => Some(Self::Status),
            "inspect" => Some(Self::Inspect),
            "logs" => Some(Self::Logs),
            "update" => Some(Self::Update),
            "rollback" => Some(Self::Rollback),
            "stop" => Some(Self::Stop),
            "force" => Some(Self::Force),
            "ownership-switch" => Some(Self::OwnershipSwitch),
            "shutdown" => Some(Self::Shutdown),
            _ => None,
        }
    }

    /// Graceful shutdown is the stop alias from architecture §4.1.
    pub fn requires_confirmation(self) -> bool {
        matches!(self, Self::Force | Self::OwnershipSwitch)
    }
}

/// Authentication assertions come from a trusted local/mTLS adapter, not the client.
/// Deliberately has no Deserialize or Debug implementation.
pub struct Principal {
    subject: Option<String>,
    role: Option<String>,
}

impl Principal {
    pub fn authenticated(subject: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            subject: Some(subject.into()),
            role: Some(role.into()),
        }
    }

    pub fn unauthenticated() -> Self {
        Self {
            subject: None,
            role: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Confirmation {
    #[default]
    None,
    Interactive {
        explicit: bool,
        accepted: bool,
    },
    NonInteractive {
        explicit: bool,
        yes: bool,
    },
}

impl Confirmation {
    fn satisfied(self) -> bool {
        matches!(
            self,
            Self::Interactive {
                explicit: true,
                accepted: true
            } | Self::NonInteractive {
                explicit: true,
                yes: true
            }
        )
    }
}

/// Targets must be canonical IDs resolved by the caller before execution.
/// No Debug/Deserialize: do not log raw request strings or trust wire identities.
pub struct Request {
    pub principal: Principal,
    pub operation: String,
    pub bot_id: Option<String>,
    pub shard_id: Option<u32>,
    pub remote: bool,
    pub confirmation: Confirmation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    Unauthenticated,
    UnknownRole,
    InsufficientPermission,
    UnknownOperation,
    ConfirmationRequired,
    InvalidSubject,
    InvalidTarget,
}

impl Denial {
    fn reason(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::UnknownRole => "unknown_role",
            Self::InsufficientPermission => "insufficient_permission",
            Self::UnknownOperation => "unknown_operation",
            Self::ConfirmationRequired => "confirmation_required",
            Self::InvalidSubject => "invalid_subject",
            Self::InvalidTarget => "invalid_target",
        }
    }
}

fn authorize(request: &Request) -> Result<(), Denial> {
    let subject = request
        .principal
        .subject
        .as_deref()
        .ok_or(Denial::Unauthenticated)?;
    if subject.trim().is_empty() {
        return Err(Denial::InvalidSubject);
    }
    let role = request
        .principal
        .role
        .as_deref()
        .and_then(Role::parse)
        .ok_or(Denial::UnknownRole)?;
    let operation = Operation::parse(&request.operation).ok_or(Denial::UnknownOperation)?;
    if !role.allows(operation) {
        return Err(Denial::InsufficientPermission);
    }
    if request
        .bot_id
        .as_ref()
        .is_some_and(|bot| bot.trim().is_empty())
        || (request.shard_id.is_some() && request.bot_id.is_none())
        || (!matches!(operation, Operation::Status | Operation::Logs) && request.bot_id.is_none())
    {
        return Err(Denial::InvalidTarget);
    }
    if operation.requires_confirmation() && !request.confirmation.satisfied() {
        return Err(Denial::ConfirmationRequired);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditConfig {
    /// Includes the trailing newline; applies to each segment independently.
    pub max_file_bytes: u64,
    pub max_record_bytes: usize,
    /// Number of additional segments after the base file. Nothing is deleted.
    pub max_archives: u32,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: 8 * 1024 * 1024,
            max_record_bytes: 16 * 1024,
            max_archives: 16,
        }
    }
}

/// Fixed classifications intentionally omit OS messages and paths (possible secrets).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditError {
    InvalidConfig,
    Io,
    CorruptLog,
    RecordTooLarge,
    CapacityExhausted,
    Busy,
    Poisoned,
    Clock,
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "audit unavailable: {self:?}")
    }
}
impl std::error::Error for AuditError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecuteError {
    Denied(Denial),
    /// The closure has not run.
    Audit(AuditError),
    OperationFailed,
    /// The closure already ran; do not retry automatically or claim rollback.
    AuditAfterExecution(AuditError),
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "management operation failed: {self:?}")
    }
}
impl std::error::Error for ExecuteError {}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Denied,
    Started,
    Succeeded,
    Failed,
    Panicked,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    timestamp_unix_ms: u64,
    request_id: u64,
    subject: String,
    role: Option<String>,
    operation: String,
    bot_id: Option<String>,
    shard_id: Option<u32>,
    remote: bool,
    result: Outcome,
    reason: String,
}

/// A single exclusive writer. Segment 0 stays locked for the entire lifetime,
/// even after rollover to `path.1`, `path.2`, etc. Locks are advisory on Unix.
/// Existing segment contents are validated without unbounded line allocation.
pub struct AuditedExecutor {
    path: PathBuf,
    config: AuditConfig,
    base: File,
    current: Option<File>,
    segment: u32,
    size: u64,
    next_id: u64,
    poisoned: bool,
}

impl AuditedExecutor {
    /// Parent directory must already exist and be private to the daemon user.
    /// Unix new files use 0600; Windows inherits directory ACLs (not verified).
    pub fn open(path: impl AsRef<Path>, config: AuditConfig) -> Result<Self, AuditError> {
        if config.max_record_bytes == 0
            || config.max_record_bytes > 1024 * 1024
            || config.max_record_bytes as u64 > config.max_file_bytes
            || config.max_archives > 1024
        {
            return Err(AuditError::InvalidConfig);
        }
        let path = path.as_ref().to_path_buf();
        // Do not silently recreate a missing base in an existing rotated series.
        // In addition to losing history, doing so would create a new lock inode.
        if !path.try_exists().map_err(|_| AuditError::Io)? {
            let mut first = path.as_os_str().to_os_string();
            first.push(".1");
            match fs::symlink_metadata(PathBuf::from(first)) {
                Ok(_) => return Err(AuditError::CorruptLog),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(_) => return Err(AuditError::Io),
            }
        }
        let base = open_segment(&path, false)?;
        match base.try_lock() {
            Ok(()) => (),
            Err(fs::TryLockError::WouldBlock) => return Err(AuditError::Busy),
            Err(fs::TryLockError::Error(_)) => return Err(AuditError::Io),
        }
        let mut this = Self {
            path,
            config,
            base,
            current: None,
            segment: 0,
            size: 0,
            next_id: 1,
            poisoned: false,
        };
        let (size, last_id) = validate_segment(&mut this.base, config, 0)?;
        this.size = size;
        let mut max_id = last_id;
        let mut gap = false;
        for index in 1..=config.max_archives + 1 {
            let path = this.segment_path(index);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    if gap || index > config.max_archives {
                        return Err(AuditError::CorruptLog);
                    }
                    let mut file = open_segment(&path, false)?;
                    let (size, last_id) = validate_segment(&mut file, config, max_id)?;
                    max_id = max_id.max(last_id);
                    this.current = Some(file);
                    this.segment = index;
                    this.size = size;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => gap = true,
                Err(_) => return Err(AuditError::Io),
            }
        }
        this.next_id = max_id.checked_add(1).ok_or(AuditError::CorruptLog)?;
        Ok(this)
    }

    /// Writes and syncs `started` before invoking the closure exactly once.
    /// Rejections are audited too. Audit failure before invocation prevents it.
    /// A crash/panic or completion-write failure cannot roll back external effects;
    /// an unmatched started record means outcome unknown and requires reconciliation.
    pub fn execute<T, F>(&mut self, request: &Request, operation: F) -> Result<T, ExecuteError>
    where
        F: FnOnce() -> Result<T, String>,
    {
        if self.poisoned {
            return Err(ExecuteError::Audit(AuditError::Poisoned));
        }
        // Bound redaction and serialization work on incoming metadata.
        let input_bytes = request
            .principal
            .subject
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(request.principal.role.as_ref().map_or(0, String::len))
            .saturating_add(request.operation.len())
            .saturating_add(request.bot_id.as_ref().map_or(0, String::len));
        if input_bytes > self.config.max_record_bytes {
            return Err(ExecuteError::Audit(AuditError::RecordTooLarge));
        }
        let id = self.next_id;
        self.next_id = id
            .checked_add(1)
            .ok_or(ExecuteError::Audit(AuditError::CapacityExhausted))?;
        if let Err(denial) = authorize(request) {
            self.record(request, id, Outcome::Denied, denial.reason())
                .map_err(ExecuteError::Audit)?;
            return Err(ExecuteError::Denied(denial));
        }
        // Reserve the worst-case bounded completion size before any side effect.
        // This is a capacity reservation, not a promise against subsequent I/O
        // failure. Private directory ownership and the exclusive writer are required.
        let started = self
            .encode_record(request, id, Outcome::Started, "authorized")
            .map_err(ExecuteError::Audit)?;
        self.ensure_capacity(&[started.len(), self.config.max_record_bytes])
            .map_err(ExecuteError::Audit)?;
        if let Err(error) = self.append(&started) {
            self.poisoned = true;
            return Err(ExecuteError::Audit(error));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
        match result {
            Ok(Ok(value)) => {
                self.record(request, id, Outcome::Succeeded, "completed")
                    .map_err(ExecuteError::AuditAfterExecution)?;
                Ok(value)
            }
            Ok(Err(reason)) => {
                self.record(request, id, Outcome::Failed, &reason)
                    .map_err(ExecuteError::AuditAfterExecution)?;
                Err(ExecuteError::OperationFailed)
            }
            Err(payload) => {
                // Never format the panic payload; application panic hooks are a
                // separate logging boundary and must redact their own output.
                let _ = self.record(request, id, Outcome::Panicked, "operation_panicked");
                std::panic::resume_unwind(payload)
            }
        }
    }

    fn record(
        &mut self,
        request: &Request,
        id: u64,
        result: Outcome,
        reason: &str,
    ) -> Result<(), AuditError> {
        let result = self
            .encode_record(request, id, result, reason)
            .and_then(|bytes| self.append(&bytes));
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn encode_record(
        &self,
        request: &Request,
        id: u64,
        result: Outcome,
        reason: &str,
    ) -> Result<Vec<u8>, AuditError> {
        let timestamp_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|time| u64::try_from(time.as_millis()).ok())
            .ok_or(AuditError::Clock)?;
        let mut reason = redact(reason);
        // Redact before truncating, so cutting a credential cannot bypass masking.
        if reason.len() > 512 {
            let mut end = 512;
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            reason.truncate(end);
            reason.push_str("[truncated]");
        }
        let mut record = Record {
            timestamp_unix_ms,
            request_id: id,
            subject: redact(request.principal.subject.as_deref().unwrap_or("anonymous")),
            role: request.principal.role.as_deref().map(redact),
            operation: redact(&request.operation),
            bot_id: request.bot_id.as_deref().map(redact),
            shard_id: request.shard_id,
            remote: request.remote,
            result,
            reason,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(|_| AuditError::Io)?;
        bytes.push(b'\n');
        if bytes.len() > self.config.max_record_bytes
            && matches!(
                result,
                Outcome::Succeeded | Outcome::Failed | Outcome::Panicked
            )
        {
            // An unusually large/escaped error cannot make completion exceed
            // the reserved record bound. Metadata is identical to `started`.
            record.reason = "omitted".into();
            bytes = serde_json::to_vec(&record).map_err(|_| AuditError::Io)?;
            bytes.push(b'\n');
        }
        if bytes.len() > self.config.max_record_bytes {
            return Err(AuditError::RecordTooLarge);
        }
        Ok(bytes)
    }

    fn ensure_capacity(&self, lengths: &[usize]) -> Result<(), AuditError> {
        let mut segment = self.segment;
        let mut size = self.size;
        for &len in lengths {
            if len as u64 > self.config.max_file_bytes - size {
                if segment >= self.config.max_archives {
                    return Err(AuditError::CapacityExhausted);
                }
                segment += 1;
                size = 0;
            }
            size += len as u64;
        }
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), AuditError> {
        if bytes.len() as u64 > self.config.max_file_bytes - self.size {
            if self.segment >= self.config.max_archives {
                return Err(AuditError::CapacityExhausted);
            }
            let path = self.segment_path(self.segment + 1);
            self.current = Some(open_segment(&path, true)?);
            self.segment += 1;
            self.size = 0;
        }
        let file = self.current.as_mut().unwrap_or(&mut self.base);
        // Detect accidental truncation/append by non-cooperating writers. Protection
        // against malicious same-user path replacement requires OS isolation.
        if file.metadata().map_err(|_| AuditError::Io)?.len() != self.size {
            return Err(AuditError::CorruptLog);
        }
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| AuditError::Io)?;
        self.size += bytes.len() as u64;
        Ok(())
    }

    fn segment_path(&self, index: u32) -> PathBuf {
        let mut name = self.path.as_os_str().to_os_string();
        name.push(format!(".{index}"));
        PathBuf::from(name)
    }
}

fn open_segment(path: &Path, new_only: bool) -> Result<File, AuditError> {
    // Reject symlinks and non-regular files before opening; the directory must be
    // trusted, since std alone does not provide race-free openat/O_NOFOLLOW here.
    match fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => return Err(AuditError::Io),
        Ok(_) if new_only => return Err(AuditError::Io),
        Ok(_) => (),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
        Err(_) => return Err(AuditError::Io),
    }
    let mut options = OpenOptions::new();
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // create_new prevents overwriting any existing entry, including a symlink.
    let (file, created) = if new_only {
        (options.create_new(true).open(path), true)
    } else {
        match options.open(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                (options.create_new(true).open(path), true)
            }
            result => (result, false),
        }
    };
    let file = file.map_err(|_| AuditError::Io)?;
    if created {
        file.sync_all().map_err(|_| AuditError::Io)?;
        #[cfg(unix)]
        {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| AuditError::Io)?;
        }
    }
    let meta = file.metadata().map_err(|_| AuditError::Io)?;
    if !meta.is_file() {
        return Err(AuditError::Io);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o077 != 0 || meta.nlink() != 1 {
            return Err(AuditError::Io);
        }
    }
    Ok(file)
}

fn validate_segment(
    file: &mut File,
    config: AuditConfig,
    previous_id: u64,
) -> Result<(u64, u64), AuditError> {
    let size = file.metadata().map_err(|_| AuditError::Io)?.len();
    if size > config.max_file_bytes {
        return Err(AuditError::CorruptLog);
    }
    file.seek(SeekFrom::Start(0)).map_err(|_| AuditError::Io)?;
    let mut reader = BufReader::new(file);
    let mut last_id = previous_id;
    loop {
        let mut line = Vec::new();
        reader
            .by_ref()
            .take(config.max_record_bytes as u64 + 1)
            .read_until(b'\n', &mut line)
            .map_err(|_| AuditError::Io)?;
        if line.is_empty() {
            break;
        }
        if line.len() > config.max_record_bytes || line.last() != Some(&b'\n') {
            return Err(AuditError::CorruptLog);
        }
        let record: Record = serde_json::from_slice(&line).map_err(|_| AuditError::CorruptLog)?;
        if record.request_id == 0 || record.request_id < last_id {
            return Err(AuditError::CorruptLog);
        }
        last_id = record.request_id;
    }
    Ok((size, last_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let name = format!("rbac-audit-io-{:032x}", u128::from_ne_bytes(nonce));
            let path = std::env::temp_dir().join(name);
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn log(&self) -> PathBuf {
            self.0.join("audit.jsonl")
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let root = fs::canonicalize(std::env::temp_dir()).unwrap();
            let path = fs::canonicalize(&self.0).unwrap();
            assert!(path.starts_with(root));
            assert!(
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("rbac-audit-io-")
            );
            fs::remove_dir_all(path).unwrap();
        }
    }
    fn request() -> Request {
        Request {
            principal: Principal::authenticated("test-operator", "admin"),
            operation: "stop".into(),
            bot_id: Some("bot-one".into()),
            shard_id: Some(0),
            remote: true,
            confirmation: Confirmation::None,
        }
    }

    #[test]
    fn actual_os_write_failure_prevents_operation_and_poisons_writer() {
        let dir = TestDirectory::new();
        let mut executor = AuditedExecutor::open(dir.log(), AuditConfig::default()).unwrap();
        let readonly = dir.0.join("readonly");
        fs::write(&readonly, b"").unwrap();
        // Substitute an actual read-only OS handle while retaining the base lock.
        // write_all now fails in the OS, rather than using a mock success/error.
        executor.current = Some(File::open(readonly).unwrap());
        let called = Cell::new(false);
        assert_eq!(
            executor.execute(&request(), || {
                called.set(true);
                Ok(())
            }),
            Err(ExecuteError::Audit(AuditError::Io))
        );
        assert!(!called.get());
        assert_eq!(
            executor.execute(&request(), || {
                called.set(true);
                Ok(())
            }),
            Err(ExecuteError::Audit(AuditError::Poisoned))
        );
        assert!(!called.get());
        drop(executor);
        assert!(fs::read(dir.log()).unwrap().is_empty());
    }

    #[test]
    fn completion_failure_reports_already_executed_and_blocks_further_operations() {
        let dir = TestDirectory::new();
        let mut executor = AuditedExecutor::open(
            dir.log(),
            AuditConfig {
                max_file_bytes: 1024,
                max_record_bytes: 512,
                max_archives: 16,
            },
        )
        .unwrap();
        let anonymous = Request {
            principal: Principal::unauthenticated(),
            ..request()
        };
        while executor.segment == 0 {
            assert_eq!(
                executor.execute(&anonymous, || Ok(())),
                Err(ExecuteError::Denied(Denial::Unauthenticated))
            );
        }
        // Extra segments are deliberately not OS-locked (the base lock guards
        // cooperating writers). An external accidental truncation is detected.
        let active = executor.segment_path(executor.segment);
        let called = Cell::new(0);
        assert_eq!(
            executor.execute(&request(), || {
                called.set(called.get() + 1);
                fs::write(active, b"").unwrap();
                Ok(())
            }),
            Err(ExecuteError::AuditAfterExecution(AuditError::CorruptLog))
        );
        assert_eq!(called.get(), 1);
        assert_eq!(
            executor.execute(&request(), || {
                called.set(called.get() + 1);
                Ok(())
            }),
            Err(ExecuteError::Audit(AuditError::Poisoned))
        );
        assert_eq!(called.get(), 1);
    }

    #[test]
    fn capacity_math_does_not_wrap_at_u64_max() {
        let dir = TestDirectory::new();
        let mut executor = AuditedExecutor::open(
            dir.log(),
            AuditConfig {
                max_file_bytes: u64::MAX,
                max_record_bytes: 1024,
                max_archives: 0,
            },
        )
        .unwrap();
        executor.size = u64::MAX - 100;
        assert_eq!(
            executor.ensure_capacity(&[101]),
            Err(AuditError::CapacityExhausted)
        );
        assert!(executor.ensure_capacity(&[100]).is_ok());
    }

    #[test]
    fn missing_base_does_not_create_a_new_lock_file_for_existing_archives() {
        let dir = TestDirectory::new();
        let mut first = dir.log().into_os_string();
        first.push(".1");
        fs::write(PathBuf::from(first), b"").unwrap();
        assert!(matches!(
            AuditedExecutor::open(dir.log(), AuditConfig::default()),
            Err(AuditError::CorruptLog)
        ));
        assert!(!dir.log().exists());
    }

    #[test]
    fn request_ids_cannot_move_backwards_between_segments() {
        let dir = TestDirectory::new();
        let config = AuditConfig {
            max_file_bytes: 1024,
            max_record_bytes: 512,
            max_archives: 16,
        };
        let mut executor = AuditedExecutor::open(dir.log(), config).unwrap();
        let anonymous = Request {
            principal: Principal::unauthenticated(),
            ..request()
        };
        while executor.segment == 0 {
            assert_eq!(
                executor.execute(&anonymous, || Ok(())),
                Err(ExecuteError::Denied(Denial::Unauthenticated))
            );
        }
        let next = executor.segment_path(1);
        drop(executor);
        // Simulate persisted corruption; opening must not append or repair it.
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&next).unwrap()).unwrap();
        record["request_id"] = 1.into();
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        fs::write(&next, &bytes).unwrap();
        assert!(matches!(
            AuditedExecutor::open(dir.log(), config),
            Err(AuditError::CorruptLog)
        ));
        assert_eq!(fs::read(next).unwrap(), bytes);
    }
}
