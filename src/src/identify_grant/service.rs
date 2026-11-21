use super::{PERMIT_TTL, Reply, VERSION, boot_now, nanos};
use crate::{
    daemon::{DaemonError, LaunchGeneration, ShardKey},
    local_identify::{LocalAdmission, LocalIdentifyAdmission, LocalIdentifyPolicy},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    path::Path,
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError},
    time::Duration,
};
use tokio::process::Command;

#[cfg(test)]
mod tests;

const JOURNAL_NAME: &str = "identify-send-grants.jsonl";
const MAX_JOURNAL: u64 = 16 * 1024 * 1024;
const MAX_LINE: usize = 4096;
const JOURNAL_VERSION: u8 = 2;
const RETRY_JOURNAL_SPACE: Duration = Duration::from_millis(100);
const UNAVAILABLE: LocalAdmission = LocalAdmission::Exhausted {
    retry_after: Duration::MAX,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum State {
    AwaitingRequest,
    Granted,
    Waiting,
    Exhausted,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Gateway {
    key: ShardKey,
    generation: LaunchGeneration,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    at_ns: u64,
    action: Action,
    sha256: String,
}

impl Record {
    fn checksum(&self) -> io::Result<String> {
        use std::fmt::Write as _;
        let canonical = serde_json::to_vec(&(self.version, self.at_ns, &self.action))?;
        let hash = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("ring TLS 1.3 cipher suite")
            .common
            .hash_provider
            .hash(&canonical);
        let mut hex = String::with_capacity(64);
        for byte in hash.as_ref() {
            write!(hex, "{byte:02x}").expect("writing to a String");
        }
        Ok(hex)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Boot {
        boot_id: String,
        hold_until_ns: u64,
    },
    Launch {
        gateway: Gateway,
    },
    Grant {
        gateway: Gateway,
        expires_ns: u64,
        converts_launch: bool,
    },
}

struct RestartSpace {
    launch: u64,
    first_grant: u64,
}

impl RestartSpace {
    fn for_target(key: &ShardKey, instance: [u8; 16]) -> io::Result<Self> {
        let gateway = Gateway {
            key: key.clone(),
            generation: LaunchGeneration {
                daemon_instance: instance,
                sequence: u64::MAX,
            },
        };
        let size = |action| -> io::Result<u64> {
            // Same JSON schema, with maximum-width dynamic fields and a
            // fixed-width digest. Serializing also counts escaped Bot IDs.
            let bytes = serde_json::to_vec(&Record {
                version: JOURNAL_VERSION,
                at_ns: u64::MAX,
                action,
                sha256: "0".repeat(64),
            })?;
            if bytes.len() > MAX_LINE {
                return Err(io::Error::other("Identify grant record too large"));
            }
            Ok(bytes.len() as u64 + 1)
        };
        Ok(Self {
            launch: size(Action::Launch {
                gateway: gateway.clone(),
            })?,
            first_grant: size(Action::Grant {
                gateway,
                expires_ns: u64::MAX,
                converts_launch: false, // Also covers a reservation that expires before use.
            })?,
        })
    }

    fn total(&self) -> u64 {
        self.launch + self.first_grant
    }
}

struct ReservedGrant {
    generation: LaunchGeneration,
    bytes: u64,
}

struct WriteRequest {
    bytes: Vec<u8>,
    ack: SyncSender<io::Result<()>>,
}

/// The worker owns the append descriptor and never touches daemon state.
fn start_writer(
    mut file: File,
    mut before_sync: impl FnMut() + Send + 'static,
) -> io::Result<SyncSender<WriteRequest>> {
    let (sender, receiver) = mpsc::sync_channel::<WriteRequest>(1);
    std::thread::Builder::new()
        .name("identify-journal".into())
        .spawn(move || {
            while let Ok(request) = receiver.recv() {
                let result = file.write_all(&request.bytes).and_then(|()| {
                    before_sync();
                    file.sync_all()
                });
                let failed = result.is_err();
                let _ = request.ack.send(result);
                if failed {
                    break;
                }
            }
        })?;
    Ok(sender)
}

/// The admission state for the shared launch/send history. Calls take the current
/// boot-relative time explicitly so replay and fault cases can be tested.
struct Budget {
    journal: File,
    writer: Option<SyncSender<WriteRequest>>,
    write_ack: Option<Receiver<io::Result<()>>>,
    len: u64,
    limit: u64,
    restart_space: HashMap<ShardKey, RestartSpace>,
    first_grant_space: HashMap<ShardKey, ReservedGrant>,
    policy: LocalIdentifyPolicy,
    history: HashMap<String, LocalIdentifyAdmission>,
    boot: Option<String>,
    last: Duration,
    hold_until: Duration,
    failed: bool,
}

impl Budget {
    fn open(
        root: &Path,
        boot: &str,
        now: Duration,
        policy: LocalIdentifyPolicy,
    ) -> io::Result<Self> {
        create_runtime_root(root)?;
        let mut journal = crate::core_units::open_append_private(&root.join(JOURNAL_NAME))?;
        // One service per journal, including while verifying and repairing it.
        if unsafe { libc::flock(journal.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        File::open(root)?.sync_all()?;
        let mut bytes = Vec::new();
        (&mut journal)
            .take(MAX_JOURNAL + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_JOURNAL {
            return Err(invalid_journal());
        }
        let needs_delimiter = !bytes.is_empty() && !bytes.ends_with(b"\n");
        let mut budget = Self {
            journal,
            writer: None,
            write_ack: None,
            len: bytes.len() as u64,
            limit: MAX_JOURNAL,
            restart_space: HashMap::new(),
            first_grant_space: HashMap::new(),
            policy,
            history: HashMap::new(),
            boot: None,
            last: Duration::ZERO,
            hold_until: Duration::ZERO,
            failed: false,
        };
        for entry in bytes.split_inclusive(|&b| b == b'\n') {
            let line = entry.strip_suffix(b"\n").unwrap_or(entry);
            if line.len() > MAX_LINE {
                return Err(invalid_journal());
            }
            let record: Record = serde_json::from_slice(line).map_err(|_| invalid_journal())?;
            if record.version != JOURNAL_VERSION || record.sha256 != record.checksum()? {
                return Err(invalid_journal());
            }
            budget.replay(record)?;
        }
        if needs_delimiter {
            // A missing delimiter can follow an already acknowledged grant.
            // Only repair after verifying the entire file, including that record.
            if budget.len == MAX_JOURNAL {
                return Err(invalid_journal());
            }
            budget.journal.write_all(b"\n")?;
            budget.journal.sync_all()?;
            budget.len += 1;
        }
        if budget.boot.as_deref() == Some(boot) {
            budget.observe(now)?;
        } else {
            if !valid_boot_id(boot) {
                return Err(invalid_journal());
            }
            let hold_until_ns = nanos(budget.boot_hold_until(now));
            budget.append(
                now,
                Action::Boot {
                    boot_id: boot.to_owned(),
                    hold_until_ns,
                },
            )?;
        }
        Ok(budget)
    }

    fn boot_hold_until(&self, now: Duration) -> Duration {
        if self.boot.is_some() {
            now.saturating_add(self.policy.window)
                .saturating_add(PERMIT_TTL)
        } else {
            Duration::ZERO
        }
    }

    fn bot(&mut self, id: &str) -> &mut LocalIdentifyAdmission {
        self.history
            .entry(id.to_owned())
            .or_insert_with(|| LocalIdentifyAdmission::new(self.policy))
    }

    fn observe(&mut self, now: Duration) -> io::Result<()> {
        if self.failed || now < self.last {
            self.failed = true;
            return Err(io::Error::other(
                "Identify grant clock or journal unavailable",
            ));
        }
        self.last = now;
        Ok(())
    }

    fn available(&mut self, now: Duration) -> LocalAdmission {
        if self.observe(now).is_err() {
            UNAVAILABLE
        } else if now < self.hold_until {
            LocalAdmission::Pending {
                retry_after: self.hold_until - now,
            }
        } else {
            LocalAdmission::Granted
        }
    }

    fn check(&mut self, bot: &str, now: Duration, required_slots: usize) -> LocalAdmission {
        match self.available(now) {
            LocalAdmission::Granted => self.bot(bot).check_slots(now, required_slots),
            result => result,
        }
    }

    fn unreserved_bytes(&self) -> u64 {
        let restart: u64 = self.restart_space.values().map(RestartSpace::total).sum();
        let first_grants: u64 = self
            .first_grant_space
            .values()
            .map(|grant| grant.bytes)
            .sum();
        self.limit.saturating_sub(self.len + restart + first_grants)
    }

    fn reserve_restart_space(
        &mut self,
        targets: &[ShardKey],
        instance: [u8; 16],
    ) -> Result<(), DaemonError> {
        if self.failed {
            return Err(DaemonError::IdentifyUnavailable);
        }
        if targets
            .iter()
            .any(|key| self.restart_space.contains_key(key))
        {
            return Err(DaemonError::IdentifyJournalBusy);
        }
        let reservations: HashMap<_, _> = targets
            .iter()
            .map(|key| {
                RestartSpace::for_target(key, instance)
                    .map(|space| (key.clone(), space))
                    .map_err(|_| DaemonError::IdentifyJournalCapacity)
            })
            .collect::<Result<_, _>>()?;
        if self.unreserved_bytes() < reservations.values().map(RestartSpace::total).sum::<u64>() {
            return Err(DaemonError::IdentifyJournalCapacity);
        }
        self.restart_space.extend(reservations);
        Ok(())
    }

    #[cfg(test)]
    fn launch(&mut self, gateway: Gateway, now: Duration, slots: usize) -> LocalAdmission {
        self.launch_with_reservation(gateway, now, slots, false)
    }

    fn launch_with_reservation(
        &mut self,
        gateway: Gateway,
        now: Duration,
        slots: usize,
        reserved_restart: bool,
    ) -> LocalAdmission {
        let admission = self.check(&gateway.key.bot_id, now, slots);
        if admission != LocalAdmission::Granted {
            return admission;
        }
        let reserved = if reserved_restart {
            let Some(space) = self.restart_space.remove(&gateway.key) else {
                return UNAVAILABLE;
            };
            Some(space)
        } else {
            None
        };
        match self.append(
            now,
            Action::Launch {
                gateway: gateway.clone(),
            },
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return LocalAdmission::Pending {
                    retry_after: RETRY_JOURNAL_SPACE,
                };
            }
            Err(_) => return UNAVAILABLE,
        }
        if let Some(space) = reserved {
            self.first_grant_space.insert(
                gateway.key,
                ReservedGrant {
                    generation: gateway.generation,
                    bytes: space.first_grant,
                },
            );
        }
        LocalAdmission::Granted
    }

    fn identify(
        &mut self,
        gateway: &Gateway,
        now: Duration,
        first: bool,
        restart_reserved: bool,
        fallback_reserved: bool,
    ) -> (State, Reply) {
        let admission = self.available(now);
        if self.failed {
            return (State::Unavailable, Reply::Unavailable);
        }
        if admission != LocalAdmission::Granted {
            return denial(admission);
        }
        if restart_reserved {
            return (
                State::Waiting,
                Reply::Wait(nanos(Duration::from_millis(100))),
            );
        }
        let mut history = self.bot(&gateway.key.bot_id).clone();
        let converts_launch = first && history.remove_launch(now, &gateway.generation);
        let required_slots = if fallback_reserved { 2 } else { 1 };
        let admission = history.check_slots(now, required_slots);
        if admission != LocalAdmission::Granted {
            return denial(admission);
        }
        let expires_ns = nanos(now.saturating_add(PERMIT_TTL));
        let action = Action::Grant {
            gateway: gateway.clone(),
            expires_ns,
            converts_launch,
        };
        self.release_first_grant_space(&gateway.key, &gateway.generation);
        match self.append(now, action) {
            Ok(()) => (State::Granted, Reply::Permit(expires_ns)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                (State::Waiting, Reply::Wait(nanos(RETRY_JOURNAL_SPACE)))
            }
            Err(_) => (State::Unavailable, Reply::Unavailable),
        }
    }

    fn release_first_grant_space(&mut self, key: &ShardKey, generation: &LaunchGeneration) {
        if self
            .first_grant_space
            .get(key)
            .is_some_and(|grant| &grant.generation == generation)
        {
            self.first_grant_space.remove(key);
        }
    }

    fn append(&mut self, at: Duration, action: Action) -> io::Result<()> {
        let mut record = Record {
            version: JOURNAL_VERSION,
            at_ns: nanos(at),
            action,
            sha256: String::new(),
        };
        record.sha256 = record.checksum()?;
        let mut bytes = serde_json::to_vec(&record)?;
        if bytes.len() > MAX_LINE || bytes.len() as u64 + 1 > self.limit.saturating_sub(self.len) {
            return Err(io::Error::other("Identify grant journal full"));
        }
        if bytes.len() as u64 + 1 > self.unreserved_bytes() {
            // A pending restart owns the remaining room and can still release it.
            return Err(io::ErrorKind::WouldBlock.into());
        }
        bytes.push(b'\n');
        let result = (|| {
            let len = bytes.len() as u64;
            if let Some(writer) = &self.writer {
                debug_assert!(self.write_ack.is_none(), "one submitted write at a time");
                let (ack, result) = mpsc::sync_channel(1);
                writer
                    .try_send(WriteRequest { bytes, ack })
                    .map_err(|_| io::Error::other("Identify grant journal worker unavailable"))?;
                self.write_ack = Some(result);
            } else {
                // Startup replay and its boot marker finish before the owner loop.
                self.journal.write_all(&bytes)?;
                self.journal.sync_all()?;
            }
            // Conservatively count a submitted write immediately. Its result
            // cannot reach a Gateway or authorize spawn before the worker ACK.
            self.len += len;
            self.replay(record)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn replay(&mut self, record: Record) -> io::Result<()> {
        let at = Duration::from_nanos(record.at_ns);
        if let Action::Boot {
            boot_id,
            hold_until_ns,
        } = record.action
        {
            if !valid_boot_id(&boot_id)
                || self.boot.as_ref() == Some(&boot_id)
                || hold_until_ns != nanos(self.boot_hold_until(at))
            {
                return Err(invalid_journal());
            }
            self.boot = Some(boot_id);
            self.last = at;
            self.hold_until = Duration::from_nanos(hold_until_ns);
            self.history.clear();
            return Ok(());
        }
        if self.boot.is_none() || self.available(at) != LocalAdmission::Granted {
            return Err(invalid_journal());
        }
        match record.action {
            Action::Launch { gateway } => {
                self.bot(&gateway.key.bot_id)
                    .record_launch(at, gateway.generation);
            }
            Action::Grant {
                gateway,
                expires_ns,
                converts_launch,
            } => {
                if expires_ns != nanos(at.saturating_add(PERMIT_TTL)) {
                    return Err(invalid_journal());
                }
                let bot = self.bot(&gateway.key.bot_id);
                if converts_launch && !bot.remove_launch(at, &gateway.generation) {
                    return Err(invalid_journal());
                }
                bot.record_permit(Duration::from_nanos(expires_ns));
            }
            Action::Boot { .. } => unreachable!("boot handled before observation"),
        }
        Ok(())
    }
}

fn valid_boot_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

fn invalid_journal() -> io::Error {
    io::Error::other("invalid Identify grant journal")
}

fn create_runtime_root(root: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    for path in root.ancestors() {
        match path.metadata() {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => missing.push(path),
            Err(error) => return Err(error),
        }
    }
    std::fs::create_dir_all(root)?;
    // If this invocation created directories, persist their entries too.
    for path in missing {
        File::open(path.parent().ok_or_else(invalid_journal)?)?.sync_all()?;
    }
    Ok(())
}

fn denial(admission: LocalAdmission) -> (State, Reply) {
    match admission {
        LocalAdmission::Pending { retry_after } => {
            (State::Waiting, Reply::Wait(nanos(retry_after)))
        }
        LocalAdmission::Exhausted { retry_after } => {
            (State::Exhausted, Reply::Wait(nanos(retry_after)))
        }
        LocalAdmission::Granted => unreachable!("denial only"),
    }
}

struct Peer {
    gateway: Gateway,
    socket: UnixStream,
    first: bool,
    standby: bool,
    state: State,
}

impl Peer {
    fn reply(&mut self, (state, reply): (State, Reply)) -> bool {
        self.state = state;
        self.socket.write_all(&reply.encode()).is_ok() && state != State::Unavailable
    }
}

struct PendingLaunch {
    gateway: Gateway,
    standby: bool,
    durable: bool,
}

enum WriteTarget {
    Launch,
    Grant { gateway: Gateway, expires_ns: u64 },
}

struct PendingWrite {
    ack: Receiver<io::Result<()>>,
    target: WriteTarget,
}

pub(crate) struct Service {
    budget: Budget,
    peers: Vec<Peer>,
    launch: Option<PendingLaunch>,
    launch_waiting: bool,
    write: Option<PendingWrite>,
}

impl Service {
    pub(crate) fn open(root: &Path, policy: LocalIdentifyPolicy) -> io::Result<Self> {
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        Self::with_writer(Budget::open(root, boot.trim(), boot_now()?, policy)?, || {})
    }

    #[cfg(test)]
    pub(crate) fn open_with_sync_hook(
        root: &Path,
        policy: LocalIdentifyPolicy,
        before_sync: impl FnMut() + Send + 'static,
    ) -> io::Result<Self> {
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        Self::with_writer(
            Budget::open(root, boot.trim(), boot_now()?, policy)?,
            before_sync,
        )
    }

    pub(crate) fn check_restart_writer(&mut self) -> Result<(), DaemonError> {
        self.settle_write(false);
        if self.budget.failed {
            Err(DaemonError::IdentifyUnavailable)
        } else if self.write.is_some() || self.launch.is_some() || self.launch_waiting {
            Err(DaemonError::IdentifyJournalBusy)
        } else {
            Ok(())
        }
    }

    fn with_writer(
        mut budget: Budget,
        before_sync: impl FnMut() + Send + 'static,
    ) -> io::Result<Self> {
        budget.writer = Some(start_writer(budget.journal.try_clone()?, before_sync)?);
        Ok(Self {
            budget,
            peers: Vec::new(),
            launch: None,
            launch_waiting: false,
            write: None,
        })
    }

    pub(crate) fn check(&mut self, bot: &str, slots: usize) -> LocalAdmission {
        self.settle_write(false);
        match self.observe_clock() {
            Some(now) => self.budget.check(bot, now, slots),
            None => UNAVAILABLE,
        }
    }

    /// An in-flight write defers launch on the owner loop. Exactly one launch
    /// owns the next process sequence until its ACK is consumed or cancelled.
    pub(crate) fn launch(
        &mut self,
        key: &ShardKey,
        generation: LaunchGeneration,
        standby: bool,
        reserved_restart: bool,
    ) -> Result<LocalAdmission, crate::local_identify::JournalPending> {
        use crate::local_identify::JournalPending;
        self.settle_write(false);
        let Some(now) = self.observe_clock() else {
            return Ok(UNAVAILABLE);
        };
        if self.budget.observe(now).is_err() {
            return Ok(UNAVAILABLE);
        }
        if let Some(launch) = &self.launch {
            if launch.gateway.key != *key || launch.standby != standby || !launch.durable {
                return Err(JournalPending);
            }
            if launch.gateway.generation != generation {
                self.fail_journal();
                return Ok(UNAVAILABLE);
            }
            self.launch = None;
            return Ok(LocalAdmission::Granted);
        }
        if self.write.is_some() {
            self.launch_waiting = true;
            return Err(JournalPending);
        }
        self.launch_waiting = false;
        let gateway = Gateway {
            key: key.clone(),
            generation,
        };
        let slots = if standby { 2 } else { 1 };
        let admission =
            self.budget
                .launch_with_reservation(gateway.clone(), now, slots, reserved_restart);
        if self.queue_ack(WriteTarget::Launch) {
            self.launch = Some(PendingLaunch {
                gateway,
                standby,
                durable: false,
            });
            return Err(JournalPending);
        }
        Ok(admission)
    }

    pub(crate) fn cancel_launch(&mut self, key: &ShardKey, standby: bool) -> Option<u64> {
        if self
            .launch
            .as_ref()
            .is_some_and(|launch| launch.gateway.key == *key && launch.standby == standby)
        {
            let launch = self.launch.take().unwrap();
            self.budget
                .release_first_grant_space(key, &launch.gateway.generation);
            return Some(launch.gateway.generation.sequence);
        }
        None
    }

    fn queue_ack(&mut self, target: WriteTarget) -> bool {
        if let Some(ack) = self.budget.write_ack.take() {
            debug_assert!(self.write.is_none());
            self.write = Some(PendingWrite { ack, target });
            return true;
        }
        false
    }

    fn fail_journal(&mut self) {
        self.budget.failed = true;
        eprintln!("{}", super::JOURNAL_UNAVAILABLE_LOG);
    }

    /// Only the direct, synchronous Daemon::start compatibility API waits.
    /// The daemon control plane and automatic recovery always use try_recv.
    pub(crate) fn wait_for_write(&mut self) -> bool {
        let was_pending = self.write.is_some();
        self.settle_write(true);
        was_pending
    }

    fn settle_write(&mut self, wait: bool) {
        let Some(write) = &self.write else {
            return;
        };
        let result = if wait {
            write.ack.recv().map_err(|_| TryRecvError::Disconnected)
        } else {
            write.ack.try_recv()
        };
        if matches!(result, Err(TryRecvError::Empty)) {
            return;
        }
        let target = self.write.take().unwrap().target;
        if !matches!(result, Ok(Ok(()))) {
            self.fail_journal();
        }
        match target {
            WriteTarget::Launch => {
                if let Some(launch) = &mut self.launch {
                    launch.durable = !self.budget.failed;
                }
            }
            WriteTarget::Grant {
                gateway,
                expires_ns,
            } => {
                let state_and_reply = match self.observe_clock() {
                    Some(now) if self.budget.observe(now).is_ok() => {
                        if nanos(now) < expires_ns {
                            (State::Granted, Reply::Permit(expires_ns))
                        } else {
                            (
                                State::Waiting,
                                Reply::Wait(nanos(Duration::from_millis(25))),
                            )
                        }
                    }
                    _ => (State::Unavailable, Reply::Unavailable),
                };
                self.peers.retain_mut(|peer| {
                    if peer.gateway != gateway {
                        return true;
                    }
                    peer.first = false; // The submitted charge is never refunded.
                    peer.reply(state_and_reply)
                });
            }
        }
    }

    pub(crate) fn reserve_restart_space(
        &mut self,
        targets: &[ShardKey],
        instance: [u8; 16],
    ) -> Result<(), DaemonError> {
        self.budget.reserve_restart_space(targets, instance)
    }

    pub(crate) fn release_restart_space(&mut self, targets: &[ShardKey]) {
        for key in targets {
            self.budget.restart_space.remove(key);
        }
    }

    pub(crate) fn release_first_grant_space(
        &mut self,
        key: &ShardKey,
        generation: &LaunchGeneration,
    ) {
        self.budget.release_first_grant_space(key, generation);
    }

    #[cfg(test)]
    pub(crate) fn limit_journal_remaining_to_one_restart(
        &mut self,
        key: &ShardKey,
        instance: [u8; 16],
    ) {
        let required = RestartSpace::for_target(key, instance).unwrap().total();
        self.budget.limit = self.budget.len + required;
    }

    fn observe_clock(&mut self) -> Option<Duration> {
        match boot_now() {
            Ok(now) => Some(now),
            Err(_) => {
                self.budget.failed = true;
                None
            }
        }
    }

    pub(crate) fn attach(
        &mut self,
        command: &mut Command,
        key: &ShardKey,
        generation: LaunchGeneration,
        standby: bool,
    ) -> io::Result<UnixStream> {
        let (socket, child) = UnixStream::pair()?;
        socket.set_nonblocking(true)?;
        let fd = child.as_raw_fd();
        command.arg(super::GATEWAY_FD_FLAG).arg(fd.to_string());
        // Only the forked child loses close-on-exec. The caller keeps the
        // returned endpoint alive through spawn and then drops its copy.
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        self.peers.push(Peer {
            gateway: Gateway {
                key: key.clone(),
                generation,
            },
            socket,
            first: true,
            standby,
            state: State::AwaitingRequest,
        });
        Ok(child)
    }

    pub(crate) fn promote(&mut self, key: &ShardKey, generation: &LaunchGeneration) {
        self.peers.retain_mut(|peer| {
            if &peer.gateway.key != key {
                true
            } else if &peer.gateway.generation == generation {
                peer.standby = false;
                true
            } else {
                self.budget
                    .release_first_grant_space(&peer.gateway.key, &peer.gateway.generation);
                false
            }
        });
    }

    pub(crate) fn poll(&mut self, reservations: impl Fn(&str) -> (bool, bool)) {
        self.settle_write(false);
        if self.write.is_some() {
            return;
        }
        // Give a management launch waiting for this writer a tick to submit
        // before another peer starts a new sync.
        if std::mem::take(&mut self.launch_waiting) {
            return;
        }
        let budget = &mut self.budget;
        let standby_launch_bot = self
            .launch
            .as_ref()
            .filter(|launch| launch.standby)
            .map(|launch| launch.gateway.key.bot_id.as_str());
        let mut grant = None;
        self.peers.sort_by_key(|peer| peer.standby);
        self.peers.retain_mut(|peer| {
            if grant.is_some() {
                return true;
            }
            let mut request = [0; 2];
            match peer.socket.read(&mut request) {
                Ok(1) if request[0] == VERSION => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    return true;
                }
                _ => {
                    budget.release_first_grant_space(&peer.gateway.key, &peer.gateway.generation);
                    return false;
                }
            }
            let (restart, fallback) = reservations(&peer.gateway.key.bot_id);
            let (state, reply) = match boot_now() {
                Ok(now) => budget.identify(
                    &peer.gateway,
                    now,
                    peer.first,
                    restart,
                    fallback || standby_launch_bot == Some(peer.gateway.key.bot_id.as_str()),
                ),
                Err(_) => {
                    budget.failed = true;
                    (State::Unavailable, Reply::Unavailable)
                }
            };
            if let Reply::Permit(expires_ns) = reply {
                grant = Some(WriteTarget::Grant {
                    gateway: peer.gateway.clone(),
                    expires_ns,
                });
                peer.state = State::Waiting;
                return true;
            }
            let keep = peer.reply((state, reply));
            if !keep {
                budget.release_first_grant_space(&peer.gateway.key, &peer.gateway.generation);
            }
            keep
        });
        if let Some(grant) = grant {
            assert!(self.queue_ack(grant), "grant has a pending durable write");
        }
    }

    pub(crate) fn status(&self, key: &ShardKey) -> serde_json::Value {
        let state = |standby| {
            self.peers
                .iter()
                .rev()
                .find(|p| &p.gateway.key == key && p.standby == standby)
                .map(|p| p.state)
        };
        serde_json::json!({
            "version": VERSION,
            "journal": if self.budget.failed { "unavailable" } else { "ready" },
            "journal_unreserved_bytes": self.budget.unreserved_bytes(),
            "active": state(false),
            "standby": state(true),
        })
    }
}
