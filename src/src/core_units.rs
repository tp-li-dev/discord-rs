//! botd's supervision of the Core processes of one Shard in the separated layout (stage 1-C
//! of docs/gateway-core-separation-design.md).
//!
//! botd starts the Cores (`src --core-listen <socket> --core-persist`), hands each its
//! capability secret on stdin, restarts one that exits (bounded exponential backoff) and
//! stops them after the Gateway. A Core exits by itself when its stdin closes, so Cores
//! are asked to exit when botd dies: the pipe is the leash. The Gateway is not a parent of
//! the Cores and does not restart them; it only reconnects (see `Manager`).
//!
//! The Bot token is never given to a Core: its environment is cleared and holds only the
//! Core identity.

use crate::core_attach::{SLOTS, identity, prepare_dir, renew_secret, secret_path, socket_path};
use crate::protocol::{BotId, Shard};
use crate::redact::Secret;
use crate::secret_file::{SECRET_BYTES, load_secret_file, wipe};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const BASE_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A Core that stays up this long starts over with the shortest backoff.
const STABLE_AFTER: Duration = Duration::from_secs(60);
const LOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// How often a running Core log is checked for size.
const LOG_CHECK_EVERY: Duration = Duration::from_secs(60);
const MANUAL_STOP_GRACE: Duration = Duration::from_secs(5);
const MANUAL_REAP_GRACE: Duration = Duration::from_secs(5);
const MANUAL_LAUNCH_TIMEOUT: Duration = Duration::from_secs(5);
const FALLBACK_ID_BIT: u64 = 1 << 63;

/// Keeps a log that processes append to from growing without bound: when it is over
/// `max_bytes`, its content is copied to `<path>.1` (replacing an older copy) and the file is
/// emptied in place. The writers keep their descriptors (they append), so nothing is lost
/// except what was already older than one rotation. Returns whether it rotated. A symlink or
/// non-file at either path is left alone.
pub fn rotate_in_place(path: &Path, max_bytes: u64) -> std::io::Result<bool> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !meta.file_type().is_file() || meta.len() <= max_bytes {
        return Ok(false);
    }
    let mut older = path.as_os_str().to_owned();
    older.push(".1");
    let older = PathBuf::from(older);
    match std::fs::symlink_metadata(&older) {
        Ok(meta) if meta.file_type().is_file() => std::fs::remove_file(&older)?,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // One descriptor for both the copy and the truncation, and it must be the file that was
    // checked (same device and inode): a path swapped in since is never touched.
    let mut live = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let opened = live.metadata()?;
    if opened.dev() != meta.dev() || opened.ino() != meta.ino() || !opened.file_type().is_file() {
        return Ok(false);
    }
    let measured = opened.len();
    let mut copy = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&older)?;
    std::io::copy(&mut Read::by_ref(&mut live).take(measured), &mut copy)?;
    // Lines the writers appended while the copy ran: copy those too (a few rounds), then empty
    // the file. A writer that never pauses must not be able to starve the rotation, or the file
    // would grow without bound - so the last few bytes written between the final copy and the
    // truncation can be lost (a window of microseconds). Bounded size wins over completeness.
    for _ in 0..4 {
        let end = live.metadata()?.len();
        let position = live.stream_position()?;
        if end <= position {
            break;
        }
        // Copy exactly what is there now; a writer that keeps ahead cannot keep this loop.
        std::io::copy(&mut Read::by_ref(&mut live).take(end - position), &mut copy)?;
    }
    live.set_len(0)?;
    Ok(true)
}

/// Opens a private append-only file without following a replaced symlink.
pub fn open_append_private(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::PermissionsExt;
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let (file, before) = match options.create_new(true).open(path) {
        Ok(file) => (file, None),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let before = std::fs::symlink_metadata(path)?;
            if !before.is_file() {
                return Err(std::io::Error::other("not a regular file"));
            }
            (options.create_new(false).open(path)?, Some(before))
        }
        Err(error) => return Err(error),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { geteuid() }
        || meta.nlink() != 1
        || before
            .as_ref()
            .is_some_and(|old| old.dev() != meta.dev() || old.ino() != meta.ino())
    {
        return Err(std::io::Error::other("unsafe file"));
    }
    if meta.mode() & 0o7777 != 0o600 {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

struct Slot {
    child: Option<Child>,
    /// Held so the Core sees EOF (and exits) when botd closes or loses it.
    stdin: Option<ChildStdin>,
    started_at: Option<Instant>,
    failures: u32,
    next_start: Option<Instant>,
    // Management identity, also supplied to P1 when diagnostics are configured.
    status_id: Option<u64>,
    last_id: Option<u64>,
    launch: Option<ManualLaunch>,
    manual_restart: Option<ManualRestart>,
    // EOF was sent, but SIGKILL was deliberately withheld. A new pinned request
    // can still act on this child. It cannot be counted as a healthy peer.
    awaiting_retry: bool,
    // 起動失敗などで回収できていない Child は、終了を確認するまで保持する。
    reap_only: bool,
    reaping_cancelled_launch: bool,
}

struct ManualRestart {
    kill_at: Option<Instant>,
    reap_by: Option<Instant>,
    force: bool,
}

struct ManualLaunch {
    result: std::sync::mpsc::Receiver<std::io::Result<LaunchOutcome>>,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManualRestartError {
    TerminationUnconfirmed,
    LaunchFailed,
    LaunchTimeout,
    ShardStopped,
    LastRunningCore,
}

impl Slot {
    fn empty() -> Self {
        Self {
            child: None,
            stdin: None,
            started_at: None,
            failures: 0,
            next_start: None,
            status_id: None,
            last_id: None,
            launch: None,
            manual_restart: None,
            awaiting_retry: false,
            reap_only: false,
            reaping_cancelled_launch: false,
        }
    }
    fn clear_child(&mut self) {
        self.child = None;
        self.stdin = None;
        self.status_id = None;
        self.awaiting_retry = false;
        self.reaping_cancelled_launch = false;
    }

    fn can_start(&self, now: Instant) -> bool {
        self.child.is_none() && self.launch.is_none() && self.next_start.is_none_or(|at| now >= at)
    }

    fn record_exit(&mut self, now: Instant) {
        self.clear_child();
        let lived = self
            .started_at
            .take()
            .map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
        self.failures = if lived >= STABLE_AFTER {
            1
        } else {
            self.failures.saturating_add(1)
        };
        self.next_start = Some(now + restart_delay(self.failures));
    }

    fn launch_failed(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.next_start = Some(now + MAX_BACKOFF.min(BASE_BACKOFF * 4));
    }

    fn install(&mut self, mut outcome: LaunchOutcome, wanted: bool, now: Instant) -> bool {
        let launched = wanted && outcome.stdin.is_some();
        self.status_id = Some(outcome.id);
        self.last_id = self.status_id;
        self.child = outcome.child.take();
        self.reap_only = !launched;
        self.reaping_cancelled_launch = !wanted && self.child.is_some();
        if launched {
            self.stdin = outcome.stdin.take();
            self.started_at = Some(now);
            self.next_start = None;
        } else {
            drop(outcome.stdin.take());
            if let Some(child) = self.child.as_mut() {
                let _ = child.kill();
            }
        }
        launched
    }
}

struct LaunchOutcome {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    id: u64,
    secret: Option<Secret<[u8; SECRET_BYTES]>>,
}

impl LaunchOutcome {
    fn bootstrap(
        &mut self,
        write: impl FnOnce(&mut ChildStdin, &Secret<[u8; SECRET_BYTES]>) -> std::io::Result<()>,
    ) {
        if let Some(secret) = self.secret.take() {
            self.stdin = self.stdin.take().and_then(|mut stdin| {
                write(&mut stdin, &secret).ok()?;
                Some(stdin)
            });
        }
    }
}

impl Drop for LaunchOutcome {
    fn drop(&mut self) {
        // A launch that raced daemon exit has nobody to collect its handle.
        drop(self.stdin.take());
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            // The owner has dropped its receiver. Reap on the launch thread.
            let _ = child.wait();
        }
    }
}

struct CoreLaunch {
    cancelled: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    dir: PathBuf,
    bot_id: BotId,
    shard: Shard,
    status_dir: Option<PathBuf>,
    status_allow_non_tmpfs: bool,
    status_interval_ms: Option<u64>,
    epoch_floor: u64,
    previous_id: Option<u64>,
    fallback_id: u64,
    random_id: fn() -> Option<u64>,
}

pub struct CoreUnits {
    dir: PathBuf,
    bot_id: BotId,
    shard: Shard,
    slots: Vec<Slot>,
    prepared: bool,
    log_checked: Option<Instant>,
    epoch_floor: u64,
    status_dir: Option<PathBuf>,
    status_allow_non_tmpfs: bool,
    status_interval_ms: Option<u64>,
    fallback_salt: u64,
    launch_sequence: u64,
    random_id: fn() -> Option<u64>,
    #[cfg(test)]
    after_manual_spawn: Option<Box<dyn FnOnce(u32) + Send>>,
}

/// What an operator may be told about a Core, never a path, secret or argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreStatus {
    pub running: bool,
    pub failures: u32,
    pub pid: Option<u32>,
    pub status_path: Option<PathBuf>,
    pub status_id: Option<u64>,
}

impl CoreUnits {
    pub fn new(dir: PathBuf, bot_id: BotId, shard: Shard, daemon_instance: [u8; 16]) -> Self {
        let (low, high) = daemon_instance.split_at(8);
        let salt = u64::from_le_bytes(low.try_into().unwrap())
            ^ u64::from_le_bytes(high.try_into().unwrap());
        Self {
            dir,
            bot_id,
            shard,
            slots: (0..SLOTS).map(|_| Slot::empty()).collect(),
            prepared: false,
            log_checked: None,
            epoch_floor: 0,
            status_dir: None,
            status_allow_non_tmpfs: false,
            status_interval_ms: None,
            fallback_salt: salt & !FALLBACK_ID_BIT,
            launch_sequence: 0,
            random_id: || getrandom::u64().ok(),
            #[cfg(test)]
            after_manual_spawn: None,
        }
    }

    pub fn configure_status(
        &mut self,
        dir: Option<PathBuf>,
        allow_non_tmpfs: bool,
        interval_ms: Option<u64>,
    ) {
        self.status_dir = dir;
        self.status_allow_non_tmpfs = allow_non_tmpfs;
        self.status_interval_ms = interval_ms;
    }

    pub fn set_epoch_floor(&mut self, floor: u64) {
        self.epoch_floor = self.epoch_floor.max(floor);
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn status(&self) -> Vec<CoreStatus> {
        self.slots
            .iter()
            .enumerate()
            .map(|(index, slot)| CoreStatus {
                running: slot.child.is_some() && !slot.reap_only && !slot.awaiting_retry,
                failures: slot.failures,
                pid: slot.child.as_ref().map(Child::id),
                status_id: slot.status_id,
                status_path: self
                    .status_dir
                    .as_ref()
                    .map(|dir| dir.join(format!("core-{index}.status"))),
            })
            .collect()
    }

    pub(crate) fn prepared(&self) -> bool {
        self.prepared
    }

    pub(crate) fn manual_restart_status(&mut self, index: usize) -> std::io::Result<CoreStatus> {
        let mut status = self.status().swap_remove(index);
        let slot = &mut self.slots[index];
        if slot.reap_only || slot.launch.is_some() {
            return Err(std::io::Error::other("core unavailable for restart"));
        }
        if let Some(child) = &mut slot.child {
            // try_wait caches exit on Child. This observation does not release ownership.
            status.running = child.try_wait()?.is_none();
        }
        Ok(status)
    }

    pub(crate) fn peer_available(&mut self, target: usize) -> bool {
        self.slots.iter_mut().enumerate().any(|(index, slot)| {
            if index == target
                || slot.reap_only
                || slot.awaiting_retry
                || slot.manual_restart.is_some()
                || slot.launch.is_some()
            {
                return false;
            }
            let Some(child) = slot.child.as_mut() else {
                return false;
            };
            matches!(child.try_wait(), Ok(None)) && process_available(child.id())
        })
    }

    pub(crate) fn manual_restart_reserved(&self) -> bool {
        self.slots.iter().any(|slot| slot.manual_restart.is_some())
    }

    pub(crate) fn reserve_manual_restart(&mut self, index: usize) {
        let slot = &mut self.slots[index];
        debug_assert!(slot.manual_restart.is_none());
        slot.manual_restart = Some(ManualRestart {
            kill_at: None,
            reap_by: None,
            force: false,
        });
    }

    pub(crate) fn release_manual_restart(&mut self, index: usize) {
        self.slots[index].manual_restart = None;
    }

    pub(crate) fn begin_manual_restart(
        &mut self,
        index: usize,
        expected: Option<u64>,
        now: Instant,
        force: bool,
    ) -> std::io::Result<bool> {
        let slot = &mut self.slots[index];
        let restart = slot
            .manual_restart
            .as_mut()
            .expect("reserved manual restart");
        if restart.kill_at.is_some()
            || slot.status_id != expected
            || slot.child.is_some() != expected.is_some()
            || slot.launch.is_some()
            || slot.reap_only
        {
            return Ok(false);
        }
        let exited = match slot.child.as_mut() {
            Some(child) => child.try_wait()?.is_some(),
            None => false,
        };
        restart.kill_at = Some(now + MANUAL_STOP_GRACE);
        restart.force = force;
        if exited {
            // Exit preceded EOF. Preserve the same backoff as ordinary supervision.
            slot.record_exit(now);
        } else {
            slot.stdin = None;
        }
        Ok(true)
    }

    pub(crate) fn cancel_manual_restart(&mut self, index: usize) {
        let slot = &mut self.slots[index];
        if let Some(launch) = &slot.launch {
            launch
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let interrupted = slot
            .manual_restart
            .take()
            .is_some_and(|op| op.kill_at.is_some());
        slot.reap_only |= interrupted && slot.child.is_some();
    }

    /// Waits for exit and backoff by polling. Only the owned slot can install the launch result.
    pub(crate) fn poll_manual_restart(
        &mut self,
        index: usize,
        program: &Path,
        now: Instant,
        wanted: bool,
    ) -> Option<Result<(), ManualRestartError>> {
        self.poll_manual_restart_with(
            index,
            program,
            now,
            wanted,
            Child::try_wait,
            CoreLaunch::spawn,
        )
    }

    #[cfg(test)]
    pub(crate) fn pause_next_manual_launch(
        &mut self,
    ) -> (std::sync::mpsc::Receiver<u32>, std::sync::mpsc::Sender<()>) {
        let (spawned, pid) = std::sync::mpsc::channel();
        let (resume, release) = std::sync::mpsc::channel();
        self.after_manual_spawn = Some(Box::new(move |pid| {
            spawned.send(pid).unwrap();
            release.recv_timeout(Duration::from_secs(5)).unwrap();
        }));
        (pid, resume)
    }

    fn poll_manual_restart_with(
        &mut self,
        index: usize,
        program: &Path,
        now: Instant,
        wanted: bool,
        mut observe: impl FnMut(&mut Child) -> std::io::Result<Option<std::process::ExitStatus>>,
        start: impl FnOnce(CoreLaunch, usize, &Path) -> std::io::Result<LaunchOutcome> + Send + 'static,
    ) -> Option<Result<(), ManualRestartError>> {
        use ManualRestartError::*;
        if !wanted {
            self.cancel_manual_restart(index);
            return Some(Err(ShardStopped));
        }
        let slot = &mut self.slots[index];
        let restart = slot.manual_restart.as_ref().expect("manual restart active");
        let kill_at = restart.kill_at.expect("requested audit synced");
        if let Some(launch) = &slot.launch {
            if now >= launch.deadline {
                self.cancel_manual_restart(index);
                self.slots[index].launch_failed(now);
                return Some(Err(LaunchTimeout));
            }
            let success = Self::collect_launch(slot, true, now)?;
            slot.manual_restart = None;
            return Some(if success { Ok(()) } else { Err(LaunchFailed) });
        }
        if let Some(child) = &mut slot.child {
            let unconfirmed = match observe(child) {
                Ok(Some(_)) => false,
                Err(_) => true,
                Ok(None) => {
                    if let Some(deadline) = restart.reap_by {
                        if now < deadline {
                            return None;
                        }
                        true
                    } else if now >= kill_at {
                        if !restart.force && !self.peer_available(index) {
                            let slot = &mut self.slots[index];
                            slot.manual_restart = None;
                            slot.awaiting_retry = true;
                            return Some(Err(LastRunningCore));
                        }
                        let slot = &mut self.slots[index];
                        slot.manual_restart.as_mut().unwrap().reap_by =
                            Some(now + MANUAL_REAP_GRACE);
                        if slot.child.as_mut().unwrap().kill().is_ok() {
                            return None;
                        }
                        true
                    } else {
                        return None;
                    }
                }
            };
            if unconfirmed {
                self.cancel_manual_restart(index);
                return Some(Err(TerminationUnconfirmed));
            }
            self.slots[index].clear_child();
        }
        let slot = &self.slots[index];
        if !slot.can_start(now) {
            return None;
        }
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let Ok(mut launch) = self.launch_config(index) else {
            self.slots[index].manual_restart = None;
            self.slots[index].launch_failed(now);
            return Some(Err(LaunchFailed));
        };
        launch.cancelled = Some(cancelled.clone());
        let program = program.to_path_buf();
        let (send, receive) = std::sync::mpsc::sync_channel(0);
        #[cfg(test)]
        let after_spawn = self.after_manual_spawn.take();
        let slot = &mut self.slots[index];
        match std::thread::Builder::new()
            .name("core-restart".into())
            .spawn(move || {
                let result = start(launch, index, &program);
                #[cfg(test)]
                if let (Some(after_spawn), Ok(outcome)) = (after_spawn, &result) {
                    after_spawn(outcome.child.as_ref().unwrap().id());
                }
                let _ = send.send(result);
            }) {
            Ok(_) => {
                slot.launch = Some(ManualLaunch {
                    result: receive,
                    cancelled,
                    deadline: now + MANUAL_LAUNCH_TIMEOUT,
                });
                None
            }
            Err(_) => {
                slot.manual_restart = None;
                slot.launch_failed(now);
                Some(Err(LaunchFailed))
            }
        }
    }

    fn collect_launch(slot: &mut Slot, wanted: bool, now: Instant) -> Option<bool> {
        use std::sync::mpsc::TryRecvError;
        let launch = slot.launch.as_ref()?;
        let wanted = wanted && !launch.cancelled.load(std::sync::atomic::Ordering::Acquire);
        let result = match launch.result.try_recv() {
            Err(TryRecvError::Empty) => return None,
            result => result,
        };
        slot.launch = None;
        let launched = result.ok().and_then(Result::ok).is_some_and(|mut outcome| {
            if wanted {
                // The owner is the only writer to this fresh pipe. The capability
                // fits in one PIPE_BUF write, and cancellation cannot race delivery.
                outcome.bootstrap(write_secret);
            }
            slot.install(outcome, wanted, now)
        });
        if wanted && !launched {
            slot.launch_failed(now);
        }
        Some(launched)
    }

    /// Starts every Core that is not running. The first call also prepares the private
    /// directory and makes a new secret (a secret never outlives its Cores). Later calls
    /// start only the missing ones, with the secret already in place.
    pub fn ensure(&mut self, program: &Path) -> std::io::Result<()> {
        if !self.prepared {
            prepare_dir(&self.dir).map_err(|_| std::io::Error::other("core directory"))?;
            renew_secret(&self.dir).map_err(|_| std::io::Error::other("core secret"))?;
            self.prepared = true;
        }
        let now = Instant::now();
        for index in 0..self.slots.len() {
            if self.slots[index].manual_restart.is_none() && self.slots[index].can_start(now) {
                self.start_slot(index, program, now)?;
            }
        }
        Ok(())
    }

    fn launch_config(&mut self, index: usize) -> std::io::Result<CoreLaunch> {
        // Reserve a separate half of the ID space for a sequence that cannot wrap.
        let sequence = self
            .launch_sequence
            .checked_add(1)
            .filter(|sequence| *sequence < FALLBACK_ID_BIT)
            .ok_or_else(|| std::io::Error::other("core incarnation sequence exhausted"))?;
        self.launch_sequence = sequence;
        Ok(CoreLaunch {
            cancelled: None,
            dir: self.dir.clone(),
            bot_id: self.bot_id,
            shard: self.shard,
            status_dir: self.status_dir.clone(),
            status_allow_non_tmpfs: self.status_allow_non_tmpfs,
            status_interval_ms: self.status_interval_ms,
            epoch_floor: self.epoch_floor,
            previous_id: self.slots[index].last_id,
            fallback_id: FALLBACK_ID_BIT | (self.fallback_salt ^ sequence),
            random_id: self.random_id,
        })
    }

    fn start_slot(&mut self, index: usize, program: &Path, now: Instant) -> std::io::Result<()> {
        let result = self
            .launch_config(index)
            .and_then(|launch| launch.run(index, program));
        let slot = &mut self.slots[index];
        if result.is_ok_and(|outcome| slot.install(outcome, true, now)) {
            return Ok(());
        }
        slot.launch_failed(now);
        Err(std::io::Error::other("core launch"))
    }

    /// Reaps exited Cores and restarts due ones. `wanted` false (the Shard is stopped
    /// on purpose) reaps but never restarts.
    pub fn poll(&mut self, program: &Path, wanted: bool) {
        let now = Instant::now();
        if self
            .log_checked
            .is_none_or(|at| now.duration_since(at) >= LOG_CHECK_EVERY)
        {
            self.log_checked = Some(now);
            for index in 0..self.slots.len() {
                let _ = rotate_in_place(&self.dir.join(format!("core-{index}.log")), LOG_MAX_BYTES);
            }
        }
        for index in 0..self.slots.len() {
            let slot = &mut self.slots[index];
            if slot.manual_restart.is_some() {
                continue;
            }
            Self::collect_launch(slot, false, now);
            if slot.reap_only {
                if let Some(child) = &mut slot.child {
                    if !matches!(child.try_wait(), Ok(Some(_))) {
                        continue;
                    }
                    slot.clear_child();
                }
                slot.reap_only = false;
                slot.next_start
                    .get_or_insert_with(|| now + restart_delay(slot.failures));
            }
            let exited = matches!(slot.child.as_mut().map(Child::try_wait), Some(Ok(Some(_))));
            if exited {
                slot.record_exit(now);
                eprintln!("Coreが終了しました（slot {index}）。再起動を予定します");
            }
            if wanted && slot.next_start.is_some() && slot.can_start(now) {
                let _ = self.start_slot(index, program, now);
            }
        }
    }

    /// Asks every Core to stop by closing its stdin. Returns without waiting.
    pub fn begin_stop(&mut self) {
        for index in 0..self.slots.len() {
            self.cancel_manual_restart(index);
            let slot = &mut self.slots[index];
            slot.stdin = None;
            slot.next_start = None;
        }
    }

    /// A shutdown acknowledgement requires both a launch result and reaping any
    /// direct child it created. Other running slots keep their normal lifetime.
    pub(crate) fn poll_cancelled_launches(&mut self) -> bool {
        let mut pending = false;
        for slot in &mut self.slots {
            Self::collect_launch(slot, false, Instant::now());
            if slot.reaping_cancelled_launch
                && let Some(child) = &mut slot.child
                && matches!(child.try_wait(), Ok(Some(_)))
            {
                slot.clear_child();
            }
            pending |= slot.launch.is_some() || slot.reaping_cancelled_launch;
        }
        pending
    }

    /// True while any Core process is still alive (reaping the ones that exited).
    pub fn any_alive(&mut self) -> bool {
        let mut alive = false;
        for slot in &mut self.slots {
            Self::collect_launch(slot, false, Instant::now());
            alive |= slot.launch.is_some();
            if let Some(child) = slot.child.as_mut() {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    slot.clear_child();
                } else {
                    alive = true;
                }
            }
        }
        alive
    }

    /// Last resort after the stop deadline. Only signal owned, unreaped children.
    pub fn kill(&mut self) {
        for index in 0..self.slots.len() {
            self.cancel_manual_restart(index);
        }
        for slot in &mut self.slots {
            drop(slot.stdin.take());
            if let Some(child) = slot.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(_)) => slot.clear_child(),
                    Ok(None) => {
                        let _ = child.kill();
                    }
                    _ => {}
                }
            }
        }
    }

    /// Removes the sockets and the secret once no Core is running.
    pub fn cleanup(&mut self) {
        if !self.any_alive() {
            crate::core_attach::cleanup(&self.dir);
            self.prepared = false;
        }
    }
}

impl CoreLaunch {
    fn run(self, index: usize, program: &Path) -> std::io::Result<LaunchOutcome> {
        self.run_with_writer(index, program, write_secret)
    }

    fn run_with_writer(
        self,
        index: usize,
        program: &Path,
        write: impl FnOnce(&mut ChildStdin, &Secret<[u8; SECRET_BYTES]>) -> std::io::Result<()>,
    ) -> std::io::Result<LaunchOutcome> {
        let mut outcome = self.spawn(index, program)?;
        outcome.bootstrap(write);
        Ok(outcome)
    }

    fn spawn(self, index: usize, program: &Path) -> std::io::Result<LaunchOutcome> {
        let secret = load_secret_file(&secret_path(&self.dir))
            .map_err(|_| std::io::Error::other("core secret"))?;
        let identity = serde_json::to_string(&identity(self.bot_id, self.shard))
            .map_err(|_| std::io::Error::other("core identity"))?;
        let log = open_log(&self.dir, index);
        let (out, err) = match log.and_then(|file| file.try_clone().ok().map(|c| (file, c))) {
            Some((a, b)) => (Stdio::from(a), Stdio::from(b)),
            None => (Stdio::null(), Stdio::null()),
        };
        let mut command = Command::new(program);
        command
            .arg("--core-listen")
            .arg(socket_path(&self.dir, index))
            .arg("--core-persist");
        // Assign the daemon's management identity even without P1 or a status file.
        let id = (0..4)
            .find_map(|_| {
                (self.random_id)()
                    .map(|id| id & !FALLBACK_ID_BIT)
                    .filter(|id| *id != 0 && Some(*id) != self.previous_id)
            })
            .unwrap_or(self.fallback_id);
        if let Some(dir) = self
            .status_dir
            .as_ref()
            .filter(|dir| self.status_allow_non_tmpfs || crate::core_status::is_tmpfs(dir))
        {
            let path = dir.join(format!("core-{index}.status"));
            if let Ok(meta) = std::fs::symlink_metadata(&path)
                && meta.file_type().is_file()
                && meta.uid() == unsafe { libc::geteuid() }
            {
                let _ = std::fs::remove_file(&path);
            }
            command
                .arg("--core-status-file")
                .arg(path)
                .arg("--core-status-id")
                .arg(id.to_string());
            if self.status_allow_non_tmpfs {
                command.arg("--core-status-allow-non-tmpfs");
            }
            if let Some(interval) = self.status_interval_ms {
                command
                    .arg("--core-status-interval-ms")
                    .arg(interval.to_string());
            }
        }
        if self.epoch_floor > 0 {
            command
                .arg("--epoch-floor")
                .arg(self.epoch_floor.to_string());
        }
        command
            .env_clear()
            .env("BOT_WORKER_IPC_IDENTITY", identity)
            .stdin(Stdio::piped())
            .stdout(out)
            .stderr(err)
            // Terminal Ctrl-C targets botd's foreground group; botd stops Cores itself.
            .process_group(0);
        if self
            .cancelled
            .as_ref()
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        let mut child = command.spawn()?;
        let stdin = child.stdin.take();
        Ok(LaunchOutcome {
            child: Some(child),
            stdin,
            id,
            secret: Some(secret),
        })
    }
}

/// Linux child state is independent of whether P1 published a status record.
fn process_available(pid: u32) -> bool {
    std::fs::read(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.iter()
            .rposition(|&byte| byte == b')')
            .and_then(|end| stat.get(end + 2))
            .is_some_and(|&state| matches!(state, b'R' | b'S' | b'I'))
    })
}

fn restart_delay(failures: u32) -> Duration {
    BASE_BACKOFF
        .saturating_mul(1u32 << failures.saturating_sub(1).min(8))
        .min(MAX_BACKOFF)
}

/// 64 hex digits and a newline from a fixed buffer that is wiped right after the write.
fn write_secret(
    stdin: &mut ChildStdin,
    secret: &Secret<[u8; SECRET_BYTES]>,
) -> std::io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut line = [0u8; SECRET_BYTES * 2 + 1];
    for (index, byte) in secret.expose_secret().iter().enumerate() {
        line[index * 2] = HEX[usize::from(byte >> 4)];
        line[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    line[SECRET_BYTES * 2] = b'\n';
    let result = stdin.write_all(&line).and_then(|()| stdin.flush());
    wipe(&mut line);
    result
}

/// `core-<slot>.log` beside the sockets: append-only, 0600, restarted when oversized.
/// Falls back to no log (null) on anything unexpected rather than failing the start.
fn open_log(dir: &Path, index: usize) -> Option<std::fs::File> {
    let path = dir.join(format!("core-{index}.log"));
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.file_type().is_file() => return None,
        Ok(meta) if meta.len() > LOG_MAX_BYTES => {
            let _ = std::fs::remove_file(&path);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return None,
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .ok()?;
    writeln!(file, "--- core slot {index} start ---").ok()?;
    Some(file)
}

#[cfg(test)]
mod manual_restart_tests {
    use super::*;

    const CORE_FIXTURE: &str =
        "#!/bin/sh\nIFS= read -r capability || exit 1\nwhile IFS= read -r line; do :; done\n";

    fn write_executable(path: &Path, script: &str) {
        // Keep the writable descriptor out of the test process. A concurrent
        // fork could retain it until exec and make our spawn fail with ETXTBSY.
        let status = Command::new("/bin/sh")
            .args([
                "-c",
                "printf '%s' \"$2\" > \"$1\" && exec /bin/chmod 700 \"$1\"",
                "sh",
            ])
            .arg(path)
            .arg(script)
            .status()
            .unwrap();
        assert!(status.success(), "fixture creation failed: {status}");
    }

    struct Fixture {
        cores: CoreUnits,
        dir: PathBuf,
        program: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("cr-{:016x}", getrandom::u64().unwrap()));
            std::fs::create_dir(&dir).unwrap();
            let program = dir.join("child");
            write_executable(&program, CORE_FIXTURE);
            let mut cores = CoreUnits::new(
                dir.join("core"),
                BotId(7),
                Shard::new(0, 1).unwrap(),
                [7; 16],
            );
            cores.ensure(&program).unwrap();
            Self {
                cores,
                dir,
                program,
            }
        }

        fn empty_slot(&mut self, index: usize) {
            let slot = &mut self.cores.slots[index];
            drop(slot.stdin.take());
            let until = Instant::now() + Duration::from_secs(2);
            while !matches!(slot.child.as_mut().unwrap().try_wait(), Ok(Some(_))) {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            slot.clear_child();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.cores.kill();
            let until = Instant::now() + Duration::from_secs(2);
            while self.cores.any_alive() && Instant::now() < until {
                std::thread::yield_now();
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn ensure_and_poll_respect_an_empty_reserved_slot_and_backoff() {
        let mut f = Fixture::new();
        f.empty_slot(0);
        let now = Instant::now();
        let due = now + Duration::from_secs(4);
        f.cores.slots[0].next_start = Some(due);
        f.cores.reserve_manual_restart(0);
        f.cores.ensure(&f.program).unwrap();
        assert!(f.cores.slots[0].child.is_none());
        assert!(f.cores.begin_manual_restart(0, None, now, true).unwrap());
        assert_eq!(f.cores.poll_manual_restart(0, &f.program, now, true), None);
        assert!(f.cores.slots[0].launch.is_none());
        // Make ensure eligible regardless of wall clock, then verify the hold still wins.
        f.cores.slots[0].next_start = Some(now);
        f.cores.ensure(&f.program).unwrap();
        f.cores.poll(&f.program, true);
        assert!(f.cores.slots[0].child.is_none());
        assert_eq!(f.cores.slots[0].next_start, Some(now));
    }

    #[test]
    fn peer_availability_rejects_a_reserved_sibling() {
        let mut f = Fixture::new();
        assert!(f.cores.peer_available(0));
        f.cores.reserve_manual_restart(1);
        assert!(!f.cores.peer_available(0));
        f.cores.release_manual_restart(1);
        assert!(f.cores.peer_available(0));
    }

    #[test]
    fn ordinary_supervision_replaces_a_dead_core_when_random_ids_fail() {
        let mut f = Fixture::new();
        let peer = f.cores.status()[1].clone();
        f.cores.random_id = || None;
        for _ in 0..2 {
            let before = f.cores.status()[0].clone();
            f.cores.slots[0].child.as_mut().unwrap().kill().unwrap();
            let until = Instant::now() + Duration::from_secs(2);
            while f.cores.status()[0].pid.is_some() {
                f.cores.poll(&f.program, true);
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            assert!(
                f.cores.slots[0].next_start.is_some(),
                "exit schedules recovery"
            );
            f.cores.slots[0].next_start = Some(Instant::now());
            f.cores.poll(&f.program, true);
            let after = f.cores.manual_restart_status(0).unwrap();
            assert!(after.running);
            assert_ne!(after.pid, before.pid);
            assert_ne!(after.status_id, before.status_id);
            assert_ne!(after.status_id.unwrap() & FALLBACK_ID_BIT, 0);
            assert_eq!(f.cores.status()[1], peer);
        }
    }

    #[test]
    fn stop_during_grace_does_not_launch_a_replacement() {
        let mut f = Fixture::new();
        let now = Instant::now();
        let old = f.cores.slots[0].status_id;
        f.cores.reserve_manual_restart(0);
        assert!(f.cores.begin_manual_restart(0, old, now, true).unwrap());
        assert_eq!(
            f.cores.poll_manual_restart(0, &f.program, now, false),
            Some(Err(ManualRestartError::ShardStopped))
        );
        assert!(f.cores.slots[0].launch.is_none());
        let until = Instant::now() + Duration::from_secs(2);
        while f.cores.slots[0].child.is_some() {
            f.cores.poll(&f.program, false);
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        assert!(f.cores.slots[0].status_id.is_none());
        assert_eq!(f.cores.slots[0].last_id, old);
    }

    #[test]
    fn cancelled_launch_cannot_bootstrap_and_late_child_is_killed_and_reaped() {
        for spawned in [false, true] {
            let mut f = Fixture::new();
            f.empty_slot(0);
            let marker = socket_path(f.cores.dir(), 0).with_extension("sock.capability");
            let program = f.dir.join("late-child");
            write_executable(
                &program,
                "#!/bin/sh\nif IFS= read -r capability; then : > \"$2.capability\"; fi\nexec /bin/sleep 30\n",
            );
            let now = Instant::now();
            f.cores.reserve_manual_restart(0);
            assert!(f.cores.begin_manual_restart(0, None, now, true).unwrap());
            let (ready, entered) = std::sync::mpsc::channel();
            let (resume, release) = std::sync::mpsc::channel();
            let (checked, check) = std::sync::mpsc::channel();
            assert_eq!(
                f.cores.poll_manual_restart_with(
                    0,
                    &program,
                    now,
                    true,
                    Child::try_wait,
                    move |launch, index, program| {
                        if spawned {
                            let result = launch.spawn(index, program)?;
                            ready.send(result.child.as_ref().map(Child::id)).unwrap();
                            release.recv_timeout(Duration::from_secs(2)).unwrap();
                            checked.send(true).unwrap();
                            Ok(result)
                        } else {
                            ready.send(None).unwrap();
                            release.recv_timeout(Duration::from_secs(2)).unwrap();
                            let result = launch.spawn(index, program);
                            checked
                                .send(
                                    result.as_ref().is_err_and(|e| {
                                        e.kind() == std::io::ErrorKind::Interrupted
                                    }),
                                )
                                .unwrap();
                            result
                        }
                    },
                ),
                None
            );
            let pid = entered.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(pid.is_some(), spawned);
            f.cores.begin_stop();
            // Shutdown's kill sweep precedes the launch result.
            f.cores.kill();
            assert!(f.cores.slots[0].launch.is_some());
            resume.send(()).unwrap();
            assert!(check.recv_timeout(Duration::from_secs(2)).unwrap());
            let until = Instant::now() + Duration::from_secs(2);
            while f.cores.any_alive() {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            if let Some(pid) = pid {
                assert!(!Path::new(&format!("/proc/{pid}")).exists());
            }
            assert!(!marker.exists());
            assert!(f.cores.slots[0].launch.is_none());
        }
    }

    #[test]
    fn pending_launch_leaves_other_supervision_live_and_bootstrap_failure_is_counted() {
        let mut f = Fixture::new();
        let mut other = Fixture::new();
        f.empty_slot(0);
        f.cores.slots[0].failures = 6;
        f.cores.set_epoch_floor(17);
        let now = Instant::now();
        f.cores.reserve_manual_restart(0);
        assert!(f.cores.begin_manual_restart(0, None, now, true).unwrap());
        let (release, paused) = std::sync::mpsc::channel();
        let (entered, launch_entered) = std::sync::mpsc::channel();
        assert_eq!(
            f.cores.poll_manual_restart_with(
                0,
                &f.program,
                now,
                true,
                Child::try_wait,
                move |launch, index, program| {
                    assert_eq!(launch.epoch_floor, 17);
                    entered.send(()).unwrap();
                    paused.recv_timeout(Duration::from_secs(2)).unwrap();
                    launch.run_with_writer(index, program, |_, _| {
                        Err(std::io::ErrorKind::BrokenPipe.into())
                    })
                }
            ),
            None
        );
        launch_entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let old = other.cores.slots[0].status_id;
        other.empty_slot(0);
        other.cores.slots[0].next_start = Some(now);
        other.cores.poll(&other.program, true);
        assert!(other.cores.slots[0].child.is_some());
        assert_ne!(other.cores.slots[0].status_id, old);
        assert_eq!(f.cores.poll_manual_restart(0, &f.program, now, true), None);
        release.send(()).unwrap();
        let until = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(result) = f.cores.poll_manual_restart(0, &f.program, now, true) {
                assert_eq!(result, Err(ManualRestartError::LaunchFailed));
                break;
            }
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        let slot = &f.cores.slots[0];
        assert_eq!(slot.failures, 7);
        assert_eq!(slot.next_start, Some(now + Duration::from_secs(4)));
        assert!(slot.reap_only);
        assert!(slot.child.is_some());
        assert!(slot.status_id.is_some());
    }

    #[test]
    fn unconfirmed_exit_retains_the_child_until_observed() {
        for observation_error in [false, true] {
            let mut f = Fixture::new();
            let now = Instant::now();
            let id = f.cores.slots[0].status_id;
            let pid = f.cores.slots[0].child.as_ref().unwrap().id();
            f.cores.reserve_manual_restart(0);
            assert!(f.cores.begin_manual_restart(0, id, now, true).unwrap());
            let deadline = now + MANUAL_STOP_GRACE + MANUAL_REAP_GRACE;
            f.cores.slots[0].manual_restart.as_mut().unwrap().reap_by = Some(deadline);
            let observe = |_: &mut Child| {
                if observation_error {
                    Err(std::io::Error::other("injected observation error"))
                } else {
                    Ok(None)
                }
            };
            if !observation_error {
                assert_eq!(
                    f.cores.poll_manual_restart_with(
                        0,
                        &f.program,
                        deadline - Duration::from_millis(1),
                        true,
                        |_| Ok(None),
                        CoreLaunch::run
                    ),
                    None
                );
            }
            assert_eq!(
                f.cores.poll_manual_restart_with(
                    0,
                    &f.program,
                    deadline,
                    true,
                    observe,
                    CoreLaunch::run
                ),
                Some(Err(ManualRestartError::TerminationUnconfirmed))
            );
            let slot = &f.cores.slots[0];
            assert!(slot.reap_only);
            assert_eq!(slot.status_id, id);
            assert_eq!(slot.child.as_ref().unwrap().id(), pid);
            assert!(slot.launch.is_none());
            f.cores.ensure(&f.program).unwrap();
            assert_eq!(f.cores.slots[0].child.as_ref().unwrap().id(), pid);
            let until = Instant::now() + Duration::from_secs(2);
            while f.cores.slots[0].child.is_some() {
                f.cores.poll(&f.program, true);
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            assert!(f.cores.slots[0].next_start.is_some());
        }
    }

    #[test]
    fn manual_restart_kills_after_grace_and_preserves_failures() {
        let dir = std::env::temp_dir().join(format!("cr-{:016x}", getrandom::u64().unwrap()));
        std::fs::create_dir(&dir).unwrap();
        let program = dir.join("child");
        write_executable(&program, CORE_FIXTURE);
        let core_dir = dir.join("core");
        let mut cores = CoreUnits::new(core_dir, BotId(7), Shard::new(0, 1).unwrap(), [7; 16]);
        cores.configure_status(Some(dir.clone()), true, None);
        cores.ensure(&program).unwrap();
        let mut old = cores.slots[0].child.take().unwrap();
        cores.slots[0].stdin = None;
        old.wait().unwrap();
        // sleep は stdin の EOF では止まらないため、猶予後の経路を試せる。
        let mut old = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        cores.slots[0].stdin = old.stdin.take();
        cores.slots[0].child = Some(old);
        cores.slots[0].failures = 6;
        let before = cores.status();
        let now = Instant::now();
        cores.reserve_manual_restart(0);
        assert!(
            cores
                .begin_manual_restart(0, before[0].status_id, now, true)
                .unwrap()
        );
        assert!(cores.slots[0].stdin.is_none());
        cores.poll(&program, true);
        assert_eq!(cores.status(), before);
        assert_eq!(
            cores.poll_manual_restart(
                0,
                &program,
                now + MANUAL_STOP_GRACE - Duration::from_millis(1),
                true
            ),
            None
        );
        assert!(
            cores.slots[0]
                .child
                .as_mut()
                .unwrap()
                .try_wait()
                .unwrap()
                .is_none()
        );
        let due = now + MANUAL_STOP_GRACE;
        assert_eq!(cores.poll_manual_restart(0, &program, due, true), None);
        assert_eq!(
            cores.slots[0].manual_restart.as_ref().unwrap().reap_by,
            Some(due + MANUAL_REAP_GRACE)
        );
        let timeout = Instant::now() + Duration::from_secs(2);
        let result = loop {
            if let Some(result) = cores.poll_manual_restart(0, &program, due, true) {
                break result;
            }
            assert!(Instant::now() < timeout, "test child did not exit");
            std::thread::yield_now();
        };
        let after = cores.status();
        cores.kill();
        while cores.any_alive() {
            assert!(Instant::now() < timeout);
            std::thread::yield_now();
        }
        std::fs::remove_dir_all(dir).unwrap();
        assert_eq!(result, Ok(()));
        assert_eq!(after[0].failures, 6);
        assert_ne!(after[0].status_id, before[0].status_id);
        assert!(after[0].running);
        assert_eq!(after[1], before[1]);
    }
}

#[cfg(test)]
mod private_append_tests {
    use super::open_append_private;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn refuses_symlink_and_restricts_existing_file() {
        let dir = std::env::temp_dir().join(format!("core-private-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("target");
        let link = dir.join("link");
        std::fs::write(&target, b"").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        symlink(&target, &link).unwrap();
        assert!(open_append_private(&link).is_err());
        open_append_private(&target).unwrap();
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(target).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn refuses_hardlink_and_fifo() {
        let dir = std::env::temp_dir().join(format!(
            "core-private-extra-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let target = dir.join("target");
        let link = dir.join("hardlink");
        std::fs::write(&target, b"").unwrap();
        std::fs::hard_link(&target, &link).unwrap();
        assert!(open_append_private(&target).is_err());
        let fifo = dir.join("fifo");
        if std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .is_ok_and(|s| s.success())
        {
            assert!(open_append_private(&fifo).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
