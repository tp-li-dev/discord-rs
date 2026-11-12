//! Bounded, fail-closed file storage for OpenRaft 0.9.26.
//!
//! Every mutation writes a complete, checksummed version of log/vote/commit,
//! application state, membership and snapshot to `raft.journal`, syncs it, then
//! updates and syncs `raft.seal`. Only then is the new state visible. The seal
//! also detects truncation at a complete journal record boundary. There is no
//! fallback to an older record on corruption or an interrupted write. Recovery
//! of such a store requires an operator procedure, which is not implemented.
//!
//! This intentionally simple backend has a 16 MiB record / 256 MiB journal
//! limit and no journal compaction; capacity exhaustion stops the node. It is
//! not a throughput-oriented production backend. CRC32 detects accidental
//! damage, not malicious editing or rollback of both files to an old backup.
//! Paths and files must be trusted and kept on a local filesystem. File locks
//! are advisory on Unix. Power-loss behavior, network filesystems, Windows
//! directory metadata durability and Linux execution remain unverified.
//! The low-level OpenRaft storage traits are trusted engine APIs, not an admin
//! interface. In particular, a caller that purges beyond applied state creates
//! an unrecoverable store; startup refuses this instead of inventing state.

use super::{Entry, Outcome, OwnershipData, TypeConfig};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

type Error = StorageError<u64>;
const JOURNAL_MAGIC: &[u8; 8] = b"RFTQJ001";
const SEAL_MAGIC: &[u8; 8] = b"RFTQS001";
const HEADER_BYTES: usize = 28;
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_JOURNAL_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    node_id: u64,
    members: BTreeSet<u64>,
    cluster_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, ()>,
    data: OwnershipData,
}

impl StoredSnapshot {
    fn snapshot(&self) -> Snapshot<TypeConfig> {
        Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(self.data.clone()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskState {
    identity: Identity,
    logs: BTreeMap<u64, Entry>,
    purged: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    data: OwnershipData,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, ()>,
    snapshot: Option<StoredSnapshot>,
    snapshot_sequence: u64,
}

impl DiskState {
    fn empty(identity: Identity) -> Self {
        Self {
            identity,
            logs: BTreeMap::new(),
            purged: None,
            vote: None,
            committed: None,
            data: OwnershipData::default(),
            applied: None,
            membership: StoredMembership::default(),
            snapshot: None,
            snapshot_sequence: 0,
        }
    }

    fn check_logs(&self) -> io::Result<()> {
        let mut previous = self.purged.map(|id| id.index);
        for (index, entry) in &self.logs {
            if *index != entry.log_id.index
                || previous.is_some_and(|p| p.checked_add(1) != Some(*index))
            {
                return Err(invalid("non-consecutive raft log"));
            }
            previous = Some(*index);
        }
        Ok(())
    }

    fn check_recovery(&self) -> io::Result<()> {
        self.check_logs()?;
        let durable_tip = self
            .logs
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .into_iter()
            .chain(self.applied)
            .max_by_key(|id| id.index);
        if self.committed.is_some_and(|committed| {
            durable_tip.is_none_or(|tip| {
                committed.index > tip.index || (committed.index == tip.index && committed != tip)
            })
        }) {
            return Err(invalid("committed log is not covered by durable state"));
        }
        if self.applied.is_none() && self.data != OwnershipData::default() {
            return Err(invalid("ownership exists without applied state"));
        }
        if self.purged.is_some_and(|p| {
            self.applied.is_none_or(|a| a.index < p.index)
                || self
                    .snapshot
                    .as_ref()
                    .is_none_or(|s| s.meta.last_log_id.is_none_or(|id| id.index < p.index))
        }) {
            return Err(invalid("purged log is not protected by a durable snapshot"));
        }
        if self.snapshot.as_ref().is_some_and(|snapshot| {
            if snapshot
                .meta
                .last_membership
                .log_id()
                .is_some_and(|membership| {
                    snapshot
                        .meta
                        .last_log_id
                        .is_none_or(|last| membership.index > last.index)
                })
            {
                return true;
            }
            if snapshot.meta.last_log_id.is_none() && snapshot.data != OwnershipData::default() {
                return true;
            }
            snapshot.meta.last_log_id.is_some_and(|id| {
                self.applied.is_none_or(|applied| {
                    applied.index < id.index || (applied.index == id.index && applied != id)
                })
            })
        }) {
            return Err(invalid("snapshot is ahead of applied state"));
        }
        Ok(())
    }

    fn make_snapshot(&mut self) -> io::Result<Snapshot<TypeConfig>> {
        self.snapshot_sequence = self
            .snapshot_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("snapshot sequence exhausted"))?;
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: self.applied,
                last_membership: self.membership.clone(),
                snapshot_id: format!(
                    "{}-{}-{}",
                    self.identity.node_id,
                    self.applied.map_or(0, |id| id.index),
                    self.snapshot_sequence
                ),
            },
            data: self.data.clone(),
        };
        let result = snapshot.snapshot();
        self.snapshot = Some(snapshot);
        Ok(result)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn storage_error(verb: ErrorVerb, error: io::Error) -> Error {
    StorageError::from_io_error(ErrorSubject::Store, verb, error)
}

fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = !0_u32;
    for byte in parts.iter().flat_map(|part| part.iter()) {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

struct BoundedBytes(Vec<u8>);
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_RECORD_BYTES {
            return Err(invalid("raft storage record exceeds capacity"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Journal {
    // The journal handle also owns the exclusive OS file lock until all clones
    // of LogStore and StateMachine have gone away.
    file: File,
    seal: File,
    state: DiskState,
    sequence: u64,
    length: u64,
    failed: bool,
}

impl Journal {
    fn persist(&mut self, state: DiskState) -> io::Result<()> {
        if self.failed {
            return Err(invalid("raft storage is poisoned"));
        }
        let result = self.persist_inner(state);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn persist_inner(&mut self, state: DiskState) -> io::Result<()> {
        let mut payload = BoundedBytes(Vec::new());
        serde_json::to_writer(&mut payload, &state)
            .map_err(|_| invalid("raft state cannot be encoded within capacity"))?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("journal sequence exhausted"))?;
        let length = self
            .length
            .checked_add((HEADER_BYTES + payload.0.len()) as u64)
            .filter(|length| *length <= MAX_JOURNAL_BYTES)
            .ok_or_else(|| invalid("raft journal exceeds capacity"))?;
        let mut header = [0_u8; HEADER_BYTES];
        header[..8].copy_from_slice(JOURNAL_MAGIC);
        header[8..16].copy_from_slice(&sequence.to_le_bytes());
        header[16..24].copy_from_slice(&(payload.0.len() as u64).to_le_bytes());
        let crc = crc32(&[&header[..24], &payload.0]);
        header[24..].copy_from_slice(&crc.to_le_bytes());

        self.file.seek(SeekFrom::Start(self.length))?;
        self.file.write_all(&header)?;
        self.file.write_all(&payload.0)?;
        self.file.sync_all()?;

        let mut seal = [0_u8; HEADER_BYTES];
        seal[..8].copy_from_slice(SEAL_MAGIC);
        seal[8..16].copy_from_slice(&sequence.to_le_bytes());
        seal[16..24].copy_from_slice(&length.to_le_bytes());
        let checksum = crc32(&[&seal[..24]]);
        seal[24..].copy_from_slice(&checksum.to_le_bytes());
        self.seal.seek(SeekFrom::Start(0))?;
        self.seal.write_all(&seal)?;
        self.seal.sync_all()?;

        self.state = state;
        self.sequence = sequence;
        self.length = length;
        Ok(())
    }
}

fn file_options(create: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_WRITE_THROUGH supplements sync_all (FlushFileBuffers).
        options.custom_flags(0x8000_0000);
    }
    options
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn load(file: &mut File, seal: &mut File, identity: &Identity) -> io::Result<JournalState> {
    let length = file.metadata()?.len();
    if length == 0 || length > MAX_JOURNAL_BYTES || seal.metadata()?.len() != HEADER_BYTES as u64 {
        return Err(invalid("invalid raft storage length"));
    }
    let mut marker = [0_u8; HEADER_BYTES];
    seal.read_exact(&mut marker)?;
    if &marker[..8] != SEAL_MAGIC
        || crc32(&[&marker[..24]]) != u32::from_le_bytes(marker[24..].try_into().unwrap())
        || u64::from_le_bytes(marker[16..24].try_into().unwrap()) != length
    {
        return Err(invalid("raft storage seal does not match journal"));
    }
    let mut offset = 0_u64;
    let mut sequence = 0_u64;
    let mut last = Vec::new();
    while offset < length {
        let mut header = [0_u8; HEADER_BYTES];
        file.read_exact(&mut header)?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("journal sequence exhausted"))?;
        let payload_length = u64::from_le_bytes(header[16..24].try_into().unwrap());
        if &header[..8] != JOURNAL_MAGIC
            || sequence != u64::from_le_bytes(header[8..16].try_into().unwrap())
            || payload_length > MAX_RECORD_BYTES as u64
            || payload_length == 0
        {
            return Err(invalid("invalid raft journal record header"));
        }
        offset = offset
            .checked_add(HEADER_BYTES as u64 + payload_length)
            .filter(|offset| *offset <= length)
            .ok_or_else(|| invalid("incomplete raft journal record"))?;
        last.resize(payload_length as usize, 0);
        file.read_exact(&mut last)?;
        if crc32(&[&header[..24], &last]) != u32::from_le_bytes(header[24..].try_into().unwrap()) {
            return Err(invalid("raft journal checksum mismatch"));
        }
    }
    if sequence != u64::from_le_bytes(marker[8..16].try_into().unwrap()) {
        return Err(invalid("raft journal sequence does not match seal"));
    }
    let state: DiskState =
        serde_json::from_slice(&last).map_err(|_| invalid("invalid raft storage schema"))?;
    if &state.identity != identity {
        return Err(invalid("raft storage identity or membership mismatch"));
    }
    state.check_recovery()?;
    Ok(JournalState {
        state,
        sequence,
        length,
    })
}

struct JournalState {
    state: DiskState,
    sequence: u64,
    length: u64,
}

#[derive(Clone)]
struct Shared(Arc<Mutex<Journal>>);

impl Shared {
    async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&DiskState) -> T + Send + 'static,
    ) -> Result<T, Error> {
        let shared = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let journal = shared
                .lock()
                .map_err(|_| invalid("raft storage mutex poisoned"))?;
            if journal.failed {
                return Err(invalid("raft storage is poisoned"));
            }
            Ok(read(&journal.state))
        })
        .await
        .map_err(|_| storage_error(ErrorVerb::Read, invalid("raft storage task failed")))?
        .map_err(|error| storage_error(ErrorVerb::Read, error))
    }

    async fn write<T: Send + 'static>(
        &self,
        mutate: impl FnOnce(&mut DiskState) -> io::Result<T> + Send + 'static,
    ) -> Result<T, Error> {
        let shared = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut journal = shared
                .lock()
                .map_err(|_| invalid("raft storage mutex poisoned"))?;
            if journal.failed {
                return Err(invalid("raft storage is poisoned"));
            }
            let mut next = journal.state.clone();
            let value = mutate(&mut next)?;
            journal.persist(next)?;
            Ok(value)
        })
        .await
        .map_err(|_| storage_error(ErrorVerb::Write, invalid("raft storage task failed")))?
        .map_err(|error| storage_error(ErrorVerb::Write, error))
    }
}

/// Explicit storage provisioning/restart boundary. `open` never creates missing
/// files and `create` refuses an existing directory, even if it is empty.
pub struct FileStore;

impl FileStore {
    pub async fn create(
        path: impl AsRef<Path>,
        node_id: u64,
        members: BTreeSet<u64>,
    ) -> io::Result<(LogStore, StateMachine)> {
        Self::create_in_cluster(path, node_id, members, "raftq-storage-contract".into()).await
    }

    pub async fn open(
        path: impl AsRef<Path>,
        node_id: u64,
        members: BTreeSet<u64>,
    ) -> io::Result<(LogStore, StateMachine)> {
        Self::open_in_cluster(path, node_id, members, "raftq-storage-contract".into()).await
    }

    pub async fn create_in_cluster(
        path: impl AsRef<Path>,
        node_id: u64,
        members: BTreeSet<u64>,
        cluster_name: String,
    ) -> io::Result<(LogStore, StateMachine)> {
        Self::start(
            path.as_ref().to_path_buf(),
            node_id,
            members,
            cluster_name,
            true,
        )
        .await
    }

    pub async fn open_in_cluster(
        path: impl AsRef<Path>,
        node_id: u64,
        members: BTreeSet<u64>,
        cluster_name: String,
    ) -> io::Result<(LogStore, StateMachine)> {
        Self::start(
            path.as_ref().to_path_buf(),
            node_id,
            members,
            cluster_name,
            false,
        )
        .await
    }

    async fn start(
        path: PathBuf,
        node_id: u64,
        members: BTreeSet<u64>,
        cluster_name: String,
        create: bool,
    ) -> io::Result<(LogStore, StateMachine)> {
        let shared = tokio::task::spawn_blocking(move || {
            if !members.contains(&node_id) {
                return Err(invalid("local node is not in raft storage membership"));
            }
            if cluster_name.trim().is_empty() || cluster_name.len() > 256 {
                return Err(invalid("invalid raft storage cluster namespace"));
            }
            let identity = Identity {
                node_id,
                members,
                cluster_name,
            };
            if create {
                let builder = fs::DirBuilder::new();
                #[cfg(unix)]
                let builder = {
                    use std::os::unix::fs::DirBuilderExt;
                    let mut builder = builder;
                    builder.mode(0o700);
                    builder
                };
                builder.create(&path)?;
            }
            let mut file = file_options(create).open(path.join("raft.journal"))?;
            file.try_lock().map_err(|_| {
                io::Error::new(io::ErrorKind::WouldBlock, "raft storage already in use")
            })?;
            let mut seal = file_options(create).open(path.join("raft.seal"))?;
            if !file.metadata()?.is_file() || !seal.metadata()?.is_file() {
                return Err(invalid("raft storage is not a regular file"));
            }
            let mut journal = if create {
                Journal {
                    file,
                    seal,
                    state: DiskState::empty(identity.clone()),
                    sequence: 0,
                    length: 0,
                    failed: false,
                }
            } else {
                let loaded = load(&mut file, &mut seal, &identity)?;
                Journal {
                    file,
                    seal,
                    state: loaded.state,
                    sequence: loaded.sequence,
                    length: loaded.length,
                    failed: false,
                }
            };
            if create {
                journal.persist(DiskState::empty(identity))?;
                sync_directory(&path)?;
                if let Some(parent) = path.parent() {
                    sync_directory(if parent.as_os_str().is_empty() {
                        Path::new(".")
                    } else {
                        parent
                    })?;
                }
            }
            Ok(Shared(Arc::new(Mutex::new(journal))))
        })
        .await
        .map_err(|_| invalid("raft storage startup task failed"))??;
        Ok((LogStore(shared.clone()), StateMachine(shared)))
    }
}

#[derive(Clone)]
pub struct LogStore(Shared);

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        self.0
            .read(move |state| {
                // Avoid BTreeMap::range panics for arbitrary caller-provided bounds.
                state
                    .logs
                    .iter()
                    .filter(|(index, _)| {
                        let after = match bounds.0 {
                            Bound::Included(v) => **index >= v,
                            Bound::Excluded(v) => **index > v,
                            Bound::Unbounded => true,
                        };
                        let before = match bounds.1 {
                            Bound::Included(v) => **index <= v,
                            Bound::Excluded(v) => **index < v,
                            Bound::Unbounded => true,
                        };
                        after && before
                    })
                    .map(|(_, entry)| entry.clone())
                    .collect()
            })
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, Error> {
        self.0
            .read(|state| LogState {
                last_purged_log_id: state.purged,
                last_log_id: state
                    .logs
                    .last_key_value()
                    .map(|(_, e)| e.log_id)
                    .or(state.purged),
            })
            .await
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), Error> {
        let vote = *vote;
        self.0
            .write(move |state| {
                if state.vote.is_some_and(|previous| vote < previous) {
                    return Err(invalid("raft vote cannot regress"));
                }
                state.vote = Some(vote);
                Ok(())
            })
            .await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, Error> {
        self.0.read(|state| state.vote).await
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), Error> {
        self.0
            .write(move |state| {
                if committed < state.committed {
                    return Err(invalid("raft commit cannot regress"));
                }
                state.committed = committed;
                Ok(())
            })
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, Error> {
        self.0.read(|state| state.committed).await
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .0
            .write(move |state| {
                for entry in entries {
                    if state.purged.is_some_and(|p| entry.log_id.index <= p.index) {
                        if state
                            .purged
                            .is_some_and(|p| entry.log_id.index == p.index && entry.log_id != p)
                        {
                            return Err(invalid("append conflicts with purged raft log"));
                        }
                        // Already compacted entries cannot be restored into the
                        // log. The upstream storage suite deliberately supplies
                        // this redundant prefix after recovering applied state.
                        continue;
                    }
                    let protected = state
                        .committed
                        .into_iter()
                        .chain(state.applied)
                        .any(|id| entry.log_id.index <= id.index);
                    if protected && let Some(previous) = state.logs.get(&entry.log_id.index) {
                        let previous = serde_json::to_vec(previous)
                            .map_err(|_| invalid("invalid persisted raft entry"))?;
                        let incoming = serde_json::to_vec(&entry)
                            .map_err(|_| invalid("invalid incoming raft entry"))?;
                        if previous != incoming {
                            return Err(invalid("append would overwrite a committed raft entry"));
                        }
                    }
                    state.logs.insert(entry.log_id.index, entry);
                }
                state.check_logs()
            })
            .await;
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(io::Error::other("raft log persistence failed")));
                Err(error)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), Error> {
        self.0
            .write(move |state| {
                if state.applied.is_some_and(|a| a.index >= log_id.index)
                    || state
                        .committed
                        .is_some_and(|committed| committed.index >= log_id.index)
                {
                    return Err(invalid("cannot truncate applied raft log"));
                }
                state.logs.split_off(&log_id.index);
                Ok(())
            })
            .await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), Error> {
        self.0
            .write(move |state| {
                if state.purged.is_some_and(|p| p.index > log_id.index) {
                    return Err(invalid("raft purge cannot regress"));
                }
                // OpenRaft decides which logs can be purged. When a persisted state
                // covers this prefix, retain it as a snapshot in the SAME durable
                // transaction. A low-level call purging beyond applied is allowed
                // by the upstream log-store tests, but check_recovery rejects it.
                if state.applied.is_some_and(|a| a.index >= log_id.index)
                    && state
                        .snapshot
                        .as_ref()
                        .is_none_or(|s| s.meta.last_log_id.is_none_or(|id| id.index < log_id.index))
                {
                    state.make_snapshot()?;
                }
                state.logs.retain(|index, _| *index > log_id.index);
                state.purged = Some(log_id);
                Ok(())
            })
            .await
    }
}

#[derive(Clone)]
pub struct StateMachine(Shared);

impl StateMachine {
    /// Diagnostic local read; never a quorum grant or linearizable authority.
    pub async fn data(&self) -> Result<OwnershipData, Error> {
        self.0.read(|state| state.data.clone()).await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, Error> {
        self.0.write(DiskState::make_snapshot).await
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, ()>), Error> {
        self.0
            .read(|state| (state.applied, state.membership.clone()))
            .await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Outcome>, Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.0
            .write(move |state| {
                let mut results = Vec::new();
                for entry in entries {
                    if state
                        .applied
                        .is_some_and(|id| id.index >= entry.log_id.index)
                    {
                        return Err(invalid("raft state cannot reapply or regress"));
                    }
                    let result = match entry.payload {
                        EntryPayload::Blank => Outcome::Noop,
                        EntryPayload::Normal(command) => state.data.apply(&command),
                        EntryPayload::Membership(membership) => {
                            state.membership =
                                StoredMembership::new(Some(entry.log_id), membership);
                            Outcome::Noop
                        }
                    };
                    state.applied = Some(entry.log_id);
                    results.push(result);
                }
                Ok(results)
            })
            .await
    }

    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<OwnershipData>, Error> {
        Ok(Box::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, ()>,
        snapshot: Box<OwnershipData>,
    ) -> Result<(), Error> {
        let meta = meta.clone();
        self.0
            .write(move |state| {
                if state.applied.is_some_and(|applied| {
                    meta.last_log_id.is_none_or(|next| {
                        next.index < applied.index
                            || (next.index == applied.index
                                && (next != applied
                                    || *snapshot != state.data
                                    || meta.last_membership != state.membership))
                    })
                }) {
                    return Err(invalid("raft snapshot cannot regress applied state"));
                }
                if meta.last_membership.log_id().is_some_and(|membership| {
                    meta.last_log_id
                        .is_none_or(|last| membership.index > last.index)
                }) || (meta.last_log_id.is_none() && *snapshot != OwnershipData::default())
                {
                    return Err(invalid("invalid raft snapshot metadata"));
                }
                if snapshot.0.values().any(|record| {
                    record.epoch == 0 || record.owner == 0 || record.session == [0; 16]
                }) {
                    return Err(invalid("invalid ownership in raft snapshot"));
                }
                for (key, previous) in &state.data.0 {
                    if snapshot.0.get(key).is_none_or(|next| {
                        next.epoch < previous.epoch
                            || (next.epoch == previous.epoch
                                && (next.owner != previous.owner
                                    || next.session != previous.session
                                    || (!previous.held && next.held)))
                    }) {
                        return Err(invalid("raft snapshot would roll back ownership"));
                    }
                }
                state.data = *snapshot;
                state.applied = meta.last_log_id;
                state.membership = meta.last_membership.clone();
                state.snapshot = Some(StoredSnapshot {
                    meta,
                    data: state.data.clone(),
                });
                Ok(())
            })
            .await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, Error> {
        self.0
            .read(|state| state.snapshot.as_ref().map(StoredSnapshot::snapshot))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, storage::RaftLogStorageExt};

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            if self.0.parent() == Some(std::env::temp_dir().as_path()) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }

    #[tokio::test]
    async fn failed_seal_io_never_acknowledges_or_serves_stale_state() {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).unwrap();
        let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let directory =
            Directory(std::env::temp_dir().join(format!("raftq-storage-io-fault-{suffix}")));
        let members: BTreeSet<_> = [1, 2, 3].into();
        let (mut logs, machine) = FileStore::create(&directory.0, 1, members.clone())
            .await
            .unwrap();
        // Force a real write error at the seal step, after the journal fsync.
        // The handle is read-only; no global permissions or host state change.
        logs.0.0.lock().unwrap().seal = File::open(directory.0.join("raft.seal")).unwrap();
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 0),
            payload: EntryPayload::Blank,
        };
        assert!(logs.blocking_append([entry]).await.is_err());
        assert!(logs.read_vote().await.is_err());
        assert!(machine.data().await.is_err());
        assert!(logs.save_vote(&Vote::new(2, 1)).await.is_err());
        drop(logs);
        drop(machine);
        // The complete but unsealed new record is not silently discarded.
        assert!(FileStore::open(&directory.0, 1, members).await.is_err());
    }
}
