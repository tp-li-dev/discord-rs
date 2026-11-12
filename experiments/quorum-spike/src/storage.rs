//! Volatile storage: callbacks acknowledge memory visibility, NEVER durable disk IO.
//! Restarting these nodes with empty storage is not a supported recovery procedure.
use crate::{Entry, Outcome, OwnershipData, TypeConfig};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use std::{fmt::Debug, ops::RangeBounds, sync::Arc};
use tokio::sync::Mutex;

type Error = StorageError<u64>;

#[derive(Default)]
struct Logs {
    entries: std::collections::BTreeMap<u64, Entry>,
    purged: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
}

#[derive(Clone, Default)]
pub struct LogStore(Arc<Mutex<Logs>>);

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        Ok(self
            .0
            .lock()
            .await
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, Error> {
        let logs = self.0.lock().await;
        Ok(LogState {
            last_purged_log_id: logs.purged,
            last_log_id: logs
                .entries
                .last_key_value()
                .map(|(_, e)| e.log_id)
                .or(logs.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), Error> {
        self.0.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, Error> {
        Ok(self.0.lock().await.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), Error> {
        self.0.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, Error> {
        Ok(self.0.lock().await.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut logs = self.0.lock().await;
        for entry in entries {
            logs.entries.insert(entry.log_id.index, entry);
        }
        // Deliberately volatile. Real storage must flush before this acknowledgement.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), Error> {
        self.0.lock().await.entries.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), Error> {
        let mut logs = self.0.lock().await;
        logs.entries.retain(|index, _| *index > log_id.index);
        logs.purged = Some(log_id);
        Ok(())
    }
}

#[derive(Default)]
struct Machine {
    data: OwnershipData,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, ()>,
    snapshot: Option<Snapshot<TypeConfig>>,
    snapshot_sequence: u64,
}

#[derive(Clone, Default)]
pub struct StateMachine(Arc<Mutex<Machine>>);

impl StateMachine {
    /// Diagnostic local read only. This is NOT a linearizable ownership grant.
    pub async fn data(&self) -> OwnershipData {
        self.0.lock().await.data.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, Error> {
        let mut sm = self.0.lock().await;
        sm.snapshot_sequence += 1;
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: sm.applied,
                last_membership: sm.membership.clone(),
                snapshot_id: format!("{:?}-{}", sm.applied, sm.snapshot_sequence),
            },
            snapshot: Box::new(sm.data.clone()),
        };
        sm.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, ()>), Error> {
        let sm = self.0.lock().await;
        Ok((sm.applied, sm.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Outcome>, Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut sm = self.0.lock().await;
        let mut results = Vec::new();
        for entry in entries {
            let result = match entry.payload {
                EntryPayload::Blank => Outcome::Noop,
                EntryPayload::Normal(request) => sm.data.transfer(&request),
                EntryPayload::Membership(membership) => {
                    sm.membership = StoredMembership::new(Some(entry.log_id), membership);
                    Outcome::Noop
                }
            };
            sm.applied = Some(entry.log_id);
            results.push(result);
        }
        Ok(results)
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
        let mut sm = self.0.lock().await;
        sm.data = *snapshot.clone();
        sm.applied = meta.last_log_id;
        sm.membership = meta.last_membership.clone();
        sm.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, Error> {
        Ok(self.0.lock().await.snapshot.clone())
    }
}
