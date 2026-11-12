//! Real local files; no simulated success for failed persistence/recovery.
use openraft::{
    CommittedLeaderId, EntryPayload, ErrorSubject, ErrorVerb, LogId, Membership, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
    testing::{StoreBuilder, Suite},
};
use src::raft_quorum::{
    Command, Entry, Outcome, Ownership, OwnershipData, ShardKey, TypeConfig,
    storage::{FileStore, LogStore, StateMachine},
};
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    process::{Child, Command as ProcessCommand, Stdio},
    sync::mpsc,
    time::Duration,
};

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).unwrap();
        let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        Self(std::env::temp_dir().join(format!("raftq-file-storage-{suffix}")))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        // The only owned path is a fresh random child of the OS temp directory.
        if self.0.parent() == Some(std::env::temp_dir().as_path()) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

fn members() -> BTreeSet<u64> {
    [1, 2, 3].into()
}
fn id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}
fn entry(index: u64) -> Entry {
    Entry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}

struct Builder;
impl StoreBuilder<TypeConfig, LogStore, StateMachine, Directory> for Builder {
    async fn build(&self) -> Result<(Directory, LogStore, StateMachine), StorageError<u64>> {
        let directory = Directory::new();
        let (logs, machine) = FileStore::create(&directory.0, 1, members())
            .await
            .map_err(|error| {
                StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, error)
            })?;
        Ok((directory, logs, machine))
    }
}

#[test]
fn upstream_storage_contract_suite() {
    Suite::test_all(Builder).unwrap();
}

#[tokio::test]
async fn provisioning_restart_and_identity_are_distinct() {
    let directory = Directory::new();
    assert!(FileStore::open(&directory.0, 1, members()).await.is_err());
    assert!(!directory.0.exists());
    let (logs, machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
    assert!(FileStore::create(&directory.0, 1, members()).await.is_err());
    assert!(
        FileStore::open(&directory.0, 1, members()).await.is_err(),
        "a second writer must not open the directory"
    );
    drop(logs);
    assert!(
        FileStore::open(&directory.0, 1, members()).await.is_err(),
        "the state machine must retain the lock too"
    );
    drop(machine);
    assert!(FileStore::open(&directory.0, 2, members()).await.is_err());
    assert!(
        FileStore::open(&directory.0, 1, [1, 2, 4].into())
            .await
            .is_err()
    );
    let (mut logs, machine) = FileStore::open(&directory.0, 1, members()).await.unwrap();
    assert_eq!(logs.read_vote().await.unwrap(), None);
    assert_eq!(machine.data().await.unwrap(), OwnershipData::default());
}

#[tokio::test]
async fn storage_cannot_rejoin_a_different_cluster_namespace() {
    let directory = Directory::new();
    let stores =
        FileStore::create_in_cluster(&directory.0, 1, members(), "fixture-cluster-a".into())
            .await
            .unwrap();
    drop(stores);
    assert!(
        FileStore::open_in_cluster(&directory.0, 1, members(), "fixture-cluster-b".into())
            .await
            .is_err()
    );
    assert!(FileStore::open(&directory.0, 1, members()).await.is_err());
    assert!(
        FileStore::open_in_cluster(&directory.0, 1, members(), "fixture-cluster-a".into())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn vote_log_commit_applied_membership_and_snapshot_survive_reopen() {
    let directory = Directory::new();
    let (mut logs, mut machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
    let vote = Vote::new_committed(7, 1);
    logs.save_vote(&vote).await.unwrap();
    let key = ShardKey {
        bot_id: 43,
        shard_id: 2,
    };
    let acquire = Command::Acquire {
        key,
        expected_epoch: 0,
        owner: 1,
        session: [7; 16],
    };
    let entries = vec![
        Entry {
            log_id: id(0),
            payload: EntryPayload::Membership(Membership::new(vec![members()], ())),
        },
        Entry {
            log_id: id(1),
            payload: EntryPayload::Normal(acquire),
        },
        entry(2),
    ];
    logs.blocking_append(entries.clone()).await.unwrap();
    logs.save_committed(Some(id(2))).await.unwrap();
    let outcomes = machine.apply(entries).await.unwrap();
    assert!(matches!(outcomes[1], Outcome::Applied(record) if record.epoch == 1));
    let expected_data = machine.data().await.unwrap();
    let snapshot = machine.build_snapshot().await.unwrap();
    logs.purge(id(1)).await.unwrap();
    drop(logs);
    drop(machine);
    let (mut logs, mut machine) = FileStore::open(&directory.0, 1, members()).await.unwrap();
    assert_eq!(logs.read_vote().await.unwrap(), Some(vote));
    assert_eq!(logs.read_committed().await.unwrap(), Some(id(2)));
    assert_eq!(
        logs.get_log_state().await.unwrap().last_purged_log_id,
        Some(id(1))
    );
    assert_eq!(logs.try_get_log_entries(0..).await.unwrap().len(), 1);
    assert_eq!(machine.applied_state().await.unwrap().0, Some(id(2)));
    assert_eq!(
        machine.applied_state().await.unwrap().1.membership(),
        &Membership::new(vec![members()], ())
    );
    let recovered = machine.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(recovered.meta, snapshot.meta);
    assert_eq!(recovered.snapshot, snapshot.snapshot);
    assert_eq!(machine.data().await.unwrap(), expected_data);
    logs.blocking_append([entry(3)]).await.unwrap();
    machine.apply([entry(3)]).await.unwrap();
}

#[tokio::test]
async fn installed_snapshot_is_durable_and_old_snapshot_is_rejected() {
    let directory = Directory::new();
    let (logs, mut machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
    let meta = SnapshotMeta {
        last_log_id: Some(id(12)),
        last_membership: StoredMembership::new(Some(id(0)), Membership::new(vec![members()], ())),
        snapshot_id: "fixture-snapshot-12".into(),
    };
    machine
        .install_snapshot(&meta, Box::default())
        .await
        .unwrap();
    let stale = SnapshotMeta {
        last_log_id: Some(id(11)),
        ..meta.clone()
    };
    assert!(
        machine
            .install_snapshot(&stale, Box::default())
            .await
            .is_err()
    );
    let conflicting = OwnershipData(
        [(
            ShardKey {
                bot_id: 92,
                shard_id: 0,
            },
            Ownership {
                epoch: 1,
                owner: 1,
                session: [1; 16],
                held: true,
            },
        )]
        .into(),
    );
    assert!(
        machine
            .install_snapshot(&meta, Box::new(conflicting))
            .await
            .is_err(),
        "same applied log cannot replace committed ownership with different data"
    );
    assert_eq!(machine.applied_state().await.unwrap().0, Some(id(12)));
    drop(logs);
    drop(machine);
    let (_, mut machine) = FileStore::open(&directory.0, 1, members()).await.unwrap();
    assert_eq!(
        machine.get_current_snapshot().await.unwrap().unwrap().meta,
        meta
    );
}

#[tokio::test]
async fn newer_snapshot_cannot_erase_epochs_or_resurrect_released_reservations() {
    let directory = Directory::new();
    let (_logs, mut machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
    let key = ShardKey {
        bot_id: 62,
        shard_id: 1,
    };
    let commands = [
        Command::Acquire {
            key,
            expected_epoch: 0,
            owner: 1,
            session: [5; 16],
        },
        Command::Release {
            key,
            epoch: 1,
            owner: 1,
            session: [5; 16],
        },
        Command::Acquire {
            key,
            expected_epoch: 1,
            owner: 1,
            session: [5; 16],
        },
        Command::Release {
            key,
            epoch: 2,
            owner: 1,
            session: [5; 16],
        },
    ];
    machine
        .apply(
            commands
                .into_iter()
                .enumerate()
                .map(|(index, command)| Entry {
                    log_id: id(index as u64),
                    payload: EntryPayload::Normal(command),
                }),
        )
        .await
        .unwrap();
    let original = machine.data().await.unwrap();
    let meta = SnapshotMeta {
        last_log_id: Some(id(10)),
        last_membership: StoredMembership::default(),
        snapshot_id: "fixture-newer-but-inconsistent".into(),
    };
    assert!(
        machine
            .install_snapshot(&meta, Box::default())
            .await
            .is_err()
    );
    for mutation in 0..3 {
        let mut conflicting = original.clone();
        let record = conflicting.0.get_mut(&key).unwrap();
        match mutation {
            0 => record.epoch = 1,
            1 => record.owner = 2,
            _ => record.held = true,
        }
        assert!(
            machine
                .install_snapshot(&meta, Box::new(conflicting))
                .await
                .is_err()
        );
        assert_eq!(machine.data().await.unwrap(), original);
    }
    let mut valid = original;
    let record = valid.0.get_mut(&key).unwrap();
    record.epoch = 3;
    record.owner = 2;
    record.session = [8; 16];
    record.held = true;
    machine
        .install_snapshot(&meta, Box::new(valid.clone()))
        .await
        .unwrap();
    assert_eq!(machine.data().await.unwrap(), valid);
}

#[tokio::test]
async fn corrupted_journal_or_seal_never_falls_back_to_older_state() {
    for filename in ["raft.journal", "raft.seal"] {
        let directory = Directory::new();
        let (mut logs, machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
        logs.save_vote(&Vote::new_committed(9, 1)).await.unwrap();
        drop(logs);
        drop(machine);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.0.join(filename))
            .unwrap();
        let offset = if filename == "raft.journal" { 30 } else { 12 };
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[byte[0] ^ 0x40]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(FileStore::open(&directory.0, 1, members()).await.is_err());
    }
}

#[tokio::test]
async fn partial_and_whole_record_truncation_are_rejected() {
    for truncate_at_record_boundary in [false, true] {
        let directory = Directory::new();
        let (mut logs, machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
        let path = directory.0.join("raft.journal");
        let old_length = fs::metadata(&path).unwrap().len();
        logs.save_vote(&Vote::new_committed(10, 1)).await.unwrap();
        drop(logs);
        drop(machine);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(if truncate_at_record_boundary {
            old_length
        } else {
            fs::metadata(&path).unwrap().len() - 1
        })
        .unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(FileStore::open(&directory.0, 1, members()).await.is_err());
    }
}

#[tokio::test]
async fn appended_unsealed_tail_and_missing_seal_are_rejected() {
    for remove_seal in [false, true] {
        let directory = Directory::new();
        let (mut logs, machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
        logs.save_vote(&Vote::new_committed(10, 1)).await.unwrap();
        drop(logs);
        drop(machine);
        if remove_seal {
            fs::remove_file(directory.0.join("raft.seal")).unwrap();
        } else {
            let mut file = OpenOptions::new()
                .append(true)
                .open(directory.0.join("raft.journal"))
                .unwrap();
            file.write_all(b"partial-next-frame").unwrap();
            file.sync_all().unwrap();
        }
        assert!(FileStore::open(&directory.0, 1, members()).await.is_err());
    }
}

#[tokio::test]
async fn a_log_gap_is_rejected_and_committed_state_cannot_be_truncated() {
    let directory = Directory::new();
    let (mut logs, mut machine) = FileStore::create(&directory.0, 1, members()).await.unwrap();
    logs.blocking_append([entry(0)]).await.unwrap();
    assert!(logs.blocking_append([entry(2)]).await.is_err());
    assert_eq!(logs.get_log_state().await.unwrap().last_log_id, Some(id(0)));
    logs.save_committed(Some(id(0))).await.unwrap();
    assert!(
        logs.truncate(id(0)).await.is_err(),
        "committed but not yet applied must survive"
    );
    let conflict = Entry {
        log_id: id(0),
        payload: EntryPayload::Membership(Membership::new(vec![members()], ())),
    };
    assert!(logs.blocking_append([conflict]).await.is_err());
    logs.blocking_append([entry(0)]).await.unwrap();
    machine.apply([entry(0)]).await.unwrap();
    assert!(logs.truncate(id(0)).await.is_err());
    assert_eq!(machine.applied_state().await.unwrap().0, Some(id(0)));
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn killed_writer_recovers_durable_epoch_without_running_destructors() {
    let directory = Directory::new();
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command.env_clear();
    let temporary_root = std::env::temp_dir();
    command
        .env("TMP", &temporary_root)
        .env("TEMP", &temporary_root)
        .env("TMPDIR", &temporary_root);
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    let mut child = ChildGuard(
        command
            .args(["--ignored", "--exact", "storage_crash_child", "--nocapture"])
            .env("RAFTQ_STORAGE_CRASH_PATH", &directory.0)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.is_ok_and(|line| line.ends_with("RAFT_STORAGE_SYNCED")) {
                let _ = tx.send(());
                break;
            }
        }
    });
    let ready = rx.recv_timeout(Duration::from_secs(20));
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    reader.join().unwrap();
    assert!(ready.is_ok(), "writer did not acknowledge completed fsync");
    let (mut logs, machine) = FileStore::open(&directory.0, 1, members()).await.unwrap();
    assert_eq!(
        logs.read_vote().await.unwrap(),
        Some(Vote::new_committed(4, 1))
    );
    let key = ShardKey {
        bot_id: 73,
        shard_id: 4,
    };
    let data = machine.data().await.unwrap();
    assert_eq!(data.0[&key].epoch, 1);
    assert!(data.0[&key].held);
}

#[test]
#[ignore = "subprocess helper used by killed_writer_recovers_durable_epoch_without_running_destructors"]
fn storage_crash_child() {
    let path =
        PathBuf::from(std::env::var_os("RAFTQ_STORAGE_CRASH_PATH").expect("owned fixture path"));
    assert_eq!(path.parent(), Some(std::env::temp_dir().as_path()));
    assert!(
        path.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("raftq-file-storage-")
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    // Keep stores and the runtime alive until the parent forcibly kills us.
    let _stores = runtime.block_on(async {
        let (mut logs, mut machine) = FileStore::create(path, 1, members()).await.unwrap();
        logs.save_vote(&Vote::new_committed(4, 1)).await.unwrap();
        let acquire = Command::Acquire {
            key: ShardKey {
                bot_id: 73,
                shard_id: 4,
            },
            expected_epoch: 0,
            owner: 1,
            session: [9; 16],
        };
        let entry = Entry {
            log_id: id(0),
            payload: EntryPayload::Normal(acquire),
        };
        logs.blocking_append([entry.clone()]).await.unwrap();
        machine.apply([entry]).await.unwrap();
        (logs, machine)
    });
    println!("RAFT_STORAGE_SYNCED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
