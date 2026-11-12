use src::{protocol::BotId, state_store::*};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone)]
struct Clock(Arc<AtomicU64>);
impl StateClock for Clock {
    fn now(&self) -> Result<Timestamp, StateError> {
        Ok(Timestamp(self.0.load(Ordering::SeqCst)))
    }
}
struct Fixture {
    path: PathBuf,
    clock: Clock,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "file-state-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        Self {
            path,
            clock: Clock(Arc::new(AtomicU64::new(100))),
        }
    }
    async fn open(&self) -> FileStateStore {
        FileStateStore::open(
            &self.path,
            limits(),
            1024 * 1024,
            Arc::new(self.clock.clone()),
        )
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_entries: 8,
        max_bytes: 4096,
        max_value_bytes: 1024,
    }
}
fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).unwrap()
}
fn key(bot: u64, shard: u32, id: u8) -> StateKey {
    StateKey::new(
        StoreScope {
            bot_id: BotId(bot),
            shard_id: shard,
        },
        PublicLabel::new("app").unwrap(),
        PublicLabel::new("count").unwrap(),
        StateKind::View,
        StateId::from_bytes([id; 16]),
    )
}
fn value(data: &[u8], expiry: Option<u64>) -> StateValue {
    StateValue {
        schema: schema(),
        expires_at: expiry.map(Timestamp),
        data: data.to_vec(),
    }
}
fn ready(path: &std::path::Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".ready");
    PathBuf::from(value)
}

#[tokio::test]
async fn reopen_preserves_data_revision_and_deletion() {
    let fixture = Fixture::new();
    let first = fixture.open().await;
    assert_eq!(first.durability(), Durability::Durable);
    let revision = first
        .put(key(1, 0, 1), value(b"first", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    drop(first);
    let second = fixture.open().await;
    let record = second.get(&key(1, 0, 1), schema()).await.unwrap().unwrap();
    assert_eq!(record.revision, revision);
    assert_eq!(record.value.data, b"first");
    assert!(second.delete(&key(1, 0, 1), Some(revision)).await.unwrap());
    drop(second);
    let third = fixture.open().await;
    assert!(third.get(&key(1, 0, 1), schema()).await.unwrap().is_none());
    assert!(
        third
            .put(key(1, 0, 1), value(b"next", None), PutCondition::IfAbsent)
            .await
            .unwrap()
            > revision
    );
}

#[tokio::test]
async fn independent_handles_enforce_cas_and_scope_isolation() {
    let fixture = Fixture::new();
    let first = fixture.open().await;
    let second = fixture.open().await;
    let revision = first
        .put(key(1, 0, 1), value(b"old", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        first.put(
            key(1, 0, 1),
            value(b"a", None),
            PutCondition::IfRevision(revision)
        ),
        second.put(
            key(1, 0, 1),
            value(b"b", None),
            PutCondition::IfRevision(revision)
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(a == Err(StateError::Conflict) || b == Err(StateError::Conflict));
    for other in [key(2, 0, 1), key(1, 1, 1)] {
        assert!(second.get(&other, schema()).await.unwrap().is_none());
    }
    assert_eq!(
        second.delete(&key(1, 0, 1), Some(revision)).await,
        Err(StateError::Conflict)
    );
}

#[tokio::test]
async fn expiry_schema_capacity_and_clock_survive_reopen() {
    let fixture = Fixture::new();
    let store = fixture.open().await;
    let revision = store
        .put(key(1, 0, 1), value(b"old", Some(200)), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        store
            .put(key(1, 0, 1), value(&[0; 1025], None), PutCondition::Any)
            .await,
        Err(StateError::ValueTooLarge)
    );
    assert_eq!(
        store
            .get(&key(1, 0, 1), SchemaVersion::new(2, 2).unwrap())
            .await,
        Err(StateError::IncompatibleSchema)
    );
    fixture.clock.0.store(200, Ordering::SeqCst);
    assert!(store.get(&key(1, 0, 1), schema()).await.unwrap().is_none());
    assert_eq!(
        store
            .put(
                key(1, 0, 1),
                value(b"bad", Some(300)),
                PutCondition::IfRevision(revision)
            )
            .await,
        Err(StateError::Conflict)
    );
    assert_eq!(
        store
            .put(key(1, 0, 1), value(b"bad", Some(200)), PutCondition::Any)
            .await,
        Err(StateError::InvalidExpiry)
    );
    store
        .put(key(1, 0, 2), value(b"watermark", None), PutCondition::Any)
        .await
        .unwrap();
    drop(store);
    fixture.clock.0.store(199, Ordering::SeqCst);
    assert!(matches!(
        FileStateStore::open(
            &fixture.path,
            limits(),
            1024 * 1024,
            Arc::new(fixture.clock.clone())
        )
        .await,
        Err(StateError::ClockRegressed)
    ));
}

#[tokio::test]
async fn torn_tail_is_ignored_and_repaired_without_losing_committed_data() {
    use std::io::Write;
    let fixture = Fixture::new();
    let store = fixture.open().await;
    let revision = store
        .put(key(1, 0, 1), value(b"old", None), PutCondition::Any)
        .await
        .unwrap();
    drop(store);
    let complete_len = std::fs::metadata(&fixture.path).unwrap().len();
    let mut writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&fixture.path)
        .unwrap();
    writer.write_all(&100u64.to_be_bytes()).unwrap();
    writer.write_all(&(!100u64).to_be_bytes()).unwrap();
    writer.write_all(b"partial").unwrap();
    drop(writer);
    let store = fixture.open().await;
    assert_eq!(
        store
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .revision,
        revision
    );
    store
        .put(
            key(1, 0, 1),
            value(b"next", None),
            PutCondition::IfRevision(revision),
        )
        .await
        .unwrap();
    assert!(std::fs::metadata(&fixture.path).unwrap().len() > complete_len);
    drop(store);
    assert_eq!(
        fixture
            .open()
            .await
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"next"
    );
}

#[tokio::test]
async fn complete_corruption_fails_closed_and_diagnostics_hide_content() {
    let fixture = Fixture::new();
    let store = fixture.open().await;
    store
        .put(
            key(1, 0, 1),
            value(b"synthetic-private-payload", None),
            PutCondition::Any,
        )
        .await
        .unwrap();
    assert_eq!(format!("{store:?}"), "FileStateStore([REDACTED])");
    drop(store);
    let original = std::fs::read(&fixture.path).unwrap();
    for index in [0, 20, original.len() - 1] {
        let mut bytes = original.clone();
        bytes[index] ^= 1;
        std::fs::write(&fixture.path, bytes).unwrap();
        assert!(matches!(
            FileStateStore::open(
                &fixture.path,
                limits(),
                1024 * 1024,
                Arc::new(fixture.clock.clone())
            )
            .await,
            Err(StateError::InvalidState)
        ));
    }
}

#[tokio::test]
async fn journal_exhaustion_preserves_committed_value() {
    let fixture = Fixture::new();
    let store = fixture.open().await;
    let revision = store
        .put(key(1, 0, 1), value(b"old", None), PutCondition::Any)
        .await
        .unwrap();
    drop(store);
    let length = std::fs::metadata(&fixture.path).unwrap().len();
    let store = FileStateStore::open(
        &fixture.path,
        limits(),
        length,
        Arc::new(fixture.clock.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .put(
                key(1, 0, 1),
                value(b"new", None),
                PutCondition::IfRevision(revision)
            )
            .await,
        Err(StateError::CapacityExceeded)
    );
    assert_eq!(std::fs::metadata(&fixture.path).unwrap().len(), length);
    assert_eq!(
        store
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"old"
    );
}

#[tokio::test]
async fn entry_capacity_preserves_live_records_and_expiry_releases_capacity() {
    let fixture = Fixture::new();
    let store = FileStateStore::open(
        &fixture.path,
        StoreLimits {
            max_entries: 1,
            ..limits()
        },
        1024 * 1024,
        Arc::new(fixture.clock.clone()),
    )
    .await
    .unwrap();
    store
        .put(key(1, 0, 1), value(b"old", Some(200)), PutCondition::Any)
        .await
        .unwrap();
    assert_eq!(
        store
            .put(key(1, 0, 2), value(b"new", None), PutCondition::Any)
            .await,
        Err(StateError::CapacityExceeded)
    );
    assert_eq!(
        store
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .value
            .data,
        b"old"
    );
    fixture.clock.0.store(200, Ordering::SeqCst);
    store
        .put(key(1, 0, 2), value(b"new", None), PutCondition::IfAbsent)
        .await
        .unwrap();
    drop(store);
    assert!(
        fixture
            .open()
            .await
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancelling_future_can_still_commit_one_complete_write() {
    struct BlockingClock {
        entered: tokio::sync::Notify,
        block: std::sync::Mutex<bool>,
        release: std::sync::Condvar,
    }
    impl StateClock for BlockingClock {
        fn now(&self) -> Result<Timestamp, StateError> {
            let mut block = self.block.lock().unwrap();
            if *block {
                self.entered.notify_one();
            }
            while *block {
                block = self.release.wait(block).unwrap();
            }
            Ok(Timestamp(100))
        }
    }
    let fixture = Fixture::new();
    let clock = Arc::new(BlockingClock {
        entered: tokio::sync::Notify::new(),
        block: std::sync::Mutex::new(false),
        release: std::sync::Condvar::new(),
    });
    let store = FileStateStore::open(&fixture.path, limits(), 1024 * 1024, clock.clone())
        .await
        .unwrap();
    *clock.block.lock().unwrap() = true;
    let writer = store.clone();
    let task = tokio::spawn(async move {
        writer
            .put(
                key(1, 0, 1),
                value(b"committed", None),
                PutCondition::IfAbsent,
            )
            .await
    });
    clock.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    *clock.block.lock().unwrap() = false;
    clock.release.notify_all();
    // This operation queues behind the still-running blocking disk operation.
    let record = store.get(&key(1, 0, 1), schema()).await.unwrap().unwrap();
    assert_eq!(record.revision, 1);
    assert_eq!(record.value.data, b"committed");
    drop(store);
    assert_eq!(
        fixture
            .open()
            .await
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
async fn process_cas() {
    // Reexecute this test as two independent writers. All values are synthetic.
    if let Some(path) = std::env::var_os("FILE_STATE_TEST_PATH") {
        let result_path = PathBuf::from(std::env::var_os("FILE_STATE_TEST_RESULT").unwrap());
        let start_path = PathBuf::from(std::env::var_os("FILE_STATE_TEST_START").unwrap());
        let store = FileStateStore::open(
            path,
            limits(),
            1024 * 1024,
            Arc::new(Clock(Arc::new(AtomicU64::new(100)))),
        )
        .await
        .unwrap();
        std::fs::write(ready(&result_path), b"ready").unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !start_path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let result = store
            .put(
                key(1, 0, 1),
                value(b"child", None),
                PutCondition::IfRevision(1),
            )
            .await;
        assert!(result.is_ok() || result == Err(StateError::Conflict));
        std::fs::write(
            result_path,
            if result.is_ok() {
                b"won".as_slice()
            } else {
                b"conflict".as_slice()
            },
        )
        .unwrap();
        return;
    }
    let fixture = Fixture::new();
    let store = fixture.open().await;
    assert_eq!(
        store
            .put(key(1, 0, 1), value(b"parent", None), PutCondition::Any)
            .await
            .unwrap(),
        1
    );
    let start = fixture.path.with_extension("start");
    let results = [
        fixture.path.with_extension("result-a"),
        fixture.path.with_extension("result-b"),
    ];
    let mut children = Vec::new();
    for result in &results {
        children.push(
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process_cas", "--nocapture"])
                .env("FILE_STATE_TEST_PATH", &fixture.path)
                .env("FILE_STATE_TEST_RESULT", result)
                .env("FILE_STATE_TEST_START", &start)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        );
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while !results.iter().all(|path| ready(path).exists()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(&start, b"start").unwrap();
    for child in &mut children {
        assert!(
            tokio::time::timeout(Duration::from_secs(20), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
    let mut actual: Vec<_> = results
        .iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect();
    actual.sort();
    assert_eq!(actual, ["conflict", "won"]);
    assert_eq!(
        store
            .get(&key(1, 0, 1), schema())
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    for result in results {
        std::fs::remove_file(ready(&result)).unwrap();
        std::fs::remove_file(result).unwrap();
    }
    std::fs::remove_file(start).unwrap();
}
