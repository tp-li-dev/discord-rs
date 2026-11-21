use super::*;

const BOOT: &str = "00000000-0000-0000-0000-000000000001";
const ANOTHER_BOOT: &str = "00000000-0000-0000-0000-000000000002";

struct Root(std::path::PathBuf);

impl Root {
    fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let path = Path::new("/tmp").join(format!(
            "igr-journal-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn budget(&self, boot: &str, now: u64, max_per_window: u32) -> io::Result<Budget> {
        Budget::open(
            &self.0,
            boot,
            secs(now),
            LocalIdentifyPolicy {
                max_per_window,
                ..LocalIdentifyPolicy::default()
            },
        )
    }

    fn path(&self) -> std::path::PathBuf {
        self.0.join(JOURNAL_NAME)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn boot_id_uses_the_systemd_copy_when_proc_is_hidden() {
    let root = Root::new();
    let proc = root.0.join("proc-boot-id");
    let credential = root.0.join(BOOT_ID_CREDENTIAL);
    std::fs::write(&credential, format!("{BOOT}\n")).unwrap();
    assert_eq!(read_boot_id(&proc, Some(&root.0)).unwrap(), BOOT);
    assert!(read_boot_id(&proc, None).is_err());
    std::fs::write(&proc, ANOTHER_BOOT).unwrap();
    assert_eq!(read_boot_id(&proc, Some(&root.0)).unwrap(), ANOTHER_BOOT);
    std::fs::remove_file(&proc).unwrap();
    std::fs::write(&credential, "invalid").unwrap();
    assert_eq!(
        read_boot_id(&proc, Some(&root.0)).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn gateway(shard_id: u32, sequence: u64) -> Gateway {
    Gateway {
        key: ShardKey {
            shard_id,
            bot_id: "42".into(),
        },
        generation: LaunchGeneration {
            daemon_instance: [0; 16],
            sequence,
        },
    }
}

#[tokio::test]
async fn cancelling_an_in_flight_launch_keeps_its_charge_and_releases_the_writer() {
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout};

    let root = Root::new();
    let budget = Budget::open(
        &root.0,
        BOOT,
        boot_now().unwrap(),
        LocalIdentifyPolicy::default(),
    )
    .unwrap();
    let (entered, entered_sync) = oneshot::channel();
    let (release, release_sync) = mpsc::channel();
    let mut entered = Some(entered);
    let mut service = Service::with_writer(budget, move || {
        if let Some(entered) = entered.take() {
            entered.send(()).unwrap();
            release_sync.recv_timeout(secs(5)).unwrap();
        }
    })
    .unwrap();
    let first = gateway(0, 1);
    let mut other = gateway(0, 2);
    other.key.bot_id = "43".into();

    assert!(
        service
            .launch(&first.key, first.generation.clone(), false, false)
            .is_err()
    );
    timeout(secs(3), entered_sync).await.unwrap().unwrap();
    assert_eq!(
        service.check_restart_writer(),
        Err(DaemonError::IdentifyJournalBusy)
    );
    assert!(
        service
            .launch(&first.key, first.generation.clone(), false, false)
            .is_err(),
        "launch must wait for a durable record"
    );
    assert_eq!(service.cancel_launch(&first.key, false), Some(1));
    assert!(
        service
            .launch(&other.key, other.generation.clone(), false, false)
            .is_err()
    );
    release.send(()).unwrap();
    assert!(service.wait_for_write());
    assert_eq!(
        service.check_restart_writer(),
        Err(DaemonError::IdentifyJournalBusy)
    );

    timeout(secs(3), async {
        loop {
            match service.launch(&other.key, other.generation.clone(), false, false) {
                Ok(result) => {
                    assert_eq!(result, LocalAdmission::Granted);
                    break;
                }
                Err(_) => sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        service.check(&first.key.bot_id, 1),
        LocalAdmission::Pending { .. }
    ));
    let journal = std::fs::read_to_string(root.path()).unwrap();
    let generations: Vec<u64> = journal
        .lines()
        .map(|line| serde_json::from_str::<Record>(line).unwrap())
        .filter_map(|record| match record.action {
            Action::Launch { gateway } => Some(gateway.generation.sequence),
            _ => None,
        })
        .collect();
    assert_eq!(generations, [1, 2]);
}

#[tokio::test]
async fn slow_sync_keeps_the_owner_and_both_grant_channels_live() {
    use crate::identify_grant::GrantQueue;
    use tokio::sync::oneshot;
    use tokio::time::{Instant, sleep, timeout};
    use twilight_gateway_queue::{InMemoryQueue, Queue};

    let root = Root::new();
    let policy = LocalIdentifyPolicy {
        min_interval: Duration::ZERO,
        max_per_window: 4,
        ..Default::default()
    };
    let budget = Budget::open(&root.0, BOOT, boot_now().unwrap(), policy).unwrap();
    let (entered, entered_sync) = oneshot::channel();
    let (release, release_sync) = mpsc::channel();
    let mut entered = Some(entered);
    let mut service = Service::with_writer(budget, move || {
        if let Some(entered) = entered.take() {
            entered.send(()).unwrap();
            release_sync.recv_timeout(secs(8)).unwrap();
        }
    })
    .unwrap();

    let mut connect = |gateway| {
        let (socket, client) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        client.set_nonblocking(true).unwrap();
        service.peers.push(Peer {
            gateway,
            socket,
            first: true,
            standby: false,
            state: State::AwaitingRequest,
        });
        let client = tokio::net::UnixStream::from_std(client).unwrap();
        let local = InMemoryQueue::new(0, 0, secs(1), 0);
        GrantQueue::new(local, client, 0)
    };
    let first = gateway(0, 1);
    let a = connect(first.clone());
    let mut second = gateway(0, 2);
    second.key.bot_id = "43".into();
    let b = connect(second);
    let mut a_permit = a.enqueue(0);
    let mut entered_sync = entered_sync;
    timeout(secs(3), async {
        loop {
            service.poll(|_| (false, false));
            match entered_sync.try_recv() {
                Ok(()) => break,
                Err(oneshot::error::TryRecvError::Empty) => sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();

    let mut b_permit = b.enqueue(0);
    let until = Instant::now() + Duration::from_millis(2200);
    let mut ticks = 0;
    while Instant::now() < until {
        service.poll(|_| (false, false));
        ticks += 1;
        assert!(matches!(
            a_permit.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            b_permit.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        sleep(Duration::from_millis(10)).await;
    }
    assert!(ticks > 10, "the owner continued polling during the sync");
    assert_eq!(service.status(&first.key)["active"], "waiting");
    release.send(()).unwrap();

    // The first durable record expired in the sync. The other client must
    // receive its own permit, and the first must retry on the same socket.
    timeout(secs(4), async {
        let (mut a_ready, mut b_ready) = (false, false);
        while !a_ready || !b_ready {
            service.poll(|_| (false, false));
            if !a_ready && a_permit.try_recv().is_ok() {
                assert!(a.take_permit());
                a_ready = true;
            }
            if !b_ready && b_permit.try_recv().is_ok() {
                assert!(b.take_permit());
                b_ready = true;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let journal = std::fs::read_to_string(root.path()).unwrap();
    let grants: Vec<Record> = journal
        .lines()
        .map(|line| serde_json::from_str::<Record>(line).unwrap())
        .filter(|record| matches!(record.action, Action::Grant { .. }))
        .collect();
    assert_eq!(grants.len(), 3, "expired charge plus two usable permits");
}

#[test]
fn other_records_cannot_consume_restart_launch_and_first_grant_space() {
    let root = Root::new();
    let mut budget = root.budget(BOOT, 0, 4).unwrap();
    let replacement = gateway(0, 1);
    let mut unrelated = gateway(0, 2);
    unrelated.key.bot_id = "43".into();
    let instance = replacement.generation.daemon_instance;
    let space = RestartSpace::for_target(&replacement.key, instance).unwrap();
    let target = std::slice::from_ref(&replacement.key);
    // One byte below the conservative bound refuses without reserving anything.
    budget.limit = budget.len + space.total() - 1;
    assert_eq!(
        budget.reserve_restart_space(target, instance),
        Err(DaemonError::IdentifyJournalCapacity)
    );
    assert!(budget.restart_space.is_empty());
    // The exact bound also fits comfortably below the old 8,194-byte cutoff.
    assert!(space.total() < 8193);
    budget.limit += 1;
    assert_eq!(budget.reserve_restart_space(target, instance), Ok(()));
    assert_eq!(
        budget.launch(unrelated.clone(), secs(0), 1),
        LocalAdmission::Pending {
            retry_after: RETRY_JOURNAL_SPACE
        }
    );
    assert_eq!(
        budget.launch_with_reservation(replacement.clone(), secs(0), 1, true),
        LocalAdmission::Granted
    );
    assert_eq!(
        budget
            .first_grant_space
            .get(&replacement.key)
            .map(|grant| &grant.generation),
        Some(&replacement.generation)
    );

    // Use the rest of the unreserved space. The first grant must still fit.
    budget.limit = budget.len + space.first_grant;
    assert_eq!(
        budget.identify(&unrelated, secs(0), false, false, false).0,
        State::Waiting
    );
    assert_eq!(
        budget.identify(&replacement, secs(0), true, false, false).0,
        State::Granted
    );
    assert!(budget.first_grant_space.is_empty());
    budget.limit = budget.len + space.first_grant;
    assert_eq!(
        budget.identify(&unrelated, secs(0), false, false, false).0,
        State::Granted
    );
}

#[test]
fn first_send_replaces_its_launch_even_if_hello_arrives_after_another_shard() {
    let root = Root::new();
    let mut budget = root.budget(BOOT, 0, 2).unwrap();
    let old = gateway(0, 1);
    let new = gateway(1, 2);
    assert_eq!(
        budget.launch(old.clone(), secs(0), 1),
        LocalAdmission::Granted
    );
    assert_eq!(
        budget.launch(new.clone(), secs(30), 1),
        LocalAdmission::Granted
    );
    assert!(matches!(
        budget.identify(&new, secs(30), true, false, false),
        (State::Granted, Reply::Permit(deadline)) if deadline == nanos(secs(32))
    ));
    assert!(matches!(
        budget.identify(&old, secs(31), true, false, false),
        (State::Waiting, Reply::Wait(delay)) if delay == nanos(secs(31))
    ));
    assert_eq!(
        budget.identify(&old, secs(62), true, false, false).0,
        State::Granted
    );
    assert_eq!(
        budget.identify(&old, secs(94), false, false, false).0,
        State::Exhausted
    );
    assert_eq!(budget.check("43", secs(94), 1), LocalAdmission::Granted);
}

#[test]
fn a_lost_grant_counts_after_same_boot_restart_and_the_window_uses_expiry() {
    let root = Root::new();
    let first = gateway(0, 1);
    {
        let mut budget = root.budget(BOOT, 0, 1).unwrap();
        assert_eq!(
            budget.launch(first.clone(), secs(0), 1),
            LocalAdmission::Granted
        );
        // Drop the response without a WebSocket send.
        assert_eq!(
            budget.identify(&first, secs(5), true, false, false).0,
            State::Granted
        );
        assert!(root.budget(BOOT, 6, 1).is_err(), "second writer");
    }
    let mut budget = root.budget(BOOT, 6, 1).unwrap();
    let replacement = gateway(0, 2);
    assert!(matches!(
        budget.launch(replacement.clone(), secs(3606), 1),
        LocalAdmission::Exhausted { retry_after } if retry_after == secs(1)
    ));
    assert_eq!(
        budget.launch(replacement, secs(3607), 1),
        LocalAdmission::Granted
    );
}

#[test]
fn torn_tail_and_corrupt_complete_record_both_fail_closed() {
    use std::os::unix::fs::PermissionsExt;

    let root = Root::new();
    {
        let mut budget = root.budget(BOOT, 0, 1).unwrap();
        assert_eq!(
            budget.launch(gateway(0, 1), secs(0), 1),
            LocalAdmission::Granted
        );
    }
    let original = std::fs::read(root.path()).unwrap();
    assert_eq!(
        std::fs::metadata(root.path()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let append = |bytes: &[u8]| {
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.path())
            .unwrap()
            .write_all(bytes)
            .unwrap();
    };
    append(b"{\"version\":1");
    let torn = std::fs::read(root.path()).unwrap();
    assert!(root.budget(BOOT, 1, 1).is_err());
    assert_eq!(std::fs::read(root.path()).unwrap(), torn);
    std::fs::write(root.path(), &original).unwrap();
    {
        let mut budget = root.budget(BOOT, 1, 1).unwrap();
        assert_eq!(std::fs::read(root.path()).unwrap(), original);
        assert!(matches!(
            budget.check("42", secs(1), 1),
            LocalAdmission::Exhausted { .. }
        ));
        assert_eq!(
            budget.launch(gateway(0, 2), secs(3600), 1),
            LocalAdmission::Granted
        );
    }
    append(b"{\"version\":1\n");
    assert!(root.budget(BOOT, 3601, 1).is_err());
}

#[test]
fn missing_final_delimiter_preserves_an_acknowledged_reidentify_charge() {
    let root = Root::new();
    let active = gateway(0, 1);
    {
        // Budget without an async writer returns the permit only after sync_all.
        let mut budget = root.budget(BOOT, 0, 2).unwrap();
        assert_eq!(
            budget.launch(active.clone(), secs(0), 1),
            LocalAdmission::Granted
        );
        assert!(matches!(
            budget.identify(&active, secs(0), true, false, false),
            (State::Granted, Reply::Permit(_))
        ));
        assert!(matches!(
            budget.identify(&active, secs(32), false, false, false),
            (State::Granted, Reply::Permit(_))
        ));
    }
    let original = std::fs::read(root.path()).unwrap();
    let without_delimiter = original.strip_suffix(b"\n").unwrap();
    let last = without_delimiter.rsplit(|&b| b == b'\n').next().unwrap();
    assert!(matches!(
        serde_json::from_slice::<Record>(last).unwrap().action,
        Action::Grant {
            converts_launch: false,
            ..
        }
    ));
    std::fs::write(root.path(), without_delimiter).unwrap();
    let mut budget = root.budget(BOOT, 33, 2).unwrap();
    assert_eq!(std::fs::read(root.path()).unwrap(), original);
    assert!(matches!(
        budget.check("42", secs(33), 1),
        LocalAdmission::Exhausted { .. }
    ));
}

#[test]
fn valid_json_with_a_changed_bot_id_is_rejected_without_repair() {
    let root = Root::new();
    {
        let mut budget = root.budget(BOOT, 0, 4).unwrap();
        let active = gateway(0, 1);
        assert_eq!(
            budget.launch(active.clone(), secs(0), 1),
            LocalAdmission::Granted
        );
        assert_eq!(
            budget.identify(&active, secs(0), true, false, false).0,
            State::Granted
        );
        assert_eq!(
            budget.identify(&active, secs(32), false, false, false).0,
            State::Granted
        );
    }

    let journal = std::fs::read_to_string(root.path()).unwrap();
    let mut lines: Vec<_> = journal.lines().map(str::to_owned).collect();
    let mut last: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["action"]["converts_launch"], false);
    assert_eq!(last["action"]["gateway"]["key"]["bot_id"], "42");
    last["action"]["gateway"]["key"]["bot_id"] = "43".into();
    *lines.last_mut().unwrap() = last.to_string();
    let corrupt = lines.join("\n") + "\n";
    std::fs::write(root.path(), &corrupt).unwrap();
    assert!(root.budget(BOOT, 33, 4).is_err());
    assert_eq!(std::fs::read_to_string(root.path()).unwrap(), corrupt);
}

#[test]
fn old_format_complete_records_are_rejected_on_either_boot() {
    let root = Root::new();
    let old = serde_json::json!({
        "version": 1,
        "at_ns": 0,
        "action": {"kind": "boot", "boot_id": BOOT, "hold_until_ns": 0},
    })
    .to_string()
        + "\n";
    std::fs::write(root.path(), &old).unwrap();
    assert!(root.budget(BOOT, 0, 4).is_err());
    assert!(root.budget(ANOTHER_BOOT, 0, 4).is_err());
    assert_eq!(std::fs::read_to_string(root.path()).unwrap(), old);
}

#[test]
fn clock_regression_fails_closed_and_reboot_wait_survives_restart() {
    let root = Root::new();
    {
        let mut budget = root.budget(BOOT, 10_000, 2).unwrap();
        assert_eq!(
            budget.launch(gateway(0, 1), secs(10_001), 1),
            LocalAdmission::Granted
        );
        assert_eq!(budget.check("42", secs(9_990), 1), UNAVAILABLE);
        assert_eq!(budget.check("42", secs(20_000), 1), UNAVAILABLE);
    }
    assert!(root.budget(BOOT, 10_000, 2).is_err());
    {
        let mut budget = root.budget(ANOTHER_BOOT, 10, 2).unwrap();
        assert_eq!(
            budget.check("42", secs(10), 1),
            LocalAdmission::Pending {
                retry_after: secs(3602),
            }
        );
    }
    let mut budget = root.budget(ANOTHER_BOOT, 100, 2).unwrap();
    assert_eq!(
        budget.check("42", secs(3611), 1),
        LocalAdmission::Pending {
            retry_after: secs(1)
        }
    );
    assert_eq!(budget.check("42", secs(3612), 1), LocalAdmission::Granted);
}

#[test]
fn restart_and_gateway_update_protect_their_slots_from_reidentify() {
    let root = Root::new();
    let mut budget = root.budget(BOOT, 0, 3).unwrap();
    let active = gateway(0, 1);
    let standby = gateway(0, 2);
    assert_eq!(
        budget.launch(active.clone(), secs(0), 1),
        LocalAdmission::Granted
    );
    assert_eq!(
        budget.identify(&active, secs(0), true, false, false).0,
        State::Granted
    );
    assert_eq!(
        budget.launch(standby.clone(), secs(32), 2),
        LocalAdmission::Granted
    );
    assert_eq!(
        budget.identify(&active, secs(62), false, true, false).0,
        State::Waiting
    );
    assert_eq!(
        budget.identify(&active, secs(62), false, false, true).0,
        State::Exhausted
    );
    assert_eq!(
        budget.identify(&standby, secs(62), true, false, true).0,
        State::Granted
    );
    assert_eq!(
        budget.identify(&active, secs(94), false, false, false).0,
        State::Granted
    );
}
