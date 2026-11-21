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
fn torn_tail_is_discarded_but_a_corrupt_complete_record_fails_closed() {
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
