//! Local persistence tests. Keys are synthetic and never emitted or saved.
use src::{
    cert_policy::{
        CaRotationSnapshot, CertificateFingerprint, MAX_POLICY_BYTES, POLICY_VERSION,
        PeerRotationSnapshot, PolicyError, PolicySnapshot, PolicyStore, certificate_fingerprint,
    },
    redact::Secret,
};
use std::{collections::BTreeSet, fs, path::PathBuf};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let mut random = [0; 16];
        getrandom::fill(&mut random).unwrap();
        let name: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let path = std::env::temp_dir().join(format!("cert-policy-test-{name}"));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("policy.bin")
    }
    fn open(&self) -> PolicyStore {
        PolicyStore::open(self.path(), Secret::new([7; 32])).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // A unique, fixed fixture directory created above; no user paths.
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn snapshot(revision: u64) -> PolicySnapshot {
    PolicySnapshot {
        version: POLICY_VERSION,
        revision,
        saved_at_unix_ms: 1000,
        trusted_cas: vec![vec![1, 2, 3]],
        ca_rotation: Some(CaRotationSnapshot {
            old_cas: vec![vec![4, 5, 6]],
            retire_at_unix_ms: 2000,
        }),
        revoked_cas: BTreeSet::from([CertificateFingerprint([9; 32])]),
        revoked_nodes: BTreeSet::from([2]),
        revoked_serials: vec![vec![10]],
        peer_rotations: vec![PeerRotationSnapshot {
            node: 3,
            old: vec![11],
            new: vec![12],
            retire_at_unix_ms: 3000,
        }],
    }
}

#[test]
fn authenticated_roundtrip_replacement_and_reopen_preserve_deadlines() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    assert_eq!(store.load(0), Err(PolicyError::NotFound));
    let first = snapshot(1);
    store.save(&first).unwrap();
    assert_eq!(store.load(1).unwrap(), first);
    let mut next = snapshot(2);
    next.revoked_nodes.insert(4);
    store.save(&next).unwrap();
    assert_eq!(store.load(2).unwrap(), next);
    drop(store);
    let mut reopened = fixture.open();
    assert_eq!(reopened.load(2).unwrap(), next);
    assert_eq!(reopened.load(3), Err(PolicyError::StaleRevision));
    let files: Vec<_> = fs::read_dir(&fixture.0).unwrap().collect();
    assert_eq!(files.len(), 2, "snapshot and stable lock only");
}

#[test]
fn writer_lock_is_exclusive_and_reusable_without_unlinking() {
    let fixture = Fixture::new();
    let first = fixture.open();
    assert!(matches!(
        PolicyStore::open(fixture.path(), Secret::new([7; 32])),
        Err(PolicyError::Locked)
    ));
    drop(first);
    let _next = fixture.open();
}

#[test]
fn tamper_truncation_trailing_bytes_and_wrong_key_fail_closed() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    store.save(&snapshot(1)).unwrap();
    let original = fs::read(fixture.path()).unwrap();
    for length in [0, 8, 12, original.len() - 1] {
        fs::write(fixture.path(), &original[..length]).unwrap();
        assert_eq!(store.load(0), Err(PolicyError::Corrupt));
        assert_eq!(store.save(&snapshot(2)), Err(PolicyError::Corrupt));
        assert_eq!(fs::read(fixture.path()).unwrap(), &original[..length]);
    }
    let mut tampered = original.clone();
    tampered[20] ^= 1;
    fs::write(fixture.path(), &tampered).unwrap();
    assert_eq!(store.load(0), Err(PolicyError::AuthenticationFailed));
    assert_eq!(
        store.save(&snapshot(2)),
        Err(PolicyError::AuthenticationFailed)
    );
    assert_eq!(fs::read(fixture.path()).unwrap(), tampered);
    let mut trailing = original.clone();
    trailing.push(0);
    fs::write(fixture.path(), trailing).unwrap();
    assert_eq!(store.load(0), Err(PolicyError::Corrupt));
    fs::write(fixture.path(), original).unwrap();
    drop(store);
    assert!(matches!(
        PolicyStore::open(fixture.path(), Secret::new([8; 32])),
        Err(PolicyError::AuthenticationFailed)
    ));
}

#[test]
fn revision_regression_and_observed_snapshot_deletion_are_rejected() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    store.save(&snapshot(1)).unwrap();
    let old = fs::read(fixture.path()).unwrap();
    store.save(&snapshot(2)).unwrap();
    for revision in [0, 1, 2] {
        let result = store.save(&snapshot(revision));
        assert_eq!(
            result,
            Err(if revision == 0 {
                PolicyError::InvalidPolicy
            } else {
                PolicyError::StaleRevision
            })
        );
    }
    fs::write(fixture.path(), old).unwrap();
    assert_eq!(store.load(0), Err(PolicyError::StaleRevision));
    assert_eq!(store.save(&snapshot(3)), Err(PolicyError::StaleRevision));
    fs::remove_file(fixture.path()).unwrap();
    assert_eq!(store.save(&snapshot(3)), Err(PolicyError::NotFound));
}

#[test]
fn valid_old_snapshot_requires_external_revision_floor_after_restart() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    store.save(&snapshot(1)).unwrap();
    let old = fs::read(fixture.path()).unwrap();
    store.save(&snapshot(2)).unwrap();
    drop(store);
    fs::write(fixture.path(), old).unwrap();
    let mut reopened = fixture.open();
    assert_eq!(reopened.load(2), Err(PolicyError::StaleRevision));
    // MAC alone does not prove freshness after reopening.
    assert_eq!(reopened.load(0).unwrap().revision, 1);
}

#[test]
fn structural_and_file_bounds_are_enforced_without_overwriting_policy() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    store.save(&snapshot(1)).unwrap();
    let original = fs::read(fixture.path()).unwrap();
    let mut invalid = snapshot(2);
    invalid.trusted_cas = vec![vec![0]; 65];
    assert_eq!(store.save(&invalid), Err(PolicyError::InvalidPolicy));
    invalid = snapshot(2);
    invalid.revoked_serials = vec![vec![0, 1]];
    assert_eq!(store.save(&invalid), Err(PolicyError::InvalidPolicy));
    invalid = snapshot(2);
    invalid
        .peer_rotations
        .push(invalid.peer_rotations[0].clone());
    assert_eq!(store.save(&invalid), Err(PolicyError::InvalidPolicy));
    assert_eq!(fs::read(fixture.path()).unwrap(), original);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(fixture.path())
        .unwrap();
    file.set_len(MAX_POLICY_BYTES as u64 + 1).unwrap();
    drop(file);
    assert_eq!(store.load(0), Err(PolicyError::TooLarge));
    assert_eq!(store.save(&snapshot(2)), Err(PolicyError::TooLarge));
}

#[test]
fn encoded_size_limit_is_checked_before_creating_a_temporary_file() {
    let fixture = Fixture::new();
    let mut store = fixture.open();
    store.save(&snapshot(1)).unwrap();
    let original = fs::read(fixture.path()).unwrap();
    let mut oversized = snapshot(2);
    // Each DER item fits its bound, but JSON decimal byte encoding exceeds
    // the complete-file budget. Distinct certificates avoid duplicate rejection.
    oversized.trusted_cas = (0..64)
        .map(|index| {
            let mut certificate = vec![255; 64 * 1024];
            certificate[0] = index;
            certificate
        })
        .collect();
    oversized.validate().unwrap();
    assert_eq!(store.save(&oversized), Err(PolicyError::TooLarge));
    assert_eq!(fs::read(fixture.path()).unwrap(), original);
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
}

#[test]
fn debug_and_errors_do_not_format_key_or_snapshot_data() {
    let fixture = Fixture::new();
    let store = fixture.open();
    let debug = format!("{store:#?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("policy.bin"));
    assert!(!debug.contains("[7, 7"));
    assert!(std::error::Error::source(&PolicyError::Io).is_none());
}

#[test]
fn fingerprint_hashes_complete_der_bytes() {
    use rustls::pki_types::CertificateDer;
    let cert = CertificateDer::from(b"abc".as_slice());
    assert_eq!(
        certificate_fingerprint(&cert),
        CertificateFingerprint([
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ])
    );
}
