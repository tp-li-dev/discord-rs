//! Authenticated local certificate-policy snapshots, without private keys.
//!
//! All file operations are synchronous: callers on Tokio must use a blocking
//! worker, outside the Discord/transport hot path. The caller supplies a separate
//! 32-byte authentication key from a protected secret store. It is never written
//! here. This authenticates policy bytes; it does not encrypt public certificates
//! or protect a host on which the key or process has been compromised.
//!
//! A same-directory temporary file is fully written and synced before atomic
//! replacement. On Unix the containing directory is synced afterwards. An error
//! after replacement is `UnknownOutcome`: reopen and reconcile before activation.
//! Windows replacement is atomic for ordinary filesystem readers, but directory
//! durability across power loss, network filesystems, media failures, ACLs and
//! ownership are not guaranteed. Keep the directory private and trusted; hostile
//! replacement of parent directories or lock files is outside this API's model.
//! The persistent sidecar must not be deleted while any store is open. Its OS
//! lock releases on process exit, so a crash does not require deleting the file.
//!
//! Authentication detects corruption and modifications without the key. A valid
//! old snapshot can still be replayed after a restart: provide `minimum_revision`
//! from an independently protected high-water mark. Missing or corrupt policy
//! must isolate the listener, never silently restore permissive defaults. Absolute
//! Unix millisecond retirement deadlines must be restored without extending the
//! overlap; the transport also needs a trustworthy clock and clock-regression
//! handling. This file API does not itself activate or distribute policy.
//!
//! # Node-to-node distribution design (not implemented)
//!
//! Use the existing authenticated mTLS control channel, then require an admin
//! RBAC permission for policy changes. Network membership alone does not authorize
//! an update. Authenticate each immutable policy envelope with a dedicated policy
//! signing authority (or a deliberately scoped MAC key), bind it to an explicit
//! cluster ID and schema version, and reject replayed revisions and unexpected
//! predecessor hashes. A cluster ID is not yet part of this local snapshot; a
//! future distribution envelope must supply and authenticate it.
//!
//! Validate bounds, certificate paths, membership and deadlines before writing;
//! durably persist and acknowledge the accepted revision before activating it.
//! Reconcile a crash between persistence and activation on restart before bind.
//! Stage the new CA on all eligible nodes, collect authenticated durable ACKs,
//! rotate node credentials, then retire the old CA at the agreed deadline.
//! Offline nodes must catch up under a trusted bootstrap path or remain isolated;
//! do not extend deadlines implicitly. Emergency revocation overrides overlap.
//! Preserve a recovery trust path so revoking a CA does not silently authorize
//! an unauthenticated administration fallback. Use the existing OpenRaft backend
//! if ordered cluster agreement is required; do not introduce custom consensus,
//! claim atomic cluster-wide activation, or infer lease authority from policy ACKs.
//! No remote endpoint, file watcher, policy issuer, CRL or OCSP client is provided.

use crate::redact::Secret;
use rustls::{crypto::tls13::OkmBlock, pki_types::CertificateDer};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use subtle::ConstantTimeEq;

pub const POLICY_VERSION: u32 = 1;
pub const MAX_POLICY_BYTES: usize = 16 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"BCPOL001";
const TAG_BYTES: usize = 32;
const HEADER_BYTES: usize = 12;

/// SHA-256 of the complete DER certificate, not its serial or subject name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CertificateFingerprint(pub [u8; 32]);

/// Metadata only. A fingerprint is not certificate/path authentication.
pub fn certificate_fingerprint(cert: &CertificateDer<'_>) -> CertificateFingerprint {
    let suite = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .expect("configured TLS 1.3 suite");
    let digest = suite.common.hash_provider.hash(cert.as_ref());
    let mut bytes = [0; 32];
    bytes.copy_from_slice(digest.as_ref());
    CertificateFingerprint(bytes)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaRotationSnapshot {
    pub old_cas: Vec<Vec<u8>>,
    pub retire_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerRotationSnapshot {
    pub node: u64,
    pub old: Vec<u8>,
    pub new: Vec<u8>,
    pub retire_at_unix_ms: u64,
}

/// Public certificate data and policy only. No TLS or authentication keys.
/// `trusted_cas` is the replacement/current set; `old_cas` is overlap-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    pub version: u32,
    pub revision: u64,
    pub saved_at_unix_ms: u64,
    pub trusted_cas: Vec<Vec<u8>>,
    pub ca_rotation: Option<CaRotationSnapshot>,
    pub revoked_cas: BTreeSet<CertificateFingerprint>,
    pub revoked_nodes: BTreeSet<u64>,
    pub revoked_serials: Vec<Vec<u8>>,
    pub peer_rotations: Vec<PeerRotationSnapshot>,
}

impl PolicySnapshot {
    /// Structural validation only; transport activation must validate CA DER,
    /// certificate paths, node membership, and local-clock assumptions too.
    pub fn validate(&self) -> Result<(), PolicyError> {
        fn cas(certs: &[Vec<u8>]) -> bool {
            !certs.is_empty()
                && certs.len() <= 64
                && certs.iter().all(|c| !c.is_empty() && c.len() <= 64 * 1024)
                && certs.iter().collect::<BTreeSet<_>>().len() == certs.len()
        }
        fn serial(bytes: &[u8]) -> bool {
            !bytes.is_empty() && bytes.len() <= 20 && (bytes.len() == 1 || bytes[0] != 0)
        }
        if self.version != POLICY_VERSION
            || self.revision == 0
            || !cas(&self.trusted_cas)
            || self.ca_rotation.as_ref().is_some_and(|r| !cas(&r.old_cas))
            || self.revoked_cas.len() > 4096
            || self.revoked_nodes.len() > 4096
            || self.revoked_serials.len() > 65536
            || self.peer_rotations.len() > 4096
            || !self.revoked_serials.iter().all(|s| serial(s))
            || self.revoked_serials.iter().collect::<BTreeSet<_>>().len()
                != self.revoked_serials.len()
            || self
                .peer_rotations
                .iter()
                .map(|r| r.node)
                .collect::<BTreeSet<_>>()
                .len()
                != self.peer_rotations.len()
            || self
                .peer_rotations
                .iter()
                .any(|r| !serial(&r.old) || !serial(&r.new) || r.old == r.new)
        {
            return Err(PolicyError::InvalidPolicy);
        }
        Ok(())
    }
}

/// Fixed diagnostics: no paths, serialized data, library errors, or key material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    InvalidPolicy,
    AuthenticationFailed,
    Corrupt,
    TooLarge,
    StaleRevision,
    NotFound,
    Locked,
    Io,
    UnknownOutcome,
}
impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "certificate policy: {self:?}")
    }
}
impl std::error::Error for PolicyError {}

/// Single-writer store. An open instance keeps a separate, persistent OS lock.
/// `save` rejects a revision equal to or below any revision already observed.
pub struct PolicyStore {
    path: PathBuf,
    key: Secret<[u8; 32]>,
    _lock: File,
    highest_revision: u64,
    poisoned: bool,
}
impl fmt::Debug for PolicyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyStore")
            .field("key", &self.key)
            .field("highest_revision", &self.highest_revision)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}
impl PolicyStore {
    pub fn open(path: impl AsRef<Path>, key: Secret<[u8; 32]>) -> Result<Self, PolicyError> {
        let path = path.as_ref().to_path_buf();
        let name = path.file_name().ok_or(PolicyError::InvalidPolicy)?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".lock");
        let lock_path = path.with_file_name(lock_name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = match options.open(&lock_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|_| PolicyError::Io)?,
            Err(_) => return Err(PolicyError::Io),
        };
        lock.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => PolicyError::Locked,
            _ => PolicyError::Io,
        })?;
        let mut store = Self {
            path,
            key,
            _lock: lock,
            highest_revision: 0,
            poisoned: false,
        };
        match store.read_snapshot() {
            Ok(snapshot) => store.highest_revision = snapshot.revision,
            Err(PolicyError::NotFound) => {}
            Err(error) => return Err(error),
        }
        Ok(store)
    }

    pub fn load(&mut self, minimum_revision: u64) -> Result<PolicySnapshot, PolicyError> {
        if self.poisoned {
            return Err(PolicyError::UnknownOutcome);
        }
        let snapshot = self.read_snapshot()?;
        if snapshot.revision < minimum_revision.max(self.highest_revision) {
            return Err(PolicyError::StaleRevision);
        }
        self.highest_revision = snapshot.revision;
        Ok(snapshot)
    }

    pub fn save(&mut self, snapshot: &PolicySnapshot) -> Result<(), PolicyError> {
        if self.poisoned {
            return Err(PolicyError::UnknownOutcome);
        }
        snapshot.validate()?;
        // Authenticate the current file again. Never overwrite corruption or
        // silently treat external deletion of an observed policy as bootstrap.
        match self.read_snapshot() {
            Ok(current) => {
                if current.revision < self.highest_revision {
                    return Err(PolicyError::StaleRevision);
                }
                self.highest_revision = current.revision;
            }
            Err(PolicyError::NotFound) if self.highest_revision == 0 => {}
            Err(error) => return Err(error),
        }
        if snapshot.revision <= self.highest_revision {
            return Err(PolicyError::StaleRevision);
        }
        let payload = serde_json::to_vec(snapshot).map_err(|_| PolicyError::InvalidPolicy)?;
        if payload.len() > MAX_POLICY_BYTES - HEADER_BYTES - TAG_BYTES {
            return Err(PolicyError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(HEADER_BYTES + payload.len() + TAG_BYTES);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&self.tag(&bytes));
        let (temporary, mut file) = self.temporary()?;
        let result = (|| {
            file.write_all(&bytes).map_err(|_| PolicyError::Io)?;
            file.sync_all().map_err(|_| PolicyError::Io)?;
            drop(file);
            std::fs::rename(&temporary, &self.path).map_err(|_| PolicyError::Io)?;
            #[cfg(unix)]
            {
                let parent = parent_dir(&self.path);
                if File::open(parent).and_then(|f| f.sync_all()).is_err() {
                    self.poisoned = true;
                    return Err(PolicyError::UnknownOutcome);
                }
            }
            self.highest_revision = snapshot.revision;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        result
    }

    fn tag(&self, bytes: &[u8]) -> Vec<u8> {
        let suite = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("configured TLS 1.3 suite");
        suite
            .hkdf_provider
            .hmac_sign(&OkmBlock::new(self.key.expose_secret()), bytes)
            .as_ref()
            .to_vec()
    }

    fn read_snapshot(&self) -> Result<PolicySnapshot, PolicyError> {
        let file = File::open(&self.path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                PolicyError::NotFound
            } else {
                PolicyError::Io
            }
        })?;
        if file.metadata().map_err(|_| PolicyError::Io)?.len() > MAX_POLICY_BYTES as u64 {
            return Err(PolicyError::TooLarge);
        }
        let mut bytes = Vec::new();
        file.take(MAX_POLICY_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| PolicyError::Io)?;
        if bytes.len() > MAX_POLICY_BYTES {
            return Err(PolicyError::TooLarge);
        }
        if bytes.len() < HEADER_BYTES + TAG_BYTES || &bytes[..8] != MAGIC {
            return Err(PolicyError::Corrupt);
        }
        let length =
            u32::from_be_bytes(bytes[8..12].try_into().map_err(|_| PolicyError::Corrupt)?) as usize;
        if length != bytes.len() - HEADER_BYTES - TAG_BYTES {
            return Err(PolicyError::Corrupt);
        }
        let (authenticated, tag) = bytes.split_at(bytes.len() - TAG_BYTES);
        if self.tag(authenticated).ct_eq(tag).unwrap_u8() != 1 {
            return Err(PolicyError::AuthenticationFailed);
        }
        let snapshot: PolicySnapshot =
            serde_json::from_slice(&bytes[HEADER_BYTES..HEADER_BYTES + length])
                .map_err(|_| PolicyError::Corrupt)?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn temporary(&self) -> Result<(PathBuf, File), PolicyError> {
        for _ in 0..8 {
            let mut random = [0; 16];
            getrandom::fill(&mut random).map_err(|_| PolicyError::Io)?;
            let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
            let path = parent_dir(&self.path).join(format!(".cert-policy-{suffix}.tmp"));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => return Ok((path, file)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(PolicyError::Io),
            }
        }
        Err(PolicyError::Io)
    }
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
