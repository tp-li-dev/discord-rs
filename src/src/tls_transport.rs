//! Bounded, mutually authenticated daemon control transport.
//!
//! This is an adapter for `cluster::Transport`, not a consensus or lease backend.
//! One TLS 1.3 connection carries one message and a queue-admission ACK. There is
//! no automatic retry, persistence or global ordering.
//! Cancelling `send_confirmed`, losing its ACK or shutting down can leave delivery
//! unknown. The synchronous trait method acknowledges only local queue admission.
//!
//! Certificate identity profile: exactly one DNS SAN `node-<u64>.cluster`, with
//! canonical decimal digits. If SAN is absent, exactly one matching UTF8String or
//! PrintableString CN is accepted. A CN alongside SAN must match. Other SAN forms,
//! duplicate identities and wildcards are rejected. Trust, time, EKU, certificate
//! signatures and TLS proof of possession are checked by rustls/webpki.
//! CN-only identities additionally require a leaf-only chain and trust anchors
//! without name constraints, since DNS name constraints do not constrain a CN.
//!
//! Leaf certificate/key rotation under the configured CAs is a local API:
//! first install `rotate_peer_certificate` on every peer, then `rotate_identity`
//! on the node being rotated, before the overlap expires. New connections use
//! the replacement immediately; existing connections keep their original TLS
//! credentials for their one bounded exchange (no TLS renegotiation). Peers
//! accept only the old/new serials during overlap and only the new serial after
//! it. Deadlines use the local monotonic clock and are checked on use, with no
//! timer task or restart required. Concurrent rotations of one peer are refused.
//!
//! Revocation is checked during TLS verification and again before application
//! writes / receive-queue admission. Idle sockets close on their existing I/O
//! deadline; an update does not synchronously abort them. Admission and policy
//! updates are serialized; already admitted messages and bytes already handed
//! to the socket cannot be withdrawn. An ACK lost during an update still means
//! delivery may be unknown. Resumption, tickets and early data remain disabled.
//!
//! These APIs require a trusted in-process caller. Distribution, persistence,
//! file watching, CA/root rotation and remote administration are not provided.
//! On restart callers must isolate the listener until peer policies are restored;
//! bind accepts initial revocations, but does not restore old rotation state.
//! A new transport otherwise defaults to the configured CA/membership policy.
//!
//! Envelope timestamps are local elapsed times, not synchronized clock evidence.
//! Heartbeat/proposal payload times retain their caller-provided clock domain.
//! Existing simulation policy requires a shared injected clock; do not interpret
//! these values as safe cross-host lease deadlines.
//! The separate `TestOnly` payload has no remote deadlines: its quorum fixture
//! measures request/hold deadlines locally and binds an exact test configuration.

use crate::{
    cluster::{
        ControlMessage, Envelope, Heartbeat, NodeId, OwnershipProposal, OwnershipRecord,
        ProposalKind, SimulationError, TestOnlyClaim, TestOnlyMessage, TestOnlyPayload,
        TestOnlyRequest, Transport,
    },
    ownership::{Lease, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    redact::Secret,
};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        verify_server_cert_signed_by_trust_anchor,
    },
    crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    server::{
        ParsedCertificate, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::{Future, poll_fn},
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub const ALPN: &[u8] = b"botd-cluster/1";
const VERSION: u16 = 1;
const MAX_FRAME: usize = 8 * 1024 * 1024;
const MAX_CERT: usize = 64 * 1024;

/// Fixed diagnostics only: never carry certificate, key, payload or library text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportError {
    InvalidConfig,
    ExternalBindDenied,
    InvalidIdentity,
    UnauthorizedNode,
    IdentityMismatch,
    CertificateRevoked,
    CertificateRetired,
    RotationInProgress,
    Tls,
    Io,
    HandshakeTimeout,
    ReadTimeout,
    WriteTimeout,
    ConnectionLimit,
    QueueFull,
    Closed,
    FrameTooLarge,
    InvalidFrame,
    CounterExhausted,
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TLS transport: {self:?}")
    }
}
impl std::error::Error for TransportError {}
impl From<TransportError> for SimulationError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

/// DER chain, leaf first. Keys are consumed into rustls and never formatted here.
pub struct TlsIdentity {
    pub certificates: Vec<CertificateDer<'static>>,
    pub private_key: Secret<PrivateKeyDer<'static>>,
}
impl fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsIdentity")
            .field("certificate_count", &self.certificates.len())
            .field("private_key", &self.private_key)
            .finish()
    }
}

/// Unsigned big-endian X.509 leaf serial, normalized without leading zeroes.
/// Serial revocations apply across ALL configured issuers: reuse of a serial
/// by different CAs deliberately revokes both certificates (fail closed).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CertificateSerial(Vec<u8>);
impl CertificateSerial {
    /// Accepts up to 20 significant bytes, with at most 21 input bytes including
    /// optional leading zeroes. Zero is preserved for existing CA compatibility.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TransportError> {
        if bytes.is_empty() || bytes.len() > 21 {
            return Err(TransportError::InvalidIdentity);
        }
        let first = bytes
            .iter()
            .position(|b| *b != 0)
            .unwrap_or(bytes.len() - 1);
        let bytes = &bytes[first..];
        if bytes.len() > 20 {
            return Err(TransportError::InvalidIdentity);
        }
        Ok(Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A local deny-list, not an X.509 CRL or OCSP implementation. Node entries
/// revoke every leaf for that node; serial entries revoke that leaf serial
/// across all issuers. CA/intermediate revocation is not represented here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RevocationList {
    /// At most 4096 node IDs per snapshot.
    pub nodes: BTreeSet<NodeId>,
    /// At most 65536 leaf serials per snapshot.
    pub serials: BTreeSet<CertificateSerial>,
}
impl RevocationList {
    fn validate(&self) -> Result<(), TransportError> {
        if self.nodes.len() > 4096 || self.serials.len() > 65536 {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }

    fn check(&self, node: NodeId, serial: &CertificateSerial) -> Result<(), TransportError> {
        if self.nodes.contains(&node) || self.serials.contains(serial) {
            return Err(TransportError::CertificateRevoked);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PeerRotation {
    old: CertificateSerial,
    new: CertificateSerial,
    deadline: Instant,
}

#[derive(Debug, Default)]
struct CertificatePolicy {
    revocations: RevocationList,
    rotations: BTreeMap<NodeId, PeerRotation>,
}
impl CertificatePolicy {
    fn check(&self, identity: &CertificateIdentity) -> Result<(), TransportError> {
        self.revocations.check(identity.node, &identity.serial)?;
        if let Some(rotation) = self.rotations.get(&identity.node)
            && identity.serial != rotation.new
            && (identity.serial != rotation.old || Instant::now() >= rotation.deadline)
        {
            return Err(TransportError::CertificateRetired);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    pub max_frame_len: usize,
    /// Shared inbound/outbound socket and TLS task limit, including handshakes.
    pub max_connections: usize,
    /// Separate limits for inbound messages, outbound requests and diagnostics.
    pub queue_capacity: usize,
    pub handshake_timeout: Duration,
    /// Absolute deadline for an entire frame, including its length prefix.
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_frame_len: 1024 * 1024,
            max_connections: 32,
            queue_capacity: 128,
            handshake_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
        }
    }
}
impl TransportLimits {
    fn validate(self) -> Result<(), TransportError> {
        if !(83..=MAX_FRAME).contains(&self.max_frame_len)
            || !(1..=1024).contains(&self.max_connections)
            || !(1..=65536).contains(&self.queue_capacity)
            || [
                self.handshake_timeout,
                self.read_timeout,
                self.write_timeout,
            ]
            .into_iter()
            .any(|d| d.is_zero() || d > Duration::from_secs(300))
        {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }
}

/// Trust anchors and membership are immutable for one listener lifetime.
/// Leaf credentials and revocations can be updated through `TlsTransport`.
pub struct TlsTransportConfig {
    pub node_id: NodeId,
    /// Caller must allocate a fresh, nonzero incarnation after a node restart.
    pub incarnation: u64,
    pub bind_addr: SocketAddr,
    pub allow_external_bind: bool,
    pub allowed_nodes: BTreeSet<NodeId>,
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub identity: TlsIdentity,
    pub trusted_cas: Vec<CertificateDer<'static>>,
    pub revocations: RevocationList,
    pub limits: TransportLimits,
}
impl TlsTransportConfig {
    pub fn new(
        node_id: NodeId,
        identity: TlsIdentity,
        trusted_cas: Vec<CertificateDer<'static>>,
    ) -> Self {
        Self {
            node_id,
            incarnation: 1,
            bind_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            allow_external_bind: false,
            allowed_nodes: BTreeSet::from([node_id]),
            peers: BTreeMap::new(),
            identity,
            trusted_cas,
            revocations: RevocationList::default(),
            limits: TransportLimits::default(),
        }
    }
}
impl fmt::Debug for TlsTransportConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsTransportConfig")
            .field("node_id", &self.node_id)
            .field("bind_addr", &self.bind_addr)
            .field("identity", &self.identity)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

pub fn node_name(node: NodeId) -> String {
    format!("node-{}.cluster", node.0)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportStats {
    /// Accepted TCP sockets, including peers later rejected by TLS.
    pub accepted: u64,
    /// Messages admitted into the local receive queue.
    pub received: u64,
    /// Inbound connection errors, including an ACK failure after admission.
    /// Such failures can also be counted in `received`; this is not a drop count.
    pub rejected: u64,
    /// Outbound messages with a remote queue-admission ACK.
    pub delivered: u64,
    /// Outbound I/O/admission errors; cancellation is not counted here.
    pub failed: u64,
    pub active_connections: usize,
}
#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    received: AtomicU64,
    rejected: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
}

struct Outbound {
    target: NodeId,
    address: SocketAddr,
    frame: Vec<u8>,
    result: Option<oneshot::Sender<Result<(), TransportError>>>,
}
struct Context {
    node: NodeId,
    incarnation: u64,
    start: Instant,
    limits: TransportLimits,
    credentials: Arc<RwLock<Arc<TlsCredentials>>>,
    policy: Arc<RwLock<CertificatePolicy>>,
    incoming: mpsc::Sender<Envelope>,
    errors: mpsc::Sender<TransportError>,
    counters: Arc<Counters>,
}

pub struct TlsTransport {
    node: NodeId,
    incarnation: u64,
    address: SocketAddr,
    allowed: BTreeSet<NodeId>,
    trusted_cas: Vec<CertificateDer<'static>>,
    credentials: Arc<RwLock<Arc<TlsCredentials>>>,
    policy: Arc<RwLock<CertificatePolicy>>,
    peers: BTreeMap<NodeId, SocketAddr>,
    limits: TransportLimits,
    start: Instant,
    sequence: u64,
    outgoing: mpsc::Sender<Outbound>,
    incoming: mpsc::Receiver<Envelope>,
    errors: mpsc::Receiver<TransportError>,
    counters: Arc<Counters>,
    connections: Arc<Semaphore>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl fmt::Debug for TlsTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsTransport")
            .field("node", &self.node)
            .field("address", &self.address)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl TlsTransport {
    /// Must be called inside a Tokio runtime with I/O and time enabled.
    pub async fn bind(config: TlsTransportConfig) -> Result<Self, TransportError> {
        config.limits.validate()?;
        if !config.bind_addr.ip().is_loopback() && !config.allow_external_bind {
            return Err(TransportError::ExternalBindDenied);
        }
        if config.incarnation == 0
            || !config.allowed_nodes.contains(&config.node_id)
            || config.allowed_nodes.len() > 4096
            || config.peers.iter().any(|(id, addr)| {
                *id == config.node_id
                    || !config.allowed_nodes.contains(id)
                    || addr.port() == 0
                    || addr.ip().is_unspecified()
            })
        {
            return Err(TransportError::InvalidConfig);
        }
        config.revocations.validate()?;
        let policy = Arc::new(RwLock::new(CertificatePolicy {
            revocations: config.revocations.clone(),
            rotations: BTreeMap::new(),
        }));
        let credentials = Arc::new(RwLock::new(Arc::new(tls_configs(&config, policy.clone())?)));
        let listener = TcpListener::bind(config.bind_addr)
            .await
            .map_err(|_| TransportError::Io)?;
        let address = listener.local_addr().map_err(|_| TransportError::Io)?;
        if !address.ip().is_loopback() {
            eprintln!(
                "WARNING: daemon mTLS transport explicitly bound outside loopback; restrict network access"
            );
        }
        let (outgoing, out_rx) = mpsc::channel(config.limits.queue_capacity);
        let (in_tx, incoming) = mpsc::channel(config.limits.queue_capacity);
        let (err_tx, errors) = mpsc::channel(config.limits.queue_capacity);
        let counters = Arc::new(Counters::default());
        let connections = Arc::new(Semaphore::new(config.limits.max_connections));
        let start = Instant::now();
        let context = Arc::new(Context {
            node: config.node_id,
            incarnation: config.incarnation,
            start,
            limits: config.limits,
            credentials: credentials.clone(),
            policy: policy.clone(),
            incoming: in_tx,
            errors: err_tx,
            counters: counters.clone(),
        });
        let (stop, stopping) = oneshot::channel();
        let task = tokio::spawn(run(
            listener,
            out_rx,
            context,
            connections.clone(),
            stopping,
        ));
        Ok(Self {
            node: config.node_id,
            incarnation: config.incarnation,
            address,
            allowed: config.allowed_nodes,
            trusted_cas: config.trusted_cas,
            credentials,
            policy,
            peers: config.peers,
            limits: config.limits,
            start,
            sequence: 0,
            outgoing,
            incoming,
            errors,
            counters,
            connections,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    fn ensure_open(&self) -> Result<(), TransportError> {
        if self.stop.is_none() || self.task.as_ref().is_none_or(|t| t.is_finished()) {
            return Err(TransportError::Closed);
        }
        Ok(())
    }
    /// Atomically replaces the local deny-list. Removing an entry explicitly
    /// restores its eligibility; it does not remove a peer's rotation policy.
    /// The list is memory-only. Self-revocation disables local traffic too.
    pub fn set_revocations(&mut self, revocations: RevocationList) -> Result<(), TransportError> {
        self.ensure_open()?;
        revocations.validate()?;
        self.policy
            .write()
            .map_err(|_| TransportError::Tls)?
            .revocations = revocations;
        Ok(())
    }
    /// Pins a member to old/new leaf serials for a nonzero local overlap period,
    /// then permanently to the new serial until its next rotation. All peers
    /// must install this policy before that node calls `rotate_identity`.
    /// Certificates still require CA, identity, time, EKU and TLS validation.
    /// Issue a distinct serial for each replacement certificate; serial reuse
    /// cannot distinguish old and new keys. This API does not issue certificates.
    /// After the first rotation, `old` must match the previous `new`; a second
    /// rotation cannot truncate or extend an active overlap. No policy is
    /// distributed to other transports, and no permission or membership is added.
    pub fn rotate_peer_certificate(
        &mut self,
        node: NodeId,
        old: CertificateSerial,
        new: CertificateSerial,
        overlap: Duration,
    ) -> Result<(), TransportError> {
        self.ensure_open()?;
        if node == self.node || !self.allowed.contains(&node) {
            return Err(TransportError::UnauthorizedNode);
        }
        if old == new || overlap.is_zero() {
            return Err(TransportError::InvalidConfig);
        }
        let mut policy = self.policy.write().map_err(|_| TransportError::Tls)?;
        let now = Instant::now();
        let deadline = now
            .checked_add(overlap)
            .ok_or(TransportError::InvalidConfig)?;
        policy.revocations.check(node, &new)?;
        if let Some(previous) = policy.rotations.get(&node) {
            if now < previous.deadline {
                return Err(TransportError::RotationInProgress);
            }
            if old != previous.new {
                return Err(TransportError::InvalidConfig);
            }
        }
        policy
            .rotations
            .insert(node, PeerRotation { old, new, deadline });
        Ok(())
    }
    /// Validates and atomically installs a replacement leaf/key pair under the
    /// existing CAs and node ID. Failure leaves the previous pair untouched.
    /// Future connections use the new pair, including already queued sends
    /// whose connection has not started. Existing connections keep their pair
    /// for one bounded exchange, subject to current revocations and peer policy.
    /// Register the overlap on every peer BEFORE calling this method.
    pub fn rotate_identity(&mut self, identity: TlsIdentity) -> Result<(), TransportError> {
        self.ensure_open()?;
        let mut config = TlsTransportConfig::new(self.node, identity, self.trusted_cas.clone());
        config.allowed_nodes = self.allowed.clone();
        let replacement = Arc::new(tls_configs(&config, self.policy.clone())?);
        *self.credentials.write().map_err(|_| TransportError::Tls)? = replacement;
        Ok(())
    }
    /// Update a route only; this cannot add trusted members or bypass identity.
    pub fn set_peer(&mut self, node: NodeId, address: SocketAddr) -> Result<(), TransportError> {
        if node == self.node || !self.allowed.contains(&node) {
            return Err(TransportError::UnauthorizedNode);
        }
        if address.port() == 0 || address.ip().is_unspecified() {
            return Err(TransportError::InvalidConfig);
        }
        self.peers.insert(node, address);
        Ok(())
    }
    /// Bounded best-effort diagnostic queue. Counters still count dropped errors.
    pub fn take_error(&mut self) -> Option<TransportError> {
        self.errors.try_recv().ok()
    }
    pub fn stats(&self) -> TransportStats {
        let c = &self.counters;
        TransportStats {
            accepted: c.accepted.load(Ordering::Relaxed),
            received: c.received.load(Ordering::Relaxed),
            rejected: c.rejected.load(Ordering::Relaxed),
            delivered: c.delivered.load(Ordering::Relaxed),
            failed: c.failed.load(Ordering::Relaxed),
            active_connections: self.limits.max_connections - self.connections.available_permits(),
        }
    }
    fn enqueue(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: ControlMessage,
        result: Option<oneshot::Sender<Result<(), TransportError>>>,
    ) -> Result<(), TransportError> {
        self.ensure_open()?;
        if from != self.node {
            return Err(TransportError::IdentityMismatch);
        }
        if !self.allowed.contains(&to) || to == self.node {
            return Err(TransportError::UnauthorizedNode);
        }
        // The serial is checked after TLS authentication, never inferred from
        // the route. A revoked node can be rejected without opening a socket.
        if self
            .policy
            .read()
            .map_err(|_| TransportError::Tls)?
            .revocations
            .nodes
            .contains(&to)
        {
            return Err(TransportError::CertificateRevoked);
        }
        let address = *self
            .peers
            .get(&to)
            .ok_or(TransportError::UnauthorizedNode)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(TransportError::CounterExhausted)?;
        // Reserve before encoding so saturated queues cannot cause extra copies.
        let permit = self.outgoing.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => TransportError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
        })?;
        let frame = encode(
            &Envelope {
                sequence,
                from,
                to,
                from_incarnation: self.incarnation,
                to_incarnation: 1,
                sent_at: self.start.elapsed(),
                deliver_at: Duration::ZERO,
                message,
            },
            self.limits.max_frame_len,
        )?;
        self.sequence = sequence;
        permit.send(Outbound {
            target: to,
            address,
            frame,
            result,
        });
        Ok(())
    }
    /// Confirms remote in-memory queue admission, not processing or durable commit.
    /// An error after writing, or cancellation, may mean delivery is unknown.
    pub async fn send_confirmed(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: ControlMessage,
    ) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(from, to, message, Some(tx))?;
        rx.await.map_err(|_| TransportError::Closed)?
    }
    /// Stops accepting and cancels/joins all connection tasks; queued data is lost.
    pub async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task.take();
        self.incoming.close();
        while self.incoming.try_recv().is_ok() {}
    }
}
impl Drop for TlsTransport {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Transport for TlsTransport {
    fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: ControlMessage,
    ) -> Result<(), SimulationError> {
        self.enqueue(from, to, message, None).map_err(Into::into)
    }
    fn receive(&mut self) -> Result<Option<Envelope>, SimulationError> {
        if self.stop.is_none() || self.task.as_ref().is_none_or(|t| t.is_finished()) {
            return Err(TransportError::Closed.into());
        }
        match self.incoming.try_recv() {
            Ok(message) => Ok(Some(message)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(TransportError::Closed.into()),
        }
    }
}

async fn run(
    listener: TcpListener,
    mut outgoing: mpsc::Receiver<Outbound>,
    context: Arc<Context>,
    slots: Arc<Semaphore>,
    mut stopping: oneshot::Receiver<()>,
) {
    let mut tasks = JoinSet::new();
    loop {
        // Reap before spawning more so completed task records are also bounded.
        while tasks.try_join_next().is_some() {}
        tokio::select! {
            _ = &mut stopping => break,
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else {
                    let _ = context.errors.try_send(TransportError::Io);
                    // A failed listener is closed instead of spinning on OS errors.
                    break;
                };
                context.counters.accepted.fetch_add(1, Ordering::Relaxed);
                let permit = slots.clone().try_acquire_owned();
                match permit {
                    Ok(permit) => {
                        let ctx = context.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = inbound(socket, &ctx).await {
                                ctx.counters.rejected.fetch_add(1, Ordering::Relaxed);
                                let _ = ctx.errors.try_send(e);
                            }
                        });
                    }
                    Err(_) => {
                        context.counters.rejected.fetch_add(1, Ordering::Relaxed);
                        let _ = context.errors.try_send(TransportError::ConnectionLimit);
                    }
                }
            }
            request = outgoing.recv() => {
                let Some(request) = request else { break };
                let permit = slots.clone().try_acquire_owned();
                match permit {
                    Ok(permit) => {
                        let ctx = context.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let result = outbound(request.target, request.address, request.frame, &ctx).await;
                            report_outbound(&ctx, result, request.result);
                        });
                    }
                    Err(_) => report_outbound(&context, Err(TransportError::ConnectionLimit), request.result),
                }
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
fn report_outbound(
    ctx: &Context,
    result: Result<(), TransportError>,
    reply: Option<oneshot::Sender<Result<(), TransportError>>>,
) {
    if let Err(error) = result {
        ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
        let _ = ctx.errors.try_send(error);
    } else {
        ctx.counters.delivered.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(reply) = reply {
        let _ = reply.send(result);
    }
}

async fn inbound(socket: TcpStream, ctx: &Context) -> Result<(), TransportError> {
    let credentials = ctx
        .credentials
        .read()
        .map_err(|_| TransportError::Tls)?
        .clone();
    credentials.validate_local()?;
    ctx.policy
        .read()
        .map_err(|_| TransportError::Tls)?
        .check(&credentials.local)?;
    let mut tls = timeout(
        ctx.limits.handshake_timeout,
        credentials.acceptor.accept(socket),
    )
    .await
    .map_err(|_| TransportError::HandshakeTimeout)?
    .map_err(|_| TransportError::Tls)?;
    let session = tls.get_ref().1;
    if session.alpn_protocol() != Some(ALPN) {
        return Err(TransportError::Tls);
    }
    let peer_certificates = session
        .peer_certificates()
        .ok_or(TransportError::Tls)?
        .to_vec();
    let peer = credentials.validate_peer(&peer_certificates, false)?;
    check_connection_policy(ctx, &credentials, &peer)?;
    let mut greeting = Vec::with_capacity(22);
    greeting.extend_from_slice(b"BTLS");
    greeting.extend_from_slice(&VERSION.to_be_bytes());
    greeting.extend_from_slice(&ctx.node.0.to_be_bytes());
    greeting.extend_from_slice(&ctx.incarnation.to_be_bytes());
    write_bytes(&mut tls, &greeting, ctx, &credentials, &peer).await?;
    let bytes = read_frame(&mut tls, ctx.limits).await?;
    let mut message = decode(&bytes)?;
    if message.from != peer.node
        || message.to != ctx.node
        || message.to_incarnation != ctx.incarnation
    {
        return Err(TransportError::IdentityMismatch);
    }
    message.deliver_at = ctx.start.elapsed();
    // Recheck time using webpki: a handshake completed before expiry does not
    // authorize a frame arriving afterwards. Never hold a std lock across I/O.
    credentials.validate_local()?;
    credentials.validate_peer(&peer_certificates, false)?;
    {
        let policy = ctx.policy.read().map_err(|_| TransportError::Tls)?;
        policy.check(&credentials.local)?;
        policy.check(&peer)?;
        ctx.incoming.try_send(message).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => TransportError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
        })?;
    }
    ctx.counters.received.fetch_add(1, Ordering::Relaxed);
    write_bytes(&mut tls, &[1], ctx, &credentials, &peer).await?;
    Ok(())
}
async fn outbound(
    target: NodeId,
    address: SocketAddr,
    mut frame: Vec<u8>,
    ctx: &Context,
) -> Result<(), TransportError> {
    let credentials = ctx
        .credentials
        .read()
        .map_err(|_| TransportError::Tls)?
        .clone();
    credentials.validate_local()?;
    ctx.policy
        .read()
        .map_err(|_| TransportError::Tls)?
        .check(&credentials.local)?;
    let name =
        ServerName::try_from(node_name(target)).map_err(|_| TransportError::InvalidIdentity)?;
    let mut tls = timeout(ctx.limits.handshake_timeout, async {
        let socket = TcpStream::connect(address)
            .await
            .map_err(|_| TransportError::Io)?;
        TlsConnector::from(credentials.client.clone())
            .connect(name, socket)
            .await
            .map_err(|_| TransportError::Tls)
    })
    .await
    .map_err(|_| TransportError::HandshakeTimeout)??;
    if tls.get_ref().1.alpn_protocol() != Some(ALPN) {
        return Err(TransportError::Tls);
    }
    let peer_certificates = tls
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(TransportError::Tls)?
        .to_vec();
    let peer = credentials.validate_peer(&peer_certificates, true)?;
    check_connection_policy(ctx, &credentials, &peer)?;
    let mut greeting = [0; 22];
    timeout(ctx.limits.read_timeout, tls.read_exact(&mut greeting))
        .await
        .map_err(|_| TransportError::ReadTimeout)?
        .map_err(|_| TransportError::Io)?;
    let mut cursor = Reader(&greeting);
    if cursor.take(4)? != b"BTLS" || cursor.u16()? != VERSION || cursor.u64()? != target.0 {
        return Err(TransportError::IdentityMismatch);
    }
    let incarnation = cursor.u64()?;
    if incarnation == 0 {
        return Err(TransportError::InvalidFrame);
    }
    // Fixed wire offset after version, sequence, from, to and from_incarnation.
    frame[38..46].copy_from_slice(&incarnation.to_be_bytes());
    credentials.validate_local()?;
    credentials.validate_peer(&peer_certificates, true)?;
    check_connection_policy(ctx, &credentials, &peer)?;
    write_authenticated(ctx, &credentials, &peer, async {
        tls.write_all(&(frame.len() as u32).to_be_bytes()).await?;
        tls.write_all(&frame).await?;
        tls.flush().await
    })
    .await?;
    let mut ack = [0];
    timeout(ctx.limits.read_timeout, tls.read_exact(&mut ack))
        .await
        .map_err(|_| TransportError::ReadTimeout)?
        .map_err(|_| TransportError::Io)?;
    if ack != [1] {
        return Err(TransportError::InvalidFrame);
    }
    // A revocation during I/O is an unknown delivery outcome, not success.
    check_connection_policy(ctx, &credentials, &peer)?;
    Ok(())
}
fn check_connection_policy(
    ctx: &Context,
    credentials: &TlsCredentials,
    peer: &CertificateIdentity,
) -> Result<(), TransportError> {
    let policy = ctx.policy.read().map_err(|_| TransportError::Tls)?;
    policy.check(&credentials.local)?;
    policy.check(peer)
}
async fn write_bytes<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    ctx: &Context,
    credentials: &TlsCredentials,
    peer: &CertificateIdentity,
) -> Result<(), TransportError> {
    write_authenticated(ctx, credentials, peer, async {
        writer.write_all(data).await?;
        writer.flush().await
    })
    .await
}
async fn write_authenticated<F: Future<Output = io::Result<()>>>(
    ctx: &Context,
    credentials: &TlsCredentials,
    peer: &CertificateIdentity,
    operation: F,
) -> Result<(), TransportError> {
    let mut operation = std::pin::pin!(operation);
    timeout(
        ctx.limits.write_timeout,
        poll_fn(|cx| {
            let policy = match ctx.policy.read() {
                Ok(policy) => policy,
                Err(_) => return Poll::Ready(Err(TransportError::Tls)),
            };
            if let Err(error) = policy
                .check(&credentials.local)
                .and_then(|_| policy.check(peer))
            {
                return Poll::Ready(Err(error));
            }
            // Serialize each nonblocking I/O poll with policy updates. A guard is
            // NEVER retained when returning Pending, so no lock crosses an await.
            // After revocation returns, a pending write cannot hand more bytes to
            // rustls/the socket. Bytes previously buffered cannot be recalled.
            operation
                .as_mut()
                .poll(cx)
                .map(|result| result.map_err(|_| TransportError::Io))
        }),
    )
    .await
    .map_err(|_| TransportError::WriteTimeout)?
}
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: TransportLimits,
) -> Result<Vec<u8>, TransportError> {
    timeout(limits.read_timeout, async {
        let len = reader.read_u32().await.map_err(|_| TransportError::Io)? as usize;
        if len > limits.max_frame_len {
            return Err(TransportError::FrameTooLarge);
        }
        if len == 0 {
            return Err(TransportError::InvalidFrame);
        }
        let mut bytes = vec![0; len];
        reader
            .read_exact(&mut bytes)
            .await
            .map_err(|_| TransportError::Io)?;
        Ok(bytes)
    })
    .await
    .map_err(|_| TransportError::ReadTimeout)?
}

// No Debug implementation: configuration objects hold signing keys. Keep only
// public certificates for validation; raw input keys are dropped after building.
struct TlsCredentials {
    client: Arc<ClientConfig>,
    acceptor: TlsAcceptor,
    local: CertificateIdentity,
    local_chain: Vec<CertificateDer<'static>>,
    client_validation: Arc<dyn ClientCertVerifier>,
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}
impl TlsCredentials {
    fn validate_local(&self) -> Result<(), TransportError> {
        self.client_validation
            .verify_client_cert(
                &self.local_chain[0],
                &self.local_chain[1..],
                UnixTime::now(),
            )
            .map_err(|_| TransportError::Tls)?;
        Ok(())
    }

    fn validate_peer(
        &self,
        certificates: &[CertificateDer<'_>],
        server: bool,
    ) -> Result<CertificateIdentity, TransportError> {
        let (leaf, chain) = certificates.split_first().ok_or(TransportError::Tls)?;
        if server {
            verify_server_cert_signed_by_trust_anchor(
                &ParsedCertificate::try_from(leaf).map_err(|_| TransportError::Tls)?,
                &self.roots,
                chain,
                UnixTime::now(),
                self.provider.signature_verification_algorithms.all,
            )
            .map_err(|_| TransportError::Tls)?;
        } else {
            self.client_validation
                .verify_client_cert(leaf, chain, UnixTime::now())
                .map_err(|_| TransportError::Tls)?;
        }
        certificate_identity(leaf)
    }
}

fn tls_configs(
    config: &TlsTransportConfig,
    policy: Arc<RwLock<CertificatePolicy>>,
) -> Result<TlsCredentials, TransportError> {
    if config.identity.certificates.is_empty()
        || config.identity.certificates.len() > 8
        || config.trusted_cas.is_empty()
        || config.trusted_cas.len() > 64
        || config
            .identity
            .certificates
            .iter()
            .chain(&config.trusted_cas)
            .any(|c| c.len() > MAX_CERT)
    {
        return Err(TransportError::InvalidConfig);
    }
    let leaf = &config.identity.certificates[0];
    let local = certificate_identity(leaf)?;
    if local.node != config.node_id {
        return Err(TransportError::IdentityMismatch);
    }
    policy
        .read()
        .map_err(|_| TransportError::Tls)?
        .check(&local)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    for cert in &config.trusted_cas {
        roots.add(cert.clone()).map_err(|_| TransportError::Tls)?;
    }
    let roots = Arc::new(roots);
    let cn_fallback = roots
        .roots
        .iter()
        .all(|root| root.name_constraints.is_none());
    verified_identity(leaf, &config.identity.certificates[1..], cn_fallback)
        .map_err(|_| TransportError::Tls)?;
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|_| TransportError::Tls)?;
    // Catch broken local configuration before opening a socket. Both directions
    // use the same certificate and therefore require both client and server usage.
    client_verifier
        .verify_client_cert(leaf, &config.identity.certificates[1..], UnixTime::now())
        .map_err(|_| TransportError::Tls)?;
    verify_server_cert_signed_by_trust_anchor(
        &ParsedCertificate::try_from(leaf).map_err(|_| TransportError::Tls)?,
        &roots,
        &config.identity.certificates[1..],
        UnixTime::now(),
        provider.signature_verification_algorithms.all,
    )
    .map_err(|_| TransportError::Tls)?;
    let mut members = config.allowed_nodes.clone();
    members.remove(&config.node_id);
    let members = Arc::new(members);
    let verifier = Arc::new(MemberClientVerifier {
        inner: client_verifier.clone(),
        members: members.clone(),
        cn_fallback,
        policy: policy.clone(),
    });
    let mut server = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| TransportError::Tls)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            config.identity.certificates.clone(),
            config.identity.private_key.expose_secret().clone_key(),
        )
        .map_err(|_| TransportError::Tls)?;
    server.alpn_protocols = vec![ALPN.to_vec()];
    server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    server.send_tls13_tickets = 0;
    server.max_early_data_size = 0;
    let mut client = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| TransportError::Tls)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(MemberServerVerifier {
            roots: roots.clone(),
            provider: provider.clone(),
            members,
            cn_fallback,
            policy,
        }))
        .with_client_auth_cert(
            config.identity.certificates.clone(),
            config.identity.private_key.expose_secret().clone_key(),
        )
        .map_err(|_| TransportError::Tls)?;
    client.alpn_protocols = vec![ALPN.to_vec()];
    client.resumption = rustls::client::Resumption::disabled();
    client.enable_early_data = false;
    Ok(TlsCredentials {
        client: Arc::new(client),
        acceptor: TlsAcceptor::from(Arc::new(server)),
        local,
        local_chain: config.identity.certificates.clone(),
        client_validation: client_verifier,
        roots,
        provider,
    })
}

fn identity_tls_error() -> rustls::Error {
    rustls::Error::InvalidCertificate(rustls::CertificateError::ApplicationVerificationFailure)
}
fn verify_certificate_policy(
    policy: &RwLock<CertificatePolicy>,
    cert: &CertificateDer<'_>,
) -> Result<(), rustls::Error> {
    let identity = certificate_identity(cert).map_err(|_| identity_tls_error())?;
    policy
        .read()
        .map_err(|_| identity_tls_error())?
        .check(&identity)
        .map_err(|error| match error {
            TransportError::CertificateRevoked => {
                rustls::Error::InvalidCertificate(rustls::CertificateError::Revoked)
            }
            _ => identity_tls_error(),
        })
}
fn verified_identity(
    cert: &CertificateDer<'_>,
    chain: &[CertificateDer<'_>],
    cn_fallback: bool,
) -> Result<NodeId, rustls::Error> {
    let identity = certificate_identity(cert).map_err(|_| identity_tls_error())?;
    if identity.cn_only && (!cn_fallback || !chain.is_empty()) {
        return Err(identity_tls_error());
    }
    Ok(identity.node)
}
#[derive(Debug)]
struct MemberClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    members: Arc<BTreeSet<NodeId>>,
    cn_fallback: bool,
    policy: Arc<RwLock<CertificatePolicy>>,
}
impl ClientCertVerifier for MemberClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }
    fn verify_client_cert(
        &self,
        cert: &CertificateDer<'_>,
        chain: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self.inner.verify_client_cert(cert, chain, now)?;
        let node = verified_identity(cert, chain, self.cn_fallback)?;
        if !self.members.contains(&node) {
            return Err(identity_tls_error());
        }
        verify_certificate_policy(&self.policy, cert)?;
        Ok(verified)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
#[derive(Debug)]
struct MemberServerVerifier {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
    members: Arc<BTreeSet<NodeId>>,
    cn_fallback: bool,
    policy: Arc<RwLock<CertificatePolicy>>,
}
impl ServerCertVerifier for MemberServerVerifier {
    fn verify_server_cert(
        &self,
        cert: &CertificateDer<'_>,
        chain: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let parsed = ParsedCertificate::try_from(cert)?;
        verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            chain,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        let node = verified_identity(cert, chain, self.cn_fallback)?;
        let ServerName::DnsName(expected) = name else {
            return Err(identity_tls_error());
        };
        // Endpoint identity uses the strict private-CA profile, not IP/DNS routing
        // or wildcard hostname matching. This also handles the documented CN-only
        // profile without bypassing webpki chain/time/EKU or TLS signatures.
        if !self.members.contains(&node) || expected.as_ref() != node_name(node) {
            return Err(identity_tls_error());
        }
        verify_certificate_policy(&self.policy, cert)?;
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// A deliberately small, bounded DER identity reader. It only locates the serial,
// subject CN and SAN extension in a signed X.509 structure. It does NOT implement
// X.509 validation: the verifiers above always call webpki before trusting it.
struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn item(&mut self) -> Result<(u8, &'a [u8]), TransportError> {
        let bad = TransportError::InvalidIdentity;
        let (&tag, rest) = self.0.split_first().ok_or(bad)?;
        if tag & 0x1f == 0x1f {
            return Err(bad);
        }
        let (&first, mut rest) = rest.split_first().ok_or(bad)?;
        let len = if first < 128 {
            usize::from(first)
        } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > 4 || rest.len() < count || rest[0] == 0 {
                return Err(bad);
            }
            let mut len = 0usize;
            for byte in &rest[..count] {
                len = len
                    .checked_mul(256)
                    .and_then(|n| n.checked_add(usize::from(*byte)))
                    .ok_or(bad)?;
            }
            rest = &rest[count..];
            if len < 128 {
                return Err(bad);
            }
            len
        };
        if rest.len() < len {
            return Err(bad);
        }
        let (value, tail) = rest.split_at(len);
        self.0 = tail;
        Ok((tag, value))
    }
    fn expect(&mut self, tag: u8) -> Result<&'a [u8], TransportError> {
        let (actual, value) = self.item()?;
        if actual != tag {
            return Err(TransportError::InvalidIdentity);
        }
        Ok(value)
    }
    fn end(&self) -> Result<(), TransportError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(TransportError::InvalidIdentity)
        }
    }
}
fn parse_node_name(bytes: &[u8]) -> Result<NodeId, TransportError> {
    let name = std::str::from_utf8(bytes).map_err(|_| TransportError::InvalidIdentity)?;
    let number = name
        .strip_prefix("node-")
        .and_then(|s| s.strip_suffix(".cluster"))
        .ok_or(TransportError::InvalidIdentity)?;
    if number.is_empty()
        || number.len() > 20
        || !number.bytes().all(|b| b.is_ascii_digit())
        || (number.len() > 1 && number.starts_with('0'))
    {
        return Err(TransportError::InvalidIdentity);
    }
    Ok(NodeId(
        number
            .parse()
            .map_err(|_| TransportError::InvalidIdentity)?,
    ))
}
#[cfg(test)]
fn certificate_node(cert: &CertificateDer<'_>) -> Result<NodeId, TransportError> {
    Ok(certificate_identity(cert)?.node)
}
/// Extracts the leaf serial for configuring local revocation/rotation policy.
/// This is metadata extraction only, NOT certificate authentication. Transport
/// handshakes always verify the certificate and proof of key possession first.
pub fn certificate_serial(cert: &CertificateDer<'_>) -> Result<CertificateSerial, TransportError> {
    Ok(certificate_identity(cert)?.serial)
}
struct CertificateIdentity {
    node: NodeId,
    cn_only: bool,
    serial: CertificateSerial,
}
fn certificate_identity(cert: &CertificateDer<'_>) -> Result<CertificateIdentity, TransportError> {
    if cert.len() > MAX_CERT {
        return Err(TransportError::InvalidIdentity);
    }
    let mut root = Der(cert.as_ref());
    let mut certificate = Der(root.expect(0x30)?);
    root.end()?;
    let mut tbs = Der(certificate.expect(0x30)?);
    certificate.expect(0x30)?; // signatureAlgorithm
    certificate.expect(0x03)?; // signatureValue
    certificate.end()?;
    if tbs.0.first() == Some(&0xa0) {
        tbs.expect(0xa0)?;
    }
    let serial = tbs.expect(0x02)?;
    // DER INTEGER must be nonnegative and minimally encoded; a sign-padding
    // zero on a high-bit unsigned serial is normalized by from_bytes.
    if serial.is_empty()
        || serial[0] & 0x80 != 0
        || (serial.len() > 1 && serial[0] == 0 && serial[1] & 0x80 == 0)
    {
        return Err(TransportError::InvalidIdentity);
    }
    let serial = CertificateSerial::from_bytes(serial)?;
    tbs.expect(0x30)?; // signature
    tbs.expect(0x30)?; // issuer (never used as the node identity)
    tbs.expect(0x30)?; // validity (webpki checks it)
    let mut subject = Der(tbs.expect(0x30)?);
    let mut cn = None;
    while !subject.0.is_empty() {
        let mut set = Der(subject.expect(0x31)?);
        while !set.0.is_empty() {
            let mut attribute = Der(set.expect(0x30)?);
            let oid = attribute.expect(0x06)?;
            let (tag, value) = attribute.item()?;
            attribute.end()?;
            if oid == [0x55, 0x04, 0x03] {
                if cn.is_some() || !matches!(tag, 0x0c | 0x13) {
                    return Err(TransportError::InvalidIdentity);
                }
                cn = Some(parse_node_name(value)?);
            }
        }
    }
    tbs.expect(0x30)?; // subjectPublicKeyInfo
    let mut san = None;
    let mut extensions_seen = false;
    while !tbs.0.is_empty() {
        let (tag, value) = tbs.item()?;
        match tag {
            0x81 | 0x82 if !extensions_seen => {} // issuer/subject unique IDs
            0xa3 if !extensions_seen => {
                extensions_seen = true;
                let mut explicit = Der(value);
                let mut extensions = Der(explicit.expect(0x30)?);
                explicit.end()?;
                while !extensions.0.is_empty() {
                    let mut extension = Der(extensions.expect(0x30)?);
                    let oid = extension.expect(0x06)?;
                    if extension.0.first() == Some(&0x01) {
                        extension.expect(0x01)?;
                    }
                    let content = extension.expect(0x04)?;
                    extension.end()?;
                    if oid == [0x55, 0x1d, 0x11] {
                        if san.is_some() {
                            return Err(TransportError::InvalidIdentity);
                        }
                        let mut wrapped = Der(content);
                        let mut names = Der(wrapped.expect(0x30)?);
                        wrapped.end()?;
                        san = Some(parse_node_name(names.expect(0x82)?)?);
                        names.end()?; // ambiguous/multiple SANs fail closed
                    }
                }
            }
            _ => return Err(TransportError::InvalidIdentity),
        }
    }
    match (san, cn) {
        (Some(a), Some(b)) if a != b => Err(TransportError::InvalidIdentity),
        (Some(node), _) => Ok(CertificateIdentity {
            node,
            cn_only: false,
            serial,
        }),
        (None, Some(node)) => Ok(CertificateIdentity {
            node,
            cn_only: true,
            serial,
        }),
        _ => Err(TransportError::InvalidIdentity),
    }
}

// Versioned binary wire schema: fixed-width BE integers, tagged messages and a
// bounded record array. No generic untrusted nesting or claimed-length allocation.
// Test-only traffic also carries a bounded canonical deployment string and a
// bounded claim list. Neither contains credentials or peer clock timestamps.
struct Writer {
    bytes: Vec<u8>,
    max: usize,
}
impl io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Writer {
    fn put(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        io::Write::write_all(self, bytes).map_err(|_| TransportError::FrameTooLarge)
    }
    fn u8(&mut self, n: u8) -> Result<(), TransportError> {
        self.put(&[n])
    }
    fn u16(&mut self, n: u16) -> Result<(), TransportError> {
        self.put(&n.to_be_bytes())
    }
    fn u32(&mut self, n: u32) -> Result<(), TransportError> {
        self.put(&n.to_be_bytes())
    }
    fn u64(&mut self, n: u64) -> Result<(), TransportError> {
        self.put(&n.to_be_bytes())
    }
    fn duration(&mut self, d: Duration) -> Result<(), TransportError> {
        self.u64(d.as_secs())?;
        self.u32(d.subsec_nanos())
    }
    fn key(&mut self, key: OwnershipKey) -> Result<(), TransportError> {
        self.u64(key.bot_id.0)?;
        self.u32(key.shard_id)
    }
    fn record(&mut self, r: OwnershipRecord) -> Result<(), TransportError> {
        self.u64(r.owner.0)?;
        self.key(r.lease.key)?;
        self.u64(r.lease.epoch.0)?;
        self.duration(r.lease.expires_at)
    }
    fn proposal(&mut self, p: OwnershipProposal) -> Result<(), TransportError> {
        self.u64(p.id)?;
        self.u64(p.requester.0)?;
        self.u64(p.requester_incarnation)?;
        self.u64(p.target.0)?;
        self.u64(p.target_incarnation)?;
        self.key(p.key)?;
        self.u64(p.expected_epoch.0)?;
        self.u8(match p.kind {
            ProposalKind::Acquire => 0,
            ProposalKind::Renew => 1,
            ProposalKind::Failback => 2,
        })?;
        self.u64(p.attempt)?;
        self.duration(p.requested_at)?;
        self.duration(p.expires_at)?;
        self.duration(p.ack_deadline)
    }
    fn test_request(&mut self, request: TestOnlyRequest) -> Result<(), TransportError> {
        self.u64(request.id)?;
        self.key(request.key)?;
        self.u64(request.requester.0)?;
        self.u64(request.incarnation)?;
        self.u64(request.epoch.0)?;
        self.u8(u8::from(request.renew))
    }
    fn test_message(&mut self, message: &TestOnlyMessage) -> Result<(), TransportError> {
        if message.configuration.is_empty() || message.configuration.len() > 64 * 1024 {
            return Err(TransportError::InvalidFrame);
        }
        self.u32(message.configuration.len() as u32)?;
        self.put(message.configuration.as_bytes())?;
        match &message.payload {
            TestOnlyPayload::Heartbeat {
                sequence,
                ownership,
            } => {
                if ownership.len() > 4096 {
                    return Err(TransportError::FrameTooLarge);
                }
                self.u8(0)?;
                self.u64(*sequence)?;
                self.u32(ownership.len() as u32)?;
                for claim in ownership {
                    self.key(claim.key)?;
                    self.u64(claim.owner.0)?;
                    self.u64(claim.incarnation)?;
                    self.u64(claim.epoch.0)?;
                }
            }
            TestOnlyPayload::Request(request) => {
                self.u8(1)?;
                self.test_request(*request)?;
            }
            TestOnlyPayload::Vote {
                request,
                granted,
                highest_epoch,
            } => {
                self.u8(2)?;
                self.test_request(*request)?;
                self.u8(u8::from(*granted))?;
                self.u64(highest_epoch.0)?;
            }
        }
        Ok(())
    }
}
fn encode(envelope: &Envelope, max: usize) -> Result<Vec<u8>, TransportError> {
    let mut w = Writer {
        bytes: Vec::with_capacity(256.min(max)),
        max,
    };
    w.put(b"BCTL")?;
    w.u16(VERSION)?;
    w.u64(envelope.sequence)?;
    w.u64(envelope.from.0)?;
    w.u64(envelope.to.0)?;
    w.u64(envelope.from_incarnation)?;
    w.u64(envelope.to_incarnation)?;
    w.duration(envelope.sent_at)?;
    match &envelope.message {
        ControlMessage::Heartbeat(h) => {
            w.u8(0)?;
            w.u64(h.sequence)?;
            w.duration(h.sent_at)?;
            w.u32(u32::try_from(h.ownership.len()).map_err(|_| TransportError::FrameTooLarge)?)?;
            for record in &h.ownership {
                w.record(*record)?;
            }
        }
        ControlMessage::OwnershipProposal(p) => {
            w.u8(1)?;
            w.proposal(*p)?;
        }
        ControlMessage::OwnershipAck { proposal } => {
            w.u8(2)?;
            w.proposal(*proposal)?;
        }
        ControlMessage::OwnershipDecision {
            proposal,
            granted,
            current,
        } => {
            w.u8(3)?;
            w.proposal(*proposal)?;
            w.u8(u8::from(*granted))?;
            w.u8(u8::from(current.is_some()))?;
            if let Some(record) = current {
                w.record(*record)?;
            }
        }
        ControlMessage::TestOnly(message) => {
            w.u8(4)?;
            w.test_message(message)?;
        }
    }
    Ok(w.bytes)
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], TransportError> {
        if len > self.0.len() {
            return Err(TransportError::InvalidFrame);
        }
        let (bytes, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<u8, TransportError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, TransportError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| TransportError::InvalidFrame)?,
        ))
    }
    fn u32(&mut self) -> Result<u32, TransportError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| TransportError::InvalidFrame)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, TransportError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| TransportError::InvalidFrame)?,
        ))
    }
    fn boolean(&mut self) -> Result<bool, TransportError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(TransportError::InvalidFrame),
        }
    }
    fn duration(&mut self) -> Result<Duration, TransportError> {
        let secs = self.u64()?;
        let nanos = self.u32()?;
        if nanos >= 1_000_000_000 {
            return Err(TransportError::InvalidFrame);
        }
        Ok(Duration::new(secs, nanos))
    }
    fn key(&mut self) -> Result<OwnershipKey, TransportError> {
        Ok(OwnershipKey {
            bot_id: BotId(self.u64()?),
            shard_id: self.u32()?,
        })
    }
    fn record(&mut self) -> Result<OwnershipRecord, TransportError> {
        Ok(OwnershipRecord {
            owner: NodeId(self.u64()?),
            lease: Lease {
                key: self.key()?,
                epoch: OwnershipEpoch(self.u64()?),
                expires_at: self.duration()?,
            },
        })
    }
    fn proposal(&mut self) -> Result<OwnershipProposal, TransportError> {
        Ok(OwnershipProposal {
            id: self.u64()?,
            requester: NodeId(self.u64()?),
            requester_incarnation: self.u64()?,
            target: NodeId(self.u64()?),
            target_incarnation: self.u64()?,
            key: self.key()?,
            expected_epoch: OwnershipEpoch(self.u64()?),
            kind: match self.u8()? {
                0 => ProposalKind::Acquire,
                1 => ProposalKind::Renew,
                2 => ProposalKind::Failback,
                _ => return Err(TransportError::InvalidFrame),
            },
            attempt: self.u64()?,
            requested_at: self.duration()?,
            expires_at: self.duration()?,
            ack_deadline: self.duration()?,
        })
    }
    fn test_request(&mut self) -> Result<TestOnlyRequest, TransportError> {
        Ok(TestOnlyRequest {
            id: self.u64()?,
            key: self.key()?,
            requester: NodeId(self.u64()?),
            incarnation: self.u64()?,
            epoch: OwnershipEpoch(self.u64()?),
            renew: self.boolean()?,
        })
    }
    fn test_message(&mut self) -> Result<TestOnlyMessage, TransportError> {
        let length = self.u32()? as usize;
        if length == 0 || length > 64 * 1024 {
            return Err(TransportError::InvalidFrame);
        }
        let configuration = std::str::from_utf8(self.take(length)?)
            .map_err(|_| TransportError::InvalidFrame)?
            .to_owned();
        let payload = match self.u8()? {
            0 => {
                let sequence = self.u64()?;
                let count = self.u32()? as usize;
                if count > 4096 || count > self.0.len() / 36 {
                    return Err(TransportError::InvalidFrame);
                }
                let mut ownership = Vec::with_capacity(count);
                for _ in 0..count {
                    ownership.push(TestOnlyClaim {
                        key: self.key()?,
                        owner: NodeId(self.u64()?),
                        incarnation: self.u64()?,
                        epoch: OwnershipEpoch(self.u64()?),
                    });
                }
                TestOnlyPayload::Heartbeat {
                    sequence,
                    ownership,
                }
            }
            1 => TestOnlyPayload::Request(self.test_request()?),
            2 => TestOnlyPayload::Vote {
                request: self.test_request()?,
                granted: self.boolean()?,
                highest_epoch: OwnershipEpoch(self.u64()?),
            },
            _ => return Err(TransportError::InvalidFrame),
        };
        Ok(TestOnlyMessage {
            configuration,
            payload,
        })
    }
}
fn decode(bytes: &[u8]) -> Result<Envelope, TransportError> {
    let mut r = Reader(bytes);
    if r.take(4)? != b"BCTL" || r.u16()? != VERSION {
        return Err(TransportError::InvalidFrame);
    }
    let sequence = r.u64()?;
    let from = NodeId(r.u64()?);
    let to = NodeId(r.u64()?);
    let from_incarnation = r.u64()?;
    let to_incarnation = r.u64()?;
    let sent_at = r.duration()?;
    if sequence == 0 || from_incarnation == 0 || to_incarnation == 0 {
        return Err(TransportError::InvalidFrame);
    }
    let message = match r.u8()? {
        0 => {
            let sequence = r.u64()?;
            let sent_at = r.duration()?;
            let count = r.u32()? as usize;
            if count > r.0.len() / 40 {
                return Err(TransportError::InvalidFrame);
            }
            let mut ownership = Vec::with_capacity(count);
            for _ in 0..count {
                ownership.push(r.record()?);
            }
            ControlMessage::Heartbeat(Heartbeat {
                sequence,
                sent_at,
                ownership,
            })
        }
        1 => ControlMessage::OwnershipProposal(r.proposal()?),
        2 => ControlMessage::OwnershipAck {
            proposal: r.proposal()?,
        },
        3 => {
            let proposal = r.proposal()?;
            let granted = r.boolean()?;
            let current = if r.boolean()? {
                Some(r.record()?)
            } else {
                None
            };
            ControlMessage::OwnershipDecision {
                proposal,
                granted,
                current,
            }
        }
        4 => ControlMessage::TestOnly(r.test_message()?),
        _ => return Err(TransportError::InvalidFrame),
    };
    if !r.0.is_empty() {
        return Err(TransportError::InvalidFrame);
    }
    Ok(Envelope {
        sequence,
        from,
        to,
        from_incarnation,
        to_incarnation,
        sent_at,
        deliver_at: Duration::ZERO,
        message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Envelope {
        Envelope {
            sequence: 1,
            from: NodeId(1),
            to: NodeId(2),
            from_incarnation: 3,
            to_incarnation: 4,
            sent_at: Duration::new(7, 99),
            deliver_at: Duration::ZERO,
            message: ControlMessage::Heartbeat(Heartbeat {
                sequence: 9,
                sent_at: Duration::new(10, 321),
                ownership: vec![OwnershipRecord {
                    owner: NodeId(1),
                    lease: Lease {
                        key: OwnershipKey {
                            bot_id: BotId(u64::MAX),
                            shard_id: u32::MAX,
                        },
                        epoch: OwnershipEpoch(u64::MAX),
                        expires_at: Duration::MAX,
                    },
                }],
            }),
        }
    }

    #[test]
    fn wire_truncation_size_boundary_and_untrusted_counts() {
        let envelope = sample();
        let encoded = encode(&envelope, MAX_FRAME).unwrap();
        assert_eq!(encoded.len(), 123);
        assert_eq!(decode(&encoded).unwrap(), envelope);
        assert_eq!(encode(&envelope, encoded.len()).unwrap(), encoded);
        assert_eq!(
            encode(&envelope, encoded.len() - 1),
            Err(TransportError::FrameTooLarge)
        );
        for len in 0..encoded.len() {
            assert_eq!(
                decode(&encoded[..len]),
                Err(TransportError::InvalidFrame),
                "length {len}"
            );
        }
        let mut extra = encoded.clone();
        extra.push(0);
        assert_eq!(decode(&extra), Err(TransportError::InvalidFrame));
        let mut huge_count = encoded.clone();
        huge_count[79..83].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(decode(&huge_count), Err(TransportError::InvalidFrame));
        for offset in [54, 75, 119] {
            // envelope/heartbeat/lease nanoseconds
            let mut invalid_duration = encoded.clone();
            invalid_duration[offset..offset + 4].copy_from_slice(&1_000_000_000u32.to_be_bytes());
            assert_eq!(decode(&invalid_duration), Err(TransportError::InvalidFrame));
        }
    }

    #[test]
    fn test_only_wire_roundtrip_truncation_and_exact_frame_limits() {
        let request = TestOnlyRequest {
            id: 9,
            key: OwnershipKey {
                bot_id: BotId(101),
                shard_id: 2,
            },
            requester: NodeId(1),
            incarnation: 7,
            epoch: OwnershipEpoch(11),
            renew: true,
        };
        for payload in [
            TestOnlyPayload::Heartbeat {
                sequence: 1,
                ownership: vec![TestOnlyClaim {
                    key: request.key,
                    owner: request.requester,
                    incarnation: request.incarnation,
                    epoch: request.epoch,
                }],
            },
            TestOnlyPayload::Request(request),
            TestOnlyPayload::Vote {
                request,
                granted: true,
                highest_epoch: OwnershipEpoch(13),
            },
            TestOnlyPayload::Vote {
                request,
                granted: false,
                highest_epoch: OwnershipEpoch(15),
            },
        ] {
            let mut envelope = sample();
            envelope.message = ControlMessage::TestOnly(TestOnlyMessage {
                configuration: "explicit-test-only-deployment".into(),
                payload,
            });
            let bytes = encode(&envelope, MAX_FRAME).unwrap();
            assert_eq!(decode(&bytes).unwrap(), envelope);
            assert_eq!(encode(&envelope, bytes.len()).unwrap(), bytes);
            assert_eq!(
                encode(&envelope, bytes.len() - 1),
                Err(TransportError::FrameTooLarge)
            );
            for length in 0..bytes.len() {
                assert_eq!(decode(&bytes[..length]), Err(TransportError::InvalidFrame));
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert_eq!(decode(&trailing), Err(TransportError::InvalidFrame));
        }
    }

    #[test]
    fn test_only_wire_rejects_untrusted_configuration_and_claim_lengths() {
        let mut envelope = sample();
        envelope.message = ControlMessage::TestOnly(TestOnlyMessage {
            configuration: "x".into(),
            payload: TestOnlyPayload::Heartbeat {
                sequence: 1,
                ownership: Vec::new(),
            },
        });
        let bytes = encode(&envelope, MAX_FRAME).unwrap();
        // Fixed envelope = 58 bytes, tag at 58, then configuration u32 + bytes.
        for length in [0, 65_537, u32::MAX] {
            let mut invalid = bytes.clone();
            invalid[59..63].copy_from_slice(&length.to_be_bytes());
            assert_eq!(decode(&invalid), Err(TransportError::InvalidFrame));
        }
        let mut invalid_utf8 = bytes.clone();
        invalid_utf8[63] = 0xff;
        assert_eq!(decode(&invalid_utf8), Err(TransportError::InvalidFrame));
        let mut oversized_claims = bytes;
        oversized_claims[73..77].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(decode(&oversized_claims), Err(TransportError::InvalidFrame));
        let ControlMessage::TestOnly(message) = &mut envelope.message else {
            panic!()
        };
        message.configuration = "x".repeat(65_537);
        assert_eq!(
            encode(&envelope, MAX_FRAME),
            Err(TransportError::InvalidFrame)
        );
    }

    #[test]
    fn der_reader_rejects_truncation_noncanonical_lengths_and_ambiguous_san() {
        use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(NodeId(u64::MAX))]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, node_name(NodeId(u64::MAX)));
        let cert = params.clone().self_signed(&key).unwrap();
        assert_eq!(certificate_node(cert.der()), Ok(NodeId(u64::MAX)));
        for len in 0..cert.der().len() {
            assert_eq!(
                certificate_node(&CertificateDer::from(&cert.der()[..len])),
                Err(TransportError::InvalidIdentity)
            );
        }
        let mut trailing = cert.der().to_vec();
        trailing.push(0);
        assert_eq!(
            certificate_node(&CertificateDer::from(trailing)),
            Err(TransportError::InvalidIdentity)
        );
        for malformed in [
            vec![0x30, 0x80],
            vec![0x30, 0x81, 0],
            vec![0x30, 0x82, 0, 128],
            vec![0x30, 0x84, 255, 255, 255, 255],
            vec![0x30, 0x85, 1, 0, 0, 0, 0],
            vec![0x3f, 0],
        ] {
            assert!(Der(&malformed).item().is_err());
        }
        // Identical duplicate SANs are still ambiguous under this strict profile.
        params
            .subject_alt_names
            .push(params.subject_alt_names[0].clone());
        assert_eq!(
            certificate_node(params.self_signed(&key).unwrap().der()),
            Err(TransportError::InvalidIdentity)
        );
    }

    #[test]
    fn node_names_have_one_canonical_representation() {
        for id in [0, 1, 10, u64::MAX] {
            assert_eq!(
                parse_node_name(node_name(NodeId(id)).as_bytes()),
                Ok(NodeId(id))
            );
        }
        for invalid in [
            "",
            "node-.cluster",
            "node-00.cluster",
            "node-01.cluster",
            "node-+1.cluster",
            "node--1.cluster",
            "node-18446744073709551616.cluster",
            "node-1.cluster.",
            "node-1.CLUSTER",
            "*.cluster",
            "node-1.cluster\0",
            "node-１.cluster",
            "node-1.cluster.evil",
        ] {
            assert_eq!(
                parse_node_name(invalid.as_bytes()),
                Err(TransportError::InvalidIdentity)
            );
        }
    }
}
