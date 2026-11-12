//! Dedicated Raft RPC sockets using `TlsTransport`'s live TLS credentials and
//! certificate policy. One TLS 1.3 session carries one bounded request/reply.
//! No control-message ACK is interpreted as a Raft result and no retry occurs.
//!
//! This experimental listener binds only loopback. Its lifetime is independent
//! of the control listener: shut down the RPC handle before the `TlsTransport`
//! that owns its policy. Policy and identity updates on that owner also apply to
//! RPC connections. Each connection retains its original leaf pair for one
//! exchange; current revocation/rotation policy is checked throughout dispatch
//! and response writes. Already submitted Raft work may commit after cancellation
//! or revocation; a failed RPC does not roll that work back or grant a lease.

use super::{
    CertificateIdentity, CertificatePolicy, Counters, NodeId, TlsCredentials, TlsTransport,
    TransportError, TransportLimits, TransportStats, node_name, read_frame,
};
use crate::raft_quorum::transport::{
    AuthenticatedChannel, AuthenticatedFrame, RpcEndpoint, RpcFuture, TransportError as RpcError,
};
use rustls::pki_types::{CertificateDer, ServerName};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Distinct from the control protocol, including when the same certificates
/// are authorized for both listeners. There is no protocol fallback.
pub const ALPN: &[u8] = b"botd-raft/1";
const GREETING_LEN: usize = 14;
const VERSION: u16 = 1;

struct State {
    node: NodeId,
    allowed: BTreeSet<NodeId>,
    credentials: Arc<RwLock<Arc<TlsCredentials>>>,
    policy: Arc<RwLock<CertificatePolicy>>,
    limits: TransportLimits,
    routes: RwLock<BTreeMap<NodeId, SocketAddr>>,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    counters: Counters,
}

impl State {
    fn ensure_open(&self) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Acquire) {
            Err(TransportError::Closed)
        } else {
            Ok(())
        }
    }

    fn credentials(&self) -> Result<Arc<TlsCredentials>, TransportError> {
        let credentials = self
            .credentials
            .read()
            .map_err(|_| TransportError::Tls)?
            .clone();
        credentials.validate_local()?;
        self.policy
            .read()
            .map_err(|_| TransportError::Tls)?
            .check(&credentials.local)?;
        Ok(credentials)
    }

    fn validate(
        &self,
        credentials: &TlsCredentials,
        peer: &CertificateIdentity,
        chain: &[CertificateDer<'_>],
        server: bool,
    ) -> Result<(), TransportError> {
        credentials.validate_local()?;
        credentials.validate_peer(chain, server)?;
        let policy = self.policy.read().map_err(|_| TransportError::Tls)?;
        policy.check(&credentials.local)?;
        policy.check(peer)
    }

    /// Serialize every nonblocking poll with policy updates. No lock remains
    /// held at an await point, and revocation cannot authorize a later dispatch
    /// or socket write. Previously admitted/buffered work cannot be recalled.
    async fn authorized<T>(
        &self,
        credentials: &TlsCredentials,
        peer: &CertificateIdentity,
        operation: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let mut operation = std::pin::pin!(operation);
        poll_fn(|cx| {
            if let Err(error) = self.ensure_open() {
                return Poll::Ready(Err(error));
            }
            let policy = match self.policy.read() {
                Ok(policy) => policy,
                Err(_) => return Poll::Ready(Err(TransportError::Tls)),
            };
            if let Err(error) = policy
                .check(&credentials.local)
                .and_then(|_| policy.check(peer))
            {
                return Poll::Ready(Err(error));
            }
            operation.as_mut().poll(cx)
        })
        .await
    }

    async fn write(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        bytes: &[u8],
        framed: bool,
        credentials: &TlsCredentials,
        peer: &CertificateIdentity,
    ) -> Result<(), TransportError> {
        timeout(
            self.limits.write_timeout,
            self.authorized(credentials, peer, async {
                if framed {
                    writer
                        .write_u32(bytes.len() as u32)
                        .await
                        .map_err(|_| TransportError::Io)?;
                }
                writer
                    .write_all(bytes)
                    .await
                    .map_err(|_| TransportError::Io)?;
                writer.flush().await.map_err(|_| TransportError::Io)
            }),
        )
        .await
        .map_err(|_| TransportError::WriteTimeout)?
    }
}

struct Outbound {
    target: NodeId,
    address: SocketAddr,
    request: Vec<u8>,
    max_response_len: usize,
    reply: oneshot::Sender<Result<AuthenticatedFrame, RpcError>>,
}

/// Cloneable via `Arc`, with no private-key access and no ability to change
/// membership. Obtain this handle with `TlsRpcTransport::channel`.
pub struct TlsRpcChannel {
    state: Arc<State>,
    outgoing: mpsc::Sender<Outbound>,
}

impl AuthenticatedChannel for TlsRpcChannel {
    fn local_node(&self) -> u64 {
        self.state.node.0
    }

    fn provides_mutual_tls(&self) -> bool {
        true
    }

    fn exchange(
        &self,
        target: u64,
        request: Vec<u8>,
        max_response_len: usize,
    ) -> RpcFuture<'_, AuthenticatedFrame> {
        Box::pin(async move {
            self.state.ensure_open().map_err(rpc_error)?;
            let target = NodeId(target);
            if target == self.state.node || !self.state.allowed.contains(&target) {
                return Err(RpcError::IdentityMismatch);
            }
            if request.is_empty() || max_response_len == 0 {
                return Err(RpcError::InvalidFrame);
            }
            if request.len() > self.state.limits.max_frame_len {
                return Err(RpcError::FrameTooLarge);
            }
            if self
                .state
                .policy
                .read()
                .map_err(|_| RpcError::Unauthenticated)?
                .revocations
                .nodes
                .contains(&target)
            {
                return Err(RpcError::Unauthenticated);
            }
            let address = *self
                .state
                .routes
                .read()
                .map_err(|_| RpcError::Unavailable)?
                .get(&target)
                .ok_or(RpcError::Unavailable)?;
            let permit = self.outgoing.try_reserve().map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RpcError::Busy,
                mpsc::error::TrySendError::Closed(_) => RpcError::Unavailable,
            })?;
            let (reply, response) = oneshot::channel();
            permit.send(Outbound {
                target,
                address,
                request,
                max_response_len: max_response_len.min(self.state.limits.max_frame_len),
                reply,
            });
            // Include time spent queued, even when this raw channel is called
            // without the higher-level adapter's overall RPC deadline.
            let deadline = self.state.limits.handshake_timeout
                + self.state.limits.read_timeout * 2
                + self.state.limits.write_timeout;
            timeout(deadline, response)
                .await
                .map_err(|_| RpcError::Timeout)?
                .map_err(|_| RpcError::Unavailable)?
        })
    }
}

/// Owns the dedicated listener and all inbound/outbound connection tasks.
/// Attach the endpoint after constructing the Raft node with `channel()`.
/// Dropping or shutting down this handle invalidates all channel clones.
pub struct TlsRpcTransport {
    address: SocketAddr,
    channel: Arc<TlsRpcChannel>,
    endpoint: Arc<RwLock<Option<Arc<RpcEndpoint>>>>,
    errors: mpsc::Receiver<TransportError>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TlsTransport {
    /// Opens a separate loopback-only Raft listener with the current TLS trust
    /// profile, limits and live revocation/rotation state. RPC routes are set
    /// separately: the control protocol's peer addresses are never reused.
    /// Closing this control transport does not close an existing RPC handle.
    pub async fn bind_raft_rpc(
        &self,
        address: SocketAddr,
    ) -> Result<TlsRpcTransport, TransportError> {
        self.ensure_open()?;
        if !address.ip().is_loopback() {
            return Err(TransportError::ExternalBindDenied);
        }
        let listener = TcpListener::bind(address)
            .await
            .map_err(|_| TransportError::Io)?;
        let address = listener.local_addr().map_err(|_| TransportError::Io)?;
        let state = Arc::new(State {
            node: self.node,
            allowed: self.allowed.clone(),
            credentials: self.credentials.clone(),
            policy: self.policy.clone(),
            limits: self.limits,
            routes: RwLock::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(self.limits.max_connections)),
            counters: Counters::default(),
        });
        let (outgoing, requests) = mpsc::channel(self.limits.queue_capacity);
        let channel = Arc::new(TlsRpcChannel {
            state: state.clone(),
            outgoing,
        });
        let endpoint = Arc::new(RwLock::new(None));
        let (errors_tx, errors) = mpsc::channel(self.limits.queue_capacity);
        let (stop, stopping) = oneshot::channel();
        let task = tokio::spawn(run(
            listener,
            requests,
            state,
            endpoint.clone(),
            errors_tx,
            stopping,
        ));
        Ok(TlsRpcTransport {
            address,
            channel,
            endpoint,
            errors,
            stop: Some(stop),
            task: Some(task),
        })
    }
}

impl TlsRpcTransport {
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn channel(&self) -> Arc<TlsRpcChannel> {
        self.channel.clone()
    }

    pub fn set_peer(&mut self, node: NodeId, address: SocketAddr) -> Result<(), TransportError> {
        let state = &self.channel.state;
        state.ensure_open()?;
        if node == state.node || !state.allowed.contains(&node) {
            return Err(TransportError::UnauthorizedNode);
        }
        if address.port() == 0 || address.ip().is_unspecified() {
            return Err(TransportError::InvalidConfig);
        }
        state
            .routes
            .write()
            .map_err(|_| TransportError::Closed)?
            .insert(node, address);
        Ok(())
    }

    /// One endpoint per listener; replacement requires stopping the old listener
    /// first. This avoids retaining a running Raft instance after reconfiguration.
    pub fn set_endpoint(&mut self, endpoint: Arc<RpcEndpoint>) -> Result<(), TransportError> {
        self.channel.state.ensure_open()?;
        if endpoint.local_node() != self.channel.state.node.0 {
            return Err(TransportError::IdentityMismatch);
        }
        let mut current = self.endpoint.write().map_err(|_| TransportError::Closed)?;
        if current.is_some() {
            return Err(TransportError::InvalidConfig);
        }
        *current = Some(endpoint);
        Ok(())
    }

    /// RPC-specific meanings: `received` counts endpoint returns (including a
    /// serialized Raft error), and `delivered` counts authenticated reply bytes
    /// received by a client. Neither counter is evidence of Raft commit or a
    /// lease grant. Other fields retain their connection/failure meanings.
    pub fn stats(&self) -> TransportStats {
        let state = &self.channel.state;
        let c = &state.counters;
        TransportStats {
            accepted: c.accepted.load(Ordering::Relaxed),
            received: c.received.load(Ordering::Relaxed),
            rejected: c.rejected.load(Ordering::Relaxed),
            delivered: c.delivered.load(Ordering::Relaxed),
            failed: c.failed.load(Ordering::Relaxed),
            active_connections: state.limits.max_connections - state.slots.available_permits(),
        }
    }

    pub fn take_error(&mut self) -> Option<TransportError> {
        self.errors.try_recv().ok()
    }

    /// Stops accepts, cancels all connection tasks and joins them. Cancellation
    /// cannot undo Raft commands already submitted. This method may be reawaited
    /// after cancellation; channel clones are disabled as soon as it is polled.
    pub async fn shutdown(&mut self) {
        self.channel.state.closed.store(true, Ordering::Release);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task.take();
        if let Ok(mut endpoint) = self.endpoint.write() {
            endpoint.take();
        }
    }
}

impl Drop for TlsRpcTransport {
    fn drop(&mut self) {
        self.channel.state.closed.store(true, Ordering::Release);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct MarkClosed(Arc<State>);
impl Drop for MarkClosed {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
    }
}

async fn run(
    listener: TcpListener,
    mut outgoing: mpsc::Receiver<Outbound>,
    state: Arc<State>,
    endpoint: Arc<RwLock<Option<Arc<RpcEndpoint>>>>,
    errors: mpsc::Sender<TransportError>,
    mut stopping: oneshot::Receiver<()>,
) {
    let _closed = MarkClosed(state.clone());
    let mut tasks = JoinSet::new();
    loop {
        while tasks.try_join_next().is_some() {}
        tokio::select! {
            _ = &mut stopping => break,
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else {
                    let _ = errors.try_send(TransportError::Io);
                    break;
                };
                state.counters.accepted.fetch_add(1, Ordering::Relaxed);
                match state.slots.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let state = state.clone();
                        let endpoint = endpoint.clone();
                        let errors = errors.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            if let Err(error) = inbound(socket, &state, &endpoint).await {
                                state.counters.rejected.fetch_add(1, Ordering::Relaxed);
                                let _ = errors.try_send(error);
                            }
                        });
                    }
                    Err(_) => {
                        state.counters.rejected.fetch_add(1, Ordering::Relaxed);
                        let _ = errors.try_send(TransportError::ConnectionLimit);
                    }
                }
            }
            request = outgoing.recv() => {
                let Some(mut request) = request else { break };
                if request.reply.is_closed() { continue; }
                match state.slots.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let state = state.clone();
                        let errors = errors.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let result = tokio::select! {
                                biased;
                                _ = request.reply.closed() => return,
                                result = outbound(&state, request.target, request.address, &request.request, request.max_response_len) => result,
                            };
                            match &result {
                                Ok(_) => { state.counters.delivered.fetch_add(1, Ordering::Relaxed); }
                                Err(error) => {
                                    state.counters.failed.fetch_add(1, Ordering::Relaxed);
                                    let _ = errors.try_send(*error);
                                }
                            }
                            let _ = request.reply.send(result.map_err(rpc_error));
                        });
                    }
                    Err(_) => {
                        state.counters.failed.fetch_add(1, Ordering::Relaxed);
                        let _ = errors.try_send(TransportError::ConnectionLimit);
                        let _ = request.reply.send(Err(RpcError::Busy));
                    }
                }
            }
        }
    }
    state.closed.store(true, Ordering::Release);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

fn greeting(node: NodeId) -> [u8; GREETING_LEN] {
    let mut greeting = [0; GREETING_LEN];
    greeting[..4].copy_from_slice(b"BRPC");
    greeting[4..6].copy_from_slice(&VERSION.to_be_bytes());
    greeting[6..].copy_from_slice(&node.0.to_be_bytes());
    greeting
}

async fn inbound(
    socket: TcpStream,
    state: &State,
    endpoint: &RwLock<Option<Arc<RpcEndpoint>>>,
) -> Result<(), TransportError> {
    let credentials = state.credentials()?;
    let mut config = credentials.acceptor.config().as_ref().clone();
    config.alpn_protocols = vec![ALPN.to_vec()];
    let mut tls = timeout(
        state.limits.handshake_timeout,
        TlsAcceptor::from(Arc::new(config)).accept(socket),
    )
    .await
    .map_err(|_| TransportError::HandshakeTimeout)?
    .map_err(|_| TransportError::Tls)?;
    if tls.get_ref().1.alpn_protocol() != Some(ALPN) {
        return Err(TransportError::Tls);
    }
    let chain = tls
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(TransportError::Tls)?
        .to_vec();
    let peer = credentials.validate_peer(&chain, false)?;
    state.validate(&credentials, &peer, &chain, false)?;
    state
        .write(&mut tls, &greeting(state.node), false, &credentials, &peer)
        .await?;
    let request = read_frame(&mut tls, state.limits).await?;
    state.validate(&credentials, &peer, &chain, false)?;
    let endpoint = endpoint
        .read()
        .map_err(|_| TransportError::Closed)?
        .clone()
        .ok_or(TransportError::Closed)?;
    let response = state
        .authorized(&credentials, &peer, async {
            endpoint
                .handle(peer.node.0, &request)
                .await
                .map_err(tls_error)
        })
        .await?;
    state.counters.received.fetch_add(1, Ordering::Relaxed);
    if response.len() > state.limits.max_frame_len {
        return Err(TransportError::FrameTooLarge);
    }
    state.validate(&credentials, &peer, &chain, false)?;
    state
        .write(&mut tls, &response, true, &credentials, &peer)
        .await
}

async fn outbound(
    state: &State,
    target: NodeId,
    address: SocketAddr,
    request: &[u8],
    max_response_len: usize,
) -> Result<AuthenticatedFrame, TransportError> {
    let credentials = state.credentials()?;
    let mut config = credentials.client.as_ref().clone();
    config.alpn_protocols = vec![ALPN.to_vec()];
    let name =
        ServerName::try_from(node_name(target)).map_err(|_| TransportError::InvalidIdentity)?;
    let mut tls = timeout(state.limits.handshake_timeout, async {
        let socket = TcpStream::connect(address)
            .await
            .map_err(|_| TransportError::Io)?;
        TlsConnector::from(Arc::new(config))
            .connect(name, socket)
            .await
            .map_err(|_| TransportError::Tls)
    })
    .await
    .map_err(|_| TransportError::HandshakeTimeout)??;
    if tls.get_ref().1.alpn_protocol() != Some(ALPN) {
        return Err(TransportError::Tls);
    }
    let chain = tls
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(TransportError::Tls)?
        .to_vec();
    let peer = credentials.validate_peer(&chain, true)?;
    if peer.node != target {
        return Err(TransportError::IdentityMismatch);
    }
    state.validate(&credentials, &peer, &chain, true)?;
    let mut received = [0; GREETING_LEN];
    timeout(state.limits.read_timeout, tls.read_exact(&mut received))
        .await
        .map_err(|_| TransportError::ReadTimeout)?
        .map_err(|_| TransportError::Io)?;
    if received != greeting(target) {
        return Err(TransportError::IdentityMismatch);
    }
    state.validate(&credentials, &peer, &chain, true)?;
    state
        .write(&mut tls, request, true, &credentials, &peer)
        .await?;
    let payload = read_frame(
        &mut tls,
        TransportLimits {
            max_frame_len: max_response_len,
            ..state.limits
        },
    )
    .await?;
    state.validate(&credentials, &peer, &chain, true)?;
    state.ensure_open()?;
    Ok(AuthenticatedFrame {
        peer: peer.node.0,
        payload,
    })
}

fn rpc_error(error: TransportError) -> RpcError {
    match error {
        TransportError::InvalidConfig | TransportError::ExternalBindDenied => {
            RpcError::InvalidConfig
        }
        TransportError::IdentityMismatch => RpcError::IdentityMismatch,
        TransportError::Tls
        | TransportError::InvalidIdentity
        | TransportError::UnauthorizedNode
        | TransportError::CertificateRevoked
        | TransportError::CertificateRetired => RpcError::Unauthenticated,
        TransportError::FrameTooLarge => RpcError::FrameTooLarge,
        TransportError::InvalidFrame => RpcError::InvalidFrame,
        TransportError::QueueFull | TransportError::ConnectionLimit => RpcError::Busy,
        TransportError::HandshakeTimeout
        | TransportError::ReadTimeout
        | TransportError::WriteTimeout => RpcError::Timeout,
        _ => RpcError::Unavailable,
    }
}

fn tls_error(error: RpcError) -> TransportError {
    match error {
        RpcError::IdentityMismatch => TransportError::IdentityMismatch,
        RpcError::FrameTooLarge => TransportError::FrameTooLarge,
        RpcError::Busy => TransportError::ConnectionLimit,
        RpcError::Timeout => TransportError::ReadTimeout,
        RpcError::Unavailable => TransportError::Closed,
        _ => TransportError::InvalidFrame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        redact::Secret,
        tls_transport::{CertificateSerial, RevocationList, TlsIdentity, TlsTransportConfig},
    };
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use std::{io, pin::Pin, sync::atomic::AtomicUsize, task::Context};

    async fn fixture() -> (TlsTransport, TlsRpcTransport, CertificateIdentity) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(NodeId(1))]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, node_name(NodeId(1)));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
        let identity = TlsIdentity {
            certificates: vec![cert.der().clone()],
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        };
        let mut config = TlsTransportConfig::new(NodeId(1), identity, vec![ca.der().clone()]);
        config.allowed_nodes = BTreeSet::from([NodeId(1), NodeId(2)]);
        let tls = TlsTransport::bind(config).await.unwrap();
        let rpc = tls
            .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        // The operation under test begins after certificate verification; only
        // its policy identity is needed to exercise revocation between polls.
        let peer = CertificateIdentity {
            node: NodeId(2),
            cn_only: false,
            serial: CertificateSerial::from_bytes(&[2]).unwrap(),
        };
        (tls, rpc, peer)
    }

    #[tokio::test]
    async fn revocation_between_dispatch_polls_does_not_repoll_the_operation() {
        let (mut tls, mut rpc, peer) = fixture().await;
        let state = rpc.channel.state.clone();
        let credentials = state.credentials().unwrap();
        let polls = AtomicUsize::new(0);
        let operation = poll_fn(|cx| {
            if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        });
        let mut authorized = std::pin::pin!(state.authorized(&credentials, &peer, operation));
        poll_fn(|cx| {
            assert!(authorized.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tls.set_revocations(RevocationList {
            nodes: BTreeSet::from([NodeId(2)]),
            ..RevocationList::default()
        })
        .unwrap();
        assert_eq!(authorized.await, Err(TransportError::CertificateRevoked));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        rpc.shutdown().await;
        tls.shutdown().await;
    }

    struct BlockedWriter(AtomicUsize);
    impl AsyncWrite for BlockedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn revocation_stops_a_blocked_response_write_before_another_socket_poll() {
        let (mut tls, mut rpc, peer) = fixture().await;
        let state = rpc.channel.state.clone();
        let credentials = state.credentials().unwrap();
        let mut writer = BlockedWriter(AtomicUsize::new(0));
        {
            let mut writing =
                std::pin::pin!(state.write(&mut writer, b"reply", true, &credentials, &peer));
            poll_fn(|cx| {
                assert!(writing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            tls.set_revocations(RevocationList {
                nodes: BTreeSet::from([NodeId(2)]),
                ..RevocationList::default()
            })
            .unwrap();
            assert_eq!(writing.await, Err(TransportError::CertificateRevoked));
        }
        assert_eq!(writer.0.load(Ordering::SeqCst), 1);
        rpc.shutdown().await;
        tls.shutdown().await;
    }
}
