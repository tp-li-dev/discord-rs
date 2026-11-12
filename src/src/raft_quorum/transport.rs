//! OpenRaft RPC adapters. Election and replication decisions remain in OpenRaft.
//!
//! `MemoryTransport` is a fault-injection transport, never an authenticated
//! network. `AuthenticatedRpcAdapter` and `RpcEndpoint` define the bounded RPC
//! layer connected by `tls_transport::raft_rpc::TlsRpcChannel` over real TCP.
//!
//! `TlsTransport::bind_raft_rpc` opens a separate, versioned RPC listener sharing
//! the transport's live credentials and certificate policy. RPC returns the
//! actual OpenRaft result; the legacy control protocol's queue ACK is unrelated.
//! Cross-host safety, snapshot chunking and protocol upgrades remain unverified.

use super::{Command, Outcome, OwnershipData, Raft, TypeConfig};
use openraft::error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, Snapshot, SnapshotMeta, Vote};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io::{self, Write},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

/// Fixed diagnostics only: no certificate, key, payload or arbitrary error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportError {
    Unavailable,
    DuplicateNode,
    InvalidConfig,
    Unauthenticated,
    IdentityMismatch,
    ScopeMismatch,
    InvalidFrame,
    FrameTooLarge,
    Busy,
    Timeout,
    RemoteFailure,
    EntropyUnavailable,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "raft RPC: {self:?}")
    }
}
impl std::error::Error for TransportError {}

pub type RpcFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send + 'a>>;

/// Snapshot data and OpenRaft metadata are sent together.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub vote: Vote<u64>,
    pub meta: SnapshotMeta<u64, ()>,
    pub data: OwnershipData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RpcRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Snapshot(SnapshotRequest),
    /// An authenticated cluster member may submit only its own commands.
    /// The receiving node never forwards this request again.
    Forward {
        source: u64,
        command: Command,
    },
}

/// Only `NotLeader` permits following a new leader hint: OpenRaft rejected the
/// submission or already discarded its conflicting, uncommitted log entry.
/// Other failures can leave a committed write, so callers must not retry them
/// automatically or start a new lease deadline.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForwardResponse {
    Applied(Outcome),
    NotLeader { leader: Option<u64> },
    OutcomeUnknown,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    Append(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    Snapshot(SnapshotResponse<u64>),
    Forward(ForwardResponse),
}

impl RpcRequest {
    fn source(&self) -> Option<u64> {
        match self {
            Self::Append(r) => r.vote.leader_id.voted_for(),
            Self::Vote(r) => r.vote.leader_id.voted_for(),
            Self::Snapshot(r) => r.vote.leader_id.voted_for(),
            Self::Forward { source, command } => {
                let (owner, session) = command.identity();
                (*source == owner && owner != 0 && session != [0; 16]).then_some(*source)
            }
        }
    }

    fn kind(&self) -> u8 {
        match self {
            Self::Append(_) => 0,
            Self::Vote(_) => 1,
            Self::Snapshot(_) => 2,
            Self::Forward { .. } => 3,
        }
    }
}
impl RpcResponse {
    fn kind(&self) -> u8 {
        match self {
            Self::Append(_) => 0,
            Self::Vote(_) => 1,
            Self::Snapshot(_) => 2,
            Self::Forward(_) => 3,
        }
    }
}

/// Trusted control-plane extension point. Implementations must bind source and
/// destination identities and return the actual remote Raft result, never a
/// message-delivery ACK. Cancellation/timeout can leave delivery unknown.
pub trait RaftRpcTransport: Send + Sync + 'static {
    fn request(&self, source: u64, target: u64, request: RpcRequest) -> RpcFuture<'_, RpcResponse>;

    /// This is an implementation contract, not runtime proof of authentication.
    /// The built-in memory transport always returns false.
    fn is_authenticated(&self) -> bool {
        false
    }
}

pub struct NetworkFactory {
    source: u64,
    transport: Arc<dyn RaftRpcTransport>,
}

impl NetworkFactory {
    pub fn new(source: u64, transport: Arc<dyn RaftRpcTransport>) -> Self {
        Self { source, transport }
    }
}

pub struct Connection {
    source: u64,
    target: u64,
    transport: Arc<dyn RaftRpcTransport>,
}

impl Connection {
    async fn request(
        &self,
        request: RpcRequest,
        option: RPCOption,
    ) -> Result<RpcResponse, TransportError> {
        tokio::time::timeout(
            option.hard_ttl(),
            self.transport.request(self.source, self.target, request),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Connection;

    async fn new_client(&mut self, target: u64, _: &()) -> Connection {
        Connection {
            source: self.source,
            target,
            transport: self.transport.clone(),
        }
    }
}

fn unreachable(error: TransportError) -> Unreachable {
    Unreachable::new(&error)
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        match self.request(RpcRequest::Append(rpc), option).await {
            Ok(RpcResponse::Append(response)) => Ok(response),
            Ok(_) => Err(RPCError::Unreachable(unreachable(
                TransportError::InvalidFrame,
            ))),
            Err(e) => Err(RPCError::Unreachable(unreachable(e))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        match self.request(RpcRequest::Vote(rpc), option).await {
            Ok(RpcResponse::Vote(response)) => Ok(response),
            Ok(_) => Err(RPCError::Unreachable(unreachable(
                TransportError::InvalidFrame,
            ))),
            Err(e) => Err(RPCError::Unreachable(unreachable(e))),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        let request = RpcRequest::Snapshot(SnapshotRequest {
            vote,
            meta: snapshot.meta,
            data: *snapshot.snapshot,
        });
        tokio::select! {
            reason = cancel => Err(StreamingError::Closed(reason)),
            result = self.request(request, option) => match result {
                Ok(RpcResponse::Snapshot(response)) => Ok(response),
                Ok(_) => Err(StreamingError::Unreachable(unreachable(TransportError::InvalidFrame))),
                Err(e) => Err(StreamingError::Unreachable(unreachable(e))),
            },
        }
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(50)))
    }
}

async fn dispatch(raft: &Raft, request: RpcRequest) -> Result<RpcResponse, TransportError> {
    match request {
        RpcRequest::Append(r) => raft
            .append_entries(r)
            .await
            .map(RpcResponse::Append)
            .map_err(|_| TransportError::RemoteFailure),
        RpcRequest::Vote(r) => raft
            .vote(r)
            .await
            .map(RpcResponse::Vote)
            .map_err(|_| TransportError::RemoteFailure),
        RpcRequest::Snapshot(r) => raft
            .install_full_snapshot(
                r.vote,
                Snapshot {
                    meta: r.meta,
                    snapshot: Box::new(r.data),
                },
            )
            .await
            .map(RpcResponse::Snapshot)
            .map_err(|_| TransportError::RemoteFailure),
        RpcRequest::Forward { command, .. } => {
            let response = match raft.client_write(command).await {
                Ok(response) => ForwardResponse::Applied(response.data),
                Err(error) => match error.forward_to_leader() {
                    Some(hint) => ForwardResponse::NotLeader {
                        leader: hint.leader_id,
                    },
                    None => ForwardResponse::OutcomeUnknown,
                },
            };
            Ok(RpcResponse::Forward(response))
        }
    }
}

#[derive(Default)]
struct Links {
    nodes: BTreeMap<u64, Raft>,
    blocked: BTreeSet<(u64, u64)>,
    snapshots: usize,
}

/// In-process test transport. Directed failures model request loss; a successful
/// direct reply does not model an independently lossy reverse network path.
#[derive(Clone, Default)]
pub struct MemoryTransport(Arc<Mutex<Links>>);

impl MemoryTransport {
    pub fn register(&self, id: u64, raft: Raft) -> Result<(), TransportError> {
        if id == 0 || raft.metrics().borrow().id != id {
            return Err(TransportError::IdentityMismatch);
        }
        let mut links = self.0.lock().map_err(|_| TransportError::Unavailable)?;
        if links.nodes.contains_key(&id) {
            return Err(TransportError::DuplicateNode);
        }
        links.nodes.insert(id, raft);
        Ok(())
    }

    pub fn remove(&self, id: u64) {
        if let Ok(mut links) = self.0.lock() {
            links.nodes.remove(&id);
        }
    }

    pub fn block(&self, from: u64, to: u64) {
        if let Ok(mut links) = self.0.lock() {
            links.blocked.insert((from, to));
        }
    }

    pub fn heal(&self) {
        if let Ok(mut links) = self.0.lock() {
            links.blocked.clear();
        }
    }

    pub fn snapshot_count(&self) -> usize {
        self.0.lock().map_or(0, |links| links.snapshots)
    }

    fn endpoint(&self, from: u64, to: u64) -> Result<Raft, TransportError> {
        let links = self.0.lock().map_err(|_| TransportError::Unavailable)?;
        if from != to && links.nodes.contains_key(&from) && !links.blocked.contains(&(from, to)) {
            return links
                .nodes
                .get(&to)
                .cloned()
                .ok_or(TransportError::Unavailable);
        }
        Err(TransportError::Unavailable)
    }
}

impl RaftRpcTransport for MemoryTransport {
    fn request(&self, source: u64, target: u64, request: RpcRequest) -> RpcFuture<'_, RpcResponse> {
        Box::pin(async move {
            if request.source() != Some(source) {
                return Err(TransportError::IdentityMismatch);
            }
            let raft = self.endpoint(source, target)?;
            let result = dispatch(&raft, request).await?;
            if matches!(result, RpcResponse::Snapshot(_)) {
                let mut links = self.0.lock().map_err(|_| TransportError::Unavailable)?;
                links.snapshots = links.snapshots.saturating_add(1);
            }
            Ok(result)
        })
    }
}

const WIRE_VERSION: u16 = 1;
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Exact cluster namespace and fixed membership; no address or secret material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcScope {
    pub cluster_id: [u8; 16],
    pub members: BTreeSet<u64>,
}

impl RpcScope {
    fn validate(&self, node: u64) -> Result<(), TransportError> {
        if self.cluster_id == [0; 16]
            || self.members.is_empty()
            || self.members.len() > 4096
            || self.members.contains(&0)
            || !self.members.contains(&node)
        {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }

    fn peers(&self, source: u64, target: u64) -> bool {
        source != target && self.members.contains(&source) && self.members.contains(&target)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RpcLimits {
    pub max_frame_len: usize,
    pub max_in_flight: usize,
    pub timeout: Duration,
}

impl Default for RpcLimits {
    fn default() -> Self {
        Self {
            max_frame_len: 1024 * 1024,
            max_in_flight: 32,
            timeout: Duration::from_secs(3),
        }
    }
}

impl RpcLimits {
    fn validate(self) -> Result<(), TransportError> {
        if !(256..=MAX_FRAME).contains(&self.max_frame_len)
            || !(1..=1024).contains(&self.max_in_flight)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(300)
        {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }
}

/// The peer identity must come from verified TLS certificate metadata, never
/// from the JSON body or the requested socket address.
pub struct AuthenticatedFrame {
    pub peer: u64,
    pub payload: Vec<u8>,
}

/// Adapter contract for an opaque-frame extension to `tls_transport`.
///
/// Implementors must perform mutual TLS using its certificate/node identity
/// profile, reject untrusted peers, enforce bounded reads *before allocating the
/// received frame*, and return a complete RPC response rather than a queue ACK.
/// A successful TLS session is not application authorization; scope and sender
/// checks are also performed below. `tls_transport::raft_rpc::TlsRpcChannel`
/// supplies this contract using the existing TLS certificate policy.
pub trait AuthenticatedChannel: Send + Sync + 'static {
    fn local_node(&self) -> u64;

    /// Explicit implementor attestation of the mTLS contract above. Default false
    /// prevents an ordinary byte channel from becoming authenticated accidentally.
    /// This is not proof of production validation or permission to deploy.
    fn provides_mutual_tls(&self) -> bool {
        false
    }

    fn exchange(
        &self,
        target: u64,
        request: Vec<u8>,
        max_response_len: usize,
    ) -> RpcFuture<'_, AuthenticatedFrame>;
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    version: u16,
    scope: RpcScope,
    from: u64,
    to: u64,
    nonce: [u8; 16],
    request: RpcRequest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireResponse {
    version: u16,
    scope: RpcScope,
    from: u64,
    to: u64,
    nonce: [u8; 16],
    result: Result<RpcResponse, TransportError>,
}

struct BoundedWriter {
    bytes: Vec<u8>,
    max: usize,
    exceeded: bool,
}

impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.max.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("raft frame too large"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>, TransportError> {
    let mut writer = BoundedWriter {
        bytes: Vec::with_capacity(256.min(max)),
        max,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            TransportError::FrameTooLarge
        } else {
            TransportError::InvalidFrame
        });
    }
    Ok(writer.bytes)
}

fn decode<T: for<'a> Deserialize<'a>>(bytes: &[u8], max: usize) -> Result<T, TransportError> {
    if bytes.len() > max {
        return Err(TransportError::FrameTooLarge);
    }
    // serde_json's recursion limit stays enabled.
    serde_json::from_slice(bytes).map_err(|_| TransportError::InvalidFrame)
}

/// Bounded request/reply adapter for a separately supplied and verified mTLS
/// channel. No sockets are opened and no keys are consumed by this type.
pub struct AuthenticatedRpcAdapter {
    channel: Arc<dyn AuthenticatedChannel>,
    scope: RpcScope,
    limits: RpcLimits,
    slots: Semaphore,
}

impl AuthenticatedRpcAdapter {
    pub fn new(
        channel: Arc<dyn AuthenticatedChannel>,
        scope: RpcScope,
        limits: RpcLimits,
    ) -> Result<Self, TransportError> {
        limits.validate()?;
        scope.validate(channel.local_node())?;
        if !channel.provides_mutual_tls() {
            return Err(TransportError::Unauthenticated);
        }
        Ok(Self {
            channel,
            scope,
            limits,
            slots: Semaphore::new(limits.max_in_flight),
        })
    }
}

impl RaftRpcTransport for AuthenticatedRpcAdapter {
    fn is_authenticated(&self) -> bool {
        self.channel.provides_mutual_tls()
    }

    fn request(&self, source: u64, target: u64, request: RpcRequest) -> RpcFuture<'_, RpcResponse> {
        Box::pin(async move {
            if !self.channel.provides_mutual_tls() {
                return Err(TransportError::Unauthenticated);
            }
            if source != self.channel.local_node()
                || !self.scope.peers(source, target)
                || request.source() != Some(source)
            {
                return Err(TransportError::IdentityMismatch);
            }
            let _permit = self.slots.try_acquire().map_err(|_| TransportError::Busy)?;
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).map_err(|_| TransportError::EntropyUnavailable)?;
            let kind = request.kind();
            let body = encode(
                &WireRequest {
                    version: WIRE_VERSION,
                    scope: self.scope.clone(),
                    from: source,
                    to: target,
                    nonce,
                    request,
                },
                self.limits.max_frame_len,
            )?;
            let reply = tokio::time::timeout(
                self.limits.timeout,
                self.channel
                    .exchange(target, body, self.limits.max_frame_len),
            )
            .await
            .map_err(|_| TransportError::Timeout)??;
            if reply.peer != target {
                return Err(TransportError::IdentityMismatch);
            }
            let reply: WireResponse = decode(&reply.payload, self.limits.max_frame_len)?;
            if reply.version != WIRE_VERSION || reply.scope != self.scope {
                return Err(TransportError::ScopeMismatch);
            }
            if reply.from != target || reply.to != source || reply.nonce != nonce {
                return Err(TransportError::IdentityMismatch);
            }
            let result = reply.result?;
            if result.kind() != kind {
                return Err(TransportError::InvalidFrame);
            }
            if let RpcResponse::Forward(ForwardResponse::NotLeader {
                leader: Some(leader),
            }) = &result
                && !self.scope.members.contains(leader)
            {
                return Err(TransportError::ScopeMismatch);
            }
            Ok(result)
        })
    }
}

/// Server-side RPC dispatcher behind the mTLS opaque-frame API.
/// Authenticate and bound reads in the channel before calling `handle`.
pub struct RpcEndpoint {
    node: u64,
    scope: RpcScope,
    raft: Raft,
    limits: RpcLimits,
    slots: Semaphore,
}

impl RpcEndpoint {
    pub(crate) fn local_node(&self) -> u64 {
        self.node
    }

    pub fn new(
        node: u64,
        scope: RpcScope,
        raft: Raft,
        limits: RpcLimits,
    ) -> Result<Self, TransportError> {
        limits.validate()?;
        scope.validate(node)?;
        if raft.metrics().borrow().id != node {
            return Err(TransportError::IdentityMismatch);
        }
        Ok(Self {
            node,
            scope,
            raft,
            limits,
            slots: Semaphore::new(limits.max_in_flight),
        })
    }

    /// `authenticated_peer` is trusted certificate metadata supplied by the
    /// channel, not an untrusted request field. This method is not a listener or
    /// an authentication API on its own. A timeout may leave an applied request.
    pub async fn handle(
        &self,
        authenticated_peer: u64,
        bytes: &[u8],
    ) -> Result<Vec<u8>, TransportError> {
        let _permit = self.slots.try_acquire().map_err(|_| TransportError::Busy)?;
        let request: WireRequest = decode(bytes, self.limits.max_frame_len)?;
        validate_request(&request, authenticated_peer, self.node, &self.scope)?;
        let result =
            tokio::time::timeout(self.limits.timeout, dispatch(&self.raft, request.request))
                .await
                .unwrap_or(Err(TransportError::Timeout));
        encode(
            &WireResponse {
                version: WIRE_VERSION,
                scope: self.scope.clone(),
                from: self.node,
                to: authenticated_peer,
                nonce: request.nonce,
                result,
            },
            self.limits.max_frame_len,
        )
    }
}

fn validate_request(
    request: &WireRequest,
    authenticated_peer: u64,
    node: u64,
    scope: &RpcScope,
) -> Result<(), TransportError> {
    if request.version != WIRE_VERSION || request.scope != *scope {
        return Err(TransportError::ScopeMismatch);
    }
    if request.from != authenticated_peer
        || request.to != node
        || !scope.peers(authenticated_peer, node)
        || request.request.source() != Some(authenticated_peer)
    {
        return Err(TransportError::IdentityMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn scope() -> RpcScope {
        RpcScope {
            cluster_id: [7; 16],
            members: BTreeSet::from([1, 2, 3]),
        }
    }

    fn vote() -> RpcRequest {
        RpcRequest::Vote(VoteRequest::new(Vote::new(5, 1), None))
    }

    fn wire() -> WireRequest {
        WireRequest {
            version: WIRE_VERSION,
            scope: scope(),
            from: 1,
            to: 2,
            nonce: [4; 16],
            request: vote(),
        }
    }

    #[test]
    fn codec_enforces_boundaries_truncation_and_recursion() {
        let bytes = encode(&wire(), MAX_FRAME).unwrap();
        assert!(decode::<WireRequest>(&bytes, bytes.len()).is_ok());
        assert_eq!(
            encode(&wire(), bytes.len() - 1).err(),
            Some(TransportError::FrameTooLarge)
        );
        assert_eq!(
            decode::<WireRequest>(&bytes, bytes.len() - 1).err(),
            Some(TransportError::FrameTooLarge)
        );
        for len in 0..bytes.len() {
            assert_eq!(
                decode::<WireRequest>(&bytes[..len], MAX_FRAME).err(),
                Some(TransportError::InvalidFrame)
            );
        }
        let mut trailing = bytes;
        trailing.extend_from_slice(b" true");
        assert_eq!(
            decode::<WireRequest>(&trailing, MAX_FRAME).err(),
            Some(TransportError::InvalidFrame)
        );
        let nested = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        assert_eq!(
            decode::<serde_json::Value>(nested.as_bytes(), MAX_FRAME).err(),
            Some(TransportError::InvalidFrame)
        );
    }

    #[test]
    fn append_and_snapshot_codec_preserve_epoch_and_raft_metadata() {
        use super::super::{Command, Entry, ShardKey};
        use openraft::{CommittedLeaderId, EntryPayload, LogId, Membership, StoredMembership};

        let command = Command::Acquire {
            key: ShardKey {
                bot_id: u64::MAX,
                shard_id: u32::MAX,
            },
            expected_epoch: 0,
            owner: 1,
            session: [9; 16],
        };
        let log_id = LogId::new(CommittedLeaderId::new(5, 1), 12);
        let append = RpcRequest::Append(AppendEntriesRequest {
            vote: Vote::new_committed(5, 1),
            prev_log_id: Some(LogId::new(CommittedLeaderId::new(5, 1), 11)),
            entries: vec![Entry {
                log_id,
                payload: EntryPayload::Normal(command.clone()),
            }],
            leader_commit: Some(log_id),
        });
        let bytes = encode(&append, MAX_FRAME).unwrap();
        let decoded: RpcRequest = decode(&bytes, MAX_FRAME).unwrap();
        let RpcRequest::Append(decoded) = decoded else {
            panic!("wrong variant")
        };
        assert_eq!(decoded.vote, Vote::new_committed(5, 1));
        assert_eq!(decoded.entries[0].log_id, log_id);
        assert_eq!(
            decoded.entries[0].payload,
            EntryPayload::Normal(command.clone())
        );
        assert_eq!(decoded.leader_commit, Some(log_id));

        let mut data = OwnershipData::default();
        data.apply(&command);
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::new(
                Some(LogId::new(CommittedLeaderId::new(5, 1), 1)),
                Membership::new(vec![BTreeSet::from([1, 2, 3])], ()),
            ),
            snapshot_id: "wire-fixture-12".into(),
        };
        let snapshot = RpcRequest::Snapshot(SnapshotRequest {
            vote: Vote::new_committed(5, 1),
            meta: meta.clone(),
            data: data.clone(),
        });
        let bytes = encode(&snapshot, MAX_FRAME).unwrap();
        let RpcRequest::Snapshot(decoded) = decode::<RpcRequest>(&bytes, MAX_FRAME).unwrap() else {
            panic!("wrong variant")
        };
        assert_eq!(decoded.meta, meta);
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.vote, Vote::new_committed(5, 1));
    }

    #[test]
    fn inbound_scope_sender_destination_and_candidate_must_match() {
        assert_eq!(validate_request(&wire(), 1, 2, &scope()), Ok(()));
        for (peer, node) in [(3, 2), (1, 3), (1, 1), (9, 2)] {
            assert_eq!(
                validate_request(&wire(), peer, node, &scope()),
                Err(TransportError::IdentityMismatch)
            );
        }
        let mut request = wire();
        request.request = RpcRequest::Vote(VoteRequest::new(Vote::new(5, 3), None));
        assert_eq!(
            validate_request(&request, 1, 2, &scope()),
            Err(TransportError::IdentityMismatch)
        );
        let mut request = wire();
        request.scope.members.remove(&3);
        assert_eq!(
            validate_request(&request, 1, 2, &scope()),
            Err(TransportError::ScopeMismatch)
        );
        let mut request = wire();
        request.scope.cluster_id = [8; 16];
        assert_eq!(
            validate_request(&request, 1, 2, &scope()),
            Err(TransportError::ScopeMismatch)
        );
        let mut request = wire();
        request.version += 1;
        assert_eq!(
            validate_request(&request, 1, 2, &scope()),
            Err(TransportError::ScopeMismatch)
        );
    }

    // Simulates the channel contract solely to test the codec/adaptation layer.
    // This does not perform TLS or claim to validate certificate authentication.
    struct ChannelFixture {
        attests_tls: bool,
        fault: u8,
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
    }

    impl ChannelFixture {
        fn new(attests_tls: bool, fault: u8) -> Arc<Self> {
            Arc::new(Self {
                attests_tls,
                fault,
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
            })
        }
    }

    impl AuthenticatedChannel for ChannelFixture {
        fn local_node(&self) -> u64 {
            1
        }

        fn provides_mutual_tls(&self) -> bool {
            self.attests_tls
        }

        fn exchange(
            &self,
            target: u64,
            request: Vec<u8>,
            max_response_len: usize,
        ) -> RpcFuture<'_, AuthenticatedFrame> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                if self.fault == 10 {
                    return std::future::pending().await;
                }
                let request: WireRequest = decode(&request, MAX_FRAME)?;
                let response = if matches!(request.request, RpcRequest::Forward { .. }) {
                    RpcResponse::Forward(ForwardResponse::NotLeader { leader: Some(3) })
                } else {
                    RpcResponse::Vote(VoteResponse::new(Vote::new(5, 1), None, true))
                };
                let mut reply = WireResponse {
                    version: request.version,
                    scope: request.scope,
                    from: target,
                    to: request.from,
                    nonce: request.nonce,
                    result: Ok(response),
                };
                let mut peer = target;
                match self.fault {
                    1 => peer = 3,
                    2 => reply.from = 3,
                    3 => reply.to = 3,
                    4 => reply.nonce[0] ^= 1,
                    5 => reply.scope.cluster_id = [8; 16],
                    6 => reply.scope.members = BTreeSet::from([1, 2]),
                    7 => reply.version += 1,
                    8 => reply.result = Ok(RpcResponse::Append(AppendEntriesResponse::Success)),
                    9 => {
                        return Ok(AuthenticatedFrame {
                            peer,
                            payload: vec![b' '; max_response_len + 1],
                        });
                    }
                    11 => {
                        reply.result = Ok(RpcResponse::Forward(ForwardResponse::NotLeader {
                            leader: Some(9),
                        }));
                    }
                    _ => {}
                }
                Ok(AuthenticatedFrame {
                    peer,
                    payload: encode(&reply, max_response_len)?,
                })
            })
        }
    }

    #[test]
    fn ordinary_channels_and_invalid_limits_cannot_enable_mtls_adapter() {
        assert!(matches!(
            AuthenticatedRpcAdapter::new(
                ChannelFixture::new(false, 0),
                scope(),
                RpcLimits::default()
            ),
            Err(TransportError::Unauthenticated)
        ));
        for limits in [
            RpcLimits {
                max_frame_len: 0,
                ..RpcLimits::default()
            },
            RpcLimits {
                max_frame_len: MAX_FRAME + 1,
                ..RpcLimits::default()
            },
            RpcLimits {
                max_in_flight: 0,
                ..RpcLimits::default()
            },
            RpcLimits {
                timeout: Duration::ZERO,
                ..RpcLimits::default()
            },
        ] {
            assert!(matches!(
                AuthenticatedRpcAdapter::new(ChannelFixture::new(true, 0), scope(), limits),
                Err(TransportError::InvalidConfig)
            ));
        }
        assert!(!MemoryTransport::default().is_authenticated());
    }

    #[tokio::test]
    async fn simulated_authenticated_reply_is_bound_to_peer_scope_and_request() {
        let good = AuthenticatedRpcAdapter::new(
            ChannelFixture::new(true, 0),
            scope(),
            RpcLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            good.request(1, 2, vote()).await,
            Ok(RpcResponse::Vote(_))
        ));
        for (fault, expected) in [
            (1, TransportError::IdentityMismatch),
            (2, TransportError::IdentityMismatch),
            (3, TransportError::IdentityMismatch),
            (4, TransportError::IdentityMismatch),
            (5, TransportError::ScopeMismatch),
            (6, TransportError::ScopeMismatch),
            (7, TransportError::ScopeMismatch),
            (8, TransportError::InvalidFrame),
            (9, TransportError::FrameTooLarge),
        ] {
            let adapter = AuthenticatedRpcAdapter::new(
                ChannelFixture::new(true, fault),
                scope(),
                RpcLimits::default(),
            )
            .unwrap();
            assert_eq!(adapter.request(1, 2, vote()).await.err(), Some(expected));
        }
    }

    #[tokio::test]
    async fn invalid_sender_or_destination_never_reaches_channel() {
        let channel = ChannelFixture::new(true, 0);
        let adapter =
            AuthenticatedRpcAdapter::new(channel.clone(), scope(), RpcLimits::default()).unwrap();
        for (source, target) in [(3, 2), (1, 1), (1, 9)] {
            assert_eq!(
                adapter.request(source, target, vote()).await.err(),
                Some(TransportError::IdentityMismatch)
            );
        }
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0);
    }

    fn forward(owner: u64, session: [u8; 16]) -> RpcRequest {
        RpcRequest::Forward {
            source: 1,
            command: Command::Acquire {
                key: super::super::ShardKey {
                    bot_id: 42,
                    shard_id: 7,
                },
                expected_epoch: 9,
                owner,
                session,
            },
        }
    }

    #[test]
    fn forwarding_binds_owner_session_sender_scope_and_destination() {
        let mut request = wire();
        request.request = forward(1, [1; 16]);
        let bytes = encode(&request, MAX_FRAME).unwrap();
        let decoded: WireRequest = decode(&bytes, MAX_FRAME).unwrap();
        assert_eq!(validate_request(&decoded, 1, 2, &scope()), Ok(()));
        let RpcRequest::Forward { source, command } = decoded.request else {
            panic!("wrong forwarding variant")
        };
        assert_eq!(source, 1);
        assert_eq!(command.identity(), (1, [1; 16]));
        for (peer, node) in [(3, 2), (1, 3), (1, 1), (9, 2)] {
            assert_eq!(
                validate_request(&request, peer, node, &scope()),
                Err(TransportError::IdentityMismatch)
            );
        }
        for (owner, session) in [(3, [1; 16]), (0, [1; 16]), (1, [0; 16])] {
            request.request = forward(owner, session);
            assert_eq!(
                validate_request(&request, 1, 2, &scope()),
                Err(TransportError::IdentityMismatch)
            );
        }
        request.request = forward(1, [1; 16]);
        request.scope.cluster_id = [8; 16];
        assert_eq!(
            validate_request(&request, 1, 2, &scope()),
            Err(TransportError::ScopeMismatch)
        );
    }

    #[tokio::test]
    async fn forwarding_spoof_is_rejected_before_channel_and_hints_are_scoped() {
        let channel = ChannelFixture::new(true, 0);
        let adapter =
            AuthenticatedRpcAdapter::new(channel.clone(), scope(), RpcLimits::default()).unwrap();
        for request in [forward(3, [1; 16]), forward(1, [0; 16])] {
            assert_eq!(
                adapter.request(1, 2, request).await.err(),
                Some(TransportError::IdentityMismatch)
            );
        }
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            adapter.request(1, 2, forward(1, [1; 16])).await,
            Ok(RpcResponse::Forward(ForwardResponse::NotLeader {
                leader: Some(3)
            }))
        ));
        for (fault, expected) in [
            (1, TransportError::IdentityMismatch),
            (4, TransportError::IdentityMismatch),
            (5, TransportError::ScopeMismatch),
            (8, TransportError::InvalidFrame),
            (11, TransportError::ScopeMismatch),
        ] {
            let adapter = AuthenticatedRpcAdapter::new(
                ChannelFixture::new(true, fault),
                scope(),
                RpcLimits::default(),
            )
            .unwrap();
            assert_eq!(
                adapter.request(1, 2, forward(1, [1; 16])).await.err(),
                Some(expected)
            );
        }
    }

    #[test]
    fn forwarding_results_preserve_commit_rejection_and_unknown_distinction() {
        for response in [
            ForwardResponse::Applied(Outcome::Stale { current_epoch: 10 }),
            ForwardResponse::Applied(Outcome::Held),
            ForwardResponse::NotLeader { leader: Some(3) },
            ForwardResponse::NotLeader { leader: None },
            ForwardResponse::OutcomeUnknown,
        ] {
            let bytes = encode(&RpcResponse::Forward(response.clone()), MAX_FRAME).unwrap();
            let RpcResponse::Forward(decoded) = decode(&bytes, MAX_FRAME).unwrap() else {
                panic!("wrong forwarding response variant")
            };
            assert_eq!(decoded, response);
        }
    }

    #[tokio::test]
    async fn inflight_bound_timeout_and_cancellation_release_capacity() {
        let channel = ChannelFixture::new(true, 10);
        let adapter = Arc::new(
            AuthenticatedRpcAdapter::new(
                channel.clone(),
                scope(),
                RpcLimits {
                    max_in_flight: 1,
                    timeout: Duration::from_millis(80),
                    ..RpcLimits::default()
                },
            )
            .unwrap(),
        );
        let first = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.request(1, 2, vote()).await }
        });
        channel.entered.notified().await;
        assert_eq!(
            adapter.request(1, 2, vote()).await.err(),
            Some(TransportError::Busy)
        );
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(
            adapter.request(1, 2, vote()).await.err(),
            Some(TransportError::Timeout)
        );
        assert_eq!(adapter.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn openraft_adapter_enforces_rpc_deadline_and_response_variant() {
        let channel = ChannelFixture::new(true, 10);
        let adapter =
            Arc::new(AuthenticatedRpcAdapter::new(channel, scope(), RpcLimits::default()).unwrap());
        let mut factory = NetworkFactory::new(1, adapter);
        let mut connection = factory.new_client(2, &()).await;
        let result = connection
            .vote(
                VoteRequest::new(Vote::new(5, 1), None),
                RPCOption::new(Duration::from_millis(20)),
            )
            .await;
        assert!(matches!(result, Err(RPCError::Unreachable(_))));

        let mut factory = NetworkFactory::new(
            1,
            Arc::new(
                AuthenticatedRpcAdapter::new(
                    ChannelFixture::new(true, 8),
                    scope(),
                    RpcLimits::default(),
                )
                .unwrap(),
            ),
        );
        let result = factory
            .new_client(2, &())
            .await
            .vote(
                VoteRequest::new(Vote::new(5, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;
        assert!(matches!(result, Err(RPCError::Unreachable(_))));
    }

    #[tokio::test]
    async fn snapshot_cancellation_is_propagated_without_waiting_for_channel() {
        let adapter = Arc::new(
            AuthenticatedRpcAdapter::new(
                ChannelFixture::new(true, 10),
                scope(),
                RpcLimits::default(),
            )
            .unwrap(),
        );
        let mut factory = NetworkFactory::new(1, adapter);
        let result = factory
            .new_client(2, &())
            .await
            .full_snapshot(
                Vote::new_committed(5, 1),
                Snapshot {
                    meta: SnapshotMeta::default(),
                    snapshot: Box::default(),
                },
                std::future::ready(ReplicationClosed::new("test replication cancelled")),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;
        assert!(matches!(result, Err(StreamingError::Closed(_))));
    }
}
