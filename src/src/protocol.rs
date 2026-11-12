//! Draft Gateway/Core message types. These types do not implement a transport,
//! authenticate a peer, or grant authority to perform an operation.
use serde::{Deserialize, Serialize};

macro_rules! id {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub $inner);
    };
}
id!(BotId, u64);
id!(GenerationId, u64);
id!(OwnershipEpoch, u64);
id!(RequestId, [u8; 16]);
id!(OperationId, [u8; 16]);

/// Discord shard number and configured shard count. Construction and decoding
/// reject zero counts and out-of-range shard numbers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ShardFields", into = "ShardFields")]
pub struct Shard {
    id: u32,
    total: u32,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct ShardFields {
    id: u32,
    total: u32,
}

impl Shard {
    pub fn new(id: u32, total: u32) -> Result<Self, &'static str> {
        if total == 0 || id >= total {
            return Err("invalid shard id or total");
        }
        Ok(Self { id, total })
    }
    pub fn id(self) -> u32 {
        self.id
    }
    pub fn total(self) -> u32 {
        self.total
    }
}
impl TryFrom<ShardFields> for Shard {
    type Error = &'static str;
    fn try_from(value: ShardFields) -> Result<Self, Self::Error> {
        Self::new(value.id, value.total)
    }
}
impl From<Shard> for ShardFields {
    fn from(value: Shard) -> Self {
        Self {
            id: value.id,
            total: value.total,
        }
    }
}

/// A connection-scoped identity. Epoch alone is not proof of a valid lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub bot_id: BotId,
    pub shard: Shard,
    pub generation_id: GenerationId,
    pub ownership_epoch: OwnershipEpoch,
}

/// Metadata only. Peer authentication must be performed separately before
/// accepting messages; capabilities alone are not authentication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_min: u16,
    pub protocol_max: u16,
    pub identity: Identity,
    pub required_capabilities: Vec<String>,
    pub optional_capabilities: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub protocol_version: u16,
    pub identity: Identity,
    pub request_id: RequestId,
    /// Unix milliseconds, for observability; never use this as a lease clock.
    pub timestamp_ms: u64,
    pub message: Message,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum Message {
    Control(Control),
    Data(Data),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Control {
    Hello(Hello),
    Ready {
        generation: GenerationId,
    },
    PrepareSwitch {
        next_generation: GenerationId,
        next_epoch: OwnershipEpoch,
    },
    PauseNewProcessing,
    Activate {
        epoch: OwnershipEpoch,
    },
    StartDrain {
        deadline_ms: u64,
    },
    DrainComplete,
    HealthCheck,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Data {
    DiscordEvent {
        local_sequence: u64,
        event_key: Option<String>,
        payload: Vec<u8>,
    },
    /// Acknowledges queue admission, not handler completion.
    EventAccepted {
        local_sequence: u64,
    },
    ApiRequest {
        operation_id: OperationId,
        payload: Vec<u8>,
    },
    ApiResult {
        operation_id: OperationId,
        outcome: ApiOutcome,
    },
    StateSnapshotRequest,
    StateSnapshotChunk {
        index: u32,
        payload: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
pub enum ApiOutcome {
    Result(Vec<u8>),
    /// Sending may have succeeded even though its result was lost. Do not retry
    /// a non-idempotent operation automatically based on this outcome.
    UnknownOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_invariants_hold_after_deserialization() {
        assert!(Shard::new(0, 0).is_err());
        assert!(Shard::new(2, 2).is_err());
        assert!(serde_json::from_str::<Shard>(r#"{"id":2,"total":2}"#).is_err());
        assert_eq!(
            serde_json::from_str::<Shard>(r#"{"id":1,"total":2}"#).unwrap(),
            Shard::new(1, 2).unwrap()
        );
    }

    #[test]
    fn api_result_preserves_origin_and_unknown_outcome() {
        let envelope = Envelope {
            protocol_version: 1,
            identity: Identity {
                bot_id: BotId(42),
                shard: Shard::new(0, 2).unwrap(),
                generation_id: GenerationId(9),
                ownership_epoch: OwnershipEpoch(3),
            },
            request_id: RequestId([7; 16]),
            timestamp_ms: 123,
            message: Message::Data(Data::ApiResult {
                operation_id: OperationId([123; 16]),
                outcome: ApiOutcome::UnknownOutcome,
            }),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(
            serde_json::from_slice::<Envelope>(&encoded).unwrap(),
            envelope
        );
    }
}
