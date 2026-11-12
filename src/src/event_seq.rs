//! Pure, in-memory IPC delivery bookkeeping for one authenticated Bot/shard stream.
//! `local_sequence` is unrelated to Discord WS sequence numbers or other sessions.
//! The caller must bind this state and ACKs to the authenticated stream, preserve
//! it across Core switches, and serialize queue insertion with `Receiver::accept`.
//! An acceptance ACK means queue admission, never successful business processing.
//! No persistence, transport, authority, retries, or exactly-once effects are provided.

use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceRange {
    pub first: u64,
    pub last: u64,
}

/// Inclusive range of events admitted to the Core queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventAccepted(pub SequenceRange);

/// Separate application notification; cannot be passed to `Sender::ack`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventCompleted(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    Interaction,
    Required,
    BestEffort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// Caller retains the event and must retry or use a separate bounded spool.
    Backpressure,
    /// Event was not retained: caller must report loss/resynchronize affected state.
    GapDetected,
    TooLarge,
    SequenceExhausted,
}

/// Rejections return ownership without retaining payload or assigning a sequence.
/// In particular, pre-admission loss is explicit here, not a fabricated IPC gap.
#[derive(PartialEq, Eq)]
pub struct Rejected {
    pub reason: Rejection,
    pub payload: Vec<u8>,
}

/// Payload deliberately has no Debug implementation to avoid accidental logging.
#[derive(Clone, PartialEq, Eq)]
pub struct Event {
    pub local_sequence: u64,
    pub payload: Vec<u8>,
}

struct Slot {
    event: Event,
    accepted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    InvalidAck,
}

/// ACKed holes keep their slots until the prefix is ACKed, bounding ACK metadata
/// as well as payload memory. Interaction capacity is reserved, not reordered:
/// events always remain in IPC sequence order; this is not a deadline scheduler.
pub struct Sender {
    slots: VecDeque<Slot>,
    capacity: usize,
    reserved: usize,
    byte_limit: usize,
    bytes: usize,
    next: Option<u64>,
    retired: Option<u64>,
    first: u64,
}

impl Sender {
    pub fn new(
        capacity: usize,
        reserved: usize,
        byte_limit: usize,
        first: u64,
    ) -> Result<Self, Error> {
        if capacity == 0 || reserved > capacity || byte_limit == 0 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            slots: VecDeque::new(),
            capacity,
            reserved,
            byte_limit,
            bytes: 0,
            next: Some(first),
            retired: None,
            first,
        })
    }

    pub fn enqueue(&mut self, payload: Vec<u8>, priority: Priority) -> Result<u64, Rejected> {
        let reason = if self.next.is_none() {
            Some(Rejection::SequenceExhausted)
        } else if payload.len() > self.byte_limit {
            Some(Rejection::TooLarge)
        } else {
            let limit = if priority == Priority::Interaction {
                self.capacity
            } else {
                self.capacity - self.reserved
            };
            if self.slots.len() >= limit || payload.len() > self.byte_limit - self.bytes {
                Some(if priority == Priority::BestEffort {
                    Rejection::GapDetected
                } else {
                    Rejection::Backpressure
                })
            } else {
                None
            }
        };
        if let Some(reason) = reason {
            return Err(Rejected { reason, payload });
        }
        let sequence = self.next.expect("checked above");
        // Discard spare Vec capacity so a small payload cannot retain an
        // arbitrarily large allocation outside the configured byte budget.
        let payload = payload.into_boxed_slice().into_vec();
        self.next = sequence.checked_add(1);
        self.bytes += payload.len();
        self.slots.push_back(Slot {
            event: Event {
                local_sequence: sequence,
                payload,
            },
            accepted: false,
        });
        Ok(sequence)
    }

    /// Validate the whole range before mutation; duplicate/stale ACKs are idempotent.
    pub fn ack(&mut self, EventAccepted(range): EventAccepted) -> Result<(), Error> {
        if range.first > range.last
            || range.first < self.first
            || self.next.is_some_and(|next| range.last >= next)
        {
            return Err(Error::InvalidAck);
        }
        for slot in &mut self.slots {
            if (range.first..=range.last).contains(&slot.event.local_sequence) {
                slot.accepted = true;
            }
        }
        while self.slots.front().is_some_and(|slot| slot.accepted) {
            let slot = self.slots.pop_front().expect("front exists");
            self.bytes -= slot.event.payload.len();
            self.retired = Some(slot.event.local_sequence);
        }
        Ok(())
    }

    /// Re-deliver only unacknowledged events in order after a Core switch.
    /// Receiver must inherit the corresponding checkpoint/accepted ranges; a
    /// fresh receiver starting at zero cannot skip work accepted by the old Core.
    pub fn replay(&self) -> impl Iterator<Item = &Event> {
        self.slots
            .iter()
            .filter(|slot| !slot.accepted)
            .map(|slot| &slot.event)
    }

    pub fn unacked_ranges(&self) -> Vec<SequenceRange> {
        let mut ranges: Vec<SequenceRange> = Vec::new();
        for event in self.replay() {
            if let Some(last) = ranges.last_mut()
                && last.last.checked_add(1) == Some(event.local_sequence)
            {
                last.last = event.local_sequence;
            } else {
                ranges.push(SequenceRange {
                    first: event.local_sequence,
                    last: event.local_sequence,
                });
            }
        }
        ranges
    }

    pub fn retained_len(&self) -> usize {
        self.slots.len()
    }
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
    pub fn accepted_prefix(&self) -> Option<u64> {
        self.retired
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiveError {
    Duplicate,
    GapDetected(SequenceRange),
    QueueFull,
    SequenceExhausted,
}

/// Constant-size ordered admission cursor. Call only when the external bounded
/// queue can atomically admit the event. No out-of-order payloads are buffered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receiver {
    next: Option<u64>,
}

impl Receiver {
    /// `first` is the explicitly agreed IPC checkpoint, not a Discord sequence.
    pub fn new(first: u64) -> Self {
        Self { next: Some(first) }
    }

    pub fn accept(
        &mut self,
        sequence: u64,
        queue_has_room: bool,
    ) -> Result<EventAccepted, ReceiveError> {
        let Some(next) = self.next else {
            return Err(ReceiveError::SequenceExhausted);
        };
        if sequence < next {
            return Err(ReceiveError::Duplicate);
        }
        if sequence > next {
            return Err(ReceiveError::GapDetected(SequenceRange {
                first: next,
                last: sequence - 1,
            }));
        }
        if !queue_has_room {
            return Err(ReceiveError::QueueFull);
        }
        self.next = sequence.checked_add(1);
        Ok(EventAccepted(SequenceRange {
            first: sequence,
            last: sequence,
        }))
    }
}
