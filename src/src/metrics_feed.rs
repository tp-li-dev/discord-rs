//! Bounded, non-blocking observations. No labels, payloads or credentials cross
//! this boundary. A full channel drops the observation and increments a counter;
//! callers must not interpret these best-effort metrics as authority or health.
use crate::{
    event_seq, failover,
    metrics::{MetricLabels, ShardMetrics, escape_label_value},
    ownership::Clock,
    rest_budget::Admission,
    switch,
};
use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    time::Duration,
};

#[derive(Clone, Copy, Debug)]
pub enum Observation {
    IpcRoundTrip(Duration),
    Failover(failover::State),
    Switch {
        phase: switch::Phase,
        active: u64,
    },
    EventBuffer {
        retained: usize,
        bytes: usize,
    },
    /// Computed by the caller in the budget clock's domain; not a reservation.
    RestWait(Duration),
    IdentifyWait(Duration),
    Drain(Duration),
}

#[derive(Default, Debug)]
pub(crate) struct FeedSnapshot {
    failover: Option<u64>,
    phase: Option<u64>,
    generation: Option<u64>,
    retained: Option<u64>,
    bytes: Option<u64>,
    rest_ns: Option<u64>,
    identify_ns: Option<u64>,
    drain_ns: Option<u64>,
    dropped: u64,
}

impl FeedSnapshot {
    /// Read-only view for status output. Absent values were never observed.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "failover_state": self.failover, "switch_phase": self.phase,
            "switch_generation": self.generation, "event_retained": self.retained,
            "event_retained_bytes": self.bytes, "rest_wait_ns": self.rest_ns,
            "identify_wait_ns": self.identify_ns, "drain_ns": self.drain_ns,
            "feed_dropped": self.dropped,
        })
    }
}

#[derive(Clone)]
pub struct FeedSender {
    tx: SyncSender<Observation>,
    dropped: Arc<AtomicU64>,
}
pub struct FeedReceiver {
    rx: Receiver<Observation>,
    dropped: Arc<AtomicU64>,
}

/// Capacity must be nonzero; the receiver is owned by one collector.
pub fn channel(capacity: usize) -> Option<(FeedSender, FeedReceiver)> {
    if capacity == 0 {
        return None;
    }
    let (tx, rx) = mpsc::sync_channel(capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    Some((
        FeedSender {
            tx,
            dropped: dropped.clone(),
        },
        FeedReceiver { rx, dropped },
    ))
}
impl FeedSender {
    /// Never waits for the collector. Returns false on saturation or disconnect.
    // fetch_update is deprecated (renamed try_update) on new toolchains but try_update is unstable on the pinned 1.90.0; keep fetch_update and silence the warning.
    #[allow(deprecated)]
    pub fn observe(&self, observation: Observation) -> bool {
        match self.tx.try_send(observation) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                let _ = self
                    .dropped
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        Some(n.saturating_add(1))
                    });
                false
            }
        }
    }
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
impl FeedReceiver {
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    /// Process at most `limit` messages so the collector can yield fairly.
    /// A channel belongs to one shard handle; do not share it between shards.
    pub fn collect(&self, target: &ShardMetrics, limit: usize) -> usize {
        let mut processed = 0;
        while processed < limit {
            let Ok(event) = self.rx.try_recv() else { break };
            match event {
                Observation::IpcRoundTrip(duration) => {
                    target.ipc_latency_ns.observe_duration(duration)
                }
                other => {
                    let mut s = target.feed.write().unwrap_or_else(|e| e.into_inner());
                    match other {
                        Observation::Failover(state) => {
                            s.failover = Some(match state {
                                failover::State::Assigned => 0,
                                failover::State::Suspected => 1,
                                failover::State::FailoverPending => 2,
                                failover::State::StandbyReady => 3,
                                failover::State::TemporaryActive => 4,
                                failover::State::FailbackWaiting => 5,
                                failover::State::Degraded => 6,
                            })
                        }
                        Observation::Switch { phase, active } => {
                            s.phase = Some(match phase {
                                switch::Phase::Active => 0,
                                switch::Phase::Prepared { .. } => 1,
                                switch::Phase::Draining { .. } => 2,
                                switch::Phase::RollingBack { .. } => 3,
                            });
                            s.generation = Some(active);
                        }
                        Observation::EventBuffer { retained, bytes } => {
                            s.retained = Some(retained as u64);
                            s.bytes = Some(bytes as u64);
                        }
                        Observation::RestWait(d) => s.rest_ns = Some(ns(d)),
                        Observation::IdentifyWait(d) => s.identify_ns = Some(ns(d)),
                        Observation::Drain(d) => s.drain_ns = Some(ns(d)),
                        Observation::IpcRoundTrip(_) => unreachable!(),
                    }
                }
            }
            processed += 1;
        }
        target
            .feed
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .dropped = self.dropped();
        processed
    }
}
fn ns(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Adapters read existing public state only; they never drive state machines.
pub fn observe_failover<C: Clock>(feed: &FeedSender, source: &failover::Failover<C>) -> bool {
    feed.observe(Observation::Failover(source.state()))
}
pub fn observe_coordinated_failover<C: Clock>(
    feed: &FeedSender,
    source: &failover::CoordinatedFailover<C>,
) -> bool {
    feed.observe(Observation::Failover(source.state()))
}
pub fn observe_switch(feed: &FeedSender, source: &switch::Switch) -> bool {
    feed.observe(Observation::Switch {
        phase: source.phase(),
        active: source.active(),
    })
}
pub fn observe_event_buffer(feed: &FeedSender, source: &event_seq::Sender) -> bool {
    feed.observe(Observation::EventBuffer {
        retained: source.retained_len(),
        bytes: source.retained_bytes(),
    })
}
/// `now` must be sampled from the same clock as the admission result.
pub fn observe_rest_admission(feed: &FeedSender, admission: Admission, now: Duration) -> bool {
    match admission {
        Admission::Granted => feed.observe(Observation::RestWait(Duration::ZERO)),
        Admission::Wait { retry_at, .. } => {
            feed.observe(Observation::RestWait(retry_at.saturating_sub(now)))
        }
    }
}

pub(crate) fn render(
    out: &mut BTreeMap<&'static str, String>,
    labels: &MetricLabels,
    s: &FeedSnapshot,
) {
    let labels = format!(
        "bot_id=\"{}\",shard_id=\"{}\",generation_id=\"{}\"",
        escape_label_value(labels.bot_id()),
        labels.shard_id(),
        labels.generation_id()
    );
    writeln!(
        out.entry("met_feed_dropped_total").or_default(),
        "met_feed_dropped_total{{{labels}}} {}",
        s.dropped
    )
    .expect("String write");
    for (name, value) in [
        ("met_feed_failover_state", s.failover),
        ("met_feed_switch_phase", s.phase),
        ("met_feed_switch_generation", s.generation),
        ("met_feed_event_retained", s.retained),
        ("met_feed_event_retained_bytes", s.bytes),
    ] {
        if let Some(value) = value {
            writeln!(out.entry(name).or_default(), "{name}{{{labels}}} {value}")
                .expect("String write");
        }
    }
    for (name, value) in [
        ("met_feed_rest_wait_seconds", s.rest_ns),
        ("met_feed_identify_wait_seconds", s.identify_ns),
        ("met_feed_drain_seconds", s.drain_ns),
    ] {
        if let Some(value) = value {
            writeln!(
                out.entry(name).or_default(),
                "{name}{{{labels}}} {}.{:09}",
                value / 1_000_000_000,
                value % 1_000_000_000
            )
            .expect("String write");
        }
    }
}
