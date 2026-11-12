//! Bounded, process-local metrics and advisory status snapshots (§13.3 / §14.2).
//!
//! Register once and retain the returned handle: updates do not lock the registry,
//! allocate, perform I/O, or consult ownership. They confer no Active authority.
//! Counter/gauge/bucket updates use relaxed atomics. Snapshots are observations,
//! not transactions: histogram count is derived from the sampled buckets, while
//! its sum and other metrics may reflect a different instant. All integers
//! saturate rather than wrap. No reset/removal API is provided: cardinality caps
//! apply to the registry's entire lifetime, including generation churn.
//!
//! This module is not wired to Gateway/Core, botctl, or an HTTP listener. The local
//! manage server exposes its own registry, initially empty. Callers
//! must supply measurements and expose output through authenticated management.
//! Only public identifiers belong in labels. Credential checks reject known
//! shapes, not arbitrary unknown secrets; never pass tokens to this API.
//!
//! ```
//! use src::metrics::*;
//! use std::time::Duration;
//!
//! let registry = MetricsRegistry::default();
//! let handle = registry.register(MetricLabels::new("public-bot-id", 0, 7)?, RuntimeStatus {
//!     gateway: GenerationStatus { generation_id: 3, role: RuntimeRole::Active },
//!     core: GenerationStatus { generation_id: 7, role: RuntimeRole::Draining },
//!     ws_connected: true,
//! })?;
//! handle.events.inc();
//! handle.queue_depth.set(2);
//! handle.ipc_latency_ns.observe_duration(Duration::from_micros(80));
//! let status_json = serde_json::to_string(&handle.snapshot())?;
//! let prometheus = registry.render_prometheus();
//! # assert!(status_json.contains("public-bot-id"));
//! # assert!(prometheus.contains("met_events_total"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub const MAX_LABEL_BYTES: usize = 128;
pub const MAX_HISTOGRAM_BUCKETS: usize = 64;
pub const MAX_REGISTERED_SERIES: usize = 4096;
/// Inclusive upper bounds in nanoseconds; an implicit +Inf bucket follows.
pub const IPC_LATENCY_BUCKETS_NS: &[u64] = &[
    1_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    3_000_000_000,
    10_000_000_000,
];

/// Category-only errors: rejected label text is never retained or formatted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricsError {
    InvalidLabel,
    SuspectedSecret,
    InvalidBuckets,
    InvalidQuantile,
    InvalidLimits,
    SeriesLimit,
    BotLimit,
    ShardLimit,
    GenerationLimit,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLabel => "invalid metric label",
            Self::SuspectedSecret => "credential-like metric label rejected",
            Self::InvalidBuckets => "invalid histogram bucket boundaries",
            Self::InvalidQuantile => "invalid histogram quantile probability",
            Self::InvalidLimits => "invalid metric cardinality limits",
            Self::SeriesLimit => "metric series limit reached",
            Self::BotLimit => "metric bot limit reached",
            Self::ShardLimit => "metric shard limit reached",
            Self::GenerationLimit => "metric generation limit reached",
        })
    }
}
impl std::error::Error for MetricsError {}

/// Monotonic, saturating counter. Relaxed ordering does not synchronize payloads.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
    pub fn inc(&self) {
        self.add(1);
    }
    pub fn add(&self, amount: u64) {
        saturating_add(&self.0, amount);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Non-negative gauge for queue sizes, task counts, and durations. Arithmetic
/// saturates at zero/u64::MAX; unlike a Counter, set and decrement are supported.
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }
    pub fn set(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
    pub fn inc(&self) {
        self.add(1);
    }
    pub fn dec(&self) {
        self.sub(1);
    }
    pub fn add(&self, amount: u64) {
        saturating_add(&self.0, amount);
    }
    pub fn sub(&self, amount: u64) {
        atomic_update(&self.0, |value| value.saturating_sub(amount));
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn saturating_add(target: &AtomicU64, amount: u64) {
    atomic_update(target, |value| value.saturating_add(amount));
}

fn atomic_update(target: &AtomicU64, update: impl Fn(u64) -> u64) {
    let mut current = target.load(Ordering::Relaxed);
    loop {
        let next = update(current);
        if next == current {
            return;
        }
        match target.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(value) => current = value,
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Fixed, inclusive integer buckets, plus overflow. Each observation increments
/// one disjoint bucket and the sum, with no shared mutex or allocation. Memory is
/// O(number of buckets), independent of observation count.
#[derive(Debug)]
pub struct Histogram {
    upper_bounds: Box<[u64]>,
    buckets: Box<[AtomicU64]>,
    sum: AtomicU64,
}

impl Histogram {
    pub fn new(upper_bounds: &[u64]) -> Result<Self, MetricsError> {
        if upper_bounds.is_empty()
            || upper_bounds.len() > MAX_HISTOGRAM_BUCKETS
            || upper_bounds.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(MetricsError::InvalidBuckets);
        }
        Ok(Self {
            upper_bounds: upper_bounds.into(),
            buckets: (0..=upper_bounds.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            sum: AtomicU64::new(0),
        })
    }
    pub fn observe(&self, value: u64) {
        let index = self.upper_bounds.partition_point(|upper| *upper < value);
        saturating_add(&self.sum, value);
        saturating_add(&self.buckets[index], 1);
    }
    /// For histograms whose unit is nanoseconds, including ipc_latency_ns.
    pub fn observe_duration(&self, duration: Duration) {
        self.observe(duration_ns(duration));
    }
    pub fn snapshot(&self) -> HistogramSnapshot {
        let bucket_counts: Vec<_> = self
            .buckets
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .collect();
        let count = bucket_counts
            .iter()
            .fold(0_u64, |a, b| a.saturating_add(*b));
        HistogramSnapshot {
            upper_bounds: self.upper_bounds.to_vec(),
            bucket_counts,
            count,
            sum: self.sum.load(Ordering::Relaxed),
        }
    }
}

/// A nearest-rank estimate is the inclusive bucket upper bound. Overflow is
/// explicit instead of being silently clamped to the last finite boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuantileEstimate {
    Empty,
    UpperBound { value: u64 },
    Overflow { lower_bound: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HistogramSnapshot {
    upper_bounds: Vec<u64>,
    /// Disjoint buckets, with the overflow bucket last.
    bucket_counts: Vec<u64>,
    count: u64,
    /// Saturates at u64::MAX; not transactionally consistent with bucket reads.
    sum: u64,
}

impl HistogramSnapshot {
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn sum(&self) -> u64 {
        self.sum
    }
    pub fn upper_bounds(&self) -> &[u64] {
        &self.upper_bounds
    }
    pub fn bucket_counts(&self) -> &[u64] {
        &self.bucket_counts
    }
    /// Exact integer nearest-rank selection in parts per million. Supports p99.9
    /// without floating-point rank rounding. Invalid probabilities are errors.
    pub fn quantile_ppm(&self, ppm: u32) -> Result<QuantileEstimate, MetricsError> {
        if ppm > 1_000_000 {
            return Err(MetricsError::InvalidQuantile);
        }
        if self.count == 0 {
            return Ok(QuantileEstimate::Empty);
        }
        let rank = (u128::from(self.count) * u128::from(ppm))
            .div_ceil(1_000_000)
            .max(1);
        let mut cumulative = 0_u64;
        for (index, count) in self.bucket_counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            if u128::from(cumulative) >= rank {
                return Ok(match self.upper_bounds.get(index) {
                    Some(value) => QuantileEstimate::UpperBound { value: *value },
                    None => QuantileEstimate::Overflow {
                        lower_bound: *self.upper_bounds.last().expect("histogram has bounds"),
                    },
                });
            }
        }
        unreachable!("count is derived from these buckets")
    }
    pub fn percentiles(&self) -> LatencyPercentiles {
        LatencyPercentiles {
            count: self.count,
            p50: self.quantile_ppm(500_000).expect("valid probability"),
            p95: self.quantile_ppm(950_000).expect("valid probability"),
            p99: self.quantile_ppm(990_000).expect("valid probability"),
            p999: self.quantile_ppm(999_000).expect("valid probability"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub count: u64,
    pub p50: QuantileEstimate,
    pub p95: QuantileEstimate,
    pub p99: QuantileEstimate,
    pub p999: QuantileEstimate,
}

/// Fixed label names prevent accidental request/user/token cardinality. Bot IDs
/// are public configuration identifiers (including UUIDs), not Discord tokens.
/// Newlines and quotes are allowed and escaped at the text export boundary.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "LabelFields")]
pub struct MetricLabels {
    bot_id: String,
    shard_id: u32,
    generation_id: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LabelFields {
    bot_id: String,
    shard_id: u32,
    generation_id: u64,
}

impl MetricLabels {
    pub fn new(
        bot_id: impl Into<String>,
        shard_id: u32,
        generation_id: u64,
    ) -> Result<Self, MetricsError> {
        let bot_id = bot_id.into();
        if bot_id.trim().is_empty()
            || bot_id.len() > MAX_LABEL_BYTES
            || bot_id.chars().any(|c| c.is_control() && c != '\n')
        {
            return Err(MetricsError::InvalidLabel);
        }
        let lower = bot_id.to_ascii_lowercase();
        // Existing redaction covers auth schemes, credential assignments,
        // Discord token shapes, and webhook/interaction callback credentials.
        // Reject common credential carriers too, including env references.
        if crate::redact::redact(&bot_id) != bot_id
            || [
                "token",
                "secret",
                "authorization",
                "password",
                "credential",
                "custom_id",
                "://",
                "mfa.",
            ]
            .iter()
            .any(|marker| lower.contains(marker))
        {
            return Err(MetricsError::SuspectedSecret);
        }
        Ok(Self {
            bot_id,
            shard_id,
            generation_id,
        })
    }
    pub fn bot_id(&self) -> &str {
        &self.bot_id
    }
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }
}

impl TryFrom<LabelFields> for MetricLabels {
    type Error = MetricsError;
    fn try_from(value: LabelFields) -> Result<Self, Self::Error> {
        Self::new(value.bot_id, value.shard_id, value.generation_id)
    }
}

/// Prometheus text 0.0.4 label-value escaping. This is encoding, not credential
/// validation; registry labels must first pass MetricLabels::new.
pub fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRole {
    Active,
    Standby,
    Draining,
}

impl RuntimeRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Standby => "standby",
            Self::Draining => "draining",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenerationStatus {
    pub generation_id: u64,
    pub role: RuntimeRole,
}

/// Roles and generation IDs are copied under one per-handle lock. They describe
/// the caller's observations and do not prove lease/quorum/Discord health.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub gateway: GenerationStatus,
    pub core: GenerationStatus,
    pub ws_connected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub labels: MetricLabels,
    pub runtime: RuntimeStatus,
    pub events_total: u64,
    pub resumes_total: u64,
    pub heartbeat_rtt_ns: u64,
    pub queue_depth: u64,
    pub queue_oldest_age_ns: u64,
    pub processing_tasks: u64,
    pub ipc_latency_ns: LatencyPercentiles,
    pub http_429_total: u64,
    pub http_5xx_total: u64,
    pub interaction_initial_response_deadline_exceeded_total: u64,
    pub active_changes_total: u64,
    pub failover: Option<RecoverySnapshot>,
    pub failback: Option<RecoverySnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoverySnapshot {
    pub in_progress: bool,
    pub elapsed_ns: u64,
}

#[derive(Debug, Default)]
struct RecoveryTimer {
    started: Option<Instant>,
    completed: Option<Duration>,
}
impl RecoveryTimer {
    fn start(&mut self) {
        // Duplicate notifications must not reset an already-running operation.
        if self.started.is_none() {
            self.started = Some(Instant::now());
            self.completed = None;
        }
    }
    fn finish(&mut self) -> Option<Duration> {
        if let Some(started) = self.started.take() {
            self.completed = Some(started.elapsed());
        }
        self.completed
    }
    fn snapshot(&self) -> Option<RecoverySnapshot> {
        self.started
            .map(|start| RecoverySnapshot {
                in_progress: true,
                elapsed_ns: duration_ns(start.elapsed()),
            })
            .or_else(|| {
                self.completed.map(|elapsed| RecoverySnapshot {
                    in_progress: false,
                    elapsed_ns: duration_ns(elapsed),
                })
            })
    }
}

#[derive(Debug)]
struct StatusState {
    runtime: RuntimeStatus,
    failover: RecoveryTimer,
    failback: RecoveryTimer,
}

/// Handles remain valid while the registry is alive or after it is dropped.
/// Queue age/RTT are nanoseconds; zero means no measurement unless set by caller.
/// State/role changes do not implicitly increment active_changes: the caller
/// should count actual authority changes once, at the authoritative source.
#[derive(Debug)]
pub struct ShardMetrics {
    labels: MetricLabels,
    state: RwLock<StatusState>,
    pub events: Counter,
    pub resumes: Counter,
    pub heartbeat_rtt_ns: Gauge,
    pub queue_depth: Gauge,
    pub queue_oldest_age_ns: Gauge,
    pub processing_tasks: Gauge,
    pub ipc_latency_ns: Histogram,
    pub http_429: Counter,
    pub http_5xx: Counter,
    pub interaction_initial_response_deadline_exceeded: Counter,
    pub active_changes: Counter,
    pub(crate) feed: RwLock<crate::metrics_feed::FeedSnapshot>,
}

impl ShardMetrics {
    fn new(labels: MetricLabels, runtime: RuntimeStatus) -> Self {
        Self {
            labels,
            state: RwLock::new(StatusState {
                runtime,
                failover: RecoveryTimer::default(),
                failback: RecoveryTimer::default(),
            }),
            events: Counter::new(),
            resumes: Counter::new(),
            heartbeat_rtt_ns: Gauge::default(),
            queue_depth: Gauge::default(),
            queue_oldest_age_ns: Gauge::default(),
            processing_tasks: Gauge::default(),
            ipc_latency_ns: Histogram::new(IPC_LATENCY_BUCKETS_NS).expect("valid fixed buckets"),
            http_429: Counter::new(),
            http_5xx: Counter::new(),
            interaction_initial_response_deadline_exceeded: Counter::new(),
            active_changes: Counter::new(),
            feed: RwLock::new(crate::metrics_feed::FeedSnapshot::default()),
        }
    }
    pub fn labels(&self) -> &MetricLabels {
        &self.labels
    }
    pub fn set_runtime_status(&self, runtime: RuntimeStatus) {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .runtime = runtime;
    }
    pub fn start_failover(&self) {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .failover
            .start();
    }
    pub fn finish_failover(&self) -> Option<Duration> {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .failover
            .finish()
    }
    pub fn start_failback(&self) {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .failback
            .start();
    }
    pub fn finish_failback(&self) -> Option<Duration> {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .failback
            .finish()
    }
    pub fn snapshot(&self) -> StatusSnapshot {
        self.snapshot_with_histogram().0
    }
    fn snapshot_with_histogram(&self) -> (StatusSnapshot, HistogramSnapshot) {
        let (runtime, failover, failback) = {
            let state = self.state.read().unwrap_or_else(|e| e.into_inner());
            (
                state.runtime,
                state.failover.snapshot(),
                state.failback.snapshot(),
            )
        };
        let histogram = self.ipc_latency_ns.snapshot();
        let status = StatusSnapshot {
            labels: self.labels.clone(),
            runtime,
            events_total: self.events.get(),
            resumes_total: self.resumes.get(),
            heartbeat_rtt_ns: self.heartbeat_rtt_ns.get(),
            queue_depth: self.queue_depth.get(),
            queue_oldest_age_ns: self.queue_oldest_age_ns.get(),
            processing_tasks: self.processing_tasks.get(),
            ipc_latency_ns: histogram.percentiles(),
            http_429_total: self.http_429.get(),
            http_5xx_total: self.http_5xx.get(),
            interaction_initial_response_deadline_exceeded_total: self
                .interaction_initial_response_deadline_exceeded
                .get(),
            active_changes_total: self.active_changes.get(),
            failover,
            failback,
        };
        (status, histogram)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CardinalityLimits {
    /// Each registered label tuple has a fixed set of metric families.
    pub max_series: usize,
    pub max_bots: usize,
    pub max_shards_per_bot: usize,
    pub max_generations_per_shard: usize,
}
impl Default for CardinalityLimits {
    fn default() -> Self {
        Self {
            max_series: 1024,
            max_bots: 16,
            max_shards_per_bot: 64,
            max_generations_per_shard: 4,
        }
    }
}

/// Registration is serialized; exporters clone handles under the read lock and
/// release it before formatting. Caps are lifetime limits, never LRU eviction.
#[derive(Debug)]
pub struct MetricsRegistry {
    limits: CardinalityLimits,
    series: RwLock<BTreeMap<MetricLabels, Arc<ShardMetrics>>>,
}
impl MetricsRegistry {
    pub fn new(limits: CardinalityLimits) -> Result<Self, MetricsError> {
        if [
            limits.max_series,
            limits.max_bots,
            limits.max_shards_per_bot,
            limits.max_generations_per_shard,
        ]
        .iter()
        .any(|value| *value == 0 || *value > MAX_REGISTERED_SERIES)
        {
            return Err(MetricsError::InvalidLimits);
        }
        Ok(Self {
            limits,
            series: RwLock::new(BTreeMap::new()),
        })
    }
    /// Re-registration returns the existing handle without changing its status.
    /// This succeeds even when new registrations would exceed a limit.
    pub fn register(
        &self,
        labels: MetricLabels,
        runtime: RuntimeStatus,
    ) -> Result<Arc<ShardMetrics>, MetricsError> {
        let mut series = self.series.write().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = series.get(&labels) {
            return Ok(Arc::clone(existing));
        }
        if series.len() >= self.limits.max_series {
            return Err(MetricsError::SeriesLimit);
        }
        let bots: BTreeSet<_> = series.keys().map(|label| label.bot_id.as_str()).collect();
        if !bots.contains(labels.bot_id.as_str()) && bots.len() >= self.limits.max_bots {
            return Err(MetricsError::BotLimit);
        }
        let shards: BTreeSet<_> = series
            .keys()
            .filter(|label| label.bot_id == labels.bot_id)
            .map(|label| label.shard_id)
            .collect();
        if !shards.contains(&labels.shard_id) && shards.len() >= self.limits.max_shards_per_bot {
            return Err(MetricsError::ShardLimit);
        }
        let generations = series
            .keys()
            .filter(|label| label.bot_id == labels.bot_id && label.shard_id == labels.shard_id)
            .count();
        if generations >= self.limits.max_generations_per_shard {
            return Err(MetricsError::GenerationLimit);
        }
        let handle = Arc::new(ShardMetrics::new(labels.clone(), runtime));
        series.insert(labels, Arc::clone(&handle));
        Ok(handle)
    }
    pub fn len(&self) -> usize {
        self.series.read().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn handles(&self) -> Vec<Arc<ShardMetrics>> {
        self.series
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    /// Stable label order; each individual status is advisory, not a global cut.
    pub fn snapshots(&self) -> Vec<StatusSnapshot> {
        self.handles()
            .iter()
            .map(|handle| handle.snapshot())
            .collect()
    }
    /// Prometheus text format 0.0.4, fixed metric names, seconds for durations.
    /// No HTTP server or remote management endpoint is created. Prometheus
    /// stores sample values as float64, so integer values above 2^53 may lose
    /// precision after ingestion; status serialization retains the u64 fields.
    pub fn render_prometheus(&self) -> String {
        let mut families = BTreeMap::new();
        for handle in self.handles() {
            let (status, histogram) = handle.snapshot_with_histogram();
            render_series(&mut families, &status, &histogram);
            crate::metrics_feed::render(
                &mut families,
                &status.labels,
                &handle.feed.read().unwrap_or_else(|e| e.into_inner()),
            );
        }
        let mut out = String::new();
        for (name, kind, help) in FAMILIES {
            writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}").expect("String write");
            if let Some(samples) = families.get(name) {
                out.push_str(samples);
            }
        }
        out
    }
}
impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new(CardinalityLimits::default()).expect("valid default limits")
    }
}

const FAMILIES: &[(&str, &str, &str)] = &[
    (
        "met_feed_dropped_total",
        "counter",
        "Observations dropped by the feed channel.",
    ),
    (
        "met_feed_failover_state",
        "gauge",
        "Observed failover state code; advisory only.",
    ),
    (
        "met_feed_switch_phase",
        "gauge",
        "Observed Core switch phase code.",
    ),
    (
        "met_feed_switch_generation",
        "gauge",
        "Observed active Core generation.",
    ),
    ("met_feed_event_retained", "gauge", "Retained event count."),
    (
        "met_feed_event_retained_bytes",
        "gauge",
        "Retained event payload bytes.",
    ),
    (
        "met_feed_rest_wait_seconds",
        "gauge",
        "Last observed REST budget wait.",
    ),
    (
        "met_feed_identify_wait_seconds",
        "gauge",
        "Last completed Identify queue wait.",
    ),
    (
        "met_feed_drain_seconds",
        "gauge",
        "Last completed drain duration.",
    ),
    (
        "met_events_total",
        "counter",
        "Processed events supplied by the caller.",
    ),
    ("met_resumes_total", "counter", "Gateway resumes."),
    (
        "met_heartbeat_rtt_seconds",
        "gauge",
        "Last heartbeat RTT; zero before measurement.",
    ),
    ("met_queue_depth", "gauge", "Queued items."),
    (
        "met_queue_oldest_age_seconds",
        "gauge",
        "Age of the oldest queued item.",
    ),
    ("met_processing_tasks", "gauge", "Tasks in progress."),
    (
        "met_ipc_latency_seconds",
        "histogram",
        "IPC latency with fixed inclusive buckets; sum saturates at u64 nanoseconds.",
    ),
    ("met_http_429_total", "counter", "HTTP 429 responses."),
    ("met_http_5xx_total", "counter", "HTTP 5xx responses."),
    (
        "met_interaction_initial_response_deadline_exceeded_total",
        "counter",
        "Interaction initial response deadline misses.",
    ),
    (
        "met_active_changes_total",
        "counter",
        "Authority changes supplied by the caller.",
    ),
    (
        "met_runtime_generation",
        "gauge",
        "Observed component generation and role; not authority. Component generation is a value to bound label churn.",
    ),
    (
        "met_ws_connected",
        "gauge",
        "Observed Gateway WebSocket connection state.",
    ),
    (
        "met_failover_elapsed_seconds",
        "gauge",
        "Current or last completed failover elapsed time; absent before first attempt.",
    ),
    (
        "met_failover_in_progress",
        "gauge",
        "Whether a failover timer is running.",
    ),
    (
        "met_failback_elapsed_seconds",
        "gauge",
        "Current or last completed failback elapsed time; absent before first attempt.",
    ),
    (
        "met_failback_in_progress",
        "gauge",
        "Whether a failback timer is running.",
    ),
];

fn seconds(ns: u64) -> String {
    // Integer formatting preserves exact nanosecond boundaries above 2^53 too.
    format!("{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000)
}

fn render_series(
    families: &mut BTreeMap<&'static str, String>,
    status: &StatusSnapshot,
    histogram: &HistogramSnapshot,
) {
    let labels = format!(
        "bot_id=\"{}\",shard_id=\"{}\",generation_id=\"{}\"",
        escape_label_value(status.labels.bot_id()),
        status.labels.shard_id(),
        status.labels.generation_id()
    );
    for (name, value) in [
        ("met_events_total", status.events_total),
        ("met_resumes_total", status.resumes_total),
        ("met_queue_depth", status.queue_depth),
        ("met_processing_tasks", status.processing_tasks),
        ("met_http_429_total", status.http_429_total),
        ("met_http_5xx_total", status.http_5xx_total),
        (
            "met_interaction_initial_response_deadline_exceeded_total",
            status.interaction_initial_response_deadline_exceeded_total,
        ),
        ("met_active_changes_total", status.active_changes_total),
        ("met_ws_connected", u64::from(status.runtime.ws_connected)),
    ] {
        let out = families.entry(name).or_default();
        writeln!(out, "{name}{{{labels}}} {value}").expect("String write");
    }
    for (name, ns) in [
        ("met_heartbeat_rtt_seconds", status.heartbeat_rtt_ns),
        ("met_queue_oldest_age_seconds", status.queue_oldest_age_ns),
    ] {
        let out = families.entry(name).or_default();
        writeln!(out, "{name}{{{labels}}} {}", seconds(ns)).expect("String write");
    }
    let out = families.entry("met_ipc_latency_seconds").or_default();
    let mut cumulative = 0_u64;
    for (upper, count) in histogram.upper_bounds.iter().zip(&histogram.bucket_counts) {
        cumulative = cumulative.saturating_add(*count);
        writeln!(
            out,
            "met_ipc_latency_seconds_bucket{{{labels},le=\"{}\"}} {cumulative}",
            seconds(*upper)
        )
        .expect("String write");
    }
    writeln!(
        out,
        "met_ipc_latency_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
        histogram.count
    )
    .expect("String write");
    writeln!(
        out,
        "met_ipc_latency_seconds_count{{{labels}}} {}",
        histogram.count
    )
    .expect("String write");
    writeln!(
        out,
        "met_ipc_latency_seconds_sum{{{labels}}} {}",
        seconds(histogram.sum)
    )
    .expect("String write");
    for (component, generation) in [
        ("gateway", status.runtime.gateway),
        ("core", status.runtime.core),
    ] {
        let out = families.entry("met_runtime_generation").or_default();
        writeln!(
            out,
            "met_runtime_generation{{{labels},component=\"{component}\",role=\"{}\"}} {}",
            generation.role.as_str(),
            generation.generation_id
        )
        .expect("String write");
    }
    for (elapsed_name, progress_name, recovery) in [
        (
            "met_failover_elapsed_seconds",
            "met_failover_in_progress",
            status.failover,
        ),
        (
            "met_failback_elapsed_seconds",
            "met_failback_in_progress",
            status.failback,
        ),
    ] {
        if let Some(recovery) = recovery {
            let out = families.entry(elapsed_name).or_default();
            writeln!(
                out,
                "{elapsed_name}{{{labels}}} {}",
                seconds(recovery.elapsed_ns)
            )
            .expect("String write");
        }
        let out = families.entry(progress_name).or_default();
        writeln!(
            out,
            "{progress_name}{{{labels}}} {}",
            u8::from(recovery.is_some_and(|r| r.in_progress))
        )
        .expect("String write");
    }
}
