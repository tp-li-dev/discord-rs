use src::metrics::*;
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn runtime() -> RuntimeStatus {
    RuntimeStatus {
        gateway: GenerationStatus {
            generation_id: 10,
            role: RuntimeRole::Active,
        },
        core: GenerationStatus {
            generation_id: 20,
            role: RuntimeRole::Standby,
        },
        ws_connected: true,
    }
}

fn labels(bot: &str, shard: u32, generation: u64) -> MetricLabels {
    MetricLabels::new(bot, shard, generation).unwrap()
}

fn limits() -> CardinalityLimits {
    CardinalityLimits {
        max_series: 32,
        max_bots: 4,
        max_shards_per_bot: 4,
        max_generations_per_shard: 4,
    }
}

fn handle(registry: &MetricsRegistry) -> Arc<ShardMetrics> {
    registry.register(labels("bot-a", 2, 7), runtime()).unwrap()
}

#[test]
fn counters_are_monotonic_and_saturate_instead_of_wrapping() {
    let counter = Counter::new();
    assert_eq!(counter.get(), 0);
    counter.inc();
    counter.add(9);
    counter.add(0);
    assert_eq!(counter.get(), 10);
    counter.add(u64::MAX - 11);
    assert_eq!(counter.get(), u64::MAX - 1);
    counter.inc();
    counter.inc();
    counter.add(u64::MAX);
    assert_eq!(counter.get(), u64::MAX);
    assert_eq!(Counter::default().get(), 0);
}

#[test]
fn nonnegative_gauges_support_set_and_saturating_arithmetic() {
    let gauge = Gauge::new(2);
    gauge.inc();
    gauge.add(5);
    gauge.dec();
    assert_eq!(gauge.get(), 7);
    gauge.sub(20);
    gauge.dec();
    assert_eq!(gauge.get(), 0);
    gauge.set(u64::MAX - 1);
    gauge.add(10);
    gauge.inc();
    assert_eq!(gauge.get(), u64::MAX);
    gauge.set(100);
    gauge.sub(1);
    gauge.add(0);
    assert_eq!(gauge.get(), 99);
    assert_eq!(Gauge::default().get(), 0);
}

#[test]
fn invalid_histogram_buckets_are_rejected_without_input_in_errors() {
    for bounds in [
        vec![],
        vec![2, 2],
        vec![2, 1],
        vec![0, 1, 0],
        (0..=MAX_HISTOGRAM_BUCKETS as u64).collect(),
    ] {
        assert!(matches!(
            Histogram::new(&bounds),
            Err(MetricsError::InvalidBuckets)
        ));
    }
    let exactly_max: Vec<_> = (0..MAX_HISTOGRAM_BUCKETS as u64).collect();
    assert!(Histogram::new(&exactly_max).is_ok());
    assert!(Histogram::new(&[0]).is_ok());
    assert!(Histogram::new(&[u64::MAX]).is_ok());
}

#[test]
fn histogram_buckets_are_inclusive_and_keep_overflow_separate() {
    let histogram = Histogram::new(&[0, 10, 100]).unwrap();
    for sample in [0, 1, 10, 11, 100, 101, u64::MAX] {
        histogram.observe(sample);
    }
    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.upper_bounds(), [0, 10, 100]);
    assert_eq!(snapshot.bucket_counts(), [1, 2, 2, 2]);
    assert_eq!(snapshot.count(), 7);
    assert_eq!(snapshot.sum(), u64::MAX);
    assert_eq!(
        snapshot.quantile_ppm(0).unwrap(),
        QuantileEstimate::UpperBound { value: 0 }
    );
    assert_eq!(
        snapshot.quantile_ppm(1_000_000).unwrap(),
        QuantileEstimate::Overflow { lower_bound: 100 }
    );
    assert_eq!(
        snapshot.quantile_ppm(1_000_001),
        Err(MetricsError::InvalidQuantile)
    );
    assert_eq!(
        snapshot.quantile_ppm(u32::MAX),
        Err(MetricsError::InvalidQuantile)
    );
}

#[test]
fn empty_zero_and_overflow_quantiles_are_distinct_and_serializable() {
    let histogram = Histogram::new(&[0]).unwrap();
    let empty = histogram.snapshot();
    assert_eq!(empty.percentiles().p50, QuantileEstimate::Empty);
    assert_eq!(empty.count(), 0);
    assert!(empty.quantile_ppm(u32::MAX).is_err());
    histogram.observe(0);
    assert_eq!(
        histogram.snapshot().percentiles().p99,
        QuantileEstimate::UpperBound { value: 0 }
    );
    histogram.observe(1);
    assert_eq!(
        histogram.snapshot().percentiles().p99,
        QuantileEstimate::Overflow { lower_bound: 0 }
    );
    for estimate in [
        QuantileEstimate::Empty,
        QuantileEstimate::UpperBound { value: u64::MAX },
        QuantileEstimate::Overflow {
            lower_bound: u64::MAX,
        },
    ] {
        let encoded = serde_json::to_string(&estimate).unwrap();
        assert!(!encoded.contains("null"));
        assert_eq!(
            serde_json::from_str::<QuantileEstimate>(&encoded).unwrap(),
            estimate
        );
    }
}

#[test]
fn percentiles_use_integer_nearest_rank_including_p999() {
    let histogram = Histogram::new(&[500, 950, 990, 999, 1000]).unwrap();
    for sample in 1..=1000 {
        histogram.observe(sample);
    }
    let snapshot = histogram.snapshot();
    let percentiles = snapshot.percentiles();
    assert_eq!(snapshot.count(), 1000);
    assert_eq!(snapshot.sum(), 500_500);
    assert_eq!(percentiles.count, 1000);
    assert_eq!(percentiles.p50, QuantileEstimate::UpperBound { value: 500 });
    assert_eq!(percentiles.p95, QuantileEstimate::UpperBound { value: 950 });
    assert_eq!(percentiles.p99, QuantileEstimate::UpperBound { value: 990 });
    assert_eq!(
        percentiles.p999,
        QuantileEstimate::UpperBound { value: 999 }
    );
    assert_eq!(
        snapshot.quantile_ppm(1_000_000).unwrap(),
        QuantileEstimate::UpperBound { value: 1000 }
    );
    // ceil(3 * .5) selects the second sample, not the first.
    let small = Histogram::new(&[10, 20, 30]).unwrap();
    for value in [10, 20, 30] {
        small.observe(value);
    }
    assert_eq!(
        small.snapshot().percentiles().p50,
        QuantileEstimate::UpperBound { value: 20 }
    );
}

#[test]
fn duration_observations_saturate_and_maximum_boundary_is_finite() {
    let histogram = Histogram::new(&[1, u64::MAX]).unwrap();
    histogram.observe_duration(Duration::ZERO);
    histogram.observe_duration(Duration::from_nanos(1));
    histogram.observe_duration(Duration::MAX);
    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.bucket_counts(), [2, 1, 0]);
    assert_eq!(snapshot.sum(), u64::MAX);
    assert_eq!(
        snapshot.quantile_ppm(1_000_000).unwrap(),
        QuantileEstimate::UpperBound { value: u64::MAX }
    );
}

#[test]
fn histogram_storage_shape_is_fixed_as_observation_count_grows() {
    let histogram = Histogram::new(&[0, 1, 10]).unwrap();
    let before = histogram.snapshot();
    for value in 0..100_000 {
        histogram.observe(value);
    }
    let after = histogram.snapshot();
    assert_eq!(before.upper_bounds(), after.upper_bounds());
    assert_eq!(before.bucket_counts().len(), after.bucket_counts().len());
    assert_eq!(after.bucket_counts().len(), 4);
    assert_eq!(after.count(), 100_000);
    assert_eq!(after.bucket_counts().iter().sum::<u64>(), after.count());
}

#[test]
fn label_validation_bounds_utf8_and_rejects_controls() {
    for invalid in ["", " ", "\n", "a\0b", "a\rb", "a\tb", "a\u{7f}b"] {
        assert_eq!(
            MetricLabels::new(invalid, 0, 0),
            Err(MetricsError::InvalidLabel)
        );
    }
    assert!(MetricLabels::new("a".repeat(MAX_LABEL_BYTES), u32::MAX, u64::MAX).is_ok());
    assert_eq!(
        MetricLabels::new("a".repeat(MAX_LABEL_BYTES + 1), 0, 0),
        Err(MetricsError::InvalidLabel)
    );
    assert_eq!(
        MetricLabels::new("あ".repeat(MAX_LABEL_BYTES / 3 + 1), 0, 0),
        Err(MetricsError::InvalidLabel)
    );
    let safe = labels("018fe000-0000-7000-8000-000000000001", 123, 456);
    assert_eq!(safe.bot_id(), "018fe000-0000-7000-8000-000000000001");
    assert_eq!(safe.shard_id(), 123);
    assert_eq!(safe.generation_id(), 456);
}

#[test]
fn credential_shapes_and_carriers_are_rejected_without_echoing_them() {
    // Deliberately synthetic fixtures; no live credential is used.
    let fake_discord = format!("{}.{}.{}", "A".repeat(24), "B".repeat(6), "C".repeat(27));
    let candidates = vec![
        fake_discord,
        "mfa.synthetic-example-only".to_owned(),
        "Bot synthetic-example-only".to_owned(),
        "Bearer synthetic-example-only".to_owned(),
        "TOKEN=synthetic-example-only".to_owned(),
        "Authorization: synthetic-example-only".to_owned(),
        "manager_secret=synthetic-example-only".to_owned(),
        "DISCORD_BOT_TOKEN".to_owned(),
        "password=synthetic-example-only".to_owned(),
        "credential=synthetic-example-only".to_owned(),
        "custom_id=synthetic-example-only".to_owned(),
        "/webhooks/123/synthetic-example-only".to_owned(),
        "/interactions/456/synthetic-example-only".to_owned(),
        "https://example.invalid/public".to_owned(),
    ];
    for candidate in candidates {
        let error = MetricLabels::new(&candidate, 0, 0).unwrap_err();
        assert_eq!(error, MetricsError::SuspectedSecret);
        assert!(!error.to_string().contains(&candidate));
        assert!(!format!("{error:?}").contains(&candidate));
        let encoded = serde_json::json!({"bot_id": candidate, "shard_id": 0, "generation_id": 0});
        let decode_error = serde_json::from_value::<MetricLabels>(encoded).unwrap_err();
        assert!(!decode_error.to_string().contains("synthetic-example-only"));
    }
}

#[test]
fn label_deserialization_revalidates_and_rejects_unknown_or_duplicate_fields() {
    let original = labels("公開Bot", 2, 3);
    let json = serde_json::to_string(&original).unwrap();
    assert_eq!(
        serde_json::from_str::<MetricLabels>(&json).unwrap(),
        original
    );
    let invalid = [
        r#"{"bot_id":"", "shard_id":0, "generation_id":0}"#,
        r#"{"bot_id":"bot", "shard_id":-1, "generation_id":0}"#,
        r#"{"bot_id":"bot", "shard_id":0, "generation_id":-1}"#,
        r#"{"bot_id":"bot", "shard_id":0, "generation_id":0, "extra":"ignored"}"#,
        r#"{"bot_id":"bot", "bot_id":"other", "shard_id":0, "generation_id":0}"#,
        r#"{"bot_id":"bot", "shard_id":0}"#,
    ];
    for json in invalid {
        assert!(serde_json::from_str::<MetricLabels>(json).is_err());
    }
}

#[test]
fn escaping_prevents_newline_quote_and_backslash_metric_injection() {
    let value = "公開\"Bot\\line\nforged_metric 1";
    let expected = "公開\\\"Bot\\\\line\\nforged_metric 1";
    assert_eq!(escape_label_value(value), expected);
    assert_eq!(escape_label_value(""), "");
    let registry = MetricsRegistry::default();
    registry.register(labels(value, 0, 0), runtime()).unwrap();
    let text = registry.render_prometheus();
    assert!(text.contains(&format!("bot_id=\"{expected}\"")));
    assert!(!text.lines().any(|line| line.starts_with("forged_metric")));
    assert!(text.ends_with('\n'));
    assert!(!text.contains('\r'));
    let decoded: Vec<StatusSnapshot> =
        serde_json::from_str(&serde_json::to_string(&registry.snapshots()).unwrap()).unwrap();
    assert_eq!(decoded[0].labels.bot_id(), value);
}

#[test]
fn registry_configuration_has_finite_nonzero_hard_limits() {
    for field in 0..4 {
        for invalid in [0, MAX_REGISTERED_SERIES + 1, usize::MAX] {
            let mut values = [32, 4, 4, 4];
            values[field] = invalid;
            let config = CardinalityLimits {
                max_series: values[0],
                max_bots: values[1],
                max_shards_per_bot: values[2],
                max_generations_per_shard: values[3],
            };
            assert!(matches!(
                MetricsRegistry::new(config),
                Err(MetricsError::InvalidLimits)
            ));
        }
    }
    let max = MAX_REGISTERED_SERIES;
    assert!(
        MetricsRegistry::new(CardinalityLimits {
            max_series: max,
            max_bots: max,
            max_shards_per_bot: max,
            max_generations_per_shard: max
        })
        .is_ok()
    );
}

#[test]
fn each_cardinality_dimension_is_bounded_and_existing_handles_survive() {
    let configs = [
        (
            CardinalityLimits {
                max_series: 1,
                ..limits()
            },
            labels("bot-b", 0, 0),
            MetricsError::SeriesLimit,
        ),
        (
            CardinalityLimits {
                max_bots: 1,
                ..limits()
            },
            labels("bot-b", 0, 0),
            MetricsError::BotLimit,
        ),
        (
            CardinalityLimits {
                max_shards_per_bot: 1,
                ..limits()
            },
            labels("bot-a", 3, 7),
            MetricsError::ShardLimit,
        ),
        (
            CardinalityLimits {
                max_generations_per_shard: 1,
                ..limits()
            },
            labels("bot-a", 2, 8),
            MetricsError::GenerationLimit,
        ),
    ];
    for (config, rejected, error) in configs {
        let registry = MetricsRegistry::new(config).unwrap();
        assert!(registry.is_empty());
        let existing = handle(&registry);
        existing.events.inc();
        assert!(matches!(registry.register(rejected, runtime()), Err(actual) if actual == error));
        let altered = RuntimeStatus {
            ws_connected: false,
            ..runtime()
        };
        let repeated = registry
            .register(existing.labels().clone(), altered)
            .unwrap();
        assert!(Arc::ptr_eq(&existing, &repeated));
        assert_eq!(repeated.snapshot().runtime, runtime());
        assert_eq!(repeated.events.get(), 1);
        assert_eq!(registry.len(), 1);
        drop(repeated);
        drop(existing);
        assert_eq!(registry.len(), 1); // Dropping caller handles does not free lifetime cardinality.
    }
}

#[test]
fn bots_shards_and_generations_remain_independent_and_sorted() {
    let registry = MetricsRegistry::new(limits()).unwrap();
    let identities = [
        labels("bot-b", 2, 3),
        labels("bot-a", 1, 2),
        labels("bot-a", 0, 2),
        labels("bot-a", 0, 1),
    ];
    for (index, identity) in identities.iter().enumerate() {
        let handle = registry.register(identity.clone(), runtime()).unwrap();
        handle.events.add((index + 1) as u64);
        handle.http_429.add(index as u64);
    }
    let snapshots = registry.snapshots();
    assert!(
        snapshots
            .windows(2)
            .all(|pair| pair[0].labels < pair[1].labels)
    );
    for snapshot in snapshots {
        let index = identities
            .iter()
            .position(|identity| identity == &snapshot.labels)
            .unwrap();
        assert_eq!(snapshot.events_total, (index + 1) as u64);
        assert_eq!(snapshot.http_429_total, index as u64);
    }
    assert_eq!(registry.render_prometheus(), registry.render_prometheus());
}

#[test]
fn concurrent_duplicate_registration_returns_one_shared_handle() {
    let registry = Arc::new(
        MetricsRegistry::new(CardinalityLimits {
            max_series: 1,
            ..limits()
        })
        .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                handle(&registry)
            })
        })
        .collect();
    let handles: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert!(
        handles
            .iter()
            .all(|handle| Arc::ptr_eq(handle, &handles[0]))
    );
    assert_eq!(registry.len(), 1);
}

#[test]
fn concurrent_distinct_registration_cannot_race_past_limits() {
    for dimension in 0..4 {
        let mut config = limits();
        let cap = 3;
        match dimension {
            0 => config.max_series = cap,
            1 => config.max_bots = cap,
            2 => config.max_shards_per_bot = cap,
            _ => config.max_generations_per_shard = cap,
        }
        let registry = Arc::new(MetricsRegistry::new(config).unwrap());
        let barrier = Arc::new(Barrier::new(12));
        let workers: Vec<_> = (0..12)
            .map(|index| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let identity = match dimension {
                        0 | 1 => labels(&format!("bot-{index}"), 0, 0),
                        2 => labels("bot", index, 0),
                        _ => labels("bot", 0, u64::from(index)),
                    };
                    barrier.wait();
                    registry.register(identity, runtime()).is_ok()
                })
            })
            .collect();
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|success| *success)
            .count();
        assert_eq!(successes, cap);
        assert_eq!(registry.len(), cap);
    }
}

#[test]
fn status_snapshot_serializes_all_requested_measurements() {
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    handle.events.add(100);
    handle.resumes.add(2);
    handle.heartbeat_rtt_ns.set(123_456);
    handle.queue_depth.set(8);
    handle.queue_oldest_age_ns.set(200_000);
    handle.processing_tasks.set(5);
    handle
        .ipc_latency_ns
        .observe_duration(Duration::from_micros(100));
    handle.http_429.add(3);
    handle.http_5xx.add(4);
    handle.interaction_initial_response_deadline_exceeded.add(6);
    handle.active_changes.add(7);
    handle.start_failover();
    handle.finish_failover().unwrap();
    handle.start_failback();
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.labels, labels("bot-a", 2, 7));
    assert_eq!(snapshot.runtime, runtime());
    assert_eq!((snapshot.events_total, snapshot.resumes_total), (100, 2));
    assert_eq!(snapshot.heartbeat_rtt_ns, 123_456);
    assert_eq!(
        (
            snapshot.queue_depth,
            snapshot.queue_oldest_age_ns,
            snapshot.processing_tasks
        ),
        (8, 200_000, 5)
    );
    assert_eq!(snapshot.ipc_latency_ns.count, 1);
    assert_eq!(
        snapshot.ipc_latency_ns.p99,
        QuantileEstimate::UpperBound { value: 100_000 }
    );
    assert_eq!((snapshot.http_429_total, snapshot.http_5xx_total), (3, 4));
    assert_eq!(
        snapshot.interaction_initial_response_deadline_exceeded_total,
        6
    );
    assert_eq!(snapshot.active_changes_total, 7);
    assert!(!snapshot.failover.unwrap().in_progress);
    assert!(snapshot.failback.unwrap().in_progress);
    let json = serde_json::to_string(&snapshot).unwrap();
    assert!(json.contains("\"role\":\"active\""));
    assert!(json.contains("\"role\":\"standby\""));
    assert_eq!(
        serde_json::from_str::<StatusSnapshot>(&json).unwrap(),
        snapshot
    );
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["labels"]["bot_id"] = "TOKEN=synthetic-example-only".into();
    assert!(serde_json::from_value::<StatusSnapshot>(value).is_err());
}

#[test]
fn recovery_timers_cover_idle_duplicate_start_completion_and_restart() {
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    assert_eq!(handle.finish_failover(), None);
    assert_eq!(handle.finish_failback(), None);
    assert_eq!(handle.snapshot().failover, None);
    assert_eq!(handle.snapshot().failback, None);
    for failback in [false, true] {
        if failback {
            handle.start_failback();
        } else {
            handle.start_failover();
        }
        let before = if failback {
            handle.snapshot().failback
        } else {
            handle.snapshot().failover
        }
        .unwrap();
        if failback {
            handle.start_failback();
        } else {
            handle.start_failover();
        }
        let after = if failback {
            handle.snapshot().failback
        } else {
            handle.snapshot().failover
        }
        .unwrap();
        assert!(before.in_progress && after.in_progress);
        assert!(after.elapsed_ns >= before.elapsed_ns);
        let elapsed = if failback {
            handle.finish_failback()
        } else {
            handle.finish_failover()
        }
        .unwrap();
        let completed = if failback {
            handle.snapshot().failback
        } else {
            handle.snapshot().failover
        }
        .unwrap();
        assert!(!completed.in_progress);
        assert_eq!(u128::from(completed.elapsed_ns), elapsed.as_nanos());
        assert_eq!(
            if failback {
                handle.finish_failback()
            } else {
                handle.finish_failover()
            },
            Some(elapsed)
        );
        assert_eq!(
            if failback {
                handle.snapshot().failback
            } else {
                handle.snapshot().failover
            },
            Some(completed)
        );
        if failback {
            handle.start_failback();
        } else {
            handle.start_failover();
        }
        assert!(
            if failback {
                handle.snapshot().failback
            } else {
                handle.snapshot().failover
            }
            .unwrap()
            .in_progress
        );
    }
}

fn sample_value(line: &str) -> u64 {
    line.rsplit_once(' ').unwrap().1.parse().unwrap()
}

fn assert_histogram_export_consistent(text: &str) {
    let buckets: Vec<_> = text
        .lines()
        .filter(|line| line.starts_with("met_ipc_latency_seconds_bucket{"))
        .map(sample_value)
        .collect();
    assert_eq!(buckets.len(), IPC_LATENCY_BUCKETS_NS.len() + 1);
    assert!(buckets.windows(2).all(|pair| pair[0] <= pair[1]));
    let count = text
        .lines()
        .find(|line| line.starts_with("met_ipc_latency_seconds_count{"))
        .map(sample_value)
        .unwrap();
    assert_eq!(*buckets.last().unwrap(), count);
    let infinity = text
        .lines()
        .find(|line| {
            line.starts_with("met_ipc_latency_seconds_bucket{") && line.contains("le=\"+Inf\"")
        })
        .unwrap();
    assert_eq!(sample_value(infinity), count);
}

#[test]
fn prometheus_families_are_unique_cumulative_and_use_exact_seconds() {
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    registry.register(labels("bot-b", 1, 2), runtime()).unwrap();
    for sample in [0, 1000, 1001, 5000, 10_000_000_001] {
        handle.ipc_latency_ns.observe(sample);
    }
    handle.heartbeat_rtt_ns.set(u64::MAX);
    handle.queue_oldest_age_ns.set(1);
    handle.http_429.add(2);
    handle.http_5xx.add(3);
    let text = registry.render_prometheus();
    let mut helps = BTreeMap::new();
    let mut types = BTreeMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            *helps
                .entry(rest.split_whitespace().next().unwrap())
                .or_insert(0) += 1;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            *types
                .entry(rest.split_whitespace().next().unwrap())
                .or_insert(0) += 1;
        }
    }
    assert!(!helps.is_empty());
    assert_eq!(helps, types);
    assert!(helps.values().all(|count| *count == 1));
    assert!(text.contains("# TYPE met_ipc_latency_seconds histogram\n"));
    assert!(text.contains("# TYPE met_events_total counter\n"));
    assert!(text.contains("# TYPE met_queue_depth gauge\n"));
    let one_series: String = text
        .lines()
        .filter(|line| line.contains("bot_id=\"bot-a\""))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_histogram_export_consistent(&one_series);
    let prefix = "bot_id=\"bot-a\",shard_id=\"2\",generation_id=\"7\"";
    assert!(text.contains(&format!(
        "met_ipc_latency_seconds_bucket{{{prefix},le=\"0.000001000\"}} 2\n"
    )));
    assert!(text.contains(&format!(
        "met_ipc_latency_seconds_bucket{{{prefix},le=\"0.000005000\"}} 4\n"
    )));
    assert!(text.contains(&format!(
        "met_ipc_latency_seconds_sum{{{prefix}}} 10.000007002\n"
    )));
    assert!(text.contains(&format!(
        "met_heartbeat_rtt_seconds{{{prefix}}} 18446744073.709551615\n"
    )));
    assert!(text.contains(&format!(
        "met_queue_oldest_age_seconds{{{prefix}}} 0.000000001\n"
    )));
    assert!(text.contains(&format!("met_http_429_total{{{prefix}}} 2\n")));
    assert!(text.contains(&format!("met_http_5xx_total{{{prefix}}} 3\n")));
    assert!(text.contains("component=\"gateway\",role=\"active\"} 10\n"));
    assert!(text.contains("component=\"core\",role=\"standby\"} 20\n"));
    assert!(
        !text
            .lines()
            .any(|line| line.starts_with("met_failover_elapsed_seconds{"))
    );
    assert!(text.ends_with('\n'));
}

#[test]
fn prometheus_timer_presence_and_runtime_role_updates_are_observable() {
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    handle.set_runtime_status(RuntimeStatus {
        gateway: GenerationStatus {
            generation_id: u64::MAX,
            role: RuntimeRole::Draining,
        },
        core: GenerationStatus {
            generation_id: 21,
            role: RuntimeRole::Active,
        },
        ws_connected: false,
    });
    handle.start_failover();
    handle.start_failback();
    let during = registry.render_prometheus();
    assert!(during.contains("component=\"gateway\",role=\"draining\"} 18446744073709551615\n"));
    for name in ["met_failover_in_progress", "met_failback_in_progress"] {
        assert_eq!(
            sample_value(
                during
                    .lines()
                    .find(|line| line.starts_with(&format!("{name}{{")))
                    .unwrap()
            ),
            1
        );
    }
    assert_eq!(
        sample_value(
            during
                .lines()
                .find(|line| line.starts_with("met_ws_connected{"))
                .unwrap()
        ),
        0
    );
    handle.finish_failover().unwrap();
    handle.finish_failback().unwrap();
    let after = registry.render_prometheus();
    for name in ["met_failover_in_progress", "met_failback_in_progress"] {
        assert_eq!(
            sample_value(
                after
                    .lines()
                    .find(|line| line.starts_with(&format!("{name}{{")))
                    .unwrap()
            ),
            0
        );
    }
    assert!(
        after
            .lines()
            .any(|line| line.starts_with("met_failover_elapsed_seconds{"))
    );
    assert!(
        after
            .lines()
            .any(|line| line.starts_with("met_failback_elapsed_seconds{"))
    );
    assert_eq!(handle.active_changes.get(), 0); // Observation updates do not grant or count authority.
}

#[test]
fn prometheus_samples_are_contiguous_with_their_family_metadata() {
    let registry = MetricsRegistry::default();
    for (bot, shard, generation) in [("bot-b", 2, 3), ("bot-a", 1, 2), ("bot-a", 0, 1)] {
        let handle = registry
            .register(labels(bot, shard, generation), runtime())
            .unwrap();
        handle.ipc_latency_ns.observe(1);
        handle.start_failover();
        handle.finish_failover();
        handle.start_failback();
    }
    let text = registry.render_prometheus();
    let mut current_family = "";
    let mut families = BTreeSet::new();
    let mut sample_keys = BTreeSet::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            current_family = rest.split_whitespace().next().unwrap();
            assert!(
                families.insert(current_family),
                "a family may occupy only one block"
            );
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            assert_eq!(rest.split_whitespace().next().unwrap(), current_family);
        } else {
            let (key, _) = line.rsplit_once(' ').unwrap();
            let metric = key.split_once('{').unwrap().0;
            let family = if metric.starts_with("met_ipc_latency_seconds_") {
                "met_ipc_latency_seconds"
            } else {
                metric
            };
            assert_eq!(family, current_family);
            assert!(
                sample_keys.insert(key),
                "each metric/label tuple must be unique"
            );
        }
    }
    assert!(families.contains("met_runtime_generation"));
    assert!(sample_keys.len() > 3 * IPC_LATENCY_BUCKETS_NS.len());
}

#[test]
fn component_generation_changes_cannot_create_unbounded_label_churn() {
    let registry = MetricsRegistry::new(CardinalityLimits {
        max_series: 1,
        max_bots: 1,
        max_shards_per_bot: 1,
        max_generations_per_shard: 1,
    })
    .unwrap();
    let handle = handle(&registry);
    let mut runtime_series = BTreeSet::new();
    for generation in 0..100 {
        let role = match generation % 3 {
            0 => RuntimeRole::Active,
            1 => RuntimeRole::Standby,
            _ => RuntimeRole::Draining,
        };
        handle.set_runtime_status(RuntimeStatus {
            gateway: GenerationStatus {
                generation_id: generation,
                role,
            },
            core: GenerationStatus {
                generation_id: 100 + generation,
                role,
            },
            ws_connected: generation % 2 == 0,
        });
        for line in registry
            .render_prometheus()
            .lines()
            .filter(|line| line.starts_with("met_runtime_generation{"))
        {
            let (key, value) = line.rsplit_once(' ').unwrap();
            assert!(value == generation.to_string() || value == (100 + generation).to_string());
            assert!(!key.contains("component_generation_id"));
            runtime_series.insert(key.to_owned());
        }
    }
    assert_eq!(runtime_series.len(), 6); // Two components times three fixed roles.
    assert_eq!(registry.len(), 1);
    assert!(matches!(
        registry.register(labels("bot-a", 2, 8), runtime()),
        Err(MetricsError::SeriesLimit)
    ));
}

#[test]
fn parallel_updates_and_exports_preserve_counts_and_bucket_invariants() {
    const WORKERS: usize = 8;
    const UPDATES: usize = 15_000;
    let registry = Arc::new(MetricsRegistry::default());
    let handle = handle(&registry);
    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let finished = Arc::new(AtomicBool::new(false));
    let exporter = {
        let registry = Arc::clone(&registry);
        let handle = Arc::clone(&handle);
        let finished = Arc::clone(&finished);
        thread::spawn(move || {
            let mut previous = 0;
            let mut snapshots = 0;
            while !finished.load(Ordering::Acquire) || snapshots < 2 {
                let snapshot = handle.ipc_latency_ns.snapshot();
                assert_eq!(
                    snapshot.count(),
                    snapshot.bucket_counts().iter().sum::<u64>()
                );
                assert!(snapshot.count() >= previous);
                previous = snapshot.count();
                assert_histogram_export_consistent(&registry.render_prometheus());
                snapshots += 1;
                thread::yield_now();
            }
            snapshots
        })
    };
    let workers: Vec<_> = (0..WORKERS)
        .map(|index| {
            let handle = Arc::clone(&handle);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..UPDATES {
                    handle.events.inc();
                    handle.queue_depth.inc();
                    handle.processing_tasks.inc();
                    handle.ipc_latency_ns.observe((index as u64 + 1) * 1000);
                    handle.http_429.inc();
                    handle.processing_tasks.dec();
                }
            })
        })
        .collect();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    finished.store(true, Ordering::Release);
    assert!(exporter.join().unwrap() >= 2);
    let total = (WORKERS * UPDATES) as u64;
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.events_total, total);
    assert_eq!(snapshot.queue_depth, total);
    assert_eq!(snapshot.processing_tasks, 0);
    assert_eq!(snapshot.http_429_total, total);
    assert_eq!(snapshot.ipc_latency_ns.count, total);
    assert_eq!(
        handle.ipc_latency_ns.snapshot().sum(),
        UPDATES as u64 * 1000 * (1..=WORKERS as u64).sum::<u64>()
    );
}

#[test]
fn concurrent_saturating_updates_never_wrap_or_underflow() {
    let counter = Arc::new(Counter::new());
    let gauge = Arc::new(Gauge::new(0));
    thread::scope(|scope| {
        for _ in 0..8 {
            let counter = Arc::clone(&counter);
            let gauge = Arc::clone(&gauge);
            scope.spawn(move || {
                for _ in 0..1000 {
                    counter.add(u64::MAX);
                    gauge.sub(u64::MAX);
                }
            });
        }
    });
    assert_eq!(counter.get(), u64::MAX);
    assert_eq!(gauge.get(), 0);
    thread::scope(|scope| {
        for _ in 0..8 {
            let gauge = Arc::clone(&gauge);
            scope.spawn(move || {
                for _ in 0..1000 {
                    gauge.add(u64::MAX);
                }
            });
        }
    });
    assert_eq!(gauge.get(), u64::MAX);
}

#[test]
fn concurrent_runtime_snapshots_preserve_generation_role_pairs() {
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    let status_a = runtime();
    let status_b = RuntimeStatus {
        gateway: GenerationStatus {
            generation_id: 100,
            role: RuntimeRole::Draining,
        },
        core: GenerationStatus {
            generation_id: 200,
            role: RuntimeRole::Active,
        },
        ws_connected: false,
    };
    let barrier = Arc::new(Barrier::new(2));
    let writer = {
        let handle = Arc::clone(&handle);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            for index in 0..10_000 {
                handle.set_runtime_status(if index % 2 == 0 { status_a } else { status_b });
            }
        })
    };
    barrier.wait();
    for _ in 0..10_000 {
        let observed = handle.snapshot().runtime;
        assert!(observed == status_a || observed == status_b);
    }
    writer.join().unwrap();
}

#[test]
fn empty_registry_exports_only_metadata_and_detached_handles_still_work() {
    let registry = MetricsRegistry::default();
    assert!(registry.snapshots().is_empty());
    assert!(
        registry
            .render_prometheus()
            .lines()
            .all(|line| line.starts_with('#'))
    );
    let handle = handle(&registry);
    drop(registry);
    handle.events.inc();
    assert_eq!(handle.snapshot().events_total, 1);
}

/// Run explicitly, preferably release:
/// cargo test --offline --release --test metrics lightweight_self_benchmark -- --ignored --nocapture
/// This measures local metric operations only, not Discord/IPC/Heartbeat safety.
#[test]
#[ignore = "manual local self-benchmark; no portable timing threshold"]
fn lightweight_self_benchmark() {
    const SAMPLES: usize = 1000;
    const BATCH: usize = 256;
    let counter = Counter::new();
    let gauge = Gauge::new(0);
    let histogram = Histogram::new(IPC_LATENCY_BUCKETS_NS).unwrap();
    let registry = MetricsRegistry::default();
    let handle = handle(&registry);
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    println!(
        "metrics self-benchmark os={} arch={} profile={} threads=1 samples={} batch={} operations_per_case={} failures=0 statistic=batch_average_ns_per_operation",
        std::env::consts::OS,
        std::env::consts::ARCH,
        profile,
        SAMPLES,
        BATCH,
        SAMPLES * BATCH
    );
    fn measure(name: &str, samples: usize, batch: usize, mut operation: impl FnMut()) {
        for _ in 0..batch {
            operation();
        }
        let mut timings = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            for _ in 0..batch {
                operation();
            }
            timings.push(start.elapsed().as_nanos() as f64 / batch as f64);
        }
        timings.sort_by(|a, b| a.total_cmp(b));
        let percentile = |percentage: usize| timings[(samples * percentage).div_ceil(100) - 1];
        println!(
            "{name}: operations={} p50={:.2} p95={:.2} p99={:.2} ns/op",
            samples * batch,
            percentile(50),
            percentile(95),
            percentile(99)
        );
    }
    measure("counter_inc", SAMPLES, BATCH, || {
        black_box(&counter).inc();
    });
    measure("gauge_inc_dec_pair", SAMPLES, BATCH, || {
        black_box(&gauge).inc();
        black_box(&gauge).dec();
    });
    measure("histogram_observe", SAMPLES, BATCH, || {
        black_box(&histogram).observe(black_box(100_001));
    });
    measure("event_task_latency_update", SAMPLES, BATCH, || {
        let handle = black_box(&handle);
        handle.events.inc();
        handle.processing_tasks.inc();
        handle.ipc_latency_ns.observe(black_box(50_001));
        handle.processing_tasks.dec();
    });
    measure("status_snapshot", 200, 8, || {
        black_box(handle.snapshot());
    });
    measure("prometheus_one_series", 200, 8, || {
        black_box(registry.render_prometheus());
    });
    black_box((
        counter.get(),
        gauge.get(),
        histogram.snapshot(),
        registry.snapshots(),
    ));
    let names: BTreeSet<_> = registry
        .snapshots()
        .into_iter()
        .map(|snapshot| snapshot.labels)
        .collect();
    assert_eq!(names.len(), 1);
}
