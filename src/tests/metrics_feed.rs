use src::{
    event_seq,
    metrics::{GenerationStatus, MetricLabels, MetricsRegistry, RuntimeRole, RuntimeStatus},
    metrics_feed::{self, Observation},
    switch,
};
use std::time::Duration;

#[test]
fn measured_waits_and_failover_are_exported_without_extra_labels() {
    use src::{
        failover,
        metrics_feed::observe_rest_admission,
        rest_budget::{Admission, WaitReason},
    };
    let registry = MetricsRegistry::default();
    let generation = GenerationStatus {
        generation_id: 1,
        role: RuntimeRole::Active,
    };
    let handle = registry
        .register(
            MetricLabels::new("42", 0, 1).unwrap(),
            RuntimeStatus {
                gateway: generation,
                core: generation,
                ws_connected: false,
            },
        )
        .unwrap();
    let (tx, rx) = metrics_feed::channel(4).unwrap();
    assert!(tx.observe(Observation::Failover(failover::State::Degraded)));
    assert!(observe_rest_admission(
        &tx,
        Admission::Wait {
            retry_at: Duration::from_secs(5),
            reason: WaitReason::Route429
        },
        Duration::from_secs(3)
    ));
    assert!(tx.observe(Observation::IdentifyWait(Duration::from_millis(20))));
    assert!(tx.observe(Observation::Drain(Duration::from_millis(40))));
    assert_eq!(rx.collect(&handle, 4), 4);
    let output = registry.render_prometheus();
    let labels = "{bot_id=\"42\",shard_id=\"0\",generation_id=\"1\"}";
    for (name, value) in [
        ("met_feed_failover_state", "6"),
        ("met_feed_rest_wait_seconds", "2.000000000"),
        ("met_feed_identify_wait_seconds", "0.020000000"),
        ("met_feed_drain_seconds", "0.040000000"),
    ] {
        assert!(output.contains(&format!("{name}{labels} {value}")));
    }
}

#[test]
fn live_adapters_and_bounded_channel() {
    let registry = MetricsRegistry::default();
    let generation = GenerationStatus {
        generation_id: 1,
        role: RuntimeRole::Active,
    };
    let handle = registry
        .register(
            MetricLabels::new("42", 0, 1).unwrap(),
            RuntimeStatus {
                gateway: generation,
                core: generation,
                ws_connected: false,
            },
        )
        .unwrap();
    let (tx, rx) = metrics_feed::channel(2).unwrap();
    assert!(tx.observe(Observation::IpcRoundTrip(Duration::from_millis(2))));
    assert!(metrics_feed::observe_switch(&tx, &switch::Switch::new(9)));
    assert!(!tx.observe(Observation::Drain(Duration::from_secs(1))));
    assert_eq!(rx.collect(&handle, 1), 1);
    assert_eq!(rx.collect(&handle, 10), 1);
    let mut source = event_seq::Sender::new(4, 0, 100, 0).unwrap();
    assert!(
        source
            .enqueue(vec![1, 2, 3], event_seq::Priority::Required)
            .is_ok()
    );
    assert!(metrics_feed::observe_event_buffer(&tx, &source));
    rx.collect(&handle, 1);
    let output = registry.render_prometheus();
    assert!(output.contains(
        "met_ipc_latency_seconds_count{bot_id=\"42\",shard_id=\"0\",generation_id=\"1\"} 1"
    ));
    assert!(output.contains(
        "met_feed_switch_generation{bot_id=\"42\",shard_id=\"0\",generation_id=\"1\"} 9"
    ));
    assert!(output.contains(
        "met_feed_event_retained_bytes{bot_id=\"42\",shard_id=\"0\",generation_id=\"1\"} 3"
    ));
    assert!(
        output
            .contains("met_feed_dropped_total{bot_id=\"42\",shard_id=\"0\",generation_id=\"1\"} 1")
    );
    assert!(!output.contains("met_feed_drain_seconds{"));
    assert!(metrics_feed::channel(0).is_none());
}
