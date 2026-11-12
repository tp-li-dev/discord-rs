use src::{
    event_seq::{Priority, Receiver, Sender},
    metrics::{Counter, Histogram, IPC_LATENCY_BUCKETS_NS},
    protocol::BotId,
    state_store::*,
    supervisor::*,
};
use std::{hint::black_box, sync::Arc, time::Instant};
const WARM: usize = 1000;
const N: usize = 10000;
fn report(name: &str, mut samples: Vec<u128>) {
    samples.sort_unstable();
    let q = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    println!(
        "{name}: n={} p50={}ns p95={}ns p99={}ns max={}ns",
        samples.len(),
        q(50),
        q(95),
        q(99),
        samples[samples.len() - 1]
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut sender = Sender::new(1, 0, 1024, 0).unwrap();
    let mut receiver = Receiver::new(0);
    let counter = Counter::new();
    let histogram = Histogram::new(IPC_LATENCY_BUCKETS_NS).unwrap();
    let key = StateKey::new(
        StoreScope {
            bot_id: BotId(1),
            shard_id: 0,
        },
        PublicLabel::new("bench").unwrap(),
        PublicLabel::new("test").unwrap(),
        StateKind::Application,
        StateId::from_bytes([1; 16]),
    );
    let schema = SchemaVersion::new(1, 1).unwrap();
    let value = StateValue {
        schema,
        expires_at: None,
        data: vec![1; 64],
    };
    let store = MemoryStateStore::new(
        StoreLimits {
            max_entries: 1,
            max_bytes: 4096,
            max_value_bytes: 1024,
        },
        Arc::new(SystemClock),
    );
    let mut manager = TaskManager::new(MonotonicClock::default(), BotId(1), 0, 1).unwrap();
    let spec = TaskSpec {
        owner: TaskOwner {
            bot_id: BotId(1),
            shard_id: 0,
            cog: None,
            request_id: None,
            parent: None,
        },
        kind: TaskKind::RequestScoped,
        timeout: None,
    };
    let mut data: [Vec<u128>; 5] = std::array::from_fn(|_| Vec::with_capacity(N));
    let mut event_total = std::time::Duration::ZERO;
    for i in 0..WARM + N {
        let start = Instant::now();
        let seq = sender
            .enqueue(vec![1; 64], Priority::Required)
            .unwrap_or_else(|_| panic!("enqueue failed"));
        let event = sender.replay().next().unwrap();
        assert_eq!(event.local_sequence, seq);
        black_box(&event.payload);
        let ack = receiver.accept(seq, true).unwrap();
        sender.ack(ack).unwrap();
        let elapsed = start.elapsed();
        if i >= WARM {
            data[0].push(elapsed.as_nanos());
            event_total += elapsed;
        }
        let start = Instant::now();
        counter.inc();
        let elapsed = start.elapsed();
        if i >= WARM {
            data[1].push(elapsed.as_nanos());
        }
        let start = Instant::now();
        histogram.observe(black_box(42_000));
        let elapsed = start.elapsed();
        if i >= WARM {
            data[2].push(elapsed.as_nanos());
        }
        let start = Instant::now();
        store
            .put(key.clone(), value.clone(), PutCondition::Any)
            .await
            .unwrap();
        let elapsed = start.elapsed();
        if i >= WARM {
            data[3].push(elapsed.as_nanos());
        }
        let start = Instant::now();
        black_box(store.get(&key, schema).await.unwrap().unwrap());
        let elapsed = start.elapsed();
        if i >= WARM {
            data[4].push(elapsed.as_nanos());
        }
    }
    for (name, values) in [
        "event_seq enqueue/replay/accept/ack",
        "counter inc",
        "histogram observe",
        "memory store put",
        "memory store get",
    ]
    .into_iter()
    .zip(data)
    {
        report(name, values);
    }
    println!(
        "event_seq serial throughput: {:.0} cycles/s (sum of measured cycle durations)",
        N as f64 / event_total.as_secs_f64()
    );
    let mut samples = Vec::with_capacity(N);
    for i in 0..WARM + N {
        let start = Instant::now();
        let id = manager.register(spec.clone()).unwrap();
        manager
            .start(id, |_| async { Ok(TaskCompletion::Completed) })
            .unwrap();
        loop {
            manager.poll().unwrap();
            if manager.snapshot(id).unwrap().state == TaskState::Completed {
                break;
            }
            tokio::task::yield_now().await;
        }
        let elapsed = start.elapsed();
        manager.forget(id).unwrap();
        if i >= WARM {
            samples.push(elapsed.as_nanos());
        }
    }
    report("task manager register/start/completion", samples);
    println!("failures=0 warmup={WARM} samples={N}");
}
