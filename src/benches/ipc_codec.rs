//! Codec-only microbenchmark; no IPC transport or authentication is exercised.
use src::protocol::{
    BotId, Data, Envelope, GenerationId, Identity, Message, OwnershipEpoch, RequestId, Shard,
};
use std::{hint::black_box, time::Instant};

fn fixture(size: usize) -> Envelope {
    Envelope {
        protocol_version: 1,
        identity: Identity {
            bot_id: BotId(42),
            shard: Shard::new(1, 4).unwrap(),
            generation_id: GenerationId(9),
            ownership_epoch: OwnershipEpoch(3),
        },
        request_id: RequestId([7; 16]),
        timestamp_ms: 1_800_000_000_000,
        message: Message::Data(Data::DiscordEvent {
            local_sequence: 123,
            event_key: Some("synthetic-event-123".into()),
            payload: (0..size).map(|i| (i % 256) as u8).collect(),
        }),
    }
}

fn measure<T>(warmup: usize, samples: usize, mut operation: impl FnMut() -> T) -> [u128; 3] {
    for _ in 0..warmup {
        black_box(operation());
    }
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let value = black_box(operation());
        let elapsed = start.elapsed().as_nanos();
        // Destruction is outside the measured interval; allocation is inside.
        drop(value);
        times.push(elapsed);
    }
    times.sort_unstable();
    [50, 95, 99].map(|p| times[(samples * p).div_ceil(100) - 1])
}

fn count(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0 && *n <= 1_000_000)
            .unwrap_or_else(|| panic!("{name} must be in 1..=1000000")),
        Err(std::env::VarError::NotPresent) => default,
        Err(_) => panic!("{name} must contain valid Unicode"),
    }
}

fn main() {
    let warmup = count("IPC_BENCH_WARMUP", 1_000);
    let samples = count("IPC_BENCH_SAMPLES", 10_000);
    println!("warmup={warmup}, samples={samples}, percentiles=nearest-rank, unit=ns");
    println!("case,payload_bytes,codec,encoded_bytes,operation,p50_ns,p95_ns,p99_ns");
    for (case, size) in [("small", 64), ("medium", 4_096), ("large", 65_536)] {
        let message = fixture(size);
        // Named maps support the actual internally tagged protocol enums.
        let json = serde_json::to_vec(&message).unwrap();
        let msgpack = rmp_serde::to_vec_named(&message).unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&json).unwrap(), message);
        assert_eq!(
            rmp_serde::from_slice::<Envelope>(&msgpack).unwrap(),
            message
        );
        // Alternate codec order by case to reduce a fixed ordering bias.
        let codecs = if size == 4_096 {
            ["msgpack", "json"]
        } else {
            ["json", "msgpack"]
        };
        for codec in codecs {
            let bytes = if codec == "json" { &json } else { &msgpack };
            let encode = measure(warmup, samples, || {
                if codec == "json" {
                    serde_json::to_vec(black_box(&message)).unwrap()
                } else {
                    rmp_serde::to_vec_named(black_box(&message)).unwrap()
                }
            });
            let decode = measure(warmup, samples, || {
                if codec == "json" {
                    serde_json::from_slice::<Envelope>(black_box(bytes)).unwrap()
                } else {
                    rmp_serde::from_slice::<Envelope>(black_box(bytes)).unwrap()
                }
            });
            for (op, [p50, p95, p99]) in [("encode", encode), ("decode", decode)] {
                println!(
                    "{case},{size},{codec},{},{op},{p50},{p95},{p99}",
                    bytes.len()
                );
            }
        }
    }
}
