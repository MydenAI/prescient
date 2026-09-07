//! Bounded async fan-in benchmark.
//!
//! The Tokio runtime is the common executor, not part of Prescient's API. Each
//! producer is a task, construction/spawn is outside the timed interval, and
//! every implementation carries the same `(producer, sequence)` payload. For
//! shared-queue baselines the capacity is `producers * per_shard_capacity`,
//! matching Prescient's total buffered slots.
//!
//! Run:
//!   cargo run --release --locked --example async_bench -- 4 250000 64 5
//! Add a final implementation name (e.g. prescient-fixed) to isolate perf counters.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prescient::mpsc::fixed;
use tokio::sync::{Barrier, Notify};

const NAMES: [&str; 6] = [
    "prescient-fixed",
    "prescient-batch",
    "tokio-mpsc",
    "async-channel",
    "flume",
    "kanal",
];

#[derive(Clone, Copy)]
struct Config {
    producers: usize,
    messages: u64,
    per_shard_capacity: usize,
}

struct Run {
    elapsed: Duration,
    checksum: u128,
}

fn value(producer: usize, sequence: u64) -> u64 {
    ((producer as u64) << 48) | sequence
}

fn expected(config: Config) -> u128 {
    (0..config.producers)
        .map(|producer| {
            (0..config.messages)
                .map(|sequence| value(producer, sequence) as u128)
                .sum::<u128>()
        })
        .sum()
}

fn start_gate(producers: usize) -> (Arc<Barrier>, Arc<Notify>) {
    (
        Arc::new(Barrier::new(producers + 1)),
        Arc::new(Notify::new()),
    )
}

async fn prescient(config: Config) -> Run {
    let (producers, mut receiver) = fixed::channel::<u64>()
        .producers(config.producers)
        .capacity(config.per_shard_capacity)
        .r#async()
        .open()
        .unwrap();
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = producers
        .into_iter()
        .enumerate()
        .map(|(id, mut producer)| {
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                for sequence in 0..config.messages {
                    producer.send_async(value(id, sequence)).await.unwrap();
                }
            })
        })
        .collect();
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut checksum = 0_u128;
    for _ in 0..total {
        checksum += receiver.recv_async().await.unwrap() as u128;
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn prescient_batch(config: Config) -> Run {
    let (producers, mut receiver) = fixed::channel::<u64>()
        .producers(config.producers)
        .capacity(config.per_shard_capacity)
        .r#async()
        .open()
        .unwrap();
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = producers
        .into_iter()
        .enumerate()
        .map(|(id, mut producer)| {
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                producer
                    .send_batch_async((0..config.messages).map(|sequence| value(id, sequence)))
                    .await
                    .unwrap();
            })
        })
        .collect();
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut received = 0_u64;
    let mut checksum = 0_u128;
    let mut batch = Vec::with_capacity(256);
    while received < total {
        batch.clear();
        let count = receiver.recv_many_async(&mut batch, 256).await;
        assert!(
            count > 0,
            "batch channel closed before delivering every value"
        );
        received += count as u64;
        checksum += batch.iter().map(|&value| value as u128).sum::<u128>();
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn tokio_mpsc(config: Config) -> Run {
    let capacity = config.producers * config.per_shard_capacity;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<u64>(capacity);
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = (0..config.producers)
        .map(|id| {
            let sender = sender.clone();
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                for sequence in 0..config.messages {
                    sender.send(value(id, sequence)).await.unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut checksum = 0_u128;
    for _ in 0..total {
        checksum += receiver.recv().await.unwrap() as u128;
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn async_channel(config: Config) -> Run {
    let capacity = config.producers * config.per_shard_capacity;
    let (sender, receiver) = async_channel::bounded::<u64>(capacity);
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = (0..config.producers)
        .map(|id| {
            let sender = sender.clone();
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                for sequence in 0..config.messages {
                    sender.send(value(id, sequence)).await.unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut checksum = 0_u128;
    for _ in 0..total {
        checksum += receiver.recv().await.unwrap() as u128;
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn flume(config: Config) -> Run {
    let capacity = config.producers * config.per_shard_capacity;
    let (sender, receiver) = flume::bounded::<u64>(capacity);
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = (0..config.producers)
        .map(|id| {
            let sender = sender.clone();
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                for sequence in 0..config.messages {
                    sender.send_async(value(id, sequence)).await.unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut checksum = 0_u128;
    for _ in 0..total {
        checksum += receiver.recv_async().await.unwrap() as u128;
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn kanal(config: Config) -> Run {
    let capacity = config.producers * config.per_shard_capacity;
    let (sender, receiver) = kanal::bounded_async::<u64>(capacity);
    let (ready, start) = start_gate(config.producers);
    let tasks: Vec<_> = (0..config.producers)
        .map(|id| {
            let sender = sender.clone();
            let ready = ready.clone();
            let start = start.clone();
            tokio::spawn(async move {
                let released = start.notified();
                ready.wait().await;
                released.await;
                for sequence in 0..config.messages {
                    sender.send(value(id, sequence)).await.unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    ready.wait().await;
    let began = Instant::now();
    start.notify_waiters();
    let total = config.producers as u64 * config.messages;
    let mut checksum = 0_u128;
    for _ in 0..total {
        checksum += receiver.recv().await.unwrap() as u128;
    }
    for task in tasks {
        task.await.unwrap();
    }
    Run {
        elapsed: began.elapsed(),
        checksum,
    }
}

async fn run(name: &str, config: Config) -> Run {
    match name {
        "prescient-fixed" => prescient(config).await,
        "prescient-batch" => prescient_batch(config).await,
        "tokio-mpsc" => tokio_mpsc(config).await,
        "async-channel" => async_channel(config).await,
        "flume" => flume(config).await,
        "kanal" => kanal(config).await,
        _ => unreachable!(),
    }
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * percentile).round() as usize]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config = Config {
        producers: args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4),
        messages: args.get(2).and_then(|s| s.parse().ok()).unwrap_or(250_000),
        per_shard_capacity: args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64),
    };
    let samples: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(5);
    assert!(config.producers > 0 && config.messages > 0);
    assert!(config.per_shard_capacity.is_power_of_two());
    assert!(samples >= 3);
    // One optional implementation per process permits attributable perf counters.
    // Selection stays outside every timed transfer and message loop.
    let names: Vec<&str> = match args.get(5) {
        Some(name) => {
            assert!(
                NAMES.contains(&name.as_str()),
                "unknown implementation {name}"
            );
            vec![name.as_str()]
        }
        None => NAMES.to_vec(),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((config.producers + 1).min(12))
        .build()
        .unwrap();
    let expected = expected(config);
    let warm = Config {
        messages: (config.messages / 20).max(1_000),
        ..config
    };
    for &name in &names {
        let result = runtime.block_on(run(name, warm));
        black_box(result.checksum);
    }

    let mut results: BTreeMap<&str, Vec<f64>> = names
        .iter()
        .copied()
        .map(|name| (name, Vec::with_capacity(samples)))
        .collect();
    for sample in 0..samples {
        // Rotate order to distribute thermal/scheduler bias.
        for offset in 0..names.len() {
            let name = names[(sample + offset) % names.len()];
            let result = runtime.block_on(run(name, config));
            assert_eq!(result.checksum, expected, "{name} checksum");
            black_box(result.checksum);
            results.get_mut(name).unwrap().push(
                config.producers as f64 * config.messages as f64
                    / result.elapsed.as_secs_f64()
                    / 1_000_000.0,
            );
        }
    }

    println!(
        "async bounded fan-in: producers={} messages/producer={} per-shard-capacity={} samples={}",
        config.producers, config.messages, config.per_shard_capacity, samples
    );
    println!(
        "shared baselines use total capacity={}",
        config.producers * config.per_shard_capacity
    );
    println!(
        "{:<18} {:>10} {:>10} {:>10}",
        "channel", "p10", "median", "p90"
    );
    for &name in &names {
        let values = results.get_mut(name).unwrap();
        values.sort_by(f64::total_cmp);
        println!(
            "{name:<18} {:>8.2} M {:>8.2} M {:>8.2} M",
            percentile(values, 0.10),
            percentile(values, 0.50),
            percentile(values, 0.90),
        );
    }
}
