//! One implementation per process; equal aggregate bounded transport capacity.
//! Usage: compare IMPL PRODUCERS CONSUMERS PER_PRODUCER SHARD_CAPACITY SAMPLES
//!        [--batch N] [--cpus producer_cpus_then_consumer_cpus]
//! IMPL: mpsc-spin, mpsc-park, mpsc-pool, mpsc-dynamic, mpmc-ring,
//!       mpmc-locked, mpmc-array, mutex, mutex-sharded, crossbeam, flume, kanal, std
//! Prescient capacity is per producer; external capacity is producers * capacity.
//! All APIs here send/receive one u64 at a time. Prescient may stage internally.
#[path = "support/affinity.rs"]
mod affinity;
#[path = "support/mutex_queue.rs"]
mod mutex_queue;
#[path = "support/start_gate.rs"]
mod start_gate;
use std::time::Instant;

use prescient::mpmc::brokerless;
use prescient::mpsc;
use prescient::wait::{Park, Spin};

trait SendValue {
    fn send_value(&mut self, value: u64);
}
trait ReceiveValue {
    fn receive_value(&mut self) -> Option<u64>;
}

impl<K: mpsc::BlockingKernel> SendValue for mpsc::Producer<u64, K> {
    #[inline]
    fn send_value(&mut self, value: u64) {
        self.send(value).unwrap();
    }
}
impl<K: mpsc::BlockingKernel, S: mpsc::ShardState<u64, K>> ReceiveValue
    for mpsc::Receiver<u64, K, S>
{
    #[inline]
    fn receive_value(&mut self) -> Option<u64> {
        self.recv()
    }
}
impl SendValue for brokerless::Producer<u64> {
    #[inline]
    fn send_value(&mut self, value: u64) {
        assert!(self.send(value));
    }
}
impl ReceiveValue for brokerless::Consumer<u64> {
    #[inline]
    fn receive_value(&mut self) -> Option<u64> {
        self.recv()
    }
}

impl<R: brokerless::dynamic::Rings<u64, prescient::backend::Ring>> SendValue
    for brokerless::dynamic::Producer<u64, prescient::backend::Ring, R>
{
    #[inline]
    fn send_value(&mut self, value: u64) {
        assert!(self.send(value));
    }
}
impl<R: brokerless::dynamic::Rings<u64, prescient::backend::Ring>> ReceiveValue
    for brokerless::dynamic::Consumer<u64, prescient::backend::Ring, R>
{
    #[inline]
    fn receive_value(&mut self) -> Option<u64> {
        self.recv()
    }
}
macro_rules! external {
    ($sender:ty, $receiver:ty) => {
        impl SendValue for $sender {
            #[inline]
            fn send_value(&mut self, value: u64) {
                self.send(value).unwrap();
            }
        }
        impl ReceiveValue for $receiver {
            #[inline]
            fn receive_value(&mut self) -> Option<u64> {
                self.recv().ok()
            }
        }
    };
}
external!(
    crossbeam_channel::Sender<u64>,
    crossbeam_channel::Receiver<u64>
);
external!(flume::Sender<u64>, flume::Receiver<u64>);
external!(kanal::Sender<u64>, kanal::Receiver<u64>);
external!(
    std::sync::mpsc::SyncSender<u64>,
    std::sync::mpsc::Receiver<u64>
);

impl SendValue for mutex_queue::Sender<u64> {
    #[inline]
    fn send_value(&mut self, value: u64) {
        self.send(value).unwrap();
    }
}
impl ReceiveValue for mutex_queue::Receiver<u64> {
    #[inline]
    fn receive_value(&mut self) -> Option<u64> {
        self.recv()
    }
}
#[derive(Debug)]
struct Config {
    implementation: String,
    producers: usize,
    consumers: usize,
    per_producer: usize,
    capacity: usize,
    samples: usize,
    batch: usize,
    placement: affinity::Placement,
}

const USAGE: &str = "usage: compare IMPL PRODUCERS CONSUMERS PER_PRODUCER SHARD_CAPACITY SAMPLES [--batch N] [--cpus P0,P1,...,C0,C1,...]";

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        if args.len() < 6 {
            return Err(USAGE.into());
        }
        fn positive(text: &str) -> Result<usize, String> {
            let value = text
                .parse::<usize>()
                .map_err(|_| format!("invalid positive integer {text:?}"))?;
            if value == 0 {
                return Err("dimensions and samples must be positive".into());
            }
            Ok(value)
        }
        let producers = positive(&args[1])?;
        let consumers = positive(&args[2])?;
        let per_producer = positive(&args[3])?;
        let capacity = positive(&args[4])?;
        let samples = positive(&args[5])?;
        if !capacity.is_power_of_two() {
            return Err("capacity must be a nonzero power of two".into());
        }
        let implementation = args[0].clone();
        match implementation.as_str() {
            "mpsc-spin" | "mpsc-park" | "mpsc-pool" | "mpsc-dynamic" | "std" if consumers != 1 => {
                return Err("this implementation requires one consumer".into());
            }
            "mpsc-spin" | "mpsc-park" | "mpsc-pool" | "mpsc-dynamic" | "mpmc-ring"
            | "mpmc-locked" | "mpmc-array" | "mutex" | "mutex-sharded" | "crossbeam" | "flume"
            | "kanal" | "std" => {}
            _ => return Err(format!("unknown implementation {implementation:?}")),
        }
        if implementation == "mpsc-pool" && producers > 64 {
            return Err("mpsc-pool supports at most 64 producers".into());
        }
        let mut batch = None;
        let mut cpu_map = None;
        for option in args[6..].chunks(2) {
            if option.len() != 2 {
                return Err(format!("{} needs a value", option[0]));
            }
            match option[0].as_str() {
                "--batch" if batch.is_none() => batch = Some(positive(&option[1])?),
                "--cpus" if cpu_map.is_none() => cpu_map = Some(option[1].as_str()),
                _ => return Err(format!("unknown or repeated option {}", option[0])),
            }
        }
        let staged = implementation.starts_with("mpsc-")
            || implementation.starts_with("mpmc-")
            || implementation == "mutex"
            || implementation == "mutex-sharded";
        if batch.is_some() && !staged {
            return Err(
                "--batch configures Prescient/mutex staging, not external channel crates".into(),
            );
        }
        let batch = if staged { batch.unwrap_or(64) } else { 0 };
        let workers = producers
            .checked_add(consumers)
            .ok_or("worker count overflow")?;
        let total = producers
            .checked_mul(per_producer)
            .ok_or("message count overflow")?;
        total
            .checked_mul(samples.checked_add(1).ok_or("sample count overflow")?)
            .ok_or("total delivered work overflow")?;
        producers
            .checked_mul(capacity)
            .ok_or("transport capacity overflow")?;
        consumers
            .checked_mul(batch)
            .ok_or("staging limit overflow")?;
        Ok(Self {
            implementation,
            producers,
            consumers,
            per_producer,
            capacity,
            samples,
            batch,
            placement: affinity::Placement::new(cpu_map, workers)?,
        })
    }

    fn total(&self) -> usize {
        self.producers * self.per_producer
    }
    fn aggregate_capacity(&self) -> usize {
        self.producers * self.capacity
    }
    fn delivered(&self) -> usize {
        self.total() * (self.samples + 1)
    }
}

fn measure<P: SendValue + Send, R: ReceiveValue + Send>(
    config: &Config,
    make: impl Fn() -> (Vec<P>, Vec<R>),
) -> Result<(), String> {
    let total = config.total();
    let expected_sum = ((total as u128 * (total as u128 - 1)) / 2) as u64;
    let expected_xor = match (total as u64 - 1) % 4 {
        0 => total as u64 - 1,
        1 => 1,
        2 => total as u64,
        _ => 0,
    };
    let mut rates = Vec::with_capacity(config.samples);
    for _ in 0..=config.samples {
        let (producers, consumers) = make();
        assert_eq!(producers.len(), config.producers);
        assert_eq!(consumers.len(), config.consumers);
        let gate = start_gate::StartGate::default();
        let (elapsed, results) = std::thread::scope(|scope| -> Result<_, String> {
            // This guard drops BEFORE scope joins workers if spawning or affinity
            // fails. A partially populated, infallible barrier would deadlock.
            let _abort = gate.abort_on_drop();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let mut senders = Vec::with_capacity(config.producers);
            for (index, mut sender) in producers.into_iter().enumerate() {
                let gate = &gate;
                let ready = ready_tx.clone();
                senders.push(
                    std::thread::Builder::new()
                        .spawn_scoped(scope, move || {
                            let start = index * config.per_producer;
                            if !gate.enter(ready, config.placement.pin(index)) {
                                return;
                            }
                            for value in start..start + config.per_producer {
                                sender.send_value(value as u64);
                            }
                        })
                        .map_err(|e| format!("cannot spawn producer {index}: {e}"))?,
                );
            }
            let mut receivers = Vec::with_capacity(config.consumers);
            for (index, mut receiver) in consumers.into_iter().enumerate() {
                let gate = &gate;
                let ready = ready_tx.clone();
                receivers.push(
                    std::thread::Builder::new()
                        .spawn_scoped(scope, move || {
                            if !gate.enter(ready, config.placement.pin(config.producers + index)) {
                                return (0, 0, 0);
                            }
                            let mut count = 0usize;
                            let mut sum = 0u64;
                            let mut xor = 0u64;
                            while let Some(value) = receiver.receive_value() {
                                count += 1;
                                sum = sum.wrapping_add(value);
                                xor ^= value;
                            }
                            (count, sum, xor)
                        })
                        .map_err(|e| format!("cannot spawn consumer {index}: {e}"))?,
                );
            }
            drop(ready_tx);
            // Receive every setup result before starting the transfer timer.
            for _ in 0..config.producers + config.consumers {
                ready_rx
                    .recv()
                    .map_err(|_| "worker exited during setup")??;
            }
            let start = Instant::now();
            gate.open();
            for sender in senders {
                sender.join().map_err(|_| "producer panicked")?;
            }
            let results = receivers
                .into_iter()
                .map(|r| r.join().map_err(|_| "consumer panicked"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((start.elapsed(), results))
        })?;
        let mut count = 0usize;
        let mut sum = 0u64;
        let mut xor = 0u64;
        for result in results {
            count += result.0;
            sum = sum.wrapping_add(result.1);
            xor ^= result.2;
        }
        assert_eq!(count, total);
        assert_eq!(sum, expected_sum);
        assert_eq!(xor, expected_xor);
        rates.push(total as f64 / elapsed.as_secs_f64() / 1e6);
    }
    rates.remove(0); // The first complete, verified workload is the warmup.
    let ordered = rates
        .iter()
        .map(|r| format!("{r:.6}"))
        .collect::<Vec<_>>()
        .join(",");
    rates.sort_by(f64::total_cmp);
    let median = (rates[(rates.len() - 1) / 2] + rates[rates.len() / 2]) / 2.0;
    println!(
        "impl={};producers={};consumers={};messages={total};samples={};warmups=1;delivered={};transport_slots={};batch={};staging_limit={};worker_cpus={};median_mps={median:.6};min_mps={:.6};max_mps={:.6};sample_mps={ordered}",
        config.implementation,
        config.producers,
        config.consumers,
        config.samples,
        config.delivered(),
        config.aggregate_capacity(),
        config.batch,
        config.consumers * config.batch,
        config.placement.label(),
        rates[0],
        rates[rates.len() - 1],
    );
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("{USAGE}");
        return;
    }
    if let Err(error) = Config::parse(&args).and_then(|config| run(&config)) {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
}

fn run(c: &Config) -> Result<(), String> {
    macro_rules! shared {
        ($factory:expr) => {
            measure(c, || {
                let (sender, receiver) = $factory;
                (
                    (0..c.producers).map(|_| sender.clone()).collect(),
                    (0..c.consumers).map(|_| receiver.clone()).collect(),
                )
            })
        };
    }
    // Dispatch surrounds the entire workload. No implementation enum or
    // function pointer is consulted in any message loop.
    match c.implementation.as_str() {
        "mpsc-spin" => {
            assert_eq!(c.consumers, 1);
            measure(c, || {
                let (tx, rx) = mpsc::fixed::channel::<u64>()
                    .wait::<Spin>()
                    .producers(c.producers)
                    .capacity(c.capacity)
                    .batch(c.batch)
                    .open()
                    .unwrap();
                (tx, vec![rx])
            })
        }
        "mpsc-park" => {
            assert_eq!(c.consumers, 1);
            measure(c, || {
                let (tx, rx) = mpsc::fixed::channel::<u64>()
                    .wait::<Park>()
                    .producers(c.producers)
                    .capacity(c.capacity)
                    .batch(c.batch)
                    .open()
                    .unwrap();
                (tx, vec![rx])
            })
        }
        "mpsc-pool" => {
            assert_eq!(c.consumers, 1);
            measure(c, || {
                let (pool, rx) = mpsc::pool::channel::<u64>()
                    .wait::<Spin>()
                    .max_producers(c.producers)
                    .capacity(c.capacity)
                    .batch(c.batch)
                    .open()
                    .unwrap();
                let tx = (0..c.producers).map(|_| pool.claim().unwrap()).collect();
                (tx, vec![rx])
            })
        }
        "mpsc-dynamic" => {
            assert_eq!(c.consumers, 1);
            measure(c, || {
                let (registrar, rx) = mpsc::dynamic::channel::<u64>()
                    .wait::<Spin>()
                    .capacity(c.capacity)
                    .batch(c.batch)
                    .open()
                    .unwrap();
                let tx = (0..c.producers).map(|_| registrar.register()).collect();
                (tx, vec![rx])
            })
        }
        "mpmc-ring" => measure(c, || {
            brokerless::channel::<u64>()
                .producers(c.producers)
                .consumers(c.consumers)
                .capacity(c.capacity)
                .batch(c.batch)
                .open()
                .unwrap()
        }),
        "mpmc-locked" => measure(c, || {
            let (registrar, consumers) = brokerless::dynamic::locked::<u64>()
                .consumers(c.consumers)
                .capacity(c.capacity)
                .batch(c.batch)
                .open()
                .unwrap();
            let producers = (0..c.producers).map(|_| registrar.register()).collect();
            (producers, consumers)
        }),
        "mpmc-array" => measure(c, || {
            let (registrar, consumers) = brokerless::dynamic::array::<u64>(c.producers)
                .consumers(c.consumers)
                .capacity(c.capacity)
                .batch(c.batch)
                .open()
                .unwrap();
            let producers = (0..c.producers).map(|_| registrar.register()).collect();
            (producers, consumers)
        }),
        "mutex" => measure(c, || {
            mutex_queue::bounded(c.aggregate_capacity(), c.producers, c.consumers, c.batch)
        }),
        "mutex-sharded" => measure(c, || {
            mutex_queue::sharded(c.capacity, c.producers, c.consumers, c.batch)
        }),
        "crossbeam" => shared!(crossbeam_channel::bounded::<u64>(c.aggregate_capacity())),
        "flume" => shared!(flume::bounded::<u64>(c.aggregate_capacity())),
        "kanal" => shared!(kanal::bounded::<u64>(c.aggregate_capacity())),
        "std" => {
            assert_eq!(c.consumers, 1);
            measure(c, || {
                let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(c.aggregate_capacity());
                ((0..c.producers).map(|_| tx.clone()).collect(), vec![rx])
            })
        }
        other => panic!("unknown implementation {other:?}"),
    }
}
#[cfg(test)]
mod config_tests {
    use super::*;

    fn parse(text: &str) -> Result<Config, String> {
        Config::parse(
            &text
                .split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn mutex_matches_capacity_staging_and_work_contract() {
        for implementation in ["mutex", "mutex-sharded"] {
            let c = parse(&format!("{implementation} 3 3 100 4096 5")).unwrap();
            assert_eq!(c.batch, 64);
            assert_eq!(c.delivered(), 1800);
            assert_eq!(c.aggregate_capacity(), 12288);
            assert_eq!(
                parse(&format!("{implementation} 3 3 100 4096 5 --batch 1"))
                    .unwrap()
                    .batch,
                1
            );
            assert!(parse(&format!("{implementation} 3 3 100 4096 5 --batch 0")).is_err());
            run(&parse(&format!("{implementation} 3 3 100 1 1")).unwrap()).unwrap();
        }
    }
    #[test]
    fn dynamic_mpmc_uses_the_same_batch_and_work_contract() {
        for implementation in ["mpmc-locked", "mpmc-array"] {
            let c = parse(&format!("{implementation} 3 3 100 64 5 --batch 1")).unwrap();
            assert_eq!(c.batch, 1);
            assert_eq!(c.delivered(), 1800);
            assert_eq!(c.aggregate_capacity(), 192);
        }
    }
    #[test]
    fn batch_and_work_accounting_are_explicit() {
        let c = parse("mpmc-ring 3 2 100 64 5").unwrap();
        assert_eq!(c.batch, 64);
        assert_eq!(c.total(), 300);
        assert_eq!(c.delivered(), 1800);
        assert_eq!(c.aggregate_capacity(), 192);
        assert_eq!(c.placement.label(), "inherited");
        assert_eq!(parse("mpsc-spin 3 1 100 64 5 --batch 1").unwrap().batch, 1);
        assert_eq!(parse("crossbeam 3 2 100 64 5").unwrap().batch, 0);
    }

    #[test]
    fn invalid_shapes_and_options_fail_before_spawning() {
        for args in [
            "",
            "mpmc-ring 2",
            "unknown 2 2 1 1 1",
            "mpmc-ring 0 2 1 1 1",
            "mpmc-ring 2 0 1 1 1",
            "mpmc-ring 2 2 0 1 1",
            "mpmc-ring 2 2 1 3 1",
            "mpmc-ring 2 2 1 1 0",
            "mpsc-spin 2 2 1 1 1",
            "std 2 2 1 1 1",
            "mpsc-pool 65 1 1 1 1",
            "mpmc-ring 2 2 1 1 1 --batch 0",
            "mpmc-ring 2 2 1 1 1 --batch",
            "mpmc-ring 2 2 1 1 1 --batch 1 --batch 2",
            "mpmc-ring 2 2 1 1 1 --cpus 0",
            "mpmc-ring 2 2 1 1 1 --cpus 0,0,0,0 --cpus 0,0,0,0",
            "mpmc-ring 2 2 1 1 1 --unknown 1",
            "crossbeam 2 2 1 1 1 --batch 1",
        ] {
            assert!(parse(args).is_err(), "{args}");
        }
        let max = usize::MAX;
        for args in [
            format!("mpmc-ring {max} 1 1 1 1"),
            format!("mpmc-ring 2 1 {max} 1 1"),
            format!("mpmc-ring 1 1 1 1 {max}"),
            format!("mpmc-ring 1 1 {max} 1 1"),
            format!("mpmc-ring 1 2 1 1 1 --batch {max}"),
        ] {
            assert!(parse(&args).is_err(), "{args}");
        }
    }
}
