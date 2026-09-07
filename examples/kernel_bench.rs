//! Receive-kernel diagnostics on dynamic Locked/Ring with owned producer batches.
//! direct and loop-fn use the same receive loop; item-fn dispatches per message.
//! drain variants execute generic callbacks under the ring claim, bypassing staging.
//! batch transfers owned staging and validates its slice after releasing the claim.
//! Full payload creation/validation are timed. Pointer black_box only models opaque
//! runtime selection before the start gate; it is never applied to message data.
#[path = "support/affinity.rs"]
mod affinity;
#[path = "support/start_gate.rs"]
mod start_gate;
use prescient::{
    backend::{Batch, Ring},
    mpmc::brokerless,
};
use std::time::Instant;

trait SendBatch<const N: usize> {
    fn send_batch_value(&mut self, source: &mut Batch<[u8; N]>);
}
trait ReceiveValue<const N: usize> {
    fn receive_value(&mut self) -> Option<[u8; N]>;
    fn receive_batch(&mut self, out: &mut Batch<[u8; N]>) -> usize;
    fn consume(&mut self, batch: usize, f: impl FnMut([u8; N]));
}
impl<const N: usize, R: brokerless::dynamic::Rings<[u8; N], Ring>> SendBatch<N>
    for brokerless::dynamic::Producer<[u8; N], Ring, R>
{
    #[inline]
    fn send_batch_value(&mut self, source: &mut Batch<[u8; N]>) {
        assert!(self.send_batch(source));
    }
}
impl<const N: usize, R: brokerless::dynamic::Rings<[u8; N], Ring>> ReceiveValue<N>
    for brokerless::dynamic::Consumer<[u8; N], Ring, R>
{
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
    #[inline]
    fn receive_batch(&mut self, out: &mut Batch<[u8; N]>) -> usize {
        self.recv_batch(out)
    }
    #[inline]
    fn consume(&mut self, batch: usize, f: impl FnMut([u8; N])) {
        self.for_each(batch, f);
    }
}
type Stats = (usize, u64, u64, usize);
#[inline]
fn observe<const N: usize>(
    stats: &mut Stats,
    value: &[u8; N],
    fixtures: &[[u8; N]; 16],
    total: usize,
) {
    let (id, bad) = inspect(value, fixtures, total);
    stats.0 += 1;
    stats.1 = stats.1.wrapping_add(id);
    stats.2 ^= id;
    stats.3 += bad;
}
#[inline]
fn consume_direct<const N: usize, C: ReceiveValue<N>>(
    rx: &mut C,
    fixtures: &[[u8; N]; 16],
    total: usize,
) -> Stats {
    let mut stats = (0, 0, 0, 0);
    while let Some(value) = rx.receive_value() {
        observe(&mut stats, &value, fixtures, total);
    }
    stats
}
#[inline]
fn consume_drained<const N: usize, C: ReceiveValue<N>>(
    rx: &mut C,
    fixtures: &[[u8; N]; 16],
    total: usize,
    batch: usize,
) -> Stats {
    let mut stats = (0, 0, 0, 0);
    rx.consume(batch, |value| observe(&mut stats, &value, fixtures, total));
    stats
}
#[inline]
fn consume_batched<const N: usize, C: ReceiveValue<N>>(
    rx: &mut C,
    fixtures: &[[u8; N]; 16],
    total: usize,
) -> Stats {
    let mut stats = (0, 0, 0, 0);
    let mut values = Batch::new();
    while rx.receive_batch(&mut values) != 0 {
        for value in values.as_slice() {
            observe(&mut stats, value, fixtures, total);
        }
    }
    stats
}
const USAGE: &str = "kernel_bench MODE BYTES PRODUCERS CONSUMERS PER_PRODUCER CAPACITY RECEIVE_BATCH SAMPLES PRODUCER_BATCH [--cpus P0,P1,...,C0,C1,...]; MODE=direct|item-fn|loop-fn|drain|drain-loop-fn|batch; BYTES=8|33|64|256|257|4096";
struct Config {
    mode: String,
    bytes: usize,
    producers: usize,
    consumers: usize,
    per: usize,
    capacity: usize,
    batch: usize,
    samples: usize,
    producer_batch: usize,
    total: usize,
    delivered: usize,
    placement: affinity::Placement,
}
impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        if args.len() != 9 && args.len() != 11 {
            return Err(USAGE.into());
        }
        let number = |i: usize| -> Result<usize, String> {
            let value = args[i].parse::<usize>().map_err(|_| USAGE)?;
            if value == 0 {
                return Err("all dimensions must be positive".into());
            }
            Ok(value)
        };
        let (bytes, producers, consumers, per, capacity, batch, samples) = (
            number(1)?,
            number(2)?,
            number(3)?,
            number(4)?,
            number(5)?,
            number(6)?,
            number(7)?,
        );
        if ![8, 33, 64, 256, 257, 4096].contains(&bytes) {
            return Err("unsupported byte size".into());
        }
        if !capacity.is_power_of_two() {
            return Err("capacity must be a power of two".into());
        }
        match args[0].as_str() {
            "direct" | "item-fn" | "loop-fn" | "drain" | "drain-loop-fn" | "batch" => {}
            _ => return Err("unknown receive kernel".into()),
        }
        let producer_batch = number(8)?;
        let overflow = "workload dimensions overflow";
        let total = producers.checked_mul(per).ok_or(overflow)?;
        let delivered = total
            .checked_mul(samples.checked_add(1).ok_or(overflow)?)
            .ok_or(overflow)?;
        let workers = producers.checked_add(consumers).ok_or(overflow)?;
        for count in [
            producers.checked_mul(capacity),
            consumers.checked_mul(batch),
            producers.checked_mul(producer_batch),
            Some(delivered),
        ] {
            let byte_count = count.and_then(|n| n.checked_mul(bytes)).ok_or(overflow)?;
            if byte_count > isize::MAX as usize {
                return Err(overflow.into());
            }
        }
        let cpu_map = if args.len() == 11 {
            if args[9] != "--cpus" {
                return Err(USAGE.into());
            }
            Some(args[10].as_str())
        } else {
            None
        };
        Ok(Self {
            mode: args[0].clone(),
            bytes,
            producers,
            consumers,
            per,
            capacity,
            batch,
            samples,
            producer_batch,
            total,
            delivered,
            placement: affinity::Placement::new(cpu_map, workers)?,
        })
    }
}
#[path = "support/payload.rs"]
mod payload;
use payload::{fixtures, inspect, packet, xor_prefix};
fn measure<const N: usize, P: SendBatch<N> + Send, C: ReceiveValue<N> + Send>(
    c: &Config,
    mut factory: impl FnMut() -> (Vec<P>, Vec<C>),
) -> Result<(), String> {
    let fixtures = &fixtures::<N>();
    let mut rates = Vec::with_capacity(c.samples + 1);
    for _ in 0..=c.samples {
        let (producers, consumers) = factory();
        let gate = start_gate::StartGate::default();
        let (elapsed, results) = std::thread::scope(|scope| -> Result<_, String> {
            let _abort = gate.abort_on_drop();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let mut senders = Vec::with_capacity(c.producers);
            for (index, mut sender) in producers.into_iter().enumerate() {
                let ready = ready_tx.clone();
                let gate = &gate;
                senders.push(
                    std::thread::Builder::new()
                        .spawn_scoped(scope, move || {
                            if !gate.enter(ready, c.placement.pin(index)) {
                                return;
                            }
                            let start = index * c.per;
                            let mut source = Batch::new();
                            let mut next = start;
                            while next < start + c.per {
                                source.clear();
                                let count = c.producer_batch.min(start + c.per - next);
                                for id in next..next + count {
                                    source.push_back(packet(id as u64, fixtures));
                                }
                                sender.send_batch_value(&mut source);
                                assert!(source.is_empty());
                                next += count;
                            }
                        })
                        .map_err(|e| format!("cannot spawn producer: {e}"))?,
                );
            }
            let mut receivers = Vec::with_capacity(c.consumers);
            for (index, mut receiver) in consumers.into_iter().enumerate() {
                let ready = ready_tx.clone();
                let gate = &gate;
                receivers.push(
                    std::thread::Builder::new()
                        .spawn_scoped(scope, move || {
                            // Model a stable but compiler-opaque runtime selection.
                            // Resolve before the start gate, never inside the hot loop.
                            let item_fn = std::hint::black_box(
                                C::receive_value as fn(&mut C) -> Option<[u8; N]>,
                            );
                            let loop_fn = std::hint::black_box(
                                consume_direct::<N, C>
                                    as fn(&mut C, &[[u8; N]; 16], usize) -> Stats,
                            );
                            let drain_fn = std::hint::black_box(
                                consume_drained::<N, C>
                                    as fn(&mut C, &[[u8; N]; 16], usize, usize) -> Stats,
                            );
                            if !gate.enter(ready, c.placement.pin(c.producers + index)) {
                                return (0, 0u64, 0u64, 0);
                            }
                            // One outer selection; none of these kernels reads mode.
                            match c.mode.as_str() {
                                "direct" => consume_direct(&mut receiver, fixtures, c.total),
                                "batch" => consume_batched(&mut receiver, fixtures, c.total),
                                "item-fn" => {
                                    let mut stats = (0, 0, 0, 0);
                                    while let Some(value) = item_fn(&mut receiver) {
                                        observe(&mut stats, &value, fixtures, c.total);
                                    }
                                    stats
                                }
                                "loop-fn" => loop_fn(&mut receiver, fixtures, c.total),
                                "drain" => {
                                    consume_drained(&mut receiver, fixtures, c.total, c.batch)
                                }
                                "drain-loop-fn" => {
                                    drain_fn(&mut receiver, fixtures, c.total, c.batch)
                                }
                                _ => unreachable!(),
                            }
                        })
                        .map_err(|e| format!("cannot spawn consumer: {e}"))?,
                );
            }
            drop(ready_tx);
            for _ in 0..c.producers + c.consumers {
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
        let (mut count, mut sum, mut xor, mut errors) = (0, 0u64, 0u64, 0);
        for (n, s, x, e) in results {
            count += n;
            sum = sum.wrapping_add(s);
            xor ^= x;
            errors += e;
        }
        assert_eq!(count, c.total);
        assert_eq!(errors, 0);
        assert_eq!(sum, ((c.total as u128 * (c.total - 1) as u128) / 2) as u64);
        assert_eq!(xor, xor_prefix((c.total - 1) as u64));
        rates.push(c.total as f64 / elapsed.as_secs_f64() / 1e6);
    }
    rates.remove(0);
    let ordered = rates
        .iter()
        .map(|v| format!("{v:.6}"))
        .collect::<Vec<_>>()
        .join(",");
    rates.sort_by(f64::total_cmp);
    let median = (rates[(rates.len() - 1) / 2] + rates[rates.len() / 2]) / 2.0;
    println!(
        "impl={};bytes={N};producers={};consumers={};messages={};samples={};warmups=1;delivered={};capacity={};batch={};producer_batch={};worker_cpus={};median_mps={median:.6};median_gbps={:.6};min_mps={:.6};max_mps={:.6};sample_mps={ordered}",
        c.mode,
        c.producers,
        c.consumers,
        c.total,
        c.samples,
        c.delivered,
        c.capacity,
        c.batch,
        c.producer_batch,
        c.placement.label(),
        median * N as f64 / 1000.0,
        rates[0],
        rates[rates.len() - 1]
    );
    Ok(())
}
fn run<const N: usize>(c: &Config) -> Result<(), String> {
    measure(c, || {
        let (registrar, rx) = brokerless::dynamic::locked::<[u8; N]>()
            .consumers(c.consumers)
            .capacity(c.capacity)
            .batch(c.batch)
            .open()
            .unwrap();
        ((0..c.producers).map(|_| registrar.register()).collect(), rx)
    })
}
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--help"] {
        println!("{USAGE}");
        return;
    }
    let result = Config::parse(&args).and_then(|c| match c.bytes {
        8 => run::<8>(&c),
        33 => run::<33>(&c),
        64 => run::<64>(&c),
        256 => run::<256>(&c),
        257 => run::<257>(&c),
        4096 => run::<4096>(&c),
        _ => unreachable!(),
    });
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn config(s: &str) -> Result<Config, String> {
        Config::parse(&s.split_whitespace().map(str::to_string).collect::<Vec<_>>())
    }
    #[test]
    fn validates_every_byte() {
        let f = fixtures::<257>();
        for id in 0..32 {
            let value = packet(id, &f);
            assert_eq!(inspect(&value, &f, 32), (id, 0));
            for index in 0..257 {
                let mut changed = value;
                changed[index] ^= 128;
                let (new_id, bad) = inspect(&changed, &f, 32);
                assert!(new_id != id || bad != 0, "byte {index}");
            }
        }
    }
    #[test]
    fn rejects_invalid_dimensions() {
        for bad in [
            "bad 64 1 1 10 1 1 1 64",
            "bad-kernel 64 1 2 10 1 1 1 64",
            "direct 65 1 1 10 1 1 1 64",
            "direct 64 1 1 10 3 1 1 64",
            "direct 64 0 1 10 1 1 1 64",
            "direct 64 1 1 10 1 1 0 64",
            "direct 64 1 1 10 1 1 1 64 --typo 0,1",
        ] {
            assert!(config(bad).is_err(), "{bad}");
        }
        assert!(config(&format!("direct 64 2 1 {} 1 1 1 64", usize::MAX)).is_err());
        assert!(config(&format!("direct 64 1 1 1 1 1 {} 64", usize::MAX)).is_err());
    }
    #[test]
    fn tiny_and_wrapping_transports_deliver_complete_payloads() {
        for mode in [
            "direct",
            "item-fn",
            "loop-fn",
            "drain",
            "drain-loop-fn",
            "batch",
        ] {
            for cap in [1, 4] {
                let c = config(&format!("{mode} 33 2 1 97 {cap} 3 1 7")).unwrap();
                run::<33>(&c).unwrap();
            }
        }
    }
}
