//! Byte-complete owned-message throughput, including payload construction/validation.
//! Setup/fixtures are outside timing; staging growth and worker completion are timed.
//! One verified full warmup precedes samples. Dispatch is outside the message loops.
//! Shared competitors use aggregate capacity = producers * capacity and native APIs.
//! Prescient batch modes also assemble owned producer batches of `BATCH` values.
//! Owned-batch modes receive owned batches and process their slices after releasing
//! the claim.
//! block-handoff sends whole preallocated blocks, including final recycling in timing.
//! Its CAPACITY bounds total payload slots per producer; BATCH is messages/block.
//! crossbeam-block-handoff uses the same pool/recycling but a shared Crossbeam forward queue.
//! `leased` uses ownership leases; `leased-direct` constructs values in destination
//! storage.
//! Block-handoff variants are diagnostic controls, not public channel engines.
//! Value-return modes provide scalar-construction controls.
//! Pool allocation happens during setup; value-copy staging growth remains inside
//! timing.
//! These are sustained-throughput comparisons, not equivalent ordering/wait/latency claims.
#[path = "support/affinity.rs"]
mod affinity;
#[path = "support/block_exchange.rs"]
mod block_exchange;
#[path = "support/mutex_queue.rs"]
mod mutex_queue;
#[path = "support/start_gate.rs"]
mod start_gate;
#[path = "support/worker_metrics.rs"]
mod worker_metrics;
use prescient::{
    backend::{Backend, Batch, Seg},
    mpmc::{brokerless, lanes},
    mpsc,
    wait::Spin,
};
use std::mem::MaybeUninit;
use std::time::Instant;

trait SendValue<const N: usize> {
    fn send_value(&mut self, value: [u8; N]);
    #[inline]
    fn send_values(&mut self, start: usize, end: usize, fixtures: &[[u8; N]; 16]) {
        for id in start..end {
            self.send_value(packet(id as u64, fixtures));
        }
    }
}
trait ReceiveValue<const N: usize> {
    fn receive_value(&mut self) -> Option<[u8; N]>;
    #[inline]
    fn consume_values(&mut self, mut f: impl FnMut(&[u8; N])) {
        while let Some(value) = self.receive_value() {
            f(&value);
        }
    }
}
trait ReceiveOwned<const N: usize> {
    fn receive_owned(&mut self, out: &mut Batch<[u8; N]>) -> usize;
}
impl<const N: usize, B: Backend> ReceiveOwned<N> for brokerless::Consumer<[u8; N], B> {
    #[inline]
    fn receive_owned(&mut self, out: &mut Batch<[u8; N]>) -> usize {
        self.recv_batch(out)
    }
}
impl<const N: usize, B: Backend> ReceiveOwned<N> for lanes::Consumer<[u8; N], B> {
    #[inline]
    fn receive_owned(&mut self, out: &mut Batch<[u8; N]>) -> usize {
        self.recv_batch(out)
    }
}
impl<const N: usize, B: Backend, R: brokerless::dynamic::Rings<[u8; N], B>> ReceiveOwned<N>
    for brokerless::dynamic::Consumer<[u8; N], B, R>
{
    #[inline]
    fn receive_owned(&mut self, out: &mut Batch<[u8; N]>) -> usize {
        self.recv_batch(out)
    }
}
struct OwnedReceiver<C>(C);
impl<const N: usize, C: ReceiveValue<N> + ReceiveOwned<N>> ReceiveValue<N> for OwnedReceiver<C> {
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.0.receive_value()
    }
    #[inline]
    fn consume_values(&mut self, mut f: impl FnMut(&[u8; N])) {
        let mut values = Batch::new();
        while self.0.receive_owned(&mut values) != 0 {
            for value in values.as_slice() {
                f(value);
            }
        }
    }
}
impl<const N: usize, B: Backend> SendValue<N> for brokerless::Producer<[u8; N], B> {
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        assert!(self.send(value));
    }
}
impl<const N: usize, B: Backend> ReceiveValue<N> for brokerless::Consumer<[u8; N], B> {
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
}
impl<const N: usize, B: Backend> SendValue<N> for lanes::Producer<[u8; N], B> {
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        assert!(self.send(value));
    }
}
impl<const N: usize, B: Backend> ReceiveValue<N> for lanes::Consumer<[u8; N], B> {
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
}
impl<const N: usize, B: Backend, R: brokerless::dynamic::Rings<[u8; N], B>> SendValue<N>
    for brokerless::dynamic::Producer<[u8; N], B, R>
{
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        assert!(self.send(value));
    }
}
impl<const N: usize, B: Backend, R: brokerless::dynamic::Rings<[u8; N], B>> ReceiveValue<N>
    for brokerless::dynamic::Consumer<[u8; N], B, R>
{
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
}
impl<const N: usize, K: mpsc::BlockingKernel> SendValue<N> for mpsc::Producer<[u8; N], K> {
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        self.send(value).unwrap();
    }
}
impl<const N: usize, K: mpsc::BlockingKernel, S: mpsc::ShardState<[u8; N], K>> ReceiveValue<N>
    for mpsc::Receiver<[u8; N], K, S>
{
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
}

trait SendOwned<const N: usize> {
    fn send_owned(&mut self, source: &mut Batch<[u8; N]>);
}
impl<const N: usize, B: Backend> SendOwned<N> for brokerless::Producer<[u8; N], B> {
    #[inline]
    fn send_owned(&mut self, source: &mut Batch<[u8; N]>) {
        assert!(self.send_batch(source));
    }
}
impl<const N: usize, B: Backend> SendOwned<N> for lanes::Producer<[u8; N], B> {
    #[inline]
    fn send_owned(&mut self, source: &mut Batch<[u8; N]>) {
        assert!(self.send_batch(source));
    }
}
impl<const N: usize, B: Backend, R: brokerless::dynamic::Rings<[u8; N], B>> SendOwned<N>
    for brokerless::dynamic::Producer<[u8; N], B, R>
{
    #[inline]
    fn send_owned(&mut self, source: &mut Batch<[u8; N]>) {
        assert!(self.send_batch(source));
    }
}
struct OwnedBatch<P>(P, usize);
impl<const N: usize, P: SendValue<N> + SendOwned<N>> SendValue<N> for OwnedBatch<P> {
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        self.0.send_value(value);
    }
    fn send_values(&mut self, start: usize, end: usize, fixtures: &[[u8; N]; 16]) {
        let mut source = Batch::new();
        let mut next = start;
        while next < end {
            source.clear();
            let count = self.1.min(end - next);
            for id in next..next + count {
                source.push_back(packet(id as u64, fixtures));
            }
            self.0.send_owned(&mut source);
            assert!(source.is_empty());
            next += count;
        }
    }
}
macro_rules! external {
    ($sender:ty, $receiver:ty) => {
        impl<const N: usize> SendValue<N> for $sender {
            #[inline]
            fn send_value(&mut self, value: [u8; N]) {
                self.send(value).unwrap();
            }
        }
        impl<const N: usize> ReceiveValue<N> for $receiver {
            #[inline]
            fn receive_value(&mut self) -> Option<[u8; N]> {
                self.recv().ok()
            }
        }
    };
}
external!(
    crossbeam_channel::Sender<[u8; N]>,
    crossbeam_channel::Receiver<[u8; N]>
);
external!(kanal::Sender<[u8; N]>, kanal::Receiver<[u8; N]>);
external!(flume::Sender<[u8; N]>, flume::Receiver<[u8; N]>);
impl<const N: usize> SendValue<N> for mutex_queue::Sender<[u8; N]> {
    #[inline]
    fn send_value(&mut self, value: [u8; N]) {
        self.send(value).unwrap();
    }
}
impl<const N: usize> ReceiveValue<N> for mutex_queue::Receiver<[u8; N]> {
    #[inline]
    fn receive_value(&mut self) -> Option<[u8; N]> {
        self.recv()
    }
}

impl<const N: usize, P: block_exchange::ForwardSend<block_exchange::Block<[u8; N]>>> SendValue<N>
    for block_exchange::Sender<[u8; N], P>
{
    fn send_value(&mut self, _: [u8; N]) {
        unreachable!("block diagnostic uses the whole-workload sender");
    }
    fn send_values(&mut self, start: usize, end: usize, fixtures: &[[u8; N]; 16]) {
        self.send_with(start, end, |values, ids| {
            for id in ids {
                values.push(packet(id as u64, fixtures));
            }
        })
        .unwrap();
    }
}
impl<const N: usize, C: block_exchange::ForwardReceive<block_exchange::Block<[u8; N]>>>
    ReceiveValue<N> for block_exchange::Receiver<[u8; N], C>
{
    fn receive_value(&mut self) -> Option<[u8; N]> {
        unreachable!("block diagnostic uses borrowed block consumption");
    }
    #[inline]
    fn consume_values(&mut self, mut f: impl FnMut(&[u8; N])) {
        self.consume(|values| {
            for value in values {
                f(value);
            }
        })
        .unwrap();
    }
}

// One destination-construction routine is shared by all direct-fill controls.
// The returned reference points into the pool, not a value-return stack temporary.
#[inline]
fn fill_packets<const N: usize>(
    slots: &mut [MaybeUninit<[u8; N]>],
    start: usize,
    fixtures: &[[u8; N]; 16],
) {
    for (offset, slot) in slots.iter_mut().enumerate() {
        let id = start + offset;
        let value = slot.write(fixtures[id & 15]);
        value[..8].copy_from_slice(&(id as u64).to_le_bytes());
    }
}

struct LeasedProducer<const N: usize, const DIRECT: bool>(brokerless::leased::Producer<[u8; N]>);
impl<const N: usize, const DIRECT: bool> SendValue<N> for LeasedProducer<N, DIRECT> {
    fn send_value(&mut self, _: [u8; N]) {
        unreachable!("leased mode publishes whole blocks");
    }
    fn send_values(&mut self, mut start: usize, end: usize, fixtures: &[[u8; N]; 16]) {
        while start < end {
            let mut block = self.0.reserve().expect("consumers disconnected");
            let count = block.capacity().min(end - start);
            // Const-generic construction choice: no runtime configuration dispatch.
            if DIRECT {
                fill_packets(&mut block.spare_capacity_mut()[..count], start, fixtures);
                // SAFETY: fill_packets initialized exactly this exclusive spare prefix.
                unsafe { block.advance_initialized(count) };
            } else {
                for id in start..start + count {
                    block.push(packet(id as u64, fixtures)).unwrap();
                }
            }
            assert_eq!(block.commit().expect("block publication failed"), count);
            start += count;
        }
        // Match the diagnostic block adapters: include final recycling in timing.
        assert!(self.0.flush());
    }
}
impl<const N: usize> ReceiveValue<N> for brokerless::leased::Consumer<[u8; N]> {
    fn receive_value(&mut self) -> Option<[u8; N]> {
        unreachable!("leased mode consumes borrowed blocks");
    }
    #[inline]
    fn consume_values(&mut self, mut f: impl FnMut(&[u8; N])) {
        while let Some(block) = self.recv() {
            for value in block.as_slice() {
                f(value);
            }
        }
    }
}

struct DirectBlock<P>(P);
impl<const N: usize, P: block_exchange::ForwardSend<block_exchange::Block<[u8; N]>>> SendValue<N>
    for DirectBlock<block_exchange::Sender<[u8; N], P>>
{
    fn send_value(&mut self, _: [u8; N]) {
        unreachable!("direct block control publishes whole blocks");
    }
    fn send_values(&mut self, start: usize, end: usize, fixtures: &[[u8; N]; 16]) {
        self.0
            .send_with(start, end, |values, ids| {
                let count = ids.len();
                assert!(values.is_empty());
                fill_packets(
                    &mut values.spare_capacity_mut()[..count],
                    ids.start,
                    fixtures,
                );
                // SAFETY: fill_packets initialized this bounded, exclusively owned prefix.
                unsafe { values.set_len(count) };
            })
            .unwrap();
    }
}

fn pooled_mode(mode: &str) -> bool {
    matches!(
        mode,
        "leased"
            | "leased-direct"
            | "block-handoff"
            | "block-handoff-direct"
            | "crossbeam-block-handoff"
            | "crossbeam-block-handoff-direct"
    )
}

fn supports_automatic_capacity(mode: &str) -> bool {
    matches!(mode, "leased" | "leased-direct" | "mpsc-spin") || mode.starts_with("mpmc-")
}
const USAGE: &str = "payload_bench MODE BYTES PRODUCERS CONSUMERS PER_PRODUCER CAPACITY BATCH SAMPLES [--capacity-mode explicit|automatic] [--cpus P0,P1,...,C0,C1,...] [--worker-metrics off|duty|perf]; MODE=leased|leased-direct|block-handoff-direct|crossbeam-block-handoff-direct|block-handoff|crossbeam-block-handoff|mpmc-ring|mpmc-locked|mpmc-array|mpmc-ring-batch|mpmc-locked-batch|mpmc-array-batch|mpmc-ring-owned-batch|mpmc-lanes|mpmc-lanes-batch|mpmc-lanes-owned-batch|mpmc-locked-owned-batch|mpmc-array-owned-batch|mpmc-seg|mpmc-seg-batch|mpmc-seg-owned-batch|mpmc-locked-seg|mpmc-locked-seg-batch|mpmc-locked-seg-owned-batch|mpmc-array-seg|mpmc-array-seg-batch|mpmc-array-seg-owned-batch|mpsc-spin|crossbeam|kanal|flume|mutex|mutex-sharded; BYTES=8|33|64|256|257|4096; BATCH=receive staging (also producer staging for -batch); external channels require BATCH=1; shared capacity=PRODUCERS*CAPACITY; leased/block-handoff modes: CAPACITY=all pool payload slots/producer, BATCH=messages/block, CAPACITY must be divisible by BATCH (need not be a power of two); worker metrics include readiness/work/retries/validation/teardown and print after timing";
struct Config {
    mode: String,
    bytes: usize,
    producers: usize,
    consumers: usize,
    per: usize,
    capacity: usize,
    automatic_capacity: bool,
    batch: usize,
    samples: usize,
    total: usize,
    delivered: usize,
    placement: affinity::Placement,
    worker_metrics: worker_metrics::Mode,
}
impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        if args.len() < 8 || !(args.len() - 8).is_multiple_of(2) {
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
        if !pooled_mode(&args[0]) && !capacity.is_power_of_two() {
            return Err("capacity must be a power of two".into());
        }
        match args[0].as_str() {
            mode if pooled_mode(mode) && capacity >= batch && capacity.is_multiple_of(batch) => {}
            mode if pooled_mode(mode) => {
                return Err("block pool must hold a whole number of batches".into());
            }
            "mpmc-ring"
            | "mpmc-locked"
            | "mpmc-array"
            | "mpmc-ring-batch"
            | "mpmc-locked-batch"
            | "mpmc-array-batch"
            | "mpmc-ring-owned-batch"
            | "mpmc-lanes"
            | "mpmc-lanes-batch"
            | "mpmc-lanes-owned-batch"
            | "mpmc-locked-owned-batch"
            | "mpmc-array-owned-batch"
            | "mpmc-seg"
            | "mpmc-seg-batch"
            | "mpmc-seg-owned-batch"
            | "mpmc-locked-seg"
            | "mpmc-locked-seg-batch"
            | "mpmc-locked-seg-owned-batch"
            | "mpmc-array-seg"
            | "mpmc-array-seg-batch"
            | "mpmc-array-seg-owned-batch"
            | "mutex"
            | "mutex-sharded" => {}
            "crossbeam" | "kanal" | "flume" if batch == 1 => {}
            "crossbeam" | "kanal" | "flume" => {
                return Err("external channels require BATCH=1".into());
            }
            "mpsc-spin" if consumers == 1 => {}
            _ => return Err("unknown mode or MPSC requires one consumer".into()),
        }
        let overflow = "workload dimensions overflow";
        let total = producers.checked_mul(per).ok_or(overflow)?;
        let delivered = total
            .checked_mul(samples.checked_add(1).ok_or(overflow)?)
            .ok_or(overflow)?;
        let workers = producers.checked_add(consumers).ok_or(overflow)?;
        for count in [
            producers.checked_mul(capacity),
            consumers.checked_mul(batch),
            producers.checked_mul(batch),
            Some(delivered),
        ] {
            let byte_count = count.and_then(|n| n.checked_mul(bytes)).ok_or(overflow)?;
            if byte_count > isize::MAX as usize {
                return Err(overflow.into());
            }
        }
        if pooled_mode(&args[0]) {
            let slots = (capacity / batch)
                .checked_next_power_of_two()
                .ok_or(overflow)?;
            let metadata = producers
                .checked_mul(consumers.checked_add(1).ok_or(overflow)?)
                .and_then(|n| n.checked_mul(slots))
                .and_then(|n| n.checked_mul(64))
                .ok_or(overflow)?;
            if metadata > isize::MAX as usize {
                return Err(overflow.into());
            }
        }
        let mut capacity_mode = None;
        let mut cpu_map = None;
        let mut metrics = None;
        for option in args[8..].as_chunks::<2>().0 {
            match option[0].as_str() {
                "--capacity-mode" if capacity_mode.is_none() => {
                    capacity_mode = Some(match option[1].as_str() {
                        "explicit" => false,
                        "automatic" => true,
                        _ => return Err("capacity mode must be explicit or automatic".into()),
                    });
                }
                "--cpus" if cpu_map.is_none() => cpu_map = Some(option[1].as_str()),
                "--worker-metrics" if metrics.is_none() => {
                    metrics = Some(worker_metrics::Mode::parse(&option[1])?);
                }
                _ => return Err("unknown or duplicate benchmark option".into()),
            }
        }
        let automatic_capacity = capacity_mode.unwrap_or(false);
        if automatic_capacity && !supports_automatic_capacity(&args[0]) {
            return Err("automatic capacity is available only for Prescient modes".into());
        }
        Ok(Self {
            mode: args[0].clone(),
            bytes,
            producers,
            consumers,
            per,
            capacity,
            automatic_capacity,
            batch,
            samples,
            total,
            delivered,
            placement: affinity::Placement::new(cpu_map, workers)?,
            worker_metrics: metrics.unwrap_or_default(),
        })
    }
}
#[path = "support/payload.rs"]
mod payload;
use payload::{fixtures, inspect, packet, xor_prefix};
fn measure<const N: usize, P: SendValue<N> + Send, C: ReceiveValue<N> + Send>(
    c: &Config,
    mut factory: impl FnMut() -> (Vec<P>, Vec<C>),
) -> Result<(), String> {
    let fixtures = &fixtures::<N>();
    let mut rates = Vec::with_capacity(c.samples + 1);
    for sample in 0..=c.samples {
        let (producers, consumers) = factory();
        let gate = start_gate::StartGate::default();
        let (elapsed, producer_reports, results) =
            std::thread::scope(|scope| -> Result<_, String> {
                let _abort = gate.abort_on_drop();
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let mut senders = Vec::with_capacity(c.producers);
                for (index, mut sender) in producers.into_iter().enumerate() {
                    let ready = ready_tx.clone();
                    let gate = &gate;
                    senders.push(
                        std::thread::Builder::new()
                            .spawn_scoped(scope, move || {
                                let meter = c.placement.pin(index).and_then(|()| {
                                    worker_metrics::Meter::prepare(c.worker_metrics)
                                });
                                let preparation = meter.as_ref().map(|_| ()).map_err(Clone::clone);
                                if !gate.enter(ready, preparation) {
                                    return Ok(None);
                                }
                                let meter = meter?;
                                let start = index * c.per;
                                sender.send_values(start, start + c.per, fixtures);
                                // Include endpoint teardown; disconnect must precede consumer join.
                                drop(sender);
                                meter.finish()
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
                                let meter = c.placement.pin(c.producers + index).and_then(|()| {
                                    worker_metrics::Meter::prepare(c.worker_metrics)
                                });
                                let preparation = meter.as_ref().map(|_| ()).map_err(Clone::clone);
                                if !gate.enter(ready, preparation) {
                                    return Ok(((0, 0u64, 0u64, 0), None));
                                }
                                let meter = meter?;
                                let (mut count, mut sum, mut xor, mut errors) = (0, 0u64, 0u64, 0);
                                receiver.consume_values(|value| {
                                    let (id, bad) = inspect(value, fixtures, c.total);
                                    count += 1;
                                    sum = sum.wrapping_add(id);
                                    xor ^= id;
                                    errors += bad;
                                });
                                drop(receiver);
                                Ok(((count, sum, xor, errors), meter.finish()?))
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
                let producer_reports = senders
                    .into_iter()
                    .map(|s| s.join().map_err(|_| "producer panicked".to_string())?)
                    .collect::<Result<Vec<_>, String>>()?;
                let results = receivers
                    .into_iter()
                    .map(|r| r.join().map_err(|_| "consumer panicked".to_string())?)
                    .collect::<Result<Vec<_>, String>>()?;
                Ok((start.elapsed(), producer_reports, results))
            })?;
        let (mut count, mut sum, mut xor, mut errors) = (0, 0u64, 0u64, 0);
        for (index, report) in producer_reports.iter().enumerate() {
            if let Some(report) = report {
                report.print(sample, "producer", index, c.per);
            }
        }
        for (index, ((n, s, x, e), report)) in results.into_iter().enumerate() {
            if let Some(report) = report {
                report.print(sample, "consumer", index, n);
            }
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
        "impl={};bytes={N};producers={};consumers={};messages={};samples={};warmups=1;delivered={};capacity={};capacity_mode={};aggregate_capacity={};batch={};producer_batch={};worker_cpus={};pool_blocks_per_producer={};pool_payload_bytes={};median_mps={median:.6};median_gbps={:.6};min_mps={:.6};max_mps={:.6};sample_mps={ordered};worker_metrics={}",
        c.mode,
        c.producers,
        c.consumers,
        c.total,
        c.samples,
        c.delivered,
        c.capacity,
        if c.automatic_capacity {
            "automatic"
        } else {
            "explicit"
        },
        c.capacity * c.producers,
        c.batch,
        if c.mode.ends_with("-batch") || pooled_mode(&c.mode) {
            c.batch
        } else {
            1
        },
        c.placement.label(),
        if pooled_mode(&c.mode) {
            c.capacity / c.batch
        } else {
            0
        },
        if pooled_mode(&c.mode) {
            c.capacity * c.producers * N
        } else {
            0
        },
        median * N as f64 / 1000.0,
        rates[0],
        rates[rates.len() - 1],
        c.worker_metrics.label()
    );
    Ok(())
}
fn run<const N: usize>(c: &Config) -> Result<(), String> {
    macro_rules! capacity {
        ($declaration:expr) => {{
            let declaration = $declaration;
            if c.automatic_capacity {
                declaration
            } else {
                declaration.capacity(c.capacity)
            }
        }};
    }
    macro_rules! prescient {
        ($factory:expr) => {{
            // Strategy selection happens once, before any workers/message loops.
            if c.mode.ends_with("-owned-batch") {
                measure(c, || {
                    let (tx, rx) = ($factory)();
                    (
                        tx.into_iter().map(|tx| OwnedBatch(tx, c.batch)).collect(),
                        rx.into_iter().map(OwnedReceiver).collect(),
                    )
                })
            } else if c.mode.ends_with("-batch") {
                measure(c, || {
                    let (tx, rx) = ($factory)();
                    (
                        tx.into_iter().map(|tx| OwnedBatch(tx, c.batch)).collect(),
                        rx,
                    )
                })
            } else {
                measure(c, $factory)
            }
        }};
    }
    macro_rules! shared {
        ($channel:expr) => {
            measure(c, || {
                let (tx, rx) = $channel;
                (
                    (0..c.producers).map(|_| tx.clone()).collect(),
                    (0..c.consumers).map(|_| rx.clone()).collect(),
                )
            })
        };
    }
    match c.mode.as_str() {
        "leased" => measure(c, || {
            let (tx, rx) = capacity!(
                brokerless::leased::channel::<[u8; N]>()
                    .producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap();
            (tx.into_iter().map(LeasedProducer::<N, false>).collect(), rx)
        }),
        "leased-direct" => measure(c, || {
            let (tx, rx) = capacity!(
                brokerless::leased::channel::<[u8; N]>()
                    .producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap();
            (tx.into_iter().map(LeasedProducer::<N, true>).collect(), rx)
        }),
        "block-handoff-direct" => measure(c, || {
            let (tx, rx) =
                block_exchange::channel::<[u8; N]>(c.producers, c.consumers, c.capacity, c.batch);
            (tx.into_iter().map(DirectBlock).collect(), rx)
        }),
        "crossbeam-block-handoff-direct" => measure(c, || {
            let (tx, rx) =
                block_exchange::crossbeam::<[u8; N]>(c.producers, c.consumers, c.capacity, c.batch);
            (tx.into_iter().map(DirectBlock).collect(), rx)
        }),
        "crossbeam-block-handoff" => measure(c, || {
            block_exchange::crossbeam::<[u8; N]>(c.producers, c.consumers, c.capacity, c.batch)
        }),
        "block-handoff" => measure(c, || {
            block_exchange::channel::<[u8; N]>(c.producers, c.consumers, c.capacity, c.batch)
        }),
        "mpmc-ring" | "mpmc-ring-batch" | "mpmc-ring-owned-batch" => prescient!(|| {
            capacity!(
                brokerless::channel::<[u8; N]>()
                    .producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap()
        }),
        "mpmc-lanes" | "mpmc-lanes-batch" | "mpmc-lanes-owned-batch" => prescient!(|| {
            capacity!(
                lanes::channel::<[u8; N]>()
                    .producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap()
        }),
        "mpmc-seg" | "mpmc-seg-batch" | "mpmc-seg-owned-batch" => prescient!(|| {
            capacity!(
                brokerless::channel::<[u8; N]>()
                    .backend::<Seg>()
                    .producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap()
        }),
        "mpmc-locked" | "mpmc-locked-batch" | "mpmc-locked-owned-batch" => prescient!(|| {
            let (registrar, rx) = capacity!(
                brokerless::dynamic::locked::<[u8; N]>()
                    .expected_producers(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap();
            (
                (0..c.producers)
                    .map(|_| registrar.register())
                    .collect::<Vec<_>>(),
                rx,
            )
        }),
        "mpmc-locked-seg" | "mpmc-locked-seg-batch" | "mpmc-locked-seg-owned-batch" => {
            prescient!(|| {
                let (registrar, rx) = capacity!(
                    brokerless::dynamic::locked::<[u8; N]>()
                        .backend::<Seg>()
                        .expected_producers(c.producers)
                        .consumers(c.consumers)
                        .batch(c.batch)
                )
                .open()
                .unwrap();
                (
                    (0..c.producers)
                        .map(|_| registrar.register())
                        .collect::<Vec<_>>(),
                    rx,
                )
            })
        }
        "mpmc-array" | "mpmc-array-batch" | "mpmc-array-owned-batch" => prescient!(|| {
            let (registrar, rx) = capacity!(
                brokerless::dynamic::array::<[u8; N]>(c.producers)
                    .consumers(c.consumers)
                    .batch(c.batch)
            )
            .open()
            .unwrap();
            (
                (0..c.producers)
                    .map(|_| registrar.register())
                    .collect::<Vec<_>>(),
                rx,
            )
        }),
        "mpmc-array-seg" | "mpmc-array-seg-batch" | "mpmc-array-seg-owned-batch" => {
            prescient!(|| {
                let (registrar, rx) = capacity!(
                    brokerless::dynamic::array::<[u8; N]>(c.producers)
                        .backend::<Seg>()
                        .consumers(c.consumers)
                        .batch(c.batch)
                )
                .open()
                .unwrap();
                (
                    (0..c.producers)
                        .map(|_| registrar.register())
                        .collect::<Vec<_>>(),
                    rx,
                )
            })
        }
        "mpsc-spin" => measure(c, || {
            let (tx, rx) = capacity!(
                mpsc::fixed::channel::<[u8; N]>()
                    .wait::<Spin>()
                    .producers(c.producers)
                    .batch(c.batch)
            )
            .open()
            .unwrap();
            (tx, vec![rx])
        }),
        "crossbeam" => shared!(crossbeam_channel::bounded::<[u8; N]>(
            c.producers * c.capacity
        )),
        "kanal" => shared!(kanal::bounded::<[u8; N]>(c.producers * c.capacity)),
        "flume" => shared!(flume::bounded::<[u8; N]>(c.producers * c.capacity)),
        "mutex" => measure(c, || {
            mutex_queue::bounded::<[u8; N]>(
                c.producers * c.capacity,
                c.producers,
                c.consumers,
                c.batch,
            )
        }),
        "mutex-sharded" => measure(c, || {
            mutex_queue::sharded::<[u8; N]>(c.capacity, c.producers, c.consumers, c.batch)
        }),
        _ => unreachable!(),
    }
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
    fn direct_fill_matches_value_construction() {
        fn check<const N: usize>() {
            let f = fixtures::<N>();
            let mut slots = [MaybeUninit::uninit(); 97];
            fill_packets(&mut slots, 31, &f);
            for (offset, slot) in slots.iter().enumerate() {
                // SAFETY: fill_packets initialized every array element.
                let value = unsafe { slot.assume_init_ref() };
                assert_eq!(*value, packet((31 + offset) as u64, &f));
            }
        }
        check::<8>();
        check::<33>();
        check::<256>();
        check::<257>();
        check::<4096>();
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
    fn automatic_capacity_is_explicit_and_prescient_only() {
        let automatic = config("mpmc-ring 4096 3 3 97 64 64 1 --capacity-mode automatic").unwrap();
        assert!(automatic.automatic_capacity);
        let explicit = config("mpmc-ring 4096 3 3 97 64 64 1 --capacity-mode explicit").unwrap();
        assert!(!explicit.automatic_capacity);
        assert!(config("crossbeam 8 1 1 97 64 1 1 --capacity-mode automatic").is_err());
        assert!(
            config("mpmc-ring 8 1 1 97 64 1 1 --capacity-mode automatic --capacity-mode explicit")
                .is_err()
        );
    }
    #[test]
    fn metrics_options_and_non_power_of_two_pools_are_explicit() {
        for cap in [192, 3072, 3840, 5120, 15360] {
            let c = config(&format!("leased 8 3 3 97 {cap} 64 1 --worker-metrics duty")).unwrap();
            assert_eq!(c.worker_metrics, worker_metrics::Mode::Duty);
        }
        for options in [
            "--worker-metrics unknown",
            "--worker-metrics",
            "--worker-metrics off --worker-metrics duty",
            "--cpus 0,1 --cpus 0,1",
        ] {
            assert!(config(&format!("leased 8 1 1 97 192 64 1 {options}")).is_err());
        }
        // A non-power-of-two pool still uses valid rounded descriptor rings.
        run::<33>(&config("leased 33 3 2 997 192 64 1").unwrap()).unwrap();
    }
    #[test]
    fn rejects_invalid_dimensions() {
        for bad in [
            "bad 64 1 1 10 1 1 1",
            "leased 64 1 1 10 4 3 1",
            "leased-direct 64 1 1 10 1 64 1",
            "block-handoff-direct 64 1 1 10 4 3 1",
            "crossbeam-block-handoff-direct 64 1 1 10 1 64 1",
            "block-handoff 64 1 1 10 4 3 1",
            "block-handoff 64 1 1 10 1 64 1",
            "crossbeam 64 1 1 10 1 64 1",
            "kanal 64 1 1 10 1 64 1",
            "flume 64 1 1 10 1 64 1",
            "mpsc-spin 64 1 2 10 1 1 1",
            "mpmc-ring 65 1 1 10 1 1 1",
            "mpmc-ring 64 1 1 10 3 1 1",
            "mpmc-ring 64 0 1 10 1 1 1",
            "mpmc-ring 64 1 1 10 1 1 0",
            "mpmc-ring 64 1 1 10 1 1 1 --typo 0,1",
        ] {
            assert!(config(bad).is_err(), "{bad}");
        }
        assert!(config(&format!("mpmc-ring 64 2 1 {} 1 1 1", usize::MAX)).is_err());
        assert!(config(&format!("mpmc-ring 64 1 1 1 1 1 {}", usize::MAX)).is_err());
    }
    #[test]
    fn block_pool_delivers_full_bytes_at_tight_and_partial_boundaries() {
        for mode in [
            "leased",
            "leased-direct",
            "block-handoff",
            "crossbeam-block-handoff",
            "block-handoff-direct",
            "crossbeam-block-handoff-direct",
        ] {
            for (p, r) in [(1, 1), (3, 3), (1, 4)] {
                for (cap, batch) in [(1, 1), (4, 1), (64, 64), (256, 64)] {
                    let c = config(&format!("{mode} 33 {p} {r} 97 {cap} {batch} 1")).unwrap();
                    run::<33>(&c).unwrap();
                }
            }
            let c = config(&format!("{mode} 4096 2 2 97 64 64 1")).unwrap();
            run::<4096>(&c).unwrap();
        }
    }
    #[test]
    fn tiny_and_wrapping_transports_deliver_complete_payloads() {
        for mode in [
            "mpmc-ring",
            "mpmc-locked",
            "mpmc-array",
            "mpmc-ring-batch",
            "mpmc-ring-owned-batch",
            "mpmc-lanes",
            "mpmc-lanes-batch",
            "mpmc-lanes-owned-batch",
            "mpmc-locked-batch",
            "mpmc-locked-owned-batch",
            "mpmc-array-batch",
            "mpmc-array-owned-batch",
            "mpmc-seg",
            "mpmc-seg-batch",
            "mpmc-seg-owned-batch",
            "mpmc-locked-seg",
            "mpmc-locked-seg-batch",
            "mpmc-locked-seg-owned-batch",
            "mpmc-array-seg",
            "mpmc-array-seg-batch",
            "mpmc-array-seg-owned-batch",
            "mpsc-spin",
            "mutex",
            "mutex-sharded",
        ] {
            for cap in [1, 4] {
                let c = config(&format!("{mode} 33 2 1 97 {cap} 3 1")).unwrap();
                run::<33>(&c).unwrap();
            }
        }
        for mode in ["crossbeam", "kanal", "flume"] {
            for cap in [1, 4] {
                let c = config(&format!("{mode} 33 2 2 97 {cap} 1 1")).unwrap();
                run::<33>(&c).unwrap();
            }
        }
    }
}
