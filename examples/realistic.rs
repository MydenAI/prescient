//! Fan-in benchmark with structured 32-byte events and application work on both
//! sides of the channel. Producers derive each event through an xorshift chain;
//! the consumer updates a bucket histogram and checksum. `cwork` controls extra
//! consumer computation so channel overhead can be measured at different shares
//! of the complete workload.
//!
//! Aggregation is order-independent. Every implementation must produce the same
//! checksum; a mismatch fails the run.
//!
//!   cargo run --release --example realistic -- [P] [per] [cwork]
//!
//! P     producers            (default 4)
//! per   events per producer  (default 1_000_000)
//! cwork extra consumer work units per event (default 0)

use std::time::Instant;

use prescient::mpsc::{dynamic, fixed, pool};

const REPS: usize = 3;
const BUCKETS: usize = 256;
const CAP: usize = 1024;

#[derive(Clone, Copy, Debug)]
struct Event {
    source: u32,
    key: u64,
    value: u64,
    seq: u64,
}

#[inline]
fn mix(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[inline]
fn seed(id: usize) -> u64 {
    // distinct, always-nonzero xorshift seed per producer
    0x9E3779B97F4A7C15 ^ (id as u64).wrapping_mul(0xD1B54A32D192ED03).wrapping_add(1)
}

#[inline]
fn make_event(state: &mut u64, source: u32, seq: u64) -> Event {
    *state = mix(*state);
    Event {
        source,
        key: *state,
        value: *state & 0xFFFF,
        seq,
    }
}

// Consumer-side per-event work: bucket the value by key, then optionally grind
// `cwork` more steps to simulate heavier processing. Returns an order-independent
// contribution to the run checksum.
#[inline]
fn aggregate(hist: &mut [u64; BUCKETS], ev: &Event, cwork: u32) -> u64 {
    let b = (ev.key as usize) & (BUCKETS - 1);
    hist[b] = hist[b].wrapping_add(ev.value);
    let mut acc = ev.value ^ ev.seq ^ (ev.source as u64);
    for _ in 0..cwork {
        acc = mix(acc);
    }
    acc
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn report(name: &str, total: usize, expected: &mut Option<u64>, mut run: impl FnMut() -> u64) {
    let mut rates = Vec::with_capacity(REPS);
    let mut last = run(); // warm-up
    for _ in 0..REPS {
        let start = Instant::now();
        last = run();
        rates.push(total as f64 / start.elapsed().as_secs_f64());
    }
    match expected {
        Some(e) => assert_eq!(
            last, *e,
            "{name}: checksum differs — lost/duplicated events"
        ),
        None => *expected = Some(last),
    }
    println!(
        "{:<20} {:>7.2} M ev/s   checksum {:016x}",
        name,
        median(rates) / 1e6,
        last
    );
}

// ---- sharded tiers --------------------------------------------------------

fn run_fixed(p: usize, per: usize, cwork: u32) -> u64 {
    let (prods, mut rx) = fixed::channel::<Event>()
        .wait::<prescient::wait::Spin>()
        .producers(p)
        .capacity(CAP)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for (id, mut prod) in prods.into_iter().enumerate() {
            s.spawn(move || {
                let mut st = seed(id);
                for i in 0..per as u64 {
                    prod.send(make_event(&mut st, id as u32, i)).unwrap();
                }
            });
        }
        consume(&mut rx, cwork)
    })
}

fn run_pool(p: usize, per: usize, cwork: u32) -> u64 {
    let (h, mut rx) = pool::channel::<Event>()
        .wait::<prescient::wait::Spin>()
        .max_producers(p.max(1))
        .capacity(CAP)
        .open()
        .unwrap();
    let claimed: Vec<_> = (0..p).map(|_| h.claim().unwrap()).collect();
    drop(h);
    std::thread::scope(|s| {
        for (id, mut prod) in claimed.into_iter().enumerate() {
            s.spawn(move || {
                let mut st = seed(id);
                for i in 0..per as u64 {
                    prod.send(make_event(&mut st, id as u32, i)).unwrap();
                }
            });
        }
        consume(&mut rx, cwork)
    })
}

fn run_dynamic(p: usize, per: usize, cwork: u32) -> u64 {
    let (reg, mut rx) = dynamic::channel::<Event>()
        .wait::<prescient::wait::Spin>()
        .capacity(CAP)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for id in 0..p {
            let reg = reg.clone();
            s.spawn(move || {
                let mut prod = reg.register();
                let mut st = seed(id);
                for i in 0..per as u64 {
                    prod.send(make_event(&mut st, id as u32, i)).unwrap();
                }
            });
        }
        drop(reg);
        consume(&mut rx, cwork)
    })
}

fn consume<S: prescient::mpsc::ShardState<Event, prescient::wait::Spin>>(
    rx: &mut prescient::mpsc::Receiver<Event, prescient::wait::Spin, S>,
    cwork: u32,
) -> u64 {
    let mut hist = [0u64; BUCKETS];
    let mut cksum = 0u64;
    while let Some(ev) = rx.recv() {
        cksum = cksum.wrapping_add(aggregate(&mut hist, &ev, cwork));
    }
    for h in hist {
        cksum ^= h;
    }
    cksum
}

// ---- std / flume baselines ------------------------------------------------

macro_rules! run_channel {
    ($p:expr, $per:expr, $cwork:expr, $mk:expr) => {{
        let (tx, rx) = $mk;
        std::thread::scope(|s| {
            for id in 0..$p {
                let tx = tx.clone();
                s.spawn(move || {
                    let mut st = seed(id);
                    for i in 0..$per as u64 {
                        tx.send(make_event(&mut st, id as u32, i)).unwrap();
                    }
                });
            }
            drop(tx);
            let mut hist = [0u64; BUCKETS];
            let mut cksum = 0u64;
            while let Ok(ev) = rx.recv() {
                cksum = cksum.wrapping_add(aggregate(&mut hist, &ev, $cwork));
            }
            for h in hist {
                cksum ^= h;
            }
            cksum
        })
    }};
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let p: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let cwork: u32 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let total = p * per;
    println!(
        "realistic fan-in: producers={p} per={per} cwork={cwork} event={}B total={total}\n",
        std::mem::size_of::<Event>()
    );

    let mut expect = None;
    report("fixed", total, &mut expect, || run_fixed(p, per, cwork));
    report("pool", total, &mut expect, || run_pool(p, per, cwork));
    report("dynamic", total, &mut expect, || run_dynamic(p, per, cwork));
    report("std::mpsc", total, &mut expect, || {
        run_channel!(p, per, cwork, std::sync::mpsc::channel::<Event>())
    });
    report("std::sync_channel", total, &mut expect, || {
        run_channel!(p, per, cwork, std::sync::mpsc::sync_channel::<Event>(8192))
    });
    report("flume-unbounded", total, &mut expect, || {
        run_channel!(p, per, cwork, flume::unbounded::<Event>())
    });
    report("flume-bounded", total, &mut expect, || {
        run_channel!(p, per, cwork, flume::bounded::<Event>(8192))
    });
}
