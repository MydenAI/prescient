//! Fan-in throughput and latency benchmark.
//!
//! Use at least `producers + 1` physical cores and compile with `--release`.
//!
//!   cargo run --release --example bench -- throughput [P] [per] [gap_ns] [batch]
//!   cargo run --release --example bench -- latency    [P] [per] [gap_ns] [batch]
//!
//! `throughput` reports median messages per second over several repetitions.
//! `latency` reports end-to-end p50/p99/p999 at the offered rate selected by
//! `gap_ns`. Use a nonzero gap to measure below the saturation point.
//!
//! Baselines include standard-library MPSC, Flume, Kanal, and ThingBuf.

use std::time::{Duration, Instant};

use prescient::mpsc::{dynamic, fixed, pool};

const REPS: usize = 7;

#[inline]
fn pace(gap: Duration) {
    if gap.is_zero() {
        return;
    }
    // busy-wait: resolves sub-50us intervals that thread::sleep cannot
    let start = Instant::now();
    while start.elapsed() < gap {
        std::hint::spin_loop();
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

// ---- throughput -----------------------------------------------------------

fn tp(name: &str, total: usize, f: impl Fn()) {
    f(); // warm-up
    let mut mps = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        f();
        mps.push(total as f64 / start.elapsed().as_secs_f64());
    }
    println!("{:<20} {:>8.2} M msg/s", name, median(mps) / 1e6);
}

fn tp_fixed(p: usize, per: usize, gap: Duration, batch: usize) {
    let (prods, mut rx) = fixed::channel::<u64>()
        .wait::<prescient::wait::Spin>()
        .producers(p)
        .capacity(1024)
        .open()
        .unwrap();
    rx.set_batch(batch);
    std::thread::scope(|s| {
        for mut prod in prods {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i).unwrap();
                    pace(gap);
                }
            });
        }
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        assert_eq!(n, p * per);
    });
}

fn tp_pool(p: usize, per: usize, gap: Duration, batch: usize) {
    let (h, mut rx) = pool::channel::<u64>()
        .wait::<prescient::wait::Spin>()
        .max_producers(p.max(1))
        .capacity(1024)
        .open()
        .unwrap();
    rx.set_batch(batch);
    let claimed: Vec<_> = (0..p).map(|_| h.claim().unwrap()).collect();
    drop(h);
    std::thread::scope(|s| {
        for mut prod in claimed {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i).unwrap();
                    pace(gap);
                }
            });
        }
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        assert_eq!(n, p * per);
    });
}

fn tp_dynamic(p: usize, per: usize, gap: Duration, batch: usize) {
    let (reg, mut rx) = dynamic::channel::<u64>()
        .wait::<prescient::wait::Spin>()
        .capacity(1024)
        .open()
        .unwrap();
    rx.set_batch(batch);
    std::thread::scope(|s| {
        for _ in 0..p {
            let reg = reg.clone();
            s.spawn(move || {
                let mut prod = reg.register();
                for i in 0..per as u64 {
                    prod.send(i).unwrap();
                    pace(gap);
                }
            });
        }
        drop(reg);
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        assert_eq!(n, p * per);
    });
}

macro_rules! tp_channel {
    ($p:expr, $per:expr, $gap:expr, $mk:expr) => {{
        let (tx, rx) = $mk;
        std::thread::scope(|s| {
            for _ in 0..$p {
                let tx = tx.clone();
                let gap = $gap;
                s.spawn(move || {
                    for i in 0..$per as u64 {
                        tx.send(i).unwrap();
                        pace(gap);
                    }
                });
            }
            drop(tx);
            let mut n = 0;
            while rx.recv().is_ok() {
                n += 1;
            }
            assert_eq!(n, $p * $per);
        });
    }};
}

// thingbuf's blocking MPSC: single shared ring, recv() -> Option, T: Default.
// Needs its own fn (doesn't fit the Result-returning tp_channel! macro).
fn tp_thingbuf(p: usize, per: usize, gap: Duration) {
    let (tx, rx) = thingbuf::mpsc::blocking::channel::<u64>(8192);
    std::thread::scope(|s| {
        for _ in 0..p {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..per as u64 {
                    tx.send(i).unwrap();
                    pace(gap);
                }
            });
        }
        drop(tx);
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        assert_eq!(n, p * per);
    });
}

// ---- latency --------------------------------------------------------------

fn now_ns(base: Instant) -> u64 {
    base.elapsed().as_nanos() as u64
}

fn lat_thingbuf(p: usize, per: usize, gap: Duration) -> Vec<u64> {
    let base = Instant::now();
    let (tx, rx) = thingbuf::mpsc::blocking::channel::<u64>(8192);
    let total = p * per;
    let mut samples = Vec::with_capacity(total);
    std::thread::scope(|s| {
        for _ in 0..p {
            let tx = tx.clone();
            s.spawn(move || {
                for _ in 0..per {
                    tx.send(now_ns(base)).unwrap();
                    pace(gap);
                }
            });
        }
        drop(tx);
        for _ in 0..total {
            if let Some(stamp) = rx.recv() {
                samples.push(now_ns(base).saturating_sub(stamp));
            }
        }
    });
    samples
}

fn lat_custom_fixed(p: usize, per: usize, gap: Duration, batch: usize) -> Vec<u64> {
    let base = Instant::now();
    let (prods, mut rx) = fixed::channel::<u64>()
        .wait::<prescient::wait::Spin>()
        .producers(p)
        .capacity(1024)
        .open()
        .unwrap();
    rx.set_batch(batch);
    let mut samples = Vec::with_capacity(p * per);
    std::thread::scope(|s| {
        for mut prod in prods {
            s.spawn(move || {
                for _ in 0..per {
                    prod.send(now_ns(base)).unwrap();
                    pace(gap);
                }
            });
        }
        let total = p * per;
        for _ in 0..total {
            if let Some(stamp) = rx.recv() {
                samples.push(now_ns(base).saturating_sub(stamp));
            }
        }
    });
    samples
}

macro_rules! lat_channel {
    ($p:expr, $per:expr, $gap:expr, $mk:expr) => {{
        let base = Instant::now();
        let (tx, rx) = $mk;
        let total = $p * $per;
        let mut samples = Vec::with_capacity(total);
        std::thread::scope(|s| {
            for _ in 0..$p {
                let tx = tx.clone();
                let gap = $gap;
                s.spawn(move || {
                    for _ in 0..$per {
                        tx.send(base.elapsed().as_nanos() as u64).unwrap();
                        pace(gap);
                    }
                });
            }
            drop(tx);
            for _ in 0..total {
                if let Ok(stamp) = rx.recv() {
                    samples.push((base.elapsed().as_nanos() as u64).saturating_sub(stamp));
                }
            }
        });
        samples
    }};
}

fn report_lat(name: &str, mut s: Vec<u64>) {
    s.sort_unstable();
    println!(
        "{:<20} p50 {:>7} ns   p99 {:>8} ns   p999 {:>9} ns",
        name,
        percentile(&s, 0.50),
        percentile(&s, 0.99),
        percentile(&s, 0.999),
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let mode = a.get(1).map(|s| s.as_str()).unwrap_or("throughput");
    let p: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let gap_ns: u64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    let gap = Duration::from_nanos(gap_ns);
    let batch: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(64);
    let total = p * per;

    println!("mode={mode} producers={p} per={per} gap={gap_ns}ns batch={batch} total={total}\n");

    if mode == "latency" {
        let lat_per = per.min(200_000); // latency runs collect samples; keep bounded
        let lt = p * lat_per;
        println!("(latency: {lt} samples/variant at gap={gap_ns}ns)\n");
        report_lat("fixed", lat_custom_fixed(p, lat_per, gap, batch));
        report_lat(
            "std::mpsc",
            lat_channel!(p, lat_per, gap, std::sync::mpsc::channel::<u64>()),
        );
        report_lat(
            "std::sync_channel",
            lat_channel!(p, lat_per, gap, std::sync::mpsc::sync_channel::<u64>(8192)),
        );
        report_lat(
            "flume-bounded",
            lat_channel!(p, lat_per, gap, flume::bounded::<u64>(8192)),
        );
        report_lat(
            "kanal-bounded",
            lat_channel!(p, lat_per, gap, kanal::bounded::<u64>(8192)),
        );
        report_lat("thingbuf", lat_thingbuf(p, lat_per, gap));
        return;
    }

    tp("fixed", total, || tp_fixed(p, per, gap, batch));
    tp("pool", total, || tp_pool(p, per, gap, batch));
    tp("dynamic", total, || tp_dynamic(p, per, gap, batch));
    tp("std::mpsc", total, || {
        tp_channel!(p, per, gap, std::sync::mpsc::channel::<u64>())
    });
    tp("std::sync_channel", total, || {
        tp_channel!(p, per, gap, std::sync::mpsc::sync_channel::<u64>(8192))
    });
    tp("flume-unbounded", total, || {
        tp_channel!(p, per, gap, flume::unbounded::<u64>())
    });
    tp("flume-bounded", total, || {
        tp_channel!(p, per, gap, flume::bounded::<u64>(8192))
    });
    tp("kanal-bounded", total, || {
        tp_channel!(p, per, gap, kanal::bounded::<u64>(8192))
    });
    tp("thingbuf-bounded", total, || tp_thingbuf(p, per, gap));
}
