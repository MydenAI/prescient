//! A/B: does the in-place, batched-reclaim `drain()` consumer beat the baseline
//! `while let Some(v) = recv()` consumer? Identical realistic aggregation
//! workload, identical `fixed` channel — only the consumer path differs. The two
//! are INTERLEAVED in one process so thermal drift hits both equally; the paired
//! per-rep ratio is what to read, not the absolute rates. Checksums are asserted
//! equal (both paths must deliver the same multiset).
//!
//!   cargo run --release --example ab_drain -- [P] [per] [cwork]

use std::time::Instant;

use prescient::mpsc::fixed;

const REPS: usize = 9;
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
    0x9E3779B97F4A7C15 ^ (id as u64).wrapping_mul(0xD1B54A32D192ED03).wrapping_add(1)
}
#[inline]
fn make_event(st: &mut u64, source: u32, seq: u64) -> Event {
    *st = mix(*st);
    Event {
        source,
        key: *st,
        value: *st & 0xFFFF,
        seq,
    }
}
#[inline]
fn agg(hist: &mut [u64; BUCKETS], ev: &Event, cw: u32) -> u64 {
    let b = (ev.key as usize) & (BUCKETS - 1);
    hist[b] = hist[b].wrapping_add(ev.value);
    let mut acc = ev.value ^ ev.seq ^ (ev.source as u64);
    for _ in 0..cw {
        acc = mix(acc);
    }
    acc
}

macro_rules! spawn_producers {
    ($s:expr, $prods:expr, $per:expr) => {
        for (id, mut prod) in $prods.into_iter().enumerate() {
            $s.spawn(move || {
                let mut st = seed(id);
                for i in 0..$per as u64 {
                    prod.send(make_event(&mut st, id as u32, i)).unwrap();
                }
            });
        }
    };
}

// baseline consumer: `while let Some(ev) = recv()`
fn run_recv(p: usize, per: usize, cw: u32) -> u64 {
    let (prods, mut rx) = fixed::channel::<Event>()
        .wait::<prescient::wait::Spin>()
        .producers(p)
        .capacity(CAP)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        spawn_producers!(s, prods, per);
        let mut hist = [0u64; BUCKETS];
        let mut ck = 0u64;
        while let Some(ev) = rx.recv() {
            ck = ck.wrapping_add(agg(&mut hist, &ev, cw));
        }
        for h in hist {
            ck ^= h;
        }
        ck
    })
}

// candidate consumer: in-place `drain()`
fn run_drain(p: usize, per: usize, cw: u32) -> u64 {
    let (prods, mut rx) = fixed::channel::<Event>()
        .wait::<prescient::wait::Spin>()
        .producers(p)
        .capacity(CAP)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        spawn_producers!(s, prods, per);
        let mut hist = [0u64; BUCKETS];
        let mut ck = 0u64;
        rx.drain(|ev| {
            ck = ck.wrapping_add(agg(&mut hist, &ev, cw));
        });
        for h in hist {
            ck ^= h;
        }
        ck
    })
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let p: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let cw: u32 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let total = p * per;
    println!("A/B recv() vs drain(): producers={p} per={per} cwork={cw} total={total}\n");

    // warm-up + correctness cross-check
    assert_eq!(
        run_recv(p, per, cw),
        run_drain(p, per, cw),
        "recv/drain checksum mismatch"
    );

    let mut recv_rates = Vec::new();
    let mut drain_rates = Vec::new();
    let mut ratios = Vec::new();
    let mut ck = 0u64;
    for _ in 0..REPS {
        let t = Instant::now();
        let a = run_recv(p, per, cw);
        let rr = total as f64 / t.elapsed().as_secs_f64();

        let t = Instant::now();
        let b = run_drain(p, per, cw);
        let dr = total as f64 / t.elapsed().as_secs_f64();

        assert_eq!(a, b, "recv/drain checksum mismatch");
        ck = a;
        recv_rates.push(rr);
        drain_rates.push(dr);
        ratios.push(dr / rr); // paired: thermal state ~constant within a rep
    }

    let rm = median(recv_rates) / 1e6;
    let dm = median(drain_rates) / 1e6;
    let pr = median(ratios);
    println!("baseline  recv()   {:>7.2} M ev/s", rm);
    println!("candidate drain()  {:>7.2} M ev/s", dm);
    println!(
        "\nmedian paired speedup: {:.3}x  ({:+.1}%)   checksum {:016x}",
        pr,
        (pr - 1.0) * 100.0,
        ck
    );
}
