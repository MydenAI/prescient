//! Throughput benchmark for competing-consumer MPMC channels.
//!
//! Use at least `P + C` physical cores, plus one core per broker, and compile
//! with `--release`.
//!
//!   cargo run --release --example mpmc_bench -- [P] [C] [per] [cap]
//!
//! Every value goes to exactly one consumer. Each run verifies the received
//! count before accepting its timing sample.
//!
//! Sharded designs allocate `cap` slots per ring, so an equal `cap` does not
//! imply an equal memory footprint to a single shared ring. Unbounded rows have
//! no equivalent capacity constraint.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use prescient::mpmc::{brokered, brokerless};

const REPS: usize = 7;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn bench(name: &str, total: usize, f: impl Fn()) {
    f(); // warm-up
    let mut mps = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        f();
        mps.push(total as f64 / start.elapsed().as_secs_f64());
    }
    println!("{:<24} {:>8.2} M msg/s", name, median(mps) / 1e6);
}

// ---- Prescient -------------------------------------------------------------

fn tp_brokerless(p: usize, c: usize, per: usize, cap: usize) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                while cc.recv().is_some() {
                    n += 1;
                }
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokerless lost/dup'd messages"
    );
}

// Zero-copy consumer path: for_each processes items in place under the claim.
fn tp_brokerless_drain(p: usize, c: usize, per: usize, cap: usize) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                cc.for_each(64, |_v| n += 1);
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokerless-drain lost/dup'd messages"
    );
}

// recv (staged) path over any Backend.
fn tp_brokerless_b_recv<B: prescient::backend::Backend>(
    p: usize,
    c: usize,
    per: usize,
    cap: usize,
) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .backend::<B>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                while cc.recv().is_some() {
                    n += 1;
                }
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokerless-B-recv lost/dup'd messages"
    );
}

// drain (zero-copy) path over any Backend (e.g. Seg).
fn tp_brokerless_b<B: prescient::backend::Backend>(p: usize, c: usize, per: usize, cap: usize) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .backend::<B>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                cc.for_each(64, |_v| n += 1);
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokerless-B lost/dup'd messages"
    );
}

fn tp_brokered_n(p: usize, c: usize, nb: usize, per: usize, cap: usize) {
    tp_brokered_nb::<prescient::backend::Ring>(p, c, nb, per, cap)
}

fn tp_brokered_nb<B: prescient::backend::Backend>(
    p: usize,
    c: usize,
    nb: usize,
    per: usize,
    cap: usize,
) {
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .backend::<B>()
        .producers(p)
        .consumers(c)
        .brokers(nb)
        .capacity(cap)
        .manual()
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in cons {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                while cc.recv().is_some() {
                    n += 1;
                }
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokered-n lost/dup'd messages"
    );
}

fn tp_brokered(p: usize, c: usize, per: usize, cap: usize) {
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .manual()
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i);
                }
            });
        }
        for mut cc in cons {
            let recv = &recv;
            s.spawn(move || {
                let mut n = 0;
                while cc.recv().is_some() {
                    n += 1;
                }
                recv.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        p * per,
        "brokered lost/dup'd messages"
    );
}

// ---- external MPMC baselines ----------------------------------------------
// All of crossbeam-channel / flume / kanal expose Clone senders AND receivers,
// send()->Result, recv()->Result — so one macro drives all three.

macro_rules! tp_ext {
    ($p:expr, $c:expr, $per:expr, $mk:expr) => {{
        let (tx, rx) = $mk;
        let recv = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..$p {
                let tx = tx.clone();
                s.spawn(move || {
                    for i in 0..$per as u64 {
                        tx.send(i).unwrap();
                    }
                });
            }
            drop(tx);
            for _ in 0..$c {
                let rx = rx.clone();
                let recv = &recv;
                s.spawn(move || {
                    let mut n = 0;
                    while rx.recv().is_ok() {
                        n += 1;
                    }
                    recv.fetch_add(n, Ordering::Relaxed);
                });
            }
            drop(rx);
        });
        assert_eq!(
            recv.load(Ordering::Relaxed),
            $p * $per,
            "ext channel lost/dup'd messages"
        );
    }};
}

// ---- skewed-load bench ----------------------------------------------------
// Funnels most/all work through ONE hot producer and gives each consumer a fixed
// per-message cost, so *consumption distribution* is the bottleneck, not push
// rate. This is the real competing-consumer test: can C consumers share the work
// when it all arrives on one producer's stream?
//
// brokerless shards by producer and a ring's claim is exclusive, so at most ONE
// consumer can drain a hot ring at a time -> it CANNOT parallelize a single
// producer's stream (expect ~1 consumer's worth of throughput regardless of C).
// brokered's router fans the hot stream out to C consumer rings, and crossbeam's
// shared queue lets C consumers pull in parallel -> both should scale with C.

/// Fixed synthetic per-message work so consumers, not the channel, set the pace.
#[inline]
fn do_work(iters: u64) {
    let mut x = 0u64;
    for i in 0..iters {
        x = std::hint::black_box(x.wrapping_add(i ^ 0x9e3779b9));
    }
    std::hint::black_box(x);
}

/// Per-producer send counts summing to `m`: producer 0 gets `hot_pct`%, the rest
/// split the remainder (leftover rounded onto the hot producer).
fn skew_counts(p: usize, m: usize, hot_pct: usize) -> Vec<usize> {
    let mut counts = vec![0usize; p];
    if p == 1 {
        counts[0] = m;
        return counts;
    }
    let hot = m * hot_pct / 100;
    let rest = m - hot;
    let each = rest / (p - 1);
    for c in counts.iter_mut().skip(1) {
        *c = each;
    }
    counts[0] = hot + (rest - each * (p - 1));
    counts
}

fn tp_brokerless_skew(counts: &[usize], c: usize, cap: usize, work: u64) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(counts.len())
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    let total: usize = counts.iter().sum();
    std::thread::scope(|s| {
        for (i, mut prod) in prods.drain(..).enumerate() {
            let n = counts[i];
            s.spawn(move || {
                for j in 0..n as u64 {
                    prod.send(j);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut k = 0;
                while cc.recv().is_some() {
                    do_work(work);
                    k += 1;
                }
                recv.fetch_add(k, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        total,
        "brokerless lost/dup'd messages"
    );
}

fn tp_brokerless_drain_skew(counts: &[usize], c: usize, cap: usize, work: u64) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(counts.len())
        .consumers(c)
        .capacity(cap)
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    let total: usize = counts.iter().sum();
    std::thread::scope(|s| {
        for (i, mut prod) in prods.drain(..).enumerate() {
            let n = counts[i];
            s.spawn(move || {
                for j in 0..n as u64 {
                    prod.send(j);
                }
            });
        }
        for mut cc in consumer_handles {
            let recv = &recv;
            s.spawn(move || {
                let mut k = 0;
                cc.for_each(64, |_v| {
                    do_work(work);
                    k += 1;
                });
                recv.fetch_add(k, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        total,
        "brokerless-drain lost/dup'd messages"
    );
}

fn tp_brokered_skew(counts: &[usize], c: usize, cap: usize, work: u64) {
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(counts.len())
        .consumers(c)
        .capacity(cap)
        .manual()
        .open()
        .unwrap();
    let recv = AtomicUsize::new(0);
    let total: usize = counts.iter().sum();
    std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for (i, mut prod) in prods.drain(..).enumerate() {
            let n = counts[i];
            s.spawn(move || {
                for j in 0..n as u64 {
                    prod.send(j);
                }
            });
        }
        for mut cc in cons {
            let recv = &recv;
            s.spawn(move || {
                let mut k = 0;
                while cc.recv().is_some() {
                    do_work(work);
                    k += 1;
                }
                recv.fetch_add(k, Ordering::Relaxed);
            });
        }
    });
    assert_eq!(
        recv.load(Ordering::Relaxed),
        total,
        "brokered lost/dup'd messages"
    );
}

macro_rules! tp_ext_skew {
    ($counts:expr, $c:expr, $work:expr, $mk:expr) => {{
        let (tx, rx) = $mk;
        let recv = AtomicUsize::new(0);
        let total: usize = $counts.iter().sum();
        std::thread::scope(|s| {
            for &n in $counts.iter() {
                let tx = tx.clone();
                s.spawn(move || {
                    for j in 0..n as u64 {
                        tx.send(j).unwrap();
                    }
                });
            }
            drop(tx);
            for _ in 0..$c {
                let rx = rx.clone();
                let recv = &recv;
                s.spawn(move || {
                    let mut k = 0;
                    while rx.recv().is_ok() {
                        do_work($work);
                        k += 1;
                    }
                    recv.fetch_add(k, Ordering::Relaxed);
                });
            }
            drop(rx);
        });
        assert_eq!(
            recv.load(Ordering::Relaxed),
            total,
            "ext channel lost/dup'd messages"
        );
    }};
}

fn run_skew(a: &[String]) {
    let p: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let c: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let m: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);
    let cap: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let work: u64 = a.get(6).and_then(|s| s.parse().ok()).unwrap_or(500);
    let hot_pct: usize = a.get(7).and_then(|s| s.parse().ok()).unwrap_or(100);
    let counts = skew_counts(p, m, hot_pct);

    println!(
        "MPMC SKEW  producers={p} consumers={c} total={m} cap={cap} work={work} hot={hot_pct}%"
    );
    println!("per-producer counts = {counts:?}");
    println!(
        "(consumption is the bottleneck: ideal = C-way speedup over 1 consumer; {} cores)\n",
        num_threads_hint()
    );

    bench("brokerless (recv)", m, || {
        tp_brokerless_skew(&counts, c, cap, work)
    });
    bench("brokerless (drain)", m, || {
        tp_brokerless_drain_skew(&counts, c, cap, work)
    });
    bench("brokered", m, || tp_brokered_skew(&counts, c, cap, work));
    bench("crossbeam-bounded", m, || {
        tp_ext_skew!(&counts, c, work, crossbeam_channel::bounded::<u64>(cap))
    });
    bench("crossbeam-unbounded", m, || {
        tp_ext_skew!(&counts, c, work, crossbeam_channel::unbounded::<u64>())
    });
    bench("flume-bounded", m, || {
        tp_ext_skew!(&counts, c, work, flume::bounded::<u64>(cap))
    });
    bench("kanal-bounded", m, || {
        tp_ext_skew!(&counts, c, work, kanal::bounded::<u64>(cap))
    });
}

// ---- dynamic-registry runtimes --------------------------------------------
// Register all producers at runtime, then compare the two membership runtimes
// (locked snapshot vs lock-free list) against the static brokerless ceiling.

macro_rules! tp_dyn {
    ($ctor:expr, $p:expr, $c:expr, $per:expr) => {{
        let (reg, consumer_handles) = $ctor;
        let recv = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..$p {
                let reg = reg.clone();
                s.spawn(move || {
                    let mut prod = reg.register(); // mint the ring at runtime
                    for i in 0..$per as u64 {
                        prod.send(i);
                    }
                });
            }
            for mut cc in consumer_handles {
                let recv = &recv;
                s.spawn(move || {
                    let mut n = 0;
                    while cc.recv().is_some() {
                        n += 1;
                    }
                    recv.fetch_add(n, Ordering::Relaxed);
                });
            }
            drop(reg);
        });
        assert_eq!(
            recv.load(Ordering::Relaxed),
            $p * $per,
            "dynamic lost/dup'd messages"
        );
    }};
}

fn run_dynamic(a: &[String]) {
    let p: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let c: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let cap: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let total = p * per;

    println!("MPMC DYNAMIC  producers={p} consumers={c} per={per} cap={cap} total={total}");
    println!("(all producers registered at runtime; static brokerless = the fixed-set ceiling)\n");

    bench("dyn-locked", total, || {
        tp_dyn!(
            brokerless::dynamic::locked::<u64>()
                .consumers(c)
                .capacity(cap)
                .open()
                .unwrap(),
            p,
            c,
            per
        )
    });
    bench("dyn-array", total, || {
        tp_dyn!(
            brokerless::dynamic::array::<u64>(p)
                .consumers(c)
                .capacity(cap)
                .open()
                .unwrap(),
            p,
            c,
            per
        )
    });
    bench("static-brokerless", total, || tp_brokerless(p, c, per, cap));
    bench("crossbeam-bounded", total, || {
        tp_ext!(p, c, per, crossbeam_channel::bounded::<u64>(cap))
    });
}

fn tp_mpsc_fixed_drain(p: usize, per: usize, cap: usize) {
    use prescient::mpsc::fixed;
    let (prods, mut rx) = fixed::channel::<u64>()
        .producers(p)
        .capacity(cap)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for mut prod in prods {
            s.spawn(move || {
                for i in 0..per as u64 {
                    prod.send(i).unwrap();
                }
            });
        }
        let mut n = 0usize;
        rx.drain(|_| n += 1);
        assert_eq!(n, p * per);
    });
}

/// The full permutation sweep at one (P, C): topology x backend x consumer path
/// x broker count, plus the external baselines.
fn run_matrix(a: &[String]) {
    use prescient::backend::{Ring, Seg};
    let p: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let c: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(500_000);
    let cap: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let total = p * per;

    println!("MATRIX  P={p} C={c} per={per} cap={cap} total={total}");
    if c == 1 {
        bench("mpsc-fixed drain", total, || {
            tp_mpsc_fixed_drain(p, per, cap)
        });
    }
    bench("brless ring recv", total, || {
        tp_brokerless_b_recv::<Ring>(p, c, per, cap)
    });
    bench("brless ring drain", total, || {
        tp_brokerless_b::<Ring>(p, c, per, cap)
    });
    bench("brless seg  recv", total, || {
        tp_brokerless_b_recv::<Seg>(p, c, per, cap)
    });
    bench("brless seg  drain", total, || {
        tp_brokerless_b::<Seg>(p, c, per, cap)
    });
    bench("dyn-locked (ring)", total, || {
        tp_dyn!(
            brokerless::dynamic::locked::<u64>()
                .consumers(c)
                .capacity(cap)
                .open()
                .unwrap(),
            p,
            c,
            per
        )
    });
    bench("dyn-array  (ring)", total, || {
        tp_dyn!(
            brokerless::dynamic::array::<u64>(p)
                .consumers(c)
                .capacity(cap)
                .open()
                .unwrap(),
            p,
            c,
            per
        )
    });
    bench("brokered ring x1", total, || {
        tp_brokered_nb::<Ring>(p, c, 1, per, cap)
    });
    bench("brokered ring x2", total, || {
        tp_brokered_nb::<Ring>(p, c, 2, per, cap)
    });
    bench("brokered seg  x1", total, || {
        tp_brokered_nb::<Seg>(p, c, 1, per, cap)
    });
    bench("crossbeam-bounded", total, || {
        tp_ext!(p, c, per, crossbeam_channel::bounded::<u64>(cap))
    });
    bench("flume-bounded", total, || {
        tp_ext!(p, c, per, flume::bounded::<u64>(cap))
    });
    bench("kanal-bounded", total, || {
        tp_ext!(p, c, per, kanal::bounded::<u64>(cap))
    });
}

/// Broadcast throughput with one publisher and `R` readers.
/// Reports input messages per second and total delivered reads per second.
fn run_broadcast(a: &[String]) {
    use prescient::mpmc::broadcast;
    let r: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let n: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);
    let cap: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(16384);

    println!("BROADCAST  readers={r} msgs={n} cap={cap}");
    let mut rates = Vec::new();
    for _ in 0..REPS {
        let (mut publisher, r0) = broadcast::channel::<u64>().capacity(cap).open().unwrap();
        let readers: Vec<_> = (1..r)
            .map(|_| publisher.subscribe().expect("reader slot"))
            .chain([r0])
            .collect();
        let start = Instant::now();
        std::thread::scope(|s| {
            for mut rd in readers {
                s.spawn(move || {
                    let mut expect = 0u64;
                    while let Some(v) = rd.recv() {
                        assert_eq!(v, expect);
                        expect += 1;
                    }
                    assert_eq!(expect, n as u64);
                });
            }
            s.spawn(move || {
                for i in 0..n as u64 {
                    publisher.send(i);
                }
            });
        });
        rates.push(n as f64 / start.elapsed().as_secs_f64());
    }
    let msgs = median(rates);
    println!("writer throughput   {:>8.2} M msg/s", msgs / 1e6);
    println!(
        "total reads         {:>8.2} M reads/s  ({} readers x every msg)",
        msgs * r as f64 / 1e6,
        r
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.get(1).map(|s| s.as_str()) == Some("broadcast") {
        run_broadcast(&a);
        return;
    }
    if a.get(1).map(|s| s.as_str()) == Some("matrix") {
        run_matrix(&a);
        return;
    }
    if a.get(1).map(|s| s.as_str()) == Some("skew") {
        run_skew(&a);
        return;
    }
    if a.get(1).map(|s| s.as_str()) == Some("dynamic") {
        run_dynamic(&a);
        return;
    }
    let p: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let c: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let per: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let cap: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let total = p * per;

    println!("MPMC throughput  producers={p} consumers={c} per={per} cap={cap} total={total}");
    println!(
        "(threads: ours P+C, brokered P+C+1, externals P+C; machine has {} cores)\n",
        num_threads_hint()
    );

    bench("brokerless (recv)", total, || tp_brokerless(p, c, per, cap));
    bench("brokerless (drain)", total, || {
        tp_brokerless_drain(p, c, per, cap)
    });
    bench("brokerless (seg)", total, || {
        tp_brokerless_b::<prescient::backend::Seg>(p, c, per, cap)
    });
    bench("brokered", total, || tp_brokered(p, c, per, cap));
    bench("brokered x2", total, || tp_brokered_n(p, c, 2, per, cap));
    bench("brokered x3", total, || tp_brokered_n(p, c, 3, per, cap));
    bench("crossbeam-bounded", total, || {
        tp_ext!(p, c, per, crossbeam_channel::bounded::<u64>(cap))
    });
    bench("crossbeam-unbounded", total, || {
        tp_ext!(p, c, per, crossbeam_channel::unbounded::<u64>())
    });
    bench("flume-bounded", total, || {
        tp_ext!(p, c, per, flume::bounded::<u64>(cap))
    });
    bench("flume-unbounded", total, || {
        tp_ext!(p, c, per, flume::unbounded::<u64>())
    });
    bench("kanal-bounded", total, || {
        tp_ext!(p, c, per, kanal::bounded::<u64>(cap))
    });
}

fn num_threads_hint() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0)
}
