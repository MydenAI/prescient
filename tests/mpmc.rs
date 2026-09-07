//! Correctness for the competing-consumer MPMC channels: every value delivered to
//! exactly one consumer (no loss, no duplication), and every value dropped exactly
//! once on teardown.
//!
//! No-loss/no-dup is checked with *disjoint* per-producer ranges: producer `p`
//! sends `[p*PER, p*PER + PER)`. The union of everything all consumers received
//! must equal the full set — a missing value is a lost message, a repeated value
//! is a duplicated delivery.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prescient::mpmc::{brokered, brokerless};

/// Payload that bumps a shared counter on construction and drops it on Drop, so a
/// non-zero final count means a value was leaked or double-dropped.
struct Tracked {
    _v: u64,
    live: Arc<AtomicI64>,
}
impl Tracked {
    fn new(v: u64, live: &Arc<AtomicI64>) -> Self {
        live.fetch_add(1, Ordering::Relaxed);
        Tracked {
            _v: v,
            live: Arc::clone(live),
        }
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

fn expected_set(producers: usize, per: u64) -> HashSet<u64> {
    (0..producers as u64)
        .flat_map(|p| p * per..p * per + per)
        .collect()
}

/// Cap per-producer message count from the env so sanitizer/Miri runs stay small.
/// `MPMC_PER=2000 cargo +nightly test ...` shrinks every case to <=2000 each.
fn per(default: u64) -> u64 {
    match std::env::var("MPMC_PER")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(cap) => default.min(cap),
        None => default,
    }
}

// ---- brokerless -----------------------------------------------------------

fn brokerless_roundtrip(producers: usize, consumers: usize, per: u64, cap: usize) {
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(producers)
        .consumers(consumers)
        .capacity(cap)
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        // Producers: disjoint ranges.
        let mut ph = Vec::new();
        for (p, mut prod) in prods.drain(..).enumerate() {
            ph.push(s.spawn(move || {
                for v in p as u64 * per..p as u64 * per + per {
                    prod.send(v);
                }
            }));
        }
        // Consumers: each drains to completion into a local Vec.
        let mut ch = Vec::new();
        for mut c in consumer_handles {
            ch.push(s.spawn(move || {
                let mut local = Vec::new();
                while let Some(v) = c.recv() {
                    local.push(v);
                }
                local
            }));
        }
        for h in ph {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });

    let expected = expected_set(producers, per);
    assert_eq!(
        got.len(),
        expected.len(),
        "count mismatch => loss or duplication"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(
        set.len(),
        got.len(),
        "duplicate delivery (a value reached >1 consumer)"
    );
    assert_eq!(set, expected, "delivered set != sent set");
}

#[test]
fn brokerless_1p_1c() {
    brokerless_roundtrip(1, 1, per(50_000), 256);
}

#[test]
fn brokerless_4p_4c() {
    brokerless_roundtrip(4, 4, per(40_000), 256);
}

#[test]
fn brokerless_8p_2c() {
    brokerless_roundtrip(8, 2, per(20_000), 128);
}

#[test]
fn brokerless_2p_8c() {
    // More consumers than producer rings: heavy claim contention on few rings.
    brokerless_roundtrip(2, 8, per(40_000), 64);
}

#[test]
fn brokerless_cap1_stress() {
    // Capacity-1 rings force maximal producer/consumer handoff churn.
    brokerless_roundtrip(4, 4, per(20_000), 1);
}

#[test]
fn brokerless_drops_every_value_exactly_once() {
    let live = Arc::new(AtomicI64::new(0));
    let per = per(10_000);
    let producers = 4;
    {
        let (mut prods, consumer_handles) = brokerless::channel::<Tracked>()
            .producers(producers)
            .consumers(3)
            .capacity(128)
            .open()
            .unwrap();
        std::thread::scope(|s| {
            for (p, mut prod) in prods.drain(..).enumerate() {
                let live = Arc::clone(&live);
                s.spawn(move || {
                    for v in 0..per {
                        prod.send(Tracked::new(p as u64 * per + v, &live));
                    }
                });
            }
            for mut c in consumer_handles {
                s.spawn(move || while c.recv().is_some() {});
            }
        });
    }
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "value leaked or double-dropped"
    );
}

#[test]
fn brokerless_undrained_teardown_drops_all() {
    // Fill rings, then drop everything WITHOUT consuming: the ring Drop backstop
    // must destroy every stranded value exactly once.
    let live = Arc::new(AtomicI64::new(0));
    let (mut prods, cons0) = brokerless::channel::<Tracked>()
        .producers(3)
        .capacity(64)
        .open()
        .unwrap();
    for (p, prod) in prods.iter_mut().enumerate() {
        for v in 0..40u64 {
            assert!(
                prod.try_send(Tracked::new(p as u64 * 40 + v, &live))
                    .is_ok()
            );
        }
    }
    assert!(live.load(Ordering::Relaxed) > 0);
    drop(cons0);
    drop(prods);
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "undrained values not dropped exactly once"
    );
}

#[test]
fn brokerless_send_after_all_consumers_gone_terminates() {
    // Regression: with no consumer to drain, a blocking send must give up on a
    // full ring instead of spinning forever. cap=4 is already a power of two, so
    // exactly 4 values fit; every further send is refused. The test *completing*
    // is the real proof — a hang would time the runner out.
    let cap = 4;
    let (mut prods, cons0) = brokerless::channel::<u64>()
        .producers(1)
        .capacity(cap)
        .open()
        .unwrap();
    drop(cons0); // no consumers remain
    let prod = &mut prods[0];
    let mut delivered = 0usize;
    for i in 0..1000u64 {
        if prod.send(i) {
            delivered += 1;
        }
    }
    assert_eq!(
        delivered, cap,
        "expected exactly cap sends to succeed, got {delivered}"
    );
}

/// Skewed per-producer load: producer `i` sends the disjoint range
/// `[start_i, start_i + counts[i])`. Verifies competing consumers still deliver
/// every value exactly once when almost all of it funnels through one producer.
fn brokerless_roundtrip_skewed(counts: &[usize], consumers: usize, cap: usize) {
    let mut starts = Vec::with_capacity(counts.len());
    let mut acc = 0u64;
    for &n in counts {
        starts.push(acc);
        acc += n as u64;
    }
    let total = acc;
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(counts.len())
        .consumers(consumers)
        .capacity(cap)
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        let mut ph = Vec::new();
        for (i, mut prod) in prods.drain(..).enumerate() {
            let (start, n) = (starts[i], counts[i] as u64);
            ph.push(s.spawn(move || {
                for v in start..start + n {
                    prod.send(v);
                }
            }));
        }
        let mut ch = Vec::new();
        for mut c in consumer_handles {
            ch.push(s.spawn(move || {
                let mut local = Vec::new();
                while let Some(v) = c.recv() {
                    local.push(v);
                }
                local
            }));
        }
        for h in ph {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(
        got.len() as u64,
        total,
        "count mismatch => loss or duplication"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "duplicate delivery");
    assert_eq!(
        set,
        (0..total).collect::<HashSet<u64>>(),
        "delivered set != sent set"
    );
}

#[test]
fn brokerless_skewed_hot_producer_no_loss() {
    // ~99% of the load on one producer, three near-idle; 4 consumers compete for
    // the hot ring. The claim is exclusive per drain, but work is delivered off it.
    brokerless_roundtrip_skewed(&[per(60_000) as usize, 200, 200, 200], 4, 256);
}

#[test]
fn brokerless_fully_skewed_single_stream_no_loss() {
    // 100% on producer 0, the other three rings empty; 6 consumers.
    brokerless_roundtrip_skewed(&[per(60_000) as usize, 0, 0, 0], 6, 128);
}

/// Same delivery guarantee as `brokerless_roundtrip`, but consumers use the
/// zero-copy `for_each` path (in-place processing under the claim) instead of recv.
fn brokerless_for_each_roundtrip(counts: &[usize], consumers: usize, cap: usize) {
    let mut starts = Vec::with_capacity(counts.len());
    let mut acc = 0u64;
    for &n in counts {
        starts.push(acc);
        acc += n as u64;
    }
    let total = acc;
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .producers(counts.len())
        .consumers(consumers)
        .capacity(cap)
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        let mut ph = Vec::new();
        for (i, mut prod) in prods.drain(..).enumerate() {
            let (start, n) = (starts[i], counts[i] as u64);
            ph.push(s.spawn(move || {
                for v in start..start + n {
                    prod.send(v);
                }
            }));
        }
        let mut ch = Vec::new();
        for mut c in consumer_handles {
            ch.push(s.spawn(move || {
                let mut local = Vec::new();
                c.for_each(64, |v| local.push(v));
                local
            }));
        }
        for h in ph {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(
        got.len() as u64,
        total,
        "count mismatch => loss or duplication"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "duplicate delivery");
    assert_eq!(
        set,
        (0..total).collect::<HashSet<u64>>(),
        "delivered set != sent set"
    );
}

#[test]
fn brokerless_for_each_balanced_no_loss() {
    let per = per(40_000) as usize;
    brokerless_for_each_roundtrip(&[per, per, per, per], 4, 256);
}

#[test]
fn brokerless_for_each_skewed_no_loss() {
    // Zero-copy path under a hot producer + 6 competing consumers.
    brokerless_for_each_roundtrip(&[per(60_000) as usize, 0, 0, 0], 6, 128);
}

// ---- brokered -------------------------------------------------------------

fn brokered_roundtrip(producers: usize, consumers: usize, per: u64, cap: usize) {
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(producers)
        .consumers(consumers)
        .capacity(cap)
        .manual()
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        let bh: Vec<_> = brokers
            .into_iter()
            .map(|b| s.spawn(move || b.run()))
            .collect();
        let mut ph = Vec::new();
        for (p, mut prod) in prods.drain(..).enumerate() {
            ph.push(s.spawn(move || {
                for v in p as u64 * per..p as u64 * per + per {
                    prod.send(v);
                }
            }));
        }
        let mut ch = Vec::new();
        for mut c in cons {
            ch.push(s.spawn(move || {
                let mut local = Vec::new();
                while let Some(v) = c.recv() {
                    local.push(v);
                }
                local
            }));
        }
        for h in ph {
            h.join().unwrap();
        }
        for h in bh {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });

    let expected = expected_set(producers, per);
    assert_eq!(
        got.len(),
        expected.len(),
        "count mismatch => loss or duplication"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(
        set.len(),
        got.len(),
        "duplicate delivery (a value reached >1 consumer)"
    );
    assert_eq!(set, expected, "delivered set != sent set");
}

#[test]
fn brokered_1p_1c() {
    brokered_roundtrip(1, 1, per(50_000), 256);
}

#[test]
fn brokered_4p_4c() {
    brokered_roundtrip(4, 4, per(40_000), 256);
}

#[test]
fn brokered_8p_2c() {
    brokered_roundtrip(8, 2, per(20_000), 128);
}

#[test]
fn brokered_2p_8c() {
    brokered_roundtrip(2, 8, per(40_000), 64);
}

#[test]
fn brokered_cap1_stress() {
    brokered_roundtrip(4, 4, per(20_000), 1);
}

#[test]
fn brokered_router_affinity() {
    // Route by value class: value v -> consumer (v % NC). Each consumer must see
    // ONLY its class (proves the closure controls placement), and every value must
    // still be delivered exactly once.
    const NC: usize = 3;
    let per = per(30_000);
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(2)
        .consumers(NC)
        .capacity(256)
        .route(|v, n| (*v as usize) % n)
        .manual()
        .open()
        .unwrap();
    let got: Vec<Vec<u64>> = std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for (p, mut prod) in prods.drain(..).enumerate() {
            s.spawn(move || {
                for v in p as u64 * per..p as u64 * per + per {
                    prod.send(v);
                }
            });
        }
        cons.into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    let mut local = Vec::new();
                    while let Some(v) = c.recv() {
                        local.push(v);
                    }
                    local
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });

    for (c, vals) in got.iter().enumerate() {
        assert!(
            vals.iter().all(|v| (*v as usize) % NC == c),
            "consumer {c} received an out-of-class value — routing not honored"
        );
    }
    let total: usize = got.iter().map(|v| v.len()).sum();
    assert_eq!(total as u64, 2 * per, "loss or duplication");
    let set: HashSet<u64> = got.iter().flatten().copied().collect();
    assert_eq!(set.len(), total, "duplicate delivery");
}

#[test]
fn brokered_pubsub_route_by_enum() {
    use prescient::mpmc::brokered::Targets;
    #[derive(Clone, Debug)]
    #[allow(dead_code)] // payloads make messages realistic; the test checks routing
    enum Ev {
        Metric(u64),
        Log(u64),
        Both(u64),
    }
    let n_each = per(10_000);
    // Route by topic: Metric → consumer 0, Log → consumer 1, Both → BOTH (fan-out).
    let (mut prods, cons, brokers) = brokered::channel::<Ev>()
        .consumers(2)
        .capacity(256)
        .pubsub(|e, _n| match e {
            Ev::Metric(_) => Targets::one(0),
            Ev::Log(_) => Targets::one(1),
            Ev::Both(_) => Targets::of([0, 1]),
        })
        .manual()
        .open()
        .unwrap();
    let got: Vec<Vec<Ev>> = std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        let mut prod = prods.pop().unwrap();
        s.spawn(move || {
            for i in 0..n_each {
                prod.send(Ev::Metric(i));
                prod.send(Ev::Log(i));
                prod.send(Ev::Both(i));
            }
        });
        cons.into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    let mut v = Vec::new();
                    while let Some(e) = c.recv() {
                        v.push(e);
                    }
                    v
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    let (c0, c1) = (&got[0], &got[1]);
    // No cross-delivery: c0 never sees a Log, c1 never sees a Metric.
    assert!(
        c0.iter().all(|e| matches!(e, Ev::Metric(_) | Ev::Both(_))),
        "c0 received a Log"
    );
    assert!(
        c1.iter().all(|e| matches!(e, Ev::Log(_) | Ev::Both(_))),
        "c1 received a Metric"
    );
    let count = |v: &[Ev], f: fn(&Ev) -> bool| v.iter().filter(|e| f(e)).count() as u64;
    assert_eq!(
        count(c0, |e| matches!(e, Ev::Metric(_))),
        n_each,
        "missing metrics"
    );
    assert_eq!(
        count(c1, |e| matches!(e, Ev::Log(_))),
        n_each,
        "missing logs"
    );
    // Fan-out: every Both reached BOTH consumers.
    assert_eq!(
        count(c0, |e| matches!(e, Ev::Both(_))),
        n_each,
        "Both not fanned to c0"
    );
    assert_eq!(
        count(c1, |e| matches!(e, Ev::Both(_))),
        n_each,
        "Both not fanned to c1"
    );
}

#[test]
fn brokered_drops_every_value_exactly_once() {
    let live = Arc::new(AtomicI64::new(0));
    let per = per(10_000);
    let producers = 4;
    {
        let (mut prods, cons, brokers) = brokered::channel::<Tracked>()
            .producers(producers)
            .consumers(3)
            .capacity(128)
            .manual()
            .open()
            .unwrap();
        std::thread::scope(|s| {
            for b in brokers {
                s.spawn(move || b.run());
            }
            for (p, mut prod) in prods.drain(..).enumerate() {
                let live = Arc::clone(&live);
                s.spawn(move || {
                    for v in 0..per {
                        prod.send(Tracked::new(p as u64 * per + v, &live));
                    }
                });
            }
            for mut c in cons {
                s.spawn(move || while c.recv().is_some() {});
            }
        });
    }
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "value leaked or double-dropped"
    );
}

// ---- dynamic brokerless (both runtimes) -----------------------------------

/// Concurrent runtime registration + competing consumption; producer `p` registers
/// a ring at runtime and sends the disjoint range `[p*per, p*per+per)`. Verifies
/// every value reaches exactly one consumer. Works for either runtime constructor.
macro_rules! dyn_roundtrip {
    ($ctor:expr, $producers:expr, $consumers:expr, $per:expr) => {{
        let (reg, consumer_handles) = $ctor;
        let producers = $producers;
        let per: u64 = $per;
        let got: Vec<u64> = std::thread::scope(|s| {
            let mut ph = Vec::new();
            for p in 0..producers {
                let reg = reg.clone();
                ph.push(s.spawn(move || {
                    let mut prod = reg.register(); // mint the ring at runtime
                    for v in p as u64 * per..p as u64 * per + per {
                        prod.send(v);
                    }
                }));
            }
            let mut ch = Vec::new();
            for mut c in consumer_handles {
                ch.push(s.spawn(move || {
                    let mut local = Vec::new();
                    while let Some(v) = c.recv() {
                        local.push(v);
                    }
                    local
                }));
            }
            drop(reg);
            for h in ph {
                h.join().unwrap();
            }
            ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
        });
        let total = producers as u64 * per;
        assert_eq!(
            got.len() as u64,
            total,
            "count mismatch => loss or duplication"
        );
        let set: HashSet<u64> = got.iter().copied().collect();
        assert_eq!(set.len(), got.len(), "duplicate delivery");
        assert_eq!(
            set,
            (0..total).collect::<HashSet<u64>>(),
            "delivered set != sent set"
        );
    }};
}

#[test]
fn dyn_locked_no_loss() {
    dyn_roundtrip!(
        brokerless::dynamic::locked::<u64>()
            .consumers(4)
            .capacity(256)
            .open()
            .unwrap(),
        6,
        4,
        per(20_000)
    );
}

#[test]
fn dyn_array_no_loss() {
    dyn_roundtrip!(
        brokerless::dynamic::array::<u64>(6)
            .consumers(4)
            .capacity(256)
            .open()
            .unwrap(),
        6,
        4,
        per(20_000)
    );
}

#[test]
fn dyn_locked_more_consumers_than_producers() {
    dyn_roundtrip!(
        brokerless::dynamic::locked::<u64>()
            .consumers(8)
            .capacity(64)
            .open()
            .unwrap(),
        2,
        8,
        per(20_000)
    );
}

#[test]
fn dyn_array_more_consumers_than_producers() {
    dyn_roundtrip!(
        brokerless::dynamic::array::<u64>(2)
            .consumers(8)
            .capacity(64)
            .open()
            .unwrap(),
        2,
        8,
        per(20_000)
    );
}

#[test]
fn dyn_array_drops_every_value_exactly_once() {
    // Exercises the array runtime's teardown: undrained rings must drop their values
    // exactly once when the channel is dropped.
    let live = Arc::new(AtomicI64::new(0));
    let per = per(8_000);
    {
        let (reg, consumer_handles) = brokerless::dynamic::array::<Tracked>(4)
            .consumers(3)
            .capacity(128)
            .open()
            .unwrap();
        std::thread::scope(|s| {
            for p in 0..4u64 {
                let reg = reg.clone();
                let live = Arc::clone(&live);
                s.spawn(move || {
                    let mut prod = reg.register();
                    for v in 0..per {
                        prod.send(Tracked::new(p * per + v, &live));
                    }
                });
            }
            for mut c in consumer_handles {
                s.spawn(move || while c.recv().is_some() {});
            }
            drop(reg);
        });
    }
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "value leaked or double-dropped"
    );
}

#[test]
fn dyn_locked_zero_copy_for_each_no_loss() {
    // Zero-copy path on the locked runtime.
    let per = per(20_000);
    let producers = 5u64;
    let (reg, consumer_handles) = brokerless::dynamic::locked::<u64>()
        .consumers(4)
        .capacity(128)
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        let mut ph = Vec::new();
        for p in 0..producers {
            let reg = reg.clone();
            ph.push(s.spawn(move || {
                let mut prod = reg.register();
                for v in p * per..p * per + per {
                    prod.send(v);
                }
            }));
        }
        let mut ch = Vec::new();
        for mut c in consumer_handles {
            ch.push(s.spawn(move || {
                let mut local = Vec::new();
                c.for_each(64, |v| local.push(v));
                local
            }));
        }
        drop(reg);
        for h in ph {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let total = producers * per;
    assert_eq!(
        got.len() as u64,
        total,
        "count mismatch => loss or duplication"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "duplicate delivery");
    assert_eq!(
        set,
        (0..total).collect::<HashSet<u64>>(),
        "delivered set != sent set"
    );
}

// ---- multi-broker ----------------------------------------------------------

#[test]
fn brokered_multi_broker_no_loss_no_dup() {
    // 6 producers split across 3 broker threads, 4 consumers; disjoint ranges.
    let per = per(20_000);
    let producers = 6usize;
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(producers)
        .consumers(4)
        .brokers(3)
        .capacity(256)
        .manual()
        .open()
        .unwrap();
    assert_eq!(brokers.len(), 3);
    let got: Vec<u64> = std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for (p, mut prod) in prods.drain(..).enumerate() {
            s.spawn(move || {
                for v in p as u64 * per..p as u64 * per + per {
                    prod.send(v);
                }
            });
        }
        cons.into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    let mut local = Vec::new();
                    while let Some(v) = c.recv() {
                        local.push(v);
                    }
                    local
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });
    let total = producers as u64 * per;
    assert_eq!(
        got.len() as u64,
        total,
        "loss or duplication across brokers"
    );
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "duplicate delivery");
    assert_eq!(set, (0..total).collect::<HashSet<u64>>());
}

#[test]
fn brokered_multi_broker_affinity_still_pure() {
    // Routing with 2 brokers: every clone of the closure computes the same
    // key->consumer mapping, so each consumer still sees ONLY its class.
    const NC: usize = 3;
    let per = per(20_000);
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(4)
        .consumers(NC)
        .brokers(2)
        .capacity(256)
        .route(|v, n| (*v as usize) % n)
        .manual()
        .open()
        .unwrap();
    let got: Vec<Vec<u64>> = std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for (p, mut prod) in prods.drain(..).enumerate() {
            s.spawn(move || {
                for v in p as u64 * per..p as u64 * per + per {
                    prod.send(v);
                }
            });
        }
        cons.into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    let mut local = Vec::new();
                    while let Some(v) = c.recv() {
                        local.push(v);
                    }
                    local
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    for (c, vals) in got.iter().enumerate() {
        assert!(
            vals.iter().all(|v| (*v as usize) % NC == c),
            "consumer {c} received an out-of-class value under multi-broker routing"
        );
    }
    let total: usize = got.iter().map(|v| v.len()).sum();
    assert_eq!(total as u64, 4 * per, "loss or duplication");
}

#[test]
fn brokered_multi_broker_pubsub_fanout_exact() {
    use prescient::mpmc::brokered::Targets;
    // Pub-sub with 2 brokers: a broadcast value still reaches BOTH consumers
    // exactly once each (each value is handled by exactly one broker).
    let n = per(15_000);
    let (mut prods, cons, brokers) = brokered::channel::<u64>()
        .producers(2)
        .consumers(2)
        .brokers(2)
        .capacity(256)
        .pubsub(|_, k| Targets::all(k))
        .manual()
        .open()
        .unwrap();
    let counts: Vec<usize> = std::thread::scope(|s| {
        for b in brokers {
            s.spawn(move || b.run());
        }
        for mut prod in prods.drain(..) {
            s.spawn(move || {
                for v in 0..n {
                    prod.send(v);
                }
            });
        }
        cons.into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    let mut k = 0usize;
                    while c.recv().is_some() {
                        k += 1;
                    }
                    k
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    // Each consumer must see every value from both producers exactly once.
    assert_eq!(
        counts,
        vec![2 * n as usize, 2 * n as usize],
        "broadcast fan-out not exact"
    );
}
#[test]
fn brokered_route_panic_disconnects_producer_and_consumer() {
    let (mut producers, mut consumers, mut brokers) = brokered::channel::<u64>()
        .producers(1)
        .consumers(1)
        .capacity(8)
        .route(|_: &u64, _| -> usize { panic!("route panic probe") })
        .manual()
        .open()
        .unwrap();

    assert!(producers[0].send(1));
    let broker = brokers.pop().unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| broker.run()));
    assert!(panic.is_err());
    assert!(!producers[0].send(2));
    assert_eq!(consumers[0].recv(), None);
}
