//! Verification for the `Seg` (unbounded segmented) backend: direct endpoint
//! tests across segment boundaries, plus the brokerless MPMC suite run over it
//! via `.backend::<Seg>()` — no-loss/no-dup and drop-exactly-once, including the
//! send-vs-consumer-teardown race that once bit the ring.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prescient::backend::{Backend, Rx as _, Seg, Tx as _};
use prescient::mpmc::brokerless;

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

fn per(default: u64) -> u64 {
    match std::env::var("MPMC_PER")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(cap) => default.min(cap),
        None => default,
    }
}

// ---- direct endpoint tests --------------------------------------------------

#[test]
fn seg_fifo_across_segment_boundaries() {
    // seg_size 4 forces boundary crossings every 4 values.
    let (mut tx, mut rx) = Seg::channel::<u64>(4);
    for i in 0..1000u64 {
        assert!(
            tx.try_push(i).is_ok(),
            "unbounded: push never fails while rx lives"
        );
    }
    for i in 0..1000u64 {
        assert_eq!(rx.try_pop(), Some(i), "FIFO order across segments");
    }
    assert!(rx.try_pop().is_none());
    assert!(rx.is_empty());
}

#[test]
fn seg_interleaved_push_pop() {
    let (mut tx, mut rx) = Seg::channel::<u64>(3);
    let mut expect = 0u64;
    for round in 0..500u64 {
        for k in 0..(round % 5 + 1) {
            tx.try_push(round * 10 + k).unwrap();
        }
        for _ in 0..(round % 5 + 1) {
            let v = rx.try_pop().unwrap();
            let _ = v;
            expect += 1;
        }
    }
    assert!(rx.is_empty());
    assert_eq!(expect, (0..500u64).map(|r| r % 5 + 1).sum::<u64>());
}

#[test]
fn seg_drain_in_place_batches() {
    let (mut tx, mut rx) = Seg::channel::<u64>(8);
    for i in 0..100u64 {
        tx.try_push(i).unwrap();
    }
    let mut got = Vec::new();
    // Drain in odd-sized batches to hit partial-segment and cross-segment cases.
    while rx.drain_in_place(7, |v| got.push(v)) > 0 {}
    assert_eq!(got, (0..100u64).collect::<Vec<_>>());
}

#[test]
fn seg_is_finished_semantics() {
    let (mut tx, mut rx) = Seg::channel::<u64>(4);
    tx.try_push(1).unwrap();
    assert!(!rx.is_finished(), "producer alive");
    drop(tx);
    assert!(!rx.is_finished(), "value still buffered");
    assert_eq!(rx.try_pop(), Some(1));
    assert!(rx.is_finished(), "producer gone + drained");
}

#[test]
fn seg_push_after_rx_drop_returns_err_and_drops_value() {
    let live = Arc::new(AtomicI64::new(0));
    let (mut tx, rx) = Seg::channel::<Tracked>(4);
    assert!(tx.try_push(Tracked::new(0, &live)).is_ok());
    drop(rx);
    // rx gone: push must refuse (Err returns the value; dropping it here).
    assert!(tx.try_push(Tracked::new(1, &live)).is_err());
    drop(tx);
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "both values dropped exactly once"
    );
}

#[test]
fn seg_undrained_teardown_drops_every_value_once() {
    let live = Arc::new(AtomicI64::new(0));
    let (mut tx, mut rx) = Seg::channel::<Tracked>(4);
    // Fill across many segments, consume PART (stops mid-segment), then drop all.
    for i in 0..37u64 {
        assert!(tx.try_push(Tracked::new(i, &live)).is_ok());
    }
    for _ in 0..13 {
        drop(rx.try_pop().unwrap());
    }
    assert_eq!(live.load(Ordering::Relaxed), 37 - 13);
    drop(rx);
    drop(tx);
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "unconsumed span dropped exactly once"
    );
}

#[test]
fn seg_teardown_race_strands_no_value() {
    // Producer hammers try_push while the consumer drops concurrently — the race
    // that stranded a value in the ring before Inner::drop. Every constructed
    // value must be dropped exactly once, delivered or not.
    let iters: u64 = std::env::var("ADV_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    for _ in 0..iters {
        let live = Arc::new(AtomicI64::new(0));
        let (mut tx, mut rx) = Seg::channel::<Tracked>(2);
        // Pre-fill so the consumer has something mid-flight.
        assert!(tx.try_push(Tracked::new(0, &live)).is_ok());
        assert!(tx.try_push(Tracked::new(1, &live)).is_ok());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b2 = Arc::clone(&barrier);
        let live2 = Arc::clone(&live);
        std::thread::scope(|s| {
            s.spawn(move || {
                b2.wait();
                for i in 2..10u64 {
                    let _ = tx.try_push(Tracked::new(i, &live2)); // Err drops here
                }
            });
            barrier.wait();
            let _ = rx.try_pop();
            drop(rx); // race: consumer leaves while producer pushes
        });
        assert_eq!(
            live.load(Ordering::Relaxed),
            0,
            "value stranded by teardown race"
        );
    }
}

// ---- brokerless MPMC over the Seg backend ------------------------------------

#[test]
fn brokerless_seg_no_loss_no_dup() {
    let per = per(40_000);
    let producers = 4usize;
    let consumers = 4usize;
    let (mut prods, consumer_handles) = brokerless::channel::<u64>()
        .backend::<Seg>()
        .producers(producers)
        .consumers(consumers)
        .capacity(64)
        .open()
        .unwrap();
    let got: Vec<u64> = std::thread::scope(|s| {
        let mut ph = Vec::new();
        for (p, mut prod) in prods.drain(..).enumerate() {
            ph.push(s.spawn(move || {
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
        for h in ph {
            h.join().unwrap();
        }
        ch.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let total = producers as u64 * per;
    assert_eq!(got.len() as u64, total, "loss or duplication");
    let set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "duplicate delivery");
    assert_eq!(set, (0..total).collect::<HashSet<u64>>());
}

#[test]
fn brokerless_seg_drops_every_value_exactly_once() {
    let live = Arc::new(AtomicI64::new(0));
    let per = per(10_000);
    {
        let (mut prods, consumer_handles) = brokerless::channel::<Tracked>()
            .backend::<Seg>()
            .producers(4)
            .consumers(3)
            .capacity(32)
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
fn brokerless_seg_send_never_blocks() {
    // Unbounded: a producer can run arbitrarily far ahead of a slow consumer.
    let (mut prods, mut consumers) = brokerless::channel::<u64>()
        .backend::<Seg>()
        .capacity(16)
        .open()
        .unwrap();
    let mut cons = consumers.pop().unwrap();
    let mut prod = prods.pop().unwrap();
    for i in 0..100_000u64 {
        assert!(
            prod.try_send(i).is_ok(),
            "seg backend must never report full"
        );
    }
    drop(prod);
    let mut n = 0u64;
    while cons.recv().is_some() {
        n += 1;
    }
    assert_eq!(n, 100_000);
}
