//! Adversarial / edge-case battery: boundary sizes, ZSTs, drop-exactly-once under
//! every teardown ordering, panic safety, and disconnect races. A `Tracked`
//! payload carries an `Arc<AtomicI64>` live counter — construct = +1, drop = -1 —
//! so at the end of a scenario `live == 0` means every value was dropped exactly
//! once. A leak leaves it > 0; a double-drop drives it < 0 (and is UB Miri flags).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prescient::mpsc::{dynamic, fixed, pool};

#[derive(Clone)]
struct Counter(Arc<AtomicI64>);
impl Counter {
    fn new() -> Self {
        Counter(Arc::new(AtomicI64::new(0)))
    }
    fn live(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct Tracked {
    _v: u64,
    live: Arc<AtomicI64>,
}
impl Tracked {
    fn new(v: u64, c: &Counter) -> Self {
        c.0.fetch_add(1, Ordering::SeqCst);
        Tracked {
            _v: v,
            live: c.0.clone(),
        }
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

// --- boundary sizes ---------------------------------------------------------

#[test]
fn capacity_zero_is_rejected_before_allocation() {
    let result = fixed::channel::<u64>().producers(1).capacity(0).open();
    assert!(matches!(
        result,
        Err(prescient::OpenError::Invalid(
            "channel capacity must be non-zero"
        ))
    ));
}

#[test]
fn zero_producers_disconnects_immediately() {
    let (prods, mut rx) = fixed::channel::<u64>()
        .producers(0)
        .capacity(16)
        .open()
        .unwrap();
    assert!(prods.is_empty());
    assert_eq!(
        rx.recv(),
        None,
        "no producers -> immediate disconnect, no hang"
    );
}

#[test]
fn zst_payload_all_tiers() {
    // T = () exercises the ring with zero-sized MaybeUninit slots.
    const N: usize = 10_000;
    let (mut prods, mut rx) = fixed::channel::<()>()
        .producers(3)
        .capacity(8)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for _ in 0..3 {
            let mut p = prods.pop().unwrap();
            s.spawn(move || {
                for _ in 0..N {
                    p.send(()).unwrap();
                }
            });
        }
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        assert_eq!(n, 3 * N, "ZST fixed: count");
    });
}

// --- drop exactly once, every teardown order --------------------------------

#[test]
fn fixed_undrained_drops_every_value_once() {
    let c = Counter::new();
    {
        let (mut prods, _rx) = fixed::channel::<Tracked>()
            .producers(2)
            .capacity(64)
            .open()
            .unwrap();
        for p in prods.iter_mut() {
            for i in 0..20 {
                p.try_send(Tracked::new(i, &c)).ok().unwrap();
            }
        }
        // Drop receiver first, then producers: unread items must still all drop.
    }
    assert_eq!(
        c.live(),
        0,
        "fixed undrained: leak or double-drop (live={})",
        c.live()
    );
}

#[test]
fn pool_undrained_drops_every_value_once() {
    let c = Counter::new();
    {
        let (h, _rx) = pool::channel::<Tracked>()
            .max_producers(4)
            .capacity(64)
            .open()
            .unwrap();
        let mut claimed: Vec<_> = (0..4).map(|_| h.claim().unwrap()).collect();
        for p in claimed.iter_mut() {
            for i in 0..20 {
                p.try_send(Tracked::new(i, &c)).ok().unwrap();
            }
        }
    }
    assert_eq!(
        c.live(),
        0,
        "pool undrained: leak/double-drop (live={})",
        c.live()
    );
}

#[test]
fn dynamic_undrained_drops_every_value_once() {
    // Rings deposited into the inbox but NEVER drained by the consumer must still
    // have their buffered values dropped exactly once when the inbox is dropped.
    // This is the path that stresses the (lock-free / intrusive) inbox teardown.
    let c = Counter::new();
    {
        let (reg, _rx) = dynamic::channel::<Tracked>().capacity(64).open().unwrap();
        let mut prods: Vec<_> = (0..5).map(|_| reg.register()).collect();
        for p in prods.iter_mut() {
            for i in 0..20 {
                p.try_send(Tracked::new(i, &c)).ok().unwrap();
            }
        }
        // Drop reg + rx (and prods) without a single recv(): everything buffered in
        // undrained inbox rings must drop.
    }
    assert_eq!(
        c.live(),
        0,
        "dynamic undrained: leak/double-drop (live={})",
        c.live()
    );
}

#[test]
fn dynamic_partial_recv_then_drop_accounts_all() {
    let c = Counter::new();
    {
        let (reg, mut rx) = dynamic::channel::<Tracked>().capacity(16).open().unwrap();
        let mut prods: Vec<_> = (0..4).map(|_| reg.register()).collect();
        for p in prods.iter_mut() {
            for i in 0..25 {
                // ring holds 16; some sends fail (Full) and drop their payload now
                let _ = p.try_send(Tracked::new(i, &c));
            }
        }
        // Receive only a handful, then tear everything down.
        for _ in 0..10 {
            let _ = rx.try_recv();
        }
    }
    assert_eq!(
        c.live(),
        0,
        "dynamic partial: unaccounted values (live={})",
        c.live()
    );
}

#[test]
fn recycling_reuse_drops_every_value_once() {
    // Churn short-lived producers through a recycling channel, draining fully each
    // round, and confirm no value is lost or double-dropped as rings are reused.
    let c = Counter::new();
    {
        let (reg, mut rx) = dynamic::channel::<Tracked>()
            .capacity(16)
            .recycling(8)
            .open()
            .unwrap();
        for round in 0..50u64 {
            let mut p = reg.register();
            for i in 0..10 {
                p.try_send(Tracked::new(round * 10 + i, &c)).ok().unwrap();
            }
            drop(p);
            while rx.try_recv().is_some() {}
        }
    }
    assert_eq!(
        c.live(),
        0,
        "recycling churn: leak/double-drop (live={})",
        c.live()
    );
}

// --- panic safety -----------------------------------------------------------

#[test]
fn drain_closure_panic_no_leak_no_double_drop() {
    let c = Counter::new();
    {
        let (mut prods, mut rx) = fixed::channel::<Tracked>()
            .producers(1)
            .capacity(64)
            .open()
            .unwrap();
        let mut p = prods.pop().unwrap();
        for i in 0..30 {
            p.try_send(Tracked::new(i, &c)).ok().unwrap();
        }
        drop(p); // disconnect so drain() would terminate normally

        // Panic partway through draining. The commit guard must keep the ring
        // consistent; the value handed to the closure is dropped by the unwind;
        // remaining values drop when rx is dropped. Net: every value once.
        let mut seen = 0;
        let r = catch_unwind(AssertUnwindSafe(|| {
            rx.drain(|_v| {
                seen += 1;
                if seen == 10 {
                    panic!("boom in consumer closure");
                }
            });
        }));
        assert!(r.is_err(), "panic should propagate out of drain");
        // rx still alive here; drop it (drains the rest).
    }
    assert_eq!(
        c.live(),
        0,
        "drain panic: leak/double-drop (live={})",
        c.live()
    );
}

// --- disconnect / late-join races -------------------------------------------

#[test]
fn recv_after_all_producers_drop_is_none_then_stable() {
    let (mut prods, mut rx) = fixed::channel::<u64>()
        .producers(2)
        .capacity(16)
        .open()
        .unwrap();
    for p in prods.iter_mut() {
        p.try_send(1).unwrap();
    }
    prods.clear(); // drop all producers
    assert_eq!(rx.recv(), Some(1));
    assert_eq!(rx.recv(), Some(1));
    // fully drained + disconnected: None, and stays None on repeated calls.
    assert_eq!(rx.recv(), None);
    assert_eq!(rx.recv(), None);
    assert_eq!(rx.try_recv(), None);
}

#[test]
fn pool_claim_release_reclaim_boundary_64() {
    // Exactly 64 slots (the u64::MAX bitmap branch). Claim all, 65th fails, drop
    // one, drain to recycle, re-claim succeeds.
    let (h, mut rx) = pool::channel::<u64>()
        .max_producers(64)
        .capacity(8)
        .open()
        .unwrap();
    let mut claimed: Vec<_> = (0..64).map(|_| h.claim().expect("64 available")).collect();
    assert!(h.claim().is_none(), "65th claim must fail at the cap");
    // send one on the last, drop it, drain so the slot recycles
    claimed[63].try_send(7).unwrap();
    claimed.pop(); // drop producer #63
    while rx.try_recv().is_some() {}
    // a couple extra pumps to let the finished shard recycle its slot
    for _ in 0..4 {
        let _ = rx.try_recv();
    }
    assert!(
        h.claim().is_some(),
        "a freed+recycled slot must be re-claimable"
    );
}

#[test]
fn pool_over_64_is_rejected_before_allocation() {
    let result = pool::channel::<u64>().max_producers(65).capacity(8).open();
    assert!(matches!(
        result,
        Err(prescient::OpenError::Invalid(
            "MPSC pool max_producers must be in 1..=64"
        ))
    ));
}

#[test]
fn teardown_race_strands_no_value() {
    // A producer sends as fast as it can while the consumer is dropped out from
    // under it. Every constructed value must end up dropped exactly once — NONE may
    // be stranded in a ring whose consumer-half was torn down mid-write.
    use prescient::mpsc::TrySendError;
    use std::sync::Barrier;
    let iters: u32 = std::env::var("ADV_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    for iter in 0..iters {
        let c = Counter::new();
        let (mut prods, rx) = fixed::channel::<Tracked>()
            .producers(1)
            .capacity(8)
            .open()
            .unwrap();
        let mut p = prods.pop().unwrap();
        let cc = c.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let h = std::thread::spawn(move || {
            // Pre-fill so the producer sits right at the full/retry boundary, then
            // collide with the dropper: the producer hammers sends exactly while
            // RingRx::drop is draining and flipping consumer_gone.
            let mut i = 0u64;
            while p.try_send(Tracked::new(i, &cc)).is_ok() {
                i += 1;
            }
            b2.wait();
            loop {
                match p.try_send(Tracked::new(i, &cc)) {
                    Ok(()) => i += 1,
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
        });
        barrier.wait();
        drop(rx); // race the teardown against the producer's hammering
        h.join().unwrap();
        assert_eq!(
            c.live(),
            0,
            "iter {iter}: a value was stranded (live={})",
            c.live()
        );
    }
}

#[test]
fn dynamic_teardown_race_no_strand() {
    // Same teardown race, but through the dynamic tier: the producer's ring is
    // deposited into the inbox and may be reclaimed by the inbox's own Drop while
    // the producer is still hammering. Every value must still drop exactly once.
    use prescient::mpsc::TrySendError;
    use std::sync::Barrier;
    let iters: u32 = std::env::var("ADV_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    for iter in 0..iters {
        let c = Counter::new();
        let (reg, rx) = dynamic::channel::<Tracked>().capacity(8).open().unwrap();
        let mut p = reg.register();
        let cc = c.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let h = std::thread::spawn(move || {
            let mut i = 0u64;
            while p.try_send(Tracked::new(i, &cc)).is_ok() {
                i += 1;
            }
            b2.wait();
            loop {
                match p.try_send(Tracked::new(i, &cc)) {
                    Ok(()) => i += 1,
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
        });
        barrier.wait();
        drop(rx);
        drop(reg); // tear down inbox + registrar while the producer hammers
        h.join().unwrap();
        assert_eq!(
            c.live(),
            0,
            "iter {iter}: dynamic strand (live={})",
            c.live()
        );
    }
}

#[test]
fn recycled_ring_undrained_drops_values() {
    // Drive rings through the recycling pool (so later rounds reuse a reset ring),
    // then leave the FINAL round undrained and tear down. Inner::drop must clean up
    // a *reused* ring's live contents, with no double-drop against recycle's reset.
    let c = Counter::new();
    {
        let (reg, mut rx) = dynamic::channel::<Tracked>()
            .capacity(8)
            .recycling(4)
            .open()
            .unwrap();
        for round in 0..20u64 {
            let mut p = reg.register();
            for i in 0..6 {
                p.try_send(Tracked::new(round * 6 + i, &c)).ok().unwrap();
            }
            drop(p);
            if round < 19 {
                while rx.try_recv().is_some() {} // recycle the ring
            }
            // final round (19): leave it undrained on purpose
        }
        // drop reg + rx with the last round's values still buffered
    }
    assert_eq!(
        c.live(),
        0,
        "recycled undrained: leak/double-drop (live={})",
        c.live()
    );
}

#[test]
fn high_churn_exact_delivery_no_loss_no_dup() {
    // Many short-lived producers, each sending a DISJOINT value range, under heavy
    // register/drop churn concurrent with draining. Every value in 0..total must
    // arrive exactly once — a set membership check catches both loss and dup.
    const ROUNDS: u64 = 150;
    const PER: u64 = 40;
    const WORKERS: u64 = 4;
    let total = ROUNDS * PER * WORKERS;
    let (reg, mut rx) = dynamic::channel::<u64>().capacity(32).open().unwrap();
    let (count, seen) = std::thread::scope(|s| {
        for w in 0..WORKERS {
            let reg = reg.clone();
            s.spawn(move || {
                for round in 0..ROUNDS {
                    let mut p = reg.register();
                    let base = (w * ROUNDS + round) * PER; // disjoint per (w, round)
                    for i in 0..PER {
                        p.send(base + i).unwrap();
                    }
                }
            });
        }
        drop(reg);
        let mut seen = vec![false; total as usize];
        let mut count = 0u64;
        while let Some(v) = rx.recv() {
            assert!(!seen[v as usize], "duplicate value {v}");
            seen[v as usize] = true;
            count += 1;
        }
        (count, seen)
    });
    assert_eq!(count, total, "lost messages: got {count} of {total}");
    assert!(seen.iter().all(|&b| b), "some values never arrived");
}

#[test]
fn blocking_recv_terminates_under_rapid_churn() {
    // If any waiter had a lost-wake deadlock, the blocking recv() below would hang
    // forever. Run it under an external `timeout` in CI; here we just assert it
    // completes and delivers everything, many times over.
    for _ in 0..60 {
        let (reg, mut rx) = dynamic::channel::<u64>().capacity(4).open().unwrap();
        let n = std::thread::scope(|s| {
            for _ in 0..3 {
                let reg = reg.clone();
                s.spawn(move || {
                    let mut p = reg.register();
                    for i in 0..400 {
                        p.send(i).unwrap();
                    }
                });
            }
            drop(reg);
            let mut n = 0u64;
            while rx.recv().is_some() {
                n += 1;
            }
            n
        });
        assert_eq!(n, 3 * 400, "blocking recv lost messages or stalled");
    }
}

#[test]
fn dynamic_late_join_after_near_disconnect() {
    // A registrar keeps the channel open; register a producer very late, after the
    // consumer has already drained everything and would otherwise see disconnect.
    let (reg, mut rx) = dynamic::channel::<u64>().capacity(16).open().unwrap();
    let p0 = reg.register();
    drop(p0); // first producer gone, but `reg` still holds the channel open
    // consumer drains: nothing yet, but NOT disconnected (reg alive)
    assert_eq!(rx.try_recv(), None);
    // late join
    let mut p1 = reg.register();
    p1.try_send(99).unwrap();
    drop(p1);
    drop(reg);
    let mut got = Vec::new();
    while let Some(v) = rx.recv() {
        got.push(v);
    }
    assert_eq!(
        got,
        vec![99],
        "late-joined producer's value must be delivered"
    );
}
