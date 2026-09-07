//! Tests for the caller-choice API surface: bulk drain into a caller-owned
//! buffer, the batch/latency knob, and the honest (claim-once) pool contract.

use prescient::mpsc::{fixed, pool};

/// `try_recv_many` must deliver every message exactly once when interleaved
/// with blocking `recv` for termination.
#[test]
fn try_recv_many_delivers_all_exactly_once() {
    const P: usize = 4;
    const M: u64 = 50_000;
    let expected_sum: u128 = (P as u128) * ((M as u128 - 1) * (M as u128) / 2);

    let (prods, mut rx) = fixed::channel::<u64>()
        .producers(P)
        .capacity(1024)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for mut p in prods {
            s.spawn(move || p.send_batch(0..M).unwrap());
        }
        let mut count = 0u64;
        let mut sum = 0u128;
        let mut buf: Vec<u64> = Vec::with_capacity(256);
        // recv() blocks (handles termination); then bulk-drain the rest.
        while let Some(v) = rx.recv() {
            sum += v as u128;
            count += 1;
            buf.clear();
            let n = rx.try_recv_many(&mut buf, 256);
            for &x in &buf {
                sum += x as u128;
            }
            count += n as u64;
        }
        assert_eq!(count, P as u64 * M, "bulk drain: message count");
        assert_eq!(sum, expected_sum, "bulk drain: checksum");
    });
}

/// `try_recv_many` never appends more than `max`, and is a no-op at `max == 0`.
#[test]
fn try_recv_many_respects_max() {
    let (mut prods, mut rx) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1024)
        .open()
        .unwrap();
    let mut p = prods.pop().unwrap();
    for i in 0..100u64 {
        assert!(p.try_send(i).is_ok());
    }
    let mut buf = Vec::new();
    assert_eq!(rx.try_recv_many(&mut buf, 0), 0);
    assert!(buf.is_empty());

    let n = rx.try_recv_many(&mut buf, 10);
    assert_eq!(n, 10);
    assert_eq!(buf.len(), 10);
    assert_eq!(buf, (0..10).collect::<Vec<_>>());
}

/// Pool slots recycle: after a producer drops and the consumer drains its ring,
/// the slot becomes claimable again — allocation-free join/leave under the cap.
#[test]
fn pool_slots_recycle_after_drain() {
    let (h, mut rx) = pool::channel::<u64>()
        .max_producers(2)
        .capacity(16)
        .open()
        .unwrap();
    let a = h.claim();
    let mut b = h.claim();
    assert!(a.is_some() && b.is_some(), "both slots claimable");
    assert!(
        h.claim().is_none(),
        "pool exhausted at n_max live producers"
    );

    b.as_mut().unwrap().try_send(7).unwrap();
    drop(a);
    drop(b);
    // Consumer drains + prunes: this is what resets the rings and frees the bits.
    let mut seen = Vec::new();
    while let Some(v) = rx.try_recv() {
        seen.push(v);
    }
    for _ in 0..4 {
        let _ = rx.try_recv(); // extra passes so both finished shards get recycled
    }
    assert_eq!(seen, vec![7]);

    // Both slots must now be re-claimable without allocation.
    assert!(h.claim().is_some(), "first slot should recycle after drain");
    assert!(
        h.claim().is_some(),
        "second slot should recycle after drain"
    );
    assert!(h.claim().is_none(), "still capped at 2 live producers");
}

/// The in-place `drain()` consumer must deliver every message exactly once,
/// same as the `recv()` loop.
#[test]
fn drain_delivers_all_exactly_once() {
    const P: usize = 4;
    const M: u64 = 50_000;
    let expected_sum: u128 = (P as u128) * ((M as u128 - 1) * (M as u128) / 2);
    let (prods, mut rx) = fixed::channel::<u64>()
        .producers(P)
        .capacity(1024)
        .open()
        .unwrap();
    let (count, sum) = std::thread::scope(|s| {
        for mut p in prods {
            s.spawn(move || p.send_batch(0..M).unwrap());
        }
        let mut count = 0u64;
        let mut sum = 0u128;
        rx.drain(|v| {
            count += 1;
            sum += v as u128;
        });
        (count, sum)
    });
    assert_eq!(count, P as u64 * M, "drain: message count");
    assert_eq!(sum, expected_sum, "drain: checksum");
}

/// Single-threaded drain: fill within capacity, drop the producer, drain on this
/// thread. Deterministic and Miri-friendly — exercises `drain_in_place`'s unsafe
/// (assume_init_read + the panic-safe commit guard) with no threads.
#[test]
fn drain_single_thread_in_order() {
    let (mut prods, mut rx) = fixed::channel::<u64>()
        .producers(1)
        .capacity(64)
        .open()
        .unwrap();
    let mut p = prods.pop().unwrap();
    for i in 0..40u64 {
        assert!(p.try_send(i).is_ok());
    }
    drop(p); // live -> 0, so drain() sees disconnect and returns after draining
    let mut got = Vec::new();
    rx.drain(|v| got.push(v));
    assert_eq!(got, (0..40).collect::<Vec<_>>());
}

/// `drain()` must also work through the pool tier's recycle path.
#[test]
fn drain_pool_delivers_all() {
    const P: usize = 4;
    const M: u64 = 25_000;
    let (h, mut rx) = pool::channel::<u64>()
        .max_producers(P)
        .capacity(1024)
        .open()
        .unwrap();
    let claimed: Vec<_> = (0..P).map(|_| h.claim().unwrap()).collect();
    drop(h);
    let n = std::thread::scope(|s| {
        for mut p in claimed {
            s.spawn(move || p.send_batch(0..M).unwrap());
        }
        let mut n = 0u64;
        rx.drain(|_| n += 1);
        n
    });
    assert_eq!(n, P as u64 * M);
}

/// `set_batch(1)` must still deliver everything (fairness/low-latency mode).
#[test]
fn batch_one_delivers_all() {
    const P: usize = 3;
    const M: u64 = 20_000;
    let (prods, mut rx) = fixed::channel::<u64>()
        .producers(P)
        .capacity(256)
        .open()
        .unwrap();
    rx.set_batch(1);
    assert_eq!(rx.batch(), 1);
    let n = std::thread::scope(|s| {
        for mut p in prods {
            s.spawn(move || {
                for i in 0..M {
                    p.send(i).unwrap();
                }
            });
        }
        let mut n = 0u64;
        while rx.recv().is_some() {
            n += 1;
        }
        n
    });
    assert_eq!(n, P as u64 * M);
}
