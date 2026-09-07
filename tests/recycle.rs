//! Correctness of the dynamic tier's opt-in ring recycling: heavy register/drop
//! churn against a persistent receiver must still deliver every message exactly
//! once (no loss, no duplication) as rings are pruned, pooled, reset, and reused.

use prescient::mpsc::{dynamic, pool};

/// Pool slot recycle under churn: with only 2 slots but many rounds of
/// claim/blast/drop, the pool must reuse reset rings and deliver every message
/// exactly once (far more total claims than slots).
#[test]
fn pool_recycles_under_churn() {
    const ROUNDS: u64 = 120;
    const M: u64 = 2_000;
    let (h, mut rx) = pool::channel::<u64>()
        .max_producers(2)
        .capacity(256)
        .open()
        .unwrap();
    let total = ROUNDS * M;
    let expected_sum = ROUNDS as u128 * ((M - 1) * M / 2) as u128;
    std::thread::scope(|s| {
        // Persistent consumer; main holds `h` so live >= 1 and recv never ends early.
        let consumer = s.spawn(move || {
            let mut n = 0u64;
            let mut sum = 0u128;
            while n < total {
                let v = rx
                    .recv()
                    .expect("disconnect while the pool handle is alive");
                sum += v as u128;
                n += 1;
            }
            (n, sum)
        });
        for _ in 0..ROUNDS {
            // A slot frees only after the consumer drains the previous producer,
            // so claim may transiently return None — spin until one frees.
            let mut p = loop {
                if let Some(p) = h.claim() {
                    break p;
                }
                std::thread::yield_now();
            };
            for i in 0..M {
                p.send(i).unwrap();
            }
            drop(p);
        }
        let (n, sum) = consumer.join().unwrap();
        assert_eq!(n, total, "pool churn: message count");
        assert_eq!(sum, expected_sum, "pool churn: checksum");
    });
}

#[test]
fn recycling_delivers_all_exactly_once_under_churn() {
    const ROUNDS: u64 = 40;
    const P: u64 = 4;
    const M: u64 = 5_000;

    // Small pool relative to the churn, so rings are reset and reused many times.
    let (reg, mut rx) = dynamic::channel::<u64>()
        .capacity(256)
        .recycling(8)
        .open()
        .unwrap();

    let total = ROUNDS * P * M;
    let per_producer_sum = (M - 1) * M / 2;
    let expected_sum = (ROUNDS * P) as u128 * per_producer_sum as u128;

    std::thread::scope(|s| {
        // Persistent consumer. The main thread holds `reg` for the whole scope,
        // so `live >= 1` throughout and recv() never sees a false disconnect.
        let consumer = s.spawn(move || {
            let mut n = 0u64;
            let mut sum = 0u128;
            while n < total {
                let v = rx.recv().expect("disconnect while a registrar is alive");
                sum += v as u128;
                n += 1;
            }
            (n, sum)
        });

        // Driver: round after round of short-lived producers that register, blast,
        // and drop — forcing prune -> pool -> reset -> reuse mid-flight.
        for _ in 0..ROUNDS {
            std::thread::scope(|s2| {
                for _ in 0..P {
                    let reg = reg.clone();
                    s2.spawn(move || {
                        let mut p = reg.register();
                        for i in 0..M {
                            p.send(i).unwrap();
                        }
                        // producer drops here -> ring becomes finished
                    });
                }
            });
        }

        let (n, sum) = consumer.join().unwrap();
        assert_eq!(n, total, "recycling churn: message count");
        assert_eq!(sum, expected_sum, "recycling churn: checksum");
    });
}

/// Deterministic single-threaded exercise of the drain -> pool -> reset -> reuse
/// path, checking payloads survive intact through a reused ring. Small counts so
/// it is cheap enough to also run under Miri (UB / uninit-read / data-race gate
/// for the recycle reset and MaybeUninit slot reuse).
#[test]
fn recycling_single_thread_values_through_reused_ring() {
    let (reg, mut rx) = dynamic::channel::<u64>()
        .capacity(8)
        .recycling(4)
        .open()
        .unwrap();
    for round in 0..6u64 {
        let mut p = reg.register(); // round 0 allocates; rounds 1.. reuse a reset ring
        let base = round * 100;
        for i in 0..5u64 {
            assert!(p.try_send(base + i).is_ok());
        }
        drop(p);
        let mut got = Vec::new();
        while let Some(v) = rx.try_recv() {
            got.push(v);
        }
        let expect: Vec<u64> = (0..5).map(|i| base + i).collect();
        assert_eq!(got, expect, "round {round}: payloads through a reused ring");
    }
}

/// A pool of capacity 1 (maximal reuse pressure) must still be correct.
#[test]
fn recycling_pool_cap_one_is_correct() {
    const ROUNDS: u64 = 200;
    const M: u64 = 1_000;
    let (reg, mut rx) = dynamic::channel::<u64>()
        .capacity(64)
        .recycling(1)
        .open()
        .unwrap();
    let total = ROUNDS * M;
    std::thread::scope(|s| {
        let consumer = s.spawn(move || {
            let mut n = 0u64;
            while n < total {
                rx.recv().expect("disconnect while registrar alive");
                n += 1;
            }
            n
        });
        for _ in 0..ROUNDS {
            let mut p = reg.register();
            for i in 0..M {
                p.send(i).unwrap();
            }
            drop(p);
        }
        assert_eq!(consumer.join().unwrap(), total);
    });
}
