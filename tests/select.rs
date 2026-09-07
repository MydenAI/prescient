//! select! correctness: values from whichever source is ready, biased order,
//! completion when all sources are exhausted, and heterogeneous mixes (MPSC +
//! MPMC + broadcast in one select).

use prescient::mpmc::{broadcast, brokerless};
use prescient::mpsc::fixed;
use prescient::select;

#[test]
fn drains_both_channels_and_completes() {
    let (mut atx, mut a) = fixed::channel::<u64>()
        .producers(1)
        // The fixture pre-fills 100 values before it starts selecting. Keep the
        // bounded ring large enough that setup itself does not correctly block
        // on backpressure.
        .capacity(128)
        .open()
        .unwrap();
    let (mut btx, mut b) = fixed::channel::<u64>()
        .producers(1)
        .capacity(128)
        .open()
        .unwrap();
    for i in 0..100u64 {
        atx[0].send(i).unwrap();
        btx[0].send(1000 + i).unwrap();
    }
    drop(atx);
    drop(btx);

    let (mut from_a, mut from_b, mut total) = (0u64, 0u64, 0u64);
    loop {
        let done = select! {
            recv(a) -> v => { assert!(v < 1000); from_a += 1; total += 1; false },
            recv(b) -> v => { assert!(v >= 1000); from_b += 1; total += 1; false },
            complete => true,
        };
        if done {
            break;
        }
    }
    assert_eq!((from_a, from_b, total), (100, 100, 200));
}

#[test]
fn biased_order_prefers_first_arm() {
    // Both channels ready: the FIRST arm must win every round (documented bias).
    let (mut atx, mut a) = fixed::channel::<u64>().capacity(16).open().unwrap();
    let (mut btx, mut b) = fixed::channel::<u64>().capacity(16).open().unwrap();
    atx[0].send(1).unwrap();
    btx[0].send(2).unwrap();
    let first = select! {
        recv(a) -> v => v,
        recv(b) -> v => v,
        complete => 0,
    };
    assert_eq!(first, 1, "first arm must be polled first");
}

#[test]
fn shutdown_signal_pattern() {
    // The canonical select use: a work stream + a priority shutdown channel.
    let (mut wtx, mut work) = fixed::channel::<u64>().capacity(1024).open().unwrap();
    let (mut stx, mut shutdown) = fixed::channel::<()>().capacity(1).open().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..50_000u64 {
                wtx[0].send(i).unwrap();
            }
            stx[0].send(()).unwrap(); // then signal shutdown
            // both txs drop here
        });
        let mut seen = 0u64;
        let reason = loop {
            let stop = select! {
                recv(shutdown) -> _sig => Some("signal"),
                recv(work) -> _v => { seen += 1; None },
                complete => Some("disconnect"),
            };
            if let Some(r) = stop {
                break r;
            }
        };
        // Shutdown arm is FIRST (biased priority): it fires as soon as the signal
        // lands, whether or not work items remain queued behind it.
        assert!(reason == "signal" || reason == "disconnect");
        assert!(seen <= 50_000);
    });
}

#[test]
fn heterogeneous_mpsc_mpmc_broadcast_mix() {
    // One select across three DIFFERENT channel types.
    let (mut mtx, mut mpsc_rx) = fixed::channel::<u64>().capacity(64).open().unwrap();
    let (mut prods, mut mpmc_receivers) = brokerless::channel::<u64>().capacity(64).open().unwrap();
    let mut mpmc_rx = mpmc_receivers.pop().unwrap();
    let (mut publisher, mut bcast_rx) = broadcast::channel::<u64>().capacity(64).open().unwrap();

    mtx[0].send(1).unwrap();
    prods[0].send(2);
    publisher.send(3);
    drop(mtx);
    drop(prods);
    drop(publisher);

    let mut got = Vec::new();
    loop {
        let done = select! {
            recv(mpsc_rx) -> v => { got.push(v); false },
            recv(mpmc_rx) -> v => { got.push(v); false },
            recv(bcast_rx) -> v => { got.push(v); false },
            complete => true,
        };
        if done {
            break;
        }
    }
    got.sort();
    assert_eq!(
        got,
        vec![1, 2, 3],
        "one value from each source, then complete"
    );
}

#[test]
fn complete_fires_immediately_on_born_dead_sources() {
    let (atx, mut a) = fixed::channel::<u64>().capacity(4).open().unwrap();
    drop(atx);
    let out: u64 = select! {
        recv(a) -> v => v,
        complete => 99,
    };
    assert_eq!(out, 99);
}

#[test]
fn cross_thread_select_receives_live_traffic() {
    // Values arrive while the selector is mid-backoff: nothing may be lost.
    let (mut atx, mut a) = fixed::channel::<u64>().capacity(256).open().unwrap();
    let (mut prods, mut receivers) = brokerless::channel::<u64>().capacity(256).open().unwrap();
    let mut b = receivers.pop().unwrap();
    let n = 20_000u64;
    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..n {
                atx[0].send(i).unwrap();
                if i % 64 == 0 {
                    std::thread::yield_now(); // force idle gaps -> backoff paths
                }
            }
        });
        s.spawn(move || {
            for i in 0..n {
                prods[0].send(n + i);
            }
        });
        let mut total = 0u64;
        loop {
            let done = select! {
                recv(a) -> _v => { total += 1; false },
                recv(b) -> _v => { total += 1; false },
                complete => true,
            };
            if done {
                break;
            }
        }
        assert_eq!(total, 2 * n, "select lost values across backoff");
    });
}
#[test]
fn pooled_and_dynamic_mpsc_receivers_are_selectable() {
    let (pool, mut pooled) = prescient::mpsc::pool::channel::<u64>()
        .max_producers(1)
        .capacity(8)
        .open()
        .unwrap();
    let mut pooled_tx = pool.claim().unwrap();
    pooled_tx.send(10).unwrap();
    drop(pooled_tx);
    drop(pool);

    let (registrar, mut dynamic) = prescient::mpsc::dynamic::channel::<u64>()
        .capacity(8)
        .open()
        .unwrap();
    let mut dynamic_tx = registrar.register();
    dynamic_tx.send(20).unwrap();
    drop(dynamic_tx);
    drop(registrar);

    let mut values = Vec::new();
    loop {
        let done = select! {
            recv(pooled) -> value => { values.push(value); false },
            recv(dynamic) -> value => { values.push(value); false },
            complete => true,
        };
        if done {
            break;
        }
    }
    values.sort_unstable();
    assert_eq!(values, [10, 20]);
}
