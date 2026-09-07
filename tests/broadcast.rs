//! Broadcast correctness: every reader sees every post-subscribe value in
//! order; gating means no loss and bounded memory; drop-exactly-once holds with
//! clones counted; a dropped reader releases its gate.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use prescient::mpmc::broadcast;

/// Live-count payload where CLONES also count: total drops must equal originals
/// plus every clone handed to a reader.
#[derive(Debug)]
struct Tracked {
    v: u64,
    live: Arc<AtomicI64>,
}
impl Tracked {
    fn new(v: u64, live: &Arc<AtomicI64>) -> Self {
        live.fetch_add(1, Ordering::Relaxed);
        Tracked {
            v,
            live: Arc::clone(live),
        }
    }
}
impl Clone for Tracked {
    fn clone(&self) -> Self {
        self.live.fetch_add(1, Ordering::Relaxed);
        Tracked {
            v: self.v,
            live: Arc::clone(&self.live),
        }
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct DropBomb {
    bytes: Box<[u8; 32]>,
    panic_on_drop: bool,
}

impl Clone for DropBomb {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            panic_on_drop: false,
        }
    }
}

impl Drop for DropBomb {
    fn drop(&mut self) {
        if self.panic_on_drop {
            self.panic_on_drop = false;
            panic!("intentional payload destructor panic");
        }
    }
}

#[derive(Debug)]
struct CloneBomb {
    value: u64,
    panic_once: Arc<AtomicBool>,
}

impl Clone for CloneBomb {
    fn clone(&self) -> Self {
        assert!(
            !self.panic_once.swap(false, Ordering::AcqRel),
            "intentional clone panic"
        );
        Self {
            value: self.value,
            panic_once: Arc::clone(&self.panic_once),
        }
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

#[test]
fn every_reader_sees_every_value_in_order() {
    let n = per(200_000);
    let (mut publisher, r0) = broadcast::channel::<u64>().capacity(256).open().unwrap();
    let readers: Vec<_> = (0..3)
        .map(|_| publisher.subscribe().expect("reader slot"))
        .chain([r0])
        .collect();
    std::thread::scope(|s| {
        let handles: Vec<_> = readers
            .into_iter()
            .map(|mut r| {
                s.spawn(move || {
                    let mut expect = 0u64;
                    while let Some(v) = r.recv() {
                        assert_eq!(v, expect, "gap or reorder in broadcast stream");
                        expect += 1;
                    }
                    expect
                })
            })
            .collect();
        s.spawn(move || {
            for i in 0..n {
                publisher.send(i);
            }
        });
        for h in handles {
            assert_eq!(h.join().unwrap(), n, "reader missed values");
        }
    });
}

#[test]
fn late_subscriber_sees_only_the_suffix() {
    let (mut publisher, mut early) = broadcast::channel::<u64>().capacity(64).open().unwrap();
    for i in 0..10u64 {
        publisher.send(i);
    }
    let mut late = publisher.subscribe().unwrap();
    for i in 10..20u64 {
        publisher.send(i);
    }
    drop(publisher);
    // Early reader: everything.
    let got_early: Vec<u64> = std::iter::from_fn(|| early.recv()).collect();
    assert_eq!(got_early, (0..20).collect::<Vec<_>>());
    // Late reader: only values published after subscribing.
    let got_late: Vec<u64> = std::iter::from_fn(|| late.recv()).collect();
    assert_eq!(got_late, (10..20).collect::<Vec<_>>());
}

#[test]
fn readers_can_mint_readers() {
    let (mut publisher, r0) = broadcast::channel::<u64>().capacity(16).open().unwrap();
    let r1 = r0.subscribe().expect("reader-minted reader");
    publisher.send(42);
    drop(publisher);
    for mut r in [r0, r1] {
        assert_eq!(r.recv(), Some(42));
        assert_eq!(r.recv(), None);
    }
}

#[test]
fn max_readers_enforced() {
    let (publisher, _r0) = broadcast::channel::<u64>().max_readers(2).open().unwrap();
    let _r1 = publisher.subscribe().expect("second slot");
    assert!(
        publisher.subscribe().is_none(),
        "third reader must be refused"
    );
    drop(_r1);
    assert!(
        publisher.subscribe().is_some(),
        "dropped reader frees its slot"
    );
}

#[test]
fn slow_reader_gates_publisher_bounded_memory() {
    // cap=8: with a stalled reader, try_send must refuse after 8 values
    // (backpressure, not loss/lag).
    let (mut publisher, _slow) = broadcast::channel::<u64>().capacity(8).open().unwrap();
    let mut accepted = 0;
    for i in 0..100u64 {
        if publisher.try_send(i).is_ok() {
            accepted += 1;
        }
    }
    assert_eq!(accepted, 8, "gating must stop the publisher at capacity");
}

#[test]
fn dropped_reader_releases_its_gate() {
    let (mut publisher, slow) = broadcast::channel::<u64>().capacity(4).open().unwrap();
    let mut live = publisher.subscribe().unwrap();
    for i in 0..4u64 {
        publisher.send(i);
    }
    assert!(publisher.try_send(4).is_err(), "both readers behind: gated");
    // The live reader catches up; the slow one still gates.
    for _ in 0..4 {
        live.try_recv().unwrap();
    }
    assert!(publisher.try_send(4).is_err(), "slow reader still gates");
    drop(slow);
    assert!(
        publisher.try_send(4).is_ok(),
        "dropping the slow reader must release the ring"
    );
}

#[test]
fn zero_readers_send_never_blocks() {
    let (mut publisher, r0) = broadcast::channel::<u64>().capacity(4).open().unwrap();
    drop(r0);
    for i in 0..10_000u64 {
        publisher.send(i); // must not block or panic; lapped values dropped
    }
}

#[test]
fn drop_exactly_once_including_clones() {
    let live = Arc::new(AtomicI64::new(0));
    let n = per(20_000);
    {
        let (mut publisher, r0) = broadcast::channel::<Tracked>().capacity(64).open().unwrap();
        let readers: Vec<_> = (0..2)
            .map(|_| publisher.subscribe().unwrap())
            .chain([r0])
            .collect();
        std::thread::scope(|s| {
            for mut r in readers {
                s.spawn(move || {
                    let mut expect = 0u64;
                    while let Some(t) = r.recv() {
                        assert_eq!(t.v, expect);
                        expect += 1;
                        drop(t); // clone dropped here
                    }
                });
            }
            let live = Arc::clone(&live);
            s.spawn(move || {
                for i in 0..n {
                    publisher.send(Tracked::new(i, &live));
                }
            });
        });
    }
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "originals + clones must all drop exactly once"
    );
}

#[test]
fn undrained_teardown_drops_ring_values() {
    let live = Arc::new(AtomicI64::new(0));
    let (mut publisher, r0) = broadcast::channel::<Tracked>().capacity(16).open().unwrap();
    for i in 0..10u64 {
        publisher.send(Tracked::new(i, &live));
    }
    assert_eq!(live.load(Ordering::Relaxed), 10);
    drop(r0);
    drop(publisher);
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "ring values dropped exactly once at teardown"
    );
}

#[test]
fn churny_subscribe_unsubscribe_under_load() {
    // Readers join and leave while the publisher runs; every live reader's view
    // stays gap-free and strictly ordered from its join point.
    let n = per(60_000);
    let (mut publisher, r0) = broadcast::channel::<u64>().capacity(128).open().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    std::thread::scope(|s| {
        // A stable reader keeps the stream honest end-to-end.
        let mut stable = r0;
        s.spawn(move || {
            let mut expect = 0u64;
            while let Some(v) = stable.recv() {
                assert_eq!(v, expect);
                expect += 1;
            }
            assert_eq!(expect, n);
        });
        // Churner: subscribe, read a few (asserting order), drop, repeat. Its
        // persistent mint-handle must KEEP DRAINING — an idle subscribed reader
        // gates the publisher (that is the backpressure contract), so a handle
        // held only for subscribe-rights would deadlock the stream.
        let stop2 = Arc::clone(&stop);
        let mut mint = publisher.subscribe().unwrap();
        s.spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                mint.drain(1024, |_| ()); // stay out of the publisher's way
                if let Some(mut r) = mint.subscribe() {
                    let mut last: Option<u64> = None;
                    for _ in 0..50 {
                        if let Some(v) = r.try_recv() {
                            if let Some(l) = last {
                                assert_eq!(v, l + 1, "gap in churned reader");
                            }
                            last = Some(v);
                        }
                    }
                } // r drops: gate released
            }
            mint.for_each(1024, |_| ()); // drain to the end so the stream finishes
        });
        for i in 0..n {
            publisher.send(i);
        }
        drop(publisher);
        stop.store(true, Ordering::Relaxed);
    });
}

#[test]
fn batched_send_and_drain_deliver_everything_in_order() {
    let n = per(100_000);
    let (mut publisher, r0) = broadcast::channel::<u64>().capacity(1024).open().unwrap();
    let readers: Vec<_> = (0..2)
        .map(|_| publisher.subscribe().unwrap())
        .chain([r0])
        .collect();
    std::thread::scope(|s| {
        let handles: Vec<_> = readers
            .into_iter()
            .map(|mut r| {
                s.spawn(move || {
                    let mut expect = 0u64;
                    r.for_each(64, |v| {
                        assert_eq!(v, expect, "gap/reorder on the batched path");
                        expect += 1;
                    });
                    expect
                })
            })
            .collect();
        s.spawn(move || {
            let mut i = 0u64;
            while i < n {
                let hi = (i + 97).min(n); // odd batch sizes cross ring boundaries
                publisher.send_batch(i..hi);
                i = hi;
            }
        });
        for h in handles {
            assert_eq!(h.join().unwrap(), n, "batched reader missed values");
        }
    });
}

#[test]
fn mixed_batched_writer_unbatched_reader_and_vice_versa() {
    let n = per(50_000);
    let (mut publisher, mut single) = broadcast::channel::<u64>().capacity(512).open().unwrap();
    let mut batched = publisher.subscribe().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || {
            let mut expect = 0u64;
            while let Some(v) = single.recv() {
                assert_eq!(v, expect);
                expect += 1;
            }
            assert_eq!(expect, n);
        });
        s.spawn(move || {
            let mut expect = 0u64;
            batched.for_each(32, |v| {
                assert_eq!(v, expect);
                expect += 1;
            });
            assert_eq!(expect, n);
        });
        publisher.send_batch(0..n);
        // The readers terminate only after observing producer_gone. Because this
        // publisher runs on the scope owner rather than a spawned thread, drop it
        // before the scope waits for its reader threads.
        drop(publisher);
    });
}

#[test]
fn suspend_releases_gate_and_resume_rejoins() {
    let (mut publisher, mut lazy) = broadcast::channel::<u64>().capacity(4).open().unwrap();
    let mut active = publisher.subscribe().unwrap();
    // Suspended reader must not gate the publisher...
    lazy.suspend();
    for i in 0..100u64 {
        publisher.send(i); // would deadlock at 4 if `lazy` still gated
        active.try_recv().unwrap();
    }
    // ...and hears nothing while suspended.
    assert_eq!(lazy.try_recv(), None);
    assert_eq!(lazy.recv(), None);
    // Resume: sees only what is published afterwards.
    lazy.resume();
    publisher.send(1000);
    assert_eq!(lazy.try_recv(), Some(1000));
    assert_eq!(active.recv(), Some(1000));
}

#[test]
fn overwrite_destructor_panic_commits_replacement_before_unwind() {
    let (mut publisher, mut reader) = broadcast::channel::<DropBomb>().capacity(2).open().unwrap();
    publisher.send(DropBomb {
        bytes: Box::new([1; 32]),
        panic_on_drop: true,
    });
    publisher.send(DropBomb {
        bytes: Box::new([3; 32]),
        panic_on_drop: false,
    });
    drop(reader.try_recv().unwrap());
    drop(reader.try_recv().unwrap());

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            publisher.send(DropBomb {
                bytes: Box::new([2; 32]),
                panic_on_drop: false,
            });
        }))
        .is_err()
    );
    assert_eq!(reader.try_recv().unwrap().bytes.as_ref(), &[2; 32]);
}

#[test]
fn batch_overwrite_destructor_panic_commits_replacement_before_unwind() {
    let (mut publisher, mut reader) = broadcast::channel::<DropBomb>().capacity(2).open().unwrap();
    publisher.send(DropBomb {
        bytes: Box::new([1; 32]),
        panic_on_drop: true,
    });
    publisher.send(DropBomb {
        bytes: Box::new([3; 32]),
        panic_on_drop: false,
    });
    drop(reader.try_recv().unwrap());
    drop(reader.try_recv().unwrap());

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            publisher.send_batch([DropBomb {
                bytes: Box::new([2; 32]),
                panic_on_drop: false,
            }]);
        }))
        .is_err()
    );
    assert_eq!(reader.try_recv().unwrap().bytes.as_ref(), &[2; 32]);
}

#[test]
fn drain_callback_unwind_releases_consumed_capacity() {
    let (mut publisher, mut reader) = broadcast::channel::<u64>().capacity(2).open().unwrap();
    publisher.send(7);
    publisher.send(9);
    let mut delivered = 0;
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            reader.drain(2, |_| {
                delivered += 1;
                if delivered == 2 {
                    panic!("intentional callback panic");
                }
            });
        }))
        .is_err()
    );
    assert_eq!(reader.try_recv(), None);
    assert_eq!(publisher.try_send(8), Ok(()));
}

#[test]
fn batch_iterator_unwind_publishes_completed_prefix() {
    let (mut publisher, mut reader) = broadcast::channel::<u64>().capacity(2).open().unwrap();
    let mut call = 0;
    let values = std::iter::from_fn(move || {
        call += 1;
        if call == 1 {
            Some(7)
        } else {
            panic!("intentional iterator panic")
        }
    });
    assert!(catch_unwind(AssertUnwindSafe(|| publisher.send_batch(values))).is_err());
    assert_eq!(reader.try_recv(), Some(7));
}

#[test]
fn clone_unwind_preserves_reader_position() {
    let panic_once = Arc::new(AtomicBool::new(true));
    let (mut publisher, mut reader) = broadcast::channel::<CloneBomb>()
        .capacity(2)
        .open()
        .unwrap();
    publisher.send(CloneBomb {
        value: 7,
        panic_once,
    });
    assert!(catch_unwind(AssertUnwindSafe(|| reader.try_recv())).is_err());
    assert_eq!(reader.try_recv().unwrap().value, 7);
}

#[test]
fn concurrent_subscription_does_not_expose_reused_payloads() {
    let (mut publisher, mut dormant) = broadcast::channel::<Box<[u8; 32]>>()
        .capacity(2)
        .max_readers(4)
        .open()
        .unwrap();
    dormant.suspend();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..80 {
                publisher.send(Box::new([value; 32]));
            }
        });
        let dormant = &dormant;
        for _ in 0..2 {
            scope.spawn(move || {
                for _ in 0..40 {
                    if let Some(mut reader) = dormant.subscribe() {
                        for _ in 0..3 {
                            drop(reader.try_recv());
                        }
                    }
                    std::thread::yield_now();
                }
            });
        }
    });
}
