//! A guided tour of Prescient: `cargo run --example tour`.
//!
//! Every scene is a small, idiomatic use of one corner of the API, printed with a
//! one-line result so you can see the shape *and* the behavior. Counts are tiny on
//! purpose; nothing here is a benchmark (see `examples/mpmc_bench.rs` for those).

use std::collections::BTreeSet;

use prescient::mpmc::{brokered, brokerless};
use prescient::mpsc::{dynamic, fixed, pool};

fn main() {
    println!("╭─────────────────────────────╮");
    println!("│  prescient · a guided tour  │");
    println!("╰─────────────────────────────╯");

    section("MPSC — many producers → one consumer");
    scene("fixed", fixed_aggregator);
    scene("pool", pool_bounded);
    scene("dynamic", dynamic_late_join);
    scene("consuming", consumer_styles);

    section("MPMC — many producers → many competing consumers");
    scene("brokerless", brokerless_queue);
    scene("brokerless·drain", brokerless_zero_copy);
    scene("brokered·rr", brokered_round_robin);
    scene("brokered·routed", brokered_routed);
    scene("brokered·pubsub", brokered_pubsub);

    section("MPMC — dynamic registry (producers join at runtime)");
    scene("dyn·locked", dyn_locked);
    scene("dyn·array", dyn_array);

    section("Broadcast & select");
    scene("broadcast", broadcast_pubsub);
    scene("select!", select_two_sources);
}

/// Broadcast: ONE shared ring, every reader sees every value (not a work queue).
/// Readers subscribe at runtime from either endpoint and gate the publisher, so
/// nothing is ever lost — backpressure instead of lag errors.
fn broadcast_pubsub() -> String {
    use prescient::mpmc::broadcast;
    let (mut publisher, mut r1) = broadcast::channel::<u64>().capacity(64).open().unwrap();
    let mut r2 = publisher.subscribe().unwrap(); // join at runtime
    let sums = std::thread::scope(|s| {
        let h1 = s.spawn(move || {
            let mut sum = 0u64;
            while let Some(v) = r1.recv() {
                sum += v;
            }
            sum
        });
        let h2 = s.spawn(move || {
            let mut sum = 0u64;
            while let Some(v) = r2.recv() {
                sum += v;
            }
            sum
        });
        for i in 1..=100u64 {
            publisher.send(i);
        }
        drop(publisher);
        (h1.join().unwrap(), h2.join().unwrap())
    });
    format!("1..=100 published once → reader sums {sums:?} (both = 5050: everyone sees everything)")
}

/// select!: wait on several receivers at once — even of different channel types.
fn select_two_sources() -> String {
    use prescient::mpmc::brokerless;
    let (mut etx, mut events) = fixed::channel::<u64>().capacity(64).open().unwrap();
    let (mut jobs_tx, mut job_receivers) =
        brokerless::channel::<u64>().capacity(64).open().unwrap();
    let mut jobs = job_receivers.pop().unwrap();
    for i in 0..5u64 {
        etx[0].send(i).unwrap();
        jobs_tx[0].send(100 + i);
    }
    drop(etx);
    drop(jobs_tx);
    let (mut ev, mut jb) = (0, 0);
    loop {
        let done = prescient::select! {
            recv(events) -> _v => { ev += 1; false }, // biased: events drain first
            recv(jobs) -> _v => { jb += 1; false },
            complete => true,
        };
        if done {
            break;
        }
    }
    format!("mixed MPSC + MPMC select → {ev} events + {jb} jobs, then `complete`")
}

fn section(title: &str) {
    println!("\n{title}");
    println!("{}", "─".repeat(title.chars().count()));
}

fn scene(label: &str, body: impl FnOnce() -> String) {
    println!("  {label:<17} {}", body());
}

// ── MPSC ───────────────────────────────────────────────────────────────────

/// A known set of producers, one aggregating consumer draining in place.
fn fixed_aggregator() -> String {
    let (producers, mut rx) = fixed::channel::<u64>()
        .producers(4)
        .capacity(1024)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for (worker, mut tx) in producers.into_iter().enumerate() {
            s.spawn(move || {
                for i in 0..1_000 {
                    tx.send(worker as u64 * 1_000 + i).unwrap();
                }
            }); // tx drops → this shard is finished
        }
        let mut sum = 0u64;
        rx.drain(|v| sum += v); // in place, to completion — no per-item Option
        format!("4 workers × 1_000, summed in place = {sum}")
    })
}

/// A capped worker set: slots are claimed, and freed for re-claim when dropped.
fn pool_bounded() -> String {
    let (pool, mut rx) = pool::channel::<u32>()
        .max_producers(4)
        .capacity(64)
        .open()
        .unwrap();
    let (full, received) = std::thread::scope(|s| {
        let claimed: Vec<_> = (0..4).map(|_| pool.claim().expect("free slot")).collect();
        let full = pool.claim().is_none(); // 5th claim fails: cap is 4
        for (w, mut tx) in claimed.into_iter().enumerate() {
            s.spawn(move || {
                for i in 0..100 {
                    tx.send(w as u32 * 100 + i).unwrap();
                }
            });
        }
        drop(pool);
        let mut n = 0;
        while rx.recv().is_some() {
            n += 1;
        }
        (full, n)
    });
    format!("4/4 slots claimed, 5th refused = {full}; received {received} msgs")
}

/// Producers created on the fly from a cloneable registrar.
fn dynamic_late_join() -> String {
    let (registrar, mut rx) = dynamic::channel::<String>().capacity(64).open().unwrap();
    std::thread::scope(|s| {
        for id in 0..3 {
            let reg = registrar.clone();
            s.spawn(move || {
                let mut tx = reg.register(); // mint a producer at runtime
                tx.send(format!("task {id} checking in")).unwrap();
            });
        }
        drop(registrar);
        let mut lines = Vec::new();
        while let Some(msg) = rx.recv() {
            lines.push(msg);
        }
        lines.sort(); // arrival order is nondeterministic
        format!("3 producers registered at runtime → {lines:?}")
    })
}

/// One channel, three ways to consume — pick per workload, not per channel.
fn consumer_styles() -> String {
    let (producers, mut rx) = fixed::channel::<u64>()
        .producers(2)
        .capacity(1024)
        .open()
        .unwrap();
    for (p, mut tx) in producers.into_iter().enumerate() {
        tx.send_batch((0..5).map(|i| p as u64 * 5 + i)).unwrap(); // one wake for the run
    }
    let first = rx.try_recv(); // one at a time
    let mut buf = Vec::new();
    let bulk = rx.try_recv_many(&mut buf, 4); // bulk into a buffer you own
    let mut rest = 0;
    rx.drain(|_| rest += 1); // in place, to completion
    format!("try_recv={first:?}  try_recv_many={bulk}  drain={rest}")
}

// ── MPMC ───────────────────────────────────────────────────────────────────

/// Competing consumers: many workers share one queue, each job handled once.
fn brokerless_queue() -> String {
    let (producers, consumers) = brokerless::channel::<u64>()
        .producers(3)
        .consumers(4)
        .capacity(256)
        .open()
        .unwrap();
    let split = std::thread::scope(|s| {
        for mut prod in producers {
            s.spawn(move || {
                for i in 0..1_000 {
                    prod.send(i);
                }
            });
        }
        let workers: Vec<_> = consumers
            .into_iter()
            .map(|mut w| {
                s.spawn(move || {
                    let mut n = 0u64;
                    while let Some(_job) = w.recv() {
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    format!(
        "3 × 1_000 jobs, 4 consumers → {} done, split {split:?}",
        split.iter().sum::<u64>()
    )
}

/// The zero-copy consumer path: `for_each` runs in place under the claim.
fn brokerless_zero_copy() -> String {
    let (producers, consumers) = brokerless::channel::<u64>()
        .producers(3)
        .consumers(4)
        .capacity(256)
        .open()
        .unwrap();
    let total = std::thread::scope(|s| {
        for mut prod in producers {
            s.spawn(move || {
                for i in 0..1_000 {
                    prod.send(i);
                }
            });
        }
        let workers: Vec<_> = consumers
            .into_iter()
            .map(|mut w| {
                s.spawn(move || {
                    let mut n = 0u64;
                    w.for_each(64, |_job| n += 1); // no staging copy
                    n
                })
            })
            .collect();
        workers.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
    });
    format!("same, via for_each (no staging copy) → {total} done")
}

/// A router thread with the default round-robin placement: even fan-out.
/// `.open()` starts the broker thread for you; it exits once producers are gone.
fn brokered_round_robin() -> String {
    let (producers, consumers) = brokered::channel::<u64>()
        .producers(2)
        .consumers(3)
        .capacity(256)
        .open()
        .unwrap();
    let split = std::thread::scope(|s| {
        for mut prod in producers {
            s.spawn(move || {
                for i in 0..900 {
                    prod.send(i);
                }
            });
        }
        run_consumers(s, consumers)
    });
    format!("round-robin router → per-consumer counts {split:?}")
}

/// The routing closure: place each value by key. Here `v → consumer v % 3`, so each
/// consumer sees exactly one key-class — placement brokerless cannot express.
fn brokered_routed() -> String {
    const NC: usize = 3;
    let (mut producers, consumers) = brokered::channel::<u64>()
        .producers(2)
        .consumers(NC)
        .capacity(256)
        .route(|v, n| (*v as usize) % n)
        .open()
        .unwrap();
    let classes: Vec<BTreeSet<u64>> = std::thread::scope(|s| {
        for (p, mut prod) in producers.drain(..).enumerate() {
            s.spawn(move || {
                for v in p as u64 * 900..p as u64 * 900 + 900 {
                    prod.send(v);
                }
            });
        }
        consumers
            .into_iter()
            .map(|mut c| {
                s.spawn(move || {
                    // record the DISTINCT classes (v % NC) this consumer received
                    let mut seen = BTreeSet::new();
                    while let Some(v) = c.recv() {
                        seen.insert(v % NC as u64);
                    }
                    seen
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    // Each consumer's class set should be exactly {its index}.
    let pure = classes
        .iter()
        .enumerate()
        .all(|(c, set)| set.iter().eq([c as u64].iter()));
    format!("routed by v%{NC}: consumer classes {classes:?}  (each pure = {pure})")
}

/// Pub-sub: a subscribe closure fans each value to a *set* of consumers by topic.
/// `'A' → c0, 'B' → c1, '*' → both` (broadcast), routed on the message's enum-like tag.
fn brokered_pubsub() -> String {
    use prescient::mpmc::brokered::Targets;
    let (mut producers, consumers) = brokered::channel::<(char, u64)>()
        .consumers(2)
        .capacity(256)
        .pubsub(|&(topic, _), n| match topic {
            'A' => Targets::one(0),
            'B' => Targets::one(1),
            _ => Targets::all(n), // broadcast to every subscriber
        })
        .open()
        .unwrap();
    let counts = std::thread::scope(|s| {
        let mut prod = producers.pop().unwrap();
        s.spawn(move || {
            for i in 0..100 {
                prod.send(('A', i));
                prod.send(('B', i));
                prod.send(('*', i)); // fans out to both consumers
            }
        });
        run_consumers(s, consumers)
    });
    format!("A→c0, B→c1, *→both → per-consumer {counts:?} (100 own + 100 broadcast each)")
}

// ── MPMC: dynamic registry ───────────────────────────────────────────────────

/// Dynamic brokerless, `locked` runtime: producers register at runtime; membership
/// is a snapshot-cached `Mutex<Vec>`.
fn dyn_locked() -> String {
    let (registrar, consumers) = brokerless::dynamic::locked::<u64>()
        .consumers(3)
        .capacity(128)
        .open()
        .unwrap();
    let split = std::thread::scope(|s| {
        for _ in 0..4 {
            let reg = registrar.clone();
            s.spawn(move || {
                let mut prod = reg.register(); // ring appears in the set at runtime
                for i in 0..500 {
                    prod.send(i);
                }
            });
        }
        let workers: Vec<_> = consumers
            .into_iter()
            .map(|mut w| {
                s.spawn(move || {
                    let mut n = 0u64;
                    while w.recv().is_some() {
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        drop(registrar);
        workers
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    format!(
        "4 producers joined at runtime → {} jobs, split {split:?}",
        split.iter().sum::<u64>()
    )
}

/// Dynamic brokerless, `array` runtime: same behavior, membership is a bounded,
/// pre-allocated atomic array — registration and scanning take no lock, no list.
fn dyn_array() -> String {
    let (registrar, consumers) = brokerless::dynamic::array::<u64>(4)
        .consumers(3)
        .capacity(128)
        .open()
        .unwrap();
    let total = std::thread::scope(|s| {
        for _ in 0..4 {
            let reg = registrar.clone();
            s.spawn(move || {
                let mut prod = reg.register();
                for i in 0..500 {
                    prod.send(i);
                }
            });
        }
        let workers: Vec<_> = consumers
            .into_iter()
            .map(|mut w| {
                s.spawn(move || {
                    let mut n = 0u64;
                    w.for_each(64, |_job| n += 1);
                    n
                })
            })
            .collect();
        drop(registrar);
        workers.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
    });
    format!("4 producers joined at runtime → {total} jobs, lock-free array membership")
}

/// Spawn a counting thread per brokered consumer; return their per-consumer counts.
fn run_consumers<'scope, 'env, T: Send + 'static>(
    s: &'scope std::thread::Scope<'scope, 'env>,
    consumers: Vec<brokered::Consumer<T>>,
) -> Vec<u64> {
    consumers
        .into_iter()
        .map(|mut c| {
            s.spawn(move || {
                let mut n = 0u64;
                while c.recv().is_some() {
                    n += 1;
                }
                n
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect()
}
