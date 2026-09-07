//! Pub/sub mask preparation and fan-out ownership boundaries.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use prescient::backend::{Backend, Ring, Seg};
use prescient::mpmc::brokered::{self, Targets};

struct Counted {
    id: usize,
    clones: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            id: self.id,
            clones: self.clones.clone(),
            drops: self.drops.clone(),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn mask_boundaries<B: Backend>() {
    for n in [1, 2, 7, 63, 64] {
        let clones = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut producers, consumers, brokers) = brokered::channel::<Counted>()
            .backend::<B>()
            .consumers(3)
            .capacity(8)
            .pubsub(|item, n| match item.id {
                0 => Targets::none(),
                1 => Targets::one(n - 1),
                2 => Targets::of([0, n / 2, n - 1]),
                3 => Targets::all(64), // includes unconfigured indices when n < 64
                4 => Targets::one(63),
                _ => unreachable!(),
            })
            .consumers(n) // open binds the final shape, not the earlier value
            .manual()
            .open()
            .unwrap();
        for id in 0..5 {
            assert!(producers[0].send(Counted {
                id,
                clones: clones.clone(),
                drops: drops.clone()
            }));
        }
        drop(producers);
        for broker in brokers {
            broker.run();
        }
        let mut deliveries = 0;
        for (index, mut consumer) in consumers.into_iter().enumerate() {
            let mut ids = Vec::new();
            while let Some(item) = consumer.recv() {
                ids.push(item.id);
            }
            let mut expected = Vec::new();
            if index == n - 1 {
                expected.push(1);
            }
            if [0, n / 2, n - 1].contains(&index) {
                expected.push(2);
            }
            expected.push(3);
            if index == 63 {
                expected.push(4);
            }
            deliveries += expected.len();
            assert_eq!(ids, expected, "n={n}, index={index}");
        }
        let nonempty_messages = if n == 64 { 4 } else { 3 };
        let expected_clones = deliveries - nonempty_messages;
        assert_eq!(clones.load(Ordering::Relaxed), expected_clones, "n={n}");
        assert_eq!(drops.load(Ordering::Relaxed), 5 + expected_clones, "n={n}");
    }
}

#[test]
fn ring_pubsub_mask_boundaries_and_exact_clones() {
    mask_boundaries::<Ring>();
}

#[test]
fn seg_pubsub_mask_boundaries_and_exact_clones() {
    mask_boundaries::<Seg>();
}

struct PanicClone {
    live: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

impl Clone for PanicClone {
    fn clone(&self) -> Self {
        assert_ne!(
            self.attempts.fetch_add(1, Ordering::Relaxed),
            1,
            "second clone panics"
        );
        self.live.fetch_add(1, Ordering::Relaxed);
        Self {
            live: self.live.clone(),
            attempts: self.attempts.clone(),
        }
    }
}

impl Drop for PanicClone {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

#[test]
fn pubsub_clone_panic_releases_moved_and_queued_values() {
    let live = Arc::new(AtomicUsize::new(2));
    let attempts = Arc::new(AtomicUsize::new(0));
    let (mut producers, mut consumers, mut brokers) = brokered::channel::<PanicClone>()
        .consumers(3)
        .capacity(8)
        .pubsub(|_, n| Targets::all(n))
        .manual()
        .open()
        .unwrap();
    for _ in 0..2 {
        assert!(producers[0].send(PanicClone {
            live: live.clone(),
            attempts: attempts.clone()
        }));
    }
    drop(producers);
    let broker = brokers.pop().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| broker.run())).is_err());
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
    assert_eq!(live.load(Ordering::Relaxed), 1);
    drop(consumers[0].recv().unwrap());
    assert_eq!(live.load(Ordering::Relaxed), 0);
    for consumer in &mut consumers {
        assert!(consumer.recv().is_none());
    }
}
