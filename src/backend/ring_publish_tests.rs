use super::*;

#[test]
fn publish_wrap_overflow_partial_and_moved_prefix() {
    for cap in [1, 2, 4, 64] {
        let (mut tx, mut rx) = ring::<String>(cap);
        let base = usize::MAX - 1;
        tx.inner.head.0.store(base, Ordering::Relaxed);
        tx.inner.tail.0.cursor.store(base, Ordering::Relaxed);
        tx.head_cache = base;
        rx.tail_cache = base;
        for round in 0..8 {
            let mut source = Batch::new();
            source.push_back(String::from("moved prefix"));
            assert_eq!(source.pop_front().unwrap(), "moved prefix");
            for id in 0..cap + 3 {
                source.push_back(format!("{round}:{id}"));
            }
            let tail = tx.inner.tail.0.cursor.load(Ordering::Relaxed);
            assert_eq!(tx.push_from(0, &mut source), 0);
            assert_eq!(tx.inner.tail.0.cursor.load(Ordering::Relaxed), tail);
            let mut got = Vec::new();
            while !source.is_empty() {
                let before = source.len();
                let n = tx.push_from(cap, &mut source);
                assert_eq!(before - source.len(), n);
                assert!(n > 0);
                // No source mutation when full. Stale cached head must refresh.
                if n == cap {
                    assert_eq!(tx.push_from(1, &mut source), 0);
                }
                let first = (round % 3 + 1).min(n);
                for _ in 0..first {
                    got.push(rx.try_pop().unwrap());
                }
                // Mix scalar writes with partially available batch writes.
                let extra = tx.push_from(1, &mut source);
                for _ in 0..n - first + extra {
                    got.push(rx.try_pop().unwrap());
                }
            }
            assert_eq!(
                got,
                (0..cap + 3)
                    .map(|id| format!("{round}:{id}"))
                    .collect::<Vec<_>>()
            );
            assert!(rx.try_pop().is_none());
            assert_eq!(tx.push_from(usize::MAX, &mut source), 0);
            tx.try_push(String::from("scalar")).unwrap();
            assert_eq!(rx.try_pop().unwrap(), "scalar");
        }
    }
}

#[test]
fn publish_noncopy_alignment_and_abandoned_values_drop_once() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[repr(align(256))]
    struct Value {
        id: usize,
        data: Box<usize>,
        drops: Arc<Vec<AtomicUsize>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            assert_eq!(*self.data, self.id);
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new((0..12).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut source = Batch::new();
    for id in 0..12 {
        source.push_back(Value {
            id,
            data: Box::new(id),
            drops: drops.clone(),
        });
    }
    drop(source.pop_front());
    let (mut tx, mut rx) = ring(4);
    assert_eq!(tx.push_from(3, &mut source), 3);
    drop(rx.try_pop());
    assert_eq!(tx.push_from(4, &mut source), 1); // cached free capacity
    drop(rx.try_pop());
    assert_eq!(tx.push_from(4, &mut source), 2); // refreshed head and wrap
    drop(rx);
    // Same primitive acceptance semantics as try_push after receiver teardown.
    let _ = tx.push_from(4, &mut source);
    drop(tx);
    drop(source);
    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
}

#[test]
fn publish_zst_ownership() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut source = Batch::new();
    for _ in 0..11 {
        source.push_back(Zst);
    }
    let (mut tx, mut rx) = ring(4);
    assert_eq!(tx.push_from(3, &mut source), 3);
    drop(rx.try_pop());
    assert_eq!(tx.push_from(4, &mut source), 1);
    drop(rx.try_pop());
    assert_eq!(tx.push_from(4, &mut source), 2);
    drop(source);
    drop(tx);
    drop(rx);
    assert_eq!(DROPS.load(Ordering::Relaxed), 11);
}

#[test]
fn publish_concurrent_bulk_and_scalar_reuse() {
    for cap in [1, 4] {
        let (mut tx, mut rx) = ring::<String>(cap);
        let sender = std::thread::spawn(move || {
            let mut source = Batch::new();
            for id in 0..53 {
                source.push_back(format!("value-{id}"));
            }
            while !source.is_empty() {
                if tx.push_from(7, &mut source) == 0 {
                    std::thread::yield_now();
                }
            }
        });
        let mut got = Batch::new();
        while got.len() < 53 {
            if got.len() % 2 == 0 {
                if let Some(value) = rx.try_pop() {
                    got.push_back(value);
                }
            } else {
                rx.drain_into(3, &mut got);
            }
            std::thread::yield_now();
        }
        sender.join().unwrap();
        assert_eq!(
            got.as_slice(),
            (0..53).map(|id| format!("value-{id}")).collect::<Vec<_>>()
        );
    }
}
