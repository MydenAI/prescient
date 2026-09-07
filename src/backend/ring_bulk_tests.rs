use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn bulk_wrap_prefix_and_partial_counts() {
    for cap in [1, 2, 4, 64] {
        let (mut tx, mut rx) = ring::<String>(cap);
        let base = usize::MAX - 1;
        tx.inner.head.0.store(base, Ordering::Relaxed);
        tx.inner.tail.0.cursor.store(base, Ordering::Relaxed);
        tx.head_cache = base;
        rx.tail_cache = base;
        let mut next = 0;
        for round in 0..8 {
            for id in next..next + cap {
                tx.try_push(format!("item-{id}")).unwrap();
            }
            assert!(tx.try_push(String::from("full")).is_err());
            let mut out = Batch::new();
            out.push_back(String::from("consumed"));
            assert_eq!(out.pop_front().unwrap(), "consumed");
            out.push_back(String::from("prefix"));
            assert_eq!(rx.drain_into(0, &mut out), 0);
            assert_eq!(out.as_slice(), ["prefix"]);
            let mut got = 0;
            while got < cap {
                let n = rx.drain_into((round % 3) + 1, &mut out);
                assert!(n > 0);
                got += n;
            }
            assert_eq!(out.len(), cap + 1);
            for id in 0..cap {
                assert_eq!(out.as_slice()[id + 1], format!("item-{}", next + id));
            }
            assert_eq!(rx.drain_into(usize::MAX, &mut out), 0);
            next += cap;
        }
        drop(tx);
        assert!(rx.is_finished());
    }
}

#[test]
fn bulk_noncopy_aligned_values_drop_once() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[repr(align(256))]
    #[derive(Debug)]
    struct Value {
        id: usize,
        owned: Box<usize>,
        drops: Arc<Vec<AtomicUsize>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new((0..9).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (mut tx, mut rx) = ring(8);
    for id in 0..8 {
        tx.try_push(Value {
            id,
            owned: Box::new(id),
            drops: drops.clone(),
        })
        .unwrap();
    }
    let mut out = Batch::new();
    out.push_back(Value {
        id: 8,
        owned: Box::new(8),
        drops: drops.clone(),
    });
    assert_eq!(rx.drain_into(3, &mut out), 3);
    assert_eq!(out.as_slice().as_ptr().addr() % 256, 0);
    for value in out.as_slice() {
        assert_eq!(*value.owned, value.id);
    }
    drop(out);
    drop(tx);
    drop(rx);
    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
}

#[test]
#[allow(clippy::uninit_vec)] // Unit is initialized without bytes; deliberate ZST overflow probe.
fn bulk_reservation_panic_does_not_consume() {
    let (mut tx, mut rx) = ring::<()>(1);
    tx.try_push(()).unwrap();
    let mut out = Batch::<()>::new();
    // SAFETY: unit is initialized without bytes; Vec's ZST capacity is MAX.
    unsafe {
        out.values.set_len(usize::MAX);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| rx.drain_into(1, &mut out))).is_err());
    assert_eq!(rx.try_pop(), Some(()));
    assert_eq!(rx.try_pop(), None);
    // Avoid a synthetic usize::MAX length in subsequent test machinery.
    out.clear();
}

#[test]
fn bulk_zst_ownership() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut tx, mut rx) = ring(4);
    for _ in 0..4 {
        tx.try_push(Zst).unwrap();
    }
    let mut out = Batch::new();
    assert_eq!(rx.drain_into(3, &mut out), 3);
    drop(out);
    drop(tx);
    drop(rx);
    assert_eq!(DROPS.load(Ordering::Relaxed), 4);
}

#[test]
fn bulk_concurrent_publication_and_reuse() {
    let (mut tx, mut rx) = ring(4);
    let sender = std::thread::spawn(move || {
        for id in 0..100 {
            let mut value = format!("value-{id}");
            loop {
                match tx.try_push(value) {
                    Ok(()) => break,
                    Err(v) => {
                        value = v;
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let mut out = Batch::new();
    while out.len() < 100 {
        if rx.drain_into(3, &mut out) == 0 {
            std::thread::yield_now();
        }
    }
    sender.join().unwrap();
    assert_eq!(
        out.as_slice(),
        (0..100).map(|id| format!("value-{id}")).collect::<Vec<_>>()
    );
}
