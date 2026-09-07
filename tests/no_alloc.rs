//! Proves the documented allocation contract instead of asserting it: with a
//! counting global allocator, a pre-sized receiver drains messages in steady
//! state with ZERO heap allocations. Isolated in its own test binary so the
//! process-wide allocator counter is not perturbed by other tests.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use prescient::mpsc::fixed;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn counting() -> bool {
    COUNTING.with(Cell::get)
}

fn count_allocations(enabled: bool) {
    COUNTING.with(|counting| counting.set(enabled));
}

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if counting() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if counting() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static GA: Counting = Counting;

#[test]
fn steady_state_receive_is_zero_alloc() {
    let (mut prods, mut rx) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1024)
        .open()
        .unwrap();
    let mut p = prods.pop().unwrap();

    // Caller makes the allocation choice up front: size the staging buffer and
    // the drain target so nothing has to grow in the hot loop.
    rx.set_batch(64); // reserves staging to 64
    rx.reserve(256); // extra staging headroom
    let mut out: Vec<u64> = Vec::with_capacity(512);

    // Warm every path once so any lazy first-touch settles before we measure.
    for i in 0..64u64 {
        assert!(p.try_send(i).is_ok());
    }
    let _ = rx.try_recv();
    out.clear();
    let _ = rx.try_recv_many(&mut out, 256);
    out.clear();

    // Measure a steady-state drain across both receive methods.
    for i in 0..256u64 {
        assert!(p.try_send(i).is_ok());
    }
    count_allocations(true);
    let n = rx.try_recv_many(&mut out, 256);
    let one = rx.try_recv();
    count_allocations(false);

    assert!(n > 0, "expected to drain items");
    let _ = one;
    assert_eq!(
        ALLOCS.load(Ordering::Relaxed),
        0,
        "steady-state receive must not allocate after reserve()/set_batch()"
    );
    // An already-owned batch needs no extra allocation to cross the backend.
    use prescient::backend::{Backend, Batch, Ring, Rx, Tx};
    for cap in [1, 4, 64, 4096] {
        let (mut tx, mut rx) = Ring::channel::<usize>(cap);
        let mut source = Batch::new();
        for id in 0..cap + 3 {
            source.push_back(id);
        }
        count_allocations(true);
        let mut count = 0;
        while !source.is_empty() {
            tx.push_from(usize::MAX, &mut source);
            while let Some(id) = rx.try_pop() {
                assert_eq!(id, count);
                count += 1;
            }
        }
        count_allocations(false);
        assert_eq!(count, cap + 3);
        assert_eq!(
            ALLOCS.load(Ordering::Relaxed),
            0,
            "owned publication must not allocate"
        );
    }
    // Lease payloads are reused through publication, borrowed consumption and
    // return lanes. Count from the first post-open cycle: descriptor handoff
    // needs no staging allocation. No other test shares this allocator window.
    for (capacity, batch) in [(1, 1), (12, 3), (4096, 64)] {
        let (mut tx, mut rx) = prescient::mpmc::brokerless::leased::channel::<u64>()
            .producers(3)
            .consumers(3)
            .capacity(capacity)
            .batch(batch)
            .open()
            .unwrap();
        for round in 0..103 {
            count_allocations(true);
            for producer in &mut tx {
                let mut block = producer.reserve().unwrap();
                for id in 0..batch {
                    block.push(id as u64).unwrap();
                }
                block.commit().unwrap();
            }
            let mut received = 0;
            for index in 0..3 {
                let block = if index % 2 == 0 {
                    rx[round % 3].recv().unwrap()
                } else {
                    rx[round % 3].try_recv().unwrap()
                };
                received += block.len();
            }
            assert!(rx[round % 3].try_recv().is_none());
            assert_eq!(received, 3 * batch);
            for producer in &mut tx {
                assert!(producer.flush());
            }
            count_allocations(false);
        }
        assert_eq!(
            ALLOCS.load(Ordering::Relaxed),
            0,
            "steady-state lease fill/publish/read/recycle must not allocate"
        );
    }
    // Exhaust and return a whole pool before refilling, including large
    // backlogs. Do not flush between rounds.
    for blocks in [3, 16, 17, 33, 64] {
        let (mut tx, mut rx) = prescient::mpmc::brokerless::leased::channel::<u64>()
            .capacity(blocks)
            .batch(1)
            .open()
            .unwrap();
        count_allocations(true);
        for _ in 0..4 {
            for id in 0..blocks {
                let mut write = tx[0].try_reserve().unwrap();
                write.push(id as u64).unwrap();
                write.commit().unwrap();
            }
            assert!(tx[0].try_reserve().is_none());
            for id in 0..blocks {
                assert_eq!(rx[0].recv().unwrap().as_slice(), &[id as u64]);
            }
        }
        assert!(tx[0].flush());
        count_allocations(false);
        assert_eq!(
            ALLOCS.load(Ordering::Relaxed),
            0,
            "return spans must not allocate"
        );
    }
}
