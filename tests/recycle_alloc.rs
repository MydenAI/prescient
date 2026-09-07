//! Proves the value of opt-in recycling with a counting global allocator:
//! steady-state register/drop churn allocates a ring per join WITHOUT recycling,
//! and allocates NOTHING with recycling. Single-threaded so the process-wide
//! allocator counter reflects only the code under test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use prescient::mpsc::{PoolState, Receiver, dynamic, pool};
use prescient::wait::Park;

// The counting allocator is process-global, so the two measuring tests in this
// binary must not run their windows concurrently. Serialize them.
static SERIAL: Mutex<()> = Mutex::new(());

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static GA: Counting = Counting;

/// One register -> send a few -> drop -> drain cycle, repeated `n` times.
macro_rules! run_cycles {
    ($reg:expr, $rx:expr, $n:expr) => {{
        for _ in 0..$n {
            let mut p = $reg.register();
            for i in 0..8u64 {
                let _ = p.try_send(i);
            }
            drop(p);
            while $rx.try_recv().is_some() {}
        }
    }};
}

#[test]
fn recycling_removes_per_join_allocation() {
    let _serial = SERIAL.lock().unwrap();
    const WARM: usize = 3_000;
    const MEASURE: usize = 1_000;

    // --- Baseline: plain channel allocates a fresh ring on every register. ---
    let (reg, mut rx) = dynamic::channel::<u64>().capacity(64).open().unwrap();
    rx.reserve(128);
    run_cycles!(reg, rx, WARM); // stabilize inbox/shards/staging capacities
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    run_cycles!(reg, rx, MEASURE);
    COUNTING.store(false, Ordering::Relaxed);
    let without = ALLOCS.load(Ordering::Relaxed);

    // --- Recycling: register reuses a pooled ring, so joins stop allocating. ---
    let (rreg, mut rrx) = dynamic::channel::<u64>()
        .capacity(64)
        .recycling(32)
        .open()
        .unwrap();
    rrx.reserve(128);
    run_cycles!(rreg, rrx, WARM); // prime the pool + stabilize capacities
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    run_cycles!(rreg, rrx, MEASURE);
    COUNTING.store(false, Ordering::Relaxed);
    let with = ALLOCS.load(Ordering::Relaxed);

    assert!(
        without >= MEASURE,
        "baseline should allocate ~>=1 per join over {MEASURE} joins, got {without}"
    );
    // Recycling register churn is fully allocation-free — and stays that way under
    // BOTH inboxes. The Mutex<Vec> reuses its Vec buffer; the lock-free inbox is an
    // *intrusive* Treiber stack (the node is the ring's own Inner), so pushing a
    // ring allocates nothing either. This pins that property for both.
    assert_eq!(
        with, 0,
        "recycling register churn must be allocation-free in steady state, got {with} \
         (baseline was {without})"
    );
}

/// The pool tier pre-creates all rings, so claim/drop/recycle churn must be
/// allocation-free from the start (nothing is allocated after construction).
#[test]
fn pool_churn_is_allocation_free() {
    let _serial = SERIAL.lock().unwrap();
    const WARM: usize = 1_000;
    const MEASURE: usize = 1_000;

    let (h, mut rx) = pool::channel::<u64>()
        .max_producers(2)
        .capacity(64)
        .open()
        .unwrap();
    rx.reserve(128);

    // One claim -> send a few -> drop -> drain (recycles the slot) cycle.
    let cycle = |h: &pool::PoolHandle<u64>, rx: &mut Receiver<u64, Park, PoolState<u64, Park>>| {
        let mut p = loop {
            if let Some(p) = h.claim() {
                break p;
            }
            while rx.try_recv().is_some() {} // free a slot
        };
        for i in 0..8u64 {
            let _ = p.try_send(i);
        }
        drop(p);
        while rx.try_recv().is_some() {}
        for _ in 0..2 {
            let _ = rx.try_recv(); // ensure the finished shard is recycled
        }
    };

    for _ in 0..WARM {
        cycle(&h, &mut rx);
    }
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    for _ in 0..MEASURE {
        cycle(&h, &mut rx);
    }
    COUNTING.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed);

    assert_eq!(
        allocs, 0,
        "pool claim/recycle churn must not allocate, got {allocs}"
    );
}
