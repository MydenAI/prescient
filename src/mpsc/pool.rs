//! Pool tier: up to `n_max` producers claiming slots from a fixed pool via an
//! atomic bitmap. All shards are pre-created, so neither claiming nor recycling
//! ever allocates. The bitmap is the *sole* synchronization authority over the
//! slot storage (see `PoolSlots`) — owning a bit grants exclusive
//! access to its slot — so there is no lock on any path after construction.
//!
//! Slots recycle: when a producer drops, the consumer drains its ring, resets
//! it, and returns a fresh sender to the slot, freeing the bit for re-claim. This
//! lets a *bounded* set of workers churn (join/leave under the cap) with zero
//! allocation. The `recycle_slot`(Release) / `claim`(Acquire) handoff over the
//! slot cell is loom-verified (`loom_slot_handoff`).

use std::sync::Arc;

use crate::mpsc::chan::{
    Kernel, PoolSlots, PoolState, Producer, Receiver, Shared, build_shared, make_producer,
    make_receiver, new_pool_slots, spawn_ring,
};
use crate::wait::{Park, Task};

/// Declare bounded producer-pool MPSC with the crate defaults.
pub fn channel<T>() -> crate::Channel<T, crate::topology::MpscPool> {
    crate::Channel::new().mpsc_pool()
}

/// Handle for claiming producers from the pool after construction.
pub struct PoolHandle<T: Send, K: Kernel = Park> {
    slots: Arc<PoolSlots<T, K>>,
    shared: Arc<Shared<K>>,
}

/// Runtime-neutral asynchronous producer-pool handle.
pub type AsyncPoolHandle<T> = PoolHandle<T, Task>;

#[allow(clippy::type_complexity)]
pub(crate) fn open<T: Send, K: Kernel>(
    n_max: usize,
    capacity: usize,
) -> (PoolHandle<T, K>, Receiver<T, K, PoolState<T, K>>) {
    assert!((1..=64).contains(&n_max), "pool supports 1..=64 producers");
    let (shared, waiter) = build_shared::<K>();
    let mut txs = Vec::with_capacity(n_max);
    let mut rxs = Vec::with_capacity(n_max);
    for _ in 0..n_max {
        let (tx, shard) = spawn_ring::<T, K>(capacity);
        txs.push((tx, shard.space.clone()));
        rxs.push(shard);
    }
    let slots = new_pool_slots::<T, K>(txs);
    shared.inc(); // the pool handle keeps the channel open while it can claim
    // The receiver drives recycling: on a finished shard it resets the ring and
    // returns the sender to its slot via this shared store.
    let receiver =
        make_receiver::<T, K, _>(rxs, shared.clone(), waiter, PoolState::new(slots.clone()));
    (PoolHandle { slots, shared }, receiver)
}

impl<T: Send, K: Kernel> PoolHandle<T, K> {
    /// Claim a free producer slot, or None if the pool is fully claimed. A slot
    /// freed by a dropped-and-drained producer becomes claimable again — this is
    /// the allocation-free join/leave path.
    pub fn claim(&self) -> Option<Producer<T, K>> {
        let (tx, space) = self.slots.claim()?;
        self.shared.inc();
        Some(make_producer::<T, K>(tx, space, self.shared.clone()))
    }
}

impl<T: Send, K: Kernel> Clone for PoolHandle<T, K> {
    fn clone(&self) -> Self {
        self.shared.inc();
        PoolHandle {
            slots: self.slots.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<T: Send, K: Kernel> Drop for PoolHandle<T, K> {
    fn drop(&mut self) {
        self.shared.release();
    }
}

// ---------------------------------------------------------------------------
// loom model: the recycle(Release) -> claim(Acquire) slot handoff. Verifies that
// a claimer taking a freshly-freed bit observes the sender the consumer wrote
// into that slot (no stale/torn read), across every interleaving. Run with:
//   RUSTFLAGS="--cfg loom" cargo test --release loom_ --lib
// ---------------------------------------------------------------------------
#[cfg(all(loom, test))]
mod loom_pool {
    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};

    struct Slot {
        free: AtomicU64,
        cell: UnsafeCell<u64>, // stands in for `Option<Sender>`; 0 == stale
    }

    // Mirrors `PoolSlots`: start "taken" (bit clear), consumer writes the slot
    // then frees the bit with a Release fetch_or; claimer waits for the bit,
    // CAS-takes it with Acquire, then reads the slot. If the ordering were wrong,
    // loom flags either a data race on the cell or a stale (0) read.
    #[test]
    fn loom_slot_handoff() {
        loom::model(|| {
            let slot = Arc::new(Slot {
                free: AtomicU64::new(0),
                cell: UnsafeCell::new(0),
            });
            let s2 = slot.clone();
            let consumer = loom::thread::spawn(move || {
                s2.cell.with_mut(|p| unsafe { *p = 42 }); // exclusive: bit is taken
                s2.free.fetch_or(1, Ordering::Release); // publish
            });
            loop {
                let cur = slot.free.load(Ordering::Acquire);
                if cur & 1 == 1
                    && slot
                        .free
                        .compare_exchange(cur, cur & !1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    let v = slot.cell.with(|p| unsafe { *p });
                    assert_eq!(v, 42, "claimer must observe the consumer's slot write");
                    break;
                }
                loom::thread::yield_now();
            }
            consumer.join().unwrap();
        });
    }
}
