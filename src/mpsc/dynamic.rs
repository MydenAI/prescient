//! Dynamic tier: unbounded producer count. A producer is minted at runtime via
//! the `Registrar`, which creates a fresh ring, keeps the sender, and deposits
//! the consumer half into a parking_lot Mutex inbox. The receiver drains the
//! inbox into its private working set when the generation bumps.
//!
//! By default each `register()` allocates a fresh ring. For join-heavy workloads
//! (short-lived producers created on a hot path), the declaration's `recycling`
//! setting opts into a bounded ring pool: when the consumer prunes a finished ring it parks it
//! instead of freeing it, and a later `register()` resets and reuses it — an
//! allocation-free join in steady state. The default declaration does not pool.
//!
//! No epoch/GC: a dropped producer marks its ring finished; the receiver drains
//! it and either drops the consumer half (freeing the ring) or, when pooling is
//! on, parks it for reuse.
//!
//! The ring, task-wakeup, and intrusive lock-free membership mechanics are
//! Loom-verified. The default parking-lot-backed membership cannot run under Loom
//! and is gated by adversarial churn, sanitizer, and drop-accounting tests instead.
//! recycling remains memory-safe by construction: reset requires exclusive
//! ownership through `Arc::get_mut`, so it cannot race a live producer.

use std::sync::Arc;

use crate::mpsc::chan::{
    DropRings, DynamicState, Inbox, Kernel, LockedInbox, PooledRings, Producer, Receiver, Reclaim,
    Shared, build_shared, make_producer, make_receiver, new_locked_inbox, new_ring_pool,
    spawn_ring,
};
use crate::wait::{Park, Task};

use crate::mpsc::chan::{LockFreeInbox, new_lock_free_inbox};

/// Declare runtime-registration MPSC with the crate defaults.
pub fn channel<T>() -> crate::Channel<T, crate::topology::MpscDynamic> {
    crate::Channel::new().mpsc_dynamic()
}

/// Cloneable handle that registers new producers with a dynamic MPSC receiver.
pub struct Registrar<
    T: Send,
    K: Kernel = Park,
    I: Inbox<T, K> = LockedInbox<T, K>,
    R: Reclaim<T, K> = DropRings,
> {
    inbox: Arc<I>,
    shared: Arc<Shared<K>>,
    reclaim: R,
    capacity: usize,
    _item: core::marker::PhantomData<fn() -> T>,
}

/// Runtime-neutral asynchronous dynamic-MPSC registrar.
pub type AsyncRegistrar<T, I = LockedInbox<T, Task>, R = DropRings> = Registrar<T, Task, I, R>;
/// Registrar and receiver returned by a dynamic-MPSC declaration.
pub type Endpoints<T, K = Park, I = LockedInbox<T, K>, R = DropRings> = (
    Registrar<T, K, I, R>,
    Receiver<T, K, DynamicState<T, K, I, R>>,
);

impl<T: Send, K: Kernel, I: Inbox<T, K>, R: Reclaim<T, K>> Clone for Registrar<T, K, I, R> {
    fn clone(&self) -> Self {
        self.shared.inc(); // another registrar keeps the channel open
        Registrar {
            inbox: self.inbox.clone(),
            shared: self.shared.clone(),
            reclaim: self.reclaim.clone(),
            capacity: self.capacity,
            _item: core::marker::PhantomData,
        }
    }
}

impl<T: Send, K: Kernel, I: Inbox<T, K>, R: Reclaim<T, K>> Drop for Registrar<T, K, I, R> {
    fn drop(&mut self) {
        self.shared.release();
    }
}

pub(crate) fn open<T: Send, K: Kernel>(capacity: usize) -> Endpoints<T, K> {
    open_inner(capacity, new_locked_inbox::<T, K>(), DropRings)
}

/// Open with a bounded ring pool of up to `max_pooled`
/// finished rings. On a churn-heavy hot path this makes `register()` reuse a
/// previously-freed ring instead of allocating one. `max_pooled` bounds the
/// retained memory: reclaimed rings beyond the cap are freed normally. A useful
/// bound covers the steady-state concurrent-producer count.
pub(crate) fn open_recycling<T: Send, K: Kernel>(
    capacity: usize,
    max_pooled: usize,
) -> Endpoints<T, K, LockedInbox<T, K>, PooledRings<T, K>> {
    open_inner(
        capacity,
        new_locked_inbox::<T, K>(),
        new_ring_pool::<T, K>(max_pooled),
    )
}

pub(crate) fn open_lock_free<T: Send, K: crate::mpsc::SyncKernel<Space = ()>>(
    capacity: usize,
) -> Endpoints<T, K, LockFreeInbox<T>, DropRings> {
    open_inner(capacity, new_lock_free_inbox(), DropRings)
}

pub(crate) fn open_lock_free_recycling<T: Send, K: crate::mpsc::SyncKernel<Space = ()>>(
    capacity: usize,
    max_pooled: usize,
) -> Endpoints<T, K, LockFreeInbox<T>, PooledRings<T, K>> {
    open_inner(
        capacity,
        new_lock_free_inbox(),
        new_ring_pool::<T, K>(max_pooled),
    )
}

fn open_inner<T: Send, K: Kernel, I: Inbox<T, K>, R: Reclaim<T, K>>(
    capacity: usize,
    inbox: Arc<I>,
    reclaim: R,
) -> Endpoints<T, K, I, R> {
    let (shared, waiter) = build_shared::<K>();
    shared.inc(); // the initial registrar counts as a live sender
    let receiver = make_receiver::<T, K, _>(
        Vec::new(),
        shared.clone(),
        waiter,
        DynamicState::new(inbox.clone(), reclaim.clone()),
    );
    (
        Registrar {
            inbox,
            shared,
            reclaim,
            capacity,
            _item: core::marker::PhantomData,
        },
        receiver,
    )
}

impl<T: Send, K: Kernel, I: Inbox<T, K>, R: Reclaim<T, K>> Registrar<T, K, I, R> {
    /// Mint a new producer with its own shard, visible to the receiver on its
    /// next membership refresh. Reuses a pooled ring when recycling is enabled
    /// and one is available; otherwise allocates a fresh ring.
    pub fn register(&self) -> Producer<T, K> {
        // Fast path: reuse a pooled ring (no allocation) if this channel pools
        // and one is exclusively available.
        let (tx, shard) = self
            .reclaim
            .take()
            .unwrap_or_else(|| spawn_ring::<T, K>(self.capacity));
        self.shared.inc();
        let space = shard.space.clone();
        crate::mpsc::chan::deposit(self.inbox.as_ref(), shard);
        make_producer::<T, K>(tx, space, self.shared.clone())
    }
}
