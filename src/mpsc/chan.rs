//! Shared handle types and the round-robin gather logic. Every tier produces
//! the same `Producer<T>` / `Receiver<T>` pair; tiers differ only in how the
//! set of shards is created and grown.

use std::cell::UnsafeCell as UnsafeCellStd;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};

use parking_lot::Mutex;

use crate::backend::ring::{self, Receiver as RingRx, Sender as RingTx};
use crate::platform::{AtomicUsize, Ordering};
use crate::task_waker::TaskWaiter;
use crate::wait::{Hybrid, Park, Spin, StdThread, Task};

mod sealed {
    pub trait Sealed {}
}

/// The statically selected MPSC wake and ring behavior.
///
/// This trait is sealed: applications select one of the public [`crate::wait`]
/// marker types on a declaration. Every call below is monomorphized into the
/// selected endpoint; there is no policy object or runtime tag.
#[doc(hidden)]
pub trait Kernel: sealed::Sealed + Send + Sync + 'static {
    type Waiter: Send;
    type Notifier: Send + Sync;
    type Space: Clone + Send + Sync;

    fn eventcount() -> (Self::Waiter, Self::Notifier);
    fn space() -> Self::Space;

    fn drain<T, F: FnMut(T)>(rx: &mut RingRx<T>, space: &Self::Space, max: usize, f: F) -> usize;
    fn notify_producer(space: &Self::Space);
    fn register_producer(space: &Self::Space, waker: &core::task::Waker);
    fn clear_producer(space: &Self::Space);
    fn wake(notifier: &Self::Notifier);
    fn producer_dropped(notifier: &Self::Notifier);
    fn clear_receiver(notifier: &Self::Notifier);
}

#[doc(hidden)]
pub trait BlockingKernel: Kernel {
    fn arm(waiter: &Self::Waiter);
    fn park(waiter: &Self::Waiter);
    fn disarm(waiter: &Self::Waiter);
}

/// Blocking kernels selectable by a synchronous declaration.
#[doc(hidden)]
pub trait SyncKernel: BlockingKernel {}

macro_rules! blocking_kernel {
    ($marker:ty, $waiter:path, $notifier:path, $eventcount:path) => {
        impl sealed::Sealed for $marker {}
        impl Kernel for $marker {
            type Waiter = $waiter;
            type Notifier = $notifier;
            type Space = ();

            #[inline]
            fn eventcount() -> (Self::Waiter, Self::Notifier) {
                $eventcount()
            }
            #[inline]
            fn space() -> Self::Space {}

            #[inline]
            fn drain<T, F: FnMut(T)>(
                rx: &mut RingRx<T>,
                _space: &Self::Space,
                max: usize,
                f: F,
            ) -> usize {
                rx.drain_in_place(max, f)
            }
            #[inline]
            fn notify_producer(_space: &Self::Space) {}
            #[inline]
            fn register_producer(_space: &Self::Space, _waker: &core::task::Waker) {}
            #[inline]
            fn clear_producer(_space: &Self::Space) {}
            #[inline]
            fn wake(notifier: &Self::Notifier) {
                notifier.wake();
            }
            #[inline]
            fn producer_dropped(_notifier: &Self::Notifier) {}
            #[inline]
            fn clear_receiver(_notifier: &Self::Notifier) {}
        }
        impl BlockingKernel for $marker {
            #[inline]
            fn arm(waiter: &Self::Waiter) {
                waiter.arm();
            }
            #[inline]
            fn park(waiter: &Self::Waiter) {
                waiter.park();
            }
            #[inline]
            fn disarm(waiter: &Self::Waiter) {
                waiter.disarm();
            }
        }
        impl SyncKernel for $marker {}
    };
}

blocking_kernel!(
    Park,
    crate::park::condvar::Waiter,
    crate::park::condvar::Notifier,
    crate::park::condvar::eventcount
);
blocking_kernel!(
    StdThread,
    crate::park::std_thread::Waiter,
    crate::park::std_thread::Notifier,
    crate::park::std_thread::eventcount
);
blocking_kernel!(
    Hybrid,
    crate::park::hybrid::Waiter,
    crate::park::hybrid::Notifier,
    crate::park::hybrid::eventcount
);
blocking_kernel!(
    Spin,
    crate::park::spin::Waiter,
    crate::park::spin::Notifier,
    crate::park::spin::eventcount
);

impl sealed::Sealed for Task {}
impl Kernel for Task {
    type Waiter = ();
    type Notifier = TaskWaiter;
    type Space = Arc<TaskWaiter>;

    #[inline]
    fn eventcount() -> (Self::Waiter, Self::Notifier) {
        ((), TaskWaiter::new())
    }
    #[inline]
    fn space() -> Self::Space {
        Arc::new(TaskWaiter::new())
    }

    #[inline]
    fn drain<T, F: FnMut(T)>(rx: &mut RingRx<T>, space: &Self::Space, max: usize, f: F) -> usize {
        rx.drain_in_place_async(max, space, f)
    }
    #[inline]
    fn notify_producer(space: &Self::Space) {
        space.notify();
    }
    #[inline]
    fn register_producer(space: &Self::Space, waker: &core::task::Waker) {
        space.arm(waker);
    }
    #[inline]
    fn clear_producer(space: &Self::Space) {
        space.clear();
    }
    #[inline]
    fn wake(notifier: &Self::Notifier) {
        notifier.notify();
    }
    #[inline]
    fn producer_dropped(notifier: &Self::Notifier) {
        notifier.notify();
    }
    #[inline]
    fn clear_receiver(notifier: &Self::Notifier) {
        notifier.clear();
    }
}

/// Default per-shard drain batch. Full-drain maximizes throughput; set to 1 for
/// tighter cross-producer tail latency (see docs on the throughput/latency
/// fork). Exposed via `Receiver::set_batch`.
pub const DEFAULT_BATCH: usize = 64;

/// Failure from a nonblocking send, returning ownership of the unsent value.
pub enum TrySendError<T> {
    /// The bounded shard has no available slot.
    Full(T),
    /// The receiver has disconnected.
    Disconnected(T),
}

// Debug/Display without requiring `T: Debug`, so callers can `.unwrap()`/`?` a
// `try_send` regardless of payload type. The payload is elided from the message.
impl<T> std::fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Disconnected(_) => f.write_str("Disconnected(..)"),
        }
    }
}
impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("sending on a full channel"),
            TrySendError::Disconnected(_) => f.write_str("sending on a disconnected channel"),
        }
    }
}
impl<T> std::error::Error for TrySendError<T> {}

/// One receive half plus the statically selected producer-capacity notifier.
#[doc(hidden)]
pub struct Shard<T, K: Kernel> {
    pub(crate) rx: RingRx<T>,
    pub(crate) space: K::Space,
}

impl<T, K: Kernel> Shard<T, K> {
    #[inline]
    fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    #[inline]
    fn is_finished(&self) -> bool {
        self.rx.is_finished()
    }

    #[inline]
    fn recycle(&mut self) -> Option<RingTx<T>> {
        let tx = self.rx.recycle()?;
        K::clear_producer(&self.space);
        Some(tx)
    }

    fn into_rx(self) -> RingRx<T>
    where
        K: SyncKernel<Space = ()>,
    {
        // The intrusive inbox takes ownership of the ring receiver. Bypass
        // `Shard::drop` because depositing membership is a move, not teardown.
        let shard = core::mem::ManuallyDrop::new(self);
        // SAFETY: `rx` is read exactly once into the inbox. The only other field
        // is proven to be `()`, so bypassing the outer destructor leaks nothing.
        unsafe { core::ptr::read(&shard.rx) }
    }
}

impl<T, K: Kernel> Drop for Shard<T, K> {
    fn drop(&mut self) {
        // Publish disconnection before waking a producer blocked on this shard.
        self.rx.close();
        K::notify_producer(&self.space);
    }
}

/// Membership inbox used by a dynamic MPSC declaration.
#[doc(hidden)]
pub trait Inbox<T, K: Kernel>: Send + Sync {
    fn deposit(&self, shard: Shard<T, K>);
    fn drain_into(&self, out: &mut Vec<Shard<T, K>>);
}

/// Mutex-backed, unbounded dynamic membership.
#[doc(hidden)]
pub struct LockedInbox<T, K: Kernel> {
    generation: AtomicUsize,
    seen: AtomicUsize, // consumer's last-drained generation (single consumer)
    queue: Mutex<Vec<Shard<T, K>>>,
}
impl<T, K: Kernel> LockedInbox<T, K> {
    pub(crate) fn new() -> Self {
        LockedInbox {
            generation: AtomicUsize::new(0),
            seen: AtomicUsize::new(0),
            queue: Mutex::new(Vec::new()),
        }
    }
}
impl<T: Send, K: Kernel> Inbox<T, K> for LockedInbox<T, K> {
    fn deposit(&self, shard: Shard<T, K>) {
        self.queue.lock().push(shard);
        self.generation.fetch_add(1, Ordering::Release);
    }
    /// Consumer-only: append newly-deposited rings to `out`. Cheap when nothing
    /// changed — one Acquire load, no lock.
    fn drain_into(&self, out: &mut Vec<Shard<T, K>>) {
        let g = self.generation.load(Ordering::Acquire);
        if g != self.seen.load(Ordering::Relaxed) {
            out.append(&mut self.queue.lock());
            self.seen.store(g, Ordering::Relaxed);
        }
    }
}

/// Lock-free Inbox: an intrusive Treiber stack over the rings themselves
/// (`ring::InboxStack`). A producer join pushes with an AcqRel CAS and **no heap
/// allocation** — the node is the ring's own `Inner`; the single consumer detaches
/// the whole chain with one Acquire swap. Removes the `Mutex` (a step toward
/// no-std) while keeping the join path allocation-free like the `Mutex<Vec>` impl.
#[doc(hidden)]
pub struct LockFreeInbox<T> {
    stack: ring::InboxStack<T>,
}
// Send/Sync are inherited from `InboxStack<T>` (both hold iff `T: Send`); no manual
// impl or Drop needed here — the stack owns the reclamation logic.

impl<T> LockFreeInbox<T> {
    pub(crate) fn new() -> Self {
        LockFreeInbox {
            stack: ring::InboxStack::new(),
        }
    }
}

impl<T: Send, K: SyncKernel<Space = ()>> Inbox<T, K> for LockFreeInbox<T> {
    #[inline]
    fn deposit(&self, shard: Shard<T, K>) {
        self.stack.push(shard.into_rx());
    }
    /// Consumer-only: append newly-deposited rings to `out`.
    #[inline]
    fn drain_into(&self, out: &mut Vec<Shard<T, K>>) {
        self.stack
            .drain_with(|rx| out.push(Shard { rx, space: () }));
    }
}

/// Opt-in ring pool for the dynamic tier: finished rings the consumer has pruned
/// are parked here (up to `cap`) instead of being freed, so a later `register()`
/// can reset and reuse one — turning an allocating join into an allocation-free
/// one. Bounded by `cap`; beyond that, reclaimed rings are simply dropped.
pub struct FreeList<T, K: Kernel> {
    rings: Mutex<Vec<Shard<T, K>>>,
    cap: usize,
}
impl<T, K: Kernel> FreeList<T, K> {
    fn new(cap: usize) -> Self {
        FreeList {
            rings: Mutex::new(Vec::with_capacity(cap)),
            cap,
        }
    }
    /// Consumer side: park a finished (drained, producer-gone) ring for reuse.
    /// Dropped if the pool is already at capacity.
    fn reclaim(&self, shard: Shard<T, K>) {
        let mut g = self.rings.lock();
        if g.len() < self.cap {
            g.push(shard);
        }
        // else: `rx` drops here, freeing the ring — keeps the pool bounded.
    }
    /// Registrar side: take a parked ring that is *exclusively owned* (its
    /// producer half is fully released) and reset it, returning both fresh
    /// halves. Skips/frees any ring whose old producer Arc still lingers, and
    /// returns None once the pool is drained.
    fn take_reset(&self) -> Option<(RingTx<T>, Shard<T, K>)> {
        loop {
            let mut shard = self.rings.lock().pop()?;
            match shard.recycle() {
                Some(tx) => return Some((tx, shard)),
                None => continue, // not yet exclusive: drop `shard`, try the next
            }
        }
    }
}

/// Lock-free slot store for the pool tier, shared between the `PoolHandle`
/// (claims) and the `Receiver` (recycles). The `free` bitmap is the sole
/// synchronization authority over `slots`: owning bit `i` grants exclusive access
/// to `slots[i]`. Defined here (not in `pool`) only because the `Receiver` needs
/// to name it; `pool` builds and drives it.
pub(crate) struct PoolSlots<T, K: Kernel> {
    // one bit per slot: 1 = free, 0 = taken. free bit set <=> slot holds a sender.
    free: AtomicU64,
    #[allow(clippy::type_complexity)]
    slots: Box<[UnsafeCellStd<Option<(RingTx<T>, K::Space)>>]>,
}

// SAFETY: access to `slots[i]` is gated entirely by ownership of `free` bit `i`.
// A claimer touches slot `i` only after CAS-taking bit `i` (1->0); the consumer
// touches slot `i` only while bit `i` is taken (0) and that ring's producer has
// been released (proven by `Arc::get_mut` in `recycle`), then publishes with a
// Release `fetch_or` paired with the claimer's Acquire CAS. free/taken are
// mutually exclusive, so two threads never touch one slot at once. The handoff
// ordering is loom-verified in `pool::loom_pool`.
unsafe impl<T: Send, K: Kernel> Sync for PoolSlots<T, K> {}

impl<T, K: Kernel> PoolSlots<T, K> {
    pub(crate) fn new(senders: Vec<(RingTx<T>, K::Space)>) -> Self {
        let n_max = senders.len();
        let slots: Vec<_> = senders
            .into_iter()
            .map(|tx| UnsafeCellStd::new(Some(tx)))
            .collect();
        let free = if n_max >= 64 {
            u64::MAX
        } else {
            (1u64 << n_max) - 1
        };
        PoolSlots {
            free: AtomicU64::new(free),
            slots: slots.into_boxed_slice(),
        }
    }

    /// Claim a free slot's sender, or None if all are taken.
    pub(crate) fn claim(&self) -> Option<(RingTx<T>, K::Space)> {
        loop {
            let cur = self.free.load(Ordering::Acquire);
            if cur == 0 {
                return None;
            }
            let bit = cur.trailing_zeros();
            let mask = 1u64 << bit;
            if self
                .free
                .compare_exchange_weak(cur, cur & !mask, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Exclusive ownership of slot `bit`; the Acquire CAS pairs with
                // the Release in `recycle_slot` (or construction).
                let tx = unsafe { (*self.slots[bit as usize].get()).take() }
                    .expect("a free bit always has a sender in its slot");
                return Some(tx);
            }
        }
    }

    /// Consumer side: return a reset sender to `slot` and free the bit. Called
    /// while bit `slot` is taken (0), so the write is exclusive; the Release
    /// publishes it to the next claimer.
    pub(crate) fn recycle_slot(&self, slot: usize, tx: RingTx<T>, space: K::Space) {
        unsafe { *self.slots[slot].get() = Some((tx, space)) };
        self.free.fetch_or(1u64 << slot, Ordering::Release);
    }
}

/// Static ring-reclamation policy for dynamic MPSC registration.
#[doc(hidden)]
pub trait Reclaim<T, K: Kernel>: Clone {
    fn take(&self) -> Option<(RingTx<T>, Shard<T, K>)>;
    fn put(&self, shard: Shard<T, K>);
}

/// Finished dynamic rings are freed.
#[doc(hidden)]
#[derive(Clone, Copy, Default)]
pub struct DropRings;

impl<T, K: Kernel> Reclaim<T, K> for DropRings {
    #[inline]
    fn take(&self) -> Option<(RingTx<T>, Shard<T, K>)> {
        None
    }

    #[inline]
    fn put(&self, _shard: Shard<T, K>) {}
}

/// Finished dynamic rings are retained in a bounded pool.
#[doc(hidden)]
pub struct PooledRings<T, K: Kernel>(Arc<FreeList<T, K>>);

impl<T, K: Kernel> Clone for PooledRings<T, K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T, K: Kernel> Reclaim<T, K> for PooledRings<T, K> {
    #[inline]
    fn take(&self) -> Option<(RingTx<T>, Shard<T, K>)> {
        self.0.take_reset()
    }

    #[inline]
    fn put(&self, shard: Shard<T, K>) {
        self.0.reclaim(shard);
    }
}

/// Static shard-membership and finished-ring behavior for an MPSC receiver.
#[doc(hidden)]
pub trait ShardState<T, K: Kernel> {
    fn refresh(&self, shards: &mut Vec<Shard<T, K>>);
    fn dispose(&self, shards: &mut Vec<Shard<T, K>>, index: usize) -> bool;
}

/// Fixed membership: no inbox and finished rings are removed.
#[doc(hidden)]
#[derive(Clone, Copy, Default)]
pub struct FixedState;

impl<T, K: Kernel> ShardState<T, K> for FixedState {
    #[inline]
    fn refresh(&self, _shards: &mut Vec<Shard<T, K>>) {}

    #[inline]
    fn dispose(&self, shards: &mut Vec<Shard<T, K>>, index: usize) -> bool {
        drop(shards.swap_remove(index));
        true
    }
}

/// Fixed pool membership: finished rings reset in place and return their sender.
#[doc(hidden)]
pub struct PoolState<T, K: Kernel>(Arc<PoolSlots<T, K>>);

impl<T, K: Kernel> PoolState<T, K> {
    pub(crate) fn new(slots: Arc<PoolSlots<T, K>>) -> Self {
        Self(slots)
    }
}

impl<T, K: Kernel> ShardState<T, K> for PoolState<T, K> {
    #[inline]
    fn refresh(&self, _shards: &mut Vec<Shard<T, K>>) {}

    #[inline]
    fn dispose(&self, shards: &mut Vec<Shard<T, K>>, index: usize) -> bool {
        if let Some(tx) = shards[index].recycle() {
            self.0.recycle_slot(index, tx, shards[index].space.clone());
        }
        false
    }
}

/// Dynamic membership with concrete inbox and reclamation policies.
#[doc(hidden)]
pub struct DynamicState<T, K: Kernel, I, R> {
    inbox: Arc<I>,
    reclaim: R,
    _item: core::marker::PhantomData<fn() -> (T, K)>,
}

impl<T, K: Kernel, I, R> DynamicState<T, K, I, R> {
    pub(crate) fn new(inbox: Arc<I>, reclaim: R) -> Self {
        Self {
            inbox,
            reclaim,
            _item: core::marker::PhantomData,
        }
    }
}

impl<T, K: Kernel, I: Inbox<T, K>, R: Reclaim<T, K>> ShardState<T, K> for DynamicState<T, K, I, R> {
    #[inline]
    fn refresh(&self, shards: &mut Vec<Shard<T, K>>) {
        self.inbox.drain_into(shards);
    }

    #[inline]
    fn dispose(&self, shards: &mut Vec<Shard<T, K>>, index: usize) -> bool {
        self.reclaim.put(shards.swap_remove(index));
        true
    }
}

/// Cross-cutting shared state: live sender count (for disconnect detection) and
/// the consumer eventcount notifier source.
pub struct Shared<K: Kernel> {
    live: AtomicUsize,
    notifier: K::Notifier,
}
impl<K: Kernel> Shared<K> {
    /// Register a live sender (producer, registrar, or pool handle).
    #[inline]
    pub fn inc(&self) {
        self.live.fetch_add(1, Ordering::AcqRel);
    }
    /// Drop a live sender; wake a parked consumer if this was the last one so it
    /// can observe the disconnect.
    #[inline]
    pub fn release(&self) {
        if self.live.fetch_sub(1, Ordering::AcqRel) == 1 {
            K::wake(&self.notifier);
        }
    }
}

/// Single-owner sending endpoint for one MPSC producer shard.
pub struct Producer<T, K: Kernel = Park> {
    // Rust drops fields in declaration order. Release the ring's sender Arc
    // before the lease notifies the receiver that this shard can be recycled.
    tx: RingTx<T>,
    lease: ProducerLease<K>,
}

struct ProducerLease<K: Kernel> {
    space: K::Space,
    shared: Arc<Shared<K>>,
}

/// Runtime-neutral asynchronous MPSC producer.
pub type AsyncProducer<T> = Producer<T, Task>;

impl<T, K: Kernel> Producer<T, K> {
    #[inline]
    fn notify_consumer(&self) {
        K::wake(&self.lease.shared.notifier);
    }

    #[inline]
    fn try_send_quiet(&mut self, v: T) -> Result<(), TrySendError<T>> {
        if self.tx.is_consumer_gone() {
            return Err(TrySendError::Disconnected(v));
        }
        self.tx.try_push(v).map_err(TrySendError::Full)
    }

    /// Attempt one send without waiting. This is available on both synchronous
    /// and asynchronous endpoints.
    #[inline]
    pub fn try_send(&mut self, v: T) -> Result<(), TrySendError<T>> {
        self.try_send_quiet(v)?;
        self.notify_consumer();
        Ok(())
    }
}

impl<T, K: BlockingKernel> Producer<T, K> {
    /// Blocking send: spin-with-backoff until space frees or the consumer drops.
    /// (Backoff escalates to yield; address-based producer parking is the
    /// documented next optimization — this variant is correct but not the
    /// lowest-idle-CPU option.)
    pub fn send(&mut self, mut v: T) -> Result<(), T> {
        let mut spins = 0u32;
        loop {
            match self.try_send(v) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(v)) => return Err(v),
                Err(TrySendError::Full(item)) => {
                    v = item;
                    spins += 1;
                    if spins < 64 {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }

    /// Push many items, notifying the consumer once. Keeps the per-item hot path
    /// free of the eventcount load — this is the throughput path.
    pub fn send_batch<I: IntoIterator<Item = T>>(&mut self, items: I) -> Result<(), T> {
        for item in items {
            let mut value = item;
            let mut spins = 0u32;
            loop {
                if self.tx.is_consumer_gone() {
                    return Err(value);
                }
                match self.tx.try_push(value) {
                    Ok(()) => {
                        break;
                    }
                    Err(item) => {
                        value = item;
                        spins += 1;
                        if spins < 64 {
                            std::hint::spin_loop();
                        } else {
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }
        self.notify_consumer();
        Ok(())
    }
}

// Clear the old task slot while the sender still prevents ring recycling.
// A new producer may arm that same slot as soon as sender ownership is released.
impl<T, K: Kernel> Drop for Producer<T, K> {
    fn drop(&mut self) {
        K::clear_producer(&self.lease.space);
    }
}
impl<K: Kernel> Drop for ProducerLease<K> {
    fn drop(&mut self) {
        // An empty finished shard still matters to async receivers so pool or
        // dynamic recycling can make the capacity claimable again.
        K::producer_dropped(&self.shared.notifier);
        self.shared.release();
    }
}

/// Future returned by [`AsyncProducer::send_async`]. It owns the item while
/// waiting for capacity; canceling the future drops that item.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct SendFuture<'a, T> {
    producer: &'a mut AsyncProducer<T>,
    item: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let item = this
            .item
            .take()
            .expect("send future polled after completion");
        match this.producer.try_send(item) {
            Ok(()) => return Poll::Ready(Ok(())),
            Err(TrySendError::Disconnected(item)) => return Poll::Ready(Err(item)),
            Err(TrySendError::Full(item)) => this.item = Some(item),
        }

        // Register before the final capacity check. If the receiver releases a
        // slot concurrently, AtomicWaker either schedules us or the recheck sees
        // the new head; no timer, spin, or runtime-specific primitive is needed.
        Task::register_producer(&this.producer.lease.space, cx.waker());
        let item = this.item.take().expect("pending send retains its item");
        match this.producer.try_send(item) {
            Ok(()) => {
                Task::clear_producer(&this.producer.lease.space);
                Poll::Ready(Ok(()))
            }
            Err(TrySendError::Disconnected(item)) => {
                Task::clear_producer(&this.producer.lease.space);
                Poll::Ready(Err(item))
            }
            Err(TrySendError::Full(item)) => {
                this.item = Some(item);
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        Task::clear_producer(&self.producer.lease.space);
    }
}

const ASYNC_SEND_BUDGET: usize = 64;

/// Future returned by [`AsyncProducer::send_batch_async`]. It publishes up to
/// 64 items per poll before cooperatively rescheduling itself, and waits on the
/// shard waker only when capacity is actually exhausted.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct SendBatchFuture<'a, T, I: Iterator<Item = T>> {
    producer: &'a mut AsyncProducer<T>,
    items: I,
    pending: Option<T>,
    // A batch publishes ring tails before it coalesces the receiver wake. Keep
    // that state on the future so unwinding from a user iterator's `next()`
    // cannot strand already-visible values behind a sleeping receiver.
    dirty: bool,
}

impl<T, I: Iterator<Item = T>> Unpin for SendBatchFuture<'_, T, I> {}

impl<T, I: Iterator<Item = T>> Future for SendBatchFuture<'_, T, I> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut published = false;
        let mut budget = ASYNC_SEND_BUDGET;

        loop {
            let item = match this.pending.take().or_else(|| this.items.next()) {
                Some(item) => item,
                None => {
                    if published {
                        this.producer.notify_consumer();
                        this.dirty = false;
                    }
                    Task::clear_producer(&this.producer.lease.space);
                    return Poll::Ready(Ok(()));
                }
            };

            let pushed = this.producer.try_send_quiet(item);
            match pushed {
                Ok(()) => {
                    published = true;
                    this.dirty = true;
                    budget -= 1;
                    if budget == 0 {
                        match this.items.next() {
                            None => {
                                this.producer.notify_consumer();
                                this.dirty = false;
                                Task::clear_producer(&this.producer.lease.space);
                                return Poll::Ready(Ok(()));
                            }
                            Some(next) => this.pending = Some(next),
                        }
                        this.producer.notify_consumer();
                        this.dirty = false;
                        Task::clear_producer(&this.producer.lease.space);
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Err(TrySendError::Disconnected(item)) => {
                    if published {
                        this.producer.notify_consumer();
                        this.dirty = false;
                    }
                    Task::clear_producer(&this.producer.lease.space);
                    return Poll::Ready(Err(item));
                }
                Err(TrySendError::Full(item)) => {
                    this.pending = Some(item);
                    if published {
                        this.producer.notify_consumer();
                        this.dirty = false;
                    }
                    Task::register_producer(&this.producer.lease.space, cx.waker());

                    // Register -> recheck closes a capacity release racing the
                    // first Full observation.
                    let item = this.pending.take().expect("full batch retains its item");
                    match this.producer.try_send_quiet(item) {
                        Ok(()) => {
                            Task::clear_producer(&this.producer.lease.space);
                            published = true;
                            this.dirty = true;
                            budget -= 1;
                            if budget == 0 {
                                this.producer.notify_consumer();
                                this.dirty = false;
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                        Err(TrySendError::Disconnected(item)) => {
                            Task::clear_producer(&this.producer.lease.space);
                            return Poll::Ready(Err(item));
                        }
                        Err(TrySendError::Full(item)) => {
                            this.pending = Some(item);
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<T, I: Iterator<Item = T>> Drop for SendBatchFuture<'_, T, I> {
    fn drop(&mut self) {
        if self.dirty {
            self.producer.notify_consumer();
        }
        Task::clear_producer(&self.producer.lease.space);
    }
}

impl<T> Producer<T, Task> {
    /// Wait asynchronously for per-shard capacity, then send `item`.
    ///
    /// This future is executor-neutral, allocation-free, and cancellation-safe
    /// with respect to the channel. Canceling it drops the unsent item. The
    /// single mutable borrow preserves the one-producer-per-ring invariant and
    /// prevents concurrent send futures on one shard.
    #[inline]
    pub fn send_async(&mut self, item: T) -> SendFuture<'_, T> {
        SendFuture {
            producer: self,
            item: Some(item),
        }
    }

    /// Send an iterator asynchronously, notifying once per published run and
    /// cooperatively yielding every 64 ready items. This is the throughput path:
    /// it amortizes future polling and wake bookkeeping without monopolizing a
    /// single-thread executor.
    pub fn send_batch_async<I>(&mut self, items: I) -> SendBatchFuture<'_, T, I::IntoIter>
    where
        I: IntoIterator<Item = T>,
    {
        SendBatchFuture {
            producer: self,
            items: items.into_iter(),
            pending: None,
            dirty: false,
        }
    }
}

/// Receiving endpoint that drains and fairly scans its current producer shards.
pub struct Receiver<T, K: Kernel = Park, S: ShardState<T, K> = FixedState> {
    shards: Vec<Shard<T, K>>,
    shared: Arc<Shared<K>>,
    waiter: K::Waiter,
    state: S,
    cursor: usize,
    batch: usize,
    staging: VecDeque<T>,
}

/// Runtime-neutral asynchronous MPSC receiver.
pub type AsyncReceiver<T, S = FixedState> = Receiver<T, Task, S>;

impl<T, K: Kernel, S: ShardState<T, K>> Receiver<T, K, S> {
    /// Set the per-shard drain batch: how many items are pulled from one shard
    /// before moving on. This is the **throughput vs. round-robin-fairness** knob.
    ///
    /// * large (e.g. 64, the default) — amortizes bookkeeping, maximizes M msg/s
    /// * `1` — strict round-robin: no producer's burst monopolizes the consumer
    ///
    /// This is NOT a tail-latency knob. `1` re-scans and parks more often, so with
    /// a blocking waiter it makes p99 dramatically *worse*, not better (measured).
    /// For low tail latency select `.wait::<Hybrid>()` or `.wait::<Spin>()` on
    /// the declaration and leave the batch large.
    ///
    /// Setting the batch also pre-sizes the internal staging buffer to match, so
    /// choosing a batch is also choosing a drain-buffer size; steady-state
    /// `recv`/`try_recv` then perform no heap allocation.
    pub fn set_batch(&mut self, n: usize) {
        self.batch = n.max(1);
        self.reserve(self.batch);
    }

    /// The current per-shard drain batch (see [`set_batch`](Self::set_batch)).
    #[inline]
    pub fn batch(&self) -> usize {
        self.batch
    }

    /// Pre-size the internal staging buffer to hold at least `n` items, so the
    /// steady-state receive path never allocates. Call once before a hot loop
    /// for a zero-allocation receiver (the send path is already allocation-free).
    /// This is the **allocation-discipline** knob for callers that cannot
    /// tolerate a first-touch allocation inside `recv`.
    pub fn reserve(&mut self, n: usize) {
        if self.staging.capacity() < n {
            self.staging.reserve(n - self.staging.len());
        }
    }

    /// Current capacity of the internal staging buffer. Steady-state `recv`/
    /// `try_recv` stage at most `batch()` items at a time, so once this is
    /// `>= batch()` the receive path cannot reallocate. A hard-real-time caller
    /// can `reserve(batch())` then assert `staging_capacity() >= batch()` before
    /// entering the hot loop to statically rule out allocation. (For a receive
    /// path that never touches the internal buffer at all, use
    /// [`try_recv_many`](Self::try_recv_many) with a pre-sized `Vec`.)
    #[inline]
    pub fn staging_capacity(&self) -> usize {
        self.staging.capacity()
    }

    /// Drain up to `max` ready items into a **caller-owned** buffer, returning
    /// how many were appended. Non-blocking; makes at most one round-robin pass
    /// over the shards. Because the destination is yours, this path performs no
    /// allocation inside the channel — the throughput/allocation tradeoff is the
    /// caller's to make. Ordering is per-shard FIFO, round-robin across shards
    /// (not a global FIFO). Returns 0 when nothing is currently ready.
    pub fn try_recv_many(&mut self, out: &mut Vec<T>, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        let mut pushed = 0;
        // 1. flush anything already staged by a prior batched try_recv
        while pushed < max {
            match self.staging.pop_front() {
                Some(v) => {
                    out.push(v);
                    pushed += 1;
                }
                None => break,
            }
        }
        if pushed == max {
            return pushed;
        }
        self.refresh_membership();
        // 2. one round-robin pass, draining directly into the caller's buffer and
        // pruning finished shards. `steps` bounds us to one visit per shard so a
        // continuously-refilled shard cannot starve the pass or spin forever.
        let mut steps = self.shards.len();
        while steps > 0 && pushed < max && !self.shards.is_empty() {
            let i = self.cursor % self.shards.len();
            let before = pushed;
            let shard = &mut self.shards[i];
            // Publish released capacity and (for Task) notify once per drain.
            // The kernel's commit guard also preserves ownership on unwind.
            pushed += K::drain(&mut shard.rx, &shard.space, max - pushed, |value| {
                out.push(value);
            });
            if pushed == before && self.shards[i].is_finished() {
                // `false` (pool) = reset in place, advance to the next shard;
                // `true` = removed, re-check the moved-in shard at slot i.
                if !self.dispose_finished(i) {
                    self.cursor = self.cursor.wrapping_add(1);
                }
            } else {
                self.cursor = self.cursor.wrapping_add(1);
            }
            steps -= 1;
        }
        pushed
    }

    #[inline]
    fn refresh_membership(&mut self) {
        self.state.refresh(&mut self.shards);
    }

    #[inline]
    /// True once every producer (and, for the dynamic tier, every registrar) has
    /// been dropped. Buffered values may still remain — `try_recv` keeps
    /// returning them; only `is_disconnected() && try_recv().is_none()` means done.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected()
    }

    fn disconnected(&self) -> bool {
        self.shared.live.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn dispose_finished(&mut self, i: usize) -> bool {
        self.state.dispose(&mut self.shards, i)
    }

    /// One non-blocking round-robin step. Drains up to `batch` items from the
    /// first non-empty shard into a local staging buffer (batch=full ==
    /// throughput; batch=1 == fair/low-tail-latency), returning one. Also prunes
    /// finished shards.
    pub fn try_recv(&mut self) -> Option<T> {
        if let Some(v) = self.staging.pop_front() {
            return Some(v);
        }
        self.refresh_membership();
        if self.shards.is_empty() {
            return None;
        }
        let n = self.shards.len();
        for _ in 0..n {
            let i = self.cursor % self.shards.len();
            let shard = &mut self.shards[i];
            let staging = &mut self.staging;
            let got = K::drain(&mut shard.rx, &shard.space, self.batch, |value| {
                staging.push_back(value);
            });
            if got > 0 {
                self.cursor = self.cursor.wrapping_add(1);
                return self.staging.pop_front();
            }
            if self.shards[i].is_finished() {
                if self.dispose_finished(i) {
                    // removed: ring freed or parked for dynamic reuse
                    if self.shards.is_empty() {
                        break;
                    }
                    // swap_remove moved a fresh element into slot i; re-check it
                } else {
                    // pool: reset in place, advance to the next shard
                    self.cursor = self.cursor.wrapping_add(1);
                }
            } else {
                self.cursor = self.cursor.wrapping_add(1);
            }
        }
        self.staging.pop_front()
    }

    /// Blocking receive. Returns None only when all producers have dropped and
    /// every shard is drained.
    pub fn recv(&mut self) -> Option<T>
    where
        K: BlockingKernel,
    {
        loop {
            if let Some(v) = self.try_recv() {
                return Some(v);
            }
            if self.disconnected() {
                // final sweep in case items landed just before the last drop
                self.refresh_membership();
                if let Some(v) = self.try_recv() {
                    return Some(v);
                }
                return None;
            }
            K::arm(&self.waiter);
            let any = self.shards.iter().any(|s| !s.is_empty());
            if !any && !self.disconnected() {
                K::park(&self.waiter);
            }
            K::disarm(&self.waiter);
        }
    }

    /// One non-blocking round-robin pass, draining every shard *in place* (no
    /// staging buffer; one `head` Release per shard-batch) and invoking `f` on
    /// each item. Prunes/recycles finished shards. Returns items drained.
    fn drain_once<F: FnMut(T)>(&mut self, f: &mut F) -> usize {
        self.refresh_membership();
        if self.shards.is_empty() {
            return 0;
        }
        let mut total = 0;
        let mut steps = self.shards.len();
        while steps > 0 && !self.shards.is_empty() {
            let i = self.cursor % self.shards.len();
            let shard = &mut self.shards[i];
            let got = K::drain(&mut shard.rx, &shard.space, self.batch, &mut *f);
            if got > 0 {
                total += got;
                self.cursor = self.cursor.wrapping_add(1);
            } else if self.shards[i].is_finished() {
                if !self.dispose_finished(i) {
                    self.cursor = self.cursor.wrapping_add(1);
                }
            } else {
                self.cursor = self.cursor.wrapping_add(1);
            }
            steps -= 1;
        }
        total
    }
    /// Drain at most one non-blocking sweep in place. This is the common
    /// synchronous/asynchronous primitive; it never waits for another value.
    pub fn try_drain<F: FnMut(T)>(&mut self, mut f: F) -> usize {
        self.drain_once(&mut f)
    }

    /// Blocking bulk consumer: invoke `f` on every item, in place, until all
    /// producers have dropped and every shard is drained. This is the
    /// allocation-free, copy-free receive path (no per-item `Option`, no staging
    /// buffer, batched slot reclaim) — the throughput-oriented alternative to a
    /// `while let Some(v) = recv()` loop.
    pub fn drain<F: FnMut(T)>(&mut self, mut f: F)
    where
        K: BlockingKernel,
    {
        loop {
            if self.drain_once(&mut f) > 0 {
                continue;
            }
            if self.disconnected() {
                self.refresh_membership();
                if self.drain_once(&mut f) > 0 {
                    continue;
                }
                return;
            }
            K::arm(&self.waiter);
            let any = self.shards.iter().any(|s| !s.is_empty());
            if !any && !self.disconnected() {
                K::park(&self.waiter);
            }
            K::disarm(&self.waiter);
        }
    }
}

impl<T, S: ShardState<T, Task>> Receiver<T, Task, S> {
    /// Poll for the next value without coupling the channel to an executor.
    /// Returns `Ready(None)` only after all producers/registrars are gone and
    /// every buffered value has been delivered.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some(value) = self.try_recv() {
            return Poll::Ready(Some(value));
        }
        if self.disconnected() {
            self.refresh_membership();
            return Poll::Ready(self.try_recv());
        }

        // Register -> recheck is mandatory. A producer may publish between the
        // first empty observation and registration; the second pass closes that
        // lost-wake window and also performs the final disconnect sweep.
        self.shared.notifier.arm(cx.waker());
        if let Some(value) = self.try_recv() {
            self.shared.notifier.clear();
            return Poll::Ready(Some(value));
        }
        if self.disconnected() {
            self.refresh_membership();
            let value = self.try_recv();
            self.shared.notifier.clear();
            return Poll::Ready(value);
        }
        Poll::Pending
    }

    /// Wait asynchronously for one value. The concrete future allocates
    /// nothing and works with Tokio, async-std, smol, or a custom executor.
    #[inline]
    pub fn recv_async(&mut self) -> RecvFuture<'_, T, S> {
        RecvFuture { receiver: self }
    }

    /// Poll until at least one value can be appended to `out`, then drain up to
    /// `max` in one pass. `Ready(0)` means either `max == 0` or the channel is
    /// fully disconnected and drained.
    pub fn poll_recv_many(
        &mut self,
        out: &mut Vec<T>,
        max: usize,
        cx: &mut Context<'_>,
    ) -> Poll<usize> {
        if max == 0 {
            return Poll::Ready(0);
        }
        let received = self.try_recv_many(out, max);
        if received > 0 {
            return Poll::Ready(received);
        }
        if self.disconnected() {
            self.refresh_membership();
            return Poll::Ready(self.try_recv_many(out, max));
        }

        self.shared.notifier.arm(cx.waker());
        let received = self.try_recv_many(out, max);
        if received > 0 {
            self.shared.notifier.clear();
            return Poll::Ready(received);
        }
        if self.disconnected() {
            self.refresh_membership();
            let received = self.try_recv_many(out, max);
            self.shared.notifier.clear();
            return Poll::Ready(received);
        }
        Poll::Pending
    }

    /// Wait for available data, then append up to `max` values to a caller-owned
    /// buffer. Pair with [`AsyncProducer::send_batch_async`] to amortize executor
    /// and wake overhead on throughput-oriented streams.
    pub fn recv_many_async<'a>(
        &'a mut self,
        out: &'a mut Vec<T>,
        max: usize,
    ) -> RecvManyFuture<'a, T, S> {
        RecvManyFuture {
            receiver: self,
            out,
            max,
        }
    }
}

impl<T, K: Kernel, S: ShardState<T, K>> Drop for Receiver<T, K, S> {
    fn drop(&mut self) {
        K::clear_receiver(&self.shared.notifier);
    }
}

/// Future returned by [`AsyncReceiver::recv_async`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct RecvFuture<'a, T, S: ShardState<T, Task> = FixedState> {
    receiver: &'a mut AsyncReceiver<T, S>,
}

impl<T, S: ShardState<T, Task>> Unpin for RecvFuture<'_, T, S> {}

impl<T, S: ShardState<T, Task>> Future for RecvFuture<'_, T, S> {
    type Output = Option<T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

impl<T, S: ShardState<T, Task>> Drop for RecvFuture<'_, T, S> {
    fn drop(&mut self) {
        self.receiver.shared.notifier.clear();
    }
}

/// Future returned by [`AsyncReceiver::recv_many_async`].
#[must_use = "futures do nothing unless polled or awaited"]
pub struct RecvManyFuture<'a, T, S: ShardState<T, Task> = FixedState> {
    receiver: &'a mut AsyncReceiver<T, S>,
    out: &'a mut Vec<T>,
    max: usize,
}

impl<T, S: ShardState<T, Task>> Unpin for RecvManyFuture<'_, T, S> {}

impl<T, S: ShardState<T, Task>> Future for RecvManyFuture<'_, T, S> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.receiver.poll_recv_many(this.out, this.max, cx)
    }
}

impl<T, S: ShardState<T, Task>> Drop for RecvManyFuture<'_, T, S> {
    fn drop(&mut self) {
        self.receiver.shared.notifier.clear();
    }
}

/// Build the shared halves used by every tier constructor.
pub(crate) fn build_shared<K: Kernel>() -> (Arc<Shared<K>>, K::Waiter) {
    let (waiter, notifier) = K::eventcount();
    (
        Arc::new(Shared {
            live: AtomicUsize::new(0),
            notifier,
        }),
        waiter,
    )
}

pub(crate) fn make_producer<T, K: Kernel>(
    tx: RingTx<T>,
    space: K::Space,
    shared: Arc<Shared<K>>,
) -> Producer<T, K> {
    Producer {
        tx,
        lease: ProducerLease { space, shared },
    }
}

pub(crate) fn make_receiver<T, K: Kernel, S: ShardState<T, K>>(
    shards: Vec<Shard<T, K>>,
    shared: Arc<Shared<K>>,
    waiter: K::Waiter,
    state: S,
) -> Receiver<T, K, S> {
    Receiver {
        shards,
        shared,
        waiter,
        state,
        cursor: 0,
        batch: DEFAULT_BATCH,
        staging: VecDeque::new(),
    }
}

pub(crate) fn new_locked_inbox<T, K: Kernel>() -> Arc<LockedInbox<T, K>> {
    Arc::new(LockedInbox::new())
}

pub(crate) fn new_lock_free_inbox<T>() -> Arc<LockFreeInbox<T>> {
    Arc::new(LockFreeInbox::new())
}

pub(crate) fn new_ring_pool<T, K: Kernel>(cap: usize) -> PooledRings<T, K> {
    PooledRings(Arc::new(FreeList::new(cap.max(1))))
}

pub(crate) fn new_pool_slots<T, K: Kernel>(
    senders: Vec<(RingTx<T>, K::Space)>,
) -> Arc<PoolSlots<T, K>> {
    Arc::new(PoolSlots::new(senders))
}

pub(crate) fn spawn_ring<T, K: Kernel>(cap: usize) -> (RingTx<T>, Shard<T, K>) {
    let (tx, rx) = ring::ring(cap);
    let space = K::space();
    (tx, Shard { rx, space })
}

pub(crate) fn deposit<T, K: Kernel>(inbox: &impl Inbox<T, K>, shard: Shard<T, K>) {
    inbox.deposit(shard);
}

// ---------------------------------------------------------------------------
// loom model for the intrusive lock-free Inbox: two producers deposit a ring
// concurrently with the consumer draining, across every interleaving, with no
// lost ring, double-free, or data race. Because deposit/drain round-trip the
// ring's Arc through `Arc::into_raw`/`from_raw`, this also exercises loom's Arc
// strong-count tracking on the intrusive push/pop. Run with:
//   RUSTFLAGS="--cfg loom" cargo test --release loom_ --lib
// ---------------------------------------------------------------------------
#[cfg(all(loom, test))]
mod loom_inbox {
    use super::{Inbox, LockFreeInbox, Shard, spawn_ring};
    use crate::platform::Arc;
    use crate::wait::Park;

    #[test]
    fn loom_inbox_deposit_drain() {
        loom::model(|| {
            let inbox = Arc::new(LockFreeInbox::<u32>::new());
            let (_t1, rx1) = spawn_ring::<u32, Park>(2);
            let (_t2, rx2) = spawn_ring::<u32, Park>(2);

            let i2 = inbox.clone();
            let h = loom::thread::spawn(move || {
                i2.deposit(rx2);
            });

            inbox.deposit(rx1);
            // consumer drains concurrently with the other producer's deposit
            let mut out: Vec<Shard<u32, Park>> = Vec::new();
            inbox.drain_into(&mut out);
            h.join().unwrap();
            inbox.drain_into(&mut out); // final sweep

            assert_eq!(out.len(), 2, "both rings must be recovered exactly once");
        });
    }
}

// End-to-end async lost-wake models. These use Loom's modeled AtomicWaker via
// `task_waker` and the real ring implementation, including the mandatory
// register -> state recheck in both futures.
#[cfg(all(loom, test))]
mod loom_async {
    use crate::mpsc::fixed;

    #[test]
    fn loom_async_batched_full_to_space() {
        for bulk in [false, true] {
            let mut model = loom::model::Builder::new();
            model.preemption_bound = Some(2);
            model.check(move || {
                let (mut producers, mut receiver) = fixed::channel::<u32>()
                    .capacity(2)
                    .r#async()
                    .open()
                    .unwrap();
                let mut producer = producers.pop().unwrap();
                producer.try_send(0).unwrap();
                producer.try_send(1).unwrap();
                let task = loom::thread::spawn(move || {
                    loom::future::block_on(producer.send_async(2)).unwrap();
                });
                let mut values = Vec::new();
                if bulk {
                    while loom::future::block_on(receiver.recv_many_async(&mut values, 2)) != 0 {}
                } else {
                    while let Some(value) = loom::future::block_on(receiver.recv_async()) {
                        values.push(value);
                    }
                }
                task.join().unwrap();
                assert_eq!(values, vec![0, 1, 2]);
            });
        }
    }
    #[test]
    fn loom_async_empty_to_ready_and_disconnect() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.check(|| {
            let (mut producers, mut receiver) = fixed::channel::<u32>()
                .producers(1)
                .capacity(1)
                .r#async()
                .open()
                .unwrap();
            let mut producer = producers.pop().unwrap();
            let task = loom::thread::spawn(move || {
                loom::future::block_on(producer.send_async(7)).unwrap();
            });

            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(7));
            task.join().unwrap();
            assert_eq!(loom::future::block_on(receiver.recv_async()), None);
        });
    }

    #[test]
    fn loom_async_full_to_space() {
        let mut model = loom::model::Builder::new();
        // Two preemptions cover registration before/after the capacity release
        // without exploding on executor bookkeeping permutations.
        model.preemption_bound = Some(2);
        model.check(|| {
            let (mut producers, mut receiver) = fixed::channel::<u32>()
                .producers(1)
                .capacity(1)
                .r#async()
                .open()
                .unwrap();
            let mut producer = producers.pop().unwrap();
            producer.try_send(1).unwrap();
            let task = loom::thread::spawn(move || {
                loom::future::block_on(producer.send_async(2)).unwrap();
            });

            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(1));
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(2));
            task.join().unwrap();
            assert_eq!(loom::future::block_on(receiver.recv_async()), None);
        });
    }
    #[test]
    fn loom_async_rearm_on_warm_shard() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let (mut producers, mut receiver) = fixed::channel::<u32>()
                .capacity(2)
                .r#async()
                .open()
                .unwrap();
            receiver.set_batch(1);
            let mut producer = producers.pop().unwrap();
            producer.try_send(0).unwrap();
            // Retain the producer in the join result: a shutdown notification
            // must not rescue a lost data notification while the receiver waits.
            let task = loom::thread::spawn(move || {
                producer.try_send(1).unwrap();
                producer
            });
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(0));
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(1));
            drop(task.join().unwrap());
            assert_eq!(loom::future::block_on(receiver.recv_async()), None);
        });
    }
    #[test]
    fn loom_async_rearm_on_warm_capacity() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let (mut producers, mut receiver) = fixed::channel::<u32>()
                .capacity(2)
                .r#async()
                .open()
                .unwrap();
            receiver.set_batch(1);
            let mut producer = producers.pop().unwrap();
            producer.try_send(0).unwrap();
            let task = loom::thread::spawn(move || {
                producer.try_send(1).unwrap();
                loom::future::block_on(producer.send_async(2)).unwrap();
                producer
            });
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(0));
            // One released slot must suffice: do not drain or shut down again
            // until the producer completes, which would rescue a missed wake.
            drop(task.join().unwrap());
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(1));
            assert_eq!(loom::future::block_on(receiver.recv_async()), Some(2));
            assert_eq!(loom::future::block_on(receiver.recv_async()), None);
        });
    }
}
