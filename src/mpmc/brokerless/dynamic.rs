//! Dynamic (runtime-registered) brokerless MPMC.
//!
//! Like [`brokerless`](super), but the producer set grows at runtime:
//! clone a [`Registrar`] and call [`register`](Registrar::register) whenever you
//! need another producer. Competing consumers discover the new rings on their own.
//!
//! The hard part of a *dynamic brokerless* channel is that many consumers share the
//! ring set while producers keep growing it — so membership needs a concurrent,
//! multi-reader, growable structure. That structure is the pluggable "runtime".
//! Both options store rings in a flat array (no linked list); they differ in whether
//! growth is unbounded-with-a-lock or bounded-and-lock-free:
//!
//! * [`locked`] — **unbounded**. Membership is a `Mutex<Vec>`; each
//!   consumer keeps a private snapshot and only re-locks when it grows, so
//!   steady-state scanning is lock-free and only *registration* briefly locks.
//! * [`array()`] — **bounded** (you pass a max producer count). Membership is
//!   a pre-allocated array of atomic slots; registration claims the next index with
//!   a bounded atomic reservation and consumers scan by direct indexing. No lock anywhere, no
//!   pointer chasing — the shortest scan path when the producer bound is known.
//!
//! Both keep the full brokerless consumer surface: `recv`/`try_recv` (staged) and
//! `drain`/`for_each` (zero-copy). Same guarantee: each value to exactly one
//! consumer. Pick the runtime by whether your producer count is bounded — see
//! `examples/mpmc_bench.rs dynamic`.

// The `Rings` backend trait is `pub` only to satisfy the bound on the public
// producer/consumer types; it is `#[doc(hidden)]` and sealed, so its methods
// mentioning the crate-internal `ring::Receiver` are unreachable to callers.
#![allow(private_interfaces)]

use crate::backend::Batch;
use core::marker::PhantomData;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::ReleaseClaim;
use crate::backend::{Backend, Ring, Rx as _, Tx as _};

/// Declare dynamic MPMC with unbounded, locked membership.
pub fn locked<T>() -> crate::Channel<
    T,
    crate::topology::MpmcDynamicLocked,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new().mpmc_dynamic_locked()
}

/// Declare dynamic MPMC with bounded, lock-free array membership.
pub fn array<T>(
    max_producers: usize,
) -> crate::Channel<
    T,
    crate::topology::MpmcDynamicArray,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new().mpmc_dynamic_array(max_producers)
}

/// One producer's ring plus its claim flag. `claimed` false→true (Acquire) grants
/// the winner exclusive access to `rx`; the true→false store (Release) publishes it.
#[doc(hidden)] // implementation detail; public only to satisfy the `Rings` bound
pub struct Slot<T: Send, B: Backend> {
    claimed: AtomicBool,
    rx: UnsafeCell<B::Rx<T>>,
}
// SAFETY: `rx` is touched only by the consumer currently holding `claimed`
// (Acquire on claim / Release on release), so at most one thread aliases a given
// RingRx at a time, and RingRx is Send when T is.
unsafe impl<T: Send, B: Backend> Sync for Slot<T, B> {}

impl<T: Send, B: Backend> Slot<T, B> {
    fn new(rx: B::Rx<T>) -> Self {
        Slot {
            claimed: AtomicBool::new(false),
            rx: UnsafeCell::new(rx),
        }
    }
}

mod sealed {
    pub trait Sealed {}
}

/// The pluggable membership backend — the "runtime". Both implementations
/// ([`LockedRings`], [`ArrayRings`]) store rings in a flat array; the
/// producer/consumer/claim logic is shared.
#[doc(hidden)] // sealed: only the runtimes in this module implement it
pub trait Rings<T: Send, B: Backend>: sealed::Sealed + Send + Sync {
    /// Per-consumer bounded-preference scan state.
    type Cursor: Default + Send;
    /// Make an independent, dephased consumer cursor (cold endpoint creation).
    fn cursor(seed: usize) -> Self::Cursor;
    /// Prefer the last productive selection for a bounded number of drains.
    fn prefer(cur: &mut Self::Cursor);
    /// Register a producer's receiving end (called from `Registrar::register`).
    fn push(&self, rx: B::Rx<T>);
    /// Rings registered so far — an upper bound for one scan pass.
    fn count(&self) -> usize;
    /// Pointer to the selected ring's slot, advancing `cur` before any claim. The
    /// slot stays valid for the channel's lifetime. May be **null** for a reserved-
    /// but-not-yet-published cell (the caller skips it). `None` only if no rings
    /// exist yet.
    fn next(&self, cur: &mut Self::Cursor) -> Option<*const Slot<T, B>>;
}

struct Shared<T: Send, B: Backend, R: Rings<T, B>> {
    rings: R,
    /// Registrars + producers still alive. A live registrar keeps this > 0, so a
    /// consumer never quits while more producers could still be registered.
    live: AtomicUsize,
    /// Live consumers, so a blocking `send` gives up instead of spinning forever
    /// once every consumer is gone.
    live_consumers: AtomicUsize,
    _pd: PhantomData<fn() -> (T, B)>,
}

// ---- producer -------------------------------------------------------------

/// A producer half. One dedicated SPSC ring; never contends with other producers.
pub struct Producer<T: Send, B: Backend, R: Rings<T, B>> {
    tx: B::Tx<T>,
    shared: Arc<Shared<T, B, R>>,
}

impl<T: Send, B: Backend, R: Rings<T, B>> Producer<T, B, R> {
    /// Non-blocking send. Returns the value back if this producer's ring is full.
    #[inline]
    pub fn try_send(&mut self, v: T) -> Result<(), T> {
        self.tx.try_push(v)
    }

    /// Send, spinning while this producer's ring is full. Returns `false` (dropping
    /// the value) if every consumer is gone, rather than spinning forever.
    pub fn send(&mut self, mut v: T) -> bool {
        let mut s = 0u32;
        loop {
            match self.tx.try_push(v) {
                Ok(()) => return true,
                Err(back) => {
                    if self.shared.live_consumers.load(Ordering::Acquire) == 0 {
                        return false;
                    }
                    v = back;
                    s = s.wrapping_add(1);
                    if s < 64 {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }
    /// Publish an already-owned batch, spinning/yielding while the ring is full.
    ///
    /// Returns true when the source is empty. If no consumer remains and no
    /// progress is possible, returns false with the unsent suffix still owned
    /// by `source`. Accepted values are not delivery acknowledgements.
    /// Empty batches succeed; call `Batch::clear` before reusing consumed storage.
    pub fn send_batch(&mut self, source: &mut Batch<T>) -> bool {
        let mut spins = 0u32;
        while !source.is_empty() {
            let before = source.len();
            self.tx.push_from(usize::MAX, source);
            // Use actual ownership, not a custom backend's reported count.
            if source.len() < before {
                // Progress starts a fresh wait episode; old contention must not
                // force every later chunk straight into scheduler yielding.
                spins = 0;
                continue;
            }
            if self.shared.live_consumers.load(Ordering::Acquire) == 0 {
                return false;
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        true
    }
}

impl<T: Send, B: Backend, R: Rings<T, B>> Drop for Producer<T, B, R> {
    fn drop(&mut self) {
        self.shared.live.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---- registrar ------------------------------------------------------------

/// Mints producers at runtime. Clone it to hand registration to many threads;
/// consumers keep running as long as any registrar or producer is alive.
pub struct Registrar<T: Send, B: Backend, R: Rings<T, B>> {
    shared: Arc<Shared<T, B, R>>,
    capacity: usize,
}

impl<T: Send, B: Backend, R: Rings<T, B>> Registrar<T, B, R> {
    /// Create a new producer with its own ring and publish it to the consumers.
    ///
    /// Panics for the [`array()`] runtime if more than the configured maximum
    /// producers are registered; [`locked`] is unbounded.
    pub fn register(&self) -> Producer<T, B, R> {
        let (tx, rx) = B::channel::<T>(self.capacity);
        // Publish the ring BEFORE counting the producer live, so it is discoverable
        // by the time anything is sent into it.
        self.shared.rings.push(rx);
        self.shared.live.fetch_add(1, Ordering::AcqRel);
        Producer {
            tx,
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send, B: Backend, R: Rings<T, B>> Clone for Registrar<T, B, R> {
    fn clone(&self) -> Self {
        self.shared.live.fetch_add(1, Ordering::AcqRel);
        Registrar {
            shared: Arc::clone(&self.shared),
            capacity: self.capacity,
        }
    }
}

impl<T: Send, B: Backend, R: Rings<T, B>> Drop for Registrar<T, B, R> {
    fn drop(&mut self) {
        self.shared.live.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---- consumer -------------------------------------------------------------

/// A competing consumer. Clone to add more; each value goes to exactly one clone.
pub struct Consumer<T: Send, B: Backend, R: Rings<T, B>> {
    shared: Arc<Shared<T, B, R>>,
    cursor: R::Cursor,
    staging: Batch<T>,
    batch: usize,
}

impl<T: Send, B: Backend, R: Rings<T, B>> Consumer<T, B, R> {
    /// Max values drained per claim (staged path). Default 64.
    pub fn set_batch(&mut self, batch: usize) {
        self.batch = batch.max(1);
    }

    /// Non-blocking receive (staged): claim a ring, drain a batch into local
    /// staging, release, serve one. See [`brokerless`](super) for the
    /// recv-vs-drain tradeoff — it applies identically here.
    #[inline]
    pub fn try_recv(&mut self) -> Option<T> {
        if let Some(v) = self.staging.pop_front() {
            return Some(v);
        }
        self.refill();
        self.staging.pop_front()
    }

    /// Receive a fresh owned batch without moving each value through `recv`.
    ///
    /// Drops unread values in `out` first, then reuses its allocation. Any values
    /// already staged by `recv` are returned together before claiming another
    /// ring; changing the batch limit does not truncate that staged prefix.
    /// Otherwise the configured batch limit bounds the next refill.
    ///
    /// The ring claim is released before returning. Process `out.as_slice()` to
    /// avoid per-value moves, or pop owned values from it. No ring slots remain
    /// borrowed. Mixing this method with `recv` and `drain` preserves staged order.
    /// A zero count does not prove global emptiness: another consumer may hold a
    /// non-empty ring.
    #[inline]
    pub fn try_recv_batch(&mut self, out: &mut Batch<T>) -> usize {
        out.clear();
        if self.staging.is_empty() {
            // Lend the cleared allocation to refill, then return it below.
            // Batch-only consumers need one reusable payload allocation. On
            // refill panic, any transferred values remain owned by staging.
            std::mem::swap(&mut self.staging, out);
            self.refill();
        }
        std::mem::swap(&mut self.staging, out);
        out.len()
    }

    /// Blocking counterpart of [`try_recv_batch`](Self::try_recv_batch).
    ///
    /// Returns zero when producers are gone and no more values are reachable by
    /// this consumer. Peers can still own other batches or claimed rings. Unread
    /// values in `out` are dropped before receiving the next batch.
    pub fn recv_batch(&mut self, out: &mut Batch<T>) -> usize {
        let mut spins = 0u32;
        loop {
            let received = self.try_recv_batch(out);
            if received != 0 {
                return received;
            }
            if self.is_disconnected() {
                return self.try_recv_batch(out);
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }

    // Keep the staged-value path inlineable without the scan's stack/register
    // setup. Refill only staging: returning T here can force an extra large-value
    // copy across a non-inlined return boundary. The caller pops after refill.
    // Not cold: batch=1 and sparse traffic can refill on every receive.
    #[inline(never)]
    fn refill(&mut self) {
        self.staging.clear();
        let n = self.shared.rings.count();
        for _ in 0..n {
            let Some(p) = self.shared.rings.next(&mut self.cursor) else {
                break;
            };
            if p.is_null() {
                continue; // reserved-but-unpublished cell (array runtime)
            }
            // SAFETY: non-null => a live slot, valid for the channel's lifetime.
            let slot: &Slot<T, B> = unsafe { &*p };
            if let Some(claim) = ReleaseClaim::try_acquire(&slot.claimed) {
                let rx = unsafe { &mut *slot.rx.get() };
                let staging = &mut self.staging;
                let batch = self.batch;
                rx.drain_into(batch, staging);
                drop(claim);
                if !self.staging.is_empty() {
                    R::prefer(&mut self.cursor);
                    return;
                }
            }
        }
    }

    /// True once every registrar and producer has been dropped. Values may remain —
    /// only `is_disconnected() && try_recv().is_none()` means this consumer is done.
    pub fn is_disconnected(&self) -> bool {
        self.shared.live.load(Ordering::Acquire) == 0
    }

    /// Blocking receive. `None` once drained and every registrar and producer is gone.
    pub fn recv(&mut self) -> Option<T> {
        let mut s = 0u32;
        loop {
            if let Some(v) = self.try_recv() {
                return Some(v);
            }
            if self.shared.live.load(Ordering::Acquire) == 0 {
                if let Some(v) = self.try_recv() {
                    return Some(v);
                }
                return None;
            }
            s = s.wrapping_add(1);
            if s < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                s = 0;
            }
        }
    }

    /// Zero-copy batch receive: claim the next ready ring and run `f` in place on up
    /// to `max` items (no staging), then release. Returns the count. `f` runs while
    /// the claim is held — see [`brokerless::Consumer::drain`](super::Consumer::drain).
    pub fn drain<F: FnMut(T)>(&mut self, max: usize, mut f: F) -> usize {
        let mut done = 0usize;
        while done < max {
            match self.staging.pop_front() {
                Some(v) => {
                    f(v);
                    done += 1;
                }
                None => break,
            }
        }
        if done >= max {
            return done;
        }
        let n = self.shared.rings.count();
        for _ in 0..n {
            let Some(p) = self.shared.rings.next(&mut self.cursor) else {
                break;
            };
            if p.is_null() {
                continue;
            }
            let slot: &Slot<T, B> = unsafe { &*p };
            if let Some(claim) = ReleaseClaim::try_acquire(&slot.claimed) {
                let rx = unsafe { &mut *slot.rx.get() };
                let got = rx.drain_in_place(max - done, &mut f);
                drop(claim);
                done += got;
                if got > 0 {
                    R::prefer(&mut self.cursor);
                    return done;
                }
            }
        }
        done
    }

    /// Blocking zero-copy consumer: run `f` on every value until drained and every
    /// registrar and producer is gone.
    pub fn for_each<F: FnMut(T)>(&mut self, batch: usize, mut f: F) {
        let batch = batch.max(1);
        let mut s = 0u32;
        loop {
            if self.drain(batch, &mut f) > 0 {
                s = 0;
                continue;
            }
            if self.shared.live.load(Ordering::Acquire) == 0 {
                if self.drain(batch, &mut f) == 0 {
                    return;
                }
                continue;
            }
            s = s.wrapping_add(1);
            if s < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                s = 0;
            }
        }
    }
}

impl<T: Send, B: Backend, R: Rings<T, B>> Clone for Consumer<T, B, R> {
    fn clone(&self) -> Self {
        let seed = self.shared.live_consumers.fetch_add(1, Ordering::AcqRel);
        Consumer {
            shared: Arc::clone(&self.shared),
            cursor: R::cursor(seed),
            staging: Batch::new(),
            batch: self.batch,
        }
    }
}

impl<T: Send, B: Backend, R: Rings<T, B>> Drop for Consumer<T, B, R> {
    fn drop(&mut self) {
        self.shared.live_consumers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn channel_with_rings<T: Send, B: Backend, R: Rings<T, B>>(
    rings: R,
    capacity: usize,
) -> (Registrar<T, B, R>, Consumer<T, B, R>) {
    let shared = Arc::new(Shared {
        rings,
        live: AtomicUsize::new(1),           // the initial registrar
        live_consumers: AtomicUsize::new(1), // the initial consumer
        _pd: PhantomData,
    });
    let registrar = Registrar {
        shared: Arc::clone(&shared),
        capacity: capacity.max(1),
    };
    let consumer = Consumer {
        shared,
        cursor: R::cursor(0),
        staging: Batch::new(),
        batch: 64,
    };
    (registrar, consumer)
}

// ===========================================================================
// Runtime 1: unbounded — Mutex<Vec> membership + per-consumer snapshot.
// ===========================================================================

/// Unbounded `Mutex<Vec>` membership. Registration locks briefly to append;
/// consumers cache a private snapshot and only re-lock when it grows, so
/// steady-state scanning is lock-free.
pub struct LockedRings<T: Send, B: Backend> {
    slots: Mutex<Vec<Arc<Slot<T, B>>>>,
    count: AtomicUsize,
}
impl<T: Send, B: Backend> sealed::Sealed for LockedRings<T, B> {}

/// Bounded-preference cursor plus a private ring snapshot, refreshed on growth.
#[doc(hidden)]
pub struct LockedCursor<T: Send, B: Backend> {
    scan: super::scan::Cursor,
    snap: Vec<Arc<Slot<T, B>>>,
}
impl<T: Send, B: Backend> Default for LockedCursor<T, B> {
    fn default() -> Self {
        LockedCursor {
            scan: super::scan::Cursor::new(0),
            snap: Vec::new(),
        }
    }
}

impl<T: Send, B: Backend> Rings<T, B> for LockedRings<T, B> {
    type Cursor = LockedCursor<T, B>;

    fn cursor(seed: usize) -> Self::Cursor {
        LockedCursor {
            scan: super::scan::Cursor::new(seed),
            snap: Vec::new(),
        }
    }

    #[inline]
    fn prefer(cur: &mut Self::Cursor) {
        cur.scan.prefer();
    }

    fn push(&self, rx: B::Rx<T>) {
        self.slots.lock().unwrap().push(Arc::new(Slot::new(rx)));
        self.count.fetch_add(1, Ordering::Release);
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn next(&self, cur: &mut LockedCursor<T, B>) -> Option<*const Slot<T, B>> {
        let n = self.count.load(Ordering::Acquire);
        if n == 0 {
            return None;
        }
        if cur.snap.len() < n {
            // Membership grew — refresh the private snapshot (brief lock, off the
            // per-item path since a claim drains a whole batch).
            cur.snap = self.slots.lock().unwrap().clone();
        }
        let len = cur.snap.len();
        let i = cur.scan.next(len);
        Some(Arc::as_ptr(&cur.snap[i]))
    }
}

/// Endpoints using unbounded, lock-protected membership.
pub type LockedEndpoints<T, B = Ring> = (
    Registrar<T, B, LockedRings<T, B>>,
    Consumer<T, B, LockedRings<T, B>>,
);

/// Endpoints using bounded, lock-free array membership.
pub type ArrayEndpoints<T, B = Ring> = (
    Registrar<T, B, ArrayRings<T, B>>,
    Consumer<T, B, ArrayRings<T, B>>,
);
pub(crate) fn open_locked<T: Send, B: Backend>(capacity: usize) -> LockedEndpoints<T, B> {
    channel_with_rings(
        LockedRings {
            slots: Mutex::new(Vec::new()),
            count: AtomicUsize::new(0),
        },
        capacity,
    )
}

pub(crate) fn open_array<T: Send, B: Backend>(
    capacity: usize,
    max_producers: usize,
) -> ArrayEndpoints<T, B> {
    let cells = (0..max_producers.max(1))
        .map(|_| AtomicPtr::new(core::ptr::null_mut()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    channel_with_rings(
        ArrayRings {
            cells,
            len: AtomicUsize::new(0),
        },
        capacity,
    )
}

// ===========================================================================
// Runtime 2: bounded — pre-allocated atomic array, lock-free append. No list.
// ===========================================================================

/// Lock-free **bounded** membership: a pre-allocated array of atomic slot pointers.
/// `push` claims the next index with a bounded compare-exchange and publishes its slot
/// with one `store`; consumers scan by direct array indexing. No lock, no linking —
/// just a flat array with an atomic append cursor.
pub struct ArrayRings<T: Send, B: Backend> {
    cells: Box<[AtomicPtr<Slot<T, B>>]>,
    /// Reserved-index cursor: bounded reservation gives each registration a distinct index.
    len: AtomicUsize,
}
impl<T: Send, B: Backend> sealed::Sealed for ArrayRings<T, B> {}

impl<T: Send, B: Backend> Rings<T, B> for ArrayRings<T, B> {
    type Cursor = super::scan::Cursor;

    fn cursor(seed: usize) -> Self::Cursor {
        super::scan::Cursor::new(seed)
    }

    #[inline]
    fn prefer(cur: &mut Self::Cursor) {
        cur.prefer();
    }

    fn push(&self, rx: B::Rx<T>) {
        // A rejected registration must not publish an out-of-bounds scan length.
        // Reserve before publishing the slot; consumers already skip null cells
        // while a successful registrar finishes construction.
        let i = self
            .len
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |i| {
                (i < self.cells.len()).then(|| i + 1)
            })
            .unwrap_or_else(|_| {
                panic!(
                    "array_channel: exceeded the {} max producers — raise the bound or use locked_channel",
                    self.cells.len()
                )
            });
        // Heap the slot and publish the pointer; consumers Acquire-load it.
        let raw = Box::into_raw(Box::new(Slot::new(rx)));
        self.cells[i].store(raw, Ordering::Release);
    }

    fn count(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    fn next(&self, cur: &mut Self::Cursor) -> Option<*const Slot<T, B>> {
        let n = self.len.load(Ordering::Acquire);
        if n == 0 {
            return None;
        }
        let i = cur.next(n);
        // May be null if index `i` was reserved but not yet published — the consumer
        // skips nulls.
        Some(self.cells[i].load(Ordering::Acquire) as *const Slot<T, B>)
    }
}

impl<T: Send, B: Backend> Drop for ArrayRings<T, B> {
    fn drop(&mut self) {
        // Exclusive at teardown: free every published slot. Dropping a Slot drops its
        // RingRx, whose Drop (and the ring's Inner::drop backstop) destroys any
        // undrained values exactly once.
        let n = (*self.len.get_mut()).min(self.cells.len());
        for cell in self.cells.iter_mut().take(n) {
            let p = *cell.get_mut();
            if !p.is_null() {
                // SAFETY: published via Box::into_raw; reclaim exactly once.
                drop(unsafe { Box::from_raw(p) });
            }
        }
    }
}
