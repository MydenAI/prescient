//! Brokerless competing-consumer MPMC.
//!
//! One SPSC ring per producer. Consumers prefer a productive ring for a bounded
//! number of drains, then advance round-robin. Failed claims or empty rings
//! advance the scan, so any consumer can steal available work. To read a
//! ring a consumer must first win its `claimed` flag with a single CAS, which
//! grants exclusive access to that ring's `Receiver` for the duration of a
//! batch drain. The claim is what upholds the ring's single-consumer invariant
//! while any number of consumers work in parallel across *different* rings —
//! contention is on the per-ring claim, not per message, and stays low while
//! `producers >= consumers`.
//!
//! There is no dedicated thread and nothing on the hot path but the producer's
//! own SPSC push and the consumer's claim + drain.

use crate::backend::Batch;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub mod dynamic;
pub mod leased;
mod scan;
use scan::Cursor;

// Construct only after successfully acquiring a slot. Backend drain guards
// finish publishing consumed positions before this outer guard releases the
// claim on unwind. The ordinary path still performs one Release store.
struct ReleaseClaim<'a>(&'a AtomicBool);

impl<'a> ReleaseClaim<'a> {
    #[inline]
    fn try_acquire(claimed: &'a AtomicBool) -> Option<Self> {
        // A busy hint only delays the RMW by four bounded pauses. Still attempt
        // every claim: a stale hint must not turn the final scan into "empty".
        if claimed.load(Ordering::Relaxed) {
            for _ in 0..4 {
                std::hint::spin_loop();
            }
        }
        claimed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Self(claimed))
    }
}

impl Drop for ReleaseClaim<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

use crate::backend::{Backend, Ring, Rx as _, Tx as _};

/// Declare fixed-membership brokerless MPMC with Ring storage and spin/yield
/// waiting defaults.
pub fn channel<T>() -> crate::Channel<
    T,
    crate::topology::Mpmc<crate::engine::Claim>,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new().mpmc()
}

/// One producer's ring plus the claim flag that serializes consumer access to it.
struct Slot<T: Send, B: Backend> {
    /// `false -> true` (CAS, Acquire) grants the winner exclusive access to `rx`;
    /// the matching `true -> false` store (Release) publishes its drain to the
    /// next claimer.
    claimed: AtomicBool,
    rx: UnsafeCell<B::Rx<T>>,
}

struct Shared<T: Send, B: Backend> {
    slots: Box<[Slot<T, B>]>,
    /// Producers still alive. Decremented on `Producer::drop`. A consumer that
    /// sees this hit zero, then makes one full claim-pass finding nothing, is done.
    live_producers: AtomicUsize,
    /// Consumers still alive (the initial consumer counts as one; each clone adds,
    /// each drop subtracts). A blocking `send` that finds its ring full *and* this
    /// at zero gives up — otherwise it would spin forever with no one to drain.
    live_consumers: AtomicUsize,
}

// SAFETY: `slot.rx` is touched only by the consumer that currently holds
// `slot.claimed`. The CAS that acquires the flag is `Acquire`; the store that
// releases it is `Release`, so a claimer observes every write the previous
// claimer made. At most one thread aliases a given `RingRx` at a time, and
// `RingRx` is `Send`, so sharing `&Shared` across threads is sound.
unsafe impl<T: Send, B: Backend> Sync for Shared<T, B> {}

/// A producer half. One dedicated SPSC ring; never contends with other producers.
pub struct Producer<T: Send, B: Backend = Ring> {
    tx: B::Tx<T>,
    shared: Arc<Shared<T, B>>,
}

/// A consumer half. Clone it to add more competing consumers; each clone pulls
/// from the shared pool of producer rings, and any given value goes to exactly
/// one of them.
pub struct Consumer<T: Send, B: Backend = Ring> {
    shared: Arc<Shared<T, B>>,
    /// Consumer-local bounded preference; no claim survives a drain.
    cursor: Cursor,
    /// Values drained under a single claim but not yet returned, so we amortize
    /// the CAS over a batch instead of paying it per message.
    staging: Batch<T>,
    batch: usize,
}

pub(crate) fn open<T: Send, B: Backend>(
    n_producers: usize,
    capacity: usize,
) -> (Vec<Producer<T, B>>, Consumer<T, B>) {
    let mut txs = Vec::with_capacity(n_producers);
    let mut slots = Vec::with_capacity(n_producers);
    for _ in 0..n_producers {
        let (tx, rx) = B::channel::<T>(capacity);
        txs.push(tx);
        slots.push(Slot {
            claimed: AtomicBool::new(false),
            rx: UnsafeCell::new(rx),
        });
    }
    let shared = Arc::new(Shared {
        slots: slots.into_boxed_slice(),
        live_producers: AtomicUsize::new(n_producers),
        live_consumers: AtomicUsize::new(1),
    });
    let producers = txs
        .into_iter()
        .map(|tx| Producer {
            tx,
            shared: Arc::clone(&shared),
        })
        .collect();
    let consumer = Consumer {
        shared,
        cursor: Cursor::new(0),
        staging: Batch::new(),
        batch: 64,
    };
    (producers, consumer)
}

impl<T: Send, B: Backend> Producer<T, B> {
    /// Non-blocking send. Returns the value back if this producer's ring is full.
    #[inline]
    pub fn try_send(&mut self, v: T) -> Result<(), T> {
        self.tx.try_push(v)
    }

    /// Send, spinning while this producer's ring is full (a consumer will drain it).
    ///
    /// If every consumer has been dropped while the ring is full, the value is
    /// undeliverable: `send` returns and the value is dropped (its destructor runs)
    /// rather than spinning forever. Returns `true` if the value was enqueued,
    /// `false` if it was discarded because no consumer remained.
    pub fn send(&mut self, mut v: T) -> bool {
        let mut s = 0u32;
        loop {
            match self.tx.try_push(v) {
                Ok(()) => return true,
                Err(back) => {
                    // Nobody left to drain this ring: give up instead of hanging.
                    if self.shared.live_consumers.load(Ordering::Acquire) == 0 {
                        return false; // `back` dropped here
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

impl<T: Send, B: Backend> Drop for Producer<T, B> {
    fn drop(&mut self) {
        self.shared.live_producers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: Send, B: Backend> Consumer<T, B> {
    /// Set the max values drained per claim (batching amortizes the CAS). Default 64.
    pub fn set_batch(&mut self, batch: usize) {
        self.batch = batch.max(1);
    }

    /// Non-blocking receive. Returns `None` if no ring currently yields a value to
    /// this consumer — which does *not* prove the channel is empty (another
    /// consumer may hold a non-empty ring); use [`recv`](Self::recv) for the
    /// drain-to-completion contract.
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
        let n = self.shared.slots.len();
        for _ in 0..n {
            let i = self.cursor.next(n);
            let slot = &self.shared.slots[i];
            // Try to take this ring. On failure another consumer owns it — skip.
            if let Some(claim) = ReleaseClaim::try_acquire(&slot.claimed) {
                // Exclusive access to slot `i`'s RingRx while we hold the flag.
                let rx = unsafe { &mut *slot.rx.get() };
                let staging = &mut self.staging;
                let batch = self.batch;
                rx.drain_into(batch, staging);
                drop(claim);
                if !self.staging.is_empty() {
                    self.cursor.prefer();
                    return;
                }
            }
        }
    }

    /// True once every producer has been dropped. Values may remain in rings —
    /// only `is_disconnected() && try_recv().is_none()` means this consumer is done.
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_producers.load(Ordering::Acquire) == 0
    }

    /// Blocking receive. Returns `Some` until the channel is drained *and* every
    /// producer has been dropped, then `None`.
    ///
    /// Note: a consumer may return `None` while values still sit in rings that are
    /// momentarily claimed by *other* consumers — those values are delivered by
    /// those consumers, so nothing is lost; this consumer just finishes first.
    pub fn recv(&mut self) -> Option<T> {
        let mut s = 0u32;
        loop {
            if let Some(v) = self.try_recv() {
                return Some(v);
            }
            if self.shared.live_producers.load(Ordering::Acquire) == 0 {
                // Producers gone => no new values. One more full claim-pass; if it
                // finds nothing, everything reachable by us is drained.
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

    /// Zero-copy batch receive. Claims the next ready ring and runs `f` on up to
    /// `max` items **in place** — no staging buffer, one move per item straight to
    /// `f` — then releases. Returns the number processed this call (0 if no ring
    /// yielded anything). Non-blocking.
    ///
    /// The catch: `f` runs while the ring's claim is **held**, so a slow `f` blocks
    /// other consumers from *that* ring. This is the mirror of [`recv`](Self::recv),
    /// which copies a batch into staging and releases the claim *before* you touch
    /// the items:
    ///
    /// * `drain` — lowest per-item cost; best when consumers are spread across rings
    ///   (little claim contention) or per-item work is light.
    /// * `recv` — best when many consumers share few hot rings under heavy per-item
    ///   work, since releasing the claim first lets them process in parallel.
    pub fn drain<F: FnMut(T)>(&mut self, max: usize, mut f: F) -> usize {
        let mut done = 0usize;
        // Flush anything a prior recv() left staged (usually nothing), so drain and
        // recv can be mixed on one consumer without stranding values.
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
        let n = self.shared.slots.len();
        for _ in 0..n {
            let i = self.cursor.next(n);
            let slot = &self.shared.slots[i];
            if let Some(claim) = ReleaseClaim::try_acquire(&slot.claimed) {
                // Exclusive access while the flag is held; `f` runs in place.
                let rx = unsafe { &mut *slot.rx.get() };
                let got = rx.drain_in_place(max - done, &mut f);
                drop(claim);
                done += got;
                if got > 0 {
                    self.cursor.prefer();
                    return done; // never retain a claim between calls
                }
            }
        }
        done
    }

    /// Blocking zero-copy consumer: run `f` on every value delivered to this
    /// consumer, in place, until the channel is drained and all producers are gone.
    /// The zero-copy counterpart of `while let Some(v) = recv() {}`.
    pub fn for_each<F: FnMut(T)>(&mut self, batch: usize, mut f: F) {
        let batch = batch.max(1);
        let mut s = 0u32;
        loop {
            if self.drain(batch, &mut f) > 0 {
                s = 0;
                continue;
            }
            if self.shared.live_producers.load(Ordering::Acquire) == 0 {
                // Producers gone: one more pass; empty => everything reachable by us
                // is drained (rings still claimed by peers are drained by them).
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

impl<T: Send, B: Backend> Clone for Consumer<T, B> {
    fn clone(&self) -> Self {
        // This cold counter also dephases clones. Reused seeds after drops are
        // harmless: a preference never grants exclusive ownership.
        let seed = self.shared.live_consumers.fetch_add(1, Ordering::AcqRel);
        let cursor = Cursor::new(seed);
        Consumer {
            shared: Arc::clone(&self.shared),
            cursor,
            staging: Batch::new(),
            batch: self.batch,
        }
    }
}

impl<T: Send, B: Backend> Drop for Consumer<T, B> {
    fn drop(&mut self) {
        self.shared.live_consumers.fetch_sub(1, Ordering::AcqRel);
        // Any values still staged in this consumer are dropped with `self.staging`
        // (their destructors run); recv() only returns None with staging empty, so
        // this is non-empty only if the caller abandoned the consumer early.
    }
}
