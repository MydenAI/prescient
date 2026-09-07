//! Bounded SPSC ring used by the local channel topologies.
//!
//! Monotonically increasing head and tail counters address a power-of-two buffer
//! through `counter & mask`. Each endpoint caches the peer's counter, avoiding a
//! shared atomic load on the common path.
//!
//! Memory visibility and slot ownership depend on the Acquire/Release protocol.
//! The inline Loom models exercise that protocol independently of x86-TSO;
//! ordering changes require those models to pass.

#[cfg(all(test, not(loom)))]
#[path = "ring_bulk_tests.rs"]
mod bulk_tests;
#[cfg(all(test, not(loom)))]
#[path = "ring_publish_tests.rs"]
mod publish_tests;
use super::Batch;
use crate::platform::Arc;
use core::mem::MaybeUninit;

use crate::platform::AtomicPtr;
use crate::platform::{AtomicBool, AtomicUsize, CachePad, Ordering, UnsafeCell};
use crate::task_waker::TaskWaiter;

#[repr(C)]
struct Tail<T> {
    cursor: AtomicUsize,
    // Intrusive link for type-selected dynamic lock-free membership. Deposit
    // finishes before the producer is returned, and drain reads the link before
    // the receiver enters service, so it never overlaps tail traffic. CachePad
    // absorbs this cold field without enlarging the producer-owned cache line.
    inbox_next: AtomicPtr<Inner<T>>,
}

struct Inner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    cap: usize,
    head: CachePad<AtomicUsize>, // next slot the consumer will read
    tail: CachePad<Tail<T>>,     // next slot the producer will write
    // one side of the pair has been dropped:
    producer_gone: AtomicBool,
    consumer_gone: AtomicBool,
}

// SAFETY: access to `buf` is disciplined by the head/tail protocol — the
// producer only touches slot `tail`, the consumer only slot `head`, and the
// Release/Acquire on those counters establishes happens-before so the two never
// touch the same slot without synchronization. loom verifies this.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    /// Map any wrapping cursor into storage validated once by build().
    #[inline]
    fn slot(&self, cursor: usize) -> &UnsafeCell<MaybeUninit<T>> {
        // SAFETY: build() is the sole constructor. It sets buf.len() = cap > 0
        // and mask = cap - 1 after checked power-of-two rounding. Neither the
        // buffer length nor mask changes, including during recycling. Therefore
        // cursor & mask <= mask < buf.len(), even when cursor wraps. This only
        // removes bounds checking; slot ownership and synchronization still
        // depend on the existing head/tail protocol and with_mut checks.
        unsafe { self.buf.get_unchecked(cursor & self.mask) }
    }
}

fn build<T>(capacity: usize) -> Arc<Inner<T>> {
    let cap = capacity
        .max(1)
        .checked_next_power_of_two()
        .expect("ring capacity exceeds supported power of two");
    let mut v = Vec::with_capacity(cap);
    for _ in 0..cap {
        v.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    Arc::new(Inner {
        buf: v.into_boxed_slice(),
        mask: cap - 1,
        cap,
        head: CachePad(AtomicUsize::new(0)),
        tail: CachePad(Tail {
            cursor: AtomicUsize::new(0),
            inbox_next: AtomicPtr::new(core::ptr::null_mut()),
        }),
        producer_gone: AtomicBool::new(false),
        consumer_gone: AtomicBool::new(false),
    })
}

/// Create one SPSC ring, returning the two single-owner halves.
pub(crate) fn ring<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    halves(build(capacity))
}

fn halves<T>(inner: Arc<Inner<T>>) -> (Sender<T>, Receiver<T>) {
    (
        Sender {
            inner: inner.clone(),
            head_cache: 0,
        },
        Receiver {
            inner,
            tail_cache: 0,
        },
    )
}

/// Single-producer half of an internal SPSC ring. It is `Send` but not `Clone`.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    head_cache: usize, // last-seen consumer position (may lag; refreshed on "full")
}

/// Single-consumer half of an internal SPSC ring; it is not cloneable.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    tail_cache: usize, // last-seen producer position (may lag; refreshed on "empty")
}

impl<T> Sender<T> {
    /// Non-blocking push. Returns the item back on a full ring.
    #[inline]
    pub(crate) fn try_push(&mut self, item: T) -> Result<(), T> {
        // Only this thread writes `tail`, so Relaxed is fine to load our own.
        let tail = self.inner.tail.0.cursor.load(Ordering::Relaxed);
        if tail.wrapping_sub(self.head_cache) >= self.inner.cap {
            // cache says full — refresh from the shared counter before giving up
            self.head_cache = self.inner.head.0.load(Ordering::Acquire);
            if tail.wrapping_sub(self.head_cache) >= self.inner.cap {
                return Err(item);
            }
        }
        let slot = self.inner.slot(tail);
        slot.with_mut(|p| unsafe { (*p).write(item) });
        // publish: everything above happens-before a consumer that Acquire-loads tail
        self.inner
            .tail
            .0
            .cursor
            .store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
    /// Publish a finite owned prefix once, without callbacks between moves.
    #[inline(always)]
    pub(crate) fn push_from(&mut self, max: usize, source: &mut Batch<T>) -> usize {
        let limit = source.len().min(max);
        if limit == 0 {
            return 0;
        }
        let tail = self.inner.tail.0.cursor.load(Ordering::Relaxed);
        let mut used = tail.wrapping_sub(self.head_cache);
        if used >= self.inner.cap {
            self.head_cache = self.inner.head.0.load(Ordering::Acquire);
            used = tail.wrapping_sub(self.head_cache);
        }
        let n = limit.min(self.inner.cap - used);
        if n == 0 {
            return 0;
        }
        if n == 1 {
            // Keep singleton batches out of variable-size copy setup.
            // SAFETY: n == 1 proves read < len. Avoid rechecking that fact;
            // no fallible work occurs between the move and tail publication.
            let read = source.read;
            let value = unsafe { source.values.get_unchecked(read).assume_init_read() };
            source.read = read + 1;
            self.inner
                .slot(tail)
                .with_mut(|p| unsafe { (*p).write(value) });
        } else {
            #[cfg(not(loom))]
            {
                let start = tail & self.inner.mask;
                let first = n.min(self.inner.cap - start);
                // SAFETY: Acquire head grants exclusive access to n free slots.
                // Production UnsafeCell and MaybeUninit are transparent, so the
                // ring allocation has T's stride/alignment. The distinct source
                // allocation owns exactly values[read..]; n <= source.len().
                // These copies move ownership, including non-Copy/padded/ZST T.
                // Nothing can allocate, call user code, or unwind between moving
                // bytes, advancing source ownership and publishing the tail.
                unsafe {
                    let src = source.values.as_ptr().add(source.read).cast::<T>();
                    let dst = self.inner.buf.as_ptr().cast::<T>().cast_mut();
                    std::ptr::copy_nonoverlapping(src, dst.add(start), first);
                    if first != n {
                        std::ptr::copy_nonoverlapping(src.add(first), dst, n - first);
                    }
                    source.read += n;
                }
            }
            #[cfg(loom)]
            {
                // Loom cells are not transparent: model each write, retaining
                // the same one-publication protocol. Miri checks the raw spans.
                for offset in 0..n {
                    let value = source.pop_front().unwrap();
                    self.inner
                        .slot(tail.wrapping_add(offset))
                        .with_mut(|p| unsafe { (*p).write(value) });
                }
            }
        }
        self.inner
            .tail
            .0
            .cursor
            .store(tail.wrapping_add(n), Ordering::Release);
        n
    }

    #[inline]
    pub(crate) fn is_consumer_gone(&self) -> bool {
        self.inner.consumer_gone.load(Ordering::Acquire)
    }
}

impl<T> Receiver<T> {
    /// Non-blocking pop.
    #[inline]
    pub(crate) fn try_pop(&mut self) -> Option<T> {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        if head == self.tail_cache {
            self.tail_cache = self.inner.tail.0.cursor.load(Ordering::Acquire);
            if head == self.tail_cache {
                return None;
            }
        }
        let slot = self.inner.slot(head);
        let val = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
        // release the slot back to the producer
        self.inner
            .head
            .0
            .store(head.wrapping_add(1), Ordering::Release);
        Some(val)
    }

    /// Append ready owned values with one reservation and at most two moves.
    #[cfg(not(loom))]
    #[inline(always)]
    pub(crate) fn drain_into(&mut self, max: usize, out: &mut Batch<T>) -> usize {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        let mut available = self.tail_cache.wrapping_sub(head);
        if available == 0 {
            self.tail_cache = self.inner.tail.0.cursor.load(Ordering::Acquire);
            available = self.tail_cache.wrapping_sub(head);
        }
        let n = available.min(max);
        if n == 0 {
            return 0;
        }
        if n == 1 {
            // Do not calculate wrap spans or set up a variable-size move for a
            // single ready value. This is data availability, not config dispatch.
            out.reserve(1);
            let len = out.values.len();
            let slot = self.inner.slot(head);
            // SAFETY: one published slot is exclusively owned and the destination
            // is reserved. No fallible work occurs between move and publication.
            slot.with_mut(|src| unsafe {
                out.values
                    .as_mut_ptr()
                    .add(len)
                    .write(MaybeUninit::new((*src).assume_init_read()));
                out.values.set_len(len + 1);
            });
            self.inner
                .head
                .0
                .store(head.wrapping_add(1), Ordering::Release);
            return 1;
        }
        // All potentially panicking work precedes moving any ring ownership.
        out.reserve(n);
        let len = out.values.len();
        let start = head & self.inner.mask;
        let first = n.min(self.inner.cap - start);
        // SAFETY: production UnsafeCell and MaybeUninit are transparent wrappers
        // around T. The allocation is contiguous and has T's stride/alignment.
        // Acquire tail establishes ownership of these n initialized slots; the
        // producer cannot reuse them until the Release below. The destination
        // owns a separate allocation, reserved for len+n values. Moving bytes
        // transfers ownership even for non-Copy T (including padding and ZSTs).
        // No callback, allocation, or destructor can unwind between the copies
        // and commits. The old slots are now unowned, never dropped as T again.
        unsafe {
            let src = self.inner.buf.as_ptr().cast::<T>();
            let dst = out.values.as_mut_ptr().add(len).cast::<T>();
            std::ptr::copy_nonoverlapping(src.add(start), dst, first);
            if first != n {
                std::ptr::copy_nonoverlapping(src, dst.add(first), n - first);
            }
            out.values.set_len(len + n);
        }
        self.inner
            .head
            .0
            .store(head.wrapping_add(n), Ordering::Release);
        n
    }

    // Loom's cells are NOT transparent. Track each source access for protocol
    // models; Miri tests exercise the production raw-span implementation.
    #[cfg(loom)]
    #[inline]
    pub(crate) fn drain_into(&mut self, max: usize, out: &mut Batch<T>) -> usize {
        self.drain_in_place(max, |value| out.push_back(value))
    }
    /// Drain up to `max` ready items *in place*, invoking `f` on each, then
    /// release ALL of them to the producer with a single Release store of `head`
    /// (vs. one per item in [`try_pop`]). No copy into an intermediate buffer.
    /// Returns the number drained.
    ///
    /// A commit guard advances `head` by the number of slots actually moved out
    /// even if `f` panics, so the ring can never re-read a moved-out slot.
    pub(crate) fn drain_in_place<F: FnMut(T)>(&mut self, max: usize, mut f: F) -> usize {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        let mut avail = self.tail_cache.wrapping_sub(head);
        if avail == 0 {
            self.tail_cache = self.inner.tail.0.cursor.load(Ordering::Acquire);
            avail = self.tail_cache.wrapping_sub(head);
            if avail == 0 {
                return 0;
            }
        }
        let n = avail.min(max);

        // Commit `head = base + done` on scope exit (normal OR panic), so a slot
        // that was moved out but not yet released is never handed to a producer
        // and never re-read by the consumer's drop-drain.
        struct Commit<'a> {
            head: &'a AtomicUsize,
            base: usize,
            done: usize,
        }
        impl Drop for Commit<'_> {
            fn drop(&mut self) {
                self.head
                    .store(self.base.wrapping_add(self.done), Ordering::Release);
            }
        }
        let mut commit = Commit {
            head: &self.inner.head.0,
            base: head,
            done: 0,
        };

        for k in 0..n {
            let slot = self.inner.slot(head.wrapping_add(k));
            let val = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
            commit.done = k + 1; // slot k is now moved out; guard will release it
            f(val);
        }
        n
    }

    /// Publish capacity once per batch, then notify through the fenced handshake.
    pub(crate) fn drain_in_place_async<F: FnMut(T)>(
        &mut self,
        max: usize,
        waiter: &TaskWaiter,
        mut f: F,
    ) -> usize {
        if max == 0 {
            return 0;
        }
        let head = self.inner.head.0.load(Ordering::Relaxed);
        let mut avail = self.tail_cache.wrapping_sub(head);
        if avail == 0 {
            self.tail_cache = self.inner.tail.0.cursor.load(Ordering::Acquire);
            avail = self.tail_cache.wrapping_sub(head);
            if avail == 0 {
                return 0;
            }
        }
        let n = avail.min(max);

        struct Commit<'a> {
            head: &'a AtomicUsize,
            waiter: &'a TaskWaiter,
            base: usize,
            done: usize,
        }
        impl Drop for Commit<'_> {
            fn drop(&mut self) {
                self.head
                    .store(self.base.wrapping_add(self.done), Ordering::Release);
                self.waiter.notify();
            }
        }
        let mut commit = Commit {
            head: &self.inner.head.0,
            waiter,
            base: head,
            done: 0,
        };
        for k in 0..n {
            let slot = self.inner.slot(head.wrapping_add(k));
            let val = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
            commit.done = k + 1;
            f(val);
        }
        n
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        head == self.inner.tail.0.cursor.load(Ordering::Acquire)
    }

    /// Reset a *finished*, fully-drained ring to pristine state and hand back a
    /// fresh `Sender` sharing the same buffer, so the allocation can be reused.
    ///
    /// Returns `None` if the ring is not exclusively owned — i.e. the producer's
    /// `Arc` has not been released yet. `Arc::get_mut` succeeding is the safety
    /// gate: it proves no other handle (no live producer) can touch `Inner`, so
    /// resetting the counters/flags cannot race. The caller must only invoke this
    /// on a ring it pruned via `is_finished()` (producer gone, buffer drained),
    /// which guarantees no slot holds a live `T`.
    pub(crate) fn recycle(&mut self) -> Option<Sender<T>> {
        // Exclusive ownership (refcount == 1) => the resets below cannot race.
        let inner = Arc::get_mut(&mut self.inner)?;
        // Drained ring: head == tail, no live slots. Reset to a clean epoch.
        inner.head.0.store(0, Ordering::Relaxed);
        inner.tail.0.cursor.store(0, Ordering::Relaxed);
        inner.producer_gone.store(false, Ordering::Relaxed);
        // consumer_gone stays false: the consumer never left.
        self.tail_cache = 0;
        Some(Sender {
            inner: self.inner.clone(),
            head_cache: 0,
        })
    }

    /// Producer dropped AND nothing left to read: this ring is finished and can
    /// be pruned by the consumer. No epoch/GC — the ring frees itself when the
    /// last `Arc` refcount drops.
    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        self.inner.producer_gone.load(Ordering::Acquire) && self.is_empty()
    }

    /// Publish consumer teardown before the outer channel wakes a producer that
    /// may be waiting for capacity. Idempotent with this endpoint's `Drop`.
    #[inline]
    pub(crate) fn close(&self) {
        self.inner.consumer_gone.store(true, Ordering::Release);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.producer_gone.store(true, Ordering::Release);
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.consumer_gone.store(true, Ordering::Release);
        // Drain unread slots promptly so T's Drop runs at receiver-drop time. This
        // is best-effort: a producer racing this teardown can still write a slot
        // after the loop exits (it read consumer_gone before the store landed).
        // Inner::drop is the authoritative backstop that reclaims any such value.
        while self.try_pop().is_some() {}
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Authoritative cleanup of the ring's contents. Runs only when the last Arc
        // is released, so it is exclusive and race-free: `head`/`tail` are final and
        // untouched by anyone else. Drop every value still occupying the ring —
        // the half-open range [head, tail). Already-consumed slots sit below `head`
        // (each pop/drain advanced it past them) and are excluded, so this never
        // double-drops; a value a producer wrote after the consumer's own drain
        // loop exited sits in [head, tail) and is reclaimed here — without this, its
        // destructor would silently never run (a send racing teardown).
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.cursor.load(Ordering::Relaxed);
        let mut i = head;
        while i != tail {
            let slot = &self.buf[i & self.mask];
            // SAFETY: slots in [head, tail) are initialized (written by the producer,
            // not yet consumed) and exclusively ours now. Drop each in place once.
            slot.with_mut(|p| unsafe { (*p).assume_init_drop() });
            i = i.wrapping_add(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Intrusive lock-free Inbox (dynamic tier, selected by its membership type).
//
// A Treiber stack whose nodes ARE the rings: each `Receiver` is pushed by leaking
// its `Arc<Inner>` (`Arc::into_raw`) and linking through the ring's own
// `inbox_next` field — so a producer join costs ZERO heap allocation, unlike the
// boxed-node stack it replaces. Many producers push with an AcqRel CAS; the single
// consumer detaches the entire chain with one Acquire swap-to-null. Push never
// dereferences the old head, so this MPSC push / drain-all pattern is ABA-safe
// without tagging. Ordering identical to the boxed stack (loom-verified via
// `chan::loom_inbox`); only the node's storage location moved into `Inner`.
// ---------------------------------------------------------------------------

pub(crate) struct InboxStack<T> {
    head: AtomicPtr<Inner<T>>,
    // Constrain auto-`Send` to `T: Send` (an `AtomicPtr` is unconditionally Send);
    // the stack logically owns the `Receiver`s parked in it.
    _marker: core::marker::PhantomData<Receiver<T>>,
}

// SAFETY: the head is atomic; a ring being pushed is exclusively owned by its
// pusher until the publishing CAS; a detached chain is exclusively owned by the
// single consumer. The parked `Receiver`s only ever move between threads (Send),
// never shared. This supplies `Sync` (suppressing the auto impl); the marker keeps
// `Send` correct (iff `T: Send`).

unsafe impl<T: Send> Sync for InboxStack<T> {}

impl<T> InboxStack<T> {
    pub(crate) fn new() -> Self {
        InboxStack {
            head: AtomicPtr::new(core::ptr::null_mut()),
            _marker: core::marker::PhantomData,
        }
    }

    /// Producer side: park a ring receiver on the stack with no allocation.
    pub(crate) fn push(&self, rx: Receiver<T>) {
        // Take the ring's Arc WITHOUT running `Receiver::drop` (which would tear
        // the ring down). Discard the cached tail and reconstruct it at the
        // published head, even for a partially consumed ring. The first read
        // refreshes tail with Acquire before accessing a slot.
        let rx = core::mem::ManuallyDrop::new(rx);
        // SAFETY: `rx` is never used again and is not dropped (ManuallyDrop), so
        // moving `inner` out via read transfers the single Arc strong ref cleanly.
        let inner = unsafe { core::ptr::read(&rx.inner) };
        let node = Arc::into_raw(inner) as *mut Inner<T>;
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // Set our link before publishing; the Release CAS below carries it.
            // SAFETY: `node` is exclusively ours until the CAS succeeds.
            unsafe { (*node).tail.0.inbox_next.store(head, Ordering::Relaxed) };
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(h) => head = h,
            }
        }
    }

    /// Consumer side: detach the whole chain without an intermediate allocation.
    pub(crate) fn drain_with(&self, mut receive: impl FnMut(Receiver<T>)) {
        let mut node = self.head.swap(core::ptr::null_mut(), Ordering::Acquire);
        while !node.is_null() {
            // SAFETY: detached from `head`, so this ring is now exclusively ours.
            // Read the link before reclaiming the Arc (from_raw balances into_raw).
            let next = unsafe { (*node).tail.0.inbox_next.load(Ordering::Relaxed) };
            let inner = unsafe { Arc::from_raw(node as *const Inner<T>) };
            let head = inner.head.0.load(Ordering::Relaxed);
            receive(Receiver {
                inner,
                tail_cache: head,
            });
            node = next;
        }
    }
}

impl<T> Drop for InboxStack<T> {
    fn drop(&mut self) {
        // Reclaim any rings deposited but never drained. Reconstructing the full
        // `Receiver` (not just the Arc) runs its Drop, so buffered `T`s are freed.
        let mut node = self.head.load(Ordering::Relaxed);
        while !node.is_null() {
            let next = unsafe { (*node).tail.0.inbox_next.load(Ordering::Relaxed) };
            let inner = unsafe { Arc::from_raw(node as *const Inner<T>) };
            let head = inner.head.0.load(Ordering::Relaxed);
            drop(Receiver {
                inner,
                tail_cache: head,
            });
            node = next;
        }
    }
}

// ---------------------------------------------------------------------------
// loom model checks — the actual correctness gate for the memory ordering.
// Run with:  RUSTFLAGS="--cfg loom" cargo test --release loom_ --lib
// ---------------------------------------------------------------------------
#[cfg(all(loom, test))]
mod loom_tests {
    use super::ring;
    #[test]
    fn loom_owned_batch_publication_reuse() {
        loom::model(|| {
            let (mut tx, mut rx) = ring::<u32>(2);
            let sender = loom::thread::spawn(move || {
                let mut source = super::Batch::new();
                for value in 0..3 {
                    source.push_back(value);
                }
                while !source.is_empty() {
                    if tx.push_from(2, &mut source) == 0 {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut out = super::Batch::new();
            while out.len() < 3 {
                if rx.drain_into(2, &mut out) == 0 {
                    loom::thread::yield_now();
                }
            }
            sender.join().unwrap();
            assert_eq!(out.as_slice(), [0, 1, 2]);
        });
    }

    // Basic handoff: producer publishes items, consumer must observe them in
    // order with no loss/dup across every interleaving. Capacity > items, so no
    // slot reuse — isolates the tail(Release)->tail(Acquire) publish handshake.
    #[test]
    fn loom_handoff_no_reuse() {
        loom::model(|| {
            let (mut tx, mut rx) = ring::<u32>(4);
            let h = loom::thread::spawn(move || {
                for i in 1..=2u32 {
                    while tx.try_push(i).is_err() {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut got = Vec::new();
            while got.len() < 2 {
                if let Some(v) = rx.try_pop() {
                    got.push(v);
                } else {
                    loom::thread::yield_now();
                }
            }
            h.join().unwrap();
            assert_eq!(got, vec![1, 2]);
        });
    }

    // Slot reuse: capacity 2, three items -> the producer must reuse a slot only
    // after the consumer has released it. Exercises the head(Release)->
    // head(Acquire) path that prevents overwriting an unread slot. If that
    // ordering is wrong, loom flags a data race on the UnsafeCell here.
    #[test]
    fn loom_slot_reuse() {
        loom::model(|| {
            let (mut tx, mut rx) = ring::<u32>(2);
            let h = loom::thread::spawn(move || {
                for i in 1..=3u32 {
                    while tx.try_push(i).is_err() {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut got = Vec::new();
            while got.len() < 3 {
                if let Some(v) = rx.try_pop() {
                    got.push(v);
                } else {
                    loom::thread::yield_now();
                }
            }
            h.join().unwrap();
            assert_eq!(got, vec![1, 2, 3]);
        });
    }

    // Same slot-reuse scenario, but the consumer drains via `drain_in_place`,
    // which releases a whole batch with ONE head Release store. Verifies the
    // batched reclaim still establishes the head(Release)->head(Acquire) ordering
    // that lets the producer safely reuse a slot (no data race, no loss/dup).
    #[test]
    fn loom_drain_in_place_reuse() {
        loom::model(|| {
            let (mut tx, mut rx) = ring::<u32>(2);
            let h = loom::thread::spawn(move || {
                for i in 1..=3u32 {
                    while tx.try_push(i).is_err() {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut got = Vec::new();
            while got.len() < 3 {
                if rx.drain_in_place(2, |v| got.push(v)) == 0 {
                    loom::thread::yield_now();
                }
            }
            h.join().unwrap();
            assert_eq!(got, vec![1, 2, 3]);
        });
    }

    // Tracked per-cell fallback checks the bulk operation's publish/reuse
    // protocol. It does not model the production raw memcpy implementation.
    #[test]
    fn loom_drain_into_reuse() {
        loom::model(|| {
            let (mut tx, mut rx) = ring::<u32>(2);
            let sender = loom::thread::spawn(move || {
                for value in 1..=3 {
                    while tx.try_push(value).is_err() {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut out = crate::backend::Batch::new();
            while out.len() < 3 {
                if rx.drain_into(2, &mut out) == 0 {
                    loom::thread::yield_now();
                }
            }
            sender.join().unwrap();
            assert_eq!(out.as_slice(), [1, 2, 3]);
        });
    }
    // Teardown race: a producer pushes while the consumer half is dropped out from
    // under it. Across EVERY interleaving, every constructed value must be dropped
    // exactly once — none stranded in a ring whose consumer already stopped
    // draining. A drop-counter payload makes a leak observable: if any value's Drop
    // is skipped, `live` ends non-zero. This is the exhaustive proof that
    // `Inner::drop` reclaims values a producer writes after the drain loop exits.
    #[test]
    fn loom_teardown_drops_every_value() {
        use crate::platform::Arc;
        use loom::sync::atomic::{AtomicUsize, Ordering};

        loom::model(|| {
            let live = Arc::new(AtomicUsize::new(0));

            struct P(Arc<AtomicUsize>);
            impl P {
                fn new(l: &Arc<AtomicUsize>) -> Self {
                    l.fetch_add(1, Ordering::SeqCst);
                    P(l.clone())
                }
            }
            impl Drop for P {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }

            let (mut tx, rx) = ring::<P>(1); // cap 1 -> maximal push/drain contention
            let l2 = live.clone();
            let h = loom::thread::spawn(move || {
                // Two send attempts; each pushed value is either delivered/stranded
                // (owned by the ring) or returned on Full and dropped here.
                let _ = tx.try_push(P::new(&l2));
                let _ = tx.try_push(P::new(&l2));
                // tx drops here -> producer_gone
            });
            drop(rx); // consumer half torn down concurrently with the sends
            h.join().unwrap();
            // Both Arcs now released -> Inner::drop has run. Nothing may leak.
            assert_eq!(
                live.load(Ordering::SeqCst),
                0,
                "a value was stranded/leaked"
            );
        });
    }
}

#[cfg(all(test, not(loom)))]
mod boundary_tests {
    use super::*;

    #[test]
    fn lock_free_membership_link_fits_existing_tail_padding() {
        assert_eq!(
            core::mem::size_of::<CachePad<Tail<u64>>>(),
            core::mem::size_of::<CachePad<AtomicUsize>>()
        );
    }

    #[test]
    fn partial_drains_commit_on_panic_and_resume() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        for asynchronous in [false, true] {
            let (mut tx, mut rx) = ring::<Box<usize>>(8);
            let waiter = TaskWaiter::new();
            for value in 0..8 {
                tx.try_push(Box::new(value)).unwrap();
            }
            assert_eq!(*rx.try_pop().unwrap(), 0);
            assert_eq!(rx.drain_in_place(0, |_| panic!("zero limit")), 0);
            assert_eq!(
                rx.drain_in_place_async(0, &waiter, |_| panic!("zero limit")),
                0
            );
            let result = catch_unwind(AssertUnwindSafe(|| {
                let mut seen = 0;
                let mut consume = |value: Box<usize>| {
                    seen += 1;
                    assert_eq!(*value, seen);
                    assert!(seen < 2, "callback panic");
                };
                if asynchronous {
                    rx.drain_in_place_async(8, &waiter, &mut consume);
                } else {
                    rx.drain_in_place(8, &mut consume);
                }
            }));
            assert!(result.is_err());
            assert_eq!(rx.inner.head.0.load(Ordering::Relaxed), 3);
            for value in 8..11 {
                tx.try_push(Box::new(value)).unwrap();
            }
            assert!(tx.try_push(Box::new(11)).is_err());
            for expected in 3..11 {
                assert_eq!(*rx.try_pop().unwrap(), expected);
            }
            assert!(rx.is_empty());
        }
    }

    #[test]
    fn async_drain_commits_before_panicking_wake() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::{Wake, Waker};

        struct PanicWake;
        impl Wake for PanicWake {
            fn wake(self: std::sync::Arc<Self>) {
                panic!("wake panic");
            }
        }
        let (mut tx, mut rx) = ring::<Box<usize>>(2);
        let waiter = TaskWaiter::new();
        let waker = Waker::from(std::sync::Arc::new(PanicWake));
        waiter.arm(&waker);
        tx.try_push(Box::new(0)).unwrap();
        tx.try_push(Box::new(1)).unwrap();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                rx.drain_in_place_async(2, &waiter, drop);
            }))
            .is_err()
        );
        assert!(rx.is_empty());
        assert_eq!(rx.inner.head.0.load(Ordering::Relaxed), 2);
        tx.try_push(Box::new(2)).unwrap();
        assert_eq!(*rx.try_pop().unwrap(), 2);
        waiter.clear();
    }

    #[test]
    fn recycled_cursors_restart_after_mixed_operations() {
        for capacity in [1, 2, 8] {
            let (mut tx, mut rx) = ring::<Box<usize>>(capacity);
            for epoch in 0..8 {
                for value in 0..capacity {
                    tx.try_push(Box::new(epoch * capacity + value)).unwrap();
                }
                assert!(rx.recycle().is_none(), "live producer prevents recycling");
                drop(tx);
                assert_eq!(*rx.try_pop().unwrap(), epoch * capacity);
                rx.drain_in_place(capacity, drop);
                assert!(rx.is_finished());
                tx = rx.recycle().unwrap();
                assert_eq!(tx.inner.tail.0.cursor.load(Ordering::Relaxed), 0);
                assert_eq!(rx.inner.head.0.load(Ordering::Relaxed), 0);
                assert!(rx.is_empty());
            }
        }
    }

    #[test]
    fn inbox_restores_partially_consumed_and_empty_positions() {
        for unread in [0, 2] {
            let stack = InboxStack::new();
            let (mut tx, mut rx) = ring::<Box<usize>>(4);
            for value in 0..4 {
                tx.try_push(Box::new(value)).unwrap();
            }
            for expected in 0..4 - unread {
                assert_eq!(*rx.try_pop().unwrap(), expected);
            }
            // Transfer twice so reconstruction cannot assume a pristine receiver.
            for _ in 0..2 {
                stack.push(rx);
                let mut received = None;
                stack.drain_with(|rx| received = Some(rx));
                rx = received.unwrap();
                assert_eq!(rx.is_empty(), unread == 0);
            }
            for expected in 4 - unread..4 {
                assert_eq!(*rx.try_pop().unwrap(), expected);
            }
            assert!(rx.try_pop().is_none());
            tx.try_push(Box::new(4)).unwrap();
            assert_eq!(*rx.try_pop().unwrap(), 4);
            stack.push(rx); // Drop also reconstructs an empty, nonzero cursor.
            drop(stack);
            assert!(tx.is_consumer_gone());
        }
    }

    #[test]
    fn inbox_drop_reclaims_only_remaining_values() {
        let stack = InboxStack::new();
        let (mut tx, mut rx) = ring::<Box<usize>>(4);
        for value in 0..4 {
            tx.try_push(Box::new(value)).unwrap();
        }
        assert_eq!(*rx.try_pop().unwrap(), 0);
        stack.push(rx);
        drop(stack); // Miri checks moved-out Box is not read/dropped again.
        assert!(tx.is_consumer_gone());
    }

    #[test]
    fn masked_slots_and_all_operations_across_counter_wrap() {
        for capacity in [0usize, 1, 2, 3, 7, 8, 31, 1023, 1024] {
            let inner = build::<usize>(capacity);
            let cap = capacity.max(1).next_power_of_two();
            assert_eq!(inner.buf.len(), cap);
            for cursor in [0, 1, cap - 1, cap, usize::MAX - 1, usize::MAX] {
                assert!(core::ptr::eq(
                    inner.slot(cursor),
                    &inner.buf[cursor & (cap - 1)]
                ));
            }
            let base = usize::MAX - (cap - 1);
            inner.head.0.store(base, Ordering::Relaxed);
            inner.tail.0.cursor.store(base, Ordering::Relaxed);
            let (mut tx, mut rx) = halves(inner);
            tx.head_cache = base;
            rx.tail_cache = base;
            let waiter = TaskWaiter::new();
            for round in 0..4 {
                for index in 0..cap {
                    let value = round * cap + index;
                    assert_eq!(tx.try_push(value), Ok(()));
                }
                assert!(tx.try_push(usize::MAX).is_err());
                let mut values = Vec::new();
                match round {
                    0 | 1 => {
                        for _ in 0..cap {
                            values.push(rx.try_pop().unwrap());
                        }
                    }

                    2 => {
                        assert_eq!(rx.drain_in_place(cap, |v| values.push(v)), cap);
                    }
                    _ => {
                        assert_eq!(
                            rx.drain_in_place_async(cap, &waiter, |v| values.push(v)),
                            cap
                        );
                    }
                }
                assert_eq!(values, (round * cap..(round + 1) * cap).collect::<Vec<_>>());
                assert!(rx.try_pop().is_none());
            }
        }
    }

    #[test]
    fn masked_zero_sized_slots() {
        let (mut tx, mut rx) = ring::<()>(3);
        for _ in 0..8 {
            for _ in 0..4 {
                assert!(tx.try_push(()).is_ok());
            }
            assert!(tx.try_push(()).is_err());
            assert_eq!(rx.drain_in_place(4, |()| {}), 4);
        }
    }

    #[test]
    #[should_panic(expected = "ring capacity exceeds supported power of two")]
    fn capacity_rounding_overflow_is_rejected_in_release_too() {
        let _ = ring::<()>((usize::MAX >> 1) + 2);
    }
}
// ---- Backend trait wiring ---------------------------------------------------

/// The default backend: this bounded Lamport ring.
pub struct Ring;

impl crate::backend::Backend for Ring {
    const BOUNDED_CAPACITY: bool = true;
    type Tx<T: Send> = Sender<T>;
    type Rx<T: Send> = Receiver<T>;

    fn channel<T: Send>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        ring(capacity)
    }
}

impl<T: Send> crate::backend::Tx<T> for Sender<T> {
    #[inline]
    fn try_push(&mut self, v: T) -> Result<(), T> {
        Sender::try_push(self, v)
    }
    #[inline(always)]
    fn push_from(&mut self, max: usize, source: &mut Batch<T>) -> usize {
        Sender::push_from(self, max, source)
    }
    #[inline]
    fn is_consumer_gone(&self) -> bool {
        Sender::is_consumer_gone(self)
    }
}

impl<T: Send> crate::backend::Rx<T> for Receiver<T> {
    #[inline]
    fn try_pop(&mut self) -> Option<T> {
        Receiver::try_pop(self)
    }
    #[inline]
    fn drain_in_place(&mut self, max: usize, f: impl FnMut(T)) -> usize {
        Receiver::drain_in_place(self, max, f)
    }
    #[inline(always)]
    fn drain_into(&mut self, max: usize, out: &mut Batch<T>) -> usize {
        Receiver::drain_into(self, max, out)
    }
    #[inline]
    fn is_empty(&self) -> bool {
        Receiver::is_empty(self)
    }
    #[inline]
    fn is_finished(&self) -> bool {
        Receiver::is_finished(self)
    }
}
