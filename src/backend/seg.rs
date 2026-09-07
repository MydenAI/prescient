//! `Seg` — an **unbounded, segmented** SPSC backend (arena-block storage).
//!
//! Values live in fixed-size segments (mini-arenas). The producer fills a
//! segment linearly and links a freshly allocated one when it runs out; the
//! consumer drains a segment linearly and frees it when it moves past. So:
//!
//! * **Unbounded** — `try_push` never reports full (only "consumer gone"), so a
//!   blocking send never waits. The queue grows by one segment allocation per
//!   `capacity` values and shrinks as the consumer retires segments.
//! * **Linear memory** — within a segment, both sides walk forward through
//!   contiguous slots (no masking, no wraparound), which is friendly to
//!   prefetchers; the cost is one allocation + pointer hop per segment.
//!
//! Trade-off vs [`Ring`](super::Ring): Ring is allocation-free in steady state
//! and provides *backpressure* (bounded). Seg trades those for never-full sends
//! and unbounded buffering — pick it when producers must never block and memory
//! is allowed to grow with the backlog.
//!
//! Teardown mirrors the ring's proven design: values stranded by a producer
//! racing a consumer's departure are destroyed exactly once when the last
//! endpoint drops (`Shared::drop` walks the chain at refcount 0, dropping the
//! unconsumed span `[consumed, published)` of every live segment).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use super::Backend;

struct Segment<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Values the producer has made visible in THIS segment (Release on store).
    published: AtomicUsize,
    /// Values the consumer has moved out of THIS segment. Only written when the
    /// consumer leaves (drop) — retired segments are freed immediately instead.
    /// Read exclusively by the teardown walk at refcount 0.
    consumed: AtomicUsize,
    next: AtomicPtr<Segment<T>>,
}

impl<T> Segment<T> {
    fn boxed(size: usize) -> *mut Segment<T> {
        let mut buf = Vec::with_capacity(size);
        for _ in 0..size {
            buf.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Box::into_raw(Box::new(Segment {
            buf: buf.into_boxed_slice(),
            published: AtomicUsize::new(0),
            consumed: AtomicUsize::new(0),
            next: AtomicPtr::new(core::ptr::null_mut()),
        }))
    }
}

struct Shared<T> {
    /// Oldest live segment; advanced by the consumer as it retires segments.
    /// The teardown walk starts here.
    head: AtomicPtr<Segment<T>>,
    producer_gone: AtomicBool,
    consumer_gone: AtomicBool,
}

// SAFETY: the raw segment pointers are owned by the endpoint protocol (producer
// touches only its tail segment, consumer only from head to published); `Shared`
// itself holds only atomics.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Refcount 0: both endpoints gone, fully exclusive. Walk the chain and
        // destroy every value that was published but never consumed — including
        // one stranded by a send racing the consumer's departure.
        let mut p = *self.head.get_mut();
        while !p.is_null() {
            // SAFETY: exclusive access; each live segment is freed exactly once.
            let next;
            {
                let seg = unsafe { &mut *p };
                let start = *seg.consumed.get_mut();
                let end = *seg.published.get_mut();
                for i in start..end {
                    // SAFETY: published-but-unconsumed slots hold initialized values;
                    // `start` excludes everything already moved out.
                    unsafe { seg.buf[i].get_mut().assume_init_drop() };
                }
                next = *seg.next.get_mut();
            }
            drop(unsafe { Box::from_raw(p) });
            p = next;
        }
    }
}

/// Producing endpoint of a [`Seg`] channel.
pub struct SegTx<T> {
    seg: *mut Segment<T>,
    idx: usize,
    seg_size: usize,
    shared: Arc<Shared<T>>,
}
// SAFETY: exclusive owner of the producer role; T: Send moves with it.
unsafe impl<T: Send> Send for SegTx<T> {}

/// Consuming endpoint of a [`Seg`] channel.
pub struct SegRx<T> {
    seg: *mut Segment<T>,
    idx: usize,
    seg_size: usize,
    shared: Arc<Shared<T>>,
}
// SAFETY: exclusive owner of the consumer role.
unsafe impl<T: Send> Send for SegRx<T> {}

/// The segmented backend selector. `capacity` = slots per segment.
pub struct Seg;

impl Backend for Seg {
    type Tx<T: Send> = SegTx<T>;
    type Rx<T: Send> = SegRx<T>;

    fn channel<T: Send>(capacity: usize) -> (SegTx<T>, SegRx<T>) {
        let size = capacity.max(2);
        let first = Segment::<T>::boxed(size);
        let shared = Arc::new(Shared {
            head: AtomicPtr::new(first),
            producer_gone: AtomicBool::new(false),
            consumer_gone: AtomicBool::new(false),
        });
        (
            SegTx {
                seg: first,
                idx: 0,
                seg_size: size,
                shared: Arc::clone(&shared),
            },
            SegRx {
                seg: first,
                idx: 0,
                seg_size: size,
                shared,
            },
        )
    }
}

impl<T: Send> super::Tx<T> for SegTx<T> {
    fn try_push(&mut self, v: T) -> Result<(), T> {
        if self.shared.consumer_gone.load(Ordering::Acquire) {
            return Err(v);
        }
        if self.idx == self.seg_size {
            // Tail segment full: link a fresh one, then move onto it. The link is
            // published with Release so a consumer that sees `next` also sees the
            // fully initialized segment.
            let new = Segment::<T>::boxed(self.seg_size);
            // SAFETY: producer exclusively owns the tail segment's `next`.
            unsafe { (*self.seg).next.store(new, Ordering::Release) };
            self.seg = new;
            self.idx = 0;
        }
        // SAFETY: slot `idx` is unpublished, so no other thread can read it yet.
        unsafe {
            (*(*self.seg).buf[self.idx].get()).write(v);
        }
        self.idx += 1;
        // Publish: everything above happens-before a consumer Acquire of `published`.
        // SAFETY: producer owns the tail segment.
        unsafe { (*self.seg).published.store(self.idx, Ordering::Release) };
        Ok(())
    }

    fn is_consumer_gone(&self) -> bool {
        self.shared.consumer_gone.load(Ordering::Acquire)
    }
}

impl<T> Drop for SegTx<T> {
    fn drop(&mut self) {
        self.shared.producer_gone.store(true, Ordering::Release);
    }
}

impl<T: Send> SegRx<T> {
    /// Move to the next segment if the current one is fully consumed and a next
    /// exists; retires (frees) the old segment. Returns false if no next yet.
    fn advance(&mut self) -> bool {
        // SAFETY: consumer owns head-side traversal; `next` read with Acquire pairs
        // with the producer's Release store, so the new segment is fully visible.
        let next = unsafe { (*self.seg).next.load(Ordering::Acquire) };
        if next.is_null() {
            return false;
        }
        // All values in the old segment are consumed (idx == seg_size == published),
        // so it holds nothing to drop. Publish the new head for the teardown walk,
        // then free the old segment: the producer moved past it when it linked
        // `next` and never touches it again.
        self.shared.head.store(next, Ordering::Release);
        // SAFETY: exclusive retire; producer no longer references the old segment.
        drop(unsafe { Box::from_raw(self.seg) });
        self.seg = next;
        self.idx = 0;
        true
    }
}

impl<T: Send> super::Rx<T> for SegRx<T> {
    fn try_pop(&mut self) -> Option<T> {
        loop {
            if self.idx == self.seg_size {
                if !self.advance() {
                    return None;
                }
                continue;
            }
            // SAFETY: consumer reads `published` with Acquire; slots below it are
            // fully written and owned by us once read.
            let published = unsafe { (*self.seg).published.load(Ordering::Acquire) };
            if self.idx < published {
                let v = unsafe { (*(*self.seg).buf[self.idx].get()).assume_init_read() };
                self.idx += 1;
                return Some(v);
            }
            return None;
        }
    }

    fn drain_in_place(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        let mut done = 0usize;
        while done < max {
            if self.idx == self.seg_size && !self.advance() {
                break;
            }
            // SAFETY: as in try_pop.
            let published = unsafe { (*self.seg).published.load(Ordering::Acquire) };
            if self.idx >= published {
                break;
            }
            let n = (published - self.idx).min(max - done);
            for _ in 0..n {
                let v = unsafe { (*(*self.seg).buf[self.idx].get()).assume_init_read() };
                // Bump BEFORE f: if f panics, Rx::drop records `consumed = idx`, so
                // the teardown walk never re-reads a moved-out slot.
                self.idx += 1;
                done += 1;
                f(v);
            }
        }
        done
    }

    fn is_empty(&self) -> bool {
        // SAFETY: reads of producer-published counters with Acquire.
        unsafe {
            let published = (*self.seg).published.load(Ordering::Acquire);
            if self.idx < published {
                return false;
            }
            if self.idx == self.seg_size {
                let next = (*self.seg).next.load(Ordering::Acquire);
                if !next.is_null() {
                    return (*next).published.load(Ordering::Acquire) == 0;
                }
            }
            true
        }
    }

    fn is_finished(&self) -> bool {
        self.shared.producer_gone.load(Ordering::Acquire) && self.is_empty()
    }
}

impl<T> Drop for SegRx<T> {
    fn drop(&mut self) {
        // Record how far we got in the current segment so the teardown walk drops
        // exactly the unconsumed span; then signal the producer.
        // SAFETY: consumer owns its position in the head segment.
        unsafe { (*self.seg).consumed.store(self.idx, Ordering::Release) };
        self.shared.consumer_gone.store(true, Ordering::Release);
    }
}
