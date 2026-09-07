//! Broadcast: one publisher, every reader sees **every** value.
//!
//! Unlike [`brokerless`](super::brokerless) and [`brokered`](super::brokered),
//! which deliver each value to one consumer, broadcast delivers each value to
//! every subscriber:
//!
//! * **One shared ring**, written by a single publisher (no CAS anywhere).
//! * **Per-reader cursors** (cache-padded): a reader consumes by *cloning* the
//!   slot and bumping its own cursor — readers never contend with each other.
//! * **Gating on the slowest reader**: the publisher can reuse a slot only when
//!   every active reader has passed it, so nothing is ever lost or torn —
//!   backpressure instead of tokio-broadcast's `Lagged` error.
//! * **Subscribe at runtime** from either endpoint ([`Publisher::subscribe`] /
//!   [`Reader::subscribe`]): a new reader claims a cursor slot from a lock-free
//!   bitmap (≤ 64 readers, like the `pool` tier) and hears everything published
//!   *after* it joined. Dropping a reader releases its slot — and its gate.
//!
//! `T: Clone` is required (each reader gets a clone; the ring keeps the
//! original until overwrite). One value cloned to R readers costs R clones but
//! no broker hop and no per-reader queue.
//!
//! The publisher is not `Clone`; preserving one writer removes publication CAS.

use std::mem::MaybeUninit;

use crate::platform::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, CachePad, ColdLock, Ordering, UnsafeCell,
};

/// Declare broadcast with spin/yield waiting defaults.
pub fn channel<T>() -> crate::Channel<
    T,
    crate::topology::Broadcast,
    crate::backend::Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new().broadcast()
}

struct Shared<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    cap: usize,
    /// Next sequence to write; slots `[.., w)` are published. Single writer.
    w: CachePad<AtomicUsize>,
    /// Serializes reader activation with the publisher's cold-path gating scan.
    /// The ordinary publish/read paths never acquire this lock.
    membership_scan: ColdLock,
    /// Gating bitmap: bit i set ⇔ `cursors[i]` currently gates the publisher.
    /// A suspended reader clears its bit here but keeps its `owned` slot.
    readers: AtomicU64,
    /// Ownership bitmap: bit i set ⇔ cursor slot i is reserved by a live Reader
    /// (gating or suspended). `subscribe` allocates from THIS set.
    owned: AtomicU64,
    /// Per-reader next-sequence-to-read.
    cursors: Box<[CachePad<AtomicUsize>]>,
    producer_gone: AtomicBool,
}

// SAFETY: slots are written only by the single publisher, and only when every
// active reader's cursor has passed them (gating); readers only clone published
// slots. Cursors/bitmap are atomics.
unsafe impl<T: Send + Sync> Sync for Shared<T> {}
unsafe impl<T: Send> Send for Shared<T> {}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Exclusive at refcount 0. The last min(w, cap) sequences hold live values.
        let w = self.w.0.load(Ordering::Relaxed);
        let live = w.min(self.cap);
        for s in (w - live)..w {
            // SAFETY: exclusive; each live slot dropped exactly once.
            self.buf[s & self.mask].with_mut(|slot| {
                // SAFETY: refcount zero gives exclusive access to every live slot.
                unsafe { (*slot).assume_init_drop() };
            });
        }
    }
}

fn activate_reader<T>(shared: &Shared<T>, bit: usize) -> usize {
    let _scan = shared.membership_scan.lock();
    let now = shared.w.0.load(Ordering::Acquire);
    // The cursor is initialized before the active bit is released. A publisher
    // that scans after this critical section must see both.
    shared.cursors[bit].0.store(now, Ordering::Relaxed);
    shared.readers.fetch_or(1u64 << bit, Ordering::Release);
    now
}

fn slowest<T>(shared: &Shared<T>, w: usize) -> usize {
    let bits = shared.readers.load(Ordering::Acquire);
    let mut min = w;
    let mut max_lag = 0usize;
    for i in 0..shared.cursors.len() {
        if bits & (1u64 << i) != 0 {
            let cursor = shared.cursors[i].0.load(Ordering::Acquire);
            let lag = w.wrapping_sub(cursor);
            if lag > max_lag {
                max_lag = lag;
                min = cursor;
            }
        }
    }
    min
}

#[cold]
#[inline(never)]
fn refresh_slowest<T>(shared: &Shared<T>, w: usize, min_cache: &mut usize) {
    let _scan = shared.membership_scan.lock();
    *min_cache = slowest(shared, w);
}

struct PublishCommit<'a, T> {
    shared: &'a Shared<T>,
    w: &'a mut usize,
    dirty: bool,
}

impl<T> PublishCommit<'_, T> {
    #[inline]
    fn position(&self) -> usize {
        *self.w
    }

    #[inline]
    fn advance(&mut self) {
        *self.w = self.w.wrapping_add(1);
        self.dirty = true;
    }

    #[inline]
    fn publish(&mut self) {
        if self.dirty {
            self.shared.w.0.store(*self.w, Ordering::Release);
            self.dirty = false;
        }
    }
}

impl<T> Drop for PublishCommit<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.publish();
    }
}

struct CursorCommit<'a> {
    cursor: &'a AtomicUsize,
    position: &'a mut usize,
}

impl Drop for CursorCommit<'_> {
    #[inline]
    fn drop(&mut self) {
        self.cursor.store(*self.position, Ordering::Release);
    }
}

/// Single publishing endpoint. It is not cloneable.
pub struct Publisher<T> {
    shared: Arc<Shared<T>>,
    /// Local mirror of `w` (we are the only writer).
    w: usize,
    /// Cached slowest-reader position; refreshed only when the ring looks full.
    min_cache: usize,
}

/// A subscribed reader: sees every value published after it joined, in order.
pub struct Reader<T> {
    shared: Arc<Shared<T>>,
    bit: usize,
    r: usize,
    suspended: bool,
}

pub(crate) fn open<T: Clone + Send>(
    capacity: usize,
    max_readers: usize,
) -> (Publisher<T>, Reader<T>) {
    assert!(
        (1..=64).contains(&max_readers),
        "broadcast supports 1..=64 readers"
    );
    let cap = capacity.next_power_of_two();
    let mut buf = Vec::with_capacity(cap);
    for _ in 0..cap {
        buf.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    let cursors = (0..max_readers)
        .map(|_| CachePad(AtomicUsize::new(0)))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        buf: buf.into_boxed_slice(),
        mask: cap - 1,
        cap,
        w: CachePad(AtomicUsize::new(0)),
        membership_scan: ColdLock::new(),
        readers: AtomicU64::new(1), // bit 0 = the initial reader
        owned: AtomicU64::new(1),
        cursors,
        producer_gone: AtomicBool::new(false),
    });
    (
        Publisher {
            shared: Arc::clone(&shared),
            w: 0,
            min_cache: 0,
        },
        Reader {
            shared,
            bit: 0,
            r: 0,
            suspended: false,
        },
    )
}

/// Claim a free cursor slot and activate it. Shared by both subscribe paths.
fn subscribe<T>(shared: &Arc<Shared<T>>) -> Option<Reader<T>> {
    loop {
        let bits = shared.owned.load(Ordering::Acquire);
        let n = shared.cursors.len();
        let free = (0..n).find(|i| bits & (1u64 << i) == 0)?;
        if shared
            .owned
            .compare_exchange(
                bits,
                bits | (1u64 << free),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // Only the successful owner may touch this cursor. Activation is
            // serialized with the publisher's cold scan so a newer cached
            // minimum can never leap ahead of the joining reader.
            let now = activate_reader(shared, free);
            return Some(Reader {
                shared: Arc::clone(shared),
                bit: free,
                r: now,
                suspended: false,
            });
        }
    }
}

impl<T: Clone + Send> Publisher<T> {
    /// Subscribe a new reader; it sees everything published after this call.
    /// `None` if `max_readers` are already subscribed.
    pub fn subscribe(&self) -> Option<Reader<T>> {
        subscribe(&self.shared)
    }

    /// Non-blocking publish. `Err(v)` if the ring is full — i.e. the slowest
    /// reader is `capacity` behind (backpressure, not loss).
    pub fn try_send(&mut self, v: T) -> Result<(), T> {
        if self.w.wrapping_sub(self.min_cache) >= self.shared.cap {
            refresh_slowest(&self.shared, self.w, &mut self.min_cache);
            if self.w.wrapping_sub(self.min_cache) >= self.shared.cap {
                return Err(v);
            }
        }
        let position = self.w;
        let slot = &self.shared.buf[position & self.shared.mask];
        // Move the old value out, install and publish the replacement, then
        // destroy the old value. If its destructor panics, the ring already
        // contains one valid, published replacement and teardown remains exact.
        let replaced = slot.with_mut(|slot| unsafe {
            let replaced = if position >= self.shared.cap {
                Some((*slot).assume_init_read())
            } else {
                None
            };
            (*slot).write(v);
            replaced
        });
        self.w = self.w.wrapping_add(1);
        self.shared.w.0.store(self.w, Ordering::Release);
        drop(replaced);
        Ok(())
    }

    /// Blocking publish: spins/yields while the slowest reader gates the ring.
    /// A dropped reader releases its gate, so this cannot deadlock on a
    /// disappeared reader; with zero readers it never blocks.
    pub fn send(&mut self, mut v: T) {
        let mut s = 0u32;
        loop {
            match self.try_send(v) {
                Ok(()) => return,
                Err(back) => {
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

    /// Batched publish: writes every value, refreshing the gate as needed, but
    /// makes the batch visible with a SINGLE `Release` store at the end — the
    /// Disruptor's batch-claim trick. Blocks (spin→yield) while gated.
    pub fn send_batch<I: IntoIterator<Item = T>>(&mut self, values: I) {
        let shared = &self.shared;
        let min_cache = &mut self.min_cache;
        let mut publication = PublishCommit {
            shared,
            w: &mut self.w,
            dirty: false,
        };
        for v in values {
            let mut spins = 0u32;
            while publication.position().wrapping_sub(*min_cache) >= shared.cap {
                // A reader cannot release unpublished values. Commit the prefix
                // before waiting; the guard does the same on every unwind path.
                publication.publish();
                refresh_slowest(shared, publication.position(), min_cache);
                if publication.position().wrapping_sub(*min_cache) < shared.cap {
                    break;
                }
                spins = spins.wrapping_add(1);
                if spins < 64 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }

            let position = publication.position();
            let slot = &shared.buf[position & shared.mask];
            let replaced = slot.with_mut(|slot| unsafe {
                let replaced = if position >= shared.cap {
                    Some((*slot).assume_init_read())
                } else {
                    None
                };
                (*slot).write(v);
                replaced
            });
            publication.advance();
            drop(replaced);
        }
        // Normal return and iterator/destructor unwind share this exact commit.
        publication.publish();
    }
}

impl<T> Drop for Publisher<T> {
    fn drop(&mut self) {
        self.shared.producer_gone.store(true, Ordering::Release);
    }
}

impl<T: Clone + Send> Reader<T> {
    /// Subscribe another reader (readers can mint readers — market_square's
    /// `create_reader`). It sees everything published after this call.
    pub fn subscribe(&self) -> Option<Reader<T>> {
        subscribe(&self.shared)
    }

    /// Non-blocking receive: clone the next value this reader hasn't seen.
    /// A [`suspend`](Self::suspend)ed reader receives nothing until
    /// [`resume`](Self::resume) — its slots are no longer protected from the
    /// publisher, so reading them would be unsound.
    pub fn try_recv(&mut self) -> Option<T> {
        if self.suspended {
            return None;
        }
        let w = self.shared.w.0.load(Ordering::Acquire);
        if self.r == w {
            return None;
        }
        // Defensive fail-closed guard. A correctly activated reader cannot be
        // lapped, but skipping is safer than reading an unprotected stale slot
        // if an invariant is ever violated.
        if w.wrapping_sub(self.r) > self.shared.cap {
            self.r = w;
            self.shared.cursors[self.bit]
                .0
                .store(self.r, Ordering::Release);
            return None;
        }
        // SAFETY: sequence r is published (r < w) and cannot be overwritten while
        // our cursor still gates it; clone, then release the slot by advancing.
        let v = self.shared.buf[self.r & self.shared.mask].with(|slot| {
            // SAFETY: sequence r is published and our active cursor gates reuse.
            unsafe { (*slot).assume_init_ref().clone() }
        });
        self.r = self.r.wrapping_add(1);
        self.shared.cursors[self.bit]
            .0
            .store(self.r, Ordering::Release);
        Some(v)
    }

    /// Batched receive: run `f` on up to `max` ready values, bumping this
    /// reader's cursor ONCE at the end (vs once per value in `try_recv`) — the
    /// throughput path. Returns the number delivered. Non-blocking.
    pub fn drain<F: FnMut(T)>(&mut self, max: usize, mut f: F) -> usize {
        if self.suspended {
            return 0;
        }
        let shared = &self.shared;
        let w = shared.w.0.load(Ordering::Acquire);
        if self.r == w {
            return 0;
        }
        if w.wrapping_sub(self.r) > shared.cap {
            self.r = w;
            shared.cursors[self.bit].0.store(self.r, Ordering::Release);
            return 0;
        }

        let n = w.wrapping_sub(self.r).min(max);
        let commit = CursorCommit {
            cursor: &shared.cursors[self.bit].0,
            position: &mut self.r,
        };
        for _ in 0..n {
            let position = *commit.position;
            let v = shared.buf[position & shared.mask].with(|slot| {
                // SAFETY: [position, w) is published and this cursor gates reuse.
                unsafe { (*slot).assume_init_ref().clone() }
            });
            // Advance before invoking user code. CursorCommit publishes this
            // exact prefix both on normal return and while unwinding.
            *commit.position = position.wrapping_add(1);
            f(v);
        }
        drop(commit);
        n
    }

    /// Blocking batched consumer: run `f` on every value until the publisher is
    /// gone and everything is seen. The broadcast analogue of the other tiers'
    /// zero-copy `for_each` (here each value is a clone — broadcast semantics).
    pub fn for_each<F: FnMut(T)>(&mut self, batch: usize, mut f: F) {
        let batch = batch.max(1);
        let mut s = 0u32;
        loop {
            if self.drain(batch, &mut f) > 0 {
                s = 0;
                continue;
            }
            if self.is_disconnected() {
                return;
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

    /// Blocking receive. `None` once the publisher is gone and this reader has
    /// seen everything it published.
    pub fn recv(&mut self) -> Option<T> {
        let mut s = 0u32;
        loop {
            if let Some(v) = self.try_recv() {
                return Some(v);
            }
            if self.suspended {
                return None; // resume() first; a suspended reader hears nothing
            }
            if self.shared.producer_gone.load(Ordering::Acquire)
                && self.r == self.shared.w.0.load(Ordering::Acquire)
            {
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

    /// Stop gating the publisher while keeping this reader's slot. A suspended
    /// reader costs the ring nothing — the publisher may lap it freely — and it
    /// can still mint new readers via [`subscribe`](Self::subscribe). Call
    /// [`resume`](Self::resume) to rejoin the stream *from that point on*
    /// (everything published while suspended is missed, by design). This is the
    /// escape hatch for "I hold a reader but won't read for a while": an idle
    /// un-suspended reader blocks the publisher once it is `capacity` behind.
    pub fn suspend(&mut self) {
        if !self.suspended {
            self.shared
                .readers
                .fetch_and(!(1u64 << self.bit), Ordering::AcqRel);
            self.suspended = true;
        }
    }

    /// Rejoin the stream after [`suspend`](Self::suspend); reading continues
    /// from the first value published after this call.
    pub fn resume(&mut self) {
        if self.suspended {
            self.r = activate_reader(&self.shared, self.bit);
            self.suspended = false;
        }
    }

    /// True once no further values can ever arrive (publisher gone, all seen).
    pub fn is_disconnected(&self) -> bool {
        self.shared.producer_gone.load(Ordering::Acquire)
            && self.r == self.shared.w.0.load(Ordering::Acquire)
    }
}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        // Release the gate first, then the slot reservation.
        self.shared
            .readers
            .fetch_and(!(1u64 << self.bit), Ordering::AcqRel);
        self.shared
            .owned
            .fetch_and(!(1u64 << self.bit), Ordering::AcqRel);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    #[test]
    fn loom_broadcast_activation_and_reuse() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let (mut publisher, mut dormant) = open::<usize>(1, 3);
            dormant.suspend();
            let mint = Arc::new(dormant);

            let first = Arc::clone(&mint);
            let first = loom::thread::spawn(move || first.subscribe().expect("first reader slot"));
            let second = Arc::clone(&mint);
            let second =
                loom::thread::spawn(move || second.subscribe().expect("second reader slot"));
            let producer = loom::thread::spawn(move || {
                assert_eq!(publisher.try_send(0), Ok(()));
                publisher
            });

            let publisher = producer.join().unwrap();
            let first = first.join().unwrap();
            let second = second.join().unwrap();
            for reader in [&first, &second] {
                let gated = reader.shared.cursors[reader.bit].0.load(Ordering::Acquire);
                assert_eq!(
                    gated, reader.r,
                    "reader gate was changed by another subscriber"
                );
                let published = publisher.shared.w.0.load(Ordering::Acquire);
                assert!(published.wrapping_sub(reader.r) <= publisher.shared.cap);
            }
        });
    }
}
