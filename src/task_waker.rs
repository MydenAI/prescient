//! Runtime-neutral task notification used by the async MPSC handles.
//!
//! Production uses the small `atomic-waker` crate (the implementation extracted
//! from futures-rs). Loom builds substitute Loom's modeled equivalent so the
//! channel's register -> recheck protocol is explored with the ring atomics.

use core::task::Waker;

use crate::platform::{AtomicBool, Ordering, fence};

#[cfg(not(loom))]
pub(crate) struct TaskWaker(atomic_waker::AtomicWaker);

#[cfg(loom)]
pub(crate) struct TaskWaker(loom::future::AtomicWaker);

impl TaskWaker {
    #[inline]
    pub(crate) fn new() -> Self {
        Self(
            #[cfg(not(loom))]
            atomic_waker::AtomicWaker::new(),
            #[cfg(loom)]
            loom::future::AtomicWaker::new(),
        )
    }

    /// Register before rechecking channel state. A racing wake either consumes
    /// this waker or is observed by that recheck.
    #[inline]
    pub(crate) fn register(&self, waker: &Waker) {
        #[cfg(not(loom))]
        self.0.register(waker);
        #[cfg(loom)]
        self.0.register_by_ref(waker);
    }

    #[inline]
    pub(crate) fn wake(&self) {
        self.0.wake();
    }

    /// Remove a canceled/completed future's registration. Each Prescient shard
    /// has exactly one producer and the gatherer has one receiver, so there is
    /// never a legitimate concurrent registrar for either slot.
    #[inline]
    pub(crate) fn clear(&self) {
        #[cfg(not(loom))]
        drop(self.0.take());
        #[cfg(loom)]
        drop(self.0.take_waker());
    }
}

/// One task slot with wake-arming and registrar-owned cleanup bookkeeping.
/// Stored out-of-line only for channels built
/// by an async declaration, preserving synchronous ring/shared layouts.
#[doc(hidden)]
pub struct TaskWaiter {
    waker: TaskWaker,
    waiting: AtomicBool,
    // Only the single registering endpoint reads/writes this bookkeeping bit;
    // notifying threads never touch it. Atomic storage keeps TaskWaiter Sync,
    // but endpoint ownership supplies serialization, so Relaxed is sufficient.
    // Unlike `waiting`, this remains true after notify consumes the armed bit.
    needs_clear: AtomicBool,
}

impl TaskWaiter {
    pub(crate) fn new() -> Self {
        Self {
            waker: TaskWaker::new(),
            waiting: AtomicBool::new(false),
            needs_clear: AtomicBool::new(false),
        }
    }

    /// Register and arm before the channel-state recheck. The matching SC
    /// fences here and in notify forbid both sides from observing stale state:
    /// either the notifier sees the arm, or the recheck sees its publication.
    #[inline]
    pub(crate) fn arm(&self, waker: &Waker) {
        // Set before register: even a panicking user Waker clone needs cleanup.
        self.needs_clear.store(true, Ordering::Relaxed);
        self.waker.register(waker);
        self.waiting.store(true, Ordering::Release);
        fence(Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn clear(&self) {
        // Immediately-ready and unpolled futures never armed this slot.
        if !self.needs_clear.load(Ordering::Relaxed) {
            return;
        }
        self.needs_clear.store(false, Ordering::Relaxed);
        self.waiting.store(false, Ordering::Release);
        self.waker.clear();
    }

    /// Called after publishing data, capacity, or lifecycle state. An unarmed
    /// notification only reads the shared flag; it never writes the receiver's
    /// cache line. The SC fence is necessary even when that read returns false.
    /// Register/recheck uses the same protocol for every event, with no reliance
    /// on potentially stale empty/full transition snapshots.
    #[inline]
    pub(crate) fn notify(&self) {
        fence(Ordering::SeqCst);
        if self.waiting.load(Ordering::Acquire) && self.waiting.swap(false, Ordering::AcqRel) {
            self.waker.wake();
        }
    }
}
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::task::{RawWaker, RawWakerVTable, Wake};

    #[derive(Default)]
    struct Counter(AtomicUsize);

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn unarmed_cleanup_is_a_noop_and_layout_is_unchanged() {
        let waiter = TaskWaiter::new();
        for _ in 0..4 {
            waiter.clear();
            assert!(!waiter.needs_clear.load(Ordering::Relaxed));
            assert!(!waiter.waiting.load(Ordering::SeqCst));
        }
        // The extra bool should occupy the old armed slot's trailing padding.
        let old_size = (size_of::<TaskWaker>() + size_of::<AtomicBool>())
            .next_multiple_of(align_of::<TaskWaker>());
        assert_eq!(size_of::<TaskWaiter>(), old_size);
    }

    #[test]
    fn cleanup_is_owned_until_cleared_even_after_notification() {
        let waiter = TaskWaiter::new();
        let counter = Arc::new(Counter::default());
        let waker = Waker::from(counter.clone());
        waiter.arm(&waker);
        assert_eq!(Arc::strong_count(&counter), 3);
        waiter.notify();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(!waiter.waiting.load(Ordering::SeqCst));
        assert!(waiter.needs_clear.load(Ordering::Relaxed));
        waiter.clear();
        waiter.clear();
        assert!(!waiter.needs_clear.load(Ordering::Relaxed));
        assert_eq!(Arc::strong_count(&counter), 2);
        waiter.arm(&waker);
        waiter.clear();
        waiter.notify();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&counter), 2);
    }

    #[test]
    fn canceled_registration_is_replaced_and_cleaned_once() {
        let waiter = TaskWaiter::new();
        let old = Arc::new(Counter::default());
        let new = Arc::new(Counter::default());
        waiter.arm(&Waker::from(old.clone()));
        waiter.clear();
        waiter.clear();
        assert_eq!(Arc::strong_count(&old), 1);
        waiter.arm(&Waker::from(new.clone()));
        waiter.notify();
        waiter.clear();
        assert_eq!(old.0.load(Ordering::Relaxed), 0);
        assert_eq!(new.0.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&new), 1);
    }

    #[test]
    fn panicking_registration_still_requires_cleanup() {
        fn clone_panics(_: *const ()) -> RawWaker {
            panic!("waker clone probe");
        }
        fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_panics, no_op, no_op, no_op);
        // SAFETY: every callback ignores the null data pointer, owns no resource,
        // and is thread-safe. Cloning deliberately unwinds without creating one.
        let waker = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VTABLE)) };
        let waiter = TaskWaiter::new();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                waiter.arm(&waker);
            }))
            .is_err()
        );
        assert!(waiter.needs_clear.load(Ordering::Relaxed));
        waiter.clear();
        assert!(!waiter.needs_clear.load(Ordering::Relaxed));
        assert!(!waiter.waiting.load(Ordering::SeqCst));
        // AtomicWaker does not recover its registration lock if a custom Waker
        // clone panics. This checks our disarming bookkeeping, not reuse after
        // such a panic; ordinary cancellation/reuse is tested separately.
    }
}
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::platform::Arc;
    use std::task::Poll;

    #[test]
    fn loom_two_publishers_one_waiter() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.check(|| {
            let waiter = Arc::new(TaskWaiter::new());
            let ready = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
            let tasks: Vec<_> = (0..2)
                .map(|index| {
                    let waiter = waiter.clone();
                    let ready = ready.clone();
                    loom::thread::spawn(move || {
                        ready[index].store(true, Ordering::Release);
                        waiter.notify();
                    })
                })
                .collect();
            loom::future::block_on(std::future::poll_fn(|cx| {
                waiter.arm(cx.waker());
                if ready.iter().all(|flag| flag.load(Ordering::Acquire)) {
                    waiter.clear();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }));
            // Joining/dropping the publishers must not supply a rescue wake.
            for task in tasks {
                task.join().unwrap();
            }
        });
    }
}
