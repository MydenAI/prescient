//! Synchronization primitives selected for normal and loom builds.
//!
//! Under `--cfg loom`, shared memory primitives are swapped for loom's
//! model-checked equivalents so the ring's memory ordering can be exhaustively
//! verified. Under normal builds these are zero-cost `std` re-exports.

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{Ordering, fence};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};

// AtomicPtr is only used by the type-selected lock-free membership inbox.
#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicPtr;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicPtr;

/// std-side `UnsafeCell` wrapper exposing loom's `with`/`with_mut` API so ring
/// code is identical in both build modes.
#[cfg(not(loom))]
#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    #[inline]
    pub(crate) fn new(v: T) -> Self {
        Self(std::cell::UnsafeCell::new(v))
    }
    #[inline]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }
    #[inline]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(loom)]
pub(crate) struct ColdLock(loom::sync::Mutex<()>);

#[cfg(not(loom))]
pub(crate) struct ColdLock(AtomicBool);

#[cfg(not(loom))]
pub(crate) struct ColdLockGuard<'a>(&'a AtomicBool);

#[cfg(not(loom))]
impl Drop for ColdLockGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(loom)]
impl ColdLock {
    pub(crate) fn new() -> Self {
        Self(loom::sync::Mutex::new(()))
    }

    pub(crate) fn lock(&self) -> loom::sync::MutexGuard<'_, ()> {
        self.0.lock().expect("cold lock poisoned")
    }
}

#[cfg(not(loom))]
impl ColdLock {
    #[inline]
    pub(crate) fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    #[inline]
    pub(crate) fn lock(&self) -> ColdLockGuard<'_> {
        while self
            .0
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        ColdLockGuard(&self.0)
    }
}

/// 128-byte aligned wrapper to keep producer- and consumer-owned atomics on
/// separate cache lines (defeats false sharing on the hot path). Alignment is
/// irrelevant under loom but harmless.
#[repr(align(128))]
pub(crate) struct CachePad<T>(pub(crate) T);

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
