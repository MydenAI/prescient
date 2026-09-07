//! Consumer eventcount built on `std::thread` park/unpark.
//!
//! It provides the same one-waiter, many-notifier contract as [`crate::park`].
//! A notifier that finds the consumer awake performs one atomic load.
//!
//! Because the single waiter can move between threads across calls, it publishes
//! its `Thread` handle under a standard `Mutex` touched only while parking, so a
//! notifier can unpark it. The 50 µs timeout is a final recovery mechanism for
//! the store-buffer wake race. Select this implementation with
//! `.wait::<crate::wait::StdThread>()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};
use std::time::Duration;

const PARK_TIMEOUT: Duration = Duration::from_micros(50);

struct Inner {
    parked: AtomicBool,
    handle: Mutex<Option<Thread>>,
}

pub struct Waiter {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct Notifier {
    inner: Arc<Inner>,
}

pub fn eventcount() -> (Waiter, Notifier) {
    let inner = Arc::new(Inner {
        parked: AtomicBool::new(false),
        handle: Mutex::new(None),
    });
    (
        Waiter {
            inner: inner.clone(),
        },
        Notifier { inner },
    )
}

impl Waiter {
    /// Arm before the final emptiness re-check. Lock-free, like `park`'s.
    #[inline]
    pub fn arm(&self) {
        self.inner.parked.store(true, Ordering::SeqCst);
    }

    /// Sleep until unparked or the timeout fires. Publishes this thread's handle
    /// under the lock and re-checks `parked` so a wake between arm and here is
    /// not missed.
    #[inline]
    pub fn park(&self) {
        if self.inner.parked.load(Ordering::SeqCst) {
            let mut h = self.inner.handle.lock().unwrap();
            if self.inner.parked.load(Ordering::SeqCst) {
                *h = Some(thread::current());
                drop(h);
                thread::park_timeout(PARK_TIMEOUT);
            }
        }
    }

    #[inline]
    pub fn disarm(&self) {
        self.inner.parked.store(false, Ordering::SeqCst);
    }
}

impl Notifier {
    /// Cheap on the hot path: one Acquire load; only pays the lock + unpark when
    /// the consumer is actually asleep.
    #[inline]
    pub fn wake(&self) {
        if self.inner.parked.load(Ordering::Acquire) {
            self.inner.parked.store(false, Ordering::SeqCst);
            // Lock serializes handle access vs. the waiter re-arming, so this is
            // never a data race; a stale unpark (waiter already moved on) is
            // harmless — it just leaves a token that a later park consumes.
            let t = self.inner.handle.lock().unwrap().clone();
            if let Some(t) = t {
                t.unpark();
            }
        }
    }
}
