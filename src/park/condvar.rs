//! Consumer-side eventcount, built on parking_lot. One waiter (the consumer),
//! many notifiers (the producers). The `parked` flag lets a notifier skip the
//! lock entirely on the hot path; the Mutex+Condvar handshake closes the
//! lost-wakeup race (notifier clears+signals under the lock; waiter checks+waits
//! under the same lock), with a timeout as a final recovery mechanism.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

const PARK_TIMEOUT: Duration = Duration::from_micros(50);

struct Inner {
    parked: AtomicBool,
    m: Mutex<()>,
    cv: Condvar,
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
        m: Mutex::new(()),
        cv: Condvar::new(),
    });
    (
        Waiter {
            inner: inner.clone(),
        },
        Notifier { inner },
    )
}

impl Waiter {
    /// Arm before the final emptiness re-check.
    #[inline]
    pub fn arm(&self) {
        self.inner.parked.store(true, Ordering::SeqCst);
    }
    /// Sleep until notified or the timeout fires. Skips sleeping if a notifier
    /// already disarmed us between arm() and here.
    #[inline]
    pub fn park(&self) {
        let mut g = self.inner.m.lock();
        if self.inner.parked.load(Ordering::SeqCst) {
            self.inner.cv.wait_for(&mut g, PARK_TIMEOUT);
        }
    }
    #[inline]
    pub fn disarm(&self) {
        self.inner.parked.store(false, Ordering::SeqCst);
    }
}

impl Notifier {
    /// Cheap on the hot path: one Acquire load; only pays the lock when the
    /// consumer is actually asleep.
    #[inline]
    pub fn wake(&self) {
        if self.inner.parked.load(Ordering::Acquire) {
            let _g = self.inner.m.lock();
            self.inner.parked.store(false, Ordering::SeqCst);
            self.inner.cv.notify_one();
        }
    }
}
