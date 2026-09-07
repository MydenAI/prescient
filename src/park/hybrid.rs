//! Adaptive spin-then-park consumer waiter (the Disruptor's PhasedBackoff shape):
//! spin briefly to catch a near-simultaneous wake at cache latency (~sub-µs), and
//! only fall back to a real blocking park (parking_lot `Condvar`) if the spin
//! window expires with no wake. This targets **tail latency without a permanently
//! burned core**: in-burst messages (gap < spin window) are caught spinning at
//! spin-wait speed; a genuine idle period parks the consumer so it stops
//! spinning. Select it explicitly with `.wait::<crate::wait::Hybrid>()`.
//!
//! Cheap wake on the common path: while the consumer is spinning, `wake` is a
//! single relaxed store — the spinner observes it via cache coherency without any
//! lock. The lock + `notify_one` is paid only when the consumer has actually
//! escalated to `Condvar::wait`, gated by the `sleeping` flag.
//!
//! Lost-wake freedom: in the park phase the consumer sets `sleeping` BEFORE its
//! final `armed` re-check, both under the lock and SeqCst-ordered. So for any
//! interleaving, either `wake` observes `sleeping` (and notifies under the lock)
//! or the consumer observes `armed` cleared (and does not sleep) — never both
//! missing. The 50us `wait_for` timeout is then pure insurance, not relied upon.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

const PARK_TIMEOUT: Duration = Duration::from_micros(50);

// Number of spin/check iterations before falling back to a blocking park. Each
// iteration is one `spin_loop` hint + one `armed` load, so this is the spin
// *window* in PAUSE units — long enough to swallow a bursty inter-message gap,
// short enough that an idle consumer parks promptly.
const SPIN_BUDGET: u32 = 512;

struct Inner {
    armed: AtomicBool,    // consumer wants a wake
    sleeping: AtomicBool, // consumer has escalated to Condvar::wait
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
        armed: AtomicBool::new(false),
        sleeping: AtomicBool::new(false),
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
        self.inner.armed.store(true, Ordering::SeqCst);
    }

    /// Spin for a bounded window; if still armed, block on the condvar.
    #[inline]
    pub fn park(&self) {
        // Phase 1: spin. Catches an in-burst wake without a syscall.
        for _ in 0..SPIN_BUDGET {
            if !self.inner.armed.load(Ordering::Acquire) {
                return;
            }
            core::hint::spin_loop();
        }
        // Phase 2: still idle after the spin window — block to stop burning CPU.
        let mut g = self.inner.m.lock();
        // Publish "sleeping" BEFORE the armed re-check so a concurrent wake either
        // sees sleeping (and notifies) or we see armed cleared (and skip the wait).
        self.inner.sleeping.store(true, Ordering::SeqCst);
        if self.inner.armed.load(Ordering::SeqCst) {
            self.inner.cv.wait_for(&mut g, PARK_TIMEOUT);
        }
        self.inner.sleeping.store(false, Ordering::SeqCst);
    }

    #[inline]
    pub fn disarm(&self) {
        self.inner.armed.store(false, Ordering::SeqCst);
    }
}

impl Notifier {
    /// Release the consumer. While it spins this is a single store; only once it
    /// has parked (`sleeping`) do we pay the lock + notify.
    #[inline]
    pub fn wake(&self) {
        if self.inner.armed.load(Ordering::Acquire) {
            self.inner.armed.store(false, Ordering::SeqCst);
            if self.inner.sleeping.load(Ordering::Acquire) {
                // Serializes with the waiter's lock..wait, so the notify is never
                // delivered into the gap before it actually waits.
                let _g = self.inner.m.lock();
                self.inner.cv.notify_one();
            }
        }
    }
}
