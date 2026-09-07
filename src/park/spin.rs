//! No-std-friendly consumer waiter: the consumer *spins* instead of parking. No
//! `Mutex`, no `Condvar`, no `thread::park` — only an `AtomicBool` and a bounded
//! `core::hint::spin_loop` backoff, so it works in environments with no OS
//! blocking primitive (bare-metal, a dedicated consumer core, cooperative
//! schedulers). Same one-waiter / many-notifier contract as the parking kernels;
//! select it explicitly with `.wait::<crate::wait::Spin>()`.
//!
//! The tradeoff is explicit: an idle consumer burns a core. In exchange you get
//! the lowest possible wake latency (no syscall, no scheduler round-trip) and zero
//! dependence on std's parking machinery — the last std-only piece on the wait
//! path when `membership::<LockFree>()` removes the membership `Mutex`.
//!
//! LIVENESS: [`park`](Waiter::park) returns after a bounded spin *unconditionally*,
//! not only when woken. That unconditional return is the backstop against a missed
//! wakeup (a producer's store to the ring not yet visible when the consumer armed):
//! the consumer simply re-polls `try_recv` on its next loop, exactly as the timed
//! `park` variants rely on their 50us timeout. So correctness never depends on
//! `wake` — `wake` only shortens the idle spin.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// Exponential spin backoff, capped per step; `park` runs at most this many steps
// before returning to re-poll. Sized so an idle park spins on the order of a
// microsecond — long enough to catch a nearly-simultaneous wake without a syscall,
// short enough to re-check disconnect/readiness promptly.
const MAX_STEPS: u32 = 7;

#[inline]
fn backoff(step: u32) {
    for _ in 0..(1u32 << step.min(6)) {
        core::hint::spin_loop();
    }
}

struct Inner {
    armed: AtomicBool,
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
    });
    (
        Waiter {
            inner: inner.clone(),
        },
        Notifier { inner },
    )
}

impl Waiter {
    /// Arm before the final emptiness re-check, so a concurrent [`Notifier::wake`]
    /// can cut a subsequent [`park`](Self::park) short.
    #[inline]
    pub fn arm(&self) {
        self.inner.armed.store(true, Ordering::SeqCst);
    }

    /// Spin with bounded backoff, then return so the recv loop re-polls. Returns
    /// early if a notifier (or `disarm`) cleared `armed`. NEVER blocks
    /// indefinitely: the bounded return is the missed-wakeup backstop (see module
    /// docs), so a spinning consumer always makes progress.
    #[inline]
    pub fn park(&self) {
        let mut step = 0;
        while self.inner.armed.load(Ordering::Acquire) {
            backoff(step);
            step += 1;
            if step >= MAX_STEPS {
                break;
            }
        }
    }

    #[inline]
    pub fn disarm(&self) {
        self.inner.armed.store(false, Ordering::SeqCst);
    }
}

impl Notifier {
    /// Release a spinning consumer from its backoff. There is no sleeping thread to
    /// signal, so this is a single store on the hot path — cheaper than the
    /// load-then-maybe-lock of the parking variants. A stale clear is harmless: the
    /// consumer just finishes its spin one iteration early.
    #[inline]
    pub fn wake(&self) {
        self.inner.armed.store(false, Ordering::Release);
    }
}
