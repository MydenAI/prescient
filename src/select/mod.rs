//! Waiting on **multiple** receivers at once: the [`select!`](crate::select!)
//! macro and the [`Selectable`] trait it drives.
//!
//! Works across every receiver type in the crate — MPSC [`crate::mpsc::Receiver`]s, MPMC
//! [`brokerless`](crate::mpmc::brokerless) / [`brokered`](crate::mpmc::brokered)
//! consumers, dynamic consumers, and broadcast [`crate::mpmc::broadcast::Reader`]s — and they can be
//! mixed freely in one `select!`.
//!
//! ```
//! use prescient::mpsc::fixed;
//! let (mut txs, mut events) = fixed::channel::<u64>().capacity(64).open().unwrap();
//! let (mut stxs, mut shutdown) = fixed::channel::<()>().capacity(1).open().unwrap();
//! txs[0].send(7).unwrap();
//! drop(txs);
//! drop(stxs);
//! let outcome = prescient::select! {
//!     recv(events) -> v => v,
//!     recv(shutdown) -> _s => 0,
//!     complete => u64::MAX,
//! };
//! assert_eq!(outcome, 7);
//! ```
//!
//! # Honest semantics (read this)
//!
//! * **Biased**: arms are tried in the order written, every round — a saturated
//!   first arm delays later ones (like `tokio::select!` with `biased;`). Put the
//!   highest-priority source (e.g. shutdown) first if that matters.
//! * **Poll-based, not parked**: between empty rounds the loop backs off
//!   spin → yield → short sleeps (capped at ~100 µs). Idle CPU is negligible,
//!   but worst-case wake latency is the backoff cap — this is a poller, not an
//!   eventcount integration. For single-channel blocking waits, the channel's
//!   own `recv()` (which parks properly on the MPSC tiers) is strictly better.
//! * The mandatory `complete` arm runs once **every** source is disconnected
//!   and drained — the multi-channel analogue of `recv()` returning `None`.

use std::time::Duration;

/// A receive source `select!` can drive: non-blocking poll + "nothing will ever
/// arrive again" detection. Implemented by every receiver type in the crate.
pub trait Selectable {
    /// The received value type.
    type Item;

    /// Non-blocking poll (the type's `try_recv`).
    fn try_select(&mut self) -> Option<Self::Item>;

    /// True once no *new* values can arrive (producers/registrars/publisher
    /// gone). Buffered values may remain: a source is exhausted only when this
    /// is true AND `try_select` returns `None`.
    fn selection_over(&self) -> bool;
}

impl<T, K, S> Selectable for crate::mpsc::Receiver<T, K, S>
where
    T: Send,
    K: crate::mpsc::Kernel,
    S: crate::mpsc::ShardState<T, K>,
{
    type Item = T;
    fn try_select(&mut self) -> Option<T> {
        self.try_recv()
    }
    fn selection_over(&self) -> bool {
        self.is_disconnected()
    }
}

impl<T: Send, B: crate::backend::Backend> Selectable for crate::mpmc::brokerless::Consumer<T, B> {
    type Item = T;
    fn try_select(&mut self) -> Option<T> {
        self.try_recv()
    }
    fn selection_over(&self) -> bool {
        self.is_disconnected()
    }
}

impl<T, B, R> Selectable for crate::mpmc::brokerless::dynamic::Consumer<T, B, R>
where
    T: Send,
    B: crate::backend::Backend,
    R: crate::mpmc::brokerless::dynamic::Rings<T, B>,
{
    type Item = T;
    fn try_select(&mut self) -> Option<T> {
        self.try_recv()
    }
    fn selection_over(&self) -> bool {
        self.is_disconnected()
    }
}

impl<T: Send, B: crate::backend::Backend> Selectable for crate::mpmc::brokered::Consumer<T, B> {
    type Item = T;
    fn try_select(&mut self) -> Option<T> {
        self.try_recv()
    }
    fn selection_over(&self) -> bool {
        self.is_disconnected()
    }
}

impl<T: Clone + Send> Selectable for crate::mpmc::broadcast::Reader<T> {
    type Item = T;
    fn try_select(&mut self) -> Option<T> {
        self.try_recv()
    }
    fn selection_over(&self) -> bool {
        self.is_disconnected()
    }
}

/// Adaptive wait between empty poll rounds: brief spin, then yields, then short
/// sleeps growing to a ~100 µs cap. Public so hand-rolled poll loops can reuse it.
pub struct Backoff {
    step: u32,
}

impl Backoff {
    #[allow(clippy::new_without_default)]
    /// Construct a backoff at its initial spin phase.
    pub fn new() -> Self {
        Backoff { step: 0 }
    }

    /// Reset to the spin phase (call after useful work).
    pub fn reset(&mut self) {
        self.step = 0;
    }

    /// Wait a little; each consecutive call without a `reset` waits more.
    pub fn snooze(&mut self) {
        match self.step {
            0..=5 => {
                for _ in 0..(1 << self.step) {
                    std::hint::spin_loop();
                }
            }
            6..=15 => std::thread::yield_now(),
            s => {
                // 10µs doubling to a 100µs cap.
                let us = (10u64 << (s - 16).min(4)).min(100);
                std::thread::sleep(Duration::from_micros(us));
            }
        }
        self.step = self.step.saturating_add(1);
    }
}

/// Receive from whichever source is ready first. Arms are
/// `recv(receiver) -> pat => body` (crossbeam's arm shape), tried in order
/// (biased); the mandatory trailing `complete => body` arm runs once every
/// source is disconnected AND drained. The whole macro is an expression: the
/// taken arm's body is its value. Unlike crossbeam, `pat` binds the value `T`
/// directly (no `Result` wrapper) — end-of-stream is the `complete` arm.
///
/// See [the module docs](mod@crate::select) for semantics and an example.
#[macro_export]
macro_rules! select {
    ($(recv($rx:expr) -> $pat:pat => $body:expr),+ , complete => $done:expr $(,)?) => {{
        let mut __backoff = $crate::select::Backoff::new();
        loop {
            let mut __exhausted = true;
            $(
                match $crate::select::Selectable::try_select(&mut $rx) {
                    ::core::option::Option::Some($pat) => break $body,
                    ::core::option::Option::None => {
                        if !$crate::select::Selectable::selection_over(&$rx) {
                            __exhausted = false;
                        }
                    }
                }
            )+
            if __exhausted {
                break $done;
            }
            __backoff.snooze();
        }
    }};
}
