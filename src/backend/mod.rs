//! Local storage backends used by channel topologies that support substitution.
//!
//! [`Ring`] is the bounded SPSC primitive used by local MPSC and MPMC kernels.
//! [`Seg`] is unbounded segmented SPSC storage supported by the brokerless and
//! brokered MPMC declarations. A topology exposes `.backend::<B>()`, but only
//! meaningful combinations have an `open()` implementation.
//!
//! * [`ring`] — bounded Lamport storage with cache-padded cursors, cached
//!   counters, and in-place batched drain. Loom/Miri/TSan-verified.
//! * [`seg`] — unbounded segmented storage whose sends never report full; memory
//!   grows with backlog instead of applying ring backpressure.
//!
//! Shared memory is intentionally absent from this namespace. It changes address
//! space, mapping ownership, liveness, and representation, and is therefore
//! selected through `ipc`/process transport rather than as a local backend.

mod batch;
pub use batch::Batch;
pub mod ring;
pub mod seg;

pub use ring::Ring;
pub use seg::Seg;

/// A single-producer / single-consumer transport factory. `capacity` is the
/// bound for bounded backends, or a chunk-size hint for unbounded ones.
pub trait Backend: 'static {
    /// Whether `capacity` bounds resident values instead of sizing allocation chunks.
    /// Automatic capacity selection is applied only to bounded backends.
    const BOUNDED_CAPACITY: bool = false;
    /// The producing endpoint (exactly one per channel).
    type Tx<T: Send>: Tx<T>;
    /// The consuming endpoint (exactly one per channel).
    type Rx<T: Send>: Rx<T>;

    /// Create one SPSC transport.
    fn channel<T: Send>(capacity: usize) -> (Self::Tx<T>, Self::Rx<T>);
}

/// Producing endpoint of an SPSC transport.
pub trait Tx<T: Send>: Send {
    /// Non-blocking push. `Err(v)` returns the value when it cannot be accepted
    /// (bounded backend full, or the consumer endpoint is gone).
    fn try_push(&mut self, v: T) -> Result<(), T>;
    /// Move up to `max` already-owned values from the front of `source`.
    /// Returns the accepted count. Unaccepted values stay in FIFO order.
    /// Implementations must preserve valid ownership on panic; accepted values
    /// are not delivery acknowledgements. No user iterator is called.
    #[inline]
    fn push_from(&mut self, max: usize, source: &mut Batch<T>) -> usize {
        let mut accepted = 0;
        while accepted < max {
            let Some(value) = source.pop_front() else {
                break;
            };
            if let Err(value) = self.try_push(value) {
                source.restore_front(value);
                break;
            }
            accepted += 1;
        }
        accepted
    }

    /// True once the consuming endpoint has been dropped.
    fn is_consumer_gone(&self) -> bool;
}

/// Consuming endpoint of an SPSC transport.
pub trait Rx<T: Send>: Send {
    /// Non-blocking pop.
    fn try_pop(&mut self) -> Option<T>;

    /// Drain up to `max` ready values **in place**, invoking `f` on each; returns
    /// the count. Must be panic-safe: a slot whose value has been moved out is
    /// never read again even if `f` panics.
    fn drain_in_place(&mut self, max: usize, f: impl FnMut(T)) -> usize;
    /// Append up to `max` ready owned values, preserving the destination prefix.
    /// Returns the number appended. The default supports any backend; contiguous
    /// backends can reserve once and move whole spans without per-item callbacks.
    /// On panic, both source and destination must retain valid ownership.
    #[inline]
    fn drain_into(&mut self, max: usize, out: &mut Batch<T>) -> usize {
        self.drain_in_place(max, |value| out.push_back(value))
    }

    /// Nothing ready to pop right now.
    fn is_empty(&self) -> bool;

    /// Producer gone AND drained: this transport will never yield again.
    fn is_finished(&self) -> bool;
}
