//! Brokered competing-consumer / pub-sub MPMC.
//!
//! A dedicated router ("broker") thread sits between producers and consumers. Each
//! producer owns an SPSC ring into the broker; each consumer owns an SPSC ring out
//! of the broker. The broker drains the producer rings and *places* each value onto
//! consumer rings according to a policy — every hop is SPSC (no CAS on the value
//! path). The policy is the reason to choose `brokered` over `brokerless`.
//!
//! Configure with the crate's [`crate::Channel`] declaration: round-robin is the
//! default, `route` selects one-consumer affinity/partitioning, and `pubsub`
//! selects a subscriber set (and therefore requires `T: Clone`). `open()` spawns
//! brokers by default; `manual()` returns them for scoped, pinned, named, or
//! inline execution.
//!
//! Round-robin/`route` deliver each value to exactly one consumer; only `pubsub`
//! can deliver a value to several. [`Targets::all`] is broadcast, [`Targets::none`]
//! drops the value.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::backend::{Backend, Ring, Rx as _, Tx as _};
use crate::routing::{PubSub, RoundRobin, Route};

/// Declare brokered MPMC with Ring storage and round-robin routing.
pub fn channel<T>() -> crate::Channel<
    T,
    crate::topology::MpmcBrokered<crate::broker::Spawned>,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new().mpmc_brokered()
}

struct Shared {
    /// Producers still alive; the broker stops once this is zero and every
    /// producer ring is drained.
    live_producers: AtomicUsize,
}

/// A producer half: one SPSC ring into the broker. Never contends with other producers.
pub struct Producer<T: Send, B: Backend = Ring> {
    tx: B::Tx<T>,
    shared: Arc<Shared>,
}

/// A consumer half: one SPSC ring out of the broker. Never contends with other consumers.
pub struct Consumer<T: Send, B: Backend = Ring> {
    /// One SPSC transport per broker (brokers never share a consumer ring).
    rxs: Vec<B::Rx<T>>,
    /// Round-robin scan position across `rxs`.
    cursor: usize,
}

// ---- placement policy -----------------------------------------------------

/// A set of target consumers for `Channel::pubsub` delivery, as a bitmask over
/// consumer indices (so up to 64 consumers). Returned by the subscribe closure.
///
/// ```
/// use prescient::mpmc::brokered::Targets;
/// # #[derive(Clone)] enum Topic { Metrics, Logs, All }
/// // route by enum: Metrics → consumer 0, Logs → consumers 1 and 2, All → everyone
/// let pick = |t: &Topic, n: usize| match t {
///     Topic::Metrics => Targets::one(0),
///     Topic::Logs => Targets::of([1, 2]),
///     Topic::All => Targets::all(n),
/// };
/// # let _ = pick;
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Targets(u64);

#[inline]
fn bit(i: usize) -> u64 {
    1u64.checked_shl(i as u32).unwrap_or(0) // indices ≥ 64 select nothing
}

impl Targets {
    /// No consumer — the value is dropped (e.g. a topic nobody subscribes to).
    pub const fn none() -> Self {
        Targets(0)
    }
    /// Every consumer: broadcast. `n` is the consumer count passed to the closure.
    pub fn all(n: usize) -> Self {
        Targets(if n >= 64 { u64::MAX } else { (1u64 << n) - 1 })
    }
    /// Exactly consumer `i`.
    pub fn one(i: usize) -> Self {
        Targets(bit(i))
    }
    /// Add consumer `i` to the set (fluent style).
    pub fn with(self, i: usize) -> Self {
        Targets(self.0 | bit(i))
    }
    /// The set of the given consumer indices.
    pub fn of(indices: impl IntoIterator<Item = usize>) -> Self {
        Targets(indices.into_iter().fold(0, |m, i| m | bit(i)))
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Statically dispatched broker delivery policy.
#[doc(hidden)]
pub trait Delivery<T: Send, B: Backend>: sealed::Sealed + Send {
    const MAX_CONSUMERS: Option<usize>;
    /// Bind immutable topology-derived state before any endpoint is published.
    #[inline]
    fn prepare(&mut self, _consumers: usize) {}
    fn deliver(&mut self, consumers: &mut [B::Tx<T>], item: T);
}

/// The router. Move it onto its own thread and call [`run`](Self::run); it returns
/// once all producers are dropped and every value has been placed.
pub struct Broker<T: Send, B: Backend = Ring, P: Delivery<T, B> = RoundRobin> {
    producer_rxs: Vec<B::Rx<T>>,
    consumer_txs: Vec<B::Tx<T>>,
    shared: Arc<Shared>,
    batch: usize,
    policy: P,
}

/// Producer and consumer endpoints returned by a spawned broker declaration.
pub type ChannelEndpoints<T, B = Ring> = (Vec<Producer<T, B>>, Vec<Consumer<T, B>>);

/// Producer, consumer, and broker handles returned by a manual broker declaration.
pub type ChannelParts<T, B = Ring, P = RoundRobin> = (
    Vec<Producer<T, B>>,
    Vec<Consumer<T, B>>,
    Vec<Broker<T, B, P>>,
);
pub(crate) fn open<T: Send, B: Backend, P: Delivery<T, B> + Clone>(
    n_producers: usize,
    n_consumers: usize,
    n_brokers: usize,
    capacity: usize,
    mut policy: P,
) -> ChannelParts<T, B, P> {
    assert!(
        n_producers >= 1 && n_consumers >= 1,
        "need >=1 producer and >=1 consumer"
    );
    policy.prepare(n_consumers);
    let n_brokers = n_brokers.min(n_producers); // an idle broker with no producers is pointless
    let shared = Arc::new(Shared {
        live_producers: AtomicUsize::new(n_producers),
    });

    // Producer transports, split round-robin across brokers (producer i -> broker i % nb).
    let mut producers = Vec::with_capacity(n_producers);
    let mut per_broker_rxs: Vec<Vec<B::Rx<T>>> = (0..n_brokers).map(|_| Vec::new()).collect();
    for i in 0..n_producers {
        let (tx, rx) = B::channel::<T>(capacity);
        producers.push(Producer {
            tx,
            shared: Arc::clone(&shared),
        });
        per_broker_rxs[i % n_brokers].push(rx);
    }

    // One transport per (broker, consumer): brokers never share a consumer ring.
    let mut per_consumer_rxs: Vec<Vec<B::Rx<T>>> = (0..n_consumers).map(|_| Vec::new()).collect();
    let mut brokers = Vec::with_capacity(n_brokers);
    for prod_rxs in per_broker_rxs {
        let mut cons_txs = Vec::with_capacity(n_consumers);
        for rxs in per_consumer_rxs.iter_mut() {
            let (tx, rx) = B::channel::<T>(capacity);
            cons_txs.push(tx);
            rxs.push(rx);
        }
        brokers.push(Broker {
            producer_rxs: prod_rxs,
            consumer_txs: cons_txs,
            shared: Arc::clone(&shared),
            batch: 64,
            policy: policy.clone(),
        });
    }

    let consumers = per_consumer_rxs
        .into_iter()
        .map(|rxs| Consumer { rxs, cursor: 0 })
        .collect();
    (producers, consumers, brokers)
}

// ---- producer / consumer --------------------------------------------------

impl<T: Send, B: Backend> Producer<T, B> {
    /// Non-blocking send. Returns the value back when the broker ring is full or
    /// every broker endpoint for this producer has gone away.
    #[inline]
    pub fn try_send(&mut self, v: T) -> Result<(), T> {
        self.tx.try_push(v)
    }

    /// Send, spinning while this producer's ring is full. Returns `false` and
    /// drops the value if its broker exited, including after a routing panic.
    pub fn send(&mut self, mut v: T) -> bool {
        let mut s = 0u32;
        loop {
            if self.tx.is_consumer_gone() {
                return false;
            }
            match self.tx.try_push(v) {
                Ok(()) => return true,
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
}

impl<T: Send, B: Backend> Drop for Producer<T, B> {
    fn drop(&mut self) {
        self.shared.live_producers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: Send, B: Backend> Consumer<T, B> {
    /// Non-blocking receive: round-robins this consumer's per-broker transports.
    pub fn try_recv(&mut self) -> Option<T> {
        let n = self.rxs.len();
        for _ in 0..n {
            let i = crate::round_robin::next(&mut self.cursor, n);
            if let Some(v) = self.rxs[i].try_pop() {
                return Some(v);
            }
        }
        None
    }

    /// True once every broker has finished AND this consumer's transports are
    /// drained — nothing more can ever arrive.
    pub fn is_disconnected(&self) -> bool {
        self.rxs.iter().all(|rx| rx.is_finished())
    }

    /// Blocking receive. `None` once every broker has finished and this consumer's
    /// transports are drained.
    pub fn recv(&mut self) -> Option<T> {
        let mut s = 0u32;
        loop {
            if let Some(v) = self.try_recv() {
                return Some(v);
            }
            if self.rxs.iter().all(|rx| rx.is_finished()) {
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
}

// ---- delivery policies ----------------------------------------------------

/// Deliver one value to a single consumer, waiting if its ring is full and dropping
/// the value if that consumer is gone.
fn forward_to<T: Send, B: Backend>(ctxs: &mut [B::Tx<T>], i: usize, mut item: T) {
    loop {
        if ctxs[i].is_consumer_gone() {
            return; // dropped
        }
        match ctxs[i].try_push(item) {
            Ok(()) => return,
            Err(back) => {
                item = back;
                std::hint::spin_loop();
            }
        }
    }
}

/// Round-robin from `*cursor`; skips full consumers (retrying) and gone ones. Drops
/// the value only if every consumer is gone.
fn forward_rr<T: Send, B: Backend>(cursor: &mut usize, ctxs: &mut [B::Tx<T>], mut item: T) {
    let ncons = ctxs.len();
    loop {
        let mut gone = 0usize;
        for _ in 0..ncons {
            let c = crate::round_robin::next(cursor, ncons);
            if ctxs[c].is_consumer_gone() {
                gone += 1;
                continue;
            }
            match ctxs[c].try_push(item) {
                Ok(()) => return,
                Err(back) => item = back,
            }
        }
        if gone == ncons {
            return; // nowhere to deliver; `item` dropped here
        }
        std::hint::spin_loop();
    }
}

impl sealed::Sealed for RoundRobin {}
impl<T: Send, B: Backend> Delivery<T, B> for RoundRobin {
    const MAX_CONSUMERS: Option<usize> = None;

    #[inline]
    fn deliver(&mut self, consumers: &mut [B::Tx<T>], item: T) {
        forward_rr::<T, B>(&mut self.0, consumers, item);
    }
}

impl<F> sealed::Sealed for Route<F> {}
impl<T: Send, B: Backend, F> Delivery<T, B> for Route<F>
where
    F: FnMut(&T, usize) -> usize + Send,
{
    const MAX_CONSUMERS: Option<usize> = None;

    #[inline]
    fn deliver(&mut self, consumers: &mut [B::Tx<T>], item: T) {
        let n = consumers.len();
        let target = (self.0)(&item, n) % n;
        forward_to::<T, B>(consumers, target, item);
    }
}

impl<F> sealed::Sealed for PubSub<F> {}
impl<T: Clone + Send, B: Backend, F> Delivery<T, B> for PubSub<F>
where
    F: FnMut(&T, usize) -> Targets + Send,
{
    const MAX_CONSUMERS: Option<usize> = Some(64);

    #[inline]
    fn prepare(&mut self, consumers: usize) {
        self.consumer_mask = Targets::all(consumers).0;
    }

    #[inline]
    fn deliver(&mut self, consumers: &mut [B::Tx<T>], item: T) {
        let mut mask = (self.subscribe)(&item, consumers.len()).0 & self.consumer_mask;
        // Visit only selected consumers, in ascending order. The last selected
        // consumer takes the original; earlier ones receive N-1 clones. An empty
        // mask drops the original. No optional payload or configured-count scan.
        while mask != 0 {
            let index = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            if mask == 0 {
                forward_to::<T, B>(consumers, index, item);
                break;
            }
            forward_to::<T, B>(consumers, index, item.clone());
        }
    }
}

// ---- broker loop ----------------------------------------------------------

impl<T: Send, B: Backend, P: Delivery<T, B>> Broker<T, B, P> {
    /// Set the max values moved per producer ring per sweep. Default 64.
    pub fn set_batch(&mut self, batch: usize) {
        self.batch = batch.max(1);
    }

    /// Run the router until every producer is dropped and every producer ring is
    /// drained, then signal consumers by dropping the consumer senders. Blocks the
    /// calling thread — spawn it.
    pub fn run(mut self) {
        loop {
            let moved = self.sweep(self.batch);
            if moved == 0 {
                if self.shared.live_producers.load(Ordering::Acquire) == 0 {
                    if self.sweep(usize::MAX) == 0 {
                        break;
                    }
                } else {
                    std::hint::spin_loop();
                }
            }
        }
        // Dropping `self` (hence `consumer_txs`) sets producer_gone on every consumer
        // ring, so each Consumer::recv observes is_finished -> None.
    }

    /// One pass over all producer rings, placing up to `per` from each. Returns the
    /// number of values moved.
    fn sweep(&mut self, per: usize) -> usize {
        let Broker {
            producer_rxs,
            consumer_txs,
            policy,
            ..
        } = self;
        let mut moved = 0usize;
        for prx in producer_rxs.iter_mut() {
            moved += prx.drain_in_place(per, |item| policy.deliver(consumer_txs, item));
        }
        moved
    }
}
