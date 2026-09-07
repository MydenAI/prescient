//! Type-state channel declarations.
//!
//! A [`Channel`] is a zero-work declaration. Configuration changes its concrete
//! type or numeric shape; [`open`](Channel::open) exists only for supported
//! combinations and consumes the declaration to construct exact endpoint types.

use core::marker::PhantomData;
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use core::time::Duration;

use crate::backend::Ring;

/// Fixed-MPMC transfer-engine markers.
pub mod engine {
    /// Selects consumer claims over one SPSC ring per producer.
    pub struct Claim;
    /// Selects one permanently owned SPSC lane per producer-consumer pair.
    pub struct Lanes;
}

/// Channel topology markers.
pub mod topology {
    use core::marker::PhantomData;

    /// Fixed-membership multiple-producer, single-consumer topology.
    pub struct MpscFixed;
    /// Bounded producer-pool MPSC topology with reusable producer slots.
    pub struct MpscPool;
    /// Runtime-registerable MPSC topology with explicit membership and reclaim policies.
    pub struct MpscDynamic<Membership = crate::membership::Locked, Reclaim = crate::reclaim::Drop>(
        pub(crate) PhantomData<fn() -> (Membership, Reclaim)>,
    );
    /// Fixed-membership competing-consumer MPMC topology.
    ///
    /// Its transfer engine is selected statically and does not enter opened hot paths.
    pub struct Mpmc<Engine = crate::engine::Claim>(pub(crate) PhantomData<fn() -> Engine>);
    /// Dynamic brokerless MPMC topology with lock-protected membership.
    pub struct MpmcDynamicLocked;
    /// Dynamic brokerless MPMC topology with bounded array membership.
    pub struct MpmcDynamicArray;
    /// Brokered MPMC topology parameterized by broker ownership.
    pub struct MpmcBrokered<Run = crate::broker::Spawned>(pub(crate) PhantomData<fn() -> Run>);
    /// Bounded broadcast topology in which every active reader receives each message.
    pub struct Broadcast;
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    /// Bidirectional shared-memory process transport topology.
    pub struct ProcessDuplex;
}

/// Broker ownership markers.
pub mod broker {
    /// Selects broker threads spawned transactionally during `open()`.
    pub struct Spawned;
    /// Selects broker handles returned to the caller for manual execution.
    pub struct Manual;
}

/// Execution-surface markers.
pub mod execution {
    /// Selects the synchronous endpoint surface.
    pub struct Sync;
    /// Selects the runtime-neutral asynchronous endpoint surface.
    pub struct Async;
}

/// Consumer wait-policy markers.
pub mod wait {
    /// Selects parking-lot eventcount waits.
    pub struct Park;
    /// Selects standard-library thread park/unpark waits.
    pub struct StdThread;
    /// Selects bounded spinning followed by parking.
    pub struct Hybrid;
    /// Selects continuous busy spinning while waiting.
    pub struct Spin;
    /// Selects spinning with scheduler yields while waiting.
    pub struct SpinYield;
    /// Selects task-waker notification for asynchronous endpoints.
    pub struct Task;
}

/// Dynamic-MPSC membership markers.
pub mod membership {
    /// Selects lock-protected dynamic-MPSC membership.
    pub struct Locked;
    /// Selects intrusive lock-free dynamic-MPSC membership.
    pub struct LockFree;
}

/// Dynamic-MPSC finished-ring policies.
pub mod reclaim {
    /// Drops finished dynamic-MPSC rings instead of retaining them.
    pub struct Drop;
    /// Retains finished dynamic-MPSC rings in a bounded reuse pool.
    pub struct Pooled;
}

/// Routing policy markers and concrete closure carriers.
pub mod routing {
    #[derive(Clone, Default)]
    /// Routes successive brokered messages across consumers in round-robin order.
    pub struct RoundRobin(pub(crate) usize);

    #[derive(Clone)]
    /// Carries a routing function that selects one consumer per message.
    pub struct Route<F>(pub(crate) F);
    #[derive(Clone)]
    /// Carries a subscription function that selects a consumer set per message.
    pub struct PubSub<F> {
        pub(crate) subscribe: F,
        // Bound once by open(); no consumer-count configuration branch in delivery.
        pub(crate) consumer_mask: u64,
    }
}

/// Address-space / transport markers.
pub mod transport {
    /// Selects in-process transport.
    pub struct Local;
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    /// Selects creation of a new process-duplex transport.
    pub struct ProcessCreate(pub(crate) String);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    /// Selects attachment to an existing process-duplex endpoint.
    pub struct ProcessAttach(pub(crate) String);
}

/// Payload representation markers.
pub mod codec {
    use core::marker::PhantomData;

    /// Selects ordinary in-process Rust value representation.
    pub struct Native;
    /// Local reusable payload blocks with explicit ownership leases.
    pub struct Leased;
    /// Selects raw byte-stream process payloads.
    pub struct Bytes;
    /// Selects fixed-layout POD process payloads of type `T`.
    pub struct Pod<T>(pub(crate) PhantomData<fn() -> T>);
    /// Selects process payloads encoded by codec `C`.
    pub struct Encoded<C>(pub(crate) PhantomData<fn() -> C>);
}

#[derive(Clone, Copy)]
pub(crate) struct Shape {
    pub(crate) producers: usize,
    pub(crate) consumers: usize,
    pub(crate) brokers: usize,
    pub(crate) capacity: Option<usize>,
    pub(crate) batch: usize,
    pub(crate) max_producers: usize,
    pub(crate) max_readers: usize,
    pub(crate) recycling: Option<usize>,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) timeout: Duration,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) lock_memory: bool,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) total_bytes: u64,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) chunk_bytes: usize,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) slots: usize,
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(crate) transfer_id: u64,
    pub(crate) array_producers: usize,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            producers: 1,
            consumers: 1,
            brokers: 1,
            capacity: None,
            batch: 64,
            max_producers: 64,
            max_readers: 64,
            recycling: None,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            timeout: Duration::from_secs(30),
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            lock_memory: false,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            total_bytes: 0,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            chunk_bytes: 64 * 1024,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            slots: 8,
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            transfer_id: 1,
            array_producers: 0,
        }
    }
}

/// A typed channel declaration.
///
/// The value stores only numeric/resource configuration and an optional concrete
/// routing policy. All behavioral axes are represented by type parameters and
/// disappear when `open()` constructs the selected endpoints.
pub struct Channel<
    T,
    Topology = topology::MpscFixed,
    Backend = Ring,
    Execution = execution::Sync,
    Wait = wait::Park,
    Routing = routing::RoundRobin,
    Transport = transport::Local,
    Codec = codec::Native,
> {
    pub(crate) shape: Shape,
    pub(crate) routing: Routing,
    pub(crate) transport: Transport,
    #[allow(clippy::type_complexity)]
    pub(crate) _axes: PhantomData<fn() -> (T, Topology, Backend, Execution, Wait, Codec)>,
}

impl<T> Channel<T> {
    /// Declare the default local channel: fixed MPSC, Ring storage, synchronous
    /// execution, parking wait, one producer, automatic bounded capacity, and batch 64.
    pub fn new() -> Self {
        Self {
            shape: Shape::default(),
            routing: routing::RoundRobin::default(),
            transport: transport::Local,
            _axes: PhantomData,
        }
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Top, B, E, W, R, A, C> Channel<T, Top, B, E, W, R, A, C> {
    fn axes<Top2, B2, E2, W2, C2>(self) -> Channel<T, Top2, B2, E2, W2, R, A, C2> {
        Channel {
            shape: self.shape,
            routing: self.routing,
            transport: self.transport,
            _axes: PhantomData,
        }
    }

    /// Select fixed-membership MPSC.
    pub fn mpsc_fixed(self) -> Channel<T, topology::MpscFixed, B, E, W, R, A, C> {
        self.axes()
    }

    /// Select allocation-free bounded producer-pool MPSC.
    pub fn mpsc_pool(self) -> Channel<T, topology::MpscPool, B, E, W, R, A, C> {
        self.axes()
    }

    /// Select runtime-registration MPSC.
    pub fn mpsc_dynamic(self) -> Channel<T, topology::MpscDynamic, B, E, W, R, A, C> {
        self.axes()
    }

    /// Select fixed-membership competing-consumer MPMC.
    ///
    /// The default claim engine preserves the shortest general-purpose layout.
    /// Use [`engine`](Channel::engine) to select another proven static engine.
    pub fn mpmc(self) -> Channel<T, topology::Mpmc<engine::Claim>, B, E, wait::SpinYield, R, A, C> {
        self.axes()
    }

    /// Select unbounded, locked-membership dynamic MPMC.
    pub fn mpmc_dynamic_locked(
        self,
    ) -> Channel<T, topology::MpmcDynamicLocked, B, E, wait::SpinYield, R, A, C> {
        self.axes()
    }

    /// Select bounded, array-membership dynamic MPMC.
    pub fn mpmc_dynamic_array(
        mut self,
        max_producers: usize,
    ) -> Channel<T, topology::MpmcDynamicArray, B, E, wait::SpinYield, R, A, C> {
        self.shape.array_producers = max_producers;
        self.axes()
    }

    /// Select brokered MPMC.
    pub fn mpmc_brokered(
        self,
    ) -> Channel<T, topology::MpmcBrokered<broker::Spawned>, B, E, wait::SpinYield, R, A, C> {
        self.axes()
    }

    /// Select publish/subscribe broadcast.
    pub fn broadcast(self) -> Channel<T, topology::Broadcast, B, E, wait::SpinYield, R, A, C> {
        self.axes()
    }

    /// Select a concrete local storage backend.
    pub fn backend<B2>(self) -> Channel<T, Top, B2, E, W, R, A, C> {
        self.axes()
    }

    /// Select the runtime-neutral asynchronous execution surface and task wakeups.
    pub fn r#async(self) -> Channel<T, Top, B, execution::Async, wait::Task, R, A, C> {
        self.axes()
    }

    /// Override automatic ring capacity or the selected backend's chunk hint.
    /// Validation occurs once in `open()`.
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.shape.capacity = Some(capacity);
        self
    }

    /// Override the receive/drain batch. Validation occurs once in `open()`.
    pub fn batch(mut self, batch: usize) -> Self {
        self.shape.batch = batch;
        self
    }
}

impl<T, Top, B, W, R, A, C> Channel<T, Top, B, execution::Sync, W, R, A, C> {
    /// Keep synchronous execution and preserve the selected waiting policy.
    pub fn sync(self) -> Self {
        self
    }

    /// Select the exact synchronous consumer wait policy.
    pub fn wait<W2>(self) -> Channel<T, Top, B, execution::Sync, W2, R, A, C> {
        self.axes()
    }
}

// A transition from task wakeups needs a topology-specific synchronous default.
// These are declaration-only implementations; no policy lookup reaches an endpoint.
macro_rules! sync_default {
    ([$($extra:ident),*] $topology:ty => $wait:ty) => {
        impl<T, B, W, R, A, C $(, $extra)*>
            Channel<T, $topology, B, execution::Async, W, R, A, C>
        {
            /// Select synchronous execution with this topology's default waiter.
            ///
            /// An earlier synchronous waiter is replaced when selecting async.
            /// Use `wait::<W>()` after `sync()` to select a different waiter.
            pub fn sync(self) -> Channel<T, $topology, B, execution::Sync, $wait, R, A, C> {
                self.axes()
            }
        }
    };
}

sync_default!([] topology::MpscFixed => wait::Park);
sync_default!([] topology::MpscPool => wait::Park);
sync_default!([M, Q] topology::MpscDynamic<M, Q> => wait::Park);
sync_default!([Engine] topology::Mpmc<Engine> => wait::SpinYield);
sync_default!([] topology::MpmcDynamicLocked => wait::SpinYield);
sync_default!([] topology::MpmcDynamicArray => wait::SpinYield);
sync_default!([Run] topology::MpmcBrokered<Run> => wait::SpinYield);
sync_default!([] topology::Broadcast => wait::SpinYield);
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
sync_default!([] topology::ProcessDuplex => wait::Park);

impl<T, B, E, W, R, A, C> Channel<T, topology::MpscFixed, B, E, W, R, A, C> {
    /// Set the number of producer endpoints created by `open()`.
    pub fn producers(mut self, producers: usize) -> Self {
        self.shape.producers = producers;
        self
    }
}

impl<T, B, E, W, R, A, C> Channel<T, topology::MpscPool, B, E, W, R, A, C> {
    /// Set the maximum number of producer slots managed by the pool.
    pub fn max_producers(mut self, max_producers: usize) -> Self {
        self.shape.max_producers = max_producers;
        self
    }
}

impl<T, M, Q, B, E, W, R, A, C> Channel<T, topology::MpscDynamic<M, Q>, B, E, W, R, A, C> {
    /// Set the expected concurrent producer count for automatic capacity sizing.
    /// This is a sizing hint; it neither creates nor limits producer endpoints.
    pub fn expected_producers(mut self, producers: usize) -> Self {
        self.shape.producers = producers;
        self
    }
    /// Select the exact dynamic-membership inbox implementation.
    pub fn membership<M2>(self) -> Channel<T, topology::MpscDynamic<M2, Q>, B, E, W, R, A, C> {
        self.axes()
    }

    /// Retain finished rings in a bounded pool for allocation-free reuse.
    pub fn recycling(
        mut self,
        max_pooled: usize,
    ) -> Channel<T, topology::MpscDynamic<M, reclaim::Pooled>, B, E, W, R, A, C> {
        self.shape.recycling = Some(max_pooled);
        self.axes()
    }
}

impl<T, B, E, W, R, A> Channel<T, topology::Mpmc<engine::Claim>, B, E, W, R, A, codec::Native> {
    /// Select reusable payload blocks with explicit write/read leases.
    ///
    /// Leases require fixed MPMC with the Claim engine, Ring storage, synchronous
    /// execution, and SpinYield.
    /// Capacity bounds all payload slots per producer, not just queued values;
    /// batch is the maximum values per block. Automatic sizing occurs once in
    /// `open()` and adds no payload-size dispatch to the endpoints.
    pub fn leased(self) -> Channel<T, topology::Mpmc<engine::Claim>, B, E, W, R, A, codec::Leased> {
        self.axes()
    }
}
impl<T, Engine, B, E, W, R, A, C> Channel<T, topology::Mpmc<Engine>, B, E, W, R, A, C> {
    /// Select the exact fixed-MPMC transfer engine.
    ///
    /// This changes the declaration type; `open()` constructs concrete endpoints
    /// whose hot paths contain no engine tag or dispatch.
    pub fn engine<Engine2>(self) -> Channel<T, topology::Mpmc<Engine2>, B, E, W, R, A, C> {
        self.axes()
    }

    /// Set the number of producer endpoints created by `open()`.
    pub fn producers(mut self, producers: usize) -> Self {
        self.shape.producers = producers;
        self
    }

    /// Set the number of consumer endpoints created by `open()`.
    pub fn consumers(mut self, consumers: usize) -> Self {
        self.shape.consumers = consumers;
        self
    }
}

impl<T, B, E, W, R, A, C> Channel<T, topology::MpmcDynamicLocked, B, E, W, R, A, C> {
    /// Set the expected concurrent producer count for automatic capacity sizing.
    /// This is a sizing hint; locked membership remains unbounded.
    pub fn expected_producers(mut self, producers: usize) -> Self {
        self.shape.producers = producers;
        self
    }
    /// Set the number of consumer endpoints created by `open()`.
    pub fn consumers(mut self, consumers: usize) -> Self {
        self.shape.consumers = consumers;
        self
    }
}

impl<T, B, E, W, R, A, C> Channel<T, topology::MpmcDynamicArray, B, E, W, R, A, C> {
    /// Set the number of consumer endpoints created by `open()`.
    pub fn consumers(mut self, consumers: usize) -> Self {
        self.shape.consumers = consumers;
        self
    }
}

impl<T, Run, B, E, W, A, C>
    Channel<T, topology::MpmcBrokered<Run>, B, E, W, routing::RoundRobin, A, C>
{
    /// Route each message to the returned index modulo the current consumer count.
    pub fn route<F>(
        self,
        route: F,
    ) -> Channel<T, topology::MpmcBrokered<Run>, B, E, W, routing::Route<F>, A, C>
    where
        F: FnMut(&T, usize) -> usize + Clone + Send + 'static,
    {
        Channel {
            shape: self.shape,
            routing: routing::Route(route),
            transport: self.transport,
            _axes: PhantomData,
        }
    }

    /// Send each message to the consumer set returned by `subscribe`.
    pub fn pubsub<F>(
        self,
        subscribe: F,
    ) -> Channel<T, topology::MpmcBrokered<Run>, B, E, W, routing::PubSub<F>, A, C>
    where
        T: Clone,
        F: FnMut(&T, usize) -> crate::mpmc::brokered::Targets + Clone + Send + 'static,
    {
        Channel {
            shape: self.shape,
            routing: routing::PubSub {
                subscribe,
                consumer_mask: 0,
            },
            transport: self.transport,
            _axes: PhantomData,
        }
    }
}

impl<T, Run, B, E, W, R, A, C> Channel<T, topology::MpmcBrokered<Run>, B, E, W, R, A, C> {
    /// Set the number of producer endpoints created by `open()`.
    pub fn producers(mut self, producers: usize) -> Self {
        self.shape.producers = producers;
        self
    }

    /// Set the number of consumer endpoints created by `open()`.
    pub fn consumers(mut self, consumers: usize) -> Self {
        self.shape.consumers = consumers;
        self
    }

    /// Set the number of broker workers created or returned by `open()`.
    pub fn brokers(mut self, brokers: usize) -> Self {
        self.shape.brokers = brokers;
        self
    }
}

impl<T, B, E, W, R, A, C> Channel<T, topology::MpmcBrokered<broker::Spawned>, B, E, W, R, A, C> {
    /// Return concrete broker handles from `open()` instead of spawning them.
    pub fn manual(self) -> Channel<T, topology::MpmcBrokered<broker::Manual>, B, E, W, R, A, C> {
        self.axes()
    }
}

impl<T, B, E, W, R, A, C> Channel<T, topology::Broadcast, B, E, W, R, A, C> {
    /// Set the maximum number of simultaneously active broadcast readers.
    pub fn max_readers(mut self, max_readers: usize) -> Self {
        self.shape.max_readers = max_readers;
        self
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, Top, B, E, W, R, A, C> Channel<T, Top, B, E, W, R, A, C> {
    /// Select byte-oriented process duplex transport. Shared memory is an
    /// address-space choice, never a local storage backend. The topology establishes
    /// its Park wait default while preserving the execution marker.
    pub fn ipc(
        self,
    ) -> Channel<
        T,
        topology::ProcessDuplex,
        Ring,
        E,
        wait::Park,
        R,
        transport::ProcessCreate,
        codec::Bytes,
    > {
        Channel {
            shape: self.shape,
            routing: self.routing,
            transport: transport::ProcessCreate(String::from("default")),
            _axes: PhantomData,
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, B, E, W, R, C>
    Channel<T, topology::ProcessDuplex, B, E, W, R, transport::ProcessCreate, C>
{
    /// Select the peer side that joins an IPC channel created by another process.
    pub fn attach(
        self,
    ) -> Channel<T, topology::ProcessDuplex, B, E, W, R, transport::ProcessAttach, C> {
        Channel {
            shape: self.shape,
            routing: self.routing,
            transport: transport::ProcessAttach(self.transport.0),
            _axes: PhantomData,
        }
    }

    /// Override the semantic rendezvous name used when `open()` creates this IPC channel.
    /// Names contain 1–20 ASCII letters, digits, dots, dashes, or underscores.
    pub fn endpoint(mut self, name: impl Into<String>) -> Self {
        self.transport.0 = name.into();
        self
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, B, E, W, R, C>
    Channel<T, topology::ProcessDuplex, B, E, W, R, transport::ProcessAttach, C>
{
    /// Override the semantic rendezvous name used when `open()` joins an IPC channel.
    /// Names contain 1–20 ASCII letters, digits, dots, dashes, or underscores.
    pub fn endpoint(mut self, name: impl Into<String>) -> Self {
        self.transport.0 = name.into();
        self
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, B, E, W, R, C>
    Channel<T, topology::ProcessDuplex, B, E, W, R, transport::ProcessCreate, C>
{
    /// Set the total byte limit, bytes per shared slot, and number of shared slots.
    pub fn shape(mut self, total_bytes: u64, chunk_bytes: usize, slots: usize) -> Self {
        self.shape.total_bytes = total_bytes;
        self.shape.chunk_bytes = chunk_bytes;
        self.shape.slots = slots;
        self
    }

    /// Set the nonzero transfer identifier used to validate both duplex directions.
    pub fn transfer_id(mut self, transfer_id: u64) -> Self {
        self.shape.transfer_id = transfer_id;
        self
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, B, E, W, R, A, C> Channel<T, topology::ProcessDuplex, B, E, W, R, A, C> {
    /// Set the maximum blocking interval for process-transport progress.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.shape.timeout = timeout;
        self
    }

    /// Require shared regions to be memory-locked when `lock` is true.
    pub fn lock_memory(mut self, lock: bool) -> Self {
        self.shape.lock_memory = lock;
        self
    }

    /// Select raw byte-stream payloads.
    pub fn bytes(self) -> Channel<T, topology::ProcessDuplex, B, E, W, R, A, codec::Bytes> {
        self.axes()
    }

    /// Select a POD representation for `T`. `T` must implement `ipc::IpcPod` when opened.
    pub fn pod(self) -> Channel<T, topology::ProcessDuplex, B, E, W, R, A, codec::Pod<T>> {
        self.axes()
    }

    /// Select the explicit process payload codec `C2`.
    pub fn codec<C2>(
        self,
    ) -> Channel<T, topology::ProcessDuplex, B, E, W, R, A, codec::Encoded<C2>> {
        self.axes()
    }
}
