//! `open()` implementations for supported declaration combinations.

// The explicit return types are the compile-time proof that each declaration opens
// only the selected endpoint machinery.
#![allow(clippy::type_complexity)]

use core::fmt;

use crate::Channel;
use crate::backend::{Backend, Ring};
use crate::broker::{Manual, Spawned};
use crate::codec::Native;
use crate::execution::{Async, Sync};
use crate::membership::Locked;
use crate::mpsc::chan::{Kernel, SyncKernel};
use crate::reclaim::{Drop as DropPolicy, Pooled};
use crate::routing::RoundRobin;
use crate::topology::{
    Broadcast, Mpmc, MpmcBrokered, MpmcDynamicArray, MpmcDynamicLocked, MpscDynamic, MpscFixed,
    MpscPool,
};
use crate::transport::Local;
use crate::wait::{SpinYield, Task};

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use crate::codec::{Bytes, Encoded, Pod};
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use crate::topology::ProcessDuplex;
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use crate::transport::{ProcessAttach, ProcessCreate};
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use crate::wait::Park;

/// Failure to validate or construct a declared channel.
#[derive(Debug)]
pub enum OpenError {
    /// A declaration contains an unsupported or internally inconsistent shape.
    Invalid(&'static str),
    /// The operating system refused to create a requested broker thread.
    Spawn(std::io::Error),
    /// Process-transport setup or I/O failed.
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    Io(std::io::Error),
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Spawn(error) => write!(formatter, "failed to spawn broker thread: {error}"),
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid(_) => None,
            Self::Spawn(error) => Some(error),
            #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
            Self::Io(error) => Some(error),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl From<std::io::Error> for OpenError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[inline]
fn validate_common(capacity: usize, batch: usize) -> Result<(), OpenError> {
    if capacity == 0 {
        return Err(OpenError::Invalid("channel capacity must be non-zero"));
    }
    if capacity.checked_next_power_of_two().is_none() {
        return Err(OpenError::Invalid(
            "channel capacity overflows its ring shape",
        ));
    }
    if batch == 0 {
        return Err(OpenError::Invalid("channel batch must be non-zero"));
    }
    Ok(())
}

const DEFAULT_CAPACITY: usize = 1024;
const RING_RESIDENT_TARGET_BYTES: u128 = 1024 * 1024;

fn capacity_or_default(configured: Option<usize>) -> usize {
    configured.unwrap_or(DEFAULT_CAPACITY)
}

// Quantize to the four measured capacity plateaus. Boundaries are geometric
// midpoints, keeping the selected window within 2x between the 64/4,096 clamps.
fn quantized_capacity(slots: u128) -> usize {
    match slots {
        0..=127 => 64,
        128..=511 => 256,
        512..=2047 => 1024,
        _ => 4096,
    }
}

fn automatic_ring_capacity<T>(lanes: usize) -> usize {
    let payload_bytes = core::mem::size_of::<T>().max(1) as u128;
    let slots = RING_RESIDENT_TARGET_BYTES / payload_bytes / lanes.max(1) as u128;
    quantized_capacity(slots)
}

fn native_capacity<T, B: Backend>(
    configured: Option<usize>,
    producers: usize,
    consumers: usize,
) -> usize {
    configured.unwrap_or_else(|| {
        if B::BOUNDED_CAPACITY {
            automatic_ring_capacity::<T>(producers.max(consumers))
        } else {
            DEFAULT_CAPACITY
        }
    })
}

fn leased_capacity<T>(
    configured: Option<usize>,
    producers: usize,
    consumers: usize,
    batch: usize,
) -> usize {
    configured.unwrap_or_else(|| {
        let payload_bytes = core::mem::size_of::<T>().max(1) as u128;
        let slots = RING_RESIDENT_TARGET_BYTES * consumers.max(1) as u128
            / (payload_bytes * producers.max(1) as u128);
        // Pools above 1,024 helped cache-line payloads only; larger values regressed.
        let maximum = if payload_bytes <= 64 { 4096 } else { 1024 };
        let capacity = quantized_capacity(slots).min(maximum);
        if batch == 0 || capacity.is_multiple_of(batch) {
            capacity
        } else if capacity < batch {
            batch
        } else {
            capacity.div_ceil(batch) * batch
        }
    })
}
fn fixed<T: Send, K: Kernel>(
    declaration: Channel<T, MpscFixed, Ring, impl Sized, K>,
) -> Result<
    (
        Vec<crate::mpsc::Producer<T, K>>,
        crate::mpsc::Receiver<T, K>,
    ),
    OpenError,
> {
    let capacity =
        native_capacity::<T, Ring>(declaration.shape.capacity, declaration.shape.producers, 1);
    validate_common(capacity, declaration.shape.batch)?;
    let (producers, mut receiver) =
        crate::mpsc::fixed::open::<T, K>(declaration.shape.producers, capacity);
    receiver.set_batch(declaration.shape.batch);
    Ok((producers, receiver))
}

impl<T: Send, W: SyncKernel> Channel<T, MpscFixed, Ring, Sync, W, RoundRobin, Local, Native> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            Vec<crate::mpsc::Producer<T, W>>,
            crate::mpsc::Receiver<T, W>,
        ),
        OpenError,
    > {
        fixed(self)
    }
}

impl<T: Send> Channel<T, MpscFixed, Ring, Async, Task, RoundRobin, Local, Native> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            Vec<crate::mpsc::Producer<T, Task>>,
            crate::mpsc::Receiver<T, Task>,
        ),
        OpenError,
    > {
        fixed(self)
    }
}

fn pool<T: Send, K: Kernel>(
    declaration: Channel<T, MpscPool, Ring, impl Sized, K>,
) -> Result<
    (
        crate::mpsc::pool::PoolHandle<T, K>,
        crate::mpsc::Receiver<T, K, crate::mpsc::PoolState<T, K>>,
    ),
    OpenError,
> {
    if !(1..=64).contains(&declaration.shape.max_producers) {
        return Err(OpenError::Invalid(
            "MPSC pool max_producers must be in 1..=64",
        ));
    }
    let capacity = native_capacity::<T, Ring>(
        declaration.shape.capacity,
        declaration.shape.max_producers,
        1,
    );
    validate_common(capacity, declaration.shape.batch)?;
    let (handle, mut receiver) =
        crate::mpsc::pool::open::<T, K>(declaration.shape.max_producers, capacity);
    receiver.set_batch(declaration.shape.batch);
    Ok((handle, receiver))
}

impl<T: Send, W: SyncKernel> Channel<T, MpscPool, Ring, Sync, W, RoundRobin, Local, Native> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            crate::mpsc::pool::PoolHandle<T, W>,
            crate::mpsc::Receiver<T, W, crate::mpsc::PoolState<T, W>>,
        ),
        OpenError,
    > {
        pool(self)
    }
}

impl<T: Send> Channel<T, MpscPool, Ring, Async, Task, RoundRobin, Local, Native> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            crate::mpsc::pool::PoolHandle<T, Task>,
            crate::mpsc::Receiver<T, Task, crate::mpsc::PoolState<T, Task>>,
        ),
        OpenError,
    > {
        pool(self)
    }
}

fn dynamic_shape(capacity: usize, batch: usize) -> Result<(), OpenError> {
    validate_common(capacity, batch)
}

fn dynamic_mpsc_capacity<T>(shape: &crate::declaration::Shape) -> usize {
    native_capacity::<T, Ring>(shape.capacity, shape.producers, 1)
}
fn finish_dynamic<T, K, I, R>(
    batch: usize,
    endpoints: crate::mpsc::dynamic::Endpoints<T, K, I, R>,
) -> crate::mpsc::dynamic::Endpoints<T, K, I, R>
where
    T: Send,
    K: Kernel,
    I: crate::mpsc::Inbox<T, K>,
    R: crate::mpsc::Reclaim<T, K>,
{
    let (registrar, mut receiver) = endpoints;
    receiver.set_batch(batch);
    (registrar, receiver)
}

impl<T: Send, W: SyncKernel>
    Channel<T, MpscDynamic<Locked, DropPolicy>, Ring, Sync, W, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::mpsc::dynamic::Endpoints<T, W>, OpenError> {
        dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
        Ok(finish_dynamic(
            self.shape.batch,
            crate::mpsc::dynamic::open::<T, W>(dynamic_mpsc_capacity::<T>(&self.shape)),
        ))
    }
}

impl<T: Send>
    Channel<T, MpscDynamic<Locked, DropPolicy>, Ring, Async, Task, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::mpsc::dynamic::Endpoints<T, Task>, OpenError> {
        dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
        Ok(finish_dynamic(
            self.shape.batch,
            crate::mpsc::dynamic::open::<T, Task>(dynamic_mpsc_capacity::<T>(&self.shape)),
        ))
    }
}

macro_rules! pooled_dynamic_open {
    ($membership:ty, $inbox:ident, $open:ident) => {
        impl<T: Send, W: SyncKernel>
            Channel<T, MpscDynamic<$membership, Pooled>, Ring, Sync, W, RoundRobin, Local, Native>
        {
            /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
            pub fn open(
                self,
            ) -> Result<
                crate::mpsc::dynamic::Endpoints<
                    T,
                    W,
                    crate::mpsc::$inbox<T, W>,
                    crate::mpsc::PooledRings<T, W>,
                >,
                OpenError,
            > {
                dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
                let max_pooled = self.shape.recycling.ok_or(OpenError::Invalid(
                    "MPSC dynamic recycling requires an explicit pool size",
                ))?;
                if max_pooled == 0 {
                    return Err(OpenError::Invalid(
                        "MPSC dynamic recycling capacity must be non-zero",
                    ));
                }
                Ok(finish_dynamic(
                    self.shape.batch,
                    crate::mpsc::dynamic::$open::<T, W>(
                        dynamic_mpsc_capacity::<T>(&self.shape),
                        max_pooled,
                    ),
                ))
            }
        }

        impl<T: Send>
            Channel<
                T,
                MpscDynamic<$membership, Pooled>,
                Ring,
                Async,
                Task,
                RoundRobin,
                Local,
                Native,
            >
        {
            /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
            pub fn open(
                self,
            ) -> Result<
                crate::mpsc::dynamic::Endpoints<
                    T,
                    Task,
                    crate::mpsc::$inbox<T, Task>,
                    crate::mpsc::PooledRings<T, Task>,
                >,
                OpenError,
            > {
                dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
                let max_pooled = self.shape.recycling.ok_or(OpenError::Invalid(
                    "MPSC dynamic recycling requires an explicit pool size",
                ))?;
                if max_pooled == 0 {
                    return Err(OpenError::Invalid(
                        "MPSC dynamic recycling capacity must be non-zero",
                    ));
                }
                Ok(finish_dynamic(
                    self.shape.batch,
                    crate::mpsc::dynamic::$open::<T, Task>(
                        dynamic_mpsc_capacity::<T>(&self.shape),
                        max_pooled,
                    ),
                ))
            }
        }
    };
}

pooled_dynamic_open!(Locked, LockedInbox, open_recycling);

mod lock_free_dynamic {
    use super::*;
    use crate::membership::LockFree;

    impl<T: Send, W: SyncKernel<Space = ()>>
        Channel<T, MpscDynamic<LockFree, DropPolicy>, Ring, Sync, W, RoundRobin, Local, Native>
    {
        /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
        pub fn open(
            self,
        ) -> Result<crate::mpsc::dynamic::Endpoints<T, W, crate::mpsc::LockFreeInbox<T>>, OpenError>
        {
            dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
            Ok(finish_dynamic(
                self.shape.batch,
                crate::mpsc::dynamic::open_lock_free::<T, W>(dynamic_mpsc_capacity::<T>(
                    &self.shape,
                )),
            ))
        }
    }

    impl<T: Send, W: SyncKernel<Space = ()>>
        Channel<T, MpscDynamic<LockFree, Pooled>, Ring, Sync, W, RoundRobin, Local, Native>
    {
        /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
        pub fn open(
            self,
        ) -> Result<
            crate::mpsc::dynamic::Endpoints<
                T,
                W,
                crate::mpsc::LockFreeInbox<T>,
                crate::mpsc::PooledRings<T, W>,
            >,
            OpenError,
        > {
            dynamic_shape(dynamic_mpsc_capacity::<T>(&self.shape), self.shape.batch)?;
            let max_pooled = self.shape.recycling.ok_or(OpenError::Invalid(
                "MPSC dynamic recycling requires an explicit pool size",
            ))?;
            if max_pooled == 0 {
                return Err(OpenError::Invalid(
                    "MPSC dynamic recycling capacity must be non-zero",
                ));
            }
            Ok(finish_dynamic(
                self.shape.batch,
                crate::mpsc::dynamic::open_lock_free_recycling::<T, W>(
                    dynamic_mpsc_capacity::<T>(&self.shape),
                    max_pooled,
                ),
            ))
        }
    }
}

impl<T: Send, B: Backend>
    Channel<T, Mpmc<crate::engine::Claim>, B, Sync, SpinYield, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            Vec<crate::mpmc::brokerless::Producer<T, B>>,
            Vec<crate::mpmc::brokerless::Consumer<T, B>>,
        ),
        OpenError,
    > {
        let capacity = native_capacity::<T, B>(
            self.shape.capacity,
            self.shape.producers,
            self.shape.consumers,
        );
        validate_common(capacity, self.shape.batch)?;
        if self.shape.producers == 0 {
            return Err(OpenError::Invalid(
                "brokerless MPMC producers must be non-zero",
            ));
        }
        if self.shape.consumers == 0 {
            return Err(OpenError::Invalid(
                "brokerless MPMC consumers must be non-zero",
            ));
        }
        let (producers, mut first) =
            crate::mpmc::brokerless::open::<T, B>(self.shape.producers, capacity);
        first.set_batch(self.shape.batch);
        let mut consumers = Vec::with_capacity(self.shape.consumers);
        for _ in 1..self.shape.consumers {
            consumers.push(first.clone());
        }
        consumers.push(first);
        Ok((producers, consumers))
    }
}

impl<T: Send, B: Backend>
    Channel<T, Mpmc<crate::engine::Lanes>, B, Sync, SpinYield, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct permanent producer-consumer lane endpoints.
    pub fn open(
        self,
    ) -> Result<
        (
            Vec<crate::mpmc::lanes::Producer<T, B>>,
            Vec<crate::mpmc::lanes::Consumer<T, B>>,
        ),
        OpenError,
    > {
        let capacity = native_capacity::<T, B>(
            self.shape.capacity,
            self.shape.producers,
            self.shape.consumers,
        );
        validate_common(capacity, self.shape.batch)?;
        if self.shape.producers == 0 {
            return Err(OpenError::Invalid(
                "lane-grid MPMC producers must be non-zero",
            ));
        }
        if self.shape.consumers == 0 {
            return Err(OpenError::Invalid(
                "lane-grid MPMC consumers must be non-zero",
            ));
        }
        // Capacity retains brokerless meaning: total slots per producer. Split it
        // across permanent consumer lanes once; the backend may round each lane.
        let lane_capacity = capacity.div_ceil(self.shape.consumers);
        validate_common(lane_capacity, self.shape.batch)?;
        let (producers, mut consumers) = crate::mpmc::lanes::open::<T, B>(
            self.shape.producers,
            self.shape.consumers,
            lane_capacity,
        );
        for consumer in &mut consumers {
            consumer.set_batch(self.shape.batch);
        }
        Ok((producers, consumers))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn process_shape(
    shape: &crate::declaration::Shape,
) -> Result<(crate::ipc::StreamShape, crate::ipc::EndpointOptions), OpenError> {
    if shape.chunk_bytes == 0 {
        return Err(OpenError::Invalid("IPC chunk_bytes must be non-zero"));
    }
    if shape.slots == 0 {
        return Err(OpenError::Invalid("IPC slots must be non-zero"));
    }
    if shape.transfer_id == 0 || shape.transfer_id == u64::MAX {
        return Err(OpenError::Invalid(
            "IPC duplex transfer_id must leave two non-zero ids",
        ));
    }
    if shape.timeout.is_zero() {
        return Err(OpenError::Invalid("IPC timeout must be non-zero"));
    }
    Ok((
        crate::ipc::StreamShape {
            total_bytes: shape.total_bytes,
            chunk_bytes: shape.chunk_bytes,
            slots: shape.slots,
        },
        crate::ipc::EndpointOptions {
            lock_memory: shape.lock_memory,
            timeout: shape.timeout,
        },
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn process_options(
    shape: &crate::declaration::Shape,
) -> Result<crate::ipc::EndpointOptions, OpenError> {
    if shape.timeout.is_zero() {
        return Err(OpenError::Invalid("IPC timeout must be non-zero"));
    }
    Ok(crate::ipc::EndpointOptions {
        lock_memory: shape.lock_memory,
        timeout: shape.timeout,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl Channel<u8, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessCreate, Bytes> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::Duplex, OpenError> {
        let (shape, options) = process_shape(&self.shape)?;
        let control = crate::ipc::accept_named(&self.transport.0, options.timeout)?;
        Ok(crate::ipc::create(
            shape,
            self.shape.transfer_id,
            options,
            crate::ipc::PayloadContract::bytes(),
            control,
        )?)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl Channel<u8, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessAttach, Bytes> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::Duplex, OpenError> {
        let options = process_options(&self.shape)?;
        let control = crate::ipc::connect_named(&self.transport.0, options.timeout)?;
        Ok(crate::ipc::attach_peer(
            options,
            crate::ipc::PayloadContract::bytes(),
            control,
        )?)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<P: crate::ipc::IpcPod>
    Channel<P, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessCreate, Pod<P>>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::PodDuplex<P>, OpenError> {
        let (shape, options) = process_shape(&self.shape)?;
        let element_size = core::mem::size_of::<P>();
        if element_size == 0 || shape.total_bytes % element_size as u64 != 0 {
            return Err(OpenError::Invalid(
                "IPC POD shape must contain a whole number of values",
            ));
        }
        let payload = crate::ipc::PayloadContract::pod::<P>();
        payload.validate()?;
        let control = crate::ipc::accept_named(&self.transport.0, options.timeout)?;
        let duplex = crate::ipc::create(shape, self.shape.transfer_id, options, payload, control)?;
        Ok(crate::ipc::PodDuplex::new(duplex))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<P: crate::ipc::IpcPod>
    Channel<P, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessAttach, Pod<P>>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::PodDuplex<P>, OpenError> {
        let options = process_options(&self.shape)?;
        let payload = crate::ipc::PayloadContract::pod::<P>();
        payload.validate()?;
        let control = crate::ipc::connect_named(&self.transport.0, options.timeout)?;
        let duplex = crate::ipc::attach_peer(options, payload, control)?;
        Ok(crate::ipc::PodDuplex::new(duplex))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, C: crate::ipc::IpcCodec<T>>
    Channel<T, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessCreate, Encoded<C>>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::CodecDuplex<T, C>, OpenError> {
        let (shape, options) = process_shape(&self.shape)?;
        let payload = crate::ipc::PayloadContract::codec::<T, C>();
        payload.validate()?;
        let control = crate::ipc::accept_named(&self.transport.0, options.timeout)?;
        let duplex = crate::ipc::create(shape, self.shape.transfer_id, options, payload, control)?;
        Ok(crate::ipc::CodecDuplex::new(duplex))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
impl<T, C: crate::ipc::IpcCodec<T>>
    Channel<T, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessAttach, Encoded<C>>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::ipc::CodecDuplex<T, C>, OpenError> {
        let options = process_options(&self.shape)?;
        let payload = crate::ipc::PayloadContract::codec::<T, C>();
        payload.validate()?;
        let control = crate::ipc::connect_named(&self.transport.0, options.timeout)?;
        let duplex = crate::ipc::attach_peer(options, payload, control)?;
        Ok(crate::ipc::CodecDuplex::new(duplex))
    }
}

impl<T: Send>
    Channel<
        T,
        Mpmc<crate::engine::Claim>,
        Ring,
        Sync,
        SpinYield,
        RoundRobin,
        Local,
        crate::codec::Leased,
    >
{
    /// Open finite, reusable payload pools and fixed-membership competing consumers.
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            Vec<crate::mpmc::brokerless::leased::Producer<T>>,
            Vec<crate::mpmc::brokerless::leased::Consumer<T>>,
        ),
        OpenError,
    > {
        let capacity = leased_capacity::<T>(
            self.shape.capacity,
            self.shape.producers,
            self.shape.consumers,
            self.shape.batch,
        );
        crate::mpmc::brokerless::leased::open(
            self.shape.producers,
            self.shape.consumers,
            capacity,
            self.shape.batch,
        )
    }
}
impl<T: Clone + Send> Channel<T, Broadcast, Ring, Sync, SpinYield, RoundRobin, Local, Native> {
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            crate::mpmc::broadcast::Publisher<T>,
            crate::mpmc::broadcast::Reader<T>,
        ),
        OpenError,
    > {
        let capacity = capacity_or_default(self.shape.capacity);
        if capacity < 2 {
            return Err(OpenError::Invalid(
                "broadcast capacity must be at least two",
            ));
        }
        validate_common(capacity, self.shape.batch)?;
        if !(1..=64).contains(&self.shape.max_readers) {
            return Err(OpenError::Invalid(
                "broadcast max_readers must be in 1..=64",
            ));
        }
        Ok(crate::mpmc::broadcast::open(
            capacity,
            self.shape.max_readers,
        ))
    }
}

fn validate_brokered(
    producers: usize,
    consumers: usize,
    brokers: usize,
    capacity: usize,
    batch: usize,
) -> Result<(), OpenError> {
    validate_common(capacity, batch)?;
    if producers == 0 {
        return Err(OpenError::Invalid(
            "brokered MPMC producers must be non-zero",
        ));
    }
    if consumers == 0 {
        return Err(OpenError::Invalid(
            "brokered MPMC consumers must be non-zero",
        ));
    }
    if brokers == 0 {
        return Err(OpenError::Invalid("brokered MPMC brokers must be non-zero"));
    }
    if brokers > producers {
        return Err(OpenError::Invalid(
            "brokered MPMC brokers cannot exceed producers",
        ));
    }
    Ok(())
}
fn validate_routing<T: Send, B: Backend, P: crate::mpmc::brokered::Delivery<T, B>>(
    consumers: usize,
) -> Result<(), OpenError> {
    if P::MAX_CONSUMERS.is_some_and(|limit| consumers > limit) {
        return Err(OpenError::Invalid(
            "brokered pub/sub supports at most 64 consumers",
        ));
    }
    Ok(())
}

fn finish_spawned_open<T, B, P, F>(
    parts: crate::mpmc::brokered::ChannelParts<T, B, P>,
    batch: usize,
    mut spawn: F,
) -> Result<crate::mpmc::brokered::ChannelEndpoints<T, B>, OpenError>
where
    T: Send,
    B: Backend,
    P: crate::mpmc::brokered::Delivery<T, B>,
    F: FnMut(crate::mpmc::brokered::Broker<T, B, P>) -> std::io::Result<()>,
{
    let (producers, consumers, mut brokers) = parts;
    for broker in &mut brokers {
        broker.set_batch(batch);
    }
    for broker in brokers {
        spawn(broker).map_err(OpenError::Spawn)?;
    }
    Ok((producers, consumers))
}

impl<T, B, P> Channel<T, MpmcBrokered<Spawned>, B, Sync, SpinYield, P, Local, Native>
where
    T: Send + 'static,
    B: Backend,
    P: crate::mpmc::brokered::Delivery<T, B> + Clone + 'static,
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::mpmc::brokered::ChannelEndpoints<T, B>, OpenError> {
        let capacity = capacity_or_default(self.shape.capacity);
        validate_brokered(
            self.shape.producers,
            self.shape.consumers,
            self.shape.brokers,
            capacity,
            self.shape.batch,
        )?;
        validate_routing::<T, B, P>(self.shape.consumers)?;
        let parts = crate::mpmc::brokered::open(
            self.shape.producers,
            self.shape.consumers,
            self.shape.brokers,
            capacity,
            self.routing,
        );
        finish_spawned_open(parts, self.shape.batch, |broker| {
            std::thread::Builder::new()
                .spawn(move || broker.run())
                .map(|_| ())
        })
    }
}

impl<T, B, P> Channel<T, MpmcBrokered<Manual>, B, Sync, SpinYield, P, Local, Native>
where
    T: Send + 'static,
    B: Backend,
    P: crate::mpmc::brokered::Delivery<T, B> + Clone + 'static,
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(self) -> Result<crate::mpmc::brokered::ChannelParts<T, B, P>, OpenError> {
        let capacity = capacity_or_default(self.shape.capacity);
        validate_brokered(
            self.shape.producers,
            self.shape.consumers,
            self.shape.brokers,
            capacity,
            self.shape.batch,
        )?;
        validate_routing::<T, B, P>(self.shape.consumers)?;
        let (producers, consumers, mut brokers) = crate::mpmc::brokered::open(
            self.shape.producers,
            self.shape.consumers,
            self.shape.brokers,
            capacity,
            self.routing,
        );
        for broker in &mut brokers {
            broker.set_batch(self.shape.batch);
        }
        Ok((producers, consumers, brokers))
    }
}

impl<T: Send, B: Backend>
    Channel<T, MpmcDynamicLocked, B, Sync, SpinYield, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            crate::mpmc::brokerless::dynamic::Registrar<
                T,
                B,
                crate::mpmc::brokerless::dynamic::LockedRings<T, B>,
            >,
            Vec<
                crate::mpmc::brokerless::dynamic::Consumer<
                    T,
                    B,
                    crate::mpmc::brokerless::dynamic::LockedRings<T, B>,
                >,
            >,
        ),
        OpenError,
    > {
        if self.shape.producers == 0 {
            return Err(OpenError::Invalid(
                "dynamic MPMC expected_producers must be non-zero",
            ));
        }
        if self.shape.consumers == 0 {
            return Err(OpenError::Invalid(
                "dynamic MPMC consumers must be non-zero",
            ));
        }
        let capacity = native_capacity::<T, B>(
            self.shape.capacity,
            self.shape.producers,
            self.shape.consumers,
        );
        validate_common(capacity, self.shape.batch)?;
        let (registrar, mut first) =
            crate::mpmc::brokerless::dynamic::open_locked::<T, B>(capacity);
        first.set_batch(self.shape.batch);
        let mut consumers = Vec::with_capacity(self.shape.consumers);
        for _ in 1..self.shape.consumers {
            consumers.push(first.clone());
        }
        consumers.push(first);
        Ok((registrar, consumers))
    }
}

impl<T: Send, B: Backend>
    Channel<T, MpmcDynamicArray, B, Sync, SpinYield, RoundRobin, Local, Native>
{
    /// Validate this declaration and construct its concrete endpoints, returning [`OpenError`] on invalid shape or setup failure.
    pub fn open(
        self,
    ) -> Result<
        (
            crate::mpmc::brokerless::dynamic::Registrar<
                T,
                B,
                crate::mpmc::brokerless::dynamic::ArrayRings<T, B>,
            >,
            Vec<
                crate::mpmc::brokerless::dynamic::Consumer<
                    T,
                    B,
                    crate::mpmc::brokerless::dynamic::ArrayRings<T, B>,
                >,
            >,
        ),
        OpenError,
    > {
        if self.shape.array_producers == 0 {
            return Err(OpenError::Invalid(
                "array MPMC max_producers must be non-zero",
            ));
        }
        if self.shape.consumers == 0 {
            return Err(OpenError::Invalid(
                "dynamic MPMC consumers must be non-zero",
            ));
        }
        let capacity = native_capacity::<T, B>(
            self.shape.capacity,
            self.shape.array_producers,
            self.shape.consumers,
        );
        validate_common(capacity, self.shape.batch)?;
        let (registrar, mut first) = crate::mpmc::brokerless::dynamic::open_array::<T, B>(
            capacity,
            self.shape.array_producers,
        );
        first.set_batch(self.shape.batch);
        let mut consumers = Vec::with_capacity(self.shape.consumers);
        for _ in 1..self.shape.consumers {
            consumers.push(first.clone());
        }
        consumers.push(first);
        Ok((registrar, consumers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn automatic_capacity_is_payload_topology_and_backend_aware() {
        assert_eq!(native_capacity::<[u8; 8], Ring>(None, 1, 1), 4096);
        assert_eq!(native_capacity::<[u8; 256], Ring>(None, 1, 1), 4096);
        assert_eq!(native_capacity::<[u8; 256], Ring>(None, 3, 3), 1024);
        assert_eq!(native_capacity::<[u8; 256], Ring>(None, 5, 1), 1024);
        assert_eq!(native_capacity::<[u8; 256], Ring>(None, 1, 5), 1024);
        assert_eq!(native_capacity::<[u8; 4096], Ring>(None, 1, 1), 256);
        assert_eq!(native_capacity::<[u8; 4096], Ring>(None, 3, 3), 64);
        assert_eq!(native_capacity::<[u8; 4096], Ring>(None, 5, 1), 64);
        assert_eq!(native_capacity::<[u8; 4096], Ring>(None, 1, 5), 64);
        assert_eq!(
            native_capacity::<[u8; 4096], crate::backend::Seg>(None, 5, 5),
            1024
        );
        assert_eq!(native_capacity::<[u8; 4096], Ring>(Some(17), 5, 5), 17);

        assert_eq!(leased_capacity::<[u8; 8]>(None, 5, 1, 64), 4096);
        assert_eq!(leased_capacity::<[u8; 256]>(None, 1, 5, 64), 1024);
        assert_eq!(leased_capacity::<[u8; 4096]>(None, 1, 1, 64), 256);
        assert_eq!(leased_capacity::<[u8; 4096]>(None, 3, 3, 64), 256);
        assert_eq!(leased_capacity::<[u8; 4096]>(None, 5, 1, 64), 64);
        assert_eq!(leased_capacity::<[u8; 4096]>(None, 1, 5, 64), 1024);
        assert_eq!(leased_capacity::<[u8; 4096]>(None, 1, 1, 96), 288);
        assert_eq!(leased_capacity::<[u8; 4096]>(Some(192), 1, 1, 64), 192);
    }
    #[test]
    fn partial_broker_spawn_failure_rolls_back_and_quiesces() {
        let parts =
            crate::mpmc::brokered::open::<u64, Ring, RoundRobin>(2, 1, 2, 4, RoundRobin::default());
        let (done_tx, done_rx) = mpsc::channel();
        let mut starts = 0;
        let result = finish_spawned_open(parts, 1, |broker| {
            starts += 1;
            if starts == 1 {
                let done_tx = done_tx.clone();
                std::thread::Builder::new()
                    .spawn(move || {
                        broker.run();
                        done_tx.send(()).unwrap();
                    })
                    .map(|_| ())
            } else {
                drop(broker);
                Err(std::io::Error::other("injected broker spawn failure"))
            }
        });

        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("partial broker startup unexpectedly succeeded"),
        };
        assert_eq!(starts, 2);
        assert_eq!(
            error.to_string(),
            "failed to spawn broker thread: injected broker spawn failure"
        );
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            "injected broker spawn failure"
        );
        match error {
            OpenError::Spawn(source) => assert_eq!(source.kind(), std::io::ErrorKind::Other),
            error => panic!("unexpected startup error: {error}"),
        }
        drop(done_tx);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("started broker did not quiesce after endpoint rollback");
    }
}
