//! Typed process channels over the shared-memory transport.
//!
//! `channel()` creates a byte-stream session and `attach()` joins it from the
//! peer process. Both default to the semantic endpoint `default`; `.endpoint(name)`
//! selects another rendezvous. Socket setup and liveness are private implementation
//! details.

mod control;
mod destination;
mod event;
mod region;
mod setup;
mod stream;
#[cfg(target_os = "windows")]
mod windows_security;

use core::marker::PhantomData;
use core::mem::{align_of, size_of};
use std::io;

pub(crate) use control::{Control, accept_named, connect_named};
pub(crate) use region::RegionHandle;
pub(crate) use stream::{
    Direction, DirectionConfig, DirectionHandle, DirectionRole, EndpointOptions, SocketLiveness,
    StreamFlow,
};
pub use stream::{StreamShape, TransferReport};

/// The payload contract authenticated alongside a duplex's region handles.
///
/// It is checked before either direction is mapped. A contract describes wire
/// representation, not the local shared-memory queue backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PayloadContract {
    kind: PayloadKind,
    schema_id: u64,
    element_size: usize,
    element_align: usize,
}

/// Process-boundary payload representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PayloadKind {
    /// Raw byte-stream payloads.
    Bytes = 1,
    /// Fixed-layout plain-data payloads.
    Pod = 2,
    /// Values serialized by an explicit versioned codec.
    Codec = 3,
}

impl PayloadContract {
    /// Construct the canonical raw-byte payload contract.
    pub const fn bytes() -> Self {
        Self {
            kind: PayloadKind::Bytes,
            schema_id: 0,
            element_size: 1,
            element_align: 1,
        }
    }

    /// Construct the POD contract declared by `P`.
    pub fn pod<P: IpcPod>() -> Self {
        Self {
            kind: PayloadKind::Pod,
            schema_id: P::SCHEMA_ID,
            element_size: size_of::<P>(),
            element_align: align_of::<P>(),
        }
    }

    /// Construct the encoded contract declared by codec `C`.
    pub fn codec<T, C: IpcCodec<T>>() -> Self {
        Self {
            kind: PayloadKind::Codec,
            schema_id: C::SCHEMA_ID,
            element_size: 0,
            element_align: 0,
        }
    }

    /// Return the payload representation kind.
    pub const fn kind(self) -> PayloadKind {
        self.kind
    }

    /// Return the authenticated schema identifier.
    pub const fn schema_id(self) -> u64 {
        self.schema_id
    }

    /// Return the POD element size, or zero for encoded payloads.
    pub const fn element_size(self) -> usize {
        self.element_size
    }

    /// Return the POD element alignment, or zero for encoded payloads.
    pub const fn element_align(self) -> usize {
        self.element_align
    }

    pub(crate) fn validate(self) -> io::Result<()> {
        let valid = match self.kind {
            PayloadKind::Bytes => {
                self.schema_id == 0 && self.element_size == 1 && self.element_align == 1
            }
            PayloadKind::Pod => {
                self.schema_id != 0
                    && self.element_size != 0
                    && self.element_align.is_power_of_two()
            }
            PayloadKind::Codec => {
                self.schema_id != 0 && self.element_size == 0 && self.element_align == 0
            }
        };
        if valid {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid IPC payload contract",
            ))
        }
    }
}

/// A plain-data type whose complete object representation is safe to copy
/// between Prescient processes.
///
/// # Safety
///
/// Implementors must be `repr(C)` or `repr(transparent)`, contain no padding,
/// pointers, references, provenance, or invalid bit patterns, and assign a
/// stable non-zero `SCHEMA_ID` that changes whenever the layout or meaning
/// changes. Both processes must use the same endianness and target ABI.
pub unsafe trait IpcPod: Copy + Send + Sync + 'static {
    /// Stable nonzero identifier for this layout or encoding contract.
    const SCHEMA_ID: u64;
}

macro_rules! primitive_pod {
    ($($ty:ty => $id:expr),+ $(,)?) => {$(
        // SAFETY: integer and floating-point primitives have no padding or
        // provenance and every bit pattern is valid.
        unsafe impl IpcPod for $ty {
            const SCHEMA_ID: u64 = $id;
        }
    )+};
}

primitive_pod!(
    u8 => 0x5053_0000_0000_0001,
    i8 => 0x5053_0000_0000_0002,
    u16 => 0x5053_0000_0000_0003,
    i16 => 0x5053_0000_0000_0004,
    u32 => 0x5053_0000_0000_0005,
    i32 => 0x5053_0000_0000_0006,
    u64 => 0x5053_0000_0000_0007,
    i64 => 0x5053_0000_0000_0008,
    u128 => 0x5053_0000_0000_0009,
    i128 => 0x5053_0000_0000_000a,
    f32 => 0x5053_0000_0000_000b,
    f64 => 0x5053_0000_0000_000c,
);

/// A stateless, explicitly versioned process-boundary codec.
///
/// `SCHEMA_ID` identifies both the payload schema and the encoding. It must be
/// stable and non-zero. Codec work happens outside the shared-memory hot loop.
pub trait IpcCodec<T>: Send + Sync + 'static {
    /// Stable nonzero identifier for this layout or encoding contract.
    const SCHEMA_ID: u64;

    /// Encode one value into `output`, returning an I/O-style codec error on failure.
    fn encode(value: &T, output: &mut Vec<u8>) -> io::Result<()>;
    /// Decode one value from a complete encoded byte sequence.
    fn decode(input: &[u8]) -> io::Result<T>;
}

#[derive(Clone, Debug)]
struct DuplexHandle {
    parent_to_worker: DirectionHandle,
    worker_to_parent: DirectionHandle,
    payload: PayloadContract,
}

/// The sending half of an opened byte-stream process channel.
///
/// ```compile_fail
/// fn wrong_role(
///     endpoint: &mut prescient::ipc::Sender,
///     output: &mut Vec<u8>,
/// ) {
///     endpoint.receive(output).unwrap();
/// }
/// ```
pub struct Sender {
    direction: Direction,
    liveness: SocketLiveness,
}

impl Sender {
    #[inline]
    /// Return the validated stream dimensions for this direction.
    pub fn shape(&self) -> StreamShape {
        self.direction.shape()
    }

    #[inline]
    /// Transfer exactly the declared byte count from `reader`.
    pub fn send<R: io::Read>(&mut self, reader: &mut R) -> io::Result<TransferReport> {
        self.direction.produce(&self.liveness, reader)
    }

    #[inline]
    /// Transfer from `reader` through EOF, bounded by the declared byte limit.
    pub fn send_to_end<R: io::Read>(&mut self, reader: &mut R) -> io::Result<TransferReport> {
        self.direction.produce_to_end(&self.liveness, reader)
    }
}

/// The receiving half of an opened byte-stream process channel.
///
/// ```compile_fail
/// fn wrong_role(
///     endpoint: &mut prescient::ipc::Receiver,
///     input: &mut &[u8],
/// ) {
///     endpoint.send(input).unwrap();
/// }
/// ```
pub struct Receiver {
    direction: Direction,
    liveness: SocketLiveness,
}

impl Receiver {
    #[inline]
    /// Return the validated stream dimensions for this direction.
    pub fn shape(&self) -> StreamShape {
        self.direction.shape()
    }

    #[inline]
    /// Receive exactly the declared byte count into `writer`.
    pub fn receive<W: io::Write>(&mut self, writer: &mut W) -> io::Result<TransferReport> {
        self.direction.consume(&self.liveness, writer)
    }

    #[inline]
    /// Receive a terminal-length stream into `writer` within the declared byte limit.
    pub fn receive_to_end<W: io::Write>(&mut self, writer: &mut W) -> io::Result<TransferReport> {
        self.direction.consume_to_end(&self.liveness, writer)
    }
}

/// An opened full-duplex raw-byte process channel.
pub struct Duplex {
    // Keep the two direction engines contiguous and first. This preserves the
    // direct engine layout; control ownership must not perturb its hot fields.
    outbound: Direction,
    inbound: Direction,
    outbound_liveness: SocketLiveness,
    inbound_liveness: SocketLiveness,
}

impl Duplex {
    fn new(outbound: Direction, inbound: Direction, control: Control) -> io::Result<Self> {
        let inbound_liveness = SocketLiveness::new(control)?;
        Ok(Self {
            outbound,
            inbound,
            outbound_liveness: inbound_liveness.try_clone()?,
            inbound_liveness,
        })
    }

    /// Return the common dimensions of both directions.
    pub fn shape(&self) -> StreamShape {
        self.outbound.shape()
    }

    /// Send exactly the declared byte count from `reader`.
    #[inline]
    pub fn send<R: io::Read>(&mut self, reader: &mut R) -> io::Result<TransferReport> {
        self.outbound.produce(&self.outbound_liveness, reader)
    }

    /// Send through EOF within the declared byte ceiling.
    #[inline]
    pub fn send_to_end<R: io::Read>(&mut self, reader: &mut R) -> io::Result<TransferReport> {
        self.outbound
            .produce_to_end(&self.outbound_liveness, reader)
    }

    /// Receive exactly the declared byte count into `writer`.
    #[inline]
    pub fn receive<W: io::Write>(&mut self, writer: &mut W) -> io::Result<TransferReport> {
        self.inbound.consume(&self.inbound_liveness, writer)
    }

    /// Receive a terminal-length stream within the declared byte ceiling.
    #[inline]
    pub fn receive_to_end<W: io::Write>(&mut self, writer: &mut W) -> io::Result<TransferReport> {
        self.inbound.consume_to_end(&self.inbound_liveness, writer)
    }

    /// Split the session into independently movable send and receive halves.
    pub fn split(self) -> (Sender, Receiver) {
        (
            Sender {
                direction: self.outbound,
                liveness: self.outbound_liveness,
            },
            Receiver {
                direction: self.inbound,
                liveness: self.inbound_liveness,
            },
        )
    }
}

/// A process duplex whose byte stream is exactly a sequence of `P` values.
pub struct PodDuplex<P: IpcPod> {
    raw: Duplex,
    _payload: PhantomData<fn() -> P>,
}

impl<P: IpcPod> PodDuplex<P> {
    pub(crate) fn new(raw: Duplex) -> Self {
        Self {
            raw,
            _payload: PhantomData,
        }
    }

    /// Send the exact POD slice required by the declared total byte count.
    pub fn send(&mut self, values: &[P]) -> io::Result<TransferReport> {
        let bytes_len = values
            .len()
            .checked_mul(size_of::<P>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "POD slice is too large"))?;
        let expected = usize::try_from(self.raw.outbound.shape().total_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "POD shape is too large"))?;
        if bytes_len != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "POD slice does not match the declared byte shape",
            ));
        }
        // SAFETY: `IpcPod` guarantees a fully initialized, padding-free byte
        // representation with no provenance-bearing fields.
        let mut bytes =
            unsafe { core::slice::from_raw_parts(values.as_ptr().cast::<u8>(), bytes_len) };
        self.raw.send(&mut bytes)
    }

    /// Receive the declared POD payload into aligned `P` storage.
    pub fn receive(&mut self) -> io::Result<(Vec<P>, TransferReport)> {
        let bytes_len = usize::try_from(self.raw.inbound.shape().total_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "POD shape is too large"))?;
        let mut destination = destination::PodDestination::<P>::new(bytes_len)?;
        let report = self.raw.receive(&mut destination)?;
        Ok((destination.finish()?, report))
    }
}

/// A process duplex that encodes and decodes values through `C`.
pub struct CodecDuplex<T, C: IpcCodec<T>> {
    raw: Duplex,
    _codec: PhantomData<fn() -> (T, C)>,
}

impl<T, C: IpcCodec<T>> CodecDuplex<T, C> {
    pub(crate) fn new(raw: Duplex) -> Self {
        Self {
            raw,
            _codec: PhantomData,
        }
    }

    /// Encode and send one value, bounded by the declared byte limit.
    pub fn send(&mut self, value: &T) -> io::Result<TransferReport> {
        let mut encoded = Vec::new();
        C::encode(value, &mut encoded)?;
        let mut input = encoded.as_slice();
        self.raw.send_to_end(&mut input)
    }

    /// Receive and decode one terminal-length encoded value.
    pub fn receive(&mut self) -> io::Result<(T, TransferReport)> {
        let mut encoded = Vec::new();
        let report = self.raw.receive_to_end(&mut encoded)?;
        Ok((C::decode(&encoded)?, report))
    }
}

use crate::Channel;
use crate::backend::Ring;
use crate::codec::Bytes;
use crate::execution::Sync as SyncExecution;
use crate::routing::RoundRobin;
use crate::topology::ProcessDuplex;
use crate::transport::{ProcessAttach, ProcessCreate};
use crate::wait::Park;

/// Creating-side raw-byte IPC declaration.
pub type Create =
    Channel<u8, ProcessDuplex, Ring, SyncExecution, Park, RoundRobin, ProcessCreate, Bytes>;
/// Attaching-side raw-byte IPC declaration.
pub type Attach =
    Channel<u8, ProcessDuplex, Ring, SyncExecution, Park, RoundRobin, ProcessAttach, Bytes>;

/// Declare the creating side of a raw-byte process channel.
pub fn channel() -> Create {
    Channel::<u8>::new().ipc()
}

/// Declare the attaching side of a raw-byte process channel.
pub fn attach() -> Attach {
    Channel::<u8>::new().ipc().attach()
}

/// POD process-duplex shorthand.
pub mod pod {
    use crate::Channel;
    use crate::backend::Ring;
    use crate::codec::Pod;
    use crate::execution::Sync;
    use crate::routing::RoundRobin;
    use crate::topology::ProcessDuplex;
    use crate::transport::{ProcessAttach, ProcessCreate};
    use crate::wait::Park;

    use super::IpcPod;

    /// Creating-side declaration for this payload representation.
    pub type Create<P> =
        Channel<P, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessCreate, Pod<P>>;
    /// Attaching-side declaration for this payload representation.
    pub type Attach<P> =
        Channel<P, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessAttach, Pod<P>>;

    /// Declare the creating side of a POD process duplex.
    pub fn channel<P: IpcPod>() -> Create<P> {
        Channel::<P>::new().ipc().pod()
    }

    /// Declare the attaching side of a POD process duplex.
    pub fn attach<P: IpcPod>() -> Attach<P> {
        Channel::<P>::new().ipc().attach().pod()
    }
}

/// Explicit-codec process-duplex shorthand.
pub mod codec {
    use crate::Channel;
    use crate::backend::Ring;
    use crate::codec::Encoded;
    use crate::execution::Sync;
    use crate::routing::RoundRobin;
    use crate::topology::ProcessDuplex;
    use crate::transport::{ProcessAttach, ProcessCreate};
    use crate::wait::Park;

    use super::IpcCodec;

    /// Creating-side declaration for this payload representation.
    pub type Create<T, C> =
        Channel<T, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessCreate, Encoded<C>>;
    /// Attaching-side declaration for this payload representation.
    pub type Attach<T, C> =
        Channel<T, ProcessDuplex, Ring, Sync, Park, RoundRobin, ProcessAttach, Encoded<C>>;

    /// Declare the creating side of an explicit-codec process duplex.
    pub fn channel<T, C: IpcCodec<T>>() -> Create<T, C> {
        Channel::<T>::new().ipc().codec::<C>()
    }

    /// Declare the attaching side of an explicit-codec process duplex.
    pub fn attach<T, C: IpcCodec<T>>() -> Attach<T, C> {
        Channel::<T>::new().ipc().attach().codec::<C>()
    }
}

pub(crate) fn create(
    shape: StreamShape,
    transfer_id: u64,
    options: EndpointOptions,
    payload: PayloadContract,
    mut control: Control,
) -> std::io::Result<Duplex> {
    let reverse_id = transfer_id.checked_add(1).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "duplex transfer id overflows",
        )
    })?;
    let (outbound, parent_to_worker) = Direction::create(
        DirectionConfig {
            shape,
            transfer_id,
            flow: StreamFlow::ParentToWorker,
            options,
        },
        DirectionRole::Producer,
    )?;
    let (inbound, worker_to_parent) = Direction::create(
        DirectionConfig {
            shape,
            transfer_id: reverse_id,
            flow: StreamFlow::WorkerToParent,
            options,
        },
        DirectionRole::Consumer,
    )?;
    let handle = DuplexHandle {
        parent_to_worker,
        worker_to_parent,
        payload,
    };
    setup::send(&mut control, &handle, options.timeout)?;
    Duplex::new(outbound, inbound, control)
}

pub(crate) fn attach_peer(
    options: EndpointOptions,
    expected_payload: PayloadContract,
    mut control: Control,
) -> std::io::Result<Duplex> {
    let result = (|| {
        let handle = setup::receive(&mut control, options.timeout)?;
        handle.payload.validate()?;
        if handle.payload != expected_payload {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "IPC payload contract mismatch",
            ));
        }
        if handle.parent_to_worker.shape() != handle.worker_to_parent.shape()
            || handle.parent_to_worker.flow() != StreamFlow::ParentToWorker
            || handle.worker_to_parent.flow() != StreamFlow::WorkerToParent
            || handle.parent_to_worker.transfer_id().checked_add(1)
                != Some(handle.worker_to_parent.transfer_id())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "IPC setup does not describe one paired transport",
            ));
        }
        if handle.payload.kind == PayloadKind::Pod
            && handle.parent_to_worker.shape().total_bytes
                % u64::try_from(handle.payload.element_size)
                    .map_err(|_| io::Error::other("IPC POD element size is too large"))?
                != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC POD shape must contain a whole number of values",
            ));
        }
        let inbound = Direction::open(&handle.parent_to_worker, options, DirectionRole::Consumer)?;
        let outbound = Direction::open(&handle.worker_to_parent, options, DirectionRole::Producer)?;
        Ok((outbound, inbound))
    })();
    let (outbound, inbound) = match result {
        Ok(directions) => directions,
        Err(error) => {
            setup::reject(&mut control);
            return Err(error);
        }
    };
    setup::accept(&mut control)?;
    Duplex::new(outbound, inbound, control)
}
