//! Fixed-width descriptor rings and bounded payload leases.

use std::io::{self, Read, Write};
use std::mem::{align_of, size_of};
use std::net::Shutdown;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::Control;
use super::event::{ProcessEvent, SharedEventState, WaitOutcome};
use super::region::{RegionHandle, SharedRegion};

const ARENA_MAGIC: u32 = 0x5053_4152; // "PSAR"
const FRAME_MAGIC: u32 = 0x5053_5354; // "PSST"
const VERSION: u16 = 2;
const FRAME_LEN: usize = 48;
const CACHE_LINE: usize = 64;
const HEAD_OFFSET: usize = CACHE_LINE;
const TAIL_OFFSET: usize = CACHE_LINE * 2;
const DATA_EVENT_OFFSET: usize = CACHE_LINE * 3;
const SPACE_EVENT_OFFSET: usize = DATA_EVENT_OFFSET + size_of::<SharedEventState>();
const HEADER_LEN: usize = SPACE_EVENT_OFFSET + size_of::<SharedEventState>();
const LIVENESS_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STREAM_FLAG: u8 = 0x40;
const RELEASE_ARENA_FLAG: u32 = 0x8000_0000;
const ROLE_CLAIMS_OFFSET: usize = 40;
const PRODUCER_CLAIM: u32 = 1 << 0;
const CONSUMER_CLAIM: u32 = 1 << 1;
const ROLE_CLAIM_MASK: u32 = PRODUCER_CLAIM | CONSUMER_CLAIM;

const _: () = assert!(HEADER_LEN == 448);
const _: () = assert!(ROLE_CLAIMS_OFFSET.is_multiple_of(align_of::<AtomicU32>()));
const _: () = assert!(ROLE_CLAIMS_OFFSET + size_of::<AtomicU32>() <= HEAD_OFFSET);

/// Logical stream dimensions. `total_bytes` may be much larger than the
/// resident `chunk_bytes * slots` window and may be zero. The ordinary
/// `produce`/`consume` methods treat it as exact; the `*_to_end` methods treat
/// it as a fail-closed byte ceiling for a length discovered while streaming.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamShape {
    /// Exact byte count, or the hard ceiling for terminal-length transfers.
    pub total_bytes: u64,
    /// Maximum payload bytes carried by one shared slot.
    pub chunk_bytes: usize,
    /// Number of reusable payload slots resident in shared memory.
    pub slots: usize,
}

impl StreamShape {
    /// Number of fixed-window leases used by this logical stream.
    pub fn chunks(self) -> u64 {
        self.total_bytes.div_ceil(self.chunk_bytes as u64)
    }

    /// Maximum resident payload bytes for this direction.
    pub fn resident_bytes(self) -> usize {
        self.slots * self.chunk_bytes
    }

    fn validate(self) -> io::Result<()> {
        if self.chunk_bytes == 0 || self.chunk_bytes > u32::MAX as usize {
            return Err(invalid_input("stream chunk_bytes must be in 1..=u32::MAX"));
        }
        if self.slots == 0 || self.slots > u32::MAX as usize {
            return Err(invalid_input("stream slots must be in 1..=u32::MAX"));
        }
        self.slots
            .checked_mul(self.chunk_bytes)
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .ok_or_else(|| invalid_input("stream resident size overflows address space"))?;
        let generations = self.chunks().div_ceil(self.slots as u64);
        if generations > u32::MAX as u64 {
            return Err(invalid_input("stream slot generation would overflow"));
        }
        Ok(())
    }
}

/// Direction relative to the supervising parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum StreamFlow {
    /// Data moves from the supervising parent to its worker.
    ParentToWorker = 1,
    /// Data moves from the worker to its supervising parent.
    WorkerToParent = 2,
}

impl TryFrom<u8> for StreamFlow {
    type Error = io::Error;

    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::ParentToWorker),
            2 => Ok(Self::WorkerToParent),
            _ => Err(invalid_input("unknown process stream flow")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectionRole {
    Producer,
    Consumer,
}

impl DirectionRole {
    fn claim(self) -> u32 {
        match self {
            Self::Producer => PRODUCER_CLAIM,
            Self::Consumer => CONSUMER_CLAIM,
        }
    }
}

fn claim_direction_role(claims: &AtomicU32, role: DirectionRole) -> io::Result<()> {
    let claim = role.claim();
    let mut current = claims.load(Ordering::Acquire);
    loop {
        if current & !ROLE_CLAIM_MASK != 0 {
            return Err(invalid_data("shared stream role claims are invalid"));
        }
        if current & claim != 0 {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "IPC direction role is already claimed",
            ));
        }
        match claims.compare_exchange_weak(
            current,
            current | claim,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

/// Configuration used by the process that creates a direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectionConfig {
    pub shape: StreamShape,
    pub transfer_id: u64,
    pub flow: StreamFlow,
    pub options: EndpointOptions,
}

impl DirectionConfig {
    fn validate(self) -> io::Result<()> {
        self.shape.validate()?;
        if self.transfer_id == 0 {
            return Err(invalid_input("transfer id zero is reserved"));
        }
        self.options.validate()
    }
}

/// Per-process transport policy. It is not serialized into an untrusted
/// descriptor; each endpoint applies its own lock and timeout requirements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EndpointOptions {
    pub lock_memory: bool,
    pub timeout: Duration,
}

impl Default for EndpointOptions {
    fn default() -> Self {
        Self {
            lock_memory: false,
            timeout: Duration::from_secs(30),
        }
    }
}

impl EndpointOptions {
    fn validate(self) -> io::Result<()> {
        if self.timeout.is_zero() {
            Err(invalid_input("stream timeout must be non-zero"))
        } else {
            Ok(())
        }
    }
}

/// Serializable capability locators for one direction. These names identify
/// private regions; they do not replace an authenticated supervisor handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectionHandle {
    data: RegionHandle,
    releases: RegionHandle,
    shape: StreamShape,
    transfer_id: u64,
    flow: StreamFlow,
}

impl DirectionHandle {
    /// Reconstruct a handle decoded from an authenticated private control frame.
    pub(crate) fn from_parts(
        data: RegionHandle,
        releases: RegionHandle,
        shape: StreamShape,
        transfer_id: u64,
        flow: StreamFlow,
    ) -> io::Result<Self> {
        shape.validate()?;
        if transfer_id == 0 {
            return Err(invalid_input("transfer id zero is reserved"));
        }
        Ok(Self {
            data,
            releases,
            shape,
            transfer_id,
            flow,
        })
    }

    /// Return the shared payload-region locator.
    pub(crate) fn data_region(&self) -> &RegionHandle {
        &self.data
    }

    /// Return the shared release-descriptor-region locator.
    pub(crate) fn release_region(&self) -> &RegionHandle {
        &self.releases
    }

    /// Return the validated logical and resident stream dimensions.
    pub(crate) fn shape(&self) -> StreamShape {
        self.shape
    }

    /// Return the nonzero identifier authenticating this transfer.
    pub(crate) fn transfer_id(&self) -> u64 {
        self.transfer_id
    }

    /// Return the direction relative to the supervising parent.
    pub(crate) fn flow(&self) -> StreamFlow {
        self.flow
    }
}

/// Nonblocking socket liveness shared by the stream and control plane.
/// Payload bytes are never read from or written to this socket by this module.
pub(crate) struct SocketLiveness {
    socket: Control,
}

impl SocketLiveness {
    /// Wrap a connected socket as a nonblocking peer-liveness signal.
    pub(crate) fn new(socket: Control) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    /// Duplicate the socket-backed liveness handle without cancelling its peer.
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Self::new(self.socket.try_clone()?)
    }

    /// Wake both endpoints and fail active leases. Closing or shutting down the
    /// control connection is cancellation, never successful custody return.
    pub(crate) fn cancel(&self) {
        let _ = self.socket.shutdown(Shutdown::Both);
    }

    fn peer_closed(&self) -> io::Result<bool> {
        let mut byte = [std::mem::MaybeUninit::uninit(); 1];
        match self.socket.peek(&mut byte) {
            Ok(0) => Ok(true),
            Ok(_) => Ok(false),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl Drop for SocketLiveness {
    fn drop(&mut self) {
        // Dropping one clone must not cancel the shared connection. The owner
        // invokes `cancel` explicitly when any transport/control task fails.
    }
}

/// Result of one completed direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferReport {
    /// Total number of payload bytes transferred.
    pub total_bytes: u64,
    /// Number of payload chunks transferred.
    pub chunks: u64,
    /// Deterministic checksum of transferred bytes.
    pub checksum: u64,
    /// Maximum shared payload bytes resident during the transfer.
    pub resident_bytes: usize,
    /// Whether this endpoint successfully locked its shared regions in memory.
    pub memory_locked: bool,
}

/// One endpoint for a direction. The mutable transfer methods enforce the
/// one-producer/one-consumer ring invariant within a process.
pub(crate) struct Direction {
    data: Arena,
    releases: Arena,
    shape: StreamShape,
    transfer_id: u64,
    flow: StreamFlow,
    options: EndpointOptions,
}

// SAFETY: a Direction owns its mappings and mutable APIs preserve one local
// participant. Cross-process sharing is coordinated by atomics and leases.
unsafe impl Send for Direction {}

impl Direction {
    pub(crate) fn shape(&self) -> StreamShape {
        self.shape
    }

    /// Create both named regions and retain the owner mappings until `self` is
    /// dropped. The returned handle may be sent only over authenticated setup.
    pub(crate) fn create(
        config: DirectionConfig,
        role: DirectionRole,
    ) -> io::Result<(Self, DirectionHandle)> {
        config.validate()?;
        let descriptor_capacity = descriptor_capacity(config.shape.slots)?;
        let arena_direction = config.flow as u32;
        let (data, data_handle) = Arena::create(
            config.shape.slots,
            config.shape.chunk_bytes,
            descriptor_capacity,
            config.transfer_id,
            arena_direction,
            config.options.lock_memory,
        )?;
        let (releases, release_handle) = Arena::create(
            0,
            0,
            descriptor_capacity,
            config.transfer_id,
            arena_direction | RELEASE_ARENA_FLAG,
            config.options.lock_memory,
        )?;
        data.claim_role(role)?;
        let handle = DirectionHandle::from_parts(
            data_handle,
            release_handle,
            config.shape,
            config.transfer_id,
            config.flow,
        )?;
        Ok((
            Self {
                data,
                releases,
                shape: config.shape,
                transfer_id: config.transfer_id,
                flow: config.flow,
                options: config.options,
            },
            handle,
        ))
    }

    /// Reopen an owner-created direction in a separately spawned process.
    pub(crate) fn open(
        handle: &DirectionHandle,
        options: EndpointOptions,
        role: DirectionRole,
    ) -> io::Result<Self> {
        options.validate()?;
        handle.shape.validate()?;
        let descriptor_capacity = descriptor_capacity(handle.shape.slots)?;
        let arena_direction = handle.flow as u32;
        let data = Arena::open(
            &handle.data,
            handle.shape.slots,
            handle.shape.chunk_bytes,
            descriptor_capacity,
            handle.transfer_id,
            arena_direction,
            options.lock_memory,
        )?;
        let releases = Arena::open(
            &handle.releases,
            0,
            0,
            descriptor_capacity,
            handle.transfer_id,
            arena_direction | RELEASE_ARENA_FLAG,
            options.lock_memory,
        )?;
        data.claim_role(role)?;
        Ok(Self {
            data,
            releases,
            shape: handle.shape,
            transfer_id: handle.transfer_id,
            flow: handle.flow,
            options,
        })
    }

    /// Stream exactly `shape.total_bytes` from `reader`. Source failure
    /// cancels liveness so the peer cannot remain parked on an orphaned lease.
    pub(crate) fn produce<R: Read>(
        &mut self,
        liveness: &SocketLiveness,
        reader: &mut R,
    ) -> io::Result<TransferReport> {
        let result = produce(
            &mut self.data,
            &mut self.releases,
            liveness,
            self.shape,
            self.transfer_id,
            frame_flags(self.flow),
            self.options.timeout,
            reader,
        );
        self.finish_or_cancel(liveness, result)
    }

    /// Stream until `reader` reaches EOF, using `shape.total_bytes` as a hard
    /// ceiling. This is the output path for invocations whose result length is
    /// not knowable before plugin execution starts.
    pub(crate) fn produce_to_end<R: Read>(
        &mut self,
        liveness: &SocketLiveness,
        reader: &mut R,
    ) -> io::Result<TransferReport> {
        let result = produce_to_end(
            &mut self.data,
            &mut self.releases,
            liveness,
            self.shape,
            self.transfer_id,
            frame_flags(self.flow),
            self.options.timeout,
            reader,
        );
        self.finish_or_cancel(liveness, result)
    }

    /// Stream exactly `shape.total_bytes` into `writer`. Sink failure cancels
    /// liveness before returning and a slot is released only after `write_all`.
    pub(crate) fn consume<W: Write>(
        &mut self,
        liveness: &SocketLiveness,
        writer: &mut W,
    ) -> io::Result<TransferReport> {
        let result = consume(
            &mut self.data,
            &mut self.releases,
            liveness,
            self.shape,
            self.transfer_id,
            frame_flags(self.flow),
            self.options.timeout,
            writer,
        );
        self.finish_or_cancel(liveness, result)
    }

    /// Consume DATA frames until their validated COMPLETE frame, rejecting a
    /// cumulative length above `shape.total_bytes`.
    pub(crate) fn consume_to_end<W: Write>(
        &mut self,
        liveness: &SocketLiveness,
        writer: &mut W,
    ) -> io::Result<TransferReport> {
        let result = consume_to_end(
            &mut self.data,
            &mut self.releases,
            liveness,
            self.shape,
            self.transfer_id,
            frame_flags(self.flow),
            self.options.timeout,
            writer,
        );
        self.finish_or_cancel(liveness, result)
    }

    fn finish_or_cancel(
        &self,
        liveness: &SocketLiveness,
        result: io::Result<(u64, u64, u64)>,
    ) -> io::Result<TransferReport> {
        match result {
            Ok((checksum, total_bytes, chunks)) => Ok(TransferReport {
                total_bytes,
                chunks,
                checksum,
                resident_bytes: self.shape.resident_bytes(),
                memory_locked: self.data.is_locked() && self.releases.is_locked(),
            }),
            Err(error) => {
                liveness.cancel();
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Kind {
    Data = 1,
    Released = 2,
    Complete = 3,
}

impl TryFrom<u8> for Kind {
    type Error = io::Error;

    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Data),
            2 => Ok(Self::Released),
            3 => Ok(Self::Complete),
            _ => Err(invalid_data("unknown stream frame kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Frame {
    kind: Kind,
    flags: u8,
    transfer_id: u64,
    offset: u64,
    slot: u32,
    generation: u32,
    len: u32,
    checksum: u64,
}

impl Frame {
    fn encode(self) -> [u8; FRAME_LEN] {
        let mut bytes = [0u8; FRAME_LEN];
        bytes[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
        bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
        bytes[6] = self.kind as u8;
        bytes[7] = self.flags;
        bytes[8..16].copy_from_slice(&self.transfer_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.offset.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.slot.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.generation.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.len.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8; FRAME_LEN]) -> io::Result<Self> {
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != FRAME_MAGIC
            || u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != VERSION
            || bytes[36..40] != [0; 4]
        {
            return Err(invalid_data("stream frame header is invalid"));
        }
        Ok(Self {
            kind: Kind::try_from(bytes[6])?,
            flags: bytes[7],
            transfer_id: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            slot: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            generation: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            checksum: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        })
    }
}

struct Arena {
    data_ready: ProcessEvent,
    space_ready: ProcessEvent,
    region: SharedRegion,
    descriptor_capacity: usize,
    slots: usize,
    slot_len: usize,
    payload_offset: usize,
}

unsafe impl Send for Arena {}

impl Arena {
    #[allow(clippy::too_many_arguments)]
    fn create(
        slots: usize,
        slot_len: usize,
        descriptor_capacity: usize,
        transfer_id: u64,
        direction: u32,
        lock_memory: bool,
    ) -> io::Result<(Self, RegionHandle)> {
        let (len, payload_offset) = arena_layout(slots, slot_len, descriptor_capacity)?;
        let (mut region, handle) = SharedRegion::create(len)?;
        let ptr = region.as_ptr();
        unsafe {
            ptr.cast::<AtomicU32>().write(AtomicU32::new(0));
            write_u16(ptr, 4, VERSION);
            write_u16(ptr, 6, FRAME_LEN as u16);
            write_u32(ptr, 8, descriptor_capacity as u32);
            write_u32(ptr, 12, slots as u32);
            write_u32(ptr, 16, slot_len as u32);
            write_u32(ptr, 20, direction);
            write_u64(ptr, 24, transfer_id);
            write_u64(ptr, 32, payload_offset as u64);
            ptr.add(ROLE_CLAIMS_OFFSET)
                .cast::<AtomicU32>()
                .write(AtomicU32::new(0));
            ptr.add(HEAD_OFFSET)
                .cast::<AtomicU64>()
                .write(AtomicU64::new(0));
            ptr.add(TAIL_OFFSET)
                .cast::<AtomicU64>()
                .write(AtomicU64::new(0));
        }
        let data_ready =
            unsafe { ProcessEvent::create(ptr.add(DATA_EVENT_OFFSET).cast::<SharedEventState>())? };
        let space_ready = unsafe {
            ProcessEvent::create(ptr.add(SPACE_EVENT_OFFSET).cast::<SharedEventState>())?
        };
        if lock_memory {
            region.lock()?;
        }
        unsafe { &*ptr.cast::<AtomicU32>() }.store(ARENA_MAGIC, Ordering::Release);
        Ok((
            Self {
                data_ready,
                space_ready,
                region,
                descriptor_capacity,
                slots,
                slot_len,
                payload_offset,
            },
            handle,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn open(
        handle: &RegionHandle,
        slots: usize,
        slot_len: usize,
        expected_descriptor_capacity: usize,
        transfer_id: u64,
        direction: u32,
        lock_memory: bool,
    ) -> io::Result<Self> {
        let mut region = SharedRegion::open(handle)?;
        let ptr = region.as_ptr();
        if unsafe { &*ptr.cast::<AtomicU32>() }.load(Ordering::Acquire) != ARENA_MAGIC {
            return Err(invalid_data("shared stream arena is not initialized"));
        }
        let descriptor_capacity = unsafe { read_u32(ptr, 8) } as usize;
        let payload_offset = usize_from(unsafe { read_u64(ptr, 32) })?;
        let (expected_len, expected_payload_offset) =
            arena_layout(slots, slot_len, expected_descriptor_capacity)?;
        if unsafe { read_u16(ptr, 4) } != VERSION
            || unsafe { read_u16(ptr, 6) } as usize != FRAME_LEN
            || descriptor_capacity != expected_descriptor_capacity
            || unsafe { read_u32(ptr, 12) } as usize != slots
            || unsafe { read_u32(ptr, 16) } as usize != slot_len
            || unsafe { read_u32(ptr, 20) } != direction
            || unsafe { read_u64(ptr, 24) } != transfer_id
            || payload_offset != expected_payload_offset
            || handle.len() != expected_len
        {
            return Err(invalid_data("shared stream arena contract mismatch"));
        }
        let data_ready =
            unsafe { ProcessEvent::open(ptr.add(DATA_EVENT_OFFSET).cast::<SharedEventState>())? };
        let space_ready =
            unsafe { ProcessEvent::open(ptr.add(SPACE_EVENT_OFFSET).cast::<SharedEventState>())? };
        if lock_memory {
            region.lock()?;
        }
        Ok(Self {
            data_ready,
            space_ready,
            region,
            descriptor_capacity,
            slots,
            slot_len,
            payload_offset,
        })
    }

    fn claim_role(&self, role: DirectionRole) -> io::Result<()> {
        let claims = unsafe {
            &*self
                .region
                .as_ptr()
                .add(ROLE_CLAIMS_OFFSET)
                .cast::<AtomicU32>()
        };
        claim_direction_role(claims, role)
    }

    fn is_locked(&self) -> bool {
        self.region.is_locked()
    }

    fn push(
        &mut self,
        frame: Frame,
        liveness: &SocketLiveness,
        timeout: Duration,
    ) -> io::Result<()> {
        let head = self.head();
        let tail = self.tail();
        let tail_value = tail.load(Ordering::Relaxed);
        let started = Instant::now();
        while tail_value.wrapping_sub(head.load(Ordering::Acquire))
            >= self.descriptor_capacity as u64
        {
            let observed = self.space_ready.observe();
            if tail_value.wrapping_sub(head.load(Ordering::Acquire))
                < self.descriptor_capacity as u64
            {
                break;
            }
            wait_for_peer_event(liveness, &self.space_ready, observed, started, timeout)?;
        }
        let encoded = frame.encode();
        unsafe {
            std::ptr::copy_nonoverlapping(
                encoded.as_ptr(),
                self.descriptor_ptr(tail_value),
                FRAME_LEN,
            );
        }
        let published = tail_value + 1;
        tail.store(published, Ordering::Release);
        self.data_ready.notify(published)
    }

    fn pop(&mut self, liveness: &SocketLiveness, timeout: Duration) -> io::Result<Frame> {
        let head = self.head();
        let tail = self.tail();
        let head_value = head.load(Ordering::Relaxed);
        let started = Instant::now();
        while tail.load(Ordering::Acquire).wrapping_sub(head_value) == 0 {
            let observed = self.data_ready.observe();
            if tail.load(Ordering::Acquire).wrapping_sub(head_value) != 0 {
                break;
            }
            wait_for_peer_event(liveness, &self.data_ready, observed, started, timeout)?;
        }
        let mut encoded = [0u8; FRAME_LEN];
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.descriptor_ptr(head_value),
                encoded.as_mut_ptr(),
                FRAME_LEN,
            );
        }
        let published = head_value + 1;
        head.store(published, Ordering::Release);
        self.space_ready.notify(published)?;
        Frame::decode(&encoded)
    }

    fn write_slot<T>(
        &mut self,
        slot: usize,
        len: usize,
        write: impl FnOnce(&mut [u8]) -> io::Result<T>,
    ) -> io::Result<T> {
        self.check_slot(slot, len)?;
        let offset = self.payload_offset + slot * self.slot_len;
        let destination =
            unsafe { std::slice::from_raw_parts_mut(self.region.as_ptr().add(offset), len) };
        write(destination)
    }

    fn slot(&self, slot: usize, len: usize) -> io::Result<&[u8]> {
        self.check_slot(slot, len)?;
        let offset = self.payload_offset + slot * self.slot_len;
        Ok(unsafe { std::slice::from_raw_parts(self.region.as_ptr().add(offset), len) })
    }

    fn check_slot(&self, slot: usize, len: usize) -> io::Result<()> {
        if slot >= self.slots || len == 0 || len > self.slot_len {
            Err(invalid_data("stream slot or length is out of bounds"))
        } else {
            Ok(())
        }
    }

    fn head(&self) -> &AtomicU64 {
        unsafe { &*self.region.as_ptr().add(HEAD_OFFSET).cast::<AtomicU64>() }
    }

    fn tail(&self) -> &AtomicU64 {
        unsafe { &*self.region.as_ptr().add(TAIL_OFFSET).cast::<AtomicU64>() }
    }

    fn descriptor_ptr(&self, sequence: u64) -> *mut u8 {
        let index = sequence as usize & (self.descriptor_capacity - 1);
        unsafe { self.region.as_ptr().add(HEADER_LEN + index * FRAME_LEN) }
    }
}

#[allow(clippy::too_many_arguments)]
fn produce<R: Read>(
    data: &mut Arena,
    releases: &mut Arena,
    liveness: &SocketLiveness,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    timeout: Duration,
    reader: &mut R,
) -> io::Result<(u64, u64, u64)> {
    let chunks = shape.chunks();
    let mut released = 0u64;
    let mut aggregate = 0u64;
    for chunk_index in 0..chunks {
        if chunk_index >= shape.slots as u64 {
            let frame = releases.pop(liveness, timeout)?;
            validate_release(frame, released, shape, transfer_id, flags)?;
            released += 1;
        }
        let (offset, len, slot, generation) = chunk_meta(shape, chunk_index)?;
        let checksum = data.write_slot(slot, len, |destination| {
            reader.read_exact(destination)?;
            Ok(checksum(destination))
        })?;
        let frame = Frame {
            kind: Kind::Data,
            flags,
            transfer_id,
            offset,
            slot: slot as u32,
            generation,
            len: len as u32,
            checksum,
        };
        aggregate = fold_checksum(aggregate, frame);
        data.push(frame, liveness, timeout)?;
    }
    while released < chunks {
        let frame = releases.pop(liveness, timeout)?;
        validate_release(frame, released, shape, transfer_id, flags)?;
        released += 1;
    }
    let complete = Frame {
        kind: Kind::Complete,
        flags,
        transfer_id,
        offset: shape.total_bytes,
        slot: 0,
        generation: 0,
        len: 0,
        checksum: aggregate,
    };
    data.push(complete, liveness, timeout)?;
    validate_complete(
        releases.pop(liveness, timeout)?,
        shape,
        transfer_id,
        flags,
        aggregate,
    )?;
    Ok((aggregate, shape.total_bytes, chunks))
}

#[allow(clippy::too_many_arguments)]
fn consume<W: Write>(
    data: &mut Arena,
    releases: &mut Arena,
    liveness: &SocketLiveness,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    timeout: Duration,
    writer: &mut W,
) -> io::Result<(u64, u64, u64)> {
    let mut aggregate = 0u64;
    for chunk_index in 0..shape.chunks() {
        let frame = data.pop(liveness, timeout)?;
        validate_chunk(data, frame, chunk_index, shape, transfer_id, flags)?;
        writer.write_all(data.slot(frame.slot as usize, frame.len as usize)?)?;
        aggregate = fold_checksum(aggregate, frame);
        releases.push(
            Frame {
                kind: Kind::Released,
                ..frame
            },
            liveness,
            timeout,
        )?;
    }
    let complete = data.pop(liveness, timeout)?;
    validate_complete(complete, shape, transfer_id, flags, aggregate)?;
    releases.push(complete, liveness, timeout)?;
    Ok((aggregate, shape.total_bytes, shape.chunks()))
}

#[allow(clippy::too_many_arguments)]
fn produce_to_end<R: Read>(
    data: &mut Arena,
    releases: &mut Arena,
    liveness: &SocketLiveness,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    timeout: Duration,
    reader: &mut R,
) -> io::Result<(u64, u64, u64)> {
    let mut chunks = 0u64;
    let mut released = 0u64;
    let mut total = 0u64;
    let mut aggregate = 0u64;
    loop {
        if total == shape.total_bytes {
            let mut excess = [0u8; 1];
            loop {
                match reader.read(&mut excess) {
                    Ok(0) => break,
                    Ok(_) => return Err(invalid_data("stream exceeded its byte ceiling")),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                }
            }
            break;
        }
        if chunks >= shape.slots as u64 {
            let frame = releases.pop(liveness, timeout)?;
            validate_dynamic_release(frame, released, shape, transfer_id, flags)?;
            released += 1;
        }
        let slot = (chunks % shape.slots as u64) as usize;
        let generation = u32::try_from(chunks / shape.slots as u64 + 1)
            .map_err(|_| invalid_data("stream generation exhausted"))?;
        let capacity = usize::try_from((shape.total_bytes - total).min(shape.chunk_bytes as u64))
            .map_err(|_| invalid_data("stream chunk length does not fit usize"))?;
        let (len, checksum) = data.write_slot(slot, capacity, |destination| {
            let len = read_chunk(reader, destination)?;
            Ok((len, checksum(&destination[..len])))
        })?;
        if len == 0 {
            break;
        }
        let frame = Frame {
            kind: Kind::Data,
            flags,
            transfer_id,
            offset: total,
            slot: slot as u32,
            generation,
            len: len as u32,
            checksum,
        };
        aggregate = fold_checksum(aggregate, frame);
        data.push(frame, liveness, timeout)?;
        total += len as u64;
        chunks += 1;
    }
    while released < chunks {
        let frame = releases.pop(liveness, timeout)?;
        validate_dynamic_release(frame, released, shape, transfer_id, flags)?;
        released += 1;
    }
    let complete = Frame {
        kind: Kind::Complete,
        flags,
        transfer_id,
        offset: total,
        slot: 0,
        generation: 0,
        len: 0,
        checksum: aggregate,
    };
    data.push(complete, liveness, timeout)?;
    validate_dynamic_complete(
        releases.pop(liveness, timeout)?,
        shape,
        transfer_id,
        flags,
        total,
        aggregate,
    )?;
    Ok((aggregate, total, chunks))
}

#[allow(clippy::too_many_arguments)]
fn consume_to_end<W: Write>(
    data: &mut Arena,
    releases: &mut Arena,
    liveness: &SocketLiveness,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    timeout: Duration,
    writer: &mut W,
) -> io::Result<(u64, u64, u64)> {
    let mut chunks = 0u64;
    let mut total = 0u64;
    let mut aggregate = 0u64;
    loop {
        let frame = data.pop(liveness, timeout)?;
        if frame.kind == Kind::Complete {
            validate_dynamic_complete(frame, shape, transfer_id, flags, total, aggregate)?;
            releases.push(frame, liveness, timeout)?;
            return Ok((aggregate, total, chunks));
        }
        validate_dynamic_chunk(data, frame, chunks, total, shape, transfer_id, flags)?;
        writer.write_all(data.slot(frame.slot as usize, frame.len as usize)?)?;
        aggregate = fold_checksum(aggregate, frame);
        total += frame.len as u64;
        chunks += 1;
        releases.push(
            Frame {
                kind: Kind::Released,
                ..frame
            },
            liveness,
            timeout,
        )?;
    }
}

fn read_chunk(reader: &mut impl Read, destination: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0usize;
    while filled < destination.len() {
        match reader.read(&mut destination[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

fn validate_dynamic_chunk(
    data: &Arena,
    frame: Frame,
    chunk_index: u64,
    offset: u64,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
) -> io::Result<()> {
    let slot = (chunk_index % shape.slots as u64) as usize;
    let generation = u32::try_from(chunk_index / shape.slots as u64 + 1)
        .map_err(|_| invalid_data("stream generation exhausted"))?;
    let len = frame.len as usize;
    let within_ceiling = offset
        .checked_add(frame.len as u64)
        .is_some_and(|end| end <= shape.total_bytes);
    if frame.kind == Kind::Data
        && frame.flags == flags
        && frame.transfer_id == transfer_id
        && frame.offset == offset
        && frame.slot == slot as u32
        && frame.generation == generation
        && len > 0
        && len <= shape.chunk_bytes
        && within_ceiling
        && frame.checksum == checksum(data.slot(slot, len)?)
    {
        Ok(())
    } else {
        Err(invalid_data("streaming DATA frame or payload is invalid"))
    }
}

fn validate_dynamic_release(
    frame: Frame,
    chunk_index: u64,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
) -> io::Result<()> {
    let slot = (chunk_index % shape.slots as u64) as u32;
    let generation = u32::try_from(chunk_index / shape.slots as u64 + 1)
        .map_err(|_| invalid_data("stream generation exhausted"))?;
    if frame.kind == Kind::Released
        && frame.flags == flags
        && frame.transfer_id == transfer_id
        && frame.slot == slot
        && frame.generation == generation
        && frame.len > 0
        && frame.len as usize <= shape.chunk_bytes
        && frame
            .offset
            .checked_add(frame.len as u64)
            .is_some_and(|end| end <= shape.total_bytes)
    {
        Ok(())
    } else {
        Err(invalid_data("streaming RELEASED frame is invalid"))
    }
}

fn validate_dynamic_complete(
    frame: Frame,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    total: u64,
    aggregate: u64,
) -> io::Result<()> {
    if total <= shape.total_bytes
        && frame.kind == Kind::Complete
        && frame.flags == flags
        && frame.transfer_id == transfer_id
        && frame.offset == total
        && frame.slot == 0
        && frame.generation == 0
        && frame.len == 0
        && frame.checksum == aggregate
    {
        Ok(())
    } else {
        Err(invalid_data("streaming COMPLETE frame is invalid"))
    }
}

#[inline(always)]
fn validate_chunk(
    data: &Arena,
    frame: Frame,
    chunk_index: u64,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
) -> io::Result<()> {
    let (offset, len, slot, generation) = chunk_meta(shape, chunk_index)?;
    let expected = Frame {
        kind: Kind::Data,
        flags,
        transfer_id,
        offset,
        slot: slot as u32,
        generation,
        len: len as u32,
        checksum: checksum(data.slot(slot, len)?),
    };
    if frame == expected {
        Ok(())
    } else {
        Err(invalid_data("DATA frame or payload is invalid"))
    }
}

#[inline(always)]
fn validate_release(
    frame: Frame,
    chunk_index: u64,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
) -> io::Result<()> {
    let (offset, len, slot, generation) = chunk_meta(shape, chunk_index)?;
    if frame.kind == Kind::Released
        && frame.flags == flags
        && frame.transfer_id == transfer_id
        && frame.offset == offset
        && frame.slot == slot as u32
        && frame.generation == generation
        && frame.len == len as u32
    {
        Ok(())
    } else {
        Err(invalid_data("RELEASED frame is invalid"))
    }
}

#[inline(always)]
fn validate_complete(
    frame: Frame,
    shape: StreamShape,
    transfer_id: u64,
    flags: u8,
    aggregate: u64,
) -> io::Result<()> {
    if frame.kind == Kind::Complete
        && frame.flags == flags
        && frame.transfer_id == transfer_id
        && frame.offset == shape.total_bytes
        && frame.slot == 0
        && frame.generation == 0
        && frame.len == 0
        && frame.checksum == aggregate
    {
        Ok(())
    } else {
        Err(invalid_data("COMPLETE frame is invalid"))
    }
}

#[inline(always)]
fn chunk_meta(shape: StreamShape, chunk_index: u64) -> io::Result<(u64, usize, usize, u32)> {
    let offset = chunk_index
        .checked_mul(shape.chunk_bytes as u64)
        .ok_or_else(|| invalid_data("stream offset overflow"))?;
    let remaining = shape.total_bytes.saturating_sub(offset);
    let len = remaining.min(shape.chunk_bytes as u64) as usize;
    let slot = (chunk_index % shape.slots as u64) as usize;
    let generation = u32::try_from(chunk_index / shape.slots as u64 + 1)
        .map_err(|_| invalid_data("stream generation exhausted"))?;
    Ok((offset, len, slot, generation))
}

fn wait_for_peer_event(
    liveness: &SocketLiveness,
    event: &ProcessEvent,
    observed: u32,
    started: Instant,
    timeout: Duration,
) -> io::Result<()> {
    let elapsed = started.elapsed();
    if elapsed >= timeout {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process stream arena wait timed out",
        ));
    }
    let wait_for = (timeout - elapsed).min(LIVENESS_POLL_INTERVAL);
    if event.wait(observed, wait_for)? == WaitOutcome::TimedOut && liveness.peer_closed()? {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "process stream peer closed",
        ));
    }
    Ok(())
}

fn frame_flags(flow: StreamFlow) -> u8 {
    STREAM_FLAG | flow as u8
}

fn arena_layout(
    slots: usize,
    slot_len: usize,
    descriptor_capacity: usize,
) -> io::Result<(usize, usize)> {
    if (slots == 0) != (slot_len == 0)
        || descriptor_capacity < 2
        || !descriptor_capacity.is_power_of_two()
        || descriptor_capacity > u32::MAX as usize
    {
        return Err(invalid_input("stream arena dimensions are invalid"));
    }
    let descriptors = descriptor_capacity
        .checked_mul(FRAME_LEN)
        .ok_or_else(|| invalid_input("descriptor bytes overflow"))?;
    let payload_offset = align_up(
        HEADER_LEN
            .checked_add(descriptors)
            .ok_or_else(|| invalid_input("arena offset overflow"))?,
        CACHE_LINE,
    )?;
    let payload = slots
        .checked_mul(slot_len)
        .ok_or_else(|| invalid_input("arena payload bytes overflow"))?;
    let len = payload_offset
        .checked_add(payload)
        .filter(|&bytes| bytes <= isize::MAX as usize)
        .ok_or_else(|| invalid_input("arena length overflow"))?;
    Ok((len, payload_offset))
}

fn descriptor_capacity(slots: usize) -> io::Result<usize> {
    slots
        .checked_mul(2)
        .and_then(|value| value.checked_add(2))
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(|| invalid_input("descriptor capacity overflow"))
}

fn align_up(value: usize, alignment: usize) -> io::Result<usize> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| invalid_input("alignment overflow"))
}

#[inline(always)]
fn checksum(payload: &[u8]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    if payload.len() >= 256 && std::arch::is_x86_feature_detected!("avx2") {
        return unsafe { checksum_avx2(payload) };
    }
    checksum_scalar(payload)
}

#[inline]
fn checksum_scalar(payload: &[u8]) -> u64 {
    let mut sums = [
        0xCBF2_9CE4_8422_2325u64,
        0x9E37_79B9_7F4A_7C15,
        0xD1B5_4A32_D192_ED03,
        0xA076_1D64_78BD_642F,
    ];
    let (chunks, remainder) = payload.as_chunks::<32>();
    for chunk in chunks {
        for (lane, sum) in sums.iter_mut().enumerate() {
            let start = lane * 8;
            let word = u64::from_le_bytes(chunk[start..start + 8].try_into().unwrap());
            *sum = sum.wrapping_add(word ^ (word >> (lane + 1)));
        }
    }
    let mut tail = 0u64;
    for chunk in remainder.chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        tail = tail.wrapping_add(u64::from_le_bytes(word));
    }
    finalize_checksum(sums, tail)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn checksum_avx2(payload: &[u8]) -> u64 {
    use core::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_loadu_si256, _mm256_set_epi64x, _mm256_setzero_si256,
        _mm256_srlv_epi64, _mm256_storeu_si256, _mm256_xor_si256,
    };
    let shifts = _mm256_set_epi64x(4, 3, 2, 1);
    let mut sums0 = _mm256_set_epi64x(
        0xA076_1D64_78BD_642Fu64 as i64,
        0xD1B5_4A32_D192_ED03u64 as i64,
        0x9E37_79B9_7F4A_7C15u64 as i64,
        0xCBF2_9CE4_8422_2325u64 as i64,
    );
    let mut sums1 = _mm256_setzero_si256();
    let mut sums2 = _mm256_setzero_si256();
    let mut sums3 = _mm256_setzero_si256();
    let full_bytes = payload.len() / 32 * 32;
    let mut offset = 0usize;
    while offset + 128 <= full_bytes {
        let source = unsafe { payload.as_ptr().add(offset) };
        let words0 = unsafe { _mm256_loadu_si256(source.cast::<__m256i>()) };
        let words1 = unsafe { _mm256_loadu_si256(source.add(32).cast::<__m256i>()) };
        let words2 = unsafe { _mm256_loadu_si256(source.add(64).cast::<__m256i>()) };
        let words3 = unsafe { _mm256_loadu_si256(source.add(96).cast::<__m256i>()) };
        sums0 = _mm256_add_epi64(
            sums0,
            _mm256_xor_si256(words0, _mm256_srlv_epi64(words0, shifts)),
        );
        sums1 = _mm256_add_epi64(
            sums1,
            _mm256_xor_si256(words1, _mm256_srlv_epi64(words1, shifts)),
        );
        sums2 = _mm256_add_epi64(
            sums2,
            _mm256_xor_si256(words2, _mm256_srlv_epi64(words2, shifts)),
        );
        sums3 = _mm256_add_epi64(
            sums3,
            _mm256_xor_si256(words3, _mm256_srlv_epi64(words3, shifts)),
        );
        offset += 128;
    }
    while offset < full_bytes {
        let words = unsafe { _mm256_loadu_si256(payload.as_ptr().add(offset).cast::<__m256i>()) };
        sums0 = _mm256_add_epi64(
            sums0,
            _mm256_xor_si256(words, _mm256_srlv_epi64(words, shifts)),
        );
        offset += 32;
    }
    let sums = _mm256_add_epi64(
        _mm256_add_epi64(sums0, sums1),
        _mm256_add_epi64(sums2, sums3),
    );
    let mut lanes = [0u64; 4];
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast::<__m256i>(), sums) };
    let mut tail = 0u64;
    for chunk in payload[full_bytes..].chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        tail = tail.wrapping_add(u64::from_le_bytes(word));
    }
    finalize_checksum(lanes, tail)
}

#[inline]
fn finalize_checksum(sums: [u64; 4], tail: u64) -> u64 {
    let mut sum = sums[0]
        ^ sums[1].rotate_left(13)
        ^ sums[2].rotate_left(29)
        ^ sums[3].rotate_left(47)
        ^ tail;
    sum ^= sum >> 30;
    sum = sum.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    sum ^= sum >> 27;
    sum = sum.wrapping_mul(0x94D0_49BB_1331_11EB);
    sum ^ (sum >> 31)
}

#[inline]
fn fold_checksum(aggregate: u64, frame: Frame) -> u64 {
    aggregate.rotate_left(7) ^ frame.checksum ^ frame.offset.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn usize_from(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_input("value does not fit usize"))
}

unsafe fn write_u16(ptr: *mut u8, offset: usize, value: u16) {
    unsafe { std::ptr::copy_nonoverlapping(value.to_le_bytes().as_ptr(), ptr.add(offset), 2) }
}

unsafe fn write_u32(ptr: *mut u8, offset: usize, value: u32) {
    unsafe { std::ptr::copy_nonoverlapping(value.to_le_bytes().as_ptr(), ptr.add(offset), 4) }
}

unsafe fn write_u64(ptr: *mut u8, offset: usize, value: u64) {
    unsafe { std::ptr::copy_nonoverlapping(value.to_le_bytes().as_ptr(), ptr.add(offset), 8) }
}

unsafe fn read_u16(ptr: *mut u8, offset: usize) -> u16 {
    let mut bytes = [0u8; 2];
    unsafe { std::ptr::copy_nonoverlapping(ptr.add(offset), bytes.as_mut_ptr(), 2) };
    u16::from_le_bytes(bytes)
}

unsafe fn read_u32(ptr: *mut u8, offset: usize) -> u32 {
    let mut bytes = [0u8; 4];
    unsafe { std::ptr::copy_nonoverlapping(ptr.add(offset), bytes.as_mut_ptr(), 4) };
    u32::from_le_bytes(bytes)
}

unsafe fn read_u64(ptr: *mut u8, offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    unsafe { std::ptr::copy_nonoverlapping(ptr.add(offset), bytes.as_mut_ptr(), 8) };
    u64::from_le_bytes(bytes)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn socket_pair() -> (SocketLiveness, SocketLiveness) {
        let (client, server) = Control::pair().unwrap();
        (
            SocketLiveness::new(client).unwrap(),
            SocketLiveness::new(server).unwrap(),
        )
    }

    fn round_trip(bytes: Vec<u8>, shape: StreamShape) {
        let config = DirectionConfig {
            shape,
            transfer_id: 77,
            flow: StreamFlow::ParentToWorker,
            options: EndpointOptions {
                timeout: Duration::from_secs(2),
                ..EndpointOptions::default()
            },
        };
        let (mut producer, handle) = Direction::create(config, DirectionRole::Producer).unwrap();
        let mut consumer =
            Direction::open(&handle, config.options, DirectionRole::Consumer).unwrap();
        let (producer_live, consumer_live) = socket_pair();
        let expected = bytes.clone();
        let producer_thread = std::thread::spawn(move || {
            producer
                .produce(&producer_live, &mut Cursor::new(bytes))
                .unwrap()
        });
        let consumer_thread = std::thread::spawn(move || {
            let mut output = Vec::new();
            let report = consumer.consume(&consumer_live, &mut output).unwrap();
            (report, output)
        });
        let produced = producer_thread.join().unwrap();
        let (consumed, output) = consumer_thread.join().unwrap();
        assert_eq!(output, expected);
        assert_eq!(produced.checksum, consumed.checksum);
        assert_eq!(produced.chunks, shape.chunks());
    }

    fn streaming_round_trip(bytes: Vec<u8>, shape: StreamShape) {
        let config = DirectionConfig {
            shape,
            transfer_id: 88,
            flow: StreamFlow::WorkerToParent,
            options: EndpointOptions {
                timeout: Duration::from_secs(2),
                ..EndpointOptions::default()
            },
        };
        let (mut producer, handle) = Direction::create(config, DirectionRole::Producer).unwrap();
        let mut consumer =
            Direction::open(&handle, config.options, DirectionRole::Consumer).unwrap();
        let (producer_live, consumer_live) = socket_pair();
        let expected = bytes.clone();
        let producer_thread = std::thread::spawn(move || {
            producer
                .produce_to_end(&producer_live, &mut Cursor::new(bytes))
                .unwrap()
        });
        let consumer_thread = std::thread::spawn(move || {
            let mut output = Vec::new();
            let report = consumer
                .consume_to_end(&consumer_live, &mut output)
                .unwrap();
            (report, output)
        });
        let produced = producer_thread.join().unwrap();
        let (consumed, output) = consumer_thread.join().unwrap();
        assert_eq!(output, expected);
        assert_eq!(produced, consumed);
        assert_eq!(produced.total_bytes, output.len() as u64);
    }

    #[test]
    fn partial_tail_and_slot_generations_round_trip() {
        let bytes = (0..65_793).map(|value| value as u8).collect();
        round_trip(
            bytes,
            StreamShape {
                total_bytes: 65_793,
                chunk_bytes: 4_097,
                slots: 3,
            },
        );
    }

    #[test]
    fn empty_stream_has_a_terminal_custody_exchange() {
        round_trip(
            Vec::new(),
            StreamShape {
                total_bytes: 0,
                chunk_bytes: 4_096,
                slots: 2,
            },
        );
    }

    #[test]
    fn unknown_length_stream_stops_at_eof_below_its_ceiling() {
        let bytes = (0..70_013).map(|value| (value * 17) as u8).collect();
        streaming_round_trip(
            bytes,
            StreamShape {
                total_bytes: 1_000_000,
                chunk_bytes: 8_191,
                slots: 2,
            },
        );
    }

    #[test]
    fn unknown_length_stream_rejects_one_byte_over_its_ceiling() {
        let shape = StreamShape {
            total_bytes: 4_096,
            chunk_bytes: 1_024,
            slots: 2,
        };
        let config = DirectionConfig {
            shape,
            transfer_id: 99,
            flow: StreamFlow::WorkerToParent,
            options: EndpointOptions {
                timeout: Duration::from_secs(2),
                ..EndpointOptions::default()
            },
        };
        let (mut producer, handle) = Direction::create(config, DirectionRole::Producer).unwrap();
        let mut consumer =
            Direction::open(&handle, config.options, DirectionRole::Consumer).unwrap();
        let (producer_live, consumer_live) = socket_pair();
        let producer_thread = std::thread::spawn(move || {
            producer.produce_to_end(&producer_live, &mut Cursor::new(vec![7u8; 4_097]))
        });
        let consumer_thread =
            std::thread::spawn(move || consumer.consume_to_end(&consumer_live, &mut Vec::new()));
        assert_eq!(
            producer_thread.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(consumer_thread.join().unwrap().is_err());
    }

    #[test]
    #[ignore = "release performance and multi-GiB bounded-stream gate"]
    fn unknown_length_multi_gib_stream_keeps_a_one_mib_resident_window() {
        let total = 2 * 1024 * 1024 * 1024 + 123;
        let shape = StreamShape {
            total_bytes: total,
            chunk_bytes: 256 * 1024,
            slots: 4,
        };
        let config = DirectionConfig {
            shape,
            transfer_id: 100,
            flow: StreamFlow::WorkerToParent,
            options: EndpointOptions {
                timeout: Duration::from_secs(10),
                ..EndpointOptions::default()
            },
        };
        let (mut producer, handle) = Direction::create(config, DirectionRole::Producer).unwrap();
        let mut consumer =
            Direction::open(&handle, config.options, DirectionRole::Consumer).unwrap();
        let (producer_live, consumer_live) = socket_pair();
        let started = Instant::now();
        let producer_thread = std::thread::spawn(move || {
            let mut input = io::repeat(0x5a).take(total);
            producer.produce_to_end(&producer_live, &mut input).unwrap()
        });
        let consumer_thread = std::thread::spawn(move || {
            consumer
                .consume_to_end(&consumer_live, &mut io::sink())
                .unwrap()
        });
        let produced = producer_thread.join().unwrap();
        let consumed = consumer_thread.join().unwrap();
        let elapsed = started.elapsed();
        assert_eq!(produced, consumed);
        assert_eq!(produced.total_bytes, total);
        assert_eq!(produced.resident_bytes, 1024 * 1024);
        eprintln!(
            "production dynamic stream: {:.2} GiB/s, {} logical bytes, {} resident bytes",
            total as f64 / elapsed.as_secs_f64() / 1024.0_f64.powi(3),
            total,
            produced.resident_bytes
        );
    }

    #[test]
    fn claim_state_allows_each_role_exactly_once() {
        let claims = std::sync::Arc::new(AtomicU32::new(0));
        claim_direction_role(&claims, DirectionRole::Producer).unwrap();
        assert_eq!(
            claim_direction_role(&claims, DirectionRole::Producer)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );

        let mut contenders = Vec::new();
        for _ in 0..2 {
            let claims = std::sync::Arc::clone(&claims);
            contenders.push(std::thread::spawn(move || {
                claim_direction_role(&claims, DirectionRole::Consumer)
                    .map(|()| true)
                    .or_else(|error| {
                        if error.kind() == io::ErrorKind::AlreadyExists {
                            Ok(false)
                        } else {
                            Err(error)
                        }
                    })
                    .unwrap()
            }));
        }
        let winners = contenders
            .into_iter()
            .map(|contender| usize::from(contender.join().unwrap()))
            .sum::<usize>();
        assert_eq!(winners, 1);
        assert_eq!(claims.load(Ordering::Acquire), ROLE_CLAIM_MASK);

        let invalid = AtomicU32::new(ROLE_CLAIM_MASK << 1);
        assert_eq!(
            claim_direction_role(&invalid, DirectionRole::Producer)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn mapped_direction_roles_are_permanent() {
        let config = DirectionConfig {
            shape: StreamShape {
                total_bytes: 4,
                chunk_bytes: 4,
                slots: 1,
            },
            transfer_id: 101,
            flow: StreamFlow::ParentToWorker,
            options: EndpointOptions::default(),
        };
        let (_producer, handle) = Direction::create(config, DirectionRole::Producer).unwrap();
        assert_eq!(
            Direction::open(&handle, config.options, DirectionRole::Producer)
                .err()
                .expect("duplicate producer role accepted")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        let consumer = Direction::open(&handle, config.options, DirectionRole::Consumer).unwrap();
        drop(consumer);
        assert_eq!(
            Direction::open(&handle, config.options, DirectionRole::Consumer)
                .err()
                .expect("duplicate consumer role accepted")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
    }

    #[test]
    fn dimensions_and_handle_parts_are_validated() {
        let invalid = StreamShape {
            total_bytes: 1,
            chunk_bytes: 0,
            slots: 1,
        };
        assert!(invalid.validate().is_err());
        let handle = RegionHandle::from_parts("test".into(), 10).unwrap();
        assert!(
            DirectionHandle::from_parts(
                handle.clone(),
                handle,
                invalid,
                1,
                StreamFlow::ParentToWorker,
            )
            .is_err()
        );
    }

    #[test]
    fn frame_round_trip_is_native_layout_independent() {
        let frame = Frame {
            kind: Kind::Data,
            flags: frame_flags(StreamFlow::WorkerToParent),
            transfer_id: 9,
            offset: 65_537,
            slot: 3,
            generation: 7,
            len: 257,
            checksum: 0xDEAD_BEEF_CAFE_BABE,
        };
        assert_eq!(Frame::decode(&frame.encode()).unwrap(), frame);
    }
}
