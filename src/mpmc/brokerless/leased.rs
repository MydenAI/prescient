//! Bounded, reusable payload ownership for fixed brokerless MPMC.
//!
//! `capacity` counts **all** payload slots per producer, including filling,
//! queued, consumer-held and returned blocks. `batch` is the maximum values per
//! block; capacity must be a positive multiple of batch. When capacity is omitted,
//! `open()` selects a payload- and topology-aware pool; batch defaults to 64.
//! Payload allocations are made by `open()` and never grow. Descriptor/return
//! metadata uses O(producers * consumers * blocks) space in addition to payloads.
//!
//! A producer reserves a write lease, initializes values and explicitly commits.
//! A consumer receives a read lease to the same allocation, after the forward
//! ring claim has been released. Dropping it returns the block to its origin.
//! Remaining values are dropped on producer reclamation/reuse or endpoint
//! teardown, not necessarily on the consumer thread. A held lease withholds
//! that block only; it never prevents a peer from taking another ready block.
//!
//! Endpoints have fixed membership and are not clonable. Leases borrow their
//! endpoint exclusively, so one endpoint cannot hoard leases or concurrently
//! write an SPSC return lane. Forgetting a lease may leak bounded pool resources;
//! it cannot make borrowed memory reusable. The transport is local, synchronous,
//! and Ring-backed; leases never cross a process or asynchronous boundary.
//!
//! ```
//! use prescient::mpmc::brokerless::leased;
//! let (mut producers, mut consumers) = leased::channel::<String>()
//!     .capacity(8).batch(2).open().unwrap();
//! let mut write = producers[0].reserve().unwrap();
//! write.push("hello".to_owned()).unwrap();
//! write.commit().unwrap();
//! let read = consumers[0].recv().unwrap();
//! assert_eq!(read.as_slice(), ["hello"]);
//! drop(read);
//! assert!(producers[0].flush());
//! ```
//!
//! Leases cannot outlive their exclusive endpoint borrow:
//! ```compile_fail
//! use prescient::mpmc::brokerless::leased;
//! let (mut tx, _) = leased::channel::<u64>().open().unwrap();
//! let write = tx[0].reserve().unwrap();
//! drop(tx);
//! drop(write);
//! ```
//! Read storage cannot survive releasing its lease:
//! ```compile_fail
//! use prescient::mpmc::brokerless::leased;
//! let (_, mut rx) = leased::channel::<u64>().open().unwrap();
//! let read = rx[0].recv().unwrap();
//! let values = read.as_slice();
//! drop(read);
//! println!("{values:?}");
//! ```
//!
//! Fixed endpoints cannot be cloned into additional return-lane writers:
//! ```compile_fail
//! let (tx, _) = prescient::mpmc::brokerless::leased::channel::<u64>().open().unwrap();
//! let duplicate = tx[0].clone();
//! ```
//!
use crate::OpenError;
use crate::backend::{Backend, Ring, ring};
use std::alloc::Layout;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

/// Declare a fixed brokerless channel of reusable, leased payload blocks.
pub fn channel<T>() -> crate::Channel<
    T,
    crate::topology::Mpmc<crate::engine::Claim>,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
    crate::routing::RoundRobin,
    crate::transport::Local,
    crate::codec::Leased,
> {
    super::channel().leased()
}

struct Block<T> {
    values: Vec<T>,
    origin: usize,
}

/// A single-owner producer with a finite, preallocated payload pool.
pub struct Producer<T: Send> {
    forward: super::Producer<Block<T>>,
    returns: Vec<ring::Receiver<Block<T>>>,
    free: Vec<Block<T>>,
    in_flight: usize,
    cursor: usize,
    batch: usize,
}

/// One competing consumer. Each lease owns an entire published block.
pub struct Consumer<T: Send> {
    forward: super::Consumer<Block<T>>,
    returns: Vec<ring::Sender<Block<T>>>,
}

/// An exclusive, unpublished payload block. Drop rolls back publication.
#[must_use = "commit the initialized block, or drop the lease to roll it back"]
pub struct WriteLease<'a, T: Send> {
    producer: &'a mut Producer<T>,
    block: Option<Block<T>>,
}

/// A published payload block, returned to its producer on drop.
#[must_use = "dropping the read lease immediately recycles its payload block"]
pub struct ReadLease<'a, T: Send> {
    consumer: &'a mut Consumer<T>,
    block: Option<Block<T>>,
}

fn allocation<T>(count: usize) -> Result<(), OpenError> {
    Layout::array::<T>(count)
        .map(|_| ())
        .map_err(|_| OpenError::Invalid("leased channel allocation shape overflows"))
}

type Endpoints<T> = (Vec<Producer<T>>, Vec<Consumer<T>>);

pub(crate) fn open<T: Send>(
    producers: usize,
    consumers: usize,
    capacity: usize,
    batch: usize,
) -> Result<Endpoints<T>, OpenError> {
    if producers == 0
        || consumers == 0
        || batch == 0
        || capacity < batch
        || !capacity.is_multiple_of(batch)
    {
        return Err(OpenError::Invalid(
            "leased channels require non-zero workers and capacity divisible by batch",
        ));
    }
    let blocks = capacity / batch;
    let slots = blocks
        .checked_next_power_of_two()
        .ok_or(OpenError::Invalid("leased descriptor capacity overflows"))?;
    let payloads = producers
        .checked_mul(capacity)
        .ok_or(OpenError::Invalid("leased payload count overflows"))?;
    let descriptors = producers
        .checked_mul(consumers)
        .and_then(|n| n.checked_mul(slots))
        .ok_or(OpenError::Invalid("leased return topology overflows"))?;
    allocation::<T>(batch)?;
    allocation::<T>(payloads)?;
    allocation::<Block<T>>(descriptors)?;
    allocation::<Producer<T>>(producers)?;
    allocation::<Consumer<T>>(consumers)?;
    let (txs, rxs) = super::channel::<Block<T>>()
        .producers(producers)
        .consumers(consumers)
        .capacity(slots)
        .batch(1)
        .open()?;
    let mut receivers = rxs
        .into_iter()
        .map(|forward| Consumer {
            forward,
            returns: Vec::with_capacity(producers),
        })
        .collect::<Vec<_>>();
    let senders = txs
        .into_iter()
        .enumerate()
        .map(|(origin, forward)| {
            let returns = receivers
                .iter_mut()
                .map(|receiver| {
                    let (tx, rx) = Ring::channel(slots);
                    receiver.returns.push(tx);
                    rx
                })
                .collect();
            Producer {
                forward,
                returns,
                free: (0..blocks)
                    .map(|_| Block {
                        values: Vec::with_capacity(batch),
                        origin,
                    })
                    .collect(),
                in_flight: 0,
                cursor: 0,
                batch,
            }
        })
        .collect();
    Ok((senders, receivers))
}

#[inline]
fn pause(spins: &mut u32) {
    *spins = spins.wrapping_add(1);
    if *spins < 64 {
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
        *spins = 0;
    }
}

impl<T: Send> Producer<T> {
    /// No consumers remain. Successful publication is not a delivery acknowledgement.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.forward.shared.live_consumers.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn returned(&mut self) -> Option<Block<T>> {
        for _ in 0..self.returns.len() {
            let index = crate::round_robin::next(&mut self.cursor, self.returns.len());
            if let Some(block) = self.returns[index].try_pop() {
                self.in_flight -= 1;
                return Some(block);
            }
        }
        None
    }

    /// Try to reserve one empty block. None means pool exhaustion or disconnection.
    /// Existing values are destroyed only after the rollback guard owns the block.
    #[inline]
    pub fn try_reserve(&mut self) -> Option<WriteLease<'_, T>> {
        if self.is_disconnected() {
            return None;
        }
        // Reuse ready storage before expanding into untouched pool capacity.
        let block = self.returned().or_else(|| self.free.pop())?;
        let mut lease = WriteLease {
            producer: self,
            block: Some(block),
        };
        lease.block.as_mut().unwrap().values.clear();
        Some(lease)
    }

    /// Reserve a block, spinning/yielding for recycling; None once consumers are gone.
    pub fn reserve(&mut self) -> Option<WriteLease<'_, T>> {
        let mut spins = 0;
        let block = loop {
            if self.is_disconnected() {
                return None;
            }
            if let Some(block) = self.returned().or_else(|| self.free.pop()) {
                break block;
            }
            pause(&mut spins);
        };
        let mut lease = WriteLease {
            producer: self,
            block: Some(block),
        };
        lease.block.as_mut().unwrap().values.clear();
        Some(lease)
    }

    /// Reclaim all currently returned blocks and destroy their remaining values.
    /// Storage is restored before Drop runs, so a destructor panic cannot lose it.
    pub fn reclaim(&mut self) -> usize {
        let mut reclaimed = 0;
        while let Some(block) = self.returned() {
            self.free.push(block);
            self.free.last_mut().unwrap().values.clear();
            reclaimed += 1;
        }
        reclaimed
    }

    /// Wait for every committed block to return, then destroy remaining values.
    /// Returns false if consumers disappear while blocks remain outstanding.
    /// Caller-held read leases can delay this operation; it is never called by Drop.
    pub fn flush(&mut self) -> bool {
        let mut spins = 0;
        loop {
            self.reclaim();
            if self.in_flight == 0 {
                return true;
            }
            if self.is_disconnected() {
                // Observe releases preceding the final consumer's teardown.
                self.reclaim();
                return self.in_flight == 0;
            }
            pause(&mut spins);
        }
    }
}

impl<T: Send> WriteLease<'_, T> {
    /// Initialized values, which remain private until commit.
    #[inline]
    /// Borrow the initialized values in this lease.
    pub fn as_slice(&self) -> &[T] {
        &self.block.as_ref().unwrap().values
    }
    #[inline]
    /// Mutably borrow the initialized values in this lease.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.block.as_mut().unwrap().values
    }
    #[inline]
    /// Return the number of initialized values in this lease.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
    #[inline]
    /// Return true when the lease contains no initialized values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Maximum values in this block, independent of allocator rounding or ZST capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.producer.batch
    }

    /// Append one value without growing the pool. Returns it unchanged when full.
    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.len() == self.capacity() {
            return Err(value);
        }
        self.block.as_mut().unwrap().values.push(value);
        Ok(())
    }

    /// Copy an entire slice directly into the payload allocation, or leave it unchanged.
    #[inline]
    pub fn extend_from_slice(&mut self, values: &[T]) -> bool
    where
        T: Copy,
    {
        if values.len() > self.capacity() - self.len() {
            return false;
        }
        self.block
            .as_mut()
            .unwrap()
            .values
            .extend_from_slice(values);
        true
    }

    /// Bounded, uninitialized destination slots for direct construction.
    /// Merely writing a slot does not transfer its drop responsibility to this lease.
    #[inline]
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<T>] {
        let remaining = self.capacity() - self.len();
        &mut self.block.as_mut().unwrap().values.spare_capacity_mut()[..remaining]
    }

    /// Adopt an initialized prefix of the current spare capacity.
    ///
    /// # Safety
    ///
    /// The first `additional` spare slots must each contain one fully initialized,
    /// exclusively owned T. They must not subsequently be read or dropped through
    /// an old pointer. Adopt each completed prefix before any operation that may
    /// panic if those values require destruction. Remaining spare slots stay unowned.
    ///
    /// Panics without changing the length if additional exceeds remaining capacity.
    #[inline]
    pub unsafe fn advance_initialized(&mut self, additional: usize) {
        assert!(
            additional <= self.capacity() - self.len(),
            "lease initialized length exceeds capacity"
        );
        let values = &mut self.block.as_mut().unwrap().values;
        // SAFETY: caller guarantees initialization/ownership; checked bound also
        // proves addition cannot overflow and the allocation cannot be exceeded.
        unsafe { values.set_len(values.len() + additional) };
    }

    /// Publish the initialized prefix. Empty commits are successful no-ops.
    /// On failure the returned lease still owns every unpublished value.
    /// Accepted blocks need not have been consumed; flush waits for their return.
    #[inline]
    pub fn commit(mut self) -> Result<usize, Self> {
        let count = self.len();
        if count == 0 {
            return Ok(0);
        }
        if self.producer.is_disconnected() {
            return Err(self);
        }
        let block = self.block.take().unwrap();
        match self.producer.forward.try_send(block) {
            Ok(()) => {
                self.producer.in_flight += 1;
                Ok(count)
            }
            Err(block) => {
                self.block = Some(block);
                Err(self)
            }
        }
    }
}

impl<T: Send> Drop for WriteLease<'_, T> {
    fn drop(&mut self) {
        if let Some(block) = self.block.take() {
            // The lease came from this finite free list; pushing cannot allocate.
            // Preserve ownership before any future clear can invoke user Drop.
            self.producer.free.push(block);
        }
    }
}

impl<T: Send> Consumer<T> {
    /// True after every producer is dropped; ready blocks must still be drained.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.forward.is_disconnected()
    }
    // A lease transfers one descriptor, not a batch of payload values. Move it
    // directly into local ownership; drain releases the shared claim before this
    // function returns. Its callback cannot invoke user code or overwrite a value.
    #[inline]
    fn take_block(&mut self) -> Option<Block<T>> {
        let mut block = None;
        self.forward.drain(1, |value| block = Some(value));
        block
    }

    #[inline]
    /// Try to acquire one published block without waiting.
    pub fn try_recv(&mut self) -> Option<ReadLease<'_, T>> {
        let block = self.take_block()?;
        Some(ReadLease {
            consumer: self,
            block: Some(block),
        })
    }
    /// Spin/yield until a published block is ready, or return None after disconnect.
    pub fn recv(&mut self) -> Option<ReadLease<'_, T>> {
        let mut spins = 0;
        let block = loop {
            if let Some(block) = self.take_block() {
                break block;
            }
            if self.forward.is_disconnected() {
                // Acquire the final producer teardown before one last full scan.
                break self.take_block()?;
            }
            pause(&mut spins);
        };
        Some(ReadLease {
            consumer: self,
            block: Some(block),
        })
    }
}

impl<T: Send> ReadLease<'_, T> {
    #[inline]
    /// Borrow the initialized values in this lease.
    pub fn as_slice(&self) -> &[T] {
        &self.block.as_ref().unwrap().values
    }
    #[inline]
    /// Mutably borrow the initialized values in this lease.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.block.as_mut().unwrap().values
    }
    #[inline]
    /// Return the number of initialized values in this lease.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
    #[inline]
    /// Return true when the lease contains no initialized values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Move values out in FIFO order. Remaining values are dropped with the drain.
    pub fn drain(&mut self) -> std::vec::Drain<'_, T> {
        self.block.as_mut().unwrap().values.drain(..)
    }
    /// Explicitly return the block; equivalent to dropping the lease.
    pub fn release(self) {}
}

impl<T: Send> Drop for ReadLease<'_, T> {
    fn drop(&mut self) {
        if let Some(block) = self.block.take() {
            // This consumer exclusively owns its SPSC return writers. Each lane
            // can hold the producer's ENTIRE pool, so a live producer's lane has
            // room for this uniquely owned block. Drop locally if its producer
            // is already gone. A racing producer teardown is safely reclaimed by
            // the return ring's final owner; try_push itself only checks capacity.
            let origin = block.origin;
            let lane = &mut self.consumer.returns[origin];
            if !lane.is_consumer_gone() {
                let _ = lane.try_push(block);
            }
        }
    }
}

impl<T: Send> fmt::Debug for WriteLease<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteLease")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}
impl<T: Send> fmt::Debug for ReadLease<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadLease")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}
