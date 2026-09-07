//! MPSC: many producers → **one** consumer, sharded per-producer.
//!
//! Each producer gets its own SPSC transport; the single consumer gathers across
//! them round-robin. Pick the tier by producer lifecycle:
//!
//! * [`fixed`] — exactly N producers, minted up front. No runtime membership coordination.
//! * [`pool`] — up to 64 producers claimed from a lock-free bitmap; slots recycle.
//! * [`dynamic`] — unbounded producers minted at runtime from a registrar.
//!
//! All tiers share the same [`Receiver`] (recv / try_recv_many / drain) and
//! [`Producer`] (try_send / send / send_batch). See the crate docs for the
//! waiter (tail-latency) and batching knobs.

pub(crate) mod chan;
pub mod dynamic;
pub mod fixed;
pub mod pool;

pub use chan::LockFreeInbox;
pub use chan::{
    AsyncProducer, AsyncReceiver, BlockingKernel, DropRings, DynamicState, FixedState, Inbox,
    Kernel, LockedInbox, PoolState, PooledRings, Producer, Receiver, Reclaim, RecvFuture,
    RecvManyFuture, SendBatchFuture, SendFuture, ShardState, SyncKernel, TrySendError,
};
