//! Fixed tier: exactly `N` producers, minted up front. No membership state at
//! all — the receiver owns a fixed `Vec` of shards, with no inbox, bitmap, or
//! membership lock.

use crate::mpsc::chan::{
    FixedState, Kernel, Producer, Receiver, build_shared, make_producer, make_receiver, spawn_ring,
};

/// Declare fixed-membership MPSC with the crate defaults.
pub fn channel<T>() -> crate::Channel<T> {
    crate::Channel::new()
}

pub(crate) fn open<T: Send, K: Kernel>(
    producers: usize,
    capacity: usize,
) -> (Vec<Producer<T, K>>, Receiver<T, K, FixedState>) {
    let (shared, waiter) = build_shared::<K>();
    let mut txs = Vec::with_capacity(producers);
    let mut rxs = Vec::with_capacity(producers);
    for _ in 0..producers {
        let (tx, shard) = spawn_ring::<T, K>(capacity);
        shared.inc();
        txs.push(make_producer::<T, K>(
            tx,
            shard.space.clone(),
            shared.clone(),
        ));
        rxs.push(shard);
    }
    let receiver = make_receiver::<T, K, _>(rxs, shared, waiter, FixedState);
    (txs, receiver)
}
