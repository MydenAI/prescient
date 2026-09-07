//! Fixed brokerless MPMC arranged as a producer-by-consumer SPSC lane grid.
//!
//! Every lane has one permanent writer and one permanent reader. Consumers
//! therefore drain without a claim CAS; producers route work across their own
//! consumer lanes. This is an explicit engine because its extra routing and
//! `producers * consumers` rings are a workload trade-off.

use crate::backend::{Backend, Batch, Ring, Rx as _, Tx as _};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Declare fixed-membership lane-grid MPMC with Ring storage and spin/yield waiting.
pub fn channel<T>() -> crate::Channel<
    T,
    crate::topology::Mpmc<crate::engine::Lanes>,
    Ring,
    crate::execution::Sync,
    crate::wait::SpinYield,
> {
    crate::Channel::new()
        .mpmc()
        .engine::<crate::engine::Lanes>()
}

struct Shared<T: Send, B: Backend> {
    live_producers: AtomicUsize,
    live_consumers: AtomicUsize,
    abandoned: Mutex<Vec<B::Rx<T>>>,
}

/// One producer with one private SPSC sending lane per consumer.
pub struct Producer<T: Send, B: Backend = Ring> {
    lanes: Box<[B::Tx<T>]>,
    cursor: usize,
    shared: Arc<Shared<T, B>>,
}

/// One consumer with one private SPSC receiving lane per producer.
pub struct Consumer<T: Send, B: Backend = Ring> {
    lanes: Vec<B::Rx<T>>,
    cursor: usize,
    staging: Batch<T>,
    batch: usize,
    shared: Arc<Shared<T, B>>,
}

type Endpoints<T, B> = (Vec<Producer<T, B>>, Vec<Consumer<T, B>>);
pub(crate) fn open<T: Send, B: Backend>(
    producers: usize,
    consumers: usize,
    lane_capacity: usize,
) -> Endpoints<T, B> {
    let mut producer_lanes = (0..producers)
        .map(|_| Vec::with_capacity(consumers))
        .collect::<Vec<_>>();
    let mut consumer_lanes = (0..consumers)
        .map(|_| Vec::with_capacity(producers))
        .collect::<Vec<_>>();

    for producer in &mut producer_lanes {
        for consumer in &mut consumer_lanes {
            let (tx, rx) = B::channel::<T>(lane_capacity);
            producer.push(tx);
            consumer.push(rx);
        }
    }

    let shared = Arc::new(Shared {
        live_producers: AtomicUsize::new(producers),
        live_consumers: AtomicUsize::new(consumers),
        abandoned: Mutex::new(Vec::new()),
    });
    let producer_endpoints = producer_lanes
        .into_iter()
        .enumerate()
        .map(|(cursor, lanes)| Producer {
            lanes: lanes.into_boxed_slice(),
            cursor: cursor % consumers,
            shared: Arc::clone(&shared),
        })
        .collect();
    let consumers = consumer_lanes
        .into_iter()
        .enumerate()
        .map(|(cursor, lanes)| Consumer {
            lanes,
            cursor: cursor % producers,
            staging: Batch::new(),
            batch: 64,
            shared: Arc::clone(&shared),
        })
        .collect();
    (producer_endpoints, consumers)
}

impl<T: Send, B: Backend> Producer<T, B> {
    #[inline]
    fn next_lane(&mut self) -> usize {
        crate::round_robin::next(&mut self.cursor, self.lanes.len())
    }

    /// Attempt one send without waiting, returning the value when every live lane is full.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), T> {
        let mut value = value;
        for _ in 0..self.lanes.len() {
            let lane = self.next_lane();
            match self.lanes[lane].try_push(value) {
                Ok(()) => return Ok(()),
                Err(back) => value = back,
            }
        }
        Err(value)
    }

    /// Send, spinning and yielding until a lane accepts the value.
    ///
    /// Returns `false` and drops the value if every consumer has disconnected.
    pub fn send(&mut self, mut value: T) -> bool {
        let mut spins = 0u32;
        loop {
            match self.try_send(value) {
                Ok(()) => return true,
                Err(back) => value = back,
            }
            if self.shared.live_consumers.load(Ordering::Acquire) == 0 {
                return false;
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }

    /// Publish an owned batch, retaining an unsent suffix if all consumers disconnect.
    ///
    /// A productive lane receives as much of the remaining prefix as it can accept;
    /// routing then advances. Accepted values are not delivery acknowledgements.
    pub fn send_batch(&mut self, source: &mut Batch<T>) -> bool {
        let mut spins = 0u32;
        while !source.is_empty() {
            let before = source.len();
            for _ in 0..self.lanes.len() {
                let lane = self.next_lane();
                self.lanes[lane].push_from(usize::MAX, source);
                if source.len() != before {
                    break;
                }
            }
            if source.len() != before {
                spins = 0;
                continue;
            }
            if self.shared.live_consumers.load(Ordering::Acquire) == 0 {
                return false;
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
        true
    }
}

impl<T: Send, B: Backend> Drop for Producer<T, B> {
    fn drop(&mut self) {
        self.shared.live_producers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: Send, B: Backend> Consumer<T, B> {
    /// Set the maximum number of values drained from one producer lane per refill.
    pub fn set_batch(&mut self, batch: usize) {
        self.batch = batch.max(1);
    }

    #[inline]
    fn scan_into_staging(&mut self) -> bool {
        let lanes = self.lanes.len();
        for _ in 0..lanes {
            let lane = crate::round_robin::next(&mut self.cursor, lanes);
            self.lanes[lane].drain_into(self.batch, &mut self.staging);
            if !self.staging.is_empty() {
                return true;
            }
        }
        false
    }

    #[cold]
    fn adopt_abandoned(&mut self) -> bool {
        let mut abandoned = self.shared.abandoned.lock();
        if abandoned.is_empty() {
            return false;
        }
        self.lanes.extend(abandoned.drain(..));
        true
    }

    #[inline(never)]
    fn refill(&mut self) {
        self.staging.clear();
        if self.scan_into_staging() {
            return;
        }
        if self.adopt_abandoned() {
            self.scan_into_staging();
        }
    }

    /// Attempt to receive one value without waiting.
    #[inline]
    pub fn try_recv(&mut self) -> Option<T> {
        if let Some(value) = self.staging.pop_front() {
            return Some(value);
        }
        self.refill();
        self.staging.pop_front()
    }

    /// Drain one owned batch without retaining a lane between calls.
    #[inline]
    pub fn try_recv_batch(&mut self, out: &mut Batch<T>) -> usize {
        out.clear();
        if self.staging.is_empty() {
            std::mem::swap(&mut self.staging, out);
            self.refill();
        }
        std::mem::swap(&mut self.staging, out);
        out.len()
    }

    /// Wait for and receive one owned batch, or return zero after final disconnect.
    pub fn recv_batch(&mut self, out: &mut Batch<T>) -> usize {
        let mut spins = 0u32;
        loop {
            let received = self.try_recv_batch(out);
            if received != 0 {
                return received;
            }
            if self.is_disconnected() {
                return self.try_recv_batch(out);
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }

    /// True once every producer endpoint has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_producers.load(Ordering::Acquire) == 0
    }

    /// Receive until all producers have disconnected and this consumer is drained.
    pub fn recv(&mut self) -> Option<T> {
        let mut spins = 0u32;
        loop {
            if let Some(value) = self.try_recv() {
                return Some(value);
            }
            if self.is_disconnected() {
                return self.try_recv();
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }

    fn scan_in_place<F: FnMut(T)>(&mut self, max: usize, f: &mut F) -> usize {
        let lanes = self.lanes.len();
        for _ in 0..lanes {
            let lane = crate::round_robin::next(&mut self.cursor, lanes);
            let got = self.lanes[lane].drain_in_place(max, &mut *f);
            if got != 0 {
                return got;
            }
        }
        0
    }

    /// Drain up to `max` values in place, returning the number processed.
    pub fn drain<F: FnMut(T)>(&mut self, max: usize, mut f: F) -> usize {
        let mut done = 0usize;
        while done < max {
            let Some(value) = self.staging.pop_front() else {
                break;
            };
            f(value);
            done += 1;
        }
        if done == max {
            return done;
        }
        let got = self.scan_in_place(max - done, &mut f);
        if got != 0 {
            return done + got;
        }
        if self.adopt_abandoned() {
            done += self.scan_in_place(max - done, &mut f);
        }
        done
    }

    /// Process values in place until every producer disconnects and all lanes drain.
    pub fn for_each<F: FnMut(T)>(&mut self, batch: usize, mut f: F) {
        let batch = batch.max(1);
        let mut spins = 0u32;
        loop {
            if self.drain(batch, &mut f) != 0 {
                spins = 0;
                continue;
            }
            if self.is_disconnected() && self.drain(batch, &mut f) == 0 {
                return;
            }
            spins = spins.wrapping_add(1);
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }
}

impl<T: Send, B: Backend> Drop for Consumer<T, B> {
    fn drop(&mut self) {
        self.shared.abandoned.lock().extend(self.lanes.drain(..));
        self.shared.live_consumers.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn grid_delivers_every_value_once_across_consumers() {
        const PRODUCERS: usize = 3;
        const CONSUMERS: usize = 5;
        const PER: usize = 10_003;
        let (producers, consumers) = open::<usize, Ring>(PRODUCERS, CONSUMERS, 16);
        let delivered = (0..PRODUCERS * PER)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>();
        std::thread::scope(|scope| {
            for (producer, mut tx) in producers.into_iter().enumerate() {
                scope.spawn(move || {
                    for sequence in 0..PER {
                        assert!(tx.send(producer * PER + sequence));
                    }
                });
            }
            for mut rx in consumers {
                let delivered = &delivered;
                scope.spawn(move || {
                    while let Some(value) = rx.recv() {
                        delivered[value].fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert!(
            delivered
                .iter()
                .all(|count| count.load(Ordering::Relaxed) == 1)
        );
    }

    #[test]
    fn surviving_consumer_adopts_buffered_lanes() {
        let (mut producers, mut consumers) = open::<usize, Ring>(1, 2, 2);
        for value in 0..4 {
            assert!(producers[0].send(value));
        }
        drop(producers);
        drop(consumers.pop());
        let mut consumer = consumers.pop().unwrap();
        let mut values = Vec::new();
        while let Some(value) = consumer.recv() {
            values.push(value);
        }
        values.sort_unstable();
        assert_eq!(values, (0..4).collect::<Vec<_>>());
    }

    #[test]
    fn full_grid_returns_batch_suffix_after_all_consumers_drop() {
        let (mut producers, consumers) = open::<usize, Ring>(1, 2, 1);
        drop(consumers);
        assert!(producers[0].send(7));
        assert!(producers[0].send(8));
        assert!(!producers[0].send(9));
        let mut source = Batch::new();
        source.push_back(10);
        assert!(!producers[0].send_batch(&mut source));
        assert_eq!(source.pop_front(), Some(10));
    }
}
