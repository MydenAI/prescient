//! Benchmark-only std mutex queues with shared or per-producer storage.
//! Full/empty retries use Prescient's spin-then-yield schedule outside locks.
//! Mutex acquisition itself uses the standard blocking lock and may park.
//! Send/receive APIs move one Copy payload; receivers stage up to the configured batch.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

struct State<T: Copy> {
    values: VecDeque<T>,
    capacity: usize,
    senders: usize,
    receivers: usize,
}
impl<T: Copy> State<T> {
    fn new(capacity: usize, senders: usize, receivers: usize) -> Arc<Mutex<Self>> {
        assert!(capacity > 0 && senders > 0 && receivers > 0);
        Arc::new(Mutex::new(Self {
            values: VecDeque::with_capacity(capacity),
            capacity,
            senders,
            receivers,
        }))
    }
}

pub struct Sender<T: Copy>(Arc<Mutex<State<T>>>);
pub struct Receiver<T: Copy> {
    shards: Vec<Arc<Mutex<State<T>>>>,
    cursor: usize,
    batch: usize,
    pending: Vec<T>,
    read: usize,
}

fn receivers<T: Copy>(
    shards: Vec<Arc<Mutex<State<T>>>>,
    count: usize,
    batch: usize,
) -> Vec<Receiver<T>> {
    assert!(batch > 0);
    (0..count)
        .map(|_| Receiver {
            shards: shards.clone(),
            cursor: 0,
            batch,
            pending: Vec::new(),
            read: 0,
        })
        .collect()
}

pub fn bounded<T: Copy>(
    capacity: usize,
    senders: usize,
    consumers: usize,
    batch: usize,
) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    let state = State::new(capacity, senders, consumers);
    (
        (0..senders).map(|_| Sender(state.clone())).collect(),
        receivers(vec![state], consumers, batch),
    )
}

pub fn sharded<T: Copy>(
    capacity: usize,
    senders: usize,
    consumers: usize,
    batch: usize,
) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    assert!(senders > 0);
    let shards: Vec<_> = (0..senders)
        .map(|_| State::new(capacity, 1, consumers))
        .collect();
    (
        shards.iter().map(|state| Sender(state.clone())).collect(),
        receivers(shards, consumers, batch),
    )
}

impl<T: Copy> Sender<T> {
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), T> {
        let mut retries = 0u32;
        loop {
            let mut state = self.0.lock().unwrap();
            if state.receivers == 0 {
                return Err(value);
            }
            if state.values.len() < state.capacity {
                state.values.push_back(value);
                return Ok(());
            }
            drop(state);
            retries = retries.wrapping_add(1);
            if retries < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

impl<T: Copy> Receiver<T> {
    #[inline]
    pub fn recv(&mut self) -> Option<T> {
        if let Some(&value) = self.pending.get(self.read) {
            self.read += 1;
            return Some(value);
        }
        self.refill()
    }

    #[inline(never)]
    fn refill(&mut self) -> Option<T> {
        self.pending.clear();
        self.read = 0;
        // Linear private staging, allocation reused and growth outside the lock.
        // For Copy payloads this needs no unsafe initialization/drop bookkeeping.
        self.pending.reserve(self.batch);
        let mut retries = 0u32;
        loop {
            let mut closed = 0;
            for _ in 0..self.shards.len() {
                let index = self.cursor;
                self.cursor += 1;
                if self.cursor == self.shards.len() {
                    self.cursor = 0;
                }
                let mut state = self.shards[index].lock().unwrap();
                let count = state.values.len().min(self.batch);
                if count > 0 {
                    self.pending.extend(state.values.drain(..count));
                    drop(state);
                    self.read = 1;
                    return Some(self.pending[0]);
                }
                closed += usize::from(state.senders == 0);
            }
            if closed == self.shards.len() {
                return None;
            }
            retries = retries.wrapping_add(1);
            if retries < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                retries = 0;
            }
        }
    }
}

impl<T: Copy> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).senders -= 1;
    }
}
impl<T: Copy> Drop for Receiver<T> {
    fn drop(&mut self) {
        for state in &self.shards {
            state.lock().unwrap_or_else(|e| e.into_inner()).receivers -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_fifo_drains_after_last_sender_drops() {
        let (mut tx, mut rx) = bounded(3, 2, 1, 2);
        tx[0].send(10).unwrap();
        tx[1].send(20).unwrap();
        tx[0].send(30).unwrap();
        drop(tx);
        for value in [10, 20, 30] {
            assert_eq!(rx[0].recv(), Some(value));
        }
        assert_eq!(rx[0].recv(), None);
        assert_eq!(rx[0].recv(), None);
    }

    #[test]
    fn shard_fifo_and_batch_limit() {
        let (mut tx, mut rx) = sharded(4, 2, 1, 2);
        for i in 0..4 {
            tx[0].send(i).unwrap();
            tx[1].send(i + 10).unwrap();
        }
        drop(tx);
        for value in [0, 1, 10, 11, 2, 3, 12, 13] {
            assert_eq!(rx[0].recv(), Some(value));
            assert!(rx[0].pending.len() <= 2);
        }
        assert_eq!(rx[0].recv(), None);
    }

    #[test]
    fn only_last_endpoint_disconnects() {
        let (mut tx, mut rx) = bounded(1, 2, 2, 64);
        drop(rx.pop());
        drop(tx.pop());
        tx[0].send(7).unwrap();
        assert_eq!(rx[0].recv(), Some(7));
        drop(rx);
        assert_eq!(tx[0].send(9), Err(9));
    }

    #[test]
    fn full_sender_releases_lock_and_observes_disconnect() {
        let (mut tx, rx) = bounded(1, 1, 1, 64);
        tx[0].send(1).unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            tx[0].send(2)
        });
        ready_rx.recv().unwrap();
        assert_eq!(rx[0].shards[0].lock().unwrap().values.len(), 1);
        drop(rx);
        assert_eq!(worker.join().unwrap(), Err(2));
    }

    #[test]
    fn empty_receiver_observes_disconnect() {
        let (tx, mut rx) = sharded::<u64>(1, 2, 1, 64);
        let worker = std::thread::spawn(move || rx[0].recv());
        drop(tx);
        assert_eq!(worker.join().unwrap(), None);
    }

    #[test]
    fn contended_shapes_transfer_every_value_once() {
        for make in [bounded, sharded] {
            for capacity in [1, 64, 4096] {
                for batch in [1, 64] {
                    let (tx, rx) = make(capacity, 3, 3, batch);
                    let mut values = std::thread::scope(|scope| {
                        let senders: Vec<_> = tx
                            .into_iter()
                            .enumerate()
                            .map(|(index, mut tx)| {
                                scope.spawn(move || {
                                    for value in index * 1000..(index + 1) * 1000 {
                                        tx.send(value as u64).unwrap();
                                    }
                                })
                            })
                            .collect();
                        let receivers: Vec<_> = rx
                            .into_iter()
                            .map(|mut rx| {
                                scope.spawn(move || {
                                    let mut values = Vec::new();
                                    while let Some(value) = rx.recv() {
                                        values.push(value);
                                    }
                                    values
                                })
                            })
                            .collect();
                        for sender in senders {
                            sender.join().unwrap();
                        }
                        receivers
                            .into_iter()
                            .flat_map(|r| r.join().unwrap())
                            .collect::<Vec<_>>()
                    });
                    values.sort_unstable();
                    assert_eq!(values, (0..3000).collect::<Vec<_>>());
                }
            }
        }
    }

    #[test]
    #[should_panic]
    fn zero_capacity_rejected() {
        bounded::<u64>(0, 1, 1, 1);
    }

    #[test]
    #[should_panic]
    fn zero_batch_rejected() {
        sharded::<u64>(1, 1, 1, 0);
    }
}
