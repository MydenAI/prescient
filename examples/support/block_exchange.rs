//! Diagnostic only: bounded whole-block ownership through per-producer rings.
//! Each consumer has one SPSC recycling lane back to each producer. Payload Vecs
//! are allocated once at setup, never cloned, and cannot be reused until returned.
//! Pool capacity includes every block state, not only queued payloads.
//! Return queues can each hold the entire origin pool: returning never waits for
//! another worker. Metadata scales with producers * consumers * blocks.
use prescient::{
    backend::{Backend, Ring, Rx, Tx, ring},
    mpmc::brokerless,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(super) struct Block<T> {
    origin: usize,
    values: Vec<T>,
}

pub(super) trait ForwardSend<T> {
    fn publish(&mut self, value: T) -> Result<(), T>;
}
impl<T: Send> ForwardSend<T> for brokerless::Producer<T> {
    #[inline]
    fn publish(&mut self, value: T) -> Result<(), T> {
        self.try_send(value)
    }
}
impl<T> ForwardSend<T> for crossbeam_channel::Sender<T> {
    #[inline]
    fn publish(&mut self, value: T) -> Result<(), T> {
        self.try_send(value).map_err(|e| e.into_inner())
    }
}
pub(super) trait ForwardReceive<T> {
    fn poll(&mut self) -> Result<T, crossbeam_channel::TryRecvError>;
}
impl<T: Send> ForwardReceive<T> for brokerless::Consumer<T> {
    #[inline]
    fn poll(&mut self) -> Result<T, crossbeam_channel::TryRecvError> {
        use crossbeam_channel::TryRecvError::{Disconnected, Empty};
        if let Some(value) = self.try_recv() {
            Ok(value)
        } else if self.is_disconnected() {
            self.try_recv().ok_or(Disconnected)
        } else {
            Err(Empty)
        }
    }
}
impl<T> ForwardReceive<T> for crossbeam_channel::Receiver<T> {
    #[inline]
    fn poll(&mut self) -> Result<T, crossbeam_channel::TryRecvError> {
        self.try_recv()
    }
}

pub(super) struct Sender<T: Send, P: ForwardSend<Block<T>> = brokerless::Producer<Block<T>>> {
    forward: P,
    returns: Vec<ring::Receiver<Block<T>>>,
    free: Vec<Block<T>>,
    in_flight: usize,
    cursor: usize,
    batch: usize,
    cancelled: Arc<AtomicBool>,
    finished: bool,
}
pub(super) struct Receiver<T: Send, C: ForwardReceive<Block<T>> = brokerless::Consumer<Block<T>>> {
    forward: C,
    returns: Vec<ring::Sender<Block<T>>>,
    cancelled: Arc<AtomicBool>,
    finished: bool,
}
struct CancelOnUnwind<'a>(&'a AtomicBool, bool);
impl Drop for CancelOnUnwind<'_> {
    fn drop(&mut self) {
        if self.1 {
            self.0.store(true, Ordering::Release);
        }
    }
}
fn pause(spins: &mut u32) {
    *spins = spins.saturating_add(1);
    if *spins < 64 {
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}
pub(super) fn channel<T: Send>(
    producers: usize,
    consumers: usize,
    capacity: usize,
    batch: usize,
) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    assert!(producers > 0 && consumers > 0 && batch > 0);
    assert!(capacity >= batch && capacity.is_multiple_of(batch));
    let blocks = capacity / batch;
    let slots = blocks.checked_next_power_of_two().unwrap();
    let (txs, rxs) = brokerless::channel::<Block<T>>()
        .producers(producers)
        .consumers(consumers)
        .capacity(slots)
        .batch(1) // A receiver never hoards a private batch of descriptors.
        .open()
        .unwrap();
    assemble(txs, rxs, blocks, batch)
}
type CrossbeamSender<T> = Sender<T, crossbeam_channel::Sender<Block<T>>>;
type CrossbeamReceiver<T> = Receiver<T, crossbeam_channel::Receiver<Block<T>>>;
pub(super) fn crossbeam<T: Send>(
    producers: usize,
    consumers: usize,
    capacity: usize,
    batch: usize,
) -> (Vec<CrossbeamSender<T>>, Vec<CrossbeamReceiver<T>>) {
    assert!(producers > 0 && consumers > 0 && batch > 0);
    assert!(capacity >= batch && capacity.is_multiple_of(batch));
    let blocks = capacity / batch;
    let slots = blocks.checked_next_power_of_two().unwrap();
    let (tx, rx) = crossbeam_channel::bounded(producers.checked_mul(slots).unwrap());
    assemble(
        (0..producers).map(|_| tx.clone()).collect(),
        (0..consumers).map(|_| rx.clone()).collect(),
        blocks,
        batch,
    )
}
type Endpoints<T, P, C> = (Vec<Sender<T, P>>, Vec<Receiver<T, C>>);
fn assemble<T: Send, P: ForwardSend<Block<T>>, C: ForwardReceive<Block<T>>>(
    txs: Vec<P>,
    rxs: Vec<C>,
    blocks: usize,
    batch: usize,
) -> Endpoints<T, P, C> {
    let producers = txs.len();
    let slots = blocks.checked_next_power_of_two().unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut receivers = rxs
        .into_iter()
        .map(|forward| Receiver {
            forward,
            returns: Vec::with_capacity(producers),
            cancelled: Arc::clone(&cancelled),
            finished: false,
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
            Sender {
                forward,
                returns,
                free: (0..blocks)
                    .map(|_| Block {
                        origin,
                        values: Vec::with_capacity(batch),
                    })
                    .collect(),
                in_flight: 0,
                cursor: 0,
                batch,
                cancelled: Arc::clone(&cancelled),
                finished: false,
            }
        })
        .collect();
    (senders, receivers)
}
impl<T: Send, P: ForwardSend<Block<T>>> Sender<T, P> {
    fn returned(&mut self) -> Option<Block<T>> {
        for _ in 0..self.returns.len() {
            let index = self.cursor;
            self.cursor += 1;
            if self.cursor == self.returns.len() {
                self.cursor = 0;
            }
            if let Some(block) = self.returns[index].try_pop() {
                self.in_flight -= 1;
                return Some(block);
            }
        }
        None
    }
    fn acquire(&mut self) -> Result<Block<T>, &'static str> {
        if let Some(block) = self.free.pop() {
            return Ok(block);
        }
        let mut spins = 0;
        loop {
            if let Some(block) = self.returned() {
                return Ok(block);
            }
            if self.cancelled.load(Ordering::Acquire) {
                return Err("block exchange cancelled");
            }
            pause(&mut spins);
        }
    }
    /// One complete workload, including reclamation of every published block.
    /// Filling receives a reusable empty Vec; the diagnostic caller must not
    /// exceed the supplied range length, reserve, or replace its allocation.
    pub(super) fn send_with(
        &mut self,
        start: usize,
        end: usize,
        mut fill: impl FnMut(&mut Vec<T>, std::ops::Range<usize>),
    ) -> Result<(), &'static str> {
        assert!(!self.finished);
        let cancellation = Arc::clone(&self.cancelled); // Once per workload.
        let mut guard = CancelOnUnwind(&cancellation, true);
        let mut next = start;
        while next < end {
            let mut block = self.acquire()?;
            let stop = next + self.batch.min(end - next);
            fill(&mut block.values, next..stop);
            assert_eq!(block.values.len(), stop - next);
            // The pool bounds all outstanding blocks, so its equally-sized
            // forward descriptor ring cannot be full while we own this block.
            if self.forward.publish(block).is_err() {
                return Err("block forward lane unavailable");
            }
            self.in_flight += 1;
            next = stop;
        }
        let mut spins = 0;
        while self.in_flight != 0 {
            if let Some(block) = self.returned() {
                self.free.push(block);
                spins = 0;
            } else {
                if self.cancelled.load(Ordering::Acquire) {
                    return Err("block exchange cancelled before final recycling");
                }
                pause(&mut spins);
            }
        }
        self.finished = true;
        guard.1 = false;
        Ok(())
    }
}
impl<T: Send, C: ForwardReceive<Block<T>>> Receiver<T, C> {
    pub(super) fn consume(&mut self, mut f: impl FnMut(&[T])) -> Result<(), &'static str> {
        let cancellation = Arc::clone(&self.cancelled); // Once per workload.
        let mut guard = CancelOnUnwind(&cancellation, true);
        let mut spins = 0;
        loop {
            let polled = self.forward.poll();
            let disconnected = matches!(polled, Err(crossbeam_channel::TryRecvError::Disconnected));
            if let Ok(mut block) = polled {
                f(&block.values); // The forward ring claim has already been released.
                block.values.clear();
                if self.returns[block.origin].try_push(block).is_err() {
                    return Err("block return lane unavailable");
                }
                spins = 0;
            } else if self.cancelled.load(Ordering::Acquire) {
                return Err("block exchange cancelled");
            } else if disconnected {
                self.finished = true;
                guard.1 = false;
                return Ok(());
            } else {
                pause(&mut spins);
            }
        }
    }
}
impl<T: Send, P: ForwardSend<Block<T>>> Drop for Sender<T, P> {
    fn drop(&mut self) {
        if !self.finished {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}
impl<T: Send, C: ForwardReceive<Block<T>>> Drop for Receiver<T, C> {
    fn drop(&mut self) {
        if !self.finished {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicUsize};

    struct Value(usize, Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[test]
    fn exact_ownership_bounded_allocations_partial_blocks_and_slow_consumer() {
        for (p, c) in [(1, 1), (3, 3), (1, 4), (4, 1)] {
            for (capacity, batch) in [(1, 1), (4, 1), (12, 3), (64, 64)] {
                let drops = Arc::new(AtomicUsize::new(0));
                let seen = Mutex::new(vec![0usize; p * 97]);
                let (tx, rx) = channel(p, c, capacity, batch);
                let pointers = tx
                    .iter()
                    .flat_map(|t| t.free.iter().map(|b| b.values.as_ptr() as usize))
                    .collect::<Vec<_>>();
                std::thread::scope(|scope| {
                    for (i, mut sender) in tx.into_iter().enumerate() {
                        let drops = Arc::clone(&drops);
                        let pointers = &pointers;
                        scope.spawn(move || {
                            sender
                                .send_with(i * 97, (i + 1) * 97, |v, ids| {
                                    assert!(v.is_empty());
                                    assert_eq!(v.capacity(), batch);
                                    assert!(pointers.contains(&(v.as_ptr() as usize)));
                                    v.extend(ids.map(|id| Value(id, Arc::clone(&drops))));
                                })
                                .unwrap();
                            assert_eq!(sender.in_flight, 0);
                            assert_eq!(sender.free.len(), capacity / batch);
                        });
                    }
                    for (i, mut receiver) in rx.into_iter().enumerate() {
                        let seen = &seen;
                        let pointers = &pointers;
                        scope.spawn(move || {
                            receiver
                                .consume(|v| {
                                    assert!(pointers.contains(&(v.as_ptr() as usize)));
                                    for value in v {
                                        seen.lock().unwrap()[value.0] += 1;
                                    }
                                    if i == 0 {
                                        std::thread::yield_now();
                                    }
                                })
                                .unwrap()
                        });
                    }
                });
                assert!(seen.into_inner().unwrap().iter().all(|n| *n == 1));
                assert_eq!(drops.load(Ordering::Relaxed), p * 97);
            }
        }
    }

    fn held_block<P: ForwardSend<Block<usize>> + Send, C: ForwardReceive<Block<usize>> + Send>(
        mut tx: Vec<Sender<usize, P>>,
        mut rx: Vec<Receiver<usize, C>>,
    ) {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                tx[0].send_with(0, 1, |v, ids| v.extend(ids)).unwrap();
                assert_eq!(tx[0].free.len(), 1);
                done_tx.send(()).unwrap();
            });
            scope.spawn(move || {
                rx[0]
                    .consume(|v| {
                        assert_eq!(v, &[0]);
                        held_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    })
                    .unwrap()
            });
            held_rx.recv().unwrap();
            assert!(
                done_rx.try_recv().is_err(),
                "completion must include the final return"
            );
            release_tx.send(()).unwrap();
            done_rx.recv().unwrap();
        });
    }
    #[test]
    fn completion_waits_for_final_borrowed_block_return() {
        let (tx, rx) = channel(1, 1, 1, 1);
        held_block(tx, rx);
        let (tx, rx) = crossbeam(1, 1, 1, 1);
        held_block(tx, rx);
    }

    #[test]
    fn early_receiver_drop_cancels_instead_of_stranding_pool() {
        let (mut tx, rx) = channel::<usize>(1, 2, 1, 1);
        drop(rx);
        assert!(tx[0].send_with(0, 10, |v, ids| v.extend(ids)).is_err());
    }
    #[test]
    fn producer_drop_cancels_idle_receiver() {
        let (tx, mut rx) = channel::<usize>(1, 1, 1, 1);
        drop(tx);
        assert!(rx[0].consume(|_| panic!("no payload")).is_err());
    }
    #[test]
    fn callback_panic_cancels_even_if_endpoint_is_retained() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = channel(1, 1, 1, 1);
        std::thread::scope(|scope| {
            let drops = Arc::clone(&drops);
            let sender = scope.spawn(move || {
                tx[0].send_with(0, 100, |v, ids| {
                    v.extend(ids.map(|id| Value(id, Arc::clone(&drops))));
                })
            });
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = rx[0].consume(|_| panic!("injected callback failure"));
            }));
            assert!(panic.is_err());
            assert!(sender.join().unwrap().is_err());
        });
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn producer_fill_panic_drops_partial_block_and_cancels_peer() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = channel(1, 1, 2, 2);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tx[0].send_with(0, 10, |v, _| {
                v.push(Value(0, Arc::clone(&drops)));
                panic!("injected fill failure");
            });
        }));
        assert!(panic.is_err());
        assert!(rx[0].consume(|_| panic!("never published")).is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
