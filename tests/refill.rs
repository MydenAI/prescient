//! Large, non-Copy and over-aligned values across refill/staged boundaries.
use prescient::backend::{Backend, Ring, Seg};
use prescient::mpmc::brokerless::{self, dynamic};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[repr(align(64))]
struct Payload {
    id: usize,
    bytes: [u8; 4096],
    drops: Arc<[AtomicUsize; 32]>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        assert_eq!(self.drops[self.id].fetch_add(1, Ordering::Relaxed), 0);
    }
}
trait SendValue {
    fn send_value(&mut self, value: Payload);
}
trait ReceiveValue {
    fn receive_value(&mut self) -> Option<Payload>;
    fn batch(&mut self, size: usize);
}
impl<B: Backend> SendValue for brokerless::Producer<Payload, B> {
    fn send_value(&mut self, value: Payload) {
        assert!(self.send(value));
    }
}
impl<B: Backend> ReceiveValue for brokerless::Consumer<Payload, B> {
    fn receive_value(&mut self) -> Option<Payload> {
        self.try_recv()
    }
    fn batch(&mut self, size: usize) {
        self.set_batch(size);
    }
}
impl<B: Backend, R: dynamic::Rings<Payload, B>> SendValue for dynamic::Producer<Payload, B, R> {
    fn send_value(&mut self, value: Payload) {
        assert!(self.send(value));
    }
}
impl<B: Backend, R: dynamic::Rings<Payload, B>> ReceiveValue for dynamic::Consumer<Payload, B, R> {
    fn receive_value(&mut self) -> Option<Payload> {
        self.try_recv()
    }
    fn batch(&mut self, size: usize) {
        self.set_batch(size);
    }
}

fn exercise<P: SendValue, C: ReceiveValue>(
    mut factory: impl FnMut(usize, usize) -> (Vec<P>, Vec<C>),
) {
    for capacity in [1, 4] {
        for batch in [1, 3, 64] {
            let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
            let (mut tx, mut rx) = factory(capacity, batch);
            let mut seen = [false; 32];
            for round in 0..3 {
                for (p, tx) in tx.iter_mut().enumerate() {
                    for offset in 0..capacity {
                        let id = round * 2 * capacity + p * capacity + offset;
                        tx.send_value(Payload {
                            id,
                            bytes: [id as u8; 4096],
                            drops: drops.clone(),
                        });
                    }
                }
                let mut received = 0;
                for _ in 0..(2 * capacity + 1) * 4 {
                    for rx in &mut rx {
                        if let Some(value) = rx.receive_value() {
                            assert_eq!(value.bytes, [value.id as u8; 4096]);
                            assert!(!std::mem::replace(&mut seen[value.id], true));
                            received += 1;
                            // Changing the next refill limit must preserve unread staging.
                            rx.batch(if received % 2 == 0 { 0 } else { 64 });
                        }
                    }
                    if received == 2 * capacity {
                        break;
                    }
                }
                assert_eq!(received, 2 * capacity);
                assert!(rx.iter_mut().all(|rx| rx.receive_value().is_none()));
            }
            // Teardown must also drop values that never cross the return boundary.
            for (p, tx) in tx.iter_mut().enumerate() {
                let id = 6 * capacity + p;
                tx.send_value(Payload {
                    id,
                    bytes: [id as u8; 4096],
                    drops: drops.clone(),
                });
            }
            drop(tx);
            drop(rx);
            for (id, count) in drops.iter().enumerate() {
                assert_eq!(
                    count.load(Ordering::Relaxed),
                    usize::from(id < 6 * capacity + 2),
                    "id={id}"
                );
            }
            assert!(seen[..6 * capacity].iter().all(|&value| value));
        }
    }
}
fn fixed<B: Backend>() {
    exercise(|capacity, batch| {
        brokerless::channel::<Payload>()
            .backend::<B>()
            .producers(2)
            .consumers(2)
            .capacity(capacity)
            .batch(batch)
            .open()
            .unwrap()
    });
}
fn locked<B: Backend>() {
    exercise(|capacity, batch| {
        let (registrar, rx) = dynamic::locked::<Payload>()
            .backend::<B>()
            .consumers(2)
            .capacity(capacity)
            .batch(batch)
            .open()
            .unwrap();
        ((0..2).map(|_| registrar.register()).collect(), rx)
    });
}
fn array<B: Backend>() {
    exercise(|capacity, batch| {
        let (registrar, rx) = dynamic::array::<Payload>(2)
            .backend::<B>()
            .consumers(2)
            .capacity(capacity)
            .batch(batch)
            .open()
            .unwrap();
        ((0..2).map(|_| registrar.register()).collect(), rx)
    });
}
#[test]
fn fixed_large_refill_owns_each_value_once() {
    fixed::<Ring>();
    fixed::<Seg>();
}
#[test]
fn locked_large_refill_owns_each_value_once() {
    locked::<Ring>();
    locked::<Seg>();
}
#[test]
fn array_large_refill_owns_each_value_once() {
    array::<Ring>();
    array::<Seg>();
}
