//! Owned receive batches preserve ownership while releasing the source ring.
use prescient::backend::{Backend, Batch, Ring, Seg};
use prescient::mpmc::brokerless::{self, dynamic};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[repr(align(64))]
struct Value {
    id: usize,
    bytes: [u8; 4096],
    drops: Arc<[AtomicUsize; 128]>,
    panic_on_drop: bool,
}
impl Drop for Value {
    fn drop(&mut self) {
        assert_eq!(self.drops[self.id].fetch_add(1, Ordering::Relaxed), 0);
        assert!(!self.panic_on_drop, "drop panic");
    }
}
fn value(id: usize, drops: &Arc<[AtomicUsize; 128]>) -> Value {
    Value {
        id,
        bytes: [id as u8; 4096],
        drops: drops.clone(),
        panic_on_drop: false,
    }
}
trait SendValue {
    fn send_value(&mut self, value: Value);
}
trait Receive {
    fn single(&mut self) -> Option<Value>;
    fn limit(&mut self, n: usize);
    fn batch(&mut self, out: &mut Batch<Value>) -> usize;
    fn blocking_batch(&mut self, out: &mut Batch<Value>) -> usize;
}
impl<B: Backend> SendValue for brokerless::Producer<Value, B> {
    fn send_value(&mut self, value: Value) {
        assert!(self.send(value));
    }
}
impl<B: Backend, R: dynamic::Rings<Value, B>> SendValue for dynamic::Producer<Value, B, R> {
    fn send_value(&mut self, value: Value) {
        assert!(self.send(value));
    }
}
impl<B: Backend> Receive for brokerless::Consumer<Value, B> {
    fn single(&mut self) -> Option<Value> {
        self.try_recv()
    }
    fn limit(&mut self, n: usize) {
        self.set_batch(n);
    }
    fn batch(&mut self, out: &mut Batch<Value>) -> usize {
        self.try_recv_batch(out)
    }
    fn blocking_batch(&mut self, out: &mut Batch<Value>) -> usize {
        self.recv_batch(out)
    }
}
impl<B: Backend, R: dynamic::Rings<Value, B>> Receive for dynamic::Consumer<Value, B, R> {
    fn single(&mut self) -> Option<Value> {
        self.try_recv()
    }
    fn limit(&mut self, n: usize) {
        self.set_batch(n);
    }
    fn batch(&mut self, out: &mut Batch<Value>) -> usize {
        self.try_recv_batch(out)
    }
    fn blocking_batch(&mut self, out: &mut Batch<Value>) -> usize {
        self.recv_batch(out)
    }
}
fn inspect(value: &Value, seen: &mut [bool; 128]) {
    assert!(!std::mem::replace(&mut seen[value.id], true));
    assert_eq!(value.bytes, [value.id as u8; 4096]);
    assert_eq!((value as *const Value).addr() % 64, 0);
}
fn exercise<P: SendValue, C: Receive>(factory: impl Fn(usize, usize) -> (Vec<P>, Vec<C>)) {
    for cap in [1, 4] {
        let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        let (mut tx, mut rx) = factory(cap, cap);
        let mut seen = [false; 128];
        let mut out = Batch::new();
        out.push_back(value(127, &drops)); // Explicitly replaced, never delivered.
        for round in 0..3 {
            for (p, tx) in tx.iter_mut().enumerate() {
                for offset in 0..cap {
                    tx.send_value(value(round * 2 * cap + p * cap + offset, &drops));
                }
            }
            rx[0].limit(cap);
            inspect(&rx[0].single().unwrap(), &mut seen);
            rx[0].limit(1); // Existing staged values must not be truncated.
            let got = rx[0].batch(&mut out);
            assert_eq!(got, if cap == 1 { 1 } else { cap - 1 });
            for value in out.as_slice() {
                inspect(value, &mut seen);
            }
            // Hold caller-owned values while a peer drains the remaining ring.
            for _ in 0..2 * cap + 1 {
                if let Some(value) = rx[1].single() {
                    inspect(&value, &mut seen);
                }
            }
            assert!(seen[..(round + 1) * 2 * cap].iter().all(|&v| v));
            assert_eq!(rx[0].batch(&mut out), 0);
            assert!(out.is_empty());
        }
        for (p, tx) in tx.iter_mut().enumerate() {
            tx.send_value(value(6 * cap + p, &drops));
        }
        drop(tx);
        // Blocking termination must make the final receive pass after disconnect.
        for _ in 0..4 {
            let got = rx[0].blocking_batch(&mut out);
            for value in out.as_slice() {
                inspect(value, &mut seen);
            }
            if got == 0 {
                break;
            }
        }
        assert!(seen[..6 * cap + 2].iter().all(|&v| v));
        assert_eq!(rx[0].blocking_batch(&mut out), 0);
        drop(out);
        drop(rx);
        for (id, count) in drops.iter().enumerate() {
            assert_eq!(
                count.load(Ordering::Relaxed),
                usize::from(id < 6 * cap + 2 || id == 127),
                "id={id}"
            );
        }
    }

    // Pure batch receives keep the same caller allocation across refills.
    let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let (mut tx, mut rx) = factory(4, 4);
    let mut out = Batch::new();
    let mut allocation = None;
    for round in 0..4 {
        for offset in 0..4 {
            tx[0].send_value(value(round * 4 + offset, &drops));
        }
        assert_eq!(rx[0].batch(&mut out), 4);
        let pointer = out.as_slice().as_ptr();
        assert_eq!(*allocation.get_or_insert(pointer), pointer);
        for (offset, value) in out.as_slice().iter().enumerate() {
            assert_eq!(value.id, round * 4 + offset);
        }
    }
    drop(out);
    drop(tx);
    drop(rx);
    assert!(drops[..16].iter().all(|n| n.load(Ordering::Relaxed) == 1));

    // Keeping the old batch and dropping the endpoint cannot strand ring capacity.
    let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let (mut tx, mut rx) = factory(4, 4);
    for id in 0..4 {
        tx[0].send_value(value(id, &drops));
    }
    let mut out = Batch::new();
    assert_eq!(rx[0].batch(&mut out), 4);
    for id in 4..8 {
        tx[0].send_value(value(id, &drops));
    }
    let mut peer = Batch::new();
    assert_eq!(rx[1].batch(&mut peer), 4);
    drop(tx);
    drop(rx);
    assert_eq!(
        out.as_slice().iter().map(|v| v.id).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_eq!(
        peer.as_slice().iter().map(|v| v.id).collect::<Vec<_>>(),
        [4, 5, 6, 7]
    );
    assert!(drops[..8].iter().all(|n| n.load(Ordering::Relaxed) == 0));
    drop(out);
    drop(peer);
    assert!(drops[..8].iter().all(|n| n.load(Ordering::Relaxed) == 1));

    // A destructor panic while clearing old output must not lose channel values.
    let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let (mut tx, mut rx) = factory(4, 4);
    tx[0].send_value(value(0, &drops));
    let mut out = Batch::new();
    let mut panicking = value(1, &drops);
    panicking.panic_on_drop = true;
    out.push_back(panicking);
    out.push_back(value(2, &drops));
    assert!(catch_unwind(AssertUnwindSafe(|| rx[0].batch(&mut out))).is_err());
    assert!(out.is_empty());
    assert_eq!(rx[1].batch(&mut out), 1);
    assert_eq!(out.as_slice()[0].id, 0);
    drop(out);
    drop(rx);
    drop(tx);
    assert!(drops[..3].iter().all(|n| n.load(Ordering::Relaxed) == 1));
}
fn fixed<B: Backend>() {
    exercise(|capacity, batch| {
        brokerless::channel::<Value>()
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
        let (registrar, rx) = dynamic::locked::<Value>()
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
        let (registrar, rx) = dynamic::array::<Value>(2)
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
fn fixed_owned_receive() {
    fixed::<Ring>();
    fixed::<Seg>();
}
#[test]
fn locked_owned_receive() {
    locked::<Ring>();
    locked::<Seg>();
}
#[test]
fn array_owned_receive() {
    array::<Ring>();
    array::<Seg>();
}

#[test]
fn zero_sized_owned_receive() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut tx, mut rx) = brokerless::channel::<Zst>()
        .producers(1)
        .consumers(1)
        .capacity(4)
        .batch(3)
        .open()
        .unwrap();
    for _ in 0..4 {
        assert!(tx[0].send(Zst));
    }
    drop(tx);
    let mut out = Batch::new();
    assert_eq!(rx[0].recv_batch(&mut out), 3);
    assert_eq!(out.as_slice().len(), 3);
    drop(out.pop_front());
    assert_eq!(rx[0].recv_batch(&mut out), 1);
    assert_eq!(rx[0].recv_batch(&mut out), 0);
    assert_eq!(DROPS.load(Ordering::Relaxed), 4);
}
