//! Preferred rings remain hints: bounded service, stealing and late membership.
use prescient::backend::{Backend, Batch, Ring, Seg};
use prescient::mpmc::brokerless::{self, dynamic};

trait SendValue {
    fn send_value(&mut self, value: u64);
}
trait Receive: Clone {
    fn one(&mut self) -> Option<u64>;
    fn batch(&mut self, out: &mut Batch<u64>) -> usize;
    fn drain(&mut self, f: impl FnMut(u64)) -> usize;
}

impl<B: Backend> SendValue for brokerless::Producer<u64, B> {
    fn send_value(&mut self, value: u64) {
        assert!(self.send(value));
    }
}
impl<B: Backend, R: dynamic::Rings<u64, B>> SendValue for dynamic::Producer<u64, B, R> {
    fn send_value(&mut self, value: u64) {
        assert!(self.send(value));
    }
}
impl<B: Backend> Receive for brokerless::Consumer<u64, B> {
    fn one(&mut self) -> Option<u64> {
        self.try_recv()
    }
    fn batch(&mut self, out: &mut Batch<u64>) -> usize {
        self.try_recv_batch(out)
    }
    fn drain(&mut self, f: impl FnMut(u64)) -> usize {
        self.drain(1, f)
    }
}
impl<B: Backend, R: dynamic::Rings<u64, B>> Receive for dynamic::Consumer<u64, B, R> {
    fn one(&mut self) -> Option<u64> {
        self.try_recv()
    }
    fn batch(&mut self, out: &mut Batch<u64>) -> usize {
        self.try_recv_batch(out)
    }
    fn drain(&mut self, f: impl FnMut(u64)) -> usize {
        self.drain(1, f)
    }
}

fn mixed_one(rx: &mut impl Receive, turn: usize) -> Option<u64> {
    match turn % 3 {
        0 => rx.one(),
        1 => {
            let mut out = Batch::new();
            let got = rx.batch(&mut out);
            assert_eq!(got, out.len());
            out.pop_front()
        }
        _ => {
            let mut value = None;
            let got = rx.drain(|v| {
                assert!(value.replace(v).is_none());
            });
            assert_eq!(got, usize::from(value.is_some()));
            value
        }
    }
}

fn exercise<P: SendValue, C: Receive>(factory: impl Fn() -> (Vec<P>, C)) {
    // A perpetually productive ring cannot hide a cold ring beyond a bounded
    // number of drains, including when scalar/batch/callback APIs are mixed.
    let (mut tx, mut rx) = factory();
    tx[0].send_value(0);
    tx[2].send_value(1000);
    let mut found = false;
    for expected in 0..64 {
        let value = mixed_one(&mut rx, expected).unwrap();
        if value == 1000 {
            found = true;
            break;
        }
        assert_eq!(value, expected as u64);
        tx[0].send_value(expected as u64 + 1);
    }
    assert!(found, "hot producer starved the cold producer");

    // Empty preferred rings immediately fall through to other rings.
    let (mut tx, mut rx) = factory();
    tx[0].send_value(10);
    assert_eq!(rx.one(), Some(10));
    tx[2].send_value(20);
    assert_eq!(rx.one(), Some(20));

    // All consumers can steal; caller-owned batches do not retain ring claims.
    let (mut tx, mut rx) = factory();
    let mut peer = rx.clone();
    tx[0].send_value(30);
    let mut out = Batch::new();
    assert_eq!(rx.batch(&mut out), 1);
    tx[0].send_value(31);
    assert_eq!(peer.one(), Some(31));
    assert_eq!(out.as_slice(), &[30]);

    // A peer preferring a currently claimed ring must skip it and make progress
    // elsewhere. The callback runs synchronously while rx holds ring zero.
    tx[0].send_value(32);
    tx[1].send_value(40);
    assert_eq!(
        rx.drain(|v| {
            assert_eq!(v, 32);
            assert_eq!(peer.one(), Some(40));
        }),
        1
    );

    // An inactive/dropped preferred consumer cannot strand remaining values.
    tx[0].send_value(33);
    drop(rx);
    assert_eq!(peer.one(), Some(33));
    drop(tx);
    assert_eq!(peer.one(), None);

    // Cold clone seeds spread the first choices without reserving those rings.
    let (mut tx, rx) = factory();
    let mut a = rx.clone();
    let mut b = rx.clone();
    for (id, sender) in tx.iter_mut().enumerate() {
        sender.send_value(id as u64);
    }
    assert_eq!(a.one(), Some(1));
    assert_eq!(b.one(), Some(2));
}

fn fixed<B: Backend>() {
    exercise(|| {
        let (tx, mut rx) = brokerless::channel::<u64>()
            .backend::<B>()
            .producers(3)
            .consumers(1)
            .capacity(4)
            .batch(1)
            .open()
            .unwrap();
        (tx, rx.pop().unwrap())
    });
}
fn locked<B: Backend>() {
    exercise(|| {
        let (reg, mut rx) = dynamic::locked::<u64>()
            .backend::<B>()
            .consumers(1)
            .capacity(4)
            .batch(1)
            .open()
            .unwrap();
        ((0..3).map(|_| reg.register()).collect(), rx.pop().unwrap())
    });
}
fn array<B: Backend>() {
    exercise(|| {
        let (reg, mut rx) = dynamic::array::<u64>(3)
            .backend::<B>()
            .consumers(1)
            .capacity(4)
            .batch(1)
            .open()
            .unwrap();
        ((0..3).map(|_| reg.register()).collect(), rx.pop().unwrap())
    });
}

fn late<P: SendValue, C: Receive>(mut first: P, mut rx: C, mut register: impl FnMut() -> P) {
    first.send_value(0);
    assert_eq!(rx.one(), Some(0));
    first.send_value(1);
    let mut second = register();
    second.send_value(1000);
    let mut found = false;
    for expected in 1..65 {
        let value = mixed_one(&mut rx, expected).unwrap();
        if value == 1000 {
            found = true;
            break;
        }
        assert_eq!(value, expected as u64);
        first.send_value(expected as u64 + 1);
    }
    assert!(found, "preferred ring hid a newly registered producer");
}
fn late_locked<B: Backend>() {
    let (reg, mut rx) = dynamic::locked::<u64>()
        .backend::<B>()
        .consumers(1)
        .capacity(4)
        .batch(1)
        .open()
        .unwrap();
    late(reg.register(), rx.pop().unwrap(), || reg.register());
}
fn late_array<B: Backend>() {
    let (reg, mut rx) = dynamic::array::<u64>(2)
        .backend::<B>()
        .consumers(1)
        .capacity(4)
        .batch(1)
        .open()
        .unwrap();
    late(reg.register(), rx.pop().unwrap(), || reg.register());
}

#[test]
fn fixed_ring_locality() {
    fixed::<Ring>();
}
#[test]
fn fixed_seg_locality() {
    fixed::<Seg>();
}
#[test]
fn locked_ring_locality() {
    locked::<Ring>();
}
#[test]
fn locked_seg_locality() {
    locked::<Seg>();
}
#[test]
fn array_ring_locality() {
    array::<Ring>();
}
#[test]
fn array_seg_locality() {
    array::<Seg>();
}
#[test]
fn late_locked_ring() {
    late_locked::<Ring>();
}
#[test]
fn late_locked_seg() {
    late_locked::<Seg>();
}
#[test]
fn late_array_ring() {
    late_array::<Ring>();
}
#[test]
fn late_array_seg() {
    late_array::<Seg>();
}
fn rejected_registration<B: Backend>() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, Barrier};

    let (registrar, mut rx) = dynamic::array::<u64>(1)
        .backend::<B>()
        .capacity(1)
        .batch(1)
        .open()
        .unwrap();
    let mut tx = registrar.register();
    assert!(catch_unwind(AssertUnwindSafe(|| registrar.register())).is_err());
    assert!(tx.send(7));
    assert_eq!(rx[0].recv(), Some(7));
    drop(tx);
    drop(registrar);
    assert_eq!(rx[0].recv(), None);

    let (registrar, mut rx) = dynamic::array::<u64>(4)
        .backend::<B>()
        .capacity(1)
        .batch(1)
        .open()
        .unwrap();
    let gate = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8u64)
        .map(|id| {
            let registrar = registrar.clone();
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                catch_unwind(AssertUnwindSafe(|| {
                    let mut tx = registrar.register();
                    assert!(tx.send(id));
                    id
                }))
                .ok()
            })
        })
        .collect();
    let mut accepted: Vec<_> = workers
        .into_iter()
        .filter_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(accepted.len(), 4);
    drop(registrar);
    let mut received = Vec::new();
    while let Some(value) = rx[0].recv() {
        received.push(value);
    }
    accepted.sort_unstable();
    received.sort_unstable();
    assert_eq!(received, accepted);
}

#[test]
fn rejected_array_registration_preserves_ring_scans() {
    rejected_registration::<Ring>();
}

#[test]
fn rejected_array_registration_preserves_seg_scans() {
    rejected_registration::<Seg>();
}
