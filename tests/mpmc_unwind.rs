//! Unwinding a brokerless batch must release its exclusive ring claim.
use prescient::backend::{Backend, Batch, Ring, Rx, Seg};
use prescient::mpmc::brokerless::{self, dynamic};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Tracked {
    id: usize,
    drops: Arc<[AtomicUsize; 10]>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Ordering::Relaxed);
    }
}

trait Receive: Sized {
    fn receive(&mut self) -> Option<Tracked>;
    fn owned_batch(&mut self, out: &mut Batch<Tracked>) -> usize;
    fn drain_values(&mut self, max: usize, f: impl FnMut(Tracked)) -> usize;
    fn run_values(&mut self, f: impl FnMut(Tracked));
    fn batch(&mut self, batch: usize);
}

impl<B: Backend> Receive for brokerless::Consumer<Tracked, B> {
    fn receive(&mut self) -> Option<Tracked> {
        self.try_recv()
    }
    fn owned_batch(&mut self, out: &mut Batch<Tracked>) -> usize {
        self.try_recv_batch(out)
    }
    fn drain_values(&mut self, max: usize, f: impl FnMut(Tracked)) -> usize {
        self.drain(max, f)
    }
    fn run_values(&mut self, f: impl FnMut(Tracked)) {
        self.for_each(4, f);
    }
    fn batch(&mut self, batch: usize) {
        self.set_batch(batch);
    }
}

impl<B: Backend, R: dynamic::Rings<Tracked, B>> Receive for dynamic::Consumer<Tracked, B, R> {
    fn receive(&mut self) -> Option<Tracked> {
        self.try_recv()
    }
    fn owned_batch(&mut self, out: &mut Batch<Tracked>) -> usize {
        self.try_recv_batch(out)
    }
    fn drain_values(&mut self, max: usize, f: impl FnMut(Tracked)) -> usize {
        self.drain(max, f)
    }
    fn run_values(&mut self, f: impl FnMut(Tracked)) {
        self.for_each(4, f);
    }
    fn batch(&mut self, batch: usize) {
        self.set_batch(batch);
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    Callback,
    StagedCallback,
    ForEach,
    Backend,
    BatchBackend,
}

fn exercise(
    mut owner: impl Receive,
    mut peer: impl Receive + Send + 'static,
    mut send: impl FnMut(Tracked),
    scenario: Scenario,
) {
    let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    for id in 0..8 {
        send(Tracked {
            id,
            drops: Arc::clone(&drops),
        });
    }
    assert_eq!(owner.drain_values(0, |_| panic!("zero max called back")), 0);
    let mut seen = Vec::new();
    if matches!(scenario, Scenario::StagedCallback) {
        seen.push(owner.receive().unwrap().id); // Leave 1, 2, 3 privately staged.
    }
    let panic = catch_unwind(AssertUnwindSafe(|| {
        if matches!(scenario, Scenario::BatchBackend) {
            let mut out = Batch::new();
            owner.owned_batch(&mut out); // Partially staged values stay owned on unwind.
        } else if matches!(scenario, Scenario::Backend) {
            owner.receive(); // Backend stages one value, then unwinds.
        } else {
            let mut callback = |value: Tracked| {
                seen.push(value.id);
                if value.id == 2 {
                    panic!("callback panic");
                }
            };
            if matches!(scenario, Scenario::ForEach) {
                owner.run_values(&mut callback);
            } else {
                owner.drain_values(8, &mut callback);
            }
        }
    }));
    assert!(panic.is_err());
    // Keep the original consumer alive. A different thread must be able to
    // claim the same ring after unwind; this is bounded, never a hanging recv.
    peer.batch(1);
    let (mut peer, value) = std::thread::spawn(move || {
        let value = peer.receive();
        (peer, value)
    })
    .join()
    .unwrap();
    seen.push(value.expect("panic stranded the ring claim").id);
    if matches!(scenario, Scenario::BatchBackend) {
        let mut out = Batch::new();
        loop {
            let got = owner.owned_batch(&mut out);
            assert_eq!(got, out.len()); // Never trust a backend's reported count.
            seen.extend(out.as_slice().iter().map(|value| value.id));
            if got == 0 {
                break;
            }
        }
    } else {
        while let Some(value) = owner.receive() {
            seen.push(value.id);
        }
    }
    while let Some(value) = peer.receive() {
        seen.push(value.id);
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
    // Released ring capacity must remain usable after the callback's unwind.
    for id in 8..10 {
        send(Tracked {
            id,
            drops: Arc::clone(&drops),
        });
    }
    for id in 8..10 {
        assert_eq!(owner.receive().unwrap().id, id);
    }
    assert!(owner.receive().is_none());
    assert!(peer.receive().is_none());
    drop(owner);
    drop(peer);
    drop(send);
    for (id, count) in drops.iter().enumerate() {
        assert_eq!(count.load(Ordering::Relaxed), 1, "drop count for {id}");
    }
}

fn fixed<B: Backend>(scenario: Scenario) {
    let (mut producers, mut consumers) = brokerless::channel::<Tracked>()
        .backend::<B>()
        .producers(1)
        .consumers(2)
        .capacity(8)
        .batch(4)
        .open()
        .unwrap();
    let owner = consumers.pop().unwrap();
    let peer = consumers.pop().unwrap();
    exercise(
        owner,
        peer,
        move |v| assert!(producers[0].try_send(v).is_ok()),
        scenario,
    );
}

fn locked<B: Backend>(scenario: Scenario) {
    let (registrar, mut consumers) = dynamic::locked::<Tracked>()
        .backend::<B>()
        .consumers(2)
        .capacity(8)
        .batch(4)
        .open()
        .unwrap();
    let mut producer = registrar.register();
    drop(registrar);
    let owner = consumers.pop().unwrap();
    let peer = consumers.pop().unwrap();
    exercise(
        owner,
        peer,
        move |v| assert!(producer.try_send(v).is_ok()),
        scenario,
    );
}

fn array<B: Backend>(scenario: Scenario) {
    let (registrar, mut consumers) = dynamic::array::<Tracked>(1)
        .backend::<B>()
        .consumers(2)
        .capacity(8)
        .batch(4)
        .open()
        .unwrap();
    let mut producer = registrar.register();
    drop(registrar);
    let owner = consumers.pop().unwrap();
    let peer = consumers.pop().unwrap();
    exercise(
        owner,
        peer,
        move |v| assert!(producer.try_send(v).is_ok()),
        scenario,
    );
}

macro_rules! callback_test {
    ($name:ident, $factory:ident, $backend:ty) => {
        #[test]
        fn $name() {
            for scenario in [
                Scenario::Callback,
                Scenario::StagedCallback,
                Scenario::ForEach,
            ] {
                $factory::<$backend>(scenario);
            }
        }
    };
}
callback_test!(fixed_ring_callback_unwind, fixed, Ring);
callback_test!(fixed_seg_callback_unwind, fixed, Seg);
callback_test!(locked_ring_callback_unwind, locked, Ring);
callback_test!(locked_seg_callback_unwind, locked, Seg);
callback_test!(array_ring_callback_unwind, array, Ring);
callback_test!(array_seg_callback_unwind, array, Seg);

// A public backend may unwind after safely handing a value to its callback.
// Exercise refill as well as the user-callback drain path.
struct PanicOnce<B>(std::marker::PhantomData<B>);
struct PanicRx<R> {
    inner: R,
    fired: bool,
}

impl<B: Backend> Backend for PanicOnce<B> {
    type Tx<T: Send> = B::Tx<T>;
    type Rx<T: Send> = PanicRx<B::Rx<T>>;
    fn channel<T: Send>(capacity: usize) -> (Self::Tx<T>, Self::Rx<T>) {
        let (tx, inner) = B::channel(capacity);
        (
            tx,
            PanicRx {
                inner,
                fired: false,
            },
        )
    }
}

impl<T: Send, R: Rx<T>> Rx<T> for PanicRx<R> {
    fn try_pop(&mut self) -> Option<T> {
        self.inner.try_pop()
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
    fn drain_in_place(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        if max != 0 && !self.fired {
            self.fired = true;
            self.inner.drain_in_place(max, |value| {
                f(value);
                panic!("backend panic after staging");
            })
        } else {
            self.inner.drain_in_place(max, f)
        }
    }
}

#[test]
fn fixed_refill_backend_unwind() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        fixed::<PanicOnce<Ring>>(scenario);
        fixed::<PanicOnce<Seg>>(scenario);
    }
}
#[test]
fn locked_refill_backend_unwind() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        locked::<PanicOnce<Ring>>(scenario);
        locked::<PanicOnce<Seg>>(scenario);
    }
}
#[test]
fn array_refill_backend_unwind() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        array::<PanicOnce<Ring>>(scenario);
        array::<PanicOnce<Seg>>(scenario);
    }
}

// Deliberately violate the safe trait's count/batch contract. This is a backend
// bug, but must not become memory unsafety in a caller's unchecked initialization.
struct Replacing;
struct ReplacingRx<R> {
    inner: R,
    fired: bool,
}
impl Backend for Replacing {
    type Tx<T: Send> = <Ring as Backend>::Tx<T>;
    type Rx<T: Send> = ReplacingRx<<Ring as Backend>::Rx<T>>;
    fn channel<T: Send>(capacity: usize) -> (Self::Tx<T>, Self::Rx<T>) {
        let (tx, inner) = Ring::channel(capacity);
        (
            tx,
            ReplacingRx {
                inner,
                fired: false,
            },
        )
    }
}
impl<T: Send, R: Rx<T>> Rx<T> for ReplacingRx<R> {
    fn try_pop(&mut self) -> Option<T> {
        self.inner.try_pop()
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
    fn drain_in_place(&mut self, max: usize, f: impl FnMut(T)) -> usize {
        self.inner.drain_in_place(max, f)
    }
    fn drain_into(&mut self, _max: usize, out: &mut Batch<T>) -> usize {
        let mut replacement = Batch::new();
        self.inner.drain_into(2, &mut replacement); // Can exceed requested max=1.
        *out = replacement;
        if !self.fired {
            self.fired = true;
            panic!("replaced destination, then unwound");
        }
        usize::MAX // Must never become Vec length or pointer arithmetic.
    }
}
#[test]
fn fixed_untrusted_bulk_destination() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        fixed::<Replacing>(scenario);
    }
}
#[test]
fn locked_untrusted_bulk_destination() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        locked::<Replacing>(scenario);
    }
}
#[test]
fn array_untrusted_bulk_destination() {
    for scenario in [Scenario::Backend, Scenario::BatchBackend] {
        array::<Replacing>(scenario);
    }
}
