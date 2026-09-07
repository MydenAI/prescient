#![cfg(not(loom))]
use prescient::backend::{Backend, Batch, Ring, Rx, Seg, Tx};
use prescient::mpmc::brokerless;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Limited<T> {
    values: Vec<T>,
    limit: usize,
    panic_at: Option<usize>,
}
impl<T: Send> Tx<T> for Limited<T> {
    fn try_push(&mut self, value: T) -> Result<(), T> {
        assert_ne!(
            self.panic_at,
            Some(self.values.len()),
            "injected push panic"
        );
        if self.values.len() == self.limit {
            return Err(value);
        }
        self.values.push(value);
        Ok(())
    }
    fn is_consumer_gone(&self) -> bool {
        false
    }
}
fn batch<T>(values: impl IntoIterator<Item = T>) -> Batch<T> {
    let mut source = Batch::new();
    for value in values {
        source.push_back(value);
    }
    source
}
#[test]
fn default_push_from_preserves_rejected_suffix_and_limits() {
    let mut tx = Limited {
        values: Vec::new(),
        limit: 2,
        panic_at: None,
    };
    let mut source = batch(["moved", "a", "b", "c"].map(String::from));
    drop(source.pop_front());
    assert_eq!(tx.push_from(0, &mut source), 0);
    assert_eq!(tx.push_from(1, &mut source), 1);
    assert_eq!(tx.push_from(10, &mut source), 1);
    assert_eq!(tx.push_from(10, &mut source), 0);
    assert_eq!(tx.values, ["a", "b"]);
    assert_eq!(source.as_slice(), ["c"]);
    tx.limit = 3;
    assert_eq!(tx.push_from(10, &mut source), 1);
    assert!(source.is_empty());
    source.clear();
    assert_eq!(tx.push_from(usize::MAX, &mut source), 0);
}
#[test]
fn default_push_panic_preserves_all_ownership() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Value(usize, Arc<Vec<AtomicUsize>>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.1[self.0].fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new((0..5).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut source = batch((0..5).map(|id| Value(id, drops.clone())));
    let mut tx = Limited {
        values: Vec::new(),
        limit: 5,
        panic_at: Some(1),
    };
    assert!(catch_unwind(AssertUnwindSafe(|| tx.push_from(5, &mut source))).is_err());
    assert_eq!(
        source.as_slice().iter().map(|v| v.0).collect::<Vec<_>>(),
        [2, 3, 4]
    );
    drop(source);
    drop(tx);
    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
}
#[test]
fn segmented_default_roundtrip() {
    let (mut tx, mut rx) = Seg::channel(2);
    let mut source = batch((0..19).map(|id| format!("item-{id}")));
    assert_eq!(tx.push_from(7, &mut source), 7);
    assert_eq!(tx.push_from(usize::MAX, &mut source), 12);
    assert!(source.is_empty());
    for id in 0..19 {
        assert_eq!(rx.try_pop(), Some(format!("item-{id}")));
    }
    drop(tx);
    assert!(rx.is_finished());
    let (mut closed, receiver) = Seg::channel(2);
    drop(receiver);
    source.clear();
    source.push_back(String::from("kept"));
    assert_eq!(closed.push_from(usize::MAX, &mut source), 0);
    assert_eq!(source.as_slice(), ["kept"]);
}
#[test]
fn fixed_and_dynamic_owned_batches_exactly_once() {
    macro_rules! check {
        ($producers:expr, $consumers:expr) => {{
            let producers = $producers;
            let consumers = $consumers;
            let mut got = std::thread::scope(|scope| {
                let senders = producers
                    .into_iter()
                    .enumerate()
                    .map(|(index, mut tx)| {
                        scope.spawn(move || {
                            let mut source = batch(index * 97..(index + 1) * 97);
                            assert!(tx.send_batch(&mut source));
                            assert!(source.is_empty());
                        })
                    })
                    .collect::<Vec<_>>();
                let receivers = consumers
                    .into_iter()
                    .map(|mut rx| {
                        scope.spawn(move || std::iter::from_fn(|| rx.recv()).collect::<Vec<_>>())
                    })
                    .collect::<Vec<_>>();
                for sender in senders {
                    sender.join().unwrap();
                }
                receivers
                    .into_iter()
                    .flat_map(|rx| rx.join().unwrap())
                    .collect::<Vec<_>>()
            });
            got.sort_unstable();
            assert_eq!(got, (0..291).collect::<Vec<_>>());
        }};
    }
    for cap in [1, 4, 64] {
        let (tx, rx) = brokerless::channel::<usize>()
            .producers(3)
            .consumers(2)
            .capacity(cap)
            .batch(3)
            .open()
            .unwrap();
        check!(tx, rx);
        let (reg, rx) = brokerless::dynamic::locked::<usize>()
            .consumers(2)
            .capacity(cap)
            .batch(3)
            .open()
            .unwrap();
        let tx = (0..3).map(|_| reg.register()).collect::<Vec<_>>();
        drop(reg);
        check!(tx, rx);
        let (reg, rx) = brokerless::dynamic::array::<usize>(3)
            .consumers(2)
            .capacity(cap)
            .batch(3)
            .open()
            .unwrap();
        let tx = (0..3).map(|_| reg.register()).collect::<Vec<_>>();
        drop(reg);
        check!(tx, rx);
    }
}
#[test]
fn disconnected_owned_batch_keeps_unsent_suffix() {
    macro_rules! check {
        ($tx:expr, $rx:expr) => {{
            let mut tx = $tx;
            drop($rx);
            let mut source = batch(0..17);
            assert!(!tx.send_batch(&mut source));
            let remaining = source.as_slice();
            assert!(!remaining.is_empty());
            assert_eq!(
                remaining,
                &(0..17).collect::<Vec<_>>()[17 - remaining.len()..]
            );
            source.clear();
            assert!(tx.send_batch(&mut source));
        }};
    }
    let (mut tx, rx) = brokerless::channel::<usize>()
        .producers(1)
        .consumers(1)
        .capacity(4)
        .open()
        .unwrap();
    check!(tx.pop().unwrap(), rx);
    let (reg, rx) = brokerless::dynamic::locked::<usize>()
        .consumers(1)
        .capacity(4)
        .open()
        .unwrap();
    check!(reg.register(), rx);
    let (reg, rx) = brokerless::dynamic::array::<usize>(1)
        .consumers(1)
        .capacity(4)
        .open()
        .unwrap();
    check!(reg.register(), rx);
}

struct LyingCount;
struct LyingTx<T: Send>(<Ring as Backend>::Tx<T>);
impl Backend for LyingCount {
    type Tx<T: Send> = LyingTx<T>;
    type Rx<T: Send> = <Ring as Backend>::Rx<T>;
    fn channel<T: Send>(capacity: usize) -> (Self::Tx<T>, Self::Rx<T>) {
        let (tx, rx) = Ring::channel(capacity);
        (LyingTx(tx), rx)
    }
}
impl<T: Send> Tx<T> for LyingTx<T> {
    fn try_push(&mut self, value: T) -> Result<(), T> {
        self.0.try_push(value)
    }
    fn is_consumer_gone(&self) -> bool {
        self.0.is_consumer_gone()
    }
    fn push_from(&mut self, max: usize, source: &mut Batch<T>) -> usize {
        self.0.push_from(max, source);
        usize::MAX
    }
}
#[test]
fn custom_backend_reported_count_does_not_control_ownership() {
    let (mut tx, rx) = brokerless::channel::<String>()
        .backend::<LyingCount>()
        .producers(1)
        .consumers(1)
        .capacity(1)
        .open()
        .unwrap();
    let mut source = batch(["a", "b", "c"].map(String::from));
    drop(rx);
    assert!(!tx[0].send_batch(&mut source));
    assert_eq!(source.as_slice(), ["b", "c"]);
}
