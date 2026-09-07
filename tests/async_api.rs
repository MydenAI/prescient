//! Runtime-neutral async MPSC contract tests. Tokio is only the test executor;
//! Prescient's library API depends solely on `core::future`/`core::task`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use prescient::mpsc::{dynamic, fixed, pool};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("test runtime")
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn counter_waker(counter: &Arc<WakeCounter>) -> Waker {
    Waker::from(counter.clone())
}

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn batched_receive_releases_capacity_and_wakes_pending_sender() {
    for bulk in [false, true] {
        let (mut producers, mut receiver) = fixed::channel::<u64>()
            .capacity(8)
            .r#async()
            .open()
            .unwrap();
        for value in 0..8 {
            producers[0].try_send(value).unwrap();
        }
        let counter = Arc::new(WakeCounter::default());
        let waker = counter_waker(&counter);
        let mut pending = producers[0].send_async(8);
        assert!(poll_once(Pin::new(&mut pending), &waker).is_pending());
        let mut values = Vec::with_capacity(9);
        assert_eq!(receiver.try_recv_many(&mut values, 0), 0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        if bulk {
            assert_eq!(receiver.try_recv_many(&mut values, 8), 8);
        } else {
            values.push(receiver.try_recv().unwrap());
        }
        assert!(counter.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            poll_once(Pin::new(&mut pending), &waker),
            Poll::Ready(Ok(()))
        ));
        drop(pending);
        drop(producers);
        while let Some(value) = receiver.try_recv() {
            values.push(value);
        }
        assert_eq!(values, (0..9).collect::<Vec<_>>());
    }
}
#[test]
fn fixed_async_ready_path_and_disconnect() {
    runtime().block_on(async {
        let (mut producers, mut receiver) = fixed::channel::<u64>()
            .producers(1)
            .capacity(8)
            .r#async()
            .open()
            .unwrap();
        let mut producer = producers.pop().unwrap();
        producer.send_async(41).await.unwrap();
        assert_eq!(receiver.recv_async().await, Some(41));
        drop(producer);
        assert_eq!(receiver.recv_async().await, None);
    });
}

#[test]
fn explicit_async_axis_builds_task_waker_handles() {
    runtime().block_on(async {
        let (mut producers, mut receiver) = fixed::channel::<u64>()
            .r#async()
            .producers(1)
            .capacity(2)
            .open()
            .unwrap();
        producers[0].send_async(5).await.unwrap();
        assert_eq!(receiver.recv_async().await, Some(5));
        drop(producers);
        assert_eq!(receiver.recv_async().await, None);
    });
}

#[test]
fn full_sender_is_woken_when_receiver_releases_capacity() {
    runtime().block_on(async {
        let (mut producers, mut receiver) = fixed::channel::<u64>()
            .producers(1)
            .capacity(1)
            .r#async()
            .open()
            .unwrap();
        let mut producer = producers.pop().unwrap();
        producer.send_async(1).await.unwrap();
        let sender = tokio::spawn(async move {
            producer.send_async(2).await.unwrap();
            producer
        });
        tokio::task::yield_now().await;
        assert_eq!(receiver.recv_async().await, Some(1));
        let producer = sender.await.unwrap();
        assert_eq!(receiver.recv_async().await, Some(2));
        drop(producer);
        assert_eq!(receiver.recv_async().await, None);
    });
}

#[test]
fn receiver_drop_wakes_a_full_sender_with_its_item() {
    runtime().block_on(async {
        let (mut producers, receiver) = fixed::channel::<u64>()
            .producers(1)
            .capacity(1)
            .r#async()
            .open()
            .unwrap();
        let mut producer = producers.pop().unwrap();
        producer.send_async(1).await.unwrap();
        let blocked = tokio::spawn(async move { producer.send_async(2).await });
        tokio::task::yield_now().await;
        drop(receiver);
        assert_eq!(blocked.await.unwrap(), Err(2));
    });
}

#[test]
fn canceled_send_does_not_poison_the_shard_waker() {
    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    let mut producer = producers.pop().unwrap();
    producer.try_send(1).unwrap();

    let wakes = Arc::new(WakeCounter::default());
    let waker = counter_waker(&wakes);
    let mut canceled = Box::pin(producer.send_async(2));
    assert!(poll_once(canceled.as_mut(), &waker).is_pending());
    drop(canceled);

    assert_eq!(receiver.try_recv(), Some(1));
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        0,
        "canceled task must be deregistered"
    );
    producer.try_send(3).unwrap();
    assert_eq!(receiver.try_recv(), Some(3));
}

#[test]
fn panicking_async_drain_commits_space_and_wakes_sender() {
    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(2)
        .r#async()
        .open()
        .unwrap();
    let mut producer = producers.pop().unwrap();
    producer.try_send(1).unwrap();
    producer.try_send(2).unwrap();

    let wakes = Arc::new(WakeCounter::default());
    let waker = counter_waker(&wakes);
    let mut blocked = Box::pin(producer.send_async(3));
    assert!(poll_once(blocked.as_mut(), &waker).is_pending());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.try_drain(|_| panic!("probe"));
    }));
    assert!(panic.is_err());
    assert!(wakes.0.load(Ordering::Relaxed) >= 1);
    assert_eq!(poll_once(blocked.as_mut(), &waker), Poll::Ready(Ok(())));
}

#[test]
fn canceled_receive_is_replaced_by_the_next_tasks_waker() {
    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    let mut producer = producers.pop().unwrap();
    let old_wakes = Arc::new(WakeCounter::default());
    let new_wakes = Arc::new(WakeCounter::default());
    let old_waker = counter_waker(&old_wakes);
    let new_waker = counter_waker(&new_wakes);

    let mut old = Box::pin(receiver.recv_async());
    assert!(poll_once(old.as_mut(), &old_waker).is_pending());
    drop(old);
    let mut current = Box::pin(receiver.recv_async());
    assert!(poll_once(current.as_mut(), &new_waker).is_pending());
    producer.try_send(9).unwrap();

    assert_eq!(old_wakes.0.load(Ordering::Relaxed), 0);
    assert!(new_wakes.0.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        poll_once(current.as_mut(), &new_waker),
        Poll::Ready(Some(9))
    );
}

#[test]
fn dynamic_async_registration_and_final_close() {
    runtime().block_on(async {
        let (registrar, mut receiver) = dynamic::channel::<u64>()
            .capacity(2)
            .recycling(2)
            .r#async()
            .open()
            .unwrap();
        let mut producer = registrar.register();
        let task = tokio::spawn(async move {
            for value in 0..100 {
                producer.send_async(value).await.unwrap();
            }
        });
        drop(registrar);
        let mut sum = 0;
        while let Some(value) = receiver.recv_async().await {
            sum += value;
        }
        task.await.unwrap();
        assert_eq!(sum, (0..100).sum());
    });
}

#[test]
fn producer_cleans_abandoned_registration_before_releasing_ring() {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    struct OnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);
    // Waker::noop() cannot exercise destruction of a registered user waker.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for OnDrop {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for OnDrop {
        fn drop(&mut self) {
            (self.0.take().unwrap())();
        }
    }

    let (pool, receiver) = pool::channel::<u64>()
        .max_producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    let mut producer = pool.claim().unwrap();
    producer.try_send(0).unwrap();
    let receiver = Arc::new(Mutex::new(receiver));
    let released_too_early = Arc::new(AtomicBool::new(false));
    let on_drop_receiver = receiver.clone();
    let on_drop_pool = pool.clone();
    let observed = released_too_early.clone();
    let waker = Waker::from(Arc::new(OnDrop(Some(Box::new(move || {
        // Cleanup may run user code. A recycled slot must not be claimable
        // until cleanup of the previous producer's registration has finished.
        assert_eq!(on_drop_receiver.lock().unwrap().try_recv(), Some(0));
        assert_eq!(on_drop_receiver.lock().unwrap().try_recv(), None);
        observed.store(on_drop_pool.claim().is_some(), Ordering::Relaxed);
    })))));
    let mut pending = producer.send_async(1);
    assert!(poll_once(Pin::new(&mut pending), &waker).is_pending());
    // Safe Rust can abandon a future; endpoint teardown still owns cleanup.
    std::mem::forget(pending);
    drop(waker);
    drop(producer);
    assert!(!released_too_early.load(Ordering::Relaxed));
    assert_eq!(receiver.lock().unwrap().try_recv(), None);
    assert!(pool.claim().is_some());
}
#[test]
fn async_pool_drop_publishes_recyclability_before_waking() {
    use std::sync::Mutex;

    struct RunOnWake(Mutex<Box<dyn FnMut() + Send>>);
    impl Wake for RunOnWake {
        fn wake(self: Arc<Self>) {
            (self.0.lock().unwrap())();
        }
    }

    let (pool, receiver) = pool::channel::<u64>()
        .max_producers(1)
        .capacity(2)
        .r#async()
        .open()
        .unwrap();
    let producer = pool.claim().unwrap();
    let receiver = Arc::new(Mutex::new(receiver));
    let on_wake = receiver.clone();
    // Model an executor which immediately runs the receiver when notified.
    // Merely polling after drop() returns hides an incorrectly ordered wake.
    let waker = Waker::from(Arc::new(RunOnWake(Mutex::new(Box::new(move || {
        assert_eq!(on_wake.lock().unwrap().try_recv(), None);
    })))));
    assert!(
        receiver
            .lock()
            .unwrap()
            .poll_recv(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(producer);
    assert!(
        pool.claim().is_some(),
        "the notified receiver must be able to recycle"
    );
}
#[test]
fn async_pool_recycles_an_empty_dropped_producer() {
    let (pool, mut receiver) = pool::channel::<u64>()
        .max_producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    let producer = pool.claim().unwrap();

    let wakes = Arc::new(WakeCounter::default());
    let waker = counter_waker(&wakes);
    let mut pending = Box::pin(receiver.recv_async());
    assert!(poll_once(pending.as_mut(), &waker).is_pending());
    drop(producer);
    assert!(wakes.0.load(Ordering::Relaxed) >= 1);

    // One receive pass observes the empty finished shard and returns it to the
    // pool even though the pool handle keeps the channel logically open.
    assert!(poll_once(pending.as_mut(), &waker).is_pending());
    assert!(pool.claim().is_some());
}

#[test]
fn async_fan_in_delivers_every_value_once_under_backpressure() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: u64 = 25_000;
    runtime().block_on(async {
        let (producers, mut receiver) = fixed::channel::<(usize, u64)>()
            .producers(PRODUCERS)
            .capacity(16)
            .r#async()
            .open()
            .unwrap();
        let tasks: Vec<_> = producers
            .into_iter()
            .enumerate()
            .map(|(id, mut producer)| {
                tokio::spawn(async move {
                    for sequence in 0..PER_PRODUCER {
                        producer.send_async((id, sequence)).await.unwrap();
                    }
                })
            })
            .collect();

        let mut counts = [0_u64; PRODUCERS];
        let mut sums = [0_u128; PRODUCERS];
        while let Some((id, sequence)) = receiver.recv_async().await {
            counts[id] += 1;
            sums[id] += sequence as u128;
        }
        for task in tasks {
            task.await.unwrap();
        }
        let expected = (PER_PRODUCER as u128 - 1) * PER_PRODUCER as u128 / 2;
        assert_eq!(counts, [PER_PRODUCER; PRODUCERS]);
        assert_eq!(sums, [expected; PRODUCERS]);
    });
}

#[test]
fn async_batch_fan_in_and_bulk_receive_deliver_exactly_once() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: u64 = 25_000;
    runtime().block_on(async {
        let (producers, mut receiver) = fixed::channel::<(usize, u64)>()
            .producers(PRODUCERS)
            .capacity(64)
            .r#async()
            .open()
            .unwrap();
        let tasks: Vec<_> = producers
            .into_iter()
            .enumerate()
            .map(|(id, mut producer)| {
                tokio::spawn(async move {
                    producer
                        .send_batch_async((0..PER_PRODUCER).map(|sequence| (id, sequence)))
                        .await
                        .unwrap();
                })
            })
            .collect();

        let mut counts = [0_u64; PRODUCERS];
        let mut sums = [0_u128; PRODUCERS];
        let mut batch = Vec::with_capacity(256);
        loop {
            batch.clear();
            let received = receiver.recv_many_async(&mut batch, 256).await;
            if received == 0 {
                break;
            }
            assert_eq!(received, batch.len());
            for &(id, sequence) in &batch {
                counts[id] += 1;
                sums[id] += sequence as u128;
            }
        }
        for task in tasks {
            task.await.unwrap();
        }
        let expected = (PER_PRODUCER as u128 - 1) * PER_PRODUCER as u128 / 2;
        assert_eq!(counts, [PER_PRODUCER; PRODUCERS]);
        assert_eq!(sums, [expected; PRODUCERS]);
    });
}

#[test]
fn panicking_batch_iterator_wakes_receiver_for_published_prefix() {
    struct OneThenPanic(bool);

    impl Iterator for OneThenPanic {
        type Item = u64;

        fn next(&mut self) -> Option<Self::Item> {
            if self.0 {
                panic!("iterator probe");
            }
            self.0 = true;
            Some(7)
        }
    }

    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(8)
        .r#async()
        .open()
        .unwrap();
    let mut producer = producers.pop().unwrap();
    let receive_wakes = Arc::new(WakeCounter::default());
    let receive_waker = counter_waker(&receive_wakes);
    let mut receive = Box::pin(receiver.recv_async());
    assert!(poll_once(receive.as_mut(), &receive_waker).is_pending());

    let send_waker = Waker::noop();
    let mut send = Box::pin(producer.send_batch_async(OneThenPanic(false)));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_once(send.as_mut(), send_waker);
    }));
    assert!(panic.is_err());
    drop(send);

    assert!(receive_wakes.0.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        poll_once(receive.as_mut(), &receive_waker),
        Poll::Ready(Some(7))
    );
}

#[test]
fn unpolled_receive_future_clears_a_previous_direct_poll_registration() {
    for bulk in [false, true] {
        let (mut producers, mut receiver) = fixed::channel::<u64>()
            .producers(1)
            .capacity(1)
            .r#async()
            .open()
            .unwrap();
        let wakes = Arc::new(WakeCounter::default());
        let waker = counter_waker(&wakes);
        assert!(
            receiver
                .poll_recv(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if bulk {
            let mut out = Vec::new();
            drop(receiver.recv_many_async(&mut out, 0));
        } else {
            drop(receiver.recv_async());
        }
        producers[0].try_send(7).unwrap();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert_eq!(receiver.try_recv(), Some(7));
    }
}

#[test]
fn send_and_receive_futures_are_send_for_task_migration() {
    fn assert_send<T: Send>(_: T) {}

    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    assert_send(producers[0].send_async(1));
    assert_send(receiver.recv_async());

    let (mut producers, mut receiver) = fixed::channel::<u64>()
        .producers(1)
        .capacity(1)
        .r#async()
        .open()
        .unwrap();
    let mut batch = Vec::new();
    assert_send(producers[0].send_batch_async(0..1));
    assert_send(receiver.recv_many_async(&mut batch, 1));
}
