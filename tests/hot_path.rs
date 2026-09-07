//! Boundary regressions for staged fast paths and division-free shard scanning.

use prescient::mpmc::{brokered, brokerless};
use prescient::mpsc::{self, dynamic, fixed};

fn fixed_removal<K: mpsc::Kernel>(
    mut producers: Vec<mpsc::Producer<u64, K>>,
    mut receiver: mpsc::Receiver<u64, K, mpsc::FixedState>,
    mode: usize,
) {
    receiver.set_batch(1);
    producers[0].try_send(0).unwrap();
    producers[1].try_send(1).unwrap();
    assert_eq!(receiver.try_recv(), Some(0));
    assert_eq!(receiver.try_recv(), Some(1));
    // Cursor now targets the last slot. Remove that empty slot during the next
    // scan: its old index is outside the resulting two-slot membership.
    drop(producers.pop());
    producers[0].try_send(10).unwrap();
    producers[1].try_send(11).unwrap();
    let mut values = Vec::new();
    for _ in 0..4 {
        match mode {
            0 => values.extend(receiver.try_recv()),
            1 => {
                receiver.try_recv_many(&mut values, 1);
            }
            _ => {
                receiver.try_drain(|v| values.push(v));
            }
        }
    }
    assert_eq!(values, [10, 11]);
    drop(producers);
    for _ in 0..4 {
        assert_eq!(receiver.try_recv(), None);
        assert_eq!(receiver.try_recv_many(&mut values, 2), 0);
        assert_eq!(receiver.try_drain(|_| panic!("already drained")), 0);
    }
    assert!(receiver.is_disconnected());
}

#[test]
fn fixed_sync_and_async_removal_at_end_of_scan() {
    for mode in 0..3 {
        let (producers, receiver) = fixed::channel().producers(3).capacity(4).open().unwrap();
        fixed_removal(producers, receiver, mode);
        let (producers, receiver) = fixed::channel()
            .producers(3)
            .capacity(4)
            .r#async()
            .open()
            .unwrap();
        fixed_removal(producers, receiver, mode);
    }
}

#[test]
fn dynamic_mpsc_empty_grow_and_staged_bulk_transition() {
    for batch in [1, 4] {
        let (registrar, mut receiver) = dynamic::channel::<u64>()
            .capacity(8)
            .recycling(3)
            .open()
            .unwrap();
        receiver.set_batch(batch);
        for round in 0..8 {
            let mut producers: Vec<_> = (0..3).map(|_| registrar.register()).collect();
            for (p, producer) in producers.iter_mut().enumerate() {
                for seq in 0..4 {
                    producer
                        .try_send(round * 100 + p as u64 * 10 + seq)
                        .unwrap();
                }
            }
            drop(producers);
            let mut values = vec![receiver.try_recv().unwrap()];
            let before = values.len();
            assert_eq!(receiver.try_recv_many(&mut values, 0), 0);
            assert_eq!(values.len(), before);
            for _ in 0..16 {
                receiver.try_recv_many(&mut values, 2);
            }
            assert_eq!(values.len(), 12);
            // Each producer retains FIFO across the staged/bulk transition.
            for p in 0..3 {
                let base = round * 100 + p * 10;
                assert_eq!(
                    values
                        .iter()
                        .copied()
                        .filter(|v| *v >= base && *v < base + 4)
                        .collect::<Vec<_>>(),
                    (base..base + 4).collect::<Vec<_>>()
                );
            }
            assert_eq!(receiver.try_recv(), None);
        }
        drop(registrar);
        assert_eq!(receiver.recv(), None);
    }
}

#[test]
fn brokerless_clone_seeds_survive_preferred_scans() {
    let (mut producers, mut consumers) = brokerless::channel::<usize>()
        .producers(3)
        .capacity(8)
        .open()
        .unwrap();
    let mut receiver = consumers.pop().unwrap();
    receiver.set_batch(1);
    for round in 0..8 {
        for (p, producer) in producers.iter_mut().enumerate() {
            producer.send(round * 10 + p);
        }
        let mut received = std::array::from_fn::<_, 3, _>(|_| receiver.try_recv().unwrap());
        received.sort_unstable();
        assert_eq!(received, [round * 10, round * 10 + 1, round * 10 + 2]);
        // Locality changes cross-producer service order, not delivery. Fresh
        // clones must still spread their starting offsets after repeated scans.
        for (p, producer) in producers.iter_mut().enumerate() {
            producer.send(round * 10 + p);
        }
        let mut first = receiver.clone();
        let mut second = first.clone();
        let mut third = second.clone();
        assert_eq!(first.try_recv(), Some(round * 10 + 1));
        assert_eq!(second.try_recv(), Some(round * 10 + 2));
        assert_eq!(third.try_recv(), Some(round * 10));
    }
    drop(producers);
    assert_eq!(receiver.recv(), None);
}

fn dynamic_mpmc_growth<R: brokerless::dynamic::Rings<u64, prescient::backend::Ring>>(
    registrar: brokerless::dynamic::Registrar<u64, prescient::backend::Ring, R>,
    mut receiver: brokerless::dynamic::Consumer<u64, prescient::backend::Ring, R>,
) {
    receiver.set_batch(3);
    assert_eq!(receiver.try_recv(), None);
    let mut producers = Vec::new();
    for p in 0..3 {
        producers.push(registrar.register());
        for seq in 0..3 {
            producers[p].send(p as u64 * 10 + seq);
        }
        assert_eq!(receiver.try_recv(), Some(p as u64 * 10));
        let mut staged = Vec::new();
        assert_eq!(receiver.drain(1, |v| staged.push(v)), 1);
        assert_eq!(staged, [p as u64 * 10 + 1]);
        assert_eq!(receiver.try_recv(), Some(p as u64 * 10 + 2));
        assert_eq!(receiver.try_recv(), None);
        // Both dynamic memberships normalize zero before the next refill.
        receiver.set_batch(0);
    }
    drop(producers);
    drop(registrar);
    assert_eq!(receiver.recv(), None);
}

#[test]
fn dynamic_mpmc_locked_and_array_growth() {
    let (registrar, mut receivers) = brokerless::dynamic::locked().capacity(8).open().unwrap();
    dynamic_mpmc_growth(registrar, receivers.pop().unwrap());
    let (registrar, mut receivers) = brokerless::dynamic::array(3).capacity(8).open().unwrap();
    dynamic_mpmc_growth(registrar, receivers.pop().unwrap());
}

#[test]
fn brokered_non_power_of_two_scan_and_delivery() {
    let (mut producers, mut consumers, brokers) = brokered::channel::<usize>()
        .producers(3)
        .consumers(3)
        .brokers(3)
        .capacity(16)
        .manual()
        .open()
        .unwrap();
    for (p, producer) in producers.iter_mut().enumerate() {
        for seq in 0..9 {
            producer.send(p * 100 + seq);
        }
    }
    drop(producers);
    // All outputs fit, so the manual brokers can finish without worker threads.
    for broker in brokers {
        broker.run();
    }
    for (c, consumer) in consumers.iter_mut().enumerate() {
        for round in 0..3 {
            for p in 0..3 {
                assert_eq!(consumer.try_recv(), Some(p * 100 + round * 3 + c));
            }
        }
        assert_eq!(consumer.recv(), None);
    }
}
fn brokerless_staging_transitions<B: prescient::backend::Backend>() {
    let (mut producers, mut consumers) = brokerless::channel::<usize>()
        .backend::<B>()
        .producers(1)
        .capacity(64)
        .batch(4)
        .open()
        .unwrap();
    let mut receiver = consumers.pop().unwrap();
    for value in 0..32 {
        assert!(producers[0].send(value));
    }
    assert_eq!(receiver.try_recv(), Some(0)); // 1..4 remain private to receiver.
    let mut clone = receiver.clone();
    clone.set_batch(1);
    assert_eq!(clone.try_recv(), Some(4)); // A clone never copies staged values.
    receiver.set_batch(0); // Normalize to one without discarding unread staging.
    assert_eq!(receiver.drain(0, |_| panic!("zero maximum")), 0);
    let mut values = Vec::new();
    assert_eq!(receiver.drain(2, |v| values.push(v)), 2);
    assert_eq!(values, [1, 2]);
    assert_eq!(receiver.try_recv(), Some(3));
    assert_eq!(receiver.try_recv(), Some(5));
    values.clear();
    receiver.set_batch(4096); // Grow only to actual data, not the configured bound.
    assert_eq!(receiver.try_recv(), Some(6));
    receiver.set_batch(1);
    drop(producers);
    receiver.for_each(3, |v| values.push(v));
    assert_eq!(values, (7..32).collect::<Vec<_>>());
    assert_eq!(clone.recv(), None);
    assert_eq!(receiver.recv(), None);
}

#[test]
fn brokerless_staging_batch_changes_clone_and_bulk_mix() {
    brokerless_staging_transitions::<prescient::backend::Ring>();
    brokerless_staging_transitions::<prescient::backend::Seg>();
}
