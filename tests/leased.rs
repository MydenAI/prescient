use prescient::{Channel, mpmc::brokerless::leased};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn channel<T: Send>(
    p: usize,
    c: usize,
    capacity: usize,
    batch: usize,
) -> (Vec<leased::Producer<T>>, Vec<leased::Consumer<T>>) {
    leased::channel()
        .producers(p)
        .consumers(c)
        .capacity(capacity)
        .batch(batch)
        .open()
        .unwrap()
}

#[test]
fn declaration_matches_shorthand_and_checks_shapes() {
    let _ = Channel::<u64>::new()
        .mpmc()
        .leased()
        .producers(2)
        .consumers(3)
        .capacity(12)
        .batch(3)
        .open()
        .unwrap();
    for (p, c, cap, b) in [
        (0, 1, 4, 2),
        (1, 0, 4, 2),
        (1, 1, 0, 2),
        (1, 1, 4, 0),
        (1, 1, 3, 2),
        (1, 1, 1, 2),
        (usize::MAX, 2, 8, 2),
        (1, usize::MAX, 8, 2),
        (1, 1, usize::MAX, 1),
    ] {
        assert!(
            leased::channel::<u64>()
                .producers(p)
                .consumers(c)
                .capacity(cap)
                .batch(b)
                .open()
                .is_err()
        );
    }
}

#[test]
fn same_allocation_commit_rollback_capacity_and_partial_blocks() {
    let (mut tx, mut rx) = channel::<u64>(1, 1, 3, 3);
    let mut w = tx[0].reserve().unwrap();
    let ptr = w.spare_capacity_mut().as_ptr().cast::<u64>();
    assert_eq!(w.capacity(), 3);
    assert!(w.extend_from_slice(&[1, 2]));
    assert!(!w.extend_from_slice(&[3, 4]));
    assert_eq!(w.as_slice(), &[1, 2]);
    w.as_mut_slice()[1] = 7;
    w.push(9).unwrap();
    assert_eq!(w.push(10), Err(10));
    assert_eq!(w.commit().unwrap(), 3);
    assert!(tx[0].try_reserve().is_none());
    let mut r = rx[0].recv().unwrap();
    assert_eq!(r.as_slice().as_ptr(), ptr);
    assert_eq!(r.as_slice(), &[1, 7, 9]);
    r.as_mut_slice()[0] = 11;
    assert_eq!(r.drain().collect::<Vec<_>>(), [11, 7, 9]);
    assert!(r.is_empty());
    r.release();
    assert!(tx[0].flush());
    let mut w = tx[0].reserve().unwrap();
    assert_eq!(w.spare_capacity_mut().as_ptr().cast::<u64>(), ptr);
    w.push(30).unwrap();
    drop(w); // rollback: no publication and no pool allocation lost
    assert!(rx[0].try_recv().is_none());
    let mut w = tx[0].reserve().unwrap();
    assert!(w.is_empty());
    w.push(40).unwrap();
    assert_eq!(w.commit().unwrap(), 1);
    assert_eq!(rx[0].recv().unwrap().as_slice(), &[40]);
    assert!(tx[0].flush());
    assert_eq!(tx[0].reserve().unwrap().commit().unwrap(), 0);
    assert!(rx[0].try_recv().is_none());
}

#[test]
fn direct_initialization_is_bounded_and_preserves_prefix() {
    let (mut tx, mut rx) = channel::<String>(1, 1, 2, 2);
    let mut w = tx[0].reserve().unwrap();
    w.spare_capacity_mut()[0].write(String::from("first"));
    // SAFETY: exactly one initialized, exclusively owned String in the spare prefix.
    unsafe { w.advance_initialized(1) };
    assert_eq!(w.spare_capacity_mut().len(), 1);
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: this deliberately out-of-bounds count panics before adoption;
        // the documented method contract promises a checked capacity bound.
        unsafe { w.advance_initialized(2) };
    }));
    assert!(result.is_err());
    assert_eq!(w.as_slice(), ["first"]);
    w.push(String::from("second")).unwrap();
    w.commit().unwrap();
    assert_eq!(rx[0].recv().unwrap().as_slice(), ["first", "second"]);
    assert!(tx[0].flush());
}

#[test]
fn held_read_block_does_not_hold_forward_claim_or_other_pool_slots() {
    let (mut tx, mut rx) = channel::<u64>(1, 2, 2, 1);
    for value in [1, 2] {
        let mut w = tx[0].reserve().unwrap();
        w.push(value).unwrap();
        w.commit().unwrap();
    }
    let (a, b) = rx.split_at_mut(1);
    let held = a[0].recv().unwrap();
    assert_eq!(held.as_slice(), &[1]);
    let peer = b[0].recv().unwrap();
    assert_eq!(peer.as_slice(), &[2]);
    assert!(tx[0].try_reserve().is_none());
    drop(peer);
    let mut w = tx[0].try_reserve().unwrap();
    w.push(3).unwrap();
    w.commit().unwrap();
    assert_eq!(b[0].recv().unwrap().as_slice(), &[3]);
    assert_eq!(held.as_slice(), &[1]);
    drop(held);
    assert!(tx[0].flush());
}

#[test]
fn returned_cold_lane_is_reused_while_peer_stays_hot() {
    for blocks in [2, 17, 33] {
        let (mut tx, mut rx) = channel::<u64>(1, 2, blocks, 1);
        for value in 0..blocks as u64 {
            let mut write = tx[0].reserve().unwrap();
            write.push(value).unwrap();
            write.commit().unwrap();
        }
        for _ in 1..blocks {
            drop(rx[0].recv().unwrap());
        }
        let cold = rx[1].recv().unwrap();
        let cold_ptr = cold.as_slice().as_ptr();
        drop(cold);

        let mut reused_cold = false;
        for value in 0..64 {
            let mut write = tx[0].try_reserve().expect("a returned block is ready");
            reused_cold |= write.spare_capacity_mut().as_ptr().cast::<u64>() == cold_ptr;
            write.push(value).unwrap();
            write.commit().unwrap();
            assert_eq!(rx[0].recv().unwrap().as_slice(), &[value]);
            if reused_cold {
                break;
            }
        }
        assert!(
            reused_cold,
            "a continuously hot peer must not hide a returned block"
        );
        assert!(tx[0].flush());
    }
}

#[test]
fn disconnect_drains_ready_blocks_and_failed_commit_retains_values() {
    let (mut tx, rx) = channel::<String>(1, 1, 2, 1);
    let mut w = tx[0].reserve().unwrap();
    w.push(String::from("unpublished")).unwrap();
    drop(rx);
    let failed = w.commit().unwrap_err();
    assert_eq!(failed.as_slice(), ["unpublished"]);
    drop(failed);
    assert!(tx[0].is_disconnected());
    assert!(tx[0].reserve().is_none());
    assert!(tx[0].flush());

    let (mut tx, mut rx) = channel::<u64>(1, 1, 2, 1);
    for value in [1, 2] {
        let mut w = tx[0].reserve().unwrap();
        w.push(value).unwrap();
        w.commit().unwrap();
    }
    drop(tx);
    assert!(rx[0].is_disconnected());
    assert_eq!(rx[0].recv().unwrap().as_slice(), &[1]);
    assert_eq!(rx[0].recv().unwrap().as_slice(), &[2]);
    assert!(rx[0].recv().is_none());
}

#[test]
fn normal_peer_drop_does_not_cancel_remaining_workers() {
    let (mut tx, mut rx) = channel::<u64>(2, 2, 2, 1);
    drop(tx.pop());
    drop(rx.pop());
    for value in 0..12 {
        let mut w = tx[0].reserve().unwrap();
        w.push(value).unwrap();
        w.commit().unwrap();
        assert_eq!(rx[0].recv().unwrap().as_slice(), &[value]);
    }
    assert!(tx[0].flush());
    drop(tx);
    assert!(rx[0].recv().is_none());
}

#[test]
fn flush_stops_when_consumers_drop_with_queued_blocks() {
    let (mut tx, rx) = channel::<u64>(1, 1, 1, 1);
    let mut w = tx[0].reserve().unwrap();
    w.push(1).unwrap();
    w.commit().unwrap();
    drop(rx);
    assert!(!tx[0].flush());
}

#[test]
fn flush_observes_last_consumer_return_before_disconnect() {
    let (mut tx, mut rx) = channel::<u64>(1, 1, 1, 1);
    let mut w = tx[0].reserve().unwrap();
    w.push(1).unwrap();
    w.commit().unwrap();
    drop(rx[0].recv().unwrap());
    drop(rx);
    assert!(tx[0].flush());
}

struct Tracked {
    drops: Arc<Vec<AtomicUsize>>,
    id: usize,
    panic: bool,
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Ordering::Relaxed);
        assert!(!self.panic, "intentional destructor panic");
    }
}
fn tracked(drops: &Arc<Vec<AtomicUsize>>, id: usize, panic: bool) -> Tracked {
    Tracked {
        drops: Arc::clone(drops),
        id,
        panic,
    }
}
fn counts(n: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect())
}

#[test]
fn producer_clear_panic_restores_block_and_drops_prefix_exactly_once() {
    for reclaim_first in [false, true] {
        let drops = counts(3);
        let (mut tx, mut rx) = channel::<Tracked>(1, 1, 2, 2);
        let mut w = tx[0].reserve().unwrap();
        assert!(w.push(tracked(&drops, 0, true)).is_ok());
        assert!(w.push(tracked(&drops, 1, false)).is_ok());
        w.commit().unwrap();
        drop(rx[0].recv().unwrap());
        assert_eq!(drops[0].load(Ordering::Relaxed), 0); // destructor deferred
        let result = catch_unwind(AssertUnwindSafe(|| {
            if reclaim_first {
                tx[0].reclaim();
            } else {
                drop(tx[0].reserve());
            }
        }));
        assert!(result.is_err());
        assert_eq!(drops[0].load(Ordering::Relaxed), 1);
        assert_eq!(drops[1].load(Ordering::Relaxed), 1);
        let mut w = tx[0].try_reserve().unwrap(); // pool survived unwind
        assert!(w.push(tracked(&drops, 2, false)).is_ok());
        w.commit().unwrap();
        drop(rx[0].recv().unwrap());
        assert!(tx[0].flush());
        drop(tx);
        drop(rx);
        assert_eq!(drops[2].load(Ordering::Relaxed), 1);
    }
}

#[test]
fn fill_and_consumer_unwind_return_blocks_without_cancelling_peers() {
    let drops = counts(3);
    let (mut tx, mut rx) = channel::<Tracked>(1, 1, 1, 1);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut w = tx[0].reserve().unwrap();
        assert!(w.push(tracked(&drops, 0, false)).is_ok());
        panic!("fill interrupted");
    }));
    assert!(result.is_err());
    let mut w = tx[0].reserve().unwrap();
    assert_eq!(drops[0].load(Ordering::Relaxed), 1);
    assert!(w.push(tracked(&drops, 1, false)).is_ok());
    w.commit().unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _r = rx[0].recv().unwrap();
        panic!("consumer interrupted");
    }));
    assert!(result.is_err());
    let mut w = tx[0].reserve().unwrap();
    assert_eq!(drops[1].load(Ordering::Relaxed), 1);
    assert!(w.push(tracked(&drops, 2, false)).is_ok());
    w.commit().unwrap();
    drop(tx); // return lane receiver gone while a published block remains
    let r = rx[0].recv().unwrap();
    assert_eq!(r.as_slice()[0].id, 2);
    drop(r);
    assert_eq!(drops[2].load(Ordering::Relaxed), 1);
}

#[test]
fn allocation_alignment_and_zero_sized_capacity() {
    #[repr(align(256))]
    struct Aligned(u64);
    let (mut tx, mut rx) = channel::<Aligned>(1, 1, 2, 2);
    let mut w = tx[0].reserve().unwrap();
    assert_eq!(w.spare_capacity_mut().as_ptr().addr() % 256, 0);
    assert!(w.push(Aligned(7)).is_ok());
    w.commit().unwrap();
    let r = rx[0].recv().unwrap();
    assert_eq!(r.as_slice().as_ptr().addr() % 256, 0);
    assert_eq!(r.as_slice()[0].0, 7);
    drop(r);
    assert!(tx[0].flush());

    static ZST_DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            ZST_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut tx, mut rx) = channel::<Zst>(1, 1, 2, 2);
    let mut w = tx[0].reserve().unwrap();
    assert_eq!(w.spare_capacity_mut().len(), 2);
    assert!(w.push(Zst).is_ok());
    assert!(w.push(Zst).is_ok());
    assert_eq!(w.capacity(), 2);
    drop(w.push(Zst).unwrap_err());
    w.commit().unwrap();
    drop(rx[0].recv().unwrap());
    assert!(tx[0].flush());
    assert_eq!(ZST_DROPS.load(Ordering::Relaxed), 3);
}

#[test]
fn contended_exact_once_partial_blocks_and_wrap() {
    let per = if cfg!(miri) { 97 } else { 2003 };
    for (p, c, cap, batch) in [(1, 1, 1, 1), (3, 3, 12, 3), (1, 4, 4, 2), (4, 1, 4, 1)] {
        let (tx, rx) = channel::<usize>(p, c, cap, batch);
        let result = std::thread::scope(|scope| {
            let senders = tx
                .into_iter()
                .enumerate()
                .map(|(producer, mut tx)| {
                    scope.spawn(move || {
                        let mut next = 0;
                        while next < per {
                            let mut w = tx.reserve().unwrap();
                            while next < per && w.len() < w.capacity() {
                                w.push(producer * per + next).unwrap();
                                next += 1;
                            }
                            w.commit().unwrap();
                        }
                        assert!(tx.flush());
                    })
                })
                .collect::<Vec<_>>();
            let receivers = rx
                .into_iter()
                .map(|mut rx| {
                    scope.spawn(move || {
                        let mut values = Vec::new();
                        while let Some(mut r) = rx.recv() {
                            let block = r.as_slice();
                            assert!(block.windows(2).all(|w| w[1] == w[0] + 1));
                            values.extend(r.drain());
                        }
                        values
                    })
                })
                .collect::<Vec<_>>();
            for tx in senders {
                tx.join().unwrap();
            }
            receivers
                .into_iter()
                .flat_map(|rx| rx.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut result = result;
        result.sort_unstable();
        assert_eq!(result, (0..p * per).collect::<Vec<_>>());
    }
}

#[test]
fn final_return_is_included_in_flush() {
    let (mut tx, mut rx) = channel::<u64>(1, 1, 1, 1);
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut w = tx[0].reserve().unwrap();
            w.push(42).unwrap();
            w.commit().unwrap();
            assert!(tx[0].flush());
            done_tx.send(()).unwrap();
        });
        scope.spawn(move || {
            let held = rx[0].recv().unwrap();
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            assert_eq!(held.as_slice(), &[42]);
            drop(held);
        });
        held_rx.recv().unwrap();
        assert!(done_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        done_rx.recv().unwrap();
    });
}
#[test]
fn drain_transfers_drop_responsibility_and_queued_teardown_drops_once() {
    let drops = counts(5);
    let (mut tx, mut rx) = channel::<Tracked>(1, 1, 6, 3);
    let mut w = tx[0].reserve().unwrap();
    for id in 0..3 {
        assert!(w.push(tracked(&drops, id, false)).is_ok());
    }
    w.commit().unwrap();
    let mut r = rx[0].recv().unwrap();
    let retained = r.drain().next().unwrap();
    drop(r);
    assert!(tx[0].flush());
    assert_eq!(drops[0].load(Ordering::Relaxed), 0);
    assert_eq!(drops[1].load(Ordering::Relaxed), 1);
    assert_eq!(drops[2].load(Ordering::Relaxed), 1);
    let mut w = tx[0].reserve().unwrap();
    for id in 3..5 {
        assert!(w.push(tracked(&drops, id, false)).is_ok());
    }
    w.commit().unwrap();
    drop(rx);
    drop(tx);
    drop(retained);
    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
}

#[test]
fn producer_teardown_racing_read_release_drops_exactly_once() {
    for _ in 0..if cfg!(miri) { 3 } else { 100 } {
        let drops = counts(2);
        let (mut tx, mut rx) = channel::<Tracked>(1, 1, 2, 2);
        let mut w = tx[0].reserve().unwrap();
        for id in 0..2 {
            assert!(w.push(tracked(&drops, id, false)).is_ok());
        }
        w.commit().unwrap();
        let r = rx[0].recv().unwrap();
        let barrier = &std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let producer = scope.spawn(move || {
                barrier.wait();
                drop(tx);
            });
            barrier.wait();
            drop(r);
            producer.join().unwrap();
        });
        drop(rx);
        assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn prepared_push_rejection_preserves_ownership_and_reuses_storage() {
    for batch in [1, 3, 64] {
        for commit in [false, true] {
            let drops = counts(batch + 1);
            let (mut tx, mut rx) = channel::<Tracked>(1, 1, batch, batch);
            let mut write = tx[0].reserve().unwrap();
            let allocation = write.spare_capacity_mut().as_ptr();
            for id in 0..batch {
                assert!(write.push(tracked(&drops, id, false)).is_ok());
            }
            let rejected = write.push(tracked(&drops, batch, false)).unwrap_err();
            assert_eq!(rejected.id, batch);
            assert_eq!(write.len(), batch);
            assert_eq!(write.spare_capacity_mut().len(), 0);
            assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 0));
            drop(rejected);
            assert_eq!(drops[batch].load(Ordering::Relaxed), 1);
            if commit {
                assert_eq!(write.commit().unwrap(), batch);
                let read = rx[0].recv().unwrap();
                for (id, value) in read.as_slice().iter().enumerate() {
                    assert_eq!(value.id, id);
                }
                drop(read);
            } else {
                drop(write);
                assert!(rx[0].try_recv().is_none());
            }
            let write = tx[0].reserve().unwrap();
            assert!(write.is_empty());
            drop(write);
            let mut write = tx[0].reserve().unwrap();
            assert_eq!(write.spare_capacity_mut().as_ptr(), allocation);
            drop(write);
            drop(tx);
            drop(rx);
            assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
        }
    }
}

#[test]
fn prepared_push_mixes_with_direct_prefix_slice_copy_and_recycling() {
    for batch in [1, 3, 64] {
        let (mut tx, mut rx) = channel::<[u8; 257]>(1, 1, batch, batch);
        for round in 0..3 {
            let mut write = tx[0].reserve().unwrap();
            let allocation = write.spare_capacity_mut().as_ptr().cast::<[u8; 257]>();
            write.spare_capacity_mut()[0].write([round; 257]);
            // SAFETY: the first spare element was initialized immediately above.
            unsafe { write.advance_initialized(1) };
            if batch > 1 {
                assert!(write.extend_from_slice(&[[round; 257]]));
            }
            while write.len() < batch {
                write.push([round; 257]).unwrap();
            }
            assert!(write.extend_from_slice(&[]));
            assert!(!write.extend_from_slice(&[[99; 257]]));
            assert_eq!(write.push([99; 257]), Err([99; 257]));
            assert_eq!(write.commit().unwrap(), batch);
            let mut read = rx[0].recv().unwrap();
            assert_eq!(read.as_slice().as_ptr(), allocation);
            assert!(read.as_slice().iter().all(|value| *value == [round; 257]));
            drop(read.drain());
            drop(read);
            assert!(tx[0].flush());
        }
    }
}

#[test]
fn returned_pool_flush_destroys_values_after_early_reuse() {
    for blocks in [1, 3, 16, 17, 33] {
        let drops = counts(blocks * 3);
        let (mut tx, mut rx) = channel::<Tracked>(1, 1, blocks * 3, 3);
        for block in 0..blocks {
            let mut write = tx[0].try_reserve().unwrap();
            for offset in 0..3 {
                assert!(
                    write
                        .push(tracked(&drops, block * 3 + offset, false))
                        .is_ok()
                );
            }
            write.commit().unwrap();
        }
        assert!(tx[0].try_reserve().is_none());
        for _ in 0..blocks {
            drop(rx[0].recv().unwrap());
        }
        // Reuse can reclaim more than the requested block. Flush must also
        // account for values whose storage was collected into the free pool.
        drop(tx[0].try_reserve().unwrap());
        assert!(tx[0].flush());
        assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
        drop(tx);
        drop(rx);
        assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn later_return_destructor_panic_conserves_the_entire_pool() {
    const BLOCKS: usize = 33;
    const BATCH: usize = 3;
    for panic_id in [1, 8 * BATCH + 1, 17 * BATCH + 1, 32 * BATCH + 1] {
        let drops = counts(BLOCKS * BATCH);
        let next_drops = counts(BLOCKS);
        let recycled_drops = counts(BLOCKS);
        let mut published = 0;
        let (mut tx, mut rx) = channel::<Tracked>(1, 1, BLOCKS * BATCH, BATCH);
        for block in 0..BLOCKS {
            let mut write = tx[0].reserve().unwrap();
            for offset in 0..BATCH {
                let id = block * BATCH + offset;
                assert!(write.push(tracked(&drops, id, id == panic_id)).is_ok());
            }
            write.commit().unwrap();
        }
        for _ in 0..BLOCKS {
            drop(rx[0].recv().unwrap());
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            // Republishing blocks consumes locally free capacity even
            // when the implementation collects multiple returns per reserve.
            // The returned prefix eventually reaches the panicking destructor.
            for id in 0..BLOCKS {
                let mut write = tx[0].reserve().unwrap();
                assert!(write.push(tracked(&next_drops, id, false)).is_ok());
                write.commit().unwrap();
                published += 1;
            }
        }));
        assert!(result.is_err());
        // Drain only the successfully republished prefix; no blocking receive.
        while let Some(read) = rx[0].try_recv() {
            drop(read);
        }
        assert!(tx[0].flush());
        assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));

        for (id, count) in next_drops.iter().enumerate() {
            assert_eq!(count.load(Ordering::Relaxed), usize::from(id < published));
        }

        // Recover every block, not merely the one involved in the panic.
        for id in 0..BLOCKS {
            let mut write = tx[0].try_reserve().unwrap();
            assert!(write.push(tracked(&recycled_drops, id, false)).is_ok());
            write.commit().unwrap();
        }
        assert!(tx[0].try_reserve().is_none());
        for _ in 0..BLOCKS {
            drop(rx[0].recv().unwrap());
        }
        assert!(tx[0].flush());
        assert!(
            recycled_drops
                .iter()
                .all(|n| n.load(Ordering::Relaxed) == 1)
        );
    }
}

#[test]
fn ready_returns_reuse_hot_storage_before_cold_pool_growth() {
    for cap in [3, 17, 65] {
        let (mut tx, mut rx) = channel::<u64>(1, 1, cap, 1);
        let mut first = tx[0].reserve().unwrap();
        let pointer = first.spare_capacity_mut().as_ptr();
        first.push(7).unwrap();
        first.commit().unwrap();
        assert_eq!(rx[0].recv().unwrap().as_slice(), &[7]);

        for blocking in [false, true, false, true] {
            let mut write = if blocking {
                tx[0].reserve()
            } else {
                tx[0].try_reserve()
            }
            .unwrap();
            assert_eq!(write.spare_capacity_mut().as_ptr(), pointer);
            assert!(write.is_empty());
            write.push(11).unwrap();
            write.commit().unwrap();
            assert_eq!(rx[0].recv().unwrap().as_slice(), &[11]);
        }

        // Rollback remains locally reusable, and all cold capacity remains
        // available when a returned block is not ready.
        drop(tx[0].reserve().unwrap());
        for id in 0..cap {
            let mut write = tx[0].try_reserve().unwrap();
            write.push(id as u64).unwrap();
            write.commit().unwrap();
        }
        assert!(tx[0].try_reserve().is_none());
        for id in 0..cap {
            assert_eq!(rx[0].recv().unwrap().as_slice(), &[id as u64]);
        }
        assert!(tx[0].flush());
    }
}
