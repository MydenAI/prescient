//! Correctness smoke tests (not throughput). Each tier must deliver every
//! message exactly once and then signal clean disconnect.

use prescient::mpsc::{dynamic, fixed, pool};

const P: usize = 4;
const M: u64 = 100_000;

fn expected_sum() -> u128 {
    // each producer sends 0..M
    (P as u128) * ((M as u128 - 1) * (M as u128) / 2)
}

#[test]
fn fixed_delivers_all_and_disconnects() {
    let (producers, mut rx) = fixed::channel::<u64>()
        .producers(P)
        .capacity(1024)
        .open()
        .unwrap();
    std::thread::scope(|s| {
        for mut p in producers {
            s.spawn(move || {
                p.send_batch(0..M).unwrap();
            });
        }
        let mut count = 0u64;
        let mut sum = 0u128;
        while let Some(v) = rx.recv() {
            sum += v as u128;
            count += 1;
        }
        assert_eq!(count, P as u64 * M, "fixed: message count");
        assert_eq!(sum, expected_sum(), "fixed: checksum");
    });
}

#[test]
fn pool_delivers_all_and_disconnects() {
    let (handle, mut rx) = pool::channel::<u64>()
        .max_producers(8)
        .capacity(1024)
        .open()
        .unwrap();
    let claimed: Vec<_> = (0..P)
        .map(|_| handle.claim().expect("slot available"))
        .collect();
    drop(handle); // done claiming -> lets the receiver terminate when producers finish
    std::thread::scope(|s| {
        for mut p in claimed {
            s.spawn(move || {
                p.send_batch(0..M).unwrap();
            });
        }
        let mut count = 0u64;
        let mut sum = 0u128;
        while let Some(v) = rx.recv() {
            sum += v as u128;
            count += 1;
        }
        assert_eq!(count, P as u64 * M, "pool: message count");
        assert_eq!(sum, expected_sum(), "pool: checksum");
    });
}

#[test]
fn dynamic_delivers_all_including_late_join() {
    let (reg, mut rx) = dynamic::channel::<u64>().capacity(1024).open().unwrap();
    std::thread::scope(|s| {
        // some producers registered up front, some mid-flight
        for k in 0..P {
            let reg = reg.clone();
            s.spawn(move || {
                if k % 2 == 1 {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                let mut p = reg.register();
                p.send_batch(0..M).unwrap();
            });
        }
        drop(reg); // drop the receiver's registrar clone so only producers hold refs
        let mut count = 0u64;
        let mut sum = 0u128;
        while let Some(v) = rx.recv() {
            sum += v as u128;
            count += 1;
        }
        assert_eq!(count, P as u64 * M, "dynamic: message count");
        assert_eq!(sum, expected_sum(), "dynamic: checksum");
    });
}
