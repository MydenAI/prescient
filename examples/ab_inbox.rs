//! A/B the two explicitly selected dynamic-MPSC membership policies in one
//! process: `Locked` (`Mutex<Vec>`) and `LockFree` (intrusive Treiber stack).
//! Both are always available and selected by declaration type; `Locked` remains
//! the default.
//!
//!   cargo run --release --example ab_inbox -- [P] [rounds] [per] [locked|lockfree|compare]

use std::time::Instant;

use prescient::membership::{LockFree, Locked};
use prescient::mpsc::dynamic;

const REPS: usize = 5;

// P producers, each registering `rounds` times and sending `per` messages per
// registration. Returns messages/sec.
macro_rules! run {
    ($membership:ty, $p:expr, $rounds:expr, $per:expr) => {{
        let (reg, mut rx) = dynamic::channel::<u64>()
            .wait::<prescient::wait::Spin>()
            .membership::<$membership>()
            .capacity(256)
            .open()
            .unwrap();
        let total = ($p * $rounds) as u64 * $per;
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..$p {
                let reg = reg.clone();
                s.spawn(move || {
                    for _ in 0..$rounds {
                        let mut prod = reg.register();
                        for i in 0..$per {
                            prod.send(i).unwrap();
                        }
                    }
                });
            }
            drop(reg);
            let mut n = 0u64;
            while rx.recv().is_some() {
                n += 1;
            }
            assert_eq!(n, total, "lost or duplicated messages");
        });
        total as f64 / start.elapsed().as_secs_f64()
    }};
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let p: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let rounds: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(4000);
    let per: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);
    let mode = a.get(4).map(String::as_str).unwrap_or("compare");
    println!("dynamic register-churn: P={p} rounds={rounds} per={per} mode={mode}\n");

    match mode {
        "locked" => {
            let _ = run!(Locked, p, rounds / 10, per);
            let rate = median((0..REPS).map(|_| run!(Locked, p, rounds, per)).collect());
            println!("{} registrations total", p * rounds);
            println!("locked    {:>7.2} M msg/s", rate / 1e6);
        }
        "lockfree" => {
            let _ = run!(LockFree, p, rounds / 10, per);
            let rate = median((0..REPS).map(|_| run!(LockFree, p, rounds, per)).collect());
            println!("{} registrations total", p * rounds);
            println!("lock-free {:>7.2} M msg/s", rate / 1e6);
        }
        "compare" => {
            let _ = run!(Locked, p, rounds / 10, per);
            let _ = run!(LockFree, p, rounds / 10, per);
            let locked = median((0..REPS).map(|_| run!(Locked, p, rounds, per)).collect());
            let lock_free = median((0..REPS).map(|_| run!(LockFree, p, rounds, per)).collect());
            println!("{} registrations total", p * rounds);
            println!("locked    {:>7.2} M msg/s", locked / 1e6);
            println!("lock-free {:>7.2} M msg/s", lock_free / 1e6);
            println!("lock-free / locked = {:.3}x", lock_free / locked);
        }
        other => panic!("unknown mode {other:?}; use locked, lockfree, or compare"),
    }
}
