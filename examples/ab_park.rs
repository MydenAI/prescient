//! A/B: idle wake latency of the four concrete MPSC wait policies.
//!
//! A ping-pong bounces a token over two capacity-one channels. Policy selection
//! happens before the measured loop and each opened endpoint is monomorphic.
//!
//!   cargo run --release --example ab_park -- [rounds] [segments]

use std::time::Instant;

use prescient::mpsc::fixed;
use prescient::wait::{Hybrid, Park, Spin, StdThread};

// One round-trip ping-pong bench for a concrete wait marker.
macro_rules! pingpong {
    ($wait:ty, $rounds:expr) => {{
        let (mut to_worker_tx, mut to_worker_rx) = fixed::channel::<u64>()
            .capacity(1)
            .wait::<$wait>()
            .open()
            .unwrap();
        let (mut to_main_tx, mut to_main_rx) = fixed::channel::<u64>()
            .capacity(1)
            .wait::<$wait>()
            .open()
            .unwrap();
        let mut to_worker_tx = to_worker_tx.pop().unwrap();
        let mut to_main_tx = to_main_tx.pop().unwrap();
        let rounds: u64 = $rounds;

        let ns = std::thread::scope(|s| {
            s.spawn(move || {
                for _ in 0..rounds {
                    let token = to_worker_rx.recv().unwrap();
                    to_main_tx.send(token).unwrap();
                }
            });

            let start = Instant::now();
            for token in 0..rounds {
                to_worker_tx.send(token).unwrap();
                assert_eq!(to_main_rx.recv(), Some(token));
            }
            start.elapsed().as_nanos() as f64 / rounds as f64
        });
        ns
    }};
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let rounds: u64 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let segments: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(7);
    println!("A/B eventcount wake latency: {rounds} round-trips/segment, {segments} segments\n");

    // warm all four
    let _ = pingpong!(Park, rounds / 4);
    let _ = pingpong!(StdThread, rounds / 4);
    let _ = pingpong!(Spin, rounds / 4);
    let _ = pingpong!(Hybrid, rounds / 4);

    let mut cond = Vec::new();
    let mut stdp = Vec::new();
    let mut spin = Vec::new();
    let mut hybrid = Vec::new();
    let mut r_std = Vec::new();
    let mut r_spin = Vec::new();
    let mut r_hyb = Vec::new();
    for _ in 0..segments {
        let c = pingpong!(Park, rounds);
        let s = pingpong!(StdThread, rounds);
        let p = pingpong!(Spin, rounds);
        let h = pingpong!(Hybrid, rounds);
        cond.push(c);
        stdp.push(s);
        spin.push(p);
        hybrid.push(h);
        r_std.push(s / c); // paired vs condvar baseline
        r_spin.push(p / c);
        r_hyb.push(h / c);
    }

    println!(
        "park        (parking_lot Condvar) {:>8.0} ns/round-trip",
        median(cond)
    );
    println!(
        "park_std    (std park/unpark)     {:>8.0} ns/round-trip",
        median(stdp)
    );
    println!(
        "park_spin   (no-std spin)         {:>8.0} ns/round-trip",
        median(spin)
    );
    println!(
        "park_hybrid (spin-then-park)      {:>8.0} ns/round-trip",
        median(hybrid)
    );
    println!("\nmedian paired vs condvar baseline (>1.0 = SLOWER, <1.0 = faster):");
    println!(
        "  std_thread   {:.3}x  ({:+.1}%)",
        median(r_std.clone()),
        (median(r_std) - 1.0) * 100.0
    );
    println!(
        "  spin         {:.3}x  ({:+.1}%)",
        median(r_spin.clone()),
        (median(r_spin) - 1.0) * 100.0
    );
    println!(
        "  hybrid       {:.3}x  ({:+.1}%)",
        median(r_hyb.clone()),
        (median(r_hyb) - 1.0) * 100.0
    );
}
