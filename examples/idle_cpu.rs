//! Idle-CPU harness: isolates the consumer wait strategy's cost when the channel
//! is *under-utilized*. One producer sends `n` messages, REAL-sleeping `gap_us`
//! between each (so the producer core is genuinely idle in the gap, unlike the
//! busy-wait pacing in `bench`). The consumer `recv()`s all of them. Total process
//! user+sys CPU time then reflects almost entirely how the idle consumer waits.
//!
//! The wait strategy is a compile-time choice selected by the final argument:
//!   cargo run --release --example idle_cpu -- 4000 500 park
//!   cargo run --release --example idle_cpu -- 4000 500 spin
//!
//! Expect: spin pegs ~1 core for the whole run; hybrid and condvar stay near idle.

use std::time::{Duration, Instant};

use prescient::mpsc::fixed;
use prescient::wait::{Hybrid, Park, Spin, StdThread};

macro_rules! run {
    ($wait:ty, $label:expr, $n:expr, $gap_us:expr) => {{
        let (mut prods, mut rx) = fixed::channel::<u64>()
            .producers(1)
            .capacity(64)
            .wait::<$wait>()
            .open()
            .unwrap();
        let mut p = prods.pop().unwrap();

        let start = Instant::now();
        let h = std::thread::spawn(move || {
            for i in 0..$n as u64 {
                p.send(i).unwrap();
                std::thread::sleep(Duration::from_micros($gap_us));
            }
        });
        let mut got = 0usize;
        while got < $n {
            if rx.recv().is_some() {
                got += 1;
            }
        }
        h.join().unwrap();
        let wall = start.elapsed().as_secs_f64();
        println!(
            "waiter={} received={got} gap={}us wall={wall:.3}s",
            $label, $gap_us
        );
    }};
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(4000);
    let gap_us: u64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(500);
    match a.get(3).map(String::as_str).unwrap_or("park") {
        "park" => run!(Park, "park", n, gap_us),
        "std" => run!(StdThread, "std", n, gap_us),
        "hybrid" => run!(Hybrid, "hybrid", n, gap_us),
        "spin" => run!(Spin, "spin", n, gap_us),
        other => panic!("unknown waiter {other:?}; use park, std, hybrid, or spin"),
    }
}
