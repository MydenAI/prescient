//! Raw std mutex throughput: one shared u64, one lock/increment/unlock per operation.
//! This is not message transfer. Each process runs a full warmup before timing samples.
#[path = "support/affinity.rs"]
mod affinity;
#[path = "support/start_gate.rs"]
mod start_gate;

use std::sync::Mutex;
use std::time::Instant;

const USAGE: &str = "usage: mutex_bench THREADS PER_THREAD SAMPLES [--cpus CPU0,CPU1,...]";

struct Config {
    threads: usize,
    per_thread: usize,
    samples: usize,
    placement: affinity::Placement,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        if args.len() != 3 && args.len() != 5 {
            return Err(USAGE.into());
        }
        let positive = |text: &str| {
            text.parse::<usize>()
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| format!("expected positive integer, got {text:?}"))
        };
        let threads = positive(&args[0])?;
        let per_thread = positive(&args[1])?;
        let samples = positive(&args[2])?;
        let cpu_map = if args.len() == 5 {
            if args[3] != "--cpus" {
                return Err(USAGE.into());
            }
            Some(args[4].as_str())
        } else {
            None
        };
        let operations = threads
            .checked_mul(per_thread)
            .ok_or("operations overflow")?;
        u64::try_from(operations).map_err(|_| "counter overflow")?;
        samples
            .checked_add(1)
            .and_then(|runs| runs.checked_mul(operations))
            .ok_or("total operations overflow")?;
        Ok(Self {
            threads,
            per_thread,
            samples,
            placement: affinity::Placement::new(cpu_map, threads)?,
        })
    }

    fn operations(&self) -> usize {
        self.threads * self.per_thread
    }

    fn completed(&self) -> usize {
        self.operations() * (self.samples + 1)
    }
}

fn sample(config: &Config) -> Result<f64, String> {
    let counter = Mutex::new(0u64);
    let gate = start_gate::StartGate::default();
    let elapsed = std::thread::scope(|scope| -> Result<_, String> {
        let _abort = gate.abort_on_drop();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut workers = Vec::with_capacity(config.threads);
        // Even the uncontended case uses a worker thread, not a single-threaded
        // process shortcut. Thread creation and affinity precede the timer.
        for index in 0..config.threads {
            let counter = &counter;
            let gate = &gate;
            let ready = ready_tx.clone();
            workers.push(
                std::thread::Builder::new()
                    .spawn_scoped(scope, move || {
                        if !gate.enter(ready, config.placement.pin(index)) {
                            return;
                        }
                        for _ in 0..config.per_thread {
                            *counter.lock().unwrap() += 1;
                        }
                    })
                    .map_err(|e| format!("cannot spawn worker {index}: {e}"))?,
            );
        }
        drop(ready_tx);
        for _ in 0..config.threads {
            ready_rx
                .recv()
                .map_err(|_| "worker exited during setup")??;
        }
        let start = Instant::now();
        gate.open();
        for worker in workers {
            worker.join().map_err(|_| "worker panicked")?;
        }
        Ok(start.elapsed())
    })?;
    assert_eq!(counter.into_inner().unwrap(), config.operations() as u64);
    Ok(config.operations() as f64 / elapsed.as_secs_f64() / 1e6)
}

fn run(config: &Config) -> Result<(), String> {
    sample(config)?; // One full, verified warmup.
    let mut rates = (0..config.samples)
        .map(|_| sample(config))
        .collect::<Result<Vec<_>, _>>()?;
    let ordered = rates
        .iter()
        .map(|r| format!("{r:.6}"))
        .collect::<Vec<_>>()
        .join(",");
    rates.sort_by(f64::total_cmp);
    let median = (rates[(rates.len() - 1) / 2] + rates[rates.len() / 2]) / 2.0;
    println!(
        "impl=std-mutex-increment;threads={};operations={};samples={};warmups=1;completed={};worker_cpus={};median_mops={median:.6};min_mops={:.6};max_mops={:.6};sample_mops={ordered}",
        config.threads,
        config.operations(),
        config.samples,
        config.completed(),
        config.placement.label(),
        rates[0],
        rates[rates.len() - 1],
    );
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("{USAGE}");
        return;
    }
    if let Err(error) = Config::parse(&args).and_then(|config| run(&config)) {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(text: &str) -> Result<Config, String> {
        Config::parse(
            &text
                .split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn workload_includes_full_warmup() {
        let config = parse("6 1000 5").unwrap();
        assert_eq!(config.operations(), 6000);
        assert_eq!(config.completed(), 36000);
    }

    #[test]
    fn invalid_work_and_options_fail_before_spawning() {
        for text in [
            "",
            "1 1",
            "0 1 1",
            "1 0 1",
            "1 1 0",
            "1 1 1 --cpus",
            "1 1 1 --unknown 0",
            "2 1 1 --cpus 0",
            "1 1 1 --cpus 0 extra",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
        for text in [
            format!("2 {} 1", usize::MAX),
            format!("1 1 {}", usize::MAX),
            format!("1 {} 1", usize::MAX),
        ] {
            assert!(parse(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn verified_increment_for_uncontended_and_contended_mutex() {
        for threads in [1, 2, 6] {
            let config = parse(&format!("{threads} 1000 1")).unwrap();
            assert!(sample(&config).unwrap() > 0.0);
        }
    }
}
