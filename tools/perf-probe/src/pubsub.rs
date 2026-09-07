//! Isolated broker routing measurements using real Ring transports.
use std::hint::black_box;
use std::time::Instant;

use prescient::mpmc::brokered::{self, Targets};

pub fn sparse(messages: usize, samples: usize, consumers: usize) {
    assert!((1..=64).contains(&consumers));
    run(
        "pubsub_sparse",
        messages,
        samples,
        consumers,
        vec![consumers - 1],
        |_, n| Targets::one(n - 1),
    );
}

pub fn dense(messages: usize, samples: usize, consumers: usize) {
    assert!((1..=64).contains(&consumers));
    run(
        "pubsub_dense",
        messages,
        samples,
        consumers,
        (0..consumers).collect(),
        |_, n| Targets::all(n),
    );
}

fn run<F>(
    name: &str,
    messages: usize,
    samples: usize,
    consumers: usize,
    active: Vec<usize>,
    subscribe: F,
) where
    F: FnMut(&u64, usize) -> Targets + Clone + Send + 'static,
{
    assert!(messages > 0 && samples > 0);
    let expected = ((messages as u128 * (messages as u128 - 1)) / 2) as u64;
    let mut rates = Vec::with_capacity(samples);
    for sample in 0..=samples {
        let (mut producers, receivers, mut brokers) = brokered::channel::<u64>()
            .producers(1)
            .consumers(black_box(consumers))
            .capacity(1024)
            .pubsub(subscribe.clone())
            .manual()
            .open()
            .unwrap();
        let mut producer = producers.pop().unwrap();
        let broker = brokers.pop().unwrap();
        let (selected, dormant): (Vec<_>, Vec<_>) = receivers
            .into_iter()
            .enumerate()
            .partition(|(index, _)| active.contains(index));

        // Open/allocation is excluded. Dormant endpoints stay alive: sparse
        // routing must skip them, not discover that they have disconnected.
        let start = Instant::now();
        let results = std::thread::scope(|scope| {
            let router = scope.spawn(move || broker.run());
            let sender = scope.spawn(move || {
                for value in 0..messages as u64 {
                    assert!(producer.send(value));
                }
            });
            let readers: Vec<_> = selected
                .into_iter()
                .map(|(_, mut receiver)| {
                    scope.spawn(move || {
                        let mut count = 0usize;
                        let mut sum = 0u64;
                        while let Some(value) = receiver.recv() {
                            count += 1;
                            sum = sum.wrapping_add(value);
                        }
                        (count, sum)
                    })
                })
                .collect();
            sender.join().unwrap();
            router.join().unwrap();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        let elapsed = start.elapsed();
        for (count, sum) in results {
            assert_eq!(count, messages);
            assert_eq!(sum, expected);
        }
        // No message may have been routed to a non-subscriber.
        for (_, mut receiver) in dormant {
            assert_eq!(receiver.recv(), None);
        }
        if sample > 0 {
            rates.push(messages as f64 / elapsed.as_secs_f64() / 1e6);
        }
    }
    rates.sort_by(f64::total_cmp);
    println!(
        "{name};messages={messages};samples={samples};consumers={consumers};subscribers={};median_mps={:.6};min_mps={:.6};max_mps={:.6}",
        active.len(),
        rates[samples / 2],
        rates[0],
        rates[samples - 1],
    );
}
