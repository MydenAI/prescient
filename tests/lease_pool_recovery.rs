//! Allocation-identity diagnostics use public leases, never production counters.
//! These are controlled recovery schedules, not throughput benchmarks.
//! Run the ignored trace with:
//! cargo test --release --test lease_pool_recovery concurrent_pool_identity_trace -- --ignored --nocapture --test-threads=1
//! PRESCIENT_POOL_CPUS optionally pins six workers (for example 0,1,2,3,4,5).
//! Presence of PRESCIENT_POOL_DRAINED_START drains expanded setup before restart,
//! without producer reclamation. Acknowledgements lag release by up to one block
//! per consumer; unacknowledged work is not an exact instantaneous queue length.
//! Pointer tracking and acknowledgements perturb execution; time payload_bench
//! separately, never use these traces as throughput measurements.
//!
//! The separate ignored bounded_recovery_opportunity_trace injects one finite
//! recovery interval. Set PRESCIENT_RECOVERY_SWEEPS (0 = control; 1 = no yield)
//! and PRESCIENT_RECOVERY_WINDOW (0 = start; 1 = after one steady window).
//! It reports actual reclaim calls/yields, recovery time and window time.
//! Window time excludes the recovery interval; neither is uninstrumented library
//! throughput. A finite call budget is not a wall-clock latency guarantee.
use prescient::mpmc::brokerless::leased;
use std::collections::BTreeSet;

fn recovery_trace(consumers: usize, blocks: usize, reclaim: bool) -> Vec<usize> {
    let (mut tx, mut rx) = leased::channel::<u64>()
        .producers(1)
        .consumers(consumers)
        .capacity(blocks)
        .batch(1)
        .open()
        .unwrap();
    let mut allocated = BTreeSet::new();
    for id in 0..blocks {
        let mut write = tx[0].try_reserve().unwrap();
        assert!(allocated.insert(write.spare_capacity_mut().as_ptr() as usize));
        write.push(id as u64).unwrap();
        write.commit().unwrap();
    }
    assert!(tx[0].try_reserve().is_none());
    for id in 0..blocks {
        let read = rx[id % consumers].recv().unwrap();
        assert_eq!(read.as_slice(), &[id as u64]);
    }
    if reclaim {
        assert!(tx[0].flush());
    }
    let rounds = blocks * 8;
    let mut trace = Vec::with_capacity(rounds);
    for id in 0..rounds {
        let mut write = tx[0].reserve().unwrap();
        let address = write.spare_capacity_mut().as_ptr() as usize;
        assert!(allocated.contains(&address));
        trace.push(address);
        write.push(id as u64).unwrap();
        write.commit().unwrap();
        let read = rx[id % consumers].recv().unwrap();
        assert_eq!(read.as_slice(), &[id as u64]);
    }
    assert!(tx[0].flush());
    // Recovery must not lower configured capacity or lose a parked block.
    let mut recovered = BTreeSet::new();
    for id in 0..blocks {
        let mut write = tx[0].try_reserve().unwrap();
        assert!(recovered.insert(write.spare_capacity_mut().as_ptr() as usize));
        write.push(id as u64).unwrap();
        write.commit().unwrap();
    }
    assert_eq!(recovered, allocated);
    assert!(tx[0].try_reserve().is_none());
    for id in 0..blocks {
        assert_eq!(rx[id % consumers].recv().unwrap().as_slice(), &[id as u64]);
    }
    assert!(tx[0].flush());
    trace
}

#[test]
fn expanded_pool_preserves_every_allocation_during_recovery() {
    for consumers in [1, 3] {
        for blocks in [3, 17, 64] {
            for reclaim in [false, true] {
                let trace = recovery_trace(consumers, blocks, reclaim);
                let tail = &trace[trace.len() / 2..];
                let distinct = tail.iter().copied().collect::<BTreeSet<_>>().len();
                eprintln!(
                    "pool_recovery;consumers={consumers};blocks={blocks};explicit_reclaim={reclaim};tail_distinct={distinct}"
                );
                assert!(distinct <= blocks);
                if reclaim {
                    assert_eq!(distinct, 1);
                }
            }
        }
    }
}

#[path = "../examples/support/affinity.rs"]
mod affinity;
#[path = "../examples/support/payload.rs"]
mod payload;
#[path = "../examples/support/start_gate.rs"]
mod start_gate;

struct ReuseTrace {
    indices: std::collections::HashMap<usize, usize>,
    last: Vec<Option<usize>>,
    touched: Vec<bool>,
    histogram: Vec<usize>,
    sequence: usize,
    max_gap: usize,
}
impl ReuseTrace {
    fn new(blocks: usize) -> Self {
        Self {
            indices: std::collections::HashMap::with_capacity(blocks),
            last: vec![None; blocks],
            touched: vec![false; blocks],
            histogram: vec![0; blocks],
            sequence: 0,
            max_gap: 0,
        }
    }
    fn observe(&mut self, address: usize) {
        let next = self.indices.len();
        let index = *self.indices.entry(address).or_insert(next);
        assert!(index < self.last.len(), "payload pool grew");
        if let Some(previous) = self.last[index] {
            // Exact distinct-allocation reuse distance, not merely elapsed time.
            let distance = self
                .last
                .iter()
                .filter(|v| v.is_some_and(|n| n > previous))
                .count();
            self.histogram[distance] += 1;
            self.max_gap = self.max_gap.max(self.sequence - previous);
        }
        self.last[index] = Some(self.sequence);
        self.sequence += 1;
        self.touched[index] = true;
    }
    fn window(&mut self) -> (usize, usize, usize, usize, usize) {
        let quantile = |percent: usize| {
            let count: usize = self.histogram.iter().sum();
            if count == 0 {
                return 0;
            }
            let target = (count * percent).div_ceil(100);
            let mut cumulative = 0;
            self.histogram
                .iter()
                .position(|n| {
                    cumulative += n;
                    cumulative >= target
                })
                .unwrap()
        };
        let result = (
            self.indices.len(),
            self.touched.iter().filter(|v| **v).count(),
            quantile(50),
            quantile(95),
            self.max_gap,
        );
        self.touched.fill(false);
        self.histogram.fill(0);
        self.max_gap = 0;
        result
    }
}

fn publish<const N: usize, const DIRECT: bool>(
    producer: &mut leased::Producer<[u8; N]>,
    start: usize,
    batch: usize,
    fixtures: &[[u8; N]; 16],
    trace: &mut ReuseTrace,
) {
    let mut write = producer.reserve().unwrap();
    trace.observe(write.spare_capacity_mut().as_ptr() as usize);
    if DIRECT {
        for (offset, slot) in write.spare_capacity_mut()[..batch].iter_mut().enumerate() {
            let id = start + offset;
            let value = slot.write(fixtures[id & 15]);
            value[..8].copy_from_slice(&(id as u64).to_le_bytes());
        }
        // SAFETY: exactly batch spare values were fully initialized above.
        unsafe { write.advance_initialized(batch) };
    } else {
        for id in start..start + batch {
            write.push(payload::packet(id as u64, fixtures)).unwrap();
        }
    }
    assert_eq!(write.commit().unwrap(), batch);
}

fn concurrent_trace<const DIRECT: bool>(
    expanded: bool,
    recovery: Option<RecoverySchedule>,
) -> Result<(), String> {
    const P: usize = 3;
    const C: usize = 3;
    const CAPACITY: usize = 4096;
    const BATCH: usize = 64;
    const BLOCKS: usize = CAPACITY / BATCH;
    const WINDOW: usize = 512;
    const WINDOWS: usize = 8;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[repr(align(64))]
    struct Completed(AtomicUsize);
    let completed: [Completed; P] = std::array::from_fn(|_| Completed(AtomicUsize::new(0)));
    let drained_start = expanded && std::env::var_os("PRESCIENT_POOL_DRAINED_START").is_some();
    let prefill = if expanded { BLOCKS } else { 0 };
    let per = (prefill + WINDOW * WINDOWS) * BATCH;
    let total = P * per;
    // Explicit placement is optional for portable test builds.
    let cpu_map = std::env::var("PRESCIENT_POOL_CPUS").ok();
    let placement = affinity::Placement::new(cpu_map.as_deref(), P + C)?;
    let fixtures = &payload::fixtures::<4096>();
    let (mut tx, mut rx) = leased::channel::<[u8; 4096]>()
        .producers(P)
        .consumers(C)
        .capacity(CAPACITY)
        .batch(BATCH)
        .open()
        .unwrap();
    let mut traces: Vec<_> = (0..P).map(|_| ReuseTrace::new(BLOCKS)).collect();
    let mut setup_result = (0usize, 0u64, 0u64);
    if drained_start {
        // Artificial quiescence control: initialize and consume the whole pool
        // before worker start, but DO NOT reclaim it on the producer.
        for (index, producer) in tx.iter_mut().enumerate() {
            for block in 0..BLOCKS {
                publish::<4096, DIRECT>(
                    producer,
                    index * per + block * BATCH,
                    BATCH,
                    fixtures,
                    &mut traces[index],
                );
            }
            assert!(producer.try_reserve().is_none());
        }
        for block in 0..P * BLOCKS {
            let read = rx[block % C].recv().unwrap();
            let origin =
                u64::from_le_bytes(read.as_slice()[0][..8].try_into().unwrap()) as usize / per;
            for value in read.as_slice() {
                let (id, bad) = payload::inspect(value, fixtures, total);
                assert_eq!(bad, 0);
                setup_result.0 += 1;
                setup_result.1 = setup_result.1.wrapping_add(id);
                setup_result.2 ^= id;
            }
            drop(read);
            completed[origin].0.fetch_add(1, Ordering::Release);
        }
    }
    let gate = start_gate::StartGate::default();
    let (reports, results) = std::thread::scope(|scope| -> Result<_, String> {
        let _abort = gate.abort_on_drop();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut senders = Vec::new();
        for (index, (mut producer, mut trace)) in tx.into_iter().zip(traces).enumerate() {
            let ready = ready_tx.clone();
            let (gate, placement, completed) = (&gate, &placement, &completed);
            senders.push(
                std::thread::Builder::new()
                    .spawn_scoped(scope, move || {
                        let prepared = placement.pin(index);
                        if prepared.is_ok() && !drained_start {
                            for block in 0..prefill {
                                publish::<4096, DIRECT>(
                                    &mut producer,
                                    index * per + block * BATCH,
                                    BATCH,
                                    fixtures,
                                    &mut trace,
                                );
                            }
                            if expanded {
                                assert!(producer.try_reserve().is_none());
                            }
                        }
                        if !gate.enter(ready, prepared) {
                            return Vec::new();
                        }
                        trace.window(); // Prefill belongs to setup, not the first recovery window.
                        let mut windows = Vec::with_capacity(WINDOWS);
                        let mut recovery_event = (0, 0, 0, 0u64);
                        for window in 0..WINDOWS {
                            if let Some(policy) = recovery.filter(|p| p.window == window) {
                                let started = std::time::Instant::now();
                                let (reclaimed, calls, yields) =
                                    bounded_reclaim(&mut producer, policy.sweeps, BLOCKS);
                                recovery_event = (
                                    reclaimed,
                                    calls,
                                    yields,
                                    started.elapsed().as_nanos() as u64,
                                );
                            }
                            let started = recovery.map(|_| std::time::Instant::now());
                            for offset in 0..WINDOW {
                                let block = prefill + window * WINDOW + offset;
                                publish::<4096, DIRECT>(
                                    &mut producer,
                                    index * per + block * BATCH,
                                    BATCH,
                                    fixtures,
                                    &mut trace,
                                );
                            }
                            let issued = prefill + (window + 1) * WINDOW;
                            let acknowledged = completed[index].0.load(Ordering::Acquire);
                            assert!(acknowledged <= issued);
                            // Includes up to C blocks released immediately before
                            // their diagnostic acknowledgement, not exact queue length.
                            windows.push((
                                trace.window(),
                                issued - acknowledged,
                                started.map_or(0, |t| t.elapsed().as_nanos() as u64),
                                recovery_event,
                            ));
                        }
                        assert!(producer.flush());
                        windows
                    })
                    .map_err(|e| e.to_string())?,
            );
        }
        let mut receivers = Vec::new();
        for (index, mut consumer) in rx.into_iter().enumerate() {
            let ready = ready_tx.clone();
            let (gate, placement, completed) = (&gate, &placement, &completed);
            receivers.push(
                std::thread::Builder::new()
                    .spawn_scoped(scope, move || {
                        if !gate.enter(ready, placement.pin(P + index)) {
                            return (0, 0u64, 0u64);
                        }
                        let (mut count, mut sum, mut xor) = (0, 0u64, 0u64);
                        while let Some(read) = consumer.recv() {
                            let origin =
                                u64::from_le_bytes(read.as_slice()[0][..8].try_into().unwrap())
                                    as usize
                                    / per;
                            for value in read.as_slice() {
                                let (id, bad) = payload::inspect(value, fixtures, total);
                                assert_eq!(bad, 0);
                                count += 1;
                                sum = sum.wrapping_add(id);
                                xor ^= id;
                            }
                            drop(read);
                            completed[origin].0.fetch_add(1, Ordering::Release);
                        }
                        (count, sum, xor)
                    })
                    .map_err(|e| e.to_string())?,
            );
        }
        drop(ready_tx);
        for _ in 0..P + C {
            ready_rx.recv().map_err(|_| "worker failed setup")??;
        }
        gate.open();
        let reports = senders
            .into_iter()
            .map(|s| s.join().map_err(|_| "producer panicked".to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let results = receivers
            .into_iter()
            .map(|r| r.join().map_err(|_| "consumer panicked".to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((reports, results))
    })?;
    let (count, sum, xor) = results
        .into_iter()
        .fold(setup_result, |(n, s, x), (nn, ss, xx)| {
            (n + nn, s.wrapping_add(ss), x ^ xx)
        });
    assert_eq!(count, total);
    assert_eq!(sum, ((total as u128 * (total - 1) as u128) / 2) as u64);
    assert_eq!(xor, payload::xor_prefix((total - 1) as u64));
    for (producer, windows) in reports.iter().enumerate() {
        assert_eq!(windows.len(), WINDOWS);
        for (
            window,
            (
                (known, touched, median, p95, gap),
                outstanding,
                window_ns,
                (reclaimed, calls, yields, recovery_ns),
            ),
        ) in windows.iter().enumerate()
        {
            eprintln!(
                "pool_window;recovery_sweeps={};recovery_window={};reclaimed={reclaimed};reclaim_calls={calls};recovery_yields={yields};recovery_ns={recovery_ns};window_ns={window_ns};direct={DIRECT};expanded={expanded};drained_start={drained_start};unacknowledged_blocks={outstanding};producer={producer};window={window};known={known};distinct={touched};reuse_distance_p50={median};reuse_distance_p95={p95};max_gap={gap};capacity={CAPACITY};batch={BATCH};bytes=4096;blocks_per_window={WINDOW};cpus={};validated_messages={count}",
                recovery.map_or(0, |p| p.sweeps),
                recovery.map_or(0, |p| p.window),
                placement.label()
            );
        }
    }
    Ok(())
}

#[test]
#[ignore = "allocation tracing perturbs execution; diagnostic only, not timing evidence"]
fn concurrent_pool_identity_trace() {
    for expanded in [false, true] {
        concurrent_trace::<false>(expanded, None).unwrap();
        concurrent_trace::<true>(expanded, None).unwrap();
    }
}

#[test]
fn trace_distinguishes_reuse_distance_from_elapsed_reservations() {
    let mut trace = ReuseTrace::new(3);
    for address in [10, 20, 20, 20, 30, 10] {
        trace.observe(address);
    }
    assert_eq!(trace.window(), (3, 3, 0, 2, 5));
    for _ in 0..8 {
        trace.observe(10);
    }
    assert_eq!(trace.window(), (3, 1, 0, 0, 1));
}

#[test]
fn a_cold_return_is_not_hidden_by_any_hot_lane_position() {
    for consumers in [2, 3] {
        for hot in 0..consumers {
            for cold in 0..consumers {
                if hot == cold {
                    continue;
                }
                for blocks in [2, 17] {
                    let (mut tx, mut rx) = leased::channel::<u64>()
                        .producers(1)
                        .consumers(consumers)
                        .capacity(blocks)
                        .batch(1)
                        .open()
                        .unwrap();
                    for id in 0..blocks {
                        let mut write = tx[0].reserve().unwrap();
                        write.push(id as u64).unwrap();
                        write.commit().unwrap();
                    }
                    for _ in 1..blocks {
                        drop(rx[hot].recv().unwrap());
                    }
                    let read = rx[cold].recv().unwrap();
                    let address = read.as_slice().as_ptr() as usize;
                    drop(read);
                    let mut reused = false;
                    for id in 0..consumers * blocks * 2 {
                        let mut write = tx[0].try_reserve().unwrap();
                        reused |= write.spare_capacity_mut().as_ptr() as usize == address;
                        write.push(id as u64).unwrap();
                        write.commit().unwrap();
                        assert_eq!(rx[hot].recv().unwrap().as_slice(), &[id as u64]);
                        if reused {
                            break;
                        }
                    }
                    assert!(
                        reused,
                        "cold={cold}, hot={hot}, consumers={consumers}, blocks={blocks}"
                    );
                    assert!(tx[0].flush());
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct RecoverySchedule {
    sweeps: usize,
    window: usize,
}

// Public-API, diagnostic-only policy. No publication can occur through the
// exclusively borrowed producer during a sweep, so at most the finite pool can
// be reclaimed. The budget bounds calls/yields, NOT elapsed time: scheduling and
// arbitrary T::drop can take unbounded time. Never use ACKs as a control oracle.
fn bounded_reclaim<T: Send>(
    producer: &mut leased::Producer<T>,
    sweeps: usize,
    blocks: usize,
) -> (usize, usize, usize) {
    let (mut reclaimed, mut calls, mut yields) = (0, 0, 0);
    for attempt in 0..sweeps {
        if attempt != 0 {
            std::thread::yield_now();
            yields += 1;
        }
        reclaimed += producer.reclaim();
        calls += 1;
        assert!(reclaimed <= blocks);
        if reclaimed == blocks {
            break;
        }
    }
    (reclaimed, calls, yields)
}

#[test]
fn bounded_recovery_preserves_held_progress_and_full_capacity() {
    for blocks in [2, 17, 64] {
        for held_position in [0, 1] {
            let (mut tx, mut rx) = leased::channel::<u64>()
                .consumers(2)
                .capacity(blocks)
                .batch(1)
                .open()
                .unwrap();
            let mut original = BTreeSet::new();
            for id in 0..blocks {
                let mut write = tx[0].try_reserve().unwrap();
                original.insert(write.spare_capacity_mut().as_ptr() as usize);
                write.push(id as u64).unwrap();
                write.commit().unwrap();
            }
            assert_eq!(bounded_reclaim(&mut tx[0], 0, blocks), (0, 0, 0));
            // All queued: the finite policy returns without waiting for progress.
            assert_eq!(bounded_reclaim(&mut tx[0], 3, blocks), (0, 3, 2));
            assert!(tx[0].try_reserve().is_none());
            let (left, right) = rx.split_at_mut(1);
            let (holder, worker) = if held_position == 0 {
                (&mut left[0], &mut right[0])
            } else {
                (&mut right[0], &mut left[0])
            };
            let held = holder.recv().unwrap();
            assert_eq!(held.as_slice(), &[0]);
            // One held + one returned + the remainder still queued.
            drop(worker.recv().unwrap());
            assert_eq!(bounded_reclaim(&mut tx[0], 3, blocks), (1, 3, 2));
            let mut write = tx[0].try_reserve().unwrap();
            write.push(blocks as u64).unwrap();
            write.commit().unwrap();
            for id in 2..=blocks {
                assert_eq!(worker.recv().unwrap().as_slice(), &[id as u64]);
            }
            assert_eq!(held.as_slice(), &[0]);
            drop(held);
            assert_eq!(bounded_reclaim(&mut tx[0], blocks, blocks), (blocks, 1, 0));
            let mut recovered = BTreeSet::new();
            for id in 0..blocks {
                let mut write = tx[0].try_reserve().unwrap();
                recovered.insert(write.spare_capacity_mut().as_ptr() as usize);
                write.push(id as u64).unwrap();
                write.commit().unwrap();
            }
            assert_eq!(recovered, original);
            assert!(tx[0].try_reserve().is_none());
            for id in 0..blocks {
                assert_eq!(worker.recv().unwrap().as_slice(), &[id as u64]);
            }
            assert!(tx[0].flush());
        }
    }
}

#[test]
#[ignore = "finite recovery/latency diagnostic; not a production throughput benchmark"]
fn bounded_recovery_opportunity_trace() {
    let parse = |name: &str| std::env::var(name).unwrap().parse::<usize>().unwrap();
    let policy = RecoverySchedule {
        sweeps: parse("PRESCIENT_RECOVERY_SWEEPS"),
        window: parse("PRESCIENT_RECOVERY_WINDOW"),
    };
    assert!(policy.sweeps <= 4096 && policy.window < 8);
    for expanded in [false, true] {
        concurrent_trace::<false>(expanded, Some(policy)).unwrap();
        concurrent_trace::<true>(expanded, Some(policy)).unwrap();
    }
}

#[test]
fn cold_return_service_drops_values_once_and_preserves_burst_capacity() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Value {
        id: usize,
        drops: Arc<Vec<AtomicUsize>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
        }
    }

    for consumers in [2, 3] {
        for hot in 0..consumers {
            for cold in 0..consumers {
                if hot == cold {
                    continue;
                }
                for blocks in [2, 17] {
                    let rounds = consumers * blocks * 2;
                    let total = blocks * 2 + rounds;
                    let drops =
                        Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
                    let value = |id| Value {
                        id,
                        drops: drops.clone(),
                    };
                    let (mut tx, mut rx) = leased::channel::<Value>()
                        .consumers(consumers)
                        .capacity(blocks)
                        .batch(1)
                        .open()
                        .unwrap();
                    let mut original = BTreeSet::new();
                    for id in 0..blocks {
                        let mut write = tx[0].try_reserve().unwrap();
                        assert!(original.insert(write.spare_capacity_mut().as_ptr() as usize));
                        write.push(value(id)).unwrap();
                        write.commit().unwrap();
                    }
                    for id in 0..blocks - 1 {
                        assert_eq!(rx[hot].recv().unwrap().as_slice()[0].id, id);
                    }
                    assert_eq!(rx[cold].recv().unwrap().as_slice()[0].id, blocks - 1);
                    // Reclamation/Drop is the service obligation. Unlike the
                    // separate baseline pointer-order test, this deliberately
                    // does not require the cold allocation to join hot rotation.
                    for id in blocks..blocks + rounds {
                        let mut write = tx[0].try_reserve().unwrap();
                        write.push(value(id)).unwrap();
                        write.commit().unwrap();
                        assert_eq!(rx[hot].recv().unwrap().as_slice()[0].id, id);
                    }
                    assert_eq!(
                        drops[blocks - 1].load(Ordering::Relaxed),
                        1,
                        "unserviced cold lane {cold}, hot {hot}, pool {blocks}"
                    );
                    assert!(tx[0].flush());
                    assert!(
                        drops[..blocks + rounds]
                            .iter()
                            .all(|n| n.load(Ordering::Relaxed) == 1)
                    );

                    // Parked storage must still be available to a full burst.
                    let mut recovered = BTreeSet::new();
                    for id in blocks + rounds..total {
                        let mut write = tx[0].try_reserve().unwrap();
                        assert!(recovered.insert(write.spare_capacity_mut().as_ptr() as usize));
                        write.push(value(id)).unwrap();
                        write.commit().unwrap();
                    }
                    assert_eq!(recovered, original);
                    assert!(tx[0].try_reserve().is_none());
                    for id in blocks + rounds..total {
                        assert_eq!(rx[hot].recv().unwrap().as_slice()[0].id, id);
                    }
                    assert!(tx[0].flush());
                    drop((tx, rx));
                    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
                }
            }
        }
    }
}
