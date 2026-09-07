use std::any::type_name_of_val;
use std::hint::black_box;
use std::mem::{align_of_val, size_of, size_of_val};

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use std::time::Duration;
use std::time::Instant;

use prescient::Channel;
use prescient::backend::{Ring, Seg};
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use prescient::ipc::{Receiver, Sender};
use prescient::mpmc::{broadcast, brokered, brokerless};
use prescient::mpsc::{self, dynamic, fixed, pool};
use prescient::wait::{Park, Spin, Task};

fn value(label: &str, value: &impl Sized) {
    println!(
        "{label};type={};size={};align={}",
        type_name_of_val(value),
        size_of_val(value),
        align_of_val(value)
    );
}

fn layout() {
    println!(
        "usize={};pointer={}",
        size_of::<usize>(),
        size_of::<*const ()>()
    );

    let fixed_decl = Channel::<u64>::new().producers(1).capacity(1024);
    let fixed_short = fixed::channel::<u64>().producers(1).capacity(1024);
    value("declaration.direct.fixed", &fixed_decl);
    value("declaration.shorthand.fixed", &fixed_short);
    let (fixed_tx, fixed_rx) = fixed_decl.open().unwrap();
    value("mpsc.fixed.producers", &fixed_tx);
    value("mpsc.fixed.producer", &fixed_tx[0]);
    value("mpsc.fixed.receiver", &fixed_rx);

    let (pool_handle, pool_rx) = pool::channel::<u64>().open().unwrap();
    value("mpsc.pool.handle", &pool_handle);
    value("mpsc.pool.receiver", &pool_rx);

    let (dynamic_reg, dynamic_rx) = dynamic::channel::<u64>().open().unwrap();
    value("mpsc.dynamic.registrar", &dynamic_reg);
    value("mpsc.dynamic.receiver", &dynamic_rx);

    let (async_tx, async_rx) = fixed::channel::<u64>().r#async().open().unwrap();
    value("mpsc.async.producer", &async_tx[0]);
    value("mpsc.async.receiver", &async_rx);

    let (bl_ring_tx, bl_ring_rx) = brokerless::channel::<u64>().open().unwrap();
    value("mpmc.brokerless.ring.producer", &bl_ring_tx[0]);
    value("mpmc.brokerless.ring.consumer", &bl_ring_rx[0]);

    let (bl_seg_tx, bl_seg_rx) = brokerless::channel::<u64>()
        .backend::<Seg>()
        .open()
        .unwrap();
    value("mpmc.brokerless.seg.producer", &bl_seg_tx[0]);
    value("mpmc.brokerless.seg.consumer", &bl_seg_rx[0]);

    let (br_tx, br_rx, br_brokers) = brokered::channel::<u64>().manual().open().unwrap();
    value("mpmc.brokered.producer", &br_tx[0]);
    value("mpmc.brokered.consumer", &br_rx[0]);
    value("mpmc.brokered.broker", &br_brokers[0]);

    let (publisher, reader) = broadcast::channel::<u64>().open().unwrap();
    value("broadcast.publisher", &publisher);
    value("broadcast.reader", &reader);

    println!("backend.ring.zst={}", size_of::<Ring>());
    println!("backend.seg.zst={}", size_of::<Seg>());
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    {
        println!("ipc.producer_direction.size={}", size_of::<Sender>());
        println!("ipc.consumer_direction.size={}", size_of::<Receiver>());
    }
}

fn select_bench(messages: usize, samples: usize) {
    let mut rates = Vec::with_capacity(samples);
    for sample in 0..=samples {
        let (mut atx, mut a) = fixed::channel::<u64>()
            .producers(1)
            .capacity(1024)
            .open()
            .unwrap();
        let (mut btx, mut b) = fixed::channel::<u64>()
            .producers(1)
            .capacity(1024)
            .open()
            .unwrap();
        let start = Instant::now();
        let mut sum = 0u64;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                for i in 0..messages as u64 {
                    atx[0].send(i).unwrap();
                }
            });
            scope.spawn(move || {
                for i in 0..messages as u64 {
                    btx[0].send(i).unwrap();
                }
            });
            loop {
                let done = prescient::select! {
                    recv(a) -> value => { sum = sum.wrapping_add(value); false },
                    recv(b) -> value => { sum = sum.wrapping_add(value); false },
                    complete => true,
                };
                if done {
                    break;
                }
            }
        });
        let elapsed = start.elapsed();
        let expected = (messages as u64).wrapping_mul((messages as u64).wrapping_sub(1));
        assert_eq!(sum, expected);
        if sample > 0 {
            rates.push((2 * messages) as f64 / elapsed.as_secs_f64());
        }
    }
    rates.sort_by(|a, b| a.total_cmp(b));
    println!(
        "select;messages={};samples={};median_mps={:.6};p95_mps={:.6}",
        messages * 2,
        samples,
        rates[samples / 2] / 1e6,
        rates[((samples as f64 * 0.95).ceil() as usize).min(samples) - 1] / 1e6,
    );
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn duplex_ipc_once(bytes: usize) -> (Duration, Duration, u64) {
    let endpoint = format!("p-{:x}", std::process::id());
    let peer_endpoint = endpoint.clone();
    let open_start = Instant::now();
    let peer = std::thread::spawn(move || {
        prescient::ipc::attach()
            .endpoint(peer_endpoint)
            .timeout(Duration::from_secs(5))
            .open()
            .unwrap()
    });
    let mut owner = prescient::ipc::channel()
        .endpoint(endpoint)
        .shape(bytes as u64, 64 * 1024, 8)
        .transfer_id(0x5053_0001)
        .timeout(Duration::from_secs(5))
        .open()
        .unwrap();
    let mut peer = peer.join().unwrap();
    let open_elapsed = open_start.elapsed();
    let input: Vec<u8> = (0..bytes).map(|i| (i as u8).wrapping_mul(31)).collect();
    let expected = input.clone();
    let transfer_start = Instant::now();
    let (producer, consumer) = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| {
            let mut output = Vec::with_capacity(bytes);
            let report = peer.receive(&mut output).unwrap();
            (output, report)
        });
        let producer = owner.send(&mut input.as_slice()).unwrap();
        (producer, consumer.join().unwrap())
    });
    let transfer_elapsed = transfer_start.elapsed();
    assert_eq!(consumer.0, expected);
    assert_eq!(producer.checksum, consumer.1.checksum);
    assert_eq!(producer.total_bytes, bytes as u64);
    (open_elapsed, transfer_elapsed, black_box(producer.checksum))
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn ipc_bench(
    label: &str,
    bytes: usize,
    samples: usize,
    run: fn(usize) -> (Duration, Duration, u64),
) {
    let _ = run(bytes);
    let mut opens = Vec::with_capacity(samples);
    let mut transfers = Vec::with_capacity(samples);
    let mut checksum = 0u64;
    for _ in 0..samples {
        let (open, transfer, sum) = run(bytes);
        opens.push(open.as_nanos() as u64);
        transfers.push(transfer.as_nanos() as u64);
        checksum ^= sum;
    }
    opens.sort_unstable();
    transfers.sort_unstable();
    let median_open = opens[samples / 2];
    let median_transfer = transfers[samples / 2];
    println!(
        "{label};bytes={bytes};samples={samples};median_open_ns={median_open};median_transfer_ns={median_transfer};median_mib_s={:.6};checksum={checksum:016x}",
        bytes as f64 / (1024.0 * 1024.0) / (median_transfer as f64 / 1e9),
    );
}

/// Code-generation probe for a concrete sender.
///
/// # Safety
/// `producer` must be non-null, aligned, and point to a live, uniquely borrowed
/// endpoint of the exact declared type for the duration of this call.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn prescient_park_try_send(
    producer: *mut mpsc::Producer<u64, Park>,
    value: u64,
) -> u8 {
    // SAFETY: the codegen harness passes one live, uniquely borrowed endpoint.
    unsafe { &mut *producer }.try_send(value).is_ok() as u8
}

/// Code-generation probe for a concrete sender.
///
/// # Safety
/// `producer` must be non-null, aligned, and point to a live, uniquely borrowed
/// endpoint of the exact declared type for the duration of this call.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn prescient_spin_try_send(
    producer: *mut mpsc::Producer<u64, Spin>,
    value: u64,
) -> u8 {
    // SAFETY: the codegen harness passes one live, uniquely borrowed endpoint.
    unsafe { &mut *producer }.try_send(value).is_ok() as u8
}

/// Code-generation probe for a concrete sender.
///
/// # Safety
/// `producer` must be non-null, aligned, and point to a live, uniquely borrowed
/// endpoint of the exact declared type for the duration of this call.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn prescient_task_try_send(
    producer: *mut mpsc::Producer<u64, Task>,
    value: u64,
) -> u8 {
    // SAFETY: the codegen harness passes one live, uniquely borrowed endpoint.
    unsafe { &mut *producer }.try_send(value).is_ok() as u8
}

/// Code-generation probe for a concrete receiver.
///
/// # Safety
/// `receiver` must be non-null, aligned, and point to a live, uniquely borrowed
/// endpoint of the exact declared type. `output` must be aligned and writable
/// for one `u64`, and must not alias the endpoint for the duration of this call.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn prescient_park_try_recv(
    receiver: *mut mpsc::Receiver<u64, Park>,
    output: *mut u64,
) -> u8 {
    // SAFETY: the codegen harness passes one live, uniquely borrowed endpoint and output slot.
    match unsafe { &mut *receiver }.try_recv() {
        Some(value) => {
            // SAFETY: `output` points at a writable `u64` supplied by the harness.
            unsafe { output.write(value) };
            1
        }
        None => 0,
    }
}

/// Code-generation probe for a concrete receiver.
///
/// # Safety
/// `receiver` must be non-null, aligned, and point to a live, uniquely borrowed
/// endpoint of the exact declared type. `output` must be aligned and writable
/// for one `u64`, and must not alias the endpoint for the duration of this call.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn prescient_task_try_recv(
    receiver: *mut mpsc::Receiver<u64, Task>,
    output: *mut u64,
) -> u8 {
    // SAFETY: the codegen harness passes one live, uniquely borrowed endpoint and output slot.
    match unsafe { &mut *receiver }.try_recv() {
        Some(value) => {
            // SAFETY: `output` points at a writable `u64` supplied by the harness.
            unsafe { output.write(value) };
            1
        }
        None => 0,
    }
}

fn codegen_smoke() {
    let (mut park_tx, mut park_rx) = fixed::channel::<u64>().open().unwrap();
    let (mut spin_tx, _) = fixed::channel::<u64>().wait::<Spin>().open().unwrap();
    let (mut task_tx, mut task_rx) = fixed::channel::<u64>().r#async().open().unwrap();
    let mut output = 0u64;
    // SAFETY: every pointer comes from a unique live mutable reference.
    unsafe {
        black_box(prescient_park_try_send(&mut park_tx[0], 1));
        black_box(prescient_park_try_recv(&mut park_rx, &mut output));
        black_box(prescient_spin_try_send(&mut spin_tx[0], 2));
        black_box(prescient_task_try_send(&mut task_tx[0], 3));
        black_box(prescient_task_try_recv(&mut task_rx, &mut output));
    }
    black_box(output);
}

mod pubsub;
fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("layout") {
        "layout" => layout(),
        "codegen" => codegen_smoke(),
        "pubsub_sparse" | "pubsub_dense" => {
            let messages = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(200_000);
            let samples = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(7);
            if args[1] == "pubsub_sparse" {
                let consumers = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(64);
                pubsub::sparse(messages, samples, consumers);
            } else {
                let consumers = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(2);
                pubsub::dense(messages, samples, consumers);
            }
        }
        "select" => select_bench(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20_000),
            args.get(3).and_then(|s| s.parse().ok()).unwrap_or(15),
        ),

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        "ipc_duplex" => ipc_bench(
            "ipc_duplex",
            args.get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(4 * 1024 * 1024),
            args.get(3).and_then(|s| s.parse().ok()).unwrap_or(15),
            duplex_ipc_once,
        ),
        other => panic!("unknown probe mode {other:?}"),
    }
}
