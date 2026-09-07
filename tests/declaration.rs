use std::mem::{align_of, size_of};

use prescient::mpsc;
use prescient::wait::{Hybrid, Park, Spin, StdThread, Task};
use prescient::{Channel, OpenError};

fn same_type<T>(_: &T, _: &T) {}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn ipc_endpoint() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "t-{:x}-{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn direct_and_fixed_shorthand_are_the_same_types() {
    let direct = Channel::<u64>::new().producers(2).capacity(64).batch(8);
    let shorthand = mpsc::fixed::channel::<u64>()
        .producers(2)
        .capacity(64)
        .batch(8);
    same_type(&direct, &shorthand);

    let direct = direct.open().unwrap();
    let shorthand = shorthand.open().unwrap();
    same_type(&direct.0[0], &shorthand.0[0]);
    same_type(&direct.1, &shorthand.1);
    assert_eq!(size_of_val(&direct.0[0]), size_of_val(&shorthand.0[0]));
    assert_eq!(size_of_val(&direct.1), size_of_val(&shorthand.1));
}

#[test]
fn mpsc_topologies_open_through_one_declaration() {
    let (mut fixed, mut fixed_rx) = Channel::<u64>::new()
        .producers(1)
        .capacity(8)
        .open()
        .unwrap();
    fixed[0].send(1).unwrap();
    drop(fixed);
    assert_eq!(fixed_rx.recv(), Some(1));
    assert_eq!(fixed_rx.recv(), None);

    let (pool, mut pool_rx) = mpsc::pool::channel::<u64>()
        .max_producers(2)
        .capacity(8)
        .open()
        .unwrap();
    let mut producer = pool.claim().unwrap();
    producer.send(2).unwrap();
    drop(producer);
    drop(pool);
    assert_eq!(pool_rx.recv(), Some(2));
    assert_eq!(pool_rx.recv(), None);

    let (registrar, mut dynamic_rx) = mpsc::dynamic::channel::<u64>()
        .expected_producers(2)
        .capacity(8)
        .recycling(2)
        .open()
        .unwrap();
    let mut producer = registrar.register();
    producer.send(3).unwrap();
    drop(producer);
    drop(registrar);
    assert_eq!(dynamic_rx.recv(), Some(3));
    assert_eq!(dynamic_rx.recv(), None);
}

#[test]
fn wait_policies_are_concrete_and_can_coexist() {
    let (_, park) = Channel::<u64>::new().wait::<Park>().open().unwrap();
    let (_, std_thread) = Channel::<u64>::new().wait::<StdThread>().open().unwrap();
    let (_, hybrid) = Channel::<u64>::new().wait::<Hybrid>().open().unwrap();
    let (_, spin) = Channel::<u64>::new().wait::<Spin>().open().unwrap();

    assert_eq!(size_of_val(&park), size_of::<mpsc::Receiver<u64, Park>>());
    assert_eq!(
        size_of_val(&std_thread),
        size_of::<mpsc::Receiver<u64, StdThread>>()
    );
    assert_eq!(
        size_of_val(&hybrid),
        size_of::<mpsc::Receiver<u64, Hybrid>>()
    );
    assert_eq!(size_of_val(&spin), size_of::<mpsc::Receiver<u64, Spin>>());
    assert_eq!(align_of::<mpsc::Receiver<u64, Park>>(), align_of::<usize>());
}

#[test]
fn async_layout_omits_the_blocking_waiter() {
    let (mut producers, mut receiver) = Channel::<u64>::new().r#async().open().unwrap();
    assert!(size_of::<mpsc::Receiver<u64, Task>>() < size_of::<mpsc::Receiver<u64, Park>>());
    assert!(size_of::<mpsc::Producer<u64, Park>>() < size_of::<mpsc::Producer<u64, Task>>());

    producers[0].try_send(7).unwrap();
    assert_eq!(receiver.try_recv(), Some(7));
}

#[test]
fn open_validates_numeric_shape_once() {
    assert!(matches!(
        Channel::<u64>::new().capacity(0).open(),
        Err(OpenError::Invalid("channel capacity must be non-zero"))
    ));
    assert!(matches!(
        Channel::<u64>::new().batch(0).open(),
        Err(OpenError::Invalid("channel batch must be non-zero"))
    ));
    assert!(matches!(
        mpsc::pool::channel::<u64>().max_producers(0).open(),
        Err(OpenError::Invalid(
            "MPSC pool max_producers must be in 1..=64"
        ))
    ));
    assert!(matches!(
        mpsc::dynamic::channel::<u64>().recycling(0).open(),
        Err(OpenError::Invalid(
            "MPSC dynamic recycling capacity must be non-zero"
        ))
    ));
    assert!(matches!(
        Channel::<u64>::new().capacity(usize::MAX).open(),
        Err(OpenError::Invalid(
            "channel capacity overflows its ring shape"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::brokerless::channel::<u64>()
            .producers(0)
            .open(),
        Err(OpenError::Invalid(
            "brokerless MPMC producers must be non-zero"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::brokerless::channel::<u64>()
            .consumers(0)
            .open(),
        Err(OpenError::Invalid(
            "brokerless MPMC consumers must be non-zero"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::brokerless::dynamic::array::<u64>(0).open(),
        Err(OpenError::Invalid(
            "array MPMC max_producers must be non-zero"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::broadcast::channel::<u64>()
            .max_readers(0)
            .open(),
        Err(OpenError::Invalid(
            "broadcast max_readers must be in 1..=64"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::brokered::channel::<u64>()
            .producers(1)
            .brokers(2)
            .open(),
        Err(OpenError::Invalid(
            "brokered MPMC brokers cannot exceed producers"
        ))
    ));
    assert!(matches!(
        prescient::mpmc::brokered::channel::<u64>()
            .consumers(65)
            .pubsub(|_, n| prescient::mpmc::brokered::Targets::all(n))
            .open(),
        Err(OpenError::Invalid(
            "brokered pub/sub supports at most 64 consumers"
        ))
    ));
}

#[test]
fn mpmc_backends_and_dynamic_membership_are_typed() {
    use prescient::backend::Seg;
    use prescient::mpmc::{brokerless, lanes};

    let direct = Channel::<u64>::new().mpmc();
    let shorthand = brokerless::channel::<u64>();
    same_type(&direct, &shorthand);
    same_type(
        &direct,
        &Channel::<u64>::new()
            .mpmc()
            .engine::<prescient::engine::Claim>(),
    );

    let (mut producers, mut consumers) =
        direct.producers(2).consumers(2).capacity(8).open().unwrap();
    assert!(producers[0].send(10));
    assert!(producers[1].send(20));
    drop(producers);
    let mut values = Vec::new();
    for consumer in &mut consumers {
        while let Some(value) = consumer.recv() {
            values.push(value);
        }
    }
    values.sort_unstable();
    assert_eq!(values, [10, 20]);

    let direct = Channel::<u64>::new()
        .mpmc()
        .engine::<prescient::engine::Lanes>();
    let shorthand = lanes::channel::<u64>();
    same_type(&direct, &shorthand);
    let (mut lane_producers, mut lane_consumers) =
        direct.producers(2).consumers(2).capacity(8).open().unwrap();
    assert!(lane_producers[0].send(21));
    assert!(lane_producers[1].send(22));
    drop(lane_producers);
    let mut lane_values = Vec::new();
    for consumer in &mut lane_consumers {
        while let Some(value) = consumer.recv() {
            lane_values.push(value);
        }
    }
    lane_values.sort_unstable();
    assert_eq!(lane_values, [21, 22]);

    let (mut seg_producers, mut seg_consumers) = brokerless::channel::<u64>()
        .backend::<Seg>()
        .capacity(8)
        .open()
        .unwrap();
    assert!(seg_producers[0].send(30));
    drop(seg_producers);
    assert_eq!(seg_consumers[0].recv(), Some(30));
    assert_eq!(seg_consumers[0].recv(), None);

    let (locked, mut locked_consumers) = brokerless::dynamic::locked::<u64>()
        .expected_producers(2)
        .consumers(2)
        .open()
        .unwrap();
    let mut producer = locked.register();
    assert!(producer.send(40));
    drop(producer);
    drop(locked);
    assert_eq!(locked_consumers[0].recv(), Some(40));

    let (array, mut array_consumers) = brokerless::dynamic::array::<u64>(2)
        .consumers(1)
        .open()
        .unwrap();
    let mut producer = array.register();
    assert!(producer.send(50));
    drop(producer);
    drop(array);
    assert_eq!(array_consumers[0].recv(), Some(50));
}

#[test]
fn broker_routing_is_concrete_and_broadcast_is_distinct() {
    use prescient::mpmc::{broadcast, brokered};

    let (mut producers, mut consumers) = brokered::channel::<u64>()
        .producers(1)
        .consumers(2)
        .route(|value: &u64, _| *value as usize)
        .open()
        .unwrap();
    for value in 0..8 {
        producers[0].send(value);
    }
    drop(producers);
    let even: Vec<_> = std::iter::from_fn(|| consumers[0].recv()).collect();
    let odd: Vec<_> = std::iter::from_fn(|| consumers[1].recv()).collect();
    assert_eq!(even, [0, 2, 4, 6]);
    assert_eq!(odd, [1, 3, 5, 7]);

    let (mut publisher, mut first) = broadcast::channel::<u64>()
        .capacity(8)
        .max_readers(2)
        .open()
        .unwrap();
    let mut second = publisher.subscribe().unwrap();
    publisher.send(9);
    drop(publisher);
    assert_eq!(first.recv(), Some(9));
    assert_eq!(second.recv(), Some(9));
    assert_eq!(first.recv(), None);
    assert_eq!(second.recv(), None);
}
#[test]
fn broker_run_and_routing_axes_are_order_independent() {
    use prescient::mpmc::brokered;

    fn route(value: &u64, consumers: usize) -> usize {
        *value as usize % consumers
    }
    fn subscribers(_: &u64, consumers: usize) -> prescient::mpmc::brokered::Targets {
        prescient::mpmc::brokered::Targets::all(consumers)
    }

    let ownership_first = brokered::channel::<u64>().manual().route(route);
    let routing_first = brokered::channel::<u64>().route(route).manual();
    same_type(&ownership_first, &routing_first);

    let ownership_first = brokered::channel::<u64>().manual().pubsub(subscribers);
    let routing_first = brokered::channel::<u64>().pubsub(subscribers).manual();
    same_type(&ownership_first, &routing_first);
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_channel_uses_plain_open_and_retains_peer_liveness() {
    use std::time::Duration;

    let worker_open = std::thread::spawn(move || {
        Channel::<u8>::new()
            .ipc()
            .attach()
            .timeout(Duration::from_secs(2))
            .open()
            .unwrap()
    });
    let mut parent = Channel::<u8>::new()
        .ipc()
        .shape(4096, 256, 4)
        .transfer_id(100)
        .timeout(Duration::from_secs(2))
        .open()
        .unwrap();
    let mut worker = worker_open.join().unwrap();

    let input = vec![0x5a; 4096];
    let expected = input.clone();
    let output = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| {
            let mut output = Vec::new();
            worker.receive(&mut output).unwrap();
            output
        });
        parent.send(&mut input.as_slice()).unwrap();
        consumer.join().unwrap()
    });
    assert_eq!(output, expected);
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_pod_contract_round_trips_and_mismatch_fails_before_mapping() {
    use std::time::Duration;

    use prescient::ipc;

    same_type(
        &Channel::<u64>::new().ipc().pod(),
        &ipc::pod::channel::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().ipc().attach().pod(),
        &ipc::pod::attach::<u64>(),
    );

    let input = vec![1u64, 2, u64::MAX, 9];
    let bytes = (input.len() * size_of::<u64>()) as u64;
    let endpoint = ipc_endpoint();
    let worker_endpoint = endpoint.clone();
    let mismatch = std::thread::spawn(move || ipc::attach().endpoint(worker_endpoint).open());
    let creator = ipc::pod::channel::<u64>()
        .endpoint(endpoint)
        .shape(bytes, 16, 2)
        .timeout(Duration::from_secs(2))
        .open();
    assert!(matches!(
        creator,
        Err(OpenError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
    assert!(matches!(
        mismatch.join().unwrap(),
        Err(OpenError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidInput
    ));

    let endpoint = ipc_endpoint();
    let worker_endpoint = endpoint.clone();
    let worker_open = std::thread::spawn(move || {
        ipc::pod::attach::<u64>()
            .endpoint(worker_endpoint)
            .timeout(Duration::from_secs(2))
            .open()
            .unwrap()
    });
    let mut parent = ipc::pod::channel::<u64>()
        .endpoint(endpoint)
        .shape(bytes, 16, 2)
        .transfer_id(200)
        .timeout(Duration::from_secs(2))
        .open()
        .unwrap();
    let mut worker = worker_open.join().unwrap();

    let output = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| worker.receive().unwrap().0);
        parent.send(&input).unwrap();
        consumer.join().unwrap()
    });
    assert_eq!(output, input);
    let returned = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| parent.receive().unwrap().0);
        worker.send(&output).unwrap();
        consumer.join().unwrap()
    });
    assert_eq!(returned, input);
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_pod_aligned_fragments_empty_and_reused_transfers() {
    use prescient::ipc;
    use std::time::Duration;

    #[repr(C, align(64))]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Aligned([u8; 64]);
    // SAFETY: exactly 64 bytes, no padding or provenance, every bit pattern valid.
    unsafe impl ipc::IpcPod for Aligned {
        const SCHEMA_ID: u64 = 0x5053_5445_5354_0002;
    }

    for count in [0, 1, 3] {
        for chunk in [1, 65] {
            let bytes = (count * size_of::<Aligned>()) as u64;
            let endpoint = ipc_endpoint();
            let worker_endpoint = endpoint.clone();
            let worker_open = std::thread::spawn(move || {
                ipc::pod::attach::<Aligned>()
                    .endpoint(worker_endpoint)
                    .timeout(Duration::from_secs(5))
                    .open()
                    .unwrap()
            });
            let mut parent = ipc::pod::channel::<Aligned>()
                .endpoint(endpoint)
                .shape(bytes, chunk, 1)
                .timeout(Duration::from_secs(5))
                .open()
                .unwrap();
            let mut worker = worker_open.join().unwrap();

            for round in 0..3 {
                let input: Vec<_> = (0..count)
                    .map(|i| Aligned(std::array::from_fn(|j| (i * 17 + j + round) as u8)))
                    .collect();
                std::thread::scope(|scope| {
                    let transfer = scope.spawn(|| {
                        let (received, report) = worker.receive().unwrap();
                        assert_eq!(received.as_ptr().addr() % 64, 0);
                        assert_eq!(received, input);
                        assert_eq!(report.total_bytes, bytes);
                        worker.send(&received).unwrap();
                        report
                    });
                    let sent = parent.send(&input).unwrap();
                    let (returned, report) = parent.receive().unwrap();
                    assert_eq!(returned.as_ptr().addr() % 64, 0);
                    assert_eq!(returned, input);
                    assert_eq!(report.total_bytes, bytes);
                    assert_eq!(transfer.join().unwrap().checksum, sent.checksum);
                });
            }
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_explicit_codec_round_trips_and_is_contract_checked() {
    use std::time::Duration;

    use prescient::ipc::{self, IpcCodec};

    struct U64VecLe;

    impl IpcCodec<Vec<u64>> for U64VecLe {
        const SCHEMA_ID: u64 = 0x5053_5445_5354_0001;

        fn encode(value: &Vec<u64>, output: &mut Vec<u8>) -> std::io::Result<()> {
            output.reserve(value.len() * size_of::<u64>());
            for item in value {
                output.extend_from_slice(&item.to_le_bytes());
            }
            Ok(())
        }

        fn decode(input: &[u8]) -> std::io::Result<Vec<u64>> {
            if !input.len().is_multiple_of(size_of::<u64>()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "encoded u64 vector is misaligned",
                ));
            }
            Ok(input
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| u64::from_le_bytes(*chunk))
                .collect())
        }
    }

    same_type(
        &Channel::<Vec<u64>>::new().ipc().codec::<U64VecLe>(),
        &ipc::codec::channel::<Vec<u64>, U64VecLe>(),
    );

    let input = vec![3u64, 5, 8, 13, 21];
    let endpoint = ipc_endpoint();
    let worker_endpoint = endpoint.clone();
    let worker_open = std::thread::spawn(move || {
        ipc::codec::attach::<Vec<u64>, U64VecLe>()
            .endpoint(worker_endpoint)
            .timeout(Duration::from_secs(2))
            .open()
            .unwrap()
    });
    let mut parent = ipc::codec::channel::<Vec<u64>, U64VecLe>()
        .endpoint(endpoint)
        .shape(1024, 64, 2)
        .transfer_id(300)
        .timeout(Duration::from_secs(2))
        .open()
        .unwrap();
    let mut worker = worker_open.join().unwrap();

    let output = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| worker.receive().unwrap().0);
        parent.send(&input).unwrap();
        consumer.join().unwrap()
    });
    assert_eq!(output, input);
    let returned = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| parent.receive().unwrap().0);
        worker.send(&output).unwrap();
        consumer.join().unwrap()
    });
    assert_eq!(returned, input);
}

#[test]
fn topology_selection_preserves_the_execution_axis() {
    use prescient::backend::Ring;
    use prescient::execution::Async;
    use prescient::wait::SpinYield;

    fn async_mpmc(_: &Channel<u64, prescient::topology::Mpmc, Ring, Async, SpinYield>) {}
    async_mpmc(&Channel::<u64>::new().r#async().mpmc());

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    fn async_ipc(
        _: &Channel<
            u8,
            prescient::topology::ProcessDuplex,
            Ring,
            Async,
            prescient::wait::Park,
            prescient::routing::RoundRobin,
            prescient::transport::ProcessCreate,
            prescient::codec::Bytes,
        >,
    ) {
    }
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    async_ipc(&Channel::<u8>::new().r#async().ipc());
}

#[test]
fn sync_preserves_an_explicit_waiter_and_opens_mpmc() {
    same_type(
        &mpsc::fixed::channel::<u64>().wait::<Spin>(),
        &mpsc::fixed::channel::<u64>().wait::<Spin>().sync(),
    );
    let (mut producers, mut consumers) = prescient::mpmc::brokerless::channel::<u64>()
        .sync()
        .open()
        .unwrap();
    assert!(producers[0].send(42));
    drop(producers);
    assert_eq!(consumers[0].recv(), Some(42));
    assert_eq!(consumers[0].recv(), None);
}

#[test]
fn sync_round_trips_use_each_topologys_supported_default() {
    macro_rules! same_opened_type {
        ($declaration:expr) => {{
            let expected = $declaration.open().unwrap();
            let restored = $declaration.r#async().sync().open().unwrap();
            same_type(&expected, &restored);
        }};
    }
    same_opened_type!(mpsc::fixed::channel::<u64>());
    same_opened_type!(mpsc::pool::channel::<u64>());
    same_opened_type!(mpsc::dynamic::channel::<u64>().recycling(2));

    same_opened_type!(
        mpsc::dynamic::channel::<u64>()
            .membership::<prescient::membership::LockFree>()
            .recycling(2)
    );
    same_opened_type!(prescient::mpmc::brokerless::channel::<u64>());
    same_opened_type!(
        prescient::mpmc::brokerless::channel::<u64>().backend::<prescient::backend::Seg>()
    );
    same_type(
        &Channel::<u64>::new().mpmc(),
        &Channel::<u64>::new().r#async().mpmc().sync(),
    );
    same_opened_type!(prescient::mpmc::brokerless::dynamic::locked::<u64>());
    same_opened_type!(prescient::mpmc::brokerless::dynamic::array::<u64>(2));
    same_opened_type!(prescient::mpmc::broadcast::channel::<u64>());
    fn route(value: &u64, count: usize) -> usize {
        *value as usize % count
    }
    same_opened_type!(
        prescient::mpmc::brokered::channel::<u64>()
            .manual()
            .route(route)
    );
}

#[test]
fn every_namespace_is_the_same_declaration_as_its_direct_form() {
    same_type(
        &Channel::<u64>::new().mpsc_pool(),
        &mpsc::pool::channel::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().mpsc_dynamic(),
        &mpsc::dynamic::channel::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().mpmc(),
        &prescient::mpmc::brokerless::channel::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().mpmc_dynamic_locked(),
        &prescient::mpmc::brokerless::dynamic::locked::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().mpmc_dynamic_array(4),
        &prescient::mpmc::brokerless::dynamic::array::<u64>(4),
    );
    same_type(
        &Channel::<u64>::new().mpmc_brokered(),
        &prescient::mpmc::brokered::channel::<u64>(),
    );
    same_type(
        &Channel::<u64>::new().broadcast(),
        &prescient::mpmc::broadcast::channel::<u64>(),
    );
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    same_type(&Channel::<u8>::new().ipc(), &prescient::ipc::channel());
}

#[test]
fn fixed_receivers_carry_no_pool_or_dynamic_membership_state() {
    let (fixed_tx, fixed) = mpsc::fixed::channel::<u64>().open().unwrap();
    let (pool_handle, pool) = mpsc::pool::channel::<u64>()
        .max_producers(1)
        .open()
        .unwrap();
    let (registrar, dynamic) = mpsc::dynamic::channel::<u64>().open().unwrap();

    assert!(size_of_val(&fixed) < size_of_val(&pool));
    assert!(size_of_val(&fixed) < size_of_val(&dynamic));
    drop((fixed_tx, pool_handle, registrar));
}

#[test]
fn lock_free_membership_is_explicit_and_typed() {
    use prescient::membership::LockFree;

    let direct = Channel::<u64>::new()
        .mpsc_dynamic()
        .membership::<LockFree>();
    let shorthand = mpsc::dynamic::channel::<u64>().membership::<LockFree>();
    same_type(&direct, &shorthand);

    let (registrar, mut receiver) = shorthand.recycling(2).open().unwrap();
    let mut producer = registrar.register();
    producer.send(71).unwrap();
    drop(producer);
    drop(registrar);
    assert_eq!(receiver.recv(), Some(71));
    assert_eq!(receiver.recv(), None);
}
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_shape_errors_fail_before_resource_creation() {
    use std::time::Duration;

    use prescient::ipc;

    assert!(matches!(
        ipc::channel().shape(1, 0, 1).open(),
        Err(OpenError::Invalid("IPC chunk_bytes must be non-zero"))
    ));
    assert!(matches!(
        ipc::channel().shape(1, 1, 0).open(),
        Err(OpenError::Invalid("IPC slots must be non-zero"))
    ));
    assert!(matches!(
        ipc::channel().shape(1, 1, 1).transfer_id(u64::MAX).open(),
        Err(OpenError::Invalid(
            "IPC duplex transfer_id must leave two non-zero ids"
        ))
    ));
    assert!(matches!(
        ipc::channel().shape(1, 1, 1).timeout(Duration::ZERO).open(),
        Err(OpenError::Invalid("IPC timeout must be non-zero"))
    ));

    #[derive(Clone, Copy)]
    struct ZeroSized;
    // SAFETY: this intentionally hostile implementation verifies that `open()`
    // rejects a zero-sized contract even if an unsafe implementor lies.
    unsafe impl ipc::IpcPod for ZeroSized {
        const SCHEMA_ID: u64 = 1;
    }
    assert!(matches!(
        ipc::pod::channel::<ZeroSized>().shape(0, 1, 1).open(),
        Err(OpenError::Invalid(
            "IPC POD shape must contain a whole number of values"
        ))
    ));

    struct ZeroCodec;
    impl ipc::IpcCodec<u8> for ZeroCodec {
        const SCHEMA_ID: u64 = 0;

        fn encode(_: &u8, _: &mut Vec<u8>) -> std::io::Result<()> {
            Ok(())
        }

        fn decode(_: &[u8]) -> std::io::Result<u8> {
            Ok(0)
        }
    }
    assert!(matches!(
        ipc::codec::channel::<u8, ZeroCodec>()
            .shape(1, 1, 1)
            .open(),
        Err(OpenError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidInput
    ));
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[test]
fn ipc_duplex_round_trips_across_spawned_process() {
    use std::process::Command;
    use std::time::Duration;

    use prescient::ipc;

    const ADDRESS_ENV: &str = "PRESCIENT_TEST_IPC_ADDRESS";

    if let Ok(endpoint) = std::env::var(ADDRESS_ENV) {
        let mut worker = ipc::attach()
            .endpoint(endpoint)
            .timeout(Duration::from_secs(5))
            .open()
            .unwrap();
        let mut received = Vec::new();
        worker.receive(&mut received).unwrap();
        assert_eq!(
            received,
            (0..4096).map(|index| index as u8).collect::<Vec<_>>()
        );
        let response: Vec<_> = received.into_iter().map(|byte| byte ^ 0xa5).collect();
        let mut source = response.as_slice();
        worker.send(&mut source).unwrap();
        return;
    }

    let endpoint = ipc_endpoint();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("ipc_duplex_round_trips_across_spawned_process")
        .arg("--nocapture")
        .env(ADDRESS_ENV, &endpoint)
        .spawn()
        .unwrap();
    let mut parent = ipc::channel()
        .endpoint(endpoint)
        .shape(4096, 128, 4)
        .transfer_id(400)
        .timeout(Duration::from_secs(5))
        .open()
        .unwrap();

    let input: Vec<_> = (0..4096).map(|index| index as u8).collect();
    let mut source = input.as_slice();
    parent.send(&mut source).unwrap();
    let mut response = Vec::new();
    parent.receive(&mut response).unwrap();
    assert_eq!(
        response,
        input
            .into_iter()
            .map(|byte| byte ^ 0xa5)
            .collect::<Vec<_>>()
    );
    assert!(child.wait().unwrap().success());
}
