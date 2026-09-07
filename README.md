# Prescient

One typed declaration covers local MPSC, MPMC, broadcast, selection, and
process-to-process transport.

Configure a `Channel<T>`, then call `open()`. Rust specializes the chosen topology,
engine, storage, execution, waiter, and routing policy at compile time. Opened
endpoints do not dispatch through a configuration enum or erased policy callback.

## Quick start

Requires Rust 1.93 or newer.

```rust
use prescient::Channel;

let (mut producers, mut receiver) = Channel::<u64>::new()
    .capacity(256)
    .open()
    .unwrap();

producers[0].send(42).unwrap();
drop(producers);
assert_eq!(receiver.recv(), Some(42));
assert_eq!(receiver.recv(), None);
```

Namespaced invocations preconfigure the same declaration; explicit arguments
remain yours to choose. These forms produce the same concrete endpoint types:

```rust
use prescient::{Channel, backend::Ring, mpsc, wait::Park};

let direct = Channel::<u64>::new()
    .mpsc_fixed()
    .backend::<Ring>()
    .sync()
    .wait::<Park>()
    .producers(4);

let short = mpsc::fixed::channel::<u64>().producers(4);

let (_producers, _receiver) = direct.open().unwrap();
let (_producers, _receiver) = short.open().unwrap();
```

## Choices and defaults

| Topology | Engine or policy | Storage and execution |
|---|---|---|
| Fixed, pool, dynamic MPSC | topology-specific direct path | `Ring`; synchronous waiter or task wakeups |
| Fixed MPMC | `Claim` (default) or `Lanes` | `Ring` or `Seg`; synchronous spin/yield |
| Dynamic brokerless MPMC | locked or bounded-array membership | `Ring` or `Seg`; synchronous spin/yield |
| Brokered MPMC | spawned or manual broker; round-robin, routed, or pub/sub | `Ring` or `Seg`; synchronous |
| Broadcast | reader-gated fanout | `Ring`; synchronous spin/yield |
| Process duplex | bytes, POD, or an explicit codec | bounded shared-memory transport; synchronous |

The direct default is fixed MPSC, Ring, synchronous Park, one producer, and batch
64. Omitting `.capacity(...)` asks `open()` to choose it; an explicit value bypasses
the automatic policy. Ring storage rounds a requested capacity up to a power of two.
`.mpmc()` selects the Claim engine, while `.engine::<engine::Lanes>()` selects
permanent producer-consumer lanes. Payload type alone does not infer topology,
engine, or whether callers transfer batches.

### Automatic capacity

For native Ring declarations, `open()` first computes
`floor(1 MiB / max(size_of::<T>(), 1) / max(producers, consumers, 1))`, using the
topology declaration for producer and consumer counts. Pool MPSC uses
`max_producers`; dynamic declarations use their expected or bounded producer count.
The result selects one of four measured plateaus:

| Computed slots | Selected capacity |
|---:|---:|
| 0–127 | 64 |
| 128–511 | 256 |
| 512–2,047 | 1,024 |
| 2,048 or more | 4,096 |

The Lanes engine then divides the selected per-producer capacity among its permanent
consumer lanes. Leased channels instead compute
`floor(1 MiB * consumers / (max(size_of::<T>(), 1) * producers))`, apply the same
plateaus, cap payloads up to 64 bytes at 4,096 slots and larger payloads at 1,024,
then round upward to a whole batch. Seg storage, broadcast, and brokered channels
use 1,024 when capacity is omitted. These decisions happen only in `open()` and do
not add payload-size or topology dispatch to endpoint operations.

Unsupported combinations have no `open()` implementation; invalid numeric shapes
and resource failures return `OpenError` during opening.

`Park` uses parking_lot eventcounts; `StdThread` uses thread park/unpark;
`Hybrid` spins before parking; `Spin` keeps a core busy while waiting.
MPSC `r#async()` selects runtime-neutral task wakeups, without a Tokio dependency
in the library. Calling `sync()` on an already synchronous declaration preserves
its waiter; returning from async selects that topology's synchronous default.
Waiting policy and membership synchronization are separate choices; a
spin/yield waiter does not mean every operation is lock-free.

`Ring` and `Seg` describe local storage. Shared memory is a different axis:
address space, mapping ownership, peer liveness, and payload representation.
It is exposed under `ipc`, not as `backend::SharedMemory`.

## Process channels

IPC is selected like any other channel axis. `open()` performs rendezvous, exchanges and
validates the private shared-memory capabilities, and retains peer-liveness internally.
No socket object or transport configuration enters user code or the transfer loop.

```rust,no_run
use prescient::Channel;

let mut session = Channel::<u8>::new()
    .ipc()
    .shape(4 * 1024 * 1024, 64 * 1024, 8)
    .open()
    .unwrap();
```

The peer process joins with `Channel::<u8>::new().ipc().attach().open()`;
`prescient::ipc::attach()` is the exact namespace shorthand for that declaration.
Both sides use the `default` semantic endpoint unless `.endpoint("orders")` selects
another name. Endpoint names contain 1–20 ASCII letters, digits, dots, dashes, or
underscores. The creator defaults to an empty bounded stream, 64 KiB shared slots,
eight slots, a 30-second progress timeout, unlocked memory, and transfer ID 1. The
attaching side learns the stream shape and transfer IDs during `open`; its local
timeout and memory-lock policy remain independently configurable.

## Namespaces and semantics

- `mpsc::{fixed,pool,dynamic}::channel::<T>()`: one receiver, FIFO within each producer; cross-producer ordering is unspecified.
- `mpmc::brokerless::channel::<T>()`: competing consumers; each message goes to one consumer.
- `mpmc::lanes::channel::<T>()`: the same delivery semantics with permanent SPSC lane ownership.
- `mpmc::brokerless::dynamic::{locked,array}`: producers can join at runtime.
- `mpmc::brokered::channel::<T>()`: round-robin or explicit routing; pub/sub fanout is opt-in.
- `mpmc::broadcast::channel::<T>()`: active readers share a ring and backpressure the publisher instead of losing messages.
- `select!`: synchronous selection across supported receiver types.
- `ipc::{channel,attach}` and `ipc::{pod,codec}::{channel,attach}`: process transport with explicit payload contracts.

Ring storage is bounded and applies backpressure. Seg storage grows by allocating
segments; it is not a bounded-memory replacement for Ring. IPC exchanges setup and
liveness over one connected Unix-domain control socket while payload bytes move
through bounded shared-memory windows. See the crate documentation for ownership,
disconnect, and payload-safety contracts.

## Platform support

Prescient has no Cargo feature flags. Every type-selected local capability is always
available. The `ipc` namespace is built on Linux, macOS, and Windows; target-specific
dependencies provide each operating system implementation.

## Working from this repository

```sh
cargo run --example tour
cargo test --locked
cargo doc --no-deps --open
cargo run --release --locked --example compare -- mpmc-ring 2 2 1000 64 1
```

See [development and validation](docs/DEVELOPMENT.md).

## License

Copyright 2026 Myden.ai.

Licensed under [Apache-2.0](LICENSE).
