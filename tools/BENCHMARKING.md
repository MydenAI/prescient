# Benchmarking Prescient

Run each bounded-channel implementation in a separate process so timing and
hardware counters remain attributable.

```sh
cargo run --release --locked --example compare -- mpmc-ring 4 4 1000000 4096 5
cargo run --release --locked --example compare -- crossbeam 4 4 1000000 4096 5
cargo run --release --locked --example async_bench -- 4 250000 4096 5
```

The synchronous implementations are `mpmc-ring` (fixed brokerless),
`mpmc-locked` and `mpmc-array` (dynamic brokerless, with producers registered
before timing), `mpsc-spin`, `mpsc-park`, `mpsc-pool`, `mpsc-dynamic`, `std`, `crossbeam`,
`kanal`, `flume`, `mutex` and `mutex-sharded` (MPSC/std modes require one consumer).
Dynamic runs drop the registrar before transferring so termination is comparable.

The synchronous comparator accepts `--batch N` (Prescient/mutex receive staging,
default 64) and, on Linux, `--cpus P0,P1,...,C0,C1,...`: one CPU per producer,
then consumer. Repeated CPUs explicitly allow oversubscription. Placement is
validated and applied before the start gate; invalid or unavailable maps fail.
Without a map, workers inherit process affinity. A process-wide `taskset` mask
is not a per-worker assignment.

Each synchronous process verifies count/sum/xor after one full warmup and every
timed sample. Output includes all timed rates, transport slots, configured
staging limits, worker map and total deliveries including warmup. Async warmup
is max(1000, messages/20) per producer. Async tasks may migrate between executor
workers; process affinity is not task affinity.

For A/A or optimization A/B runs, use identical work, compiler/profile and CPU
maps. Alternate process order and retain all samples. Compare paired changes
alongside medians/ranges, instructions per completed message and IPC.
External perf counters cover startup, warmup, verification and output; the
internal transfer timer excludes setup and result verification. Normalize by
all delivered work, not timed work alone.

```sh
cargo build --release --locked --example compare
perf stat -e instructions,cycles -- target/release/examples/compare mpmc-ring 3 3 1000000 1024 5
```

Prescient has per-producer ordering and private staging; peers use shared queues
with different ordering/waiting contracts. Equal transport capacity does not
imply equal total memory. Neither fewer instructions nor higher IPC alone proves
a throughput win; evaluate elapsed time and variability too.

Current layout, code-generation, selection and process-transport probes:

```sh
cargo run --release --locked --manifest-path tools/perf-probe/Cargo.toml -- layout
cargo run --release --locked --manifest-path tools/perf-probe/Cargo.toml -- codegen
cargo run --release --locked --manifest-path tools/perf-probe/Cargo.toml -- select 1000 1
cargo run --release --locked --manifest-path tools/perf-probe/Cargo.toml -- ipc_duplex 65536 1
```

Small runs check data integrity, not performance. Broader examples include
`bench`, `mpmc_bench`, `ab_drain`, `ab_park`, `ab_inbox` and `idle_cpu`.

## Mutex baselines

`mutex` uses one shared bounded `std::sync::Mutex<VecDeque<u64>>`;
`mutex-sharded` uses one such queue per producer. Both accept `--batch`
(default 64), use linear private receive staging, and preserve individual-message
APIs. Full/empty retries match Prescient's spin-then-yield schedule outside
locks; mutex acquisition itself may block/park.
Transport allocation precedes timing; staging grows lazily and is reused.
The sharded variant matches Prescient's transport topology and per-shard FIFO.

```sh
cargo build --release --locked --example compare --example mutex_bench
target/release/examples/compare mpmc-ring 3 3 1000000 4096 5 --batch 64 --cpus 0,1,2,3,4,5
target/release/examples/compare mutex-sharded 3 3 1000000 4096 5 --batch 64 --cpus 0,1,2,3,4,5
target/release/examples/compare mutex 3 3 1000000 4096 5 --batch 64 --cpus 0,1,2,3,4,5
target/release/examples/mutex_bench 6 1000000 5 --cpus 0,1,2,3,4,5
```

Use CPUs actually available on your host. `mutex_bench` is a separate raw
lock/increment/unlock workload, not message transfer: its rate is million
operations/s, and its `completed` field includes the full warmup for counter
normalization. All threads contend on the same u64; even the one-worker case
uses a spawned thread. Compare queue rates only against other queue rates.
This matches workload shape, not scheduling/fairness or idle-power semantics.

## Focused broker routing

The standalone probe also isolates sparse and dense pub/sub with real bounded
Ring transports (capacity 1024 per transport, one producer and one broker):

```sh
CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  cargo build --release --locked --manifest-path tools/perf-probe/Cargo.toml
tools/perf-probe/target/release/prescient-perf-probe pubsub_sparse 200000 7 64
tools/perf-probe/target/release/prescient-perf-probe pubsub_dense 200000 7 2
```

Arguments are messages, timed samples, and configured consumers (1–64). Each
process also runs one warmup. Sparse routing selects only the highest consumer
index; all other endpoints remain alive but receive nothing. Dense routing
selects every consumer. Each active consumer runs on its own thread. The
reported M/s counts input messages, not fan-out deliveries; do not compare these
two workloads without accounting for their different delivered work.

Opening/allocation and output verification are outside the internal timer;
thread startup, routing, sends, receives, and joins are inside it. Every active
consumer's count and checksum are checked, as is the absence of messages on
inactive consumers. External `perf stat` counts the whole process.

For an optimization A/B, build the same probe at both Git revisions with
separate `CARGO_TARGET_DIR` paths and identical compiler/profile settings, then
alternate the two binaries on the same CPU set. Keep the raw counter samples.

## Hot-path specialization contract

Backend, waiter, execution, membership, and routing policies use concrete types.
The compiler specializes the operation bodies; `open()` validates the numeric
shape, allocates resources, and binds immutable derived state. It is not a JIT.

Pub/sub binds its valid-consumer mask at open, visits only selected mask bits,
and moves the original value to the last selected consumer. Non-subscribers add
no scan iterations. Empty selections still drop the value; N selected consumers
still require N-1 clones. The pub/sub policy stores a u64 mask; layout padding
also depends on the callback type.

Ring construction establishes a nonzero power-of-two capacity and an immutable
mask. Slot access uses that invariant to avoid a redundant bounds branch in
synchronous and asynchronous push/pop/drain operations. Counter wrap, zero-sized
values, and capacity overflow have dedicated tests.

MPSC receivers use their statically selected drain kernel to publish released
capacity once per batch. This covers fixed, pool, and dynamic membership, including
async receivers, while retaining the drain guard's unwind and wake-up behavior.
MPMC also uses batch drains; the Ring slot-access optimization applies there too.

Brokerless MPMC keeps staged receive inlineable and puts ring claiming/refill
in a separate function. Fixed/dynamic brokerless scanning and brokered scanning/
round-robin delivery use bounded cursor advancement instead of runtime division.
Claim atomics, notification ordering, and message-dependent routing are still
required for correctness.

Keep callback types generic through the endpoint. Coercing a function item or
closure to a `fn` pointer erases useful callee identity and may leave an indirect
call. Inlining attributes, including `inline(always)`, are hints, not guarantees.
See the [Rust function-item reference](https://doc.rust-lang.org/reference/types/function-item.html)
and [code-generation attributes](https://doc.rust-lang.org/reference/attributes/codegen.html#the-inline-attribute).

“Switchless” means no per-operation selection of configuration policies. It does
not mean removing empty/full/disconnected checks or changing message-dependent
routing. A safety check may be eliminated only when an established invariant
proves it redundant. Runtime capacities and batch limits remain numeric operands.
A runtime choice between different kernels should dispatch outside the processing
loop if it must avoid per-operation indirect calls.

Inspect generated code before claiming removed work: the compiler may already
hoist or eliminate a source-level expression. Report instructions per completed
work, IPC, and throughput together; none alone is a sufficient performance gate.

## Equal-capacity competitor comparison

`examples/compare.rs` runs exactly one implementation per process. Its arguments
are implementation, producers, consumers, messages per producer, per-producer
capacity, and timed samples:

```sh
cargo build --release --locked --example compare
target/release/examples/compare mpsc-park 4 1 250000 1024 5
target/release/examples/compare mpsc-spin 4 1 250000 1024 5
target/release/examples/compare crossbeam 4 1 250000 1024 5
target/release/examples/compare mpmc-ring 2 2 500000 1024 5
```

Implementations: `mpsc-spin`, `mpsc-park`, `mpsc-pool`, `mpsc-dynamic`,
`mpmc-ring` (brokerless), `crossbeam`, `flume`, `kanal`, and `std`.
MPSC and std require one consumer. Capacity must be a power of two: Prescient
gets that many slots per producer; competitors get producers × capacity slots
in their shared queue. Prescient's internal receiver staging is additional
storage, and its per-shard FIFO is not a global FIFO. Equal transport capacity
does not imply identical semantics or memory footprint.

All implementations send individual u64 messages and receive individual values.
Workers start at an abortable gate; opening, endpoint creation, affinity, and
thread creation are outside timing. Gate release, transfer, disconnection, and joins
are inside. Count, sum, and XOR are checked after timing. There is one warmup
plus the requested timed samples. Performance counters taken around the process
also include setup, warmup, verification, and teardown.

Prescient pool/dynamic use Spin, and mpmc-ring uses its SpinYield kernel. The
park case and competitors use their ordinary blocking APIs; different waiting
strategies affect both CPU consumption and idle behavior. This is a sustained
throughput comparison, not an idle-power or latency ranking.

Use locked dependencies and identical release flags, separate output directories
for before/after builds, alternating run order, and several independent processes.
Record compiler/CPU/affinity, exact crate versions, variability, instructions per
delivered message, IPC, and elapsed time. Do not run builds alongside measurements.

## Isolated async comparisons

The async example accepts an optional final implementation name. Omit it to run
the rotating six-implementation suite; select one to attribute process counters:

```sh
cargo build --release --locked --example async_bench
perf stat -e instructions,cycles,task-clock,context-switches,cpu-migrations \
  -- target/release/examples/async_bench 4 250000 64 5 prescient-fixed
```

Names are `prescient-fixed`, `prescient-batch`, `tokio-mpsc`, `async-channel`,
`flume`, and `kanal`. Arguments before the name are producers, messages per
producer, power-of-two per-shard capacity, and timed samples (at least three).
Shared-queue competitors get producers × per-shard capacity transport slots.
Prescient's staging and per-shard ordering still differ from shared queues.

Every variant uses the same Tokio executor and u64 payload. Setup/spawn precedes
the transfer timer; release, sends, receives, checksum accumulation, and joins
are timed. The checksum is compared afterward. There is one smaller warmup:
`max(messages_per_producer / 20, 1000)` messages per producer. Whole-process
counters include the runtime, setup, warmup, verification, and teardown. For the
command above, divide instructions by 5,050,000 delivered messages, not 5,000,000.
Never attribute counters around the six-variant suite to a single implementation.

`prescient-batch` uses explicit send/receive batching, so it is not a like-for-like
per-message API comparison with the other rows. Use repeated, alternating
before/after processes and report variability; test capacity one as well as larger
buffers to distinguish waiting costs from immediately-ready work.

The Task waiter retains `atomic-waker` for concurrent registration/notification.
Registrar-owned bookkeeping skips cancellation cleanup when no registration needs
clearing. The cleanup bit is separate from the wake-armed bit: a wake may consume
the latter while registration still needs cleanup. Tests cover cancellation,
re-registration, direct polling, and unpolled futures. Recovery after a custom
waker's clone callback panics is not supported by the underlying AtomicWaker.

### Async publication handshake

Every data, capacity, and lifecycle notification uses the same protocol:

- Publisher: publish state with Release ordering, execute a SeqCst fence, read
  the armed flag, and only claim/wake the task when armed.
- Waiter: register its waker, publish the arm, execute a SeqCst fence, and recheck
  channel state with Acquire ordering before returning Pending.

The fences forbid both sides from missing the other's publication. An unarmed
notification does not write a shared wake flag, avoiding producer contention on
that cache line. It still needs a fence: removing it or relying on an empty/full
snapshot admits a lost wake. Async send reuses the ordinary cached ring cursor
path; batch drains publish released capacity and notify once per batch.

The sync kernel has no TaskWaiter or task-notification fence. Configuration remains statically
specialized; empty/full, disconnection, and armed-state branches are runtime
correctness checks, not policy dispatch.

Loom runs the same fences as production. This matters because
[Loom 0.7 does not fully model SeqCst loads/stores, but supports SeqCst fences](https://github.com/tokio-rs/loom/blob/v0.7.2/README.md#unsupported-features).
The bounded models cover re-arming on a warm shard, re-arming for capacity, and
two publishers sharing a waiter. They retain producer handles or stop draining
so shutdown/extra capacity cannot rescue a missed notification. Model checks
are evidence within their bounds, not an exhaustive proof for every execution.

Producer teardown is ordered: clear the old producer registration, release its
ring sender, then notify the receiver. Clearing before release prevents a new
producer's registration from being erased after recycling. Releasing before
notification lets a promptly scheduled receiver recycle the shard. A drop-only
lease enforces that order with safe Rust and no per-message check. Tests cover
an immediate wake and cleanup of an abandoned send future.
