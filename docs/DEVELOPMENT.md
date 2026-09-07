# Developing Prescient

Use Rust 1.93 or newer. The lockfile is retained for reproducible validation and
benchmarks; guided tours and comparison programs live under `examples/`.

## Local checks

```sh
cargo fmt --all -- --check
cargo fmt --manifest-path tools/perf-probe/Cargo.toml -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --locked --all-targets
cargo build --locked
rustdoc --test README.md --edition=2024 --extern prescient=target/debug/libprescient.rlib -L dependency=target/debug/deps
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.93.0 check --locked --all-targets
cargo package --locked
```

The IPC tests include actual child-process attachment and peer
failure handling. The deliberately large streaming test is ignored by default;
run it explicitly when validating IPC changes:

```sh
cargo test --release --locked --lib -- --ignored
```

## Concurrency models

Use an isolated target directory because Loom substitutes synchronization types:

```sh
CARGO_TARGET_DIR=target/loom RUSTFLAGS="--cfg loom" cargo test --release --locked loom_ --lib
```

For unsafe ownership, allocation, or wakeup changes, also run the relevant Miri
and sanitizer suites. These require compatible nightly toolchains and platform
support; select focused tests rather than silently ignoring unsupported checks.
Benchmark and inspect the concrete affected endpoints after correctness passes.

## Repository and release hygiene

CI defines native test jobs for Linux, macOS, and Windows; formatting, Clippy,
documentation, package, MSRV, Loom, and benchmark-smoke jobs run on Linux.
A configured job is not evidence that an unpushed revision passed on that host.

The Cargo package uses an explicit include list for library sources, tests,
examples, documentation, and license. Build output, measurement output, local
tickets, agent caches, credentials, and editor state are ignored. Benchmark
tools and CI remain versioned in the repository but outside the published crate.

Before publishing, review `cargo package --list`, set the repository URL to the
actual public destination, and validate the release commit. There is no automatic
publishing workflow. Never put secrets in examples, fixtures, or benchmark logs.

Use normal Git commits for source and benchmark changes. Compare revisions with the same benchmark harness and workload.
