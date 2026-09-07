#![deny(missing_docs)]
//! A typed [`Channel`] declaration selects
//! topology, engine, local storage, execution, wait, routing, transport, and payload
//! representation before [`Channel::open`] constructs the exact endpoint types.
//! Those choices are type parameters: opened hot paths contain no configuration
//! tag, topology switch, or erased routing callback.
//!
//! Namespaced `channel()` functions are preconfigured invocations of the same
//! declaration, not alternate APIs:
//!
//! ```
//! use prescient::{Channel, mpsc};
//!
//! let direct = Channel::<u64>::new().producers(4).capacity(256);
//! let short = mpsc::fixed::channel::<u64>().producers(4).capacity(256);
//! let (direct_tx, direct_rx) = direct.open().unwrap();
//! let (short_tx, short_rx) = short.open().unwrap();
//! # let _ = (direct_tx, direct_rx, short_tx, short_rx);
//! ```
//!
//! Local storage is selected with [`backend::Ring`] or [`backend::Seg`]. Process
//! shared memory is an address-space and lifecycle choice exposed under [`ipc`],
//! never a local backend. Synchronous MPSC declarations default to [`wait::Park`]
//! and may explicitly select [`wait::StdThread`], [`wait::Hybrid`], or
//! [`wait::Spin`]. Calling `r#async()` selects the task-waker kernel.
//! Calling `sync()` on a synchronous declaration preserves its waiter. Converting
//! from async selects the topology's synchronous default: Park for MPSC/process
//! transport, SpinYield for MPMC/broadcast. Other axes and numeric shape are retained.
//!
//! The supported combinations are intentionally narrower than the Cartesian
//! product of all markers:
//!
//! | topology | supported specialization |
//! |----------|--------------------------|
//! | fixed, pool, dynamic MPSC | Ring; sync with a concrete waiter, or async with task wakeups |
//! | fixed MPMC | Claim or Lanes engine; Ring or Seg; synchronous spin/yield |
//! | dynamic brokerless MPMC | locked or array membership; Ring or Seg; synchronous spin/yield |
//! | brokered MPMC | Ring or Seg; synchronous concrete round-robin, route, or pub/sub policy |
//! | broadcast | Ring; synchronous spin/yield |
//! | process duplex | shared memory on Linux, macOS, and Windows; synchronous bytes, POD, or an explicit codec |
//!
//! A state outside that matrix has no `open()` method. Numeric shape is checked
//! once by `open()` before endpoint construction. Invalid axes are rejected by
//! the type system; for example, shared memory is not a local backend:
//!
//! ```compile_fail
//! use prescient::{Channel, backend};
//! let _ = Channel::<u64>::new()
//!     .backend::<backend::SharedMemory>()
//!     .open();
//! ```
//!
//! Fixed MPMC has no asynchronous kernel:
//!
//! ```compile_fail
//! use prescient::Channel;
//! let _ = Channel::<u64>::new()
//!     .mpmc()
//!     .r#async()
//!     .open();
//! ```
//!
//! Leased payload blocks use the claim engine, so a lane declaration cannot
//! select them:
//!
//! ```compile_fail
//! use prescient::{Channel, engine};
//! let _ = Channel::<u64>::new()
//!     .mpmc()
//!     .engine::<engine::Lanes>()
//!     .leased();
//! ```
//!
//! Routing is available only on brokered declarations:
//!
//! ```compile_fail
//! use prescient::Channel;
//! let _ = Channel::<u64>::new().route(|_: &u64, _| 0).open();
//! ```
//!
//! Process transport does not infer a representation for arbitrary native `T`:
//!
//! ```compile_fail
//! use prescient::Channel;
//! let _ = Channel::<String>::new().ipc().open();
//! ```
//!
//! POD and codec shorthands bind an explicit payload contract into the setup
//! handshake. The attaching process must select the same contract before either
//! shared region is mapped:
//!
//! ```no_run
//! # #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
//! # fn creator() -> Result<(), prescient::OpenError> {
//! use prescient::ipc;
//! let _parent = ipc::pod::channel::<u64>()
//!     .shape(8 * 1024, 64 * 1024, 8)
//!     .open()?;
//! # Ok(())
//! # }
//! # #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
//! # fn worker() -> Result<(), prescient::OpenError> {
//! # use prescient::ipc;
//! let _worker = ipc::pod::attach::<u64>().open()?;
//! # Ok(())
//! # }
//! ```

mod declaration;
mod open;
mod platform;
mod round_robin;
mod task_waker;

pub mod backend;
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
pub mod ipc;
pub mod mpmc;
pub mod mpsc;
mod park;

pub mod select;

pub use declaration::{
    Channel, broker, codec, engine, execution, membership, reclaim, routing, topology, transport,
    wait,
};
pub use open::OpenError;
