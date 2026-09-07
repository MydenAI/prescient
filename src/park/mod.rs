//! Consumer wait kernels used by typed MPSC channel declarations.
//!
//! A declaration selects exactly one policy with `.wait::<W>()`; the opened
//! endpoint contains that waiter and no runtime policy tag or dispatch branch.
//!
//! | marker | behavior | idle CPU |
//! |--------|----------|----------|
//! | [`crate::wait::Park`] | parking_lot eventcount (default) | ~0 |
//! | [`crate::wait::StdThread`] | std thread park/unpark | ~0 |
//! | [`crate::wait::Hybrid`] | adaptive spin then park | scales with load |
//! | [`crate::wait::Spin`] | dedicated-core spin | 100% of one core |
pub mod condvar;
pub mod hybrid;
pub mod spin;
pub mod std_thread;
