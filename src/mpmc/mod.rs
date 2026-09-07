//! Multi-consumer local channels keep delivery semantics and transfer engines explicit.
//!
//! * [`brokerless`] is a competing-consumer work queue with no middle thread.
//!   Consumers claim one producer's SPSC storage long enough to drain a batch;
//!   each value reaches exactly one consumer.
//! * [`lanes`] preserves competing-consumer delivery while assigning one permanent
//!   SPSC lane to every producer-consumer pair.
//! * [`brokered`] is also competing-consumer, but one or more explicit brokers
//!   drain producer storage and apply a concrete round-robin, route, or pub/sub
//!   policy before forwarding. Use it when centralized placement is the point.
//! * [`broadcast`] owns its own reader-gated ring. Every active reader observes
//!   every value, so it is not a work queue and is not a storage backend.
//!
//! Brokerless declarations have the shortest producer-to-consumer path and need
//! no dedicated broker core. Brokered declarations add a hop in exchange for an
//! exact routing point; spawned and manual broker ownership are separate type
//! states. Both support [`crate::backend::Ring`] and [`crate::backend::Seg`]
//! where their `open()` implementations exist. Broadcast uses its dedicated
//! bounded ring.
//!
//! All choices are made by the declaration before `open()`. Opened hot loops do
//! not switch on topology, engine, backend, routing, or broker ownership.

pub mod broadcast;
pub mod brokered;
pub mod brokerless;
pub mod lanes;
