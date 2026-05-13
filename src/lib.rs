//! Float Protocols - Ultra-lightweight async protocol translation bridge
//! 
//! Inspired by vLLM's batching and LMCache's caching patterns, adapted for
//! protocol translation in constrained dead zone environments.
//! 
//! Design Principles:
//! - Async-first architecture for low latency
//! - 99.9% uptime with circuit breakers, retries, and health checks
//! - Ultra-lightweight: runs on pre-existing RAM on local devices
//! - Zero-allocation where possible using heapless
//! - Fixed-size buffers for memory efficiency
//! - Memory sharding for immediate deadzone uplink (InferX pattern)
//! - Snapshotting for fast uplink building
//! - Inspired by inference serving optimization patterns

pub mod protocol;
pub mod translator;
pub mod batcher;
pub mod cache;
pub mod gateway;
pub mod reliability;
pub mod metrics;
pub mod sharding;
pub mod snapshot;

pub use protocol::{Protocol, Message};
pub use translator::Translator;
pub use batcher::AsyncBatcher;
pub use cache::AsyncCache;
pub use gateway::Gateway;
pub use reliability::{CircuitBreaker, RetryPolicy};
pub use sharding::{ShardManager, ShardId};
pub use snapshot::{SnapshotManager, Snapshot};
