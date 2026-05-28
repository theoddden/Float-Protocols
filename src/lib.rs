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

pub mod asts_protobuf;
pub mod batcher;
pub mod bitemporal;
pub mod cache;
pub mod cadence_translation;
pub mod clock_reconciliation;
pub mod dynamic_buffer;
pub mod gateway;
pub mod hfvhf;
pub mod inmarsat_c;
pub mod iridium_sbd;
pub mod lifetime_safe;
pub mod metrics;
pub mod nidd;
pub mod otel_bundle;
pub mod otel_compact_span;
pub mod otel_converter;
pub mod otel_reconciler;
pub mod protocol;
pub mod reliability;
pub mod samsara;
pub mod sharding;
pub mod snapshot;
pub mod translator;
pub mod transmission_batcher;
pub mod vsat;

pub use asts_protobuf::{ASTSProtobufMessage, ZeroCopyTranslator};
pub use batcher::AsyncBatcher;
pub use bitemporal::{BiTemporalQuery, BiTemporalStore, QueryTime, SpreadStats};
pub use cache::AsyncCache;
pub use cadence_translation::{CadenceRule, CadenceTranslator, MessageCadence, TranslationAction};
pub use clock_reconciliation::{ClockOffset, ClockReconciler, NetworkTimeSource};
pub use dynamic_buffer::{BufferError, DynamicBuffer, DynamicBufferPool};
pub use gateway::Gateway;
pub use hfvhf::{HFVHFMessage, ModulationType};
pub use inmarsat_c::InmarsatCMessage;
pub use iridium_sbd::IridiumSBDMessage;
pub use lifetime_safe::{HybridTranslator, SafeTranslationResult, TranslationArena};
pub use metrics::Metrics;
pub use nidd::NIDDMessage;
pub use otel_bundle::{CompressionType, TelemetryBundle};
pub use otel_compact_span::{CompactSpan, SpanStatus};
pub use otel_converter::{OtlpExportRequest, OtlpSpan, OtlpStatus};
pub use otel_reconciler::{BitemporalReconciler, BitemporalRecord, ReconciliationStats};
pub use protocol::{Message, Protocol};
pub use reliability::{CircuitBreaker, RetryPolicy};
pub use samsara::SamsaraMessage;
pub use sharding::{ShardId, ShardManager};
pub use snapshot::{Snapshot, SnapshotManager};
pub use translator::{translate_iridium_to_asts_sync, BufferPool, Translator};
pub use transmission_batcher::TransmissionBatcher;
pub use vsat::VSATMessage;
