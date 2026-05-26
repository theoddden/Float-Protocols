//! Lightweight metrics for monitoring and 99.9% uptime tracking
//!
//! Tracks message throughput, latency, error rates, and cache performance.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct Metrics {
    // Counters
    messages_translated: AtomicU64,
    messages_batched: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    errors: AtomicU64,

    // Latency tracking (in milliseconds)
    total_latency_ms: AtomicU64,
    latency_samples: AtomicU64,

    // Bi-temporal spread routing counter
    spread_sharded_messages: AtomicU64,

    // Protocol-specific counters
    iridium_messages: AtomicU64,
    inmarsat_messages: AtomicU64,
    vsat_messages: AtomicU64,
    hfvhf_messages: AtomicU64,
    rockblock_messages: AtomicU64,
    samsara_messages: AtomicU64,
    nidd_messages: AtomicU64,
    asts_messages: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            messages_translated: AtomicU64::new(0),
            messages_batched: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            spread_sharded_messages: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            latency_samples: AtomicU64::new(0),
            iridium_messages: AtomicU64::new(0),
            inmarsat_messages: AtomicU64::new(0),
            vsat_messages: AtomicU64::new(0),
            hfvhf_messages: AtomicU64::new(0),
            rockblock_messages: AtomicU64::new(0),
            samsara_messages: AtomicU64::new(0),
            nidd_messages: AtomicU64::new(0),
            asts_messages: AtomicU64::new(0),
        }
    }

    pub fn increment_translated(&self) {
        self.messages_translated.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_batched(&self, count: u64) {
        self.messages_batched.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_spread_shard(&self) {
        self.spread_sharded_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency: Duration) {
        let latency_ms = latency.as_millis() as u64;
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.latency_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_protocol(&self, protocol: crate::protocol::Protocol) {
        use crate::protocol::Protocol;
        match protocol {
            Protocol::IridiumSBD => self.iridium_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::InmarsatC => self.inmarsat_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::VSAT => self.vsat_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::HFVHF => self.hfvhf_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::RockBLOCK => self.rockblock_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::Samsara => self.samsara_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::NIDD => self.nidd_messages.fetch_add(1, Ordering::Relaxed),
            Protocol::ASTSpaceMobile => self.asts_messages.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
        let samples = self.latency_samples.load(Ordering::Relaxed);
        let avg_latency_ms = total_latency.checked_div(samples).unwrap_or(0);

        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache_lookups = cache_hits + cache_misses;
        let cache_hit_rate = if total_cache_lookups > 0 {
            (cache_hits as f64 / total_cache_lookups as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            messages_translated: self.messages_translated.load(Ordering::Relaxed),
            messages_batched: self.messages_batched.load(Ordering::Relaxed),
            cache_hit_rate,
            errors: self.errors.load(Ordering::Relaxed),
            spread_sharded_messages: self.spread_sharded_messages.load(Ordering::Relaxed),
            avg_latency_ms,
            iridium_messages: self.iridium_messages.load(Ordering::Relaxed),
            inmarsat_messages: self.inmarsat_messages.load(Ordering::Relaxed),
            vsat_messages: self.vsat_messages.load(Ordering::Relaxed),
            hfvhf_messages: self.hfvhf_messages.load(Ordering::Relaxed),
            rockblock_messages: self.rockblock_messages.load(Ordering::Relaxed),
            samsara_messages: self.samsara_messages.load(Ordering::Relaxed),
            nidd_messages: self.nidd_messages.load(Ordering::Relaxed),
            asts_messages: self.asts_messages.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub messages_translated: u64,
    pub messages_batched: u64,
    pub cache_hit_rate: f64,
    pub errors: u64,
    pub spread_sharded_messages: u64,
    pub avg_latency_ms: u64,
    pub iridium_messages: u64,
    pub inmarsat_messages: u64,
    pub vsat_messages: u64,
    pub hfvhf_messages: u64,
    pub rockblock_messages: u64,
    pub samsara_messages: u64,
    pub nidd_messages: u64,
    pub asts_messages: u64,
}

impl MetricsSnapshot {
    pub fn uptime_percentage(&self, errors: u64, total_requests: u64) -> f64 {
        if total_requests == 0 {
            100.0
        } else {
            ((total_requests - errors) as f64 / total_requests as f64) * 100.0
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
