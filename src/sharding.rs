//! Memory sharding for immediate uplink when deadzone is hit (InferX pattern)
//!
//! Pre-shards memory into dedicated uplink buffers that are immediately available
//! when a deadzone is detected, eliminating allocation latency during critical
//! transitions from connected to disconnected states.

use crate::protocol::{Message, Protocol};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub u64);

pub struct MemoryShard {
    _id: ShardId,
    buffer: Vec<Message>,
    max_size: usize,
    last_access: Instant,
    is_deadzone_shard: bool, // Dedicated shard for deadzone uplink
}

impl MemoryShard {
    pub fn new(id: ShardId, max_size: usize, is_deadzone_shard: bool) -> Self {
        let mut shard = Self {
            _id: id,
            buffer: Vec::with_capacity(max_size),
            max_size,
            last_access: Instant::now(),
            is_deadzone_shard,
        };

        // Pre-allocate buffer immediately for all shards
        // This ensures immediate uplink capability when deadzone is detected
        shard.buffer.reserve(max_size);

        shard
    }

    pub fn push(&mut self, message: Message) -> Result<(), ShardError> {
        if self.buffer.len() >= self.max_size {
            return Err(ShardError::Full);
        }
        self.buffer.push(message);
        self.last_access = Instant::now();
        Ok(())
    }

    pub fn drain(&mut self) -> Vec<Message> {
        self.last_access = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn last_access(&self) -> Instant {
        self.last_access
    }

    pub fn is_deadzone_shard(&self) -> bool {
        self.is_deadzone_shard
    }
}

pub struct ShardManager {
    shards: DashMap<ShardId, MemoryShard>,
    num_shards: usize,
    shard_size: usize,
    _next_shard_id: u64,
    deadzone_shard_id: ShardId,
    // Dedicated shard for high bi-temporal spread (reconnect burst recovery).
    // Pre-allocated, no backpressure gate, no timeout — messages are already
    // stale so we route and flush them out as fast as possible.
    spread_shard_id: ShardId,
    // Leak detection counters
    messages_allocated: Arc<AtomicU64>,
    messages_dropped: Arc<AtomicU64>,
    messages_leaked: Arc<AtomicU64>,
}

impl ShardManager {
    pub fn new(num_shards: usize, shard_size: usize) -> Self {
        let shards = DashMap::new();

        // ShardId(0): dedicated deadzone shard (emergency, highest priority)
        let deadzone_shard_id = ShardId(0);
        shards.insert(
            deadzone_shard_id,
            MemoryShard::new(deadzone_shard_id, shard_size, true),
        );

        // ShardId(1..num_shards): regular load-balanced shards
        for i in 1..num_shards {
            shards.insert(
                ShardId(i as u64),
                MemoryShard::new(ShardId(i as u64), shard_size, false),
            );
        }

        // ShardId(num_shards): spread shard for reconnect-burst recovery.
        // Pre-allocated at startup so there is zero allocation latency when
        // a wide bi-temporal spread triggers routing here.
        let spread_shard_id = ShardId(num_shards as u64);
        shards.insert(
            spread_shard_id,
            MemoryShard::new(spread_shard_id, shard_size, false),
        );

        Self {
            shards,
            num_shards,
            shard_size,
            _next_shard_id: num_shards as u64 + 1,
            deadzone_shard_id,
            spread_shard_id,
            messages_allocated: Arc::new(AtomicU64::new(0)),
            messages_dropped: Arc::new(AtomicU64::new(0)),
            messages_leaked: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get or create a shard for a specific protocol
    pub fn get_shard(&self, protocol: Protocol) -> ShardId {
        let shard_id = self.protocol_to_shard_id(protocol);

        if !self.shards.contains_key(&shard_id) {
            self.create_shard(shard_id);
        }

        shard_id
    }

    /// Push message to appropriate shard with load balancing and backpressure
    pub async fn push(&self, message: Message) -> Result<ShardId, ShardError> {
        // Check utilization before attempting push (backpressure)
        let stats = self.stats();
        if stats.utilization() > 0.8 {
            self.messages_dropped.fetch_add(1, Ordering::AcqRel);
            return Err(ShardError::Backpressure);
        }

        let shard_id = self.select_shard_for_message(&message);

        // Increment allocated counter
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        // Try to push with timeout
        match tokio::time::timeout(Duration::from_millis(100), async {
            if let Some(mut shard) = self.shards.get_mut(&shard_id) {
                shard.push(message)
            } else {
                Err(ShardError::NotFound)
            }
        })
        .await
        {
            Ok(Ok(())) => Ok(shard_id),
            Ok(Err(e)) => {
                self.messages_dropped.fetch_add(1, Ordering::AcqRel);
                Err(e)
            }
            Err(_) => {
                self.messages_dropped.fetch_add(1, Ordering::AcqRel);
                Err(ShardError::Timeout)
            }
        }
    }

    /// Push message to spread shard for reconnect-burst recovery.
    ///
    /// Called when a message's bi-temporal spread (t_system - t_event) exceeds
    /// the high-spread threshold, indicating the message sat in a dead zone for
    /// a long time and has just arrived in a reconnect burst. These messages:
    ///   - Bypass the 80% backpressure gate (already stale, dropping makes it worse)
    ///   - Bypass the 100ms push timeout (pre-allocated buffer, should be immediate)
    ///   - Bypass cadence rate limiting in the gateway (enforced by caller)
    ///
    /// The goal is to drain the reconnect burst as fast as possible.
    pub async fn push_spread(&self, message: Message) -> Result<ShardId, ShardError> {
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        if let Some(mut shard) = self.shards.get_mut(&self.spread_shard_id) {
            shard.push(message)?;
            Ok(self.spread_shard_id)
        } else {
            self.messages_dropped.fetch_add(1, Ordering::AcqRel);
            Err(ShardError::NotFound)
        }
    }

    /// Get the spread shard ID for burst-recovery draining
    pub fn get_spread_shard(&self) -> ShardId {
        self.spread_shard_id
    }

    /// Push message to deadzone shard for immediate uplink when deadzone detected
    /// Buffer is already pre-allocated during initialization for zero-latency access
    pub async fn push_deadzone(&self, message: Message) -> Result<ShardId, ShardError> {
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        if let Some(mut shard) = self.shards.get_mut(&self.deadzone_shard_id) {
            shard.push(message)?;
            Ok(self.deadzone_shard_id)
        } else {
            self.messages_dropped.fetch_add(1, Ordering::AcqRel);
            Err(ShardError::NotFound)
        }
    }

    /// Get deadzone shard for immediate uplink access
    pub fn get_deadzone_shard(&self) -> ShardId {
        self.deadzone_shard_id
    }

    /// Drain all messages from a shard
    pub fn drain_shard(&self, shard_id: ShardId) -> Vec<Message> {
        if let Some(mut shard) = self.shards.get_mut(&shard_id) {
            shard.drain()
        } else {
            Vec::new()
        }
    }

    /// Get statistics across all shards
    pub fn stats(&self) -> ShardStats {
        let total_messages: usize = self.shards.iter().map(|s| s.len()).sum();
        let active_shards = self.shards.iter().filter(|s| !s.is_empty()).count();

        ShardStats {
            total_shards: self.shards.len(),
            active_shards,
            total_messages,
            shard_size: self.shard_size,
        }
    }

    /// Evict idle shards to free memory
    pub fn evict_idle(&self, idle_threshold: Duration) {
        self.shards
            .retain(|_, shard| shard.last_access().elapsed() < idle_threshold);
    }

    fn protocol_to_shard_id(&self, protocol: Protocol) -> ShardId {
        // Consistent hashing based on protocol
        let hash = match protocol {
            Protocol::IridiumSBD => 1,
            Protocol::InmarsatC => 2,
            Protocol::VSAT => 3,
            Protocol::HFVHF => 4,
            Protocol::RockBLOCK => 5,
            Protocol::Samsara => 6,
            Protocol::NIDD => 7,
            Protocol::ASTSpaceMobile => 8,
        };
        // Ensure we never collide with the reserved deadzone shard (ShardId(0))
        ShardId(1 + (hash % self.num_shards as u64))
    }

    fn select_shard_for_message(&self, _message: &Message) -> ShardId {
        // Load balancing: select from regular shards only.
        // ShardId(0) is reserved for emergency deadzone uplinks.
        // ShardId(num_shards) is reserved for spread burst-recovery.
        // Neither should receive general traffic.
        self.shards
            .iter()
            .filter(|entry| *entry.key() != ShardId(0) && *entry.key() != self.spread_shard_id)
            .min_by_key(|entry| entry.value().len())
            .map(|entry| *entry.key())
            .unwrap_or_else(|| ShardId(1))
    }

    fn create_shard(&self, shard_id: ShardId) {
        self.shards
            .entry(shard_id)
            .or_insert_with(|| MemoryShard::new(shard_id, self.shard_size, false));
    }

    /// Get leak detection statistics
    pub fn leak_stats(&self) -> LeakStats {
        LeakStats {
            allocated: self.messages_allocated.load(Ordering::Acquire),
            dropped: self.messages_dropped.load(Ordering::Acquire),
            leaked: self.messages_leaked.load(Ordering::Acquire),
        }
    }

    /// Reset leak detection counters
    pub fn reset_leak_stats(&self) {
        self.messages_allocated.store(0, Ordering::Release);
        self.messages_dropped.store(0, Ordering::Release);
        self.messages_leaked.store(0, Ordering::Release);
    }
}

#[derive(Debug)]
pub enum ShardError {
    Full,
    NotFound,
    Backpressure,
    Timeout,
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Full => write!(f, "Shard is full"),
            ShardError::NotFound => write!(f, "Shard not found"),
            ShardError::Backpressure => write!(f, "Backpressure: buffer utilization >80%"),
            ShardError::Timeout => write!(f, "Operation timeout"),
        }
    }
}

impl std::error::Error for ShardError {}

#[derive(Debug, Clone)]
pub struct LeakStats {
    pub allocated: u64,
    pub dropped: u64,
    pub leaked: u64,
}

impl LeakStats {
    pub fn leak_rate(&self) -> f64 {
        if self.allocated == 0 {
            0.0
        } else {
            (self.leaked as f64) / (self.allocated as f64)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardStats {
    pub total_shards: usize,
    pub active_shards: usize,
    pub total_messages: usize,
    pub shard_size: usize,
}

impl ShardStats {
    pub fn utilization(&self) -> f64 {
        if self.shard_size == 0 {
            0.0
        } else {
            (self.total_messages as f64) / ((self.total_shards * self.shard_size) as f64)
        }
    }
}
