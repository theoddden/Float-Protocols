//! Memory sharding for immediate uplink when deadzone is hit (InferX pattern)
//!
//! Pre-shards memory into dedicated uplink buffers that are immediately available
//! when a deadzone is detected, eliminating allocation latency during critical
//! transitions from connected to disconnected states.
//!
//! Per-shard worker architecture: each shard has a lock-free crossbeam channel.
//! Workers own the Receiver and drain messages, ShardManager holds Senders for push.

use crate::protocol::{Message, Protocol};
use crossbeam_channel::{self, Receiver, Sender};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub u64);

pub struct MemoryShard {
    _id: ShardId,
    tx: Sender<Message>,
    #[allow(dead_code)]
    max_size: usize,
    last_access: Instant,
    is_deadzone_shard: bool, // Dedicated shard for deadzone uplink
}

impl MemoryShard {
    pub fn new(id: ShardId, max_size: usize, is_deadzone_shard: bool) -> (Self, Receiver<Message>) {
        let (tx, rx) = crossbeam_channel::bounded(max_size);
        let shard = Self {
            _id: id,
            tx,
            max_size,
            last_access: Instant::now(),
            is_deadzone_shard,
        };
        (shard, rx)
    }

    pub fn push(&self, message: Message) -> Result<(), ShardError> {
        self.tx.try_send(message).map_err(|_| ShardError::Full)
    }

    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx.is_empty()
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
    receivers: DashMap<ShardId, Receiver<Message>>,
    num_shards: usize,
    shard_size: usize,
    _next_shard_id: u64,
    deadzone_shard_id: ShardId,
    // Dedicated shard for high bi-temporal spread (reconnect burst recovery).
    // Pre-allocated, no backpressure gate, no timeout — messages are already
    // stale so we route and flush them out as fast as possible.
    spread_shard_id: ShardId,
    // O(1) backpressure: atomic count of messages in regular shards only.
    // Incremented in push(), decremented in drain_shard() for regular shards.
    // Avoids the O(n) stats() full-scan on every push().
    total_regular_messages: Arc<AtomicU64>,
    // Precomputed 80% capacity limit for regular shards.
    backpressure_limit: u64,
    // Round-robin index for O(1) shard selection.
    round_robin: Arc<AtomicU64>,
    // Leak detection counters
    messages_allocated: Arc<AtomicU64>,
    messages_dropped: Arc<AtomicU64>,
    messages_leaked: Arc<AtomicU64>,
}

impl ShardManager {
    pub fn new(num_shards: usize, shard_size: usize) -> Self {
        let shards = DashMap::new();
        let receivers = DashMap::new();

        // ShardId(0): dedicated deadzone shard (emergency, highest priority)
        let deadzone_shard_id = ShardId(0);
        let (deadzone_shard, deadzone_rx) = MemoryShard::new(deadzone_shard_id, shard_size, true);
        shards.insert(deadzone_shard_id, deadzone_shard);
        receivers.insert(deadzone_shard_id, deadzone_rx);

        // ShardId(1..num_shards): regular load-balanced shards
        for i in 1..num_shards {
            let shard_id = ShardId(i as u64);
            let (shard, rx) = MemoryShard::new(shard_id, shard_size, false);
            shards.insert(shard_id, shard);
            receivers.insert(shard_id, rx);
        }

        // ShardId(num_shards): spread shard for reconnect-burst recovery.
        // Pre-allocated at startup so there is zero allocation latency when
        // a wide bi-temporal spread triggers routing here.
        let spread_shard_id = ShardId(num_shards as u64);
        let (spread_shard, spread_rx) = MemoryShard::new(spread_shard_id, shard_size, false);
        shards.insert(spread_shard_id, spread_shard);
        receivers.insert(spread_shard_id, spread_rx);

        // Regular shards are ShardId(1)..ShardId(num_shards-1) — (num_shards-1) total.
        // Backpressure fires at 80% of their combined capacity.
        let regular_shard_count = (num_shards - 1) as u64;
        let backpressure_limit = regular_shard_count * shard_size as u64 * 8 / 10;

        Self {
            shards,
            receivers,
            num_shards,
            shard_size,
            _next_shard_id: num_shards as u64 + 1,
            deadzone_shard_id,
            spread_shard_id,
            total_regular_messages: Arc::new(AtomicU64::new(0)),
            backpressure_limit,
            round_robin: Arc::new(AtomicU64::new(0)),
            messages_allocated: Arc::new(AtomicU64::new(0)),
            messages_dropped: Arc::new(AtomicU64::new(0)),
            messages_leaked: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get or create a shard for a specific protocol
    /// Note: Dynamically created shards do not have dedicated workers.
    /// Use only for protocols that are handled by the spread shard or deadzone shard.
    pub fn get_shard(&self, protocol: Protocol) -> ShardId {
        let shard_id = self.protocol_to_shard_id(protocol);

        if !self.shards.contains_key(&shard_id) {
            // Log warning: dynamic shards lack workers
            tracing::warn!(
                protocol = %protocol,
                shard_id = shard_id.0,
                "Creating dynamic shard without dedicated worker - messages may not be processed"
            );
            self.create_shard(shard_id);
        }

        shard_id
    }

    /// Push message to appropriate shard with load balancing and backpressure.
    ///
    /// O(1): atomic utilization check + round-robin shard selection.
    /// No async, no timeout registration — DashMap shard lock is held for nanoseconds.
    pub fn push(&self, message: Message) -> Result<ShardId, ShardError> {
        // O(1) backpressure via atomic counter — replaces the O(n) stats() scan
        if self.total_regular_messages.load(Ordering::Relaxed) >= self.backpressure_limit {
            self.messages_dropped.fetch_add(1, Ordering::AcqRel);
            return Err(ShardError::Backpressure);
        }

        let shard_id = self.select_shard_for_message();
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        if let Some(shard) = self.shards.get(&shard_id) {
            shard.push(message)?;
            self.total_regular_messages.fetch_add(1, Ordering::AcqRel);
            Ok(shard_id)
        } else {
            self.messages_dropped.fetch_add(1, Ordering::AcqRel);
            Err(ShardError::NotFound)
        }
    }

    /// Push message to spread shard for reconnect-burst recovery.
    ///
    /// Called when a message's bi-temporal spread (t_system - t_event) exceeds
    /// the adaptive threshold, indicating the message sat in a dead zone for
    /// a long time and has just arrived in a reconnect burst. These messages:
    ///   - Bypass the 80% backpressure gate (already stale, dropping makes it worse)
    ///   - Bypass cadence rate limiting in the gateway (enforced by caller)
    ///
    /// The goal is to drain the reconnect burst as fast as possible.
    pub fn push_spread(&self, message: Message) -> Result<ShardId, ShardError> {
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        if let Some(shard) = self.shards.get(&self.spread_shard_id) {
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

    /// Push message to deadzone shard for immediate uplink when deadzone detected.
    /// Buffer is pre-allocated at startup for zero allocation latency.
    pub fn push_deadzone(&self, message: Message) -> Result<ShardId, ShardError> {
        self.messages_allocated.fetch_add(1, Ordering::AcqRel);

        if let Some(shard) = self.shards.get(&self.deadzone_shard_id) {
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

    /// Get the receiver for a shard (for workers to own).
    /// Returns None if shard doesn't exist.
    pub fn get_receiver(&self, shard_id: ShardId) -> Option<Receiver<Message>> {
        self.receivers.remove(&shard_id).map(|(_, rx)| rx)
    }

    /// Drain all messages from a shard (legacy API for spread shard drain task).
    /// For regular shards, workers drain via receiver. This is only used for
    /// the spread shard which doesn't have a dedicated worker.
    pub fn drain_shard(&self, shard_id: ShardId) -> Vec<Message> {
        let mut messages = Vec::new();
        if let Some(rx) = self.receivers.get(&shard_id) {
            while let Ok(msg) = rx.try_recv() {
                messages.push(msg);
            }
        }
        if self.is_regular_shard(shard_id) && !messages.is_empty() {
            let count = messages.len() as u64;
            let _ = self.total_regular_messages.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |v| Some(v.saturating_sub(count)),
            );
        }
        // Track leaked messages: if shard is not being drained by a worker,
        // messages may have been sitting in the channel without being processed
        if !self.is_regular_shard(shard_id) && !messages.is_empty() {
            self.messages_leaked
                .fetch_add(messages.len() as u64, Ordering::AcqRel);
        }
        messages
    }

    fn is_regular_shard(&self, shard_id: ShardId) -> bool {
        shard_id != self.deadzone_shard_id && shard_id != self.spread_shard_id
    }
}

/// Worker that drains a single shard and processes messages.
///
/// Workers own the Receiver for their shard and run in an async task,
/// continuously draining messages and invoking the callback for each.
pub struct ShardWorker {
    shard_id: ShardId,
    receiver: Receiver<Message>,
}

impl ShardWorker {
    /// Create a new worker for the given shard.
    pub fn new(shard_id: ShardId, receiver: Receiver<Message>) -> Self {
        Self { shard_id, receiver }
    }

    /// Run the worker, processing messages via the callback.
    /// The callback receives the shard_id and the message.
    pub fn run<F, Fut>(self, mut callback: F)
    where
        F: FnMut(ShardId, Message) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let shard_id = self.shard_id;
        let receiver = self.receiver;
        tokio::spawn(async move {
            while let Ok(message) = receiver.recv() {
                callback(shard_id, message).await;
            }
        });
    }

    /// Batched variant: parks on `spawn_blocking` waiting for the first message,
    /// then greedily drains up to `batch_size` more via non-blocking `try_recv`.
    /// Calling `spawn_blocking` for the wait keeps the tokio thread free between
    /// batches, avoiding starvation of other tasks.
    pub fn run_batched<F, Fut>(self, batch_size: usize, mut callback: F)
    where
        F: FnMut(ShardId, Vec<Message>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let shard_id = self.shard_id;
        let receiver = self.receiver;
        tokio::spawn(async move {
            loop {
                // Park a blocking thread until the first message arrives.
                // crossbeam Receiver is Clone + Send so we can move a clone in.
                let rx = receiver.clone();
                let first = match tokio::task::spawn_blocking(move || rx.recv().ok()).await {
                    Ok(Some(msg)) => msg,
                    _ => break, // channel disconnected
                };
                let mut batch = Vec::with_capacity(batch_size);
                batch.push(first);
                // Non-blocking greedy drain for the rest of the batch
                while batch.len() < batch_size {
                    match receiver.try_recv() {
                        Ok(msg) => batch.push(msg),
                        Err(_) => break,
                    }
                }
                callback(shard_id, batch).await;
            }
        });
    }
}

impl ShardManager {
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
    /// Never evicts the reserved deadzone or spread shards
    pub fn evict_idle(&self, idle_threshold: Duration) {
        self.shards.retain(|id, shard| {
            // Never evict reserved shards
            if *id == self.deadzone_shard_id || *id == self.spread_shard_id {
                return true;
            }
            shard.last_access().elapsed() < idle_threshold
        });
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

    fn select_shard_for_message(&self) -> ShardId {
        // O(1) round-robin across regular shards (ShardId(1)..ShardId(num_shards-1)).
        // ShardId(0) is deadzone, ShardId(num_shards) is spread — both excluded.
        let idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % (self.num_shards as u64 - 1);
        ShardId(1 + idx)
    }

    fn create_shard(&self, shard_id: ShardId) {
        if !self.shards.contains_key(&shard_id) {
            let (shard, rx) = MemoryShard::new(shard_id, self.shard_size, false);
            self.shards.insert(shard_id, shard);
            self.receivers.insert(shard_id, rx);
        }
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
