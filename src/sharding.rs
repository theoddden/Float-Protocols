//! Memory sharding for immediate uplink when deadzone is hit (InferX pattern)
//! 
//! Pre-shards memory into dedicated uplink buffers that are immediately available
//! when a deadzone is detected, eliminating allocation latency during critical
//! transitions from connected to disconnected states.

use crate::protocol::{Message, Protocol};
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub u64);

pub struct MemoryShard {
    id: ShardId,
    buffer: Vec<Message>,
    max_size: usize,
    last_access: Instant,
    is_deadzone_shard: bool, // Dedicated shard for deadzone uplink
}

impl MemoryShard {
    pub fn new(id: ShardId, max_size: usize, is_deadzone_shard: bool) -> Self {
        Self {
            id,
            buffer: Vec::with_capacity(max_size),
            max_size,
            last_access: Instant::now(),
            is_deadzone_shard,
        }
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

    /// Pre-allocate buffer for immediate deadzone uplink
    pub fn preallocate_for_deadzone(&mut self) {
        if self.buffer.capacity() < self.max_size {
            self.buffer.reserve(self.max_size - self.buffer.capacity());
        }
    }

pub struct ShardManager {
    shards: RwLock<HashMap<ShardId, MemoryShard>>,
    num_shards: usize,
    shard_size: usize,
    next_shard_id: u64,
    deadzone_shard_id: ShardId,
}

impl ShardManager {
    pub fn new(num_shards: usize, shard_size: usize) -> Self {
        let mut shards = HashMap::new();
        
        // Create dedicated deadzone shard (highest priority)
        let deadzone_shard_id = ShardId(0);
        shards.insert(
            deadzone_shard_id,
            MemoryShard::new(deadzone_shard_id, shard_size, true)
        );
        
        // Create regular shards
        for i in 1..num_shards {
            shards.insert(
                ShardId(i as u64),
                MemoryShard::new(ShardId(i as u64), shard_size, false)
            );
        }

        Self {
            shards: RwLock::new(shards),
            num_shards,
            shard_size,
            next_shard_id: num_shards as u64,
            deadzone_shard_id,
        }
    }

    /// Get or create a shard for a specific protocol
    pub async fn get_shard(&self, protocol: Protocol) -> ShardId {
        let shard_id = self.protocol_to_shard_id(protocol);
        let shards = self.shards.read().await;
        
        if shards.contains_key(&shard_id) {
            return shard_id;
        }

        drop(shards);
        self.create_shard(shard_id).await;
        shard_id
    }

    /// Push message to appropriate shard with load balancing
    pub async fn push(&self, message: Message) -> Result<ShardId, ShardError> {
        let shard_id = self.select_shard_for_message(&message).await;
        let mut shards = self.shards.write().await;
        
        if let Some(shard) = shards.get_mut(&shard_id) {
            shard.push(message)?;
            Ok(shard_id)
        } else {
            Err(ShardError::NotFound)
        }
    }

    /// Push message to deadzone shard for immediate uplink when deadzone detected
    pub async fn push_deadzone(&self, message: Message) -> Result<ShardId, ShardError> {
        let mut shards = self.shards.write().await;
        
        if let Some(shard) = shards.get_mut(&self.deadzone_shard_id) {
            // Pre-allocate if needed for immediate uplink
            shard.preallocate_for_deadzone();
            shard.push(message)?;
            Ok(self.deadzone_shard_id)
        } else {
            Err(ShardError::NotFound)
        }
    }

    /// Get deadzone shard for immediate uplink access
    pub async fn get_deadzone_shard(&self) -> ShardId {
        self.deadzone_shard_id
    }

    /// Drain all messages from a shard
    pub async fn drain_shard(&self, shard_id: ShardId) -> Vec<Message> {
        let mut shards = self.shards.write().await;
        if let Some(shard) = shards.get_mut(&shard_id) {
            shard.drain()
        } else {
            Vec::new()
        }
    }

    /// Get statistics across all shards
    pub async fn stats(&self) -> ShardStats {
        let shards = self.shards.read().await;
        let total_messages: usize = shards.values().map(|s| s.len()).sum();
        let active_shards = shards.values().filter(|s| !s.is_empty()).count();
        
        ShardStats {
            total_shards: shards.len(),
            active_shards,
            total_messages,
            shard_size: self.shard_size,
        }
    }

    /// Evict idle shards to free memory
    pub async fn evict_idle(&self, idle_threshold: Duration) {
        let mut shards = self.shards.write().await;
        shards.retain(|_, shard| shard.last_access().elapsed() < idle_threshold);
    }

    fn protocol_to_shard_id(&self, protocol: Protocol) -> ShardId {
        // Consistent hashing based on protocol
        let hash = match protocol {
            Protocol::IridiumSBD => 1,
            Protocol::InmarsatC => 2,
            Protocol::VSAT => 3,
            Protocol::HFVHF => 4,
            Protocol::RockBLOCK => 5,
            Protocol::ASTSpaceMobile => 6,
        };
        ShardId((hash % self.num_shards as u64) as u64)
    }

    async fn select_shard_for_message(&self, message: &Message) -> ShardId {
        // Load balancing: select shard with least messages
        let shards = self.shards.read().await;
        let min_shard = shards
            .iter()
            .min_by_key(|(_, shard)| shard.len())
            .map(|(id, _)| *id)
            .unwrap_or_else(|| ShardId(0));
        
        min_shard
    }

    async fn create_shard(&self, shard_id: ShardId) {
        let mut shards = self.shards.write().await;
        shards.entry(shard_id)
            .or_insert_with(|| MemoryShard::new(shard_id, self.shard_size));
    }
}

#[derive(Debug)]
pub enum ShardError {
    Full,
    NotFound,
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
