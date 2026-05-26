//! Snapshotting for fast uplink building (InferX pattern)
//!
//! Creates snapshots of message batches for rapid uplink construction
//! during deadzone transitions, enabling instant uplink without reprocessing.

use crate::protocol::{Message, Protocol};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub messages: Vec<Message>,
    pub protocol: Protocol,
    pub created_at: u64, // Unix timestamp in milliseconds
    pub size_bytes: usize,
    pub device_id: u64, // djb2 hash of sender payload; 0 = unknown
}

pub struct SnapshotManager {
    snapshots: RwLock<HashMap<String, Snapshot>>,
    max_snapshots: usize,
    snapshot_ttl: Duration,
}

impl SnapshotManager {
    pub fn new(max_snapshots: usize, snapshot_ttl: Duration) -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
            max_snapshots,
            snapshot_ttl,
        }
    }

    /// Create a snapshot of a batch of messages for fast uplink.
    /// `device_id` is the djb2 hash of the sender's payload (from clock reconciler).
    pub async fn create_snapshot(
        &self,
        messages: Vec<Message>,
        protocol: Protocol,
        device_id: u64,
    ) -> String {
        let snapshot_id = self.generate_snapshot_id(&messages, protocol);
        let size_bytes = messages.iter().map(|m| m.size()).sum();

        let snapshot = Snapshot {
            id: snapshot_id.clone(),
            messages,
            protocol,
            created_at: self.now_ms(),
            size_bytes,
            device_id,
        };

        let mut snapshots = self.snapshots.write().await;

        if snapshots.len() >= self.max_snapshots {
            self.evict_oldest(&mut snapshots);
        }

        snapshots.insert(snapshot_id.clone(), snapshot);
        snapshot_id
    }

    /// Create a snapshot for an entire translated batch with a single write lock.
    /// Prefer this over N individual `create_snapshot()` calls in hot paths.
    pub async fn create_batch_snapshot(
        &self,
        messages: Vec<Message>,
        protocol: Protocol,
        device_id: u64,
    ) -> String {
        self.create_snapshot(messages, protocol, device_id).await
    }

    /// Drain all unexpired snapshots belonging to a device since `since_ms`.
    /// Intended for fast uplink construction when a device exits a dead zone:
    /// the caller retrieves pre-translated snapshots and sends them in bulk
    /// without re-translation.
    pub async fn drain_for_uplink(&self, device_id: u64, since_ms: u64) -> Vec<Snapshot> {
        let mut snapshots = self.snapshots.write().await;
        let now = self.now_ms();
        let ttl_ms = self.snapshot_ttl.as_millis() as u64;
        let mut result = Vec::new();
        snapshots.retain(|_, s| {
            let matches = s.device_id == device_id
                && s.created_at >= since_ms
                && (now - s.created_at) < ttl_ms;
            if matches {
                result.push(s.clone());
            }
            !matches
        });
        result
    }

    /// Retrieve a snapshot for instant uplink (no reprocessing needed)
    pub async fn get_snapshot(&self, snapshot_id: &str) -> Option<Snapshot> {
        let snapshots = self.snapshots.read().await;
        let snapshot = snapshots.get(snapshot_id)?;

        // Check if snapshot is still valid (not expired)
        let age_ms = self.now_ms() - snapshot.created_at;
        if age_ms > self.snapshot_ttl.as_millis() as u64 {
            return None;
        }

        Some(snapshot.clone())
    }

    /// Get all snapshots for a specific protocol
    pub async fn get_protocol_snapshots(&self, protocol: Protocol) -> Vec<Snapshot> {
        let snapshots = self.snapshots.read().await;
        let now = self.now_ms();

        snapshots
            .values()
            .filter(|s| {
                s.protocol == protocol
                    && (now - s.created_at) < self.snapshot_ttl.as_millis() as u64
            })
            .cloned()
            .collect()
    }

    /// Delete a specific snapshot
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> bool {
        let mut snapshots = self.snapshots.write().await;
        snapshots.remove(snapshot_id).is_some()
    }

    /// Clear all expired snapshots
    pub async fn clear_expired(&self) {
        let mut snapshots = self.snapshots.write().await;
        let now = self.now_ms();
        snapshots.retain(|_, s| (now - s.created_at) < self.snapshot_ttl.as_millis() as u64);
    }

    /// Get snapshot statistics
    pub async fn stats(&self) -> SnapshotStats {
        let snapshots = self.snapshots.read().await;
        let now = self.now_ms();
        let valid_snapshots = snapshots
            .values()
            .filter(|s| (now - s.created_at) < self.snapshot_ttl.as_millis() as u64)
            .count();
        let total_size: usize = snapshots.values().map(|s| s.size_bytes).sum();

        SnapshotStats {
            total_snapshots: snapshots.len(),
            valid_snapshots,
            total_size_bytes: total_size,
            max_snapshots: self.max_snapshots,
        }
    }

    fn generate_snapshot_id(&self, messages: &[Message], protocol: Protocol) -> String {
        // Generate deterministic ID based on message content
        let hash = self.hash_messages(messages, protocol);
        format!("snap_{}_{}", protocol, hash)
    }

    fn hash_messages(&self, messages: &[Message], protocol: Protocol) -> u64 {
        // FNV-1a: deterministic across process restarts (unlike DefaultHasher
        // which is randomized since Rust 1.7). Required for stable snapshot IDs.
        const FNV_OFFSET: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;
        let mut h = FNV_OFFSET;
        // Mix protocol discriminant
        for &b in (protocol as u64).to_le_bytes().iter() {
            h ^= b as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for msg in messages {
            for &b in msg.data.iter() {
                h ^= b as u64;
                h = h.wrapping_mul(FNV_PRIME);
            }
        }
        h
    }

    fn evict_oldest(&self, snapshots: &mut HashMap<String, Snapshot>) {
        if let Some(oldest_id) = snapshots
            .iter()
            .min_by_key(|(_, s)| s.created_at)
            .map(|(id, _)| id.clone())
        {
            snapshots.remove(&oldest_id);
        }
    }

    fn now_ms(&self) -> u64 {
        #[cfg(feature = "std")]
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        #[cfg(not(feature = "std"))]
        {
            0
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotStats {
    pub total_snapshots: usize,
    pub valid_snapshots: usize,
    pub total_size_bytes: usize,
    pub max_snapshots: usize,
}

impl SnapshotStats {
    pub fn utilization(&self) -> f64 {
        if self.max_snapshots == 0 {
            0.0
        } else {
            (self.total_snapshots as f64) / (self.max_snapshots as f64)
        }
    }
}
