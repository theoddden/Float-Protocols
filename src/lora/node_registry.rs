//! LoRa Node Registry
//!
//! Registry for Float Node sensor hubs.
//! Node ID → session key, last seen, sensor type.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// LoRa node entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoRaNode {
    pub node_id: u32,
    pub session_key: [u8; 16],
    pub last_seen: u64,
    pub sensor_type: String,
}

/// LoRa node registry
pub struct NodeRegistry {
    nodes: Arc<Mutex<HashMap<u32, LoRaNode>>>,
}

impl NodeRegistry {
    /// Create new node registry
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a new node
    pub async fn register(&self, node: LoRaNode) {
        self.nodes.lock().await.insert(node.node_id, node);
    }

    /// Get node by ID
    pub fn get_node(&self, node_id: u32) -> Option<LoRaNode> {
        // Note: This is synchronous for simplicity, could be async
        let nodes = self.nodes.blocking_lock();
        nodes.get(&node_id).cloned()
    }

    /// Update node last seen timestamp
    pub async fn update_last_seen(&self, node_id: u32) {
        let mut nodes = self.nodes.lock().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_seen = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }
    }

    /// Remove node
    pub async fn remove(&self, node_id: u32) {
        self.nodes.lock().await.remove(&node_id);
    }

    /// Get all nodes
    pub async fn get_all(&self) -> Vec<LoRaNode> {
        self.nodes.lock().await.values().cloned().collect()
    }

    /// Get stale nodes (not seen in > 1 hour)
    pub async fn get_stale(&self) -> Vec<LoRaNode> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let stale_threshold = 3_600_000; // 1 hour in ms

        self.nodes
            .lock()
            .await
            .values()
            .filter(|n| now - n.last_seen > stale_threshold)
            .cloned()
            .collect()
    }

    /// Get node count
    pub async fn count(&self) -> usize {
        self.nodes.lock().await.len()
    }
}
