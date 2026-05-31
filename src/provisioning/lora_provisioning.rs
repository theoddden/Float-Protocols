//! LoRa Provisioning Mode
//!
//! LoRa node provisioning via SX1262.
//! Activated by 3s config button hold.
//! Listens for Float Node join requests, issues session keys.

use crate::hardware::sx1262::{SX1262, LoRaFrame, LoRaConfig};
use crate::hardware::config_button::ButtonEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// LoRa node registry entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoRaNode {
    pub node_id: u32,
    pub session_key: [u8; 16],
    pub last_seen: u64,
    pub sensor_type: String,
}

/// LoRa provisioning mode
pub struct LoRaProvisioningMode {
    sx1262: Arc<Mutex<SX1262>>,
    nodes: Arc<Mutex<HashMap<u32, LoRaNode>>>,
    active: Arc<Mutex<bool>>,
}

impl LoRaProvisioningMode {
    /// Initialize LoRa provisioning mode
    pub fn new(sx1262: SX1262) -> Self {
        Self {
            sx1262: Arc::new(Mutex::new(sx1262)),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            active: Arc::new(Mutex::new(false)),
        }
    }

    /// Start provisioning mode
    pub async fn start(&self) -> Result<(), ProvisioningError> {
        *self.active.lock().await = true;
        tracing::info!("LoRa provisioning mode started");

        let sx1262 = self.sx1262.clone();
        let nodes = self.nodes.clone();
        let active = self.active.clone();

        tokio::spawn(async move {
            while *active.lock().await {
                // Listen for join requests
                if let Ok(frame) = sx1262.lock().await.rx_packet().await {
                    if let Ok(node) = Self::parse_join_request(&frame) {
                        // Issue session key
                        let session_key = Self::generate_session_key();
                        let node_entry = LoRaNode {
                            node_id: node.node_id,
                            session_key,
                            last_seen: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            sensor_type: node.sensor_type,
                        };

                        nodes.lock().await.insert(node.node_id, node_entry.clone());

                        // Send join accept
                        let accept = Self::build_join_accept(&node_entry);
                        let _ = sx1262.lock().await.tx_packet(&accept).await;

                        tracing::info!("Provisioned LoRa node: {}", node.node_id);
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });

        Ok(())
    }

    /// Stop provisioning mode
    pub async fn stop(&self) {
        *self.active.lock().await = false;
        tracing::info!("LoRa provisioning mode stopped");
    }

    /// Parse join request from LoRa frame
    fn parse_join_request(frame: &LoRaFrame) -> Result<JoinRequest, ProvisioningError> {
        if frame.data.len() < 8 {
            return Err(ProvisioningError::InvalidFrame);
        }

        // Join request format: [node_id (4)][sensor_type_len (1)][sensor_type (N)]
        let node_id = u32::from_be_bytes([frame.data[0], frame.data[1], frame.data[2], frame.data[3]]);
        let sensor_type_len = frame.data[4] as usize;

        if frame.data.len() < 5 + sensor_type_len {
            return Err(ProvisioningError::InvalidFrame);
        }

        let sensor_type = String::from_utf8_lossy(&frame.data[5..5 + sensor_type_len]).to_string();

        Ok(JoinRequest { node_id, sensor_type })
    }

    /// Generate session key
    fn generate_session_key() -> [u8; 16] {
        use rand::Rng;
        let mut key = [0u8; 16];
        rand::thread_rng().fill(&mut key);
        key
    }

    /// Build join accept frame
    fn build_join_accept(node: &LoRaNode) -> Vec<u8> {
        let mut frame = Vec::with_capacity(21);
        frame.extend_from_slice(&node.node_id.to_be_bytes());
        frame.extend_from_slice(&node.session_key);
        frame
    }

    /// Get node registry
    pub async fn get_nodes(&self) -> Vec<LoRaNode> {
        self.nodes.lock().await.values().cloned().collect()
    }

    /// Remove node from registry
    pub async fn remove_node(&self, node_id: u32) {
        self.nodes.lock().await.remove(&node_id);
    }
}

/// Join request
#[derive(Debug, Clone)]
struct JoinRequest {
    node_id: u32,
    sensor_type: String,
}

#[derive(Debug)]
pub enum ProvisioningError {
    InvalidFrame,
}

impl std::fmt::Display for ProvisioningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProvisioningError::InvalidFrame => write!(f, "Invalid LoRa frame"),
        }
    }
}

impl std::error::Error for ProvisioningError {}
