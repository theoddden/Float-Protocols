//! LoRa Mesh Aggregator
//!
//! Aggregates sensor data from Float Node hubs over LoRa.
//! Decodes node payloads, forwards to gateway.

use crate::hardware::lr1121::{LoRaFrame, LR1121};
use crate::lora::node_registry::{LoRaNode, NodeRegistry};
use crate::protocol::{Message, Priority, Protocol};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

/// LoRa mesh aggregator
pub struct LoRaMeshAggregator {
    lr1121: Arc<tokio::sync::Mutex<LR1121>>,
    node_registry: Arc<NodeRegistry>,
    tx: mpsc::Sender<Message>,
}

impl LoRaMeshAggregator {
    /// Initialize LoRa mesh aggregator
    pub fn new(lr1121: LR1121, tx: mpsc::Sender<Message>) -> Self {
        Self {
            lr1121: Arc::new(tokio::sync::Mutex::new(lr1121)),
            node_registry: Arc::new(NodeRegistry::new()),
            tx,
        }
    }

    /// Start listening for LoRa frames
    pub async fn start(&self) -> Result<(), MeshError> {
        let lr1121 = self.lr1121.clone();
        let node_registry = self.node_registry.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            loop {
                // Receive LoRa frame
                match lr1121.lock().await.rx_packet().await {
                    Ok(frame) => {
                        // Try to decode as sensor data
                        if let Ok(message) = Self::decode_sensor_frame(&frame, &node_registry) {
                            let _ = tx.send(message).await;
                        } else {
                            tracing::warn!("Failed to decode LoRa frame");
                        }
                    }
                    Err(e) => {
                        tracing::error!("LoRa RX error: {}", e);
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });

        Ok(())
    }

    /// Decode sensor frame from LoRa node
    fn decode_sensor_frame(
        frame: &LoRaFrame,
        registry: &NodeRegistry,
    ) -> Result<Message, MeshError> {
        if frame.data.len() < 12 {
            return Err(MeshError::InvalidFrame);
        }

        // Frame format: [node_id (4)][seq (2)][payload_type (1)][payload (N)][mic (4)]
        let node_id =
            u32::from_be_bytes([frame.data[0], frame.data[1], frame.data[2], frame.data[3]]);
        let seq = u16::from_be_bytes([frame.data[4], frame.data[5]]);
        let payload_type = frame.data[6];
        let payload = &frame.data[7..frame.data.len() - 4];
        let mic = &frame.data[frame.data.len() - 4..];

        // Verify node is registered
        let node = registry.get_node(node_id).ok_or(MeshError::UnknownNode)?;

        // Verify MIC (Message Integrity Code) using session key
        if !Self::verify_mic(&frame.data[..frame.data.len() - 4], mic, &node.session_key) {
            return Err(MeshError::MicMismatch);
        }

        // Update node last seen
        registry.update_last_seen(node_id);

        // Build message
        let mut message_data = Vec::with_capacity(7 + payload.len());
        message_data.extend_from_slice(&node_id.to_be_bytes());
        message_data.extend_from_slice(&seq.to_be_bytes());
        message_data.push(payload_type);
        message_data.extend_from_slice(payload);

        Ok(Message::new(
            Protocol::LoRaMesh,
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Verify MIC using session key (HMAC-SHA256 truncated to 4 bytes)
    fn verify_mic(data: &[u8], mic: &[u8], key: &[u8; 16]) -> bool {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        let result = mac.finalize().into_bytes();

        // Compare first 4 bytes
        &result[..4] == mic
    }

    /// Send ACK to node
    pub async fn send_ack(&self, node_id: u32, seq: u16) -> Result<(), MeshError> {
        let mut ack = Vec::with_capacity(6);
        ack.extend_from_slice(&node_id.to_be_bytes());
        ack.extend_from_slice(&seq.to_be_bytes());

        self.lr1121.lock().await.tx_packet(&ack).await?;
        Ok(())
    }

    /// Get node registry
    pub fn node_registry(&self) -> Arc<NodeRegistry> {
        self.node_registry.clone()
    }
}

#[derive(Debug)]
pub enum MeshError {
    InvalidFrame,
    UnknownNode,
    MicMismatch,
}

impl std::fmt::Display for MeshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeshError::InvalidFrame => write!(f, "Invalid LoRa frame"),
            MeshError::UnknownNode => write!(f, "Unknown LoRa node"),
            MeshError::MicMismatch => write!(f, "MIC verification failed"),
        }
    }
}

impl std::error::Error for MeshError {}
