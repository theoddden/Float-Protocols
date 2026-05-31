//! CAN Bus Sensor Reader
//!
//! CAN Bus interface via TCAN337 CAN FD transceiver (ECO Change 2).
//! SocketCAN interface on Linux.
//!
//! Supported protocols:
//! - J1939 (heavy equipment, vehicles)
//! - CANopen (industrial automation)
//!
//! Note: TCAN337 transceiver is not in v0.0.3 BOM.
//! This driver requires ECO Change 2 hardware addition.

use crate::protocol::{Message, Priority, Protocol};
use bytes::Bytes;
use socketcan::{CANFrame, CANSocket, Socket};
use std::io;
use tokio::sync::mpsc;

/// CAN interface name
const CAN_INTERFACE: &str = "can0";

/// J1939 PGN (Parameter Group Number) for common messages
const J1939_PGN_ADDRESS_CLAIMED: u32 = 0x00EE00;
const J1939_PGN_REQUEST: u32 = 0x00EA00;
const J1939_PGN_PDU1: u32 = 0x00F000;
const J1939_PGN_PDU2: u32 = 0x00FF00;

/// CAN reader
pub struct CanReader {
    socket: CANSocket,
    tx: mpsc::Sender<Message>,
}

impl CanReader {
    /// Initialize CAN reader
    pub fn new(tx: mpsc::Sender<Message>) -> Result<Self, CanError> {
        let socket =
            CANSocket::open(CAN_INTERFACE).map_err(|e| CanError::SocketOpen(e.to_string()))?;

        // Set filters for J1939 and CANopen
        socket
            .set_filters(&[
                socketcan::CANFilter::new(0x000, 0x7FF).unwrap(), // Accept all
            ])
            .map_err(|e| CanError::SocketConfig(e.to_string()))?;

        tracing::info!("CAN reader initialized on {}", CAN_INTERFACE);
        Ok(Self { socket, tx })
    }

    /// Start reading CAN frames
    pub async fn start(&mut self) -> Result<(), CanError> {
        loop {
            match self.socket.read_frame() {
                Ok(frame) => {
                    // Try to parse as J1939
                    if let Ok(msg) = self.parse_j1939(&frame) {
                        let _ = self.tx.send(msg).await;
                        continue;
                    }

                    // Try to parse as CANopen
                    if let Ok(msg) = self.parse_canopen(&frame) {
                        let _ = self.tx.send(msg).await;
                        continue;
                    }

                    // Raw CAN frame
                    let _ = self.tx.send(self.parse_raw(&frame)).await;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(CanError::SocketRead(e.to_string())),
            }
        }
    }

    /// Parse J1939 frame
    fn parse_j1939(&self, frame: &CANFrame) -> Result<Message, CanError> {
        let can_id = frame.id();

        // J1939 CAN ID format: [priority (3)][PGN (18)][source (8)]
        let priority = (can_id >> 26) & 0x07;
        let pgn = (can_id >> 8) & 0x03FFFF;
        let source = can_id & 0xFF;

        // Extract data
        let data = frame.data();

        // Build J1939 message
        let mut message_data = Vec::with_capacity(4 + data.len());
        message_data.extend_from_slice(&priority.to_be_bytes());
        message_data.extend_from_slice(&pgn.to_be_bytes());
        message_data.push(source);
        message_data.extend_from_slice(data);

        Ok(Message::new(
            Protocol::IridiumSBD,
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Parse CANopen frame
    fn parse_canopen(&self, frame: &CANFrame) -> Result<Message, CanError> {
        let can_id = frame.id();

        // CANopen CAN ID format: [function (4)][node_id (7)]
        let function = (can_id >> 7) & 0x0F;
        let node_id = can_id & 0x7F;

        // Extract data
        let data = frame.data();

        // Build CANopen message
        let mut message_data = Vec::with_capacity(2 + data.len());
        message_data.push(function);
        message_data.push(node_id);
        message_data.extend_from_slice(data);

        Ok(Message::new(
            Protocol::IridiumSBD,
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Parse raw CAN frame
    fn parse_raw(&self, frame: &CANFrame) -> Message {
        let can_id = frame.id();
        let data = frame.data();

        let mut message_data = Vec::with_capacity(4 + data.len());
        message_data.extend_from_slice(&can_id.to_be_bytes());
        message_data.extend_from_slice(data);

        Message::new(
            Protocol::IridiumSBD,
            Bytes::from(message_data),
            Priority::Operational,
        )
    }

    /// Send CAN frame
    pub fn send_frame(&mut self, can_id: u32, data: &[u8]) -> Result<(), CanError> {
        let frame = CANFrame::new(can_id, data).map_err(|e| CanError::FrameBuild(e.to_string()))?;
        self.socket
            .write_frame(&frame)
            .map_err(|e| CanError::SocketWrite(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum CanError {
    SocketOpen(String),
    SocketConfig(String),
    SocketRead(String),
    SocketWrite(String),
    FrameBuild(String),
}

impl std::fmt::Display for CanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanError::SocketOpen(e) => write!(f, "Socket open failed: {}", e),
            CanError::SocketConfig(e) => write!(f, "Socket config failed: {}", e),
            CanError::SocketRead(e) => write!(f, "Socket read failed: {}", e),
            CanError::SocketWrite(e) => write!(f, "Socket write failed: {}", e),
            CanError::FrameBuild(e) => write!(f, "Frame build failed: {}", e),
        }
    }
}

impl std::error::Error for CanError {}
