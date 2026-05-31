//! RS-232 Sensor Reader
//!
//! RS-232 sensor interface via MAX3232 transceiver.
//! UART2 (GPIO0/GPIO1) through M12 A-coded connector.
//!
//! Supported protocols:
//! - Carrier Micro-Link binary frames
//! - Thermo King temperature data
//! - Modbus RTU over RS-232

use crate::protocol::{Message, Priority, Protocol};
use bytes::Bytes;
use nom::bytes::complete::take;
use nom::number::complete::{be_f32, be_u16, be_u8};
use nom::sequence::tuple;
use serialport::{SerialPort, SerialPortInfo};
use std::io::Read;
use std::time::Duration;
use tokio::sync::mpsc;

/// RS-232 UART device
const RS232_UART: &str = "/dev/ttyAMA1";
const RS232_BAUD: u32 = 9600;

/// RS-232 reader
pub struct Rs232Reader {
    port: Box<dyn SerialPort>,
    tx: mpsc::Sender<Message>,
}

impl Rs232Reader {
    /// Initialize RS-232 reader
    pub fn new(tx: mpsc::Sender<Message>) -> Result<Self, Rs232Error> {
        let port = serialport::new(RS232_UART, RS232_BAUD)
            .timeout(Duration::from_millis(100))
            .open()
            .map_err(|e| Rs232Error::SerialInit(e.to_string()))?;

        tracing::info!("RS-232 reader initialized on {}", RS232_UART);
        Ok(Self { port, tx })
    }

    /// Start reading frames
    pub async fn start(&mut self) -> Result<(), Rs232Error> {
        let mut buffer = [0u8; 1024];

        loop {
            match self.port.read(&mut buffer) {
                Ok(n) if n > 0 => {
                    let frame = &buffer[..n];

                    // Try to parse as Carrier Micro-Link
                    if let Ok(msg) = self.parse_carrier_micro_link(frame) {
                        let _ = self.tx.send(msg).await;
                        continue;
                    }

                    // Try to parse as Thermo King
                    if let Ok(msg) = self.parse_thermo_king(frame) {
                        let _ = self.tx.send(msg).await;
                        continue;
                    }

                    // Try to parse as Modbus RTU
                    if let Ok(msg) = self.parse_modbus_rtu(frame) {
                        let _ = self.tx.send(msg).await;
                        continue;
                    }

                    tracing::warn!("Unknown RS-232 frame: {:?}", frame);
                }
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(e) => return Err(Rs232Error::SerialRead(e.to_string())),
            }
        }
    }

    /// Parse Carrier Micro-Link binary frame
    fn parse_carrier_micro_link(&self, data: &[u8]) -> Result<Message, Rs232Error> {
        // Carrier Micro-Link format:
        // [header (2)][length (2)][type (1)][payload (N)][crc (2)]
        if data.len() < 5 {
            return Err(Rs232Error::InvalidFrame);
        }

        let header = &data[0..2];
        if header != b"CM" {
            return Err(Rs232Error::InvalidFrame);
        }

        let length = u16::from_be_bytes([data[2], data[3]]) as usize;
        if data.len() < 5 + length + 2 {
            return Err(Rs232Error::InvalidFrame);
        }

        let frame_type = data[4];
        let payload = &data[5..5 + length];
        let crc = u16::from_be_bytes([data[5 + length], data[5 + length + 1]]);

        // Verify CRC
        let computed_crc = self.compute_crc16(&data[..5 + length]);
        if computed_crc != crc {
            return Err(Rs232Error::CrcMismatch);
        }

        // Convert to Message
        let mut message_data = Vec::with_capacity(1 + payload.len());
        message_data.push(frame_type);
        message_data.extend_from_slice(payload);

        Ok(Message::new(
            Protocol::IridiumSBD, // Use IridiumSBD as carrier for sensor data
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Parse Thermo King temperature data frame
    fn parse_thermo_king(&self, data: &[u8]) -> Result<Message, Rs232Error> {
        // Thermo King format:
        // [STX (1)][addr (1)][cmd (1)][data (N)][ETX (1)][crc (1)]
        if data.len() < 5 {
            return Err(Rs232Error::InvalidFrame);
        }

        if data[0] != 0x02 || data[data.len() - 2] != 0x03 {
            return Err(Rs232Error::InvalidFrame);
        }

        let addr = data[1];
        let cmd = data[2];
        let payload = &data[3..data.len() - 2];
        let crc = data[data.len() - 1];

        // Verify CRC (XOR checksum)
        let computed_crc = data[1..data.len() - 1].iter().fold(0u8, |acc, &b| acc ^ b);
        if computed_crc != crc {
            return Err(Rs232Error::CrcMismatch);
        }

        // Convert to Message
        let mut message_data = Vec::with_capacity(2 + payload.len());
        message_data.push(addr);
        message_data.push(cmd);
        message_data.extend_from_slice(payload);

        Ok(Message::new(
            Protocol::IridiumSBD,
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Parse Modbus RTU frame
    fn parse_modbus_rtu(&self, data: &[u8]) -> Result<Message, Rs232Error> {
        // Modbus RTU format:
        // [addr (1)][func (1)][data (N)][crc (2)]
        if data.len() < 4 {
            return Err(Rs232Error::InvalidFrame);
        }

        let addr = data[0];
        let func = data[1];
        let payload = &data[2..data.len() - 2];
        let crc = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);

        // Verify CRC (Modbus CRC-16)
        let computed_crc = self.compute_modbus_crc(&data[..data.len() - 2]);
        if computed_crc != crc {
            return Err(Rs232Error::CrcMismatch);
        }

        // Convert to Message
        let mut message_data = Vec::with_capacity(2 + payload.len());
        message_data.push(addr);
        message_data.push(func);
        message_data.extend_from_slice(payload);

        Ok(Message::new(
            Protocol::IridiumSBD,
            Bytes::from(message_data),
            Priority::Operational,
        ))
    }

    /// Compute CRC-16 for Carrier Micro-Link
    fn compute_crc16(&self, data: &[u8]) -> u16 {
        let mut crc = 0xFFFF;
        for &byte in data {
            crc ^= u16::from(byte);
            for _ in 0..8 {
                if crc & 0x0001 != 0 {
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        crc
    }

    /// Compute Modbus CRC-16
    fn compute_modbus_crc(&self, data: &[u8]) -> u16 {
        let mut crc = 0xFFFF;
        for &byte in data {
            crc ^= u16::from(byte);
            for _ in 0..8 {
                if crc & 0x0001 != 0 {
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        crc
    }
}

#[derive(Debug)]
pub enum Rs232Error {
    SerialInit(String),
    SerialRead(String),
    InvalidFrame,
    CrcMismatch,
}

impl std::fmt::Display for Rs232Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rs232Error::SerialInit(e) => write!(f, "Serial init failed: {}", e),
            Rs232Error::SerialRead(e) => write!(f, "Serial read failed: {}", e),
            Rs232Error::InvalidFrame => write!(f, "Invalid frame format"),
            Rs232Error::CrcMismatch => write!(f, "CRC mismatch"),
        }
    }
}

impl std::error::Error for Rs232Error {}
