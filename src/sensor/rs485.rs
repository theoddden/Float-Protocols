//! RS-485 Sensor Reader
//!
//! RS-485 sensor interface via THVD1450 transceiver (ECO Change 4).
//! Half-duplex Modbus RTU with direction control.
//!
//! GPIO assignments:
//! - UART TX/RX: GPIO pins (same as RS-232, multiplexed)
//! - DE/RE: GPIO for direction control
//!
//! Note: THVD1450 transceiver is not in v0.0.3 BOM.
//! This driver requires ECO Change 4 hardware addition.

use crate::protocol::{Message, Priority, Protocol};
use bytes::Bytes;
use rppal::gpio::{Gpio, Level, OutputPin};
use serialport::{SerialPort, SerialPortInfo};
use std::io::{Read, Write};
use std::time::Duration;
use tokio::sync::mpsc;

/// RS-485 UART device (shared with RS-232 via multiplexer)
const RS485_UART: &str = "/dev/ttyAMA1";
const RS485_BAUD: u32 = 9600;

/// GPIO for DE/RE direction control
const RS485_DE_RE_PIN: u8 = 5; // GPIO5 (example, adjust based on PCB)

/// RS-485 reader
pub struct Rs485Reader {
    port: Box<dyn SerialPort>,
    de_re: OutputPin,
    tx: mpsc::Sender<Message>,
}

impl Rs485Reader {
    /// Initialize RS-485 reader
    pub fn new(tx: mpsc::Sender<Message>) -> Result<Self, Rs485Error> {
        let port = serialport::new(RS485_UART, RS485_BAUD)
            .timeout(Duration::from_millis(100))
            .open()
            .map_err(|e| Rs485Error::SerialInit(e.to_string()))?;

        let gpio = Gpio::new().map_err(|e| Rs485Error::GpioInit(e.to_string()))?;
        let de_re = gpio
            .get(RS485_DE_RE_PIN)
            .map_err(|e| Rs485Error::GpioInit(e.to_string()))?
            .into_output();

        // Set to receive mode initially
        de_re.set_low();

        tracing::info!("RS-485 reader initialized on {}", RS485_UART);
        Ok(Self { port, de_re, tx })
    }

    /// Start reading Modbus RTU frames
    pub async fn start(&mut self) -> Result<(), Rs485Error> {
        let mut buffer = [0u8; 256];

        loop {
            // Set to receive mode
            self.de_re.set_low();

            match self.port.read(&mut buffer) {
                Ok(n) if n > 0 => {
                    let frame = &buffer[..n];

                    // Try to parse as Modbus RTU
                    if let Ok(msg) = self.parse_modbus_rtu(frame) {
                        let _ = self.tx.send(msg).await;
                    } else {
                        tracing::warn!("Unknown RS-485 frame: {:?}", frame);
                    }
                }
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(e) => return Err(Rs485Error::SerialRead(e.to_string())),
            }
        }
    }

    /// Send Modbus RTU request
    pub fn send_modbus_request(
        &mut self,
        addr: u8,
        func: u8,
        data: &[u8],
    ) -> Result<(), Rs485Error> {
        // Set to transmit mode
        self.de_re.set_high();
        std::thread::sleep(Duration::from_millis(1));

        // Build Modbus RTU frame
        let mut frame = Vec::with_capacity(3 + data.len() + 2);
        frame.push(addr);
        frame.push(func);
        frame.extend_from_slice(data);

        // Compute CRC
        let crc = self.compute_modbus_crc(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());

        // Send frame
        self.port
            .write_all(&frame)
            .map_err(|e| Rs485Error::SerialWrite(e.to_string()))?;

        // Wait for transmission complete
        std::thread::sleep(Duration::from_millis(10));

        // Set back to receive mode
        self.de_re.set_low();

        Ok(())
    }

    /// Parse Modbus RTU frame
    fn parse_modbus_rtu(&self, data: &[u8]) -> Result<Message, Rs485Error> {
        // Modbus RTU format:
        // [addr (1)][func (1)][data (N)][crc (2)]
        if data.len() < 4 {
            return Err(Rs485Error::InvalidFrame);
        }

        let addr = data[0];
        let func = data[1];
        let payload = &data[2..data.len() - 2];
        let crc = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);

        // Verify CRC
        let computed_crc = self.compute_modbus_crc(&data[..data.len() - 2]);
        if computed_crc != crc {
            return Err(Rs485Error::CrcMismatch);
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
pub enum Rs485Error {
    SerialInit(String),
    SerialRead(String),
    SerialWrite(String),
    GpioInit(String),
    InvalidFrame,
    CrcMismatch,
}

impl std::fmt::Display for Rs485Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rs485Error::SerialInit(e) => write!(f, "Serial init failed: {}", e),
            Rs485Error::SerialRead(e) => write!(f, "Serial read failed: {}", e),
            Rs485Error::SerialWrite(e) => write!(f, "Serial write failed: {}", e),
            Rs485Error::GpioInit(e) => write!(f, "GPIO init failed: {}", e),
            Rs485Error::InvalidFrame => write!(f, "Invalid frame format"),
            Rs485Error::CrcMismatch => write!(f, "CRC mismatch"),
        }
    }
}

impl std::error::Error for Rs485Error {}
