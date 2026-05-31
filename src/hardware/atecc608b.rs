//! ATECC608B Hardware Root of Trust Driver
//!
//! I2C ECC P-256 cryptographic chip for signing bi-temporal records.
//! I2C address: 0x60
//!
//! Features:
//! - ECC P-256 signing/verification
//! - Secure key storage (non-volatile)
//! - Device serial number (9 bytes)
//! - Random number generation
//!
//! Linux I2C via rppal (Raspberry Pi CM5)
//!
//! Quectel Hardware Design Guide section 4 for reference.

use rppal::i2c::I2c;
use std::time::Duration;

/// ATECC608B I2C address
const ATECC608B_ADDR: u8 = 0x60;

/// ATECC608B command opcodes
#[repr(u8)]
enum Opcode {
    /// Read memory zone
    Read = 0x02,
    /// Write memory zone
    Write = 0x12,
    /// Random number generation
    Random = 0x1B,
    /// Sign message (ECC P-256)
    Sign = 0x41,
    /// Verify signature
    Verify = 0x45,
    /// Get device serial number
    Serial = 0x03,
    /// Lock configuration zone
    LockConfig = 0x17,
    /// Lock data zone
    LockData = 0x16,
}

/// Memory zones
#[repr(u8)]
enum Zone {
    Configuration = 0x00,
    Data = 0x01,
    OTP = 0x02,
}

/// ATECC608B driver
pub struct ATECC608B {
    i2c: I2c,
}

impl ATECC608B {
    /// Initialize ATECC608B on I2C bus 1
    pub fn new() -> Result<Self, AteccError> {
        let i2c = I2c::with_bus(1).map_err(|e| AteccError::I2cInit(e.to_string()))?;
        let mut atecc = Self { i2c };

        // Wake up the chip (send dummy address)
        atecc.wake()?;

        // Verify chip is responsive by reading serial
        let _serial = atecc.read_serial()?;

        Ok(atecc)
    }

    /// Wake up the chip from sleep mode
    fn wake(&mut self) -> Result<(), AteccError> {
        // Send wake condition (dummy write to address 0x00)
        self.i2c
            .set_slave_address(0x00)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;
        self.i2c
            .write(&[0x00])
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        // Wait for wake-up complete
        std::thread::sleep(Duration::from_millis(1));

        // Switch back to normal address
        self.i2c
            .set_slave_address(ATECC608B_ADDR)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        Ok(())
    }

    /// Read device serial number (9 bytes)
    pub fn read_serial(&mut self) -> Result<[u8; 9], AteccError> {
        self.wake()?;

        // Read serial from configuration zone (addresses 0x00-0x08)
        let mut cmd = vec![0u8; 7];
        cmd[0] = Opcode::Read as u8;
        cmd[1] = Zone::Configuration as u8;
        cmd[2] = 0x00; // Address MSB
        cmd[3] = 0x00; // Address LSB
        cmd[4] = 0x00; // Length MSB
        cmd[5] = 0x09; // Length LSB (9 bytes for serial)
        cmd[6] = Self::crc8(&cmd[0..6]);

        self.i2c
            .write(&cmd)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        // Wait for response
        std::thread::sleep(Duration::from_millis(5));

        let mut response = [0u8; 32]; // Response includes header + data + CRC
        self.i2c
            .read(&mut response)
            .map_err(|e| AteccError::I2cRead(e.to_string()))?;

        // Check response header (should be 0x01 for success)
        if response[0] != 0x01 {
            return Err(AteccError::InvalidResponse(response[0]));
        }

        // Extract serial (bytes 1-9)
        let mut serial = [0u8; 9];
        serial.copy_from_slice(&response[1..10]);

        Ok(serial)
    }

    /// Sign a 32-byte digest using ECC P-256 private key in slot 0
    pub fn sign(&mut self, digest: &[u8; 32]) -> Result<[u8; 64], AteccError> {
        if digest.len() != 32 {
            return Err(AteccError::InvalidDigestLength);
        }

        self.wake()?;

        // Sign command: opcode + zone + key_id + [digest] + crc
        let mut cmd = vec![0u8; 36];
        cmd[0] = Opcode::Sign as u8;
        cmd[1] = 0x80; // Mode: external signature, message in TempKey
        cmd[2] = 0x00; // Key ID (slot 0)
        cmd[3..35].copy_from_slice(digest);
        cmd[35] = Self::crc8(&cmd[0..35]);

        self.i2c
            .write(&cmd)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        // Wait for signature generation (can take up to 70ms)
        std::thread::sleep(Duration::from_millis(100));

        let mut response = [0u8; 64 + 4]; // 64-byte signature + header + CRC
        self.i2c
            .read(&mut response)
            .map_err(|e| AteccError::I2cRead(e.to_string()))?;

        if response[0] != 0x01 {
            return Err(AteccError::InvalidResponse(response[0]));
        }

        // Extract signature (bytes 1-64)
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&response[1..65]);

        Ok(signature)
    }

    /// Verify a signature using ECC P-256 public key in slot 0
    pub fn verify(&mut self, digest: &[u8; 32], signature: &[u8; 64]) -> Result<bool, AteccError> {
        if digest.len() != 32 {
            return Err(AteccError::InvalidDigestLength);
        }
        if signature.len() != 64 {
            return Err(AteccError::InvalidSignatureLength);
        }

        self.wake()?;

        // Verify command: opcode + [message] + [signature] + crc
        let mut cmd = vec![0u8; 99];
        cmd[0] = Opcode::Verify as u8;
        cmd[1] = 0x00; // Mode: verify with stored public key
        cmd[2] = 0x00; // Key ID (slot 0)
        cmd[3] = 0x00; // Signature index
        cmd[4..36].copy_from_slice(digest);
        cmd[36..100].copy_from_slice(signature);
        cmd[98] = Self::crc8(&cmd[0..98]);

        self.i2c
            .write(&cmd)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        std::thread::sleep(Duration::from_millis(50));

        let mut response = [0u8; 4];
        self.i2c
            .read(&mut response)
            .map_err(|e| AteccError::I2cRead(e.to_string()))?;

        // Response[0] = 0x00 for valid, 0x01 for invalid
        Ok(response[0] == 0x00)
    }

    /// Generate random number (32 bytes)
    pub fn random(&mut self) -> Result<[u8; 32], AteccError> {
        self.wake()?;

        let mut cmd = [0u8; 3];
        cmd[0] = Opcode::Random as u8;
        cmd[1] = 0x00; // Mode: update seed
        cmd[2] = 0x20; // Length: 32 bytes

        self.i2c
            .write(&cmd)
            .map_err(|e| AteccError::I2cWrite(e.to_string()))?;

        std::thread::sleep(Duration::from_millis(20));

        let mut response = [0u8; 35]; // 32 bytes + header + CRC
        self.i2c
            .read(&mut response)
            .map_err(|e| AteccError::I2cRead(e.to_string()))?;

        if response[0] != 0x01 {
            return Err(AteccError::InvalidResponse(response[0]));
        }

        let mut random = [0u8; 32];
        random.copy_from_slice(&response[1..33]);

        Ok(random)
    }

    /// CRC-8 calculation for ATECC608B commands
    fn crc8(data: &[u8]) -> u8 {
        let mut crc = 0u8;
        for &byte in data {
            crc ^= byte;
            for _ in 0..8 {
                if crc & 0x80 != 0 {
                    crc = (crc << 1) ^ 0x07; // Polynomial: x^8 + x^2 + x + 1
                } else {
                    crc <<= 1;
                }
            }
        }
        crc
    }
}

#[derive(Debug)]
pub enum AteccError {
    I2cInit(String),
    I2cWrite(String),
    I2cRead(String),
    InvalidResponse(u8),
    InvalidDigestLength,
    InvalidSignatureLength,
    LockFailed,
}

impl std::fmt::Display for AteccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AteccError::I2cInit(e) => write!(f, "I2C init failed: {}", e),
            AteccError::I2cWrite(e) => write!(f, "I2C write failed: {}", e),
            AteccError::I2cRead(e) => write!(f, "I2C read failed: {}", e),
            AteccError::InvalidResponse(b) => write!(f, "Invalid response byte: 0x{:02X}", b),
            AteccError::InvalidDigestLength => write!(f, "Digest must be 32 bytes"),
            AteccError::InvalidSignatureLength => write!(f, "Signature must be 64 bytes"),
            AteccError::LockFailed => write!(f, "Lock operation failed"),
        }
    }
}

impl std::error::Error for AteccError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc8() {
        // Test vector from ATECC608B datasheet
        let data = [0x01, 0x02, 0x03];
        let crc = ATECC608B::crc8(&data);
        // Expected CRC-8 for this data
        assert_eq!(crc, 0x5B);
    }
}
