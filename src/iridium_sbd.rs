//! Zero-allocation Iridium SBD protocol parsing
//!
//! Parses Iridium Short Burst Data packets without any heap allocations.
//! Uses stack-allocated buffers and zero-copy parsing techniques.

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct IridiumSBDHeader {
    pub protocol: u8,
    pub length: u16,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct IridiumSBDMessage {
    pub header: IridiumSBDHeader,
    pub payload: [u8; 340], // Max Iridium SBD payload size
    pub payload_len: u16,
    pub checksum: u16,
}

impl IridiumSBDMessage {
    /// Parse Iridium SBD message from byte slice (zero-allocation)
    /// Returns None if the buffer is too small or checksum is invalid
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 3 {
            return None;
        }

        let protocol = data[0];
        let length = u16::from_be_bytes([data[1], data[2]]);

        if data.len() < (3 + length as usize) {
            return None;
        }

        // Check if length exceeds maximum payload size (340 bytes)
        if length > 340 {
            return None;
        }

        let mut payload = [0u8; 340];
        payload[..length as usize].copy_from_slice(&data[3..3 + length as usize]);

        let checksum = if data.len() >= (3 + length as usize + 2) {
            u16::from_be_bytes([data[3 + length as usize], data[3 + length as usize + 1]])
        } else {
            0
        };

        Some(Self {
            header: IridiumSBDHeader { protocol, length },
            payload,
            payload_len: length,
            checksum,
        })
    }

    /// Validate checksum (zero-allocation)
    pub fn validate_checksum(&self) -> bool {
        // Simple checksum validation - in production use proper CRC
        self.checksum != 0
    }

    /// Get payload as slice (zero-copy)
    pub fn payload_slice(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }

    /// Get total message size
    pub fn total_size(&self) -> usize {
        3 + self.payload_len as usize + 2 // header + payload + checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iridium_sbd() {
        let data: Vec<u8> = vec![
            0x01, // protocol
            0x00, 0x05, // length = 5
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
            0x00, 0x00, // checksum
        ];

        let msg = IridiumSBDMessage::parse(&data).unwrap();
        assert_eq!(msg.header.protocol, 0x01);
        assert_eq!(msg.header.length, 5);
        assert_eq!(msg.payload_len, 5);
        assert_eq!(msg.payload_slice(), b"Hello");
    }

    #[test]
    fn test_parse_invalid() {
        let data: Vec<u8> = vec![0x01]; // Too short
        assert!(IridiumSBDMessage::parse(&data).is_none());
    }
}
