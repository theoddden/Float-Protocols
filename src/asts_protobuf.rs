//! Zero-allocation AST SpaceMobile Protobuf encoding
//!
//! Encodes messages in AST SpaceMobile-compatible Protobuf format
//! without any heap allocations on the hot path.

use std::sync::atomic::{AtomicU32, Ordering};

/// Global sequence counter shared across all ZeroCopyTranslator instances.
/// Using Relaxed ordering — sequence uniqueness (no repeats within a process
/// lifetime) is the only requirement; strict inter-thread ordering is not needed.
static GLOBAL_SEQUENCE: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ASTSProtobufHeader {
    pub message_type: u8,
    pub sequence: u32,
    pub length: u16,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ASTSProtobufMessage {
    pub header: ASTSProtobufHeader,
    pub payload: [u8; 1024], // ASTS max payload size
    pub payload_len: u16,
}

impl ASTSProtobufMessage {
    /// Create new ASTS Protobuf message (zero-allocation)
    pub fn new(message_type: u8, sequence: u32) -> Self {
        Self {
            header: ASTSProtobufHeader {
                message_type,
                sequence,
                length: 0,
            },
            payload: [0u8; 1024],
            payload_len: 0,
        }
    }

    /// Set payload from slice (zero-copy)
    pub fn set_payload(&mut self, data: &[u8]) -> bool {
        if data.len() > 1024 {
            return false;
        }

        self.payload[..data.len()].copy_from_slice(data);
        self.payload_len = data.len() as u16;
        self.header.length = data.len() as u16;
        true
    }

    /// Encode to byte buffer (zero-allocation, writes to provided buffer)
    pub fn encode_to_buffer(&self, buffer: &mut [u8]) -> Option<usize> {
        let total_size = self.total_size();
        if buffer.len() < total_size {
            return None;
        }

        // Write header
        buffer[0] = self.header.message_type;
        buffer[1..5].copy_from_slice(&self.header.sequence.to_be_bytes());
        buffer[5..7].copy_from_slice(&self.header.length.to_be_bytes());

        // Write payload
        buffer[7..7 + self.payload_len as usize]
            .copy_from_slice(&self.payload[..self.payload_len as usize]);

        Some(total_size)
    }

    /// Get total encoded size
    pub fn total_size(&self) -> usize {
        7 + self.payload_len as usize // header (7 bytes) + payload
    }

    /// Get payload as slice (zero-copy)
    pub fn payload_slice(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }
}

/// Zero-allocation translator from Iridium SBD to ASTS Protobuf.
/// Zero-size type — all state lives in GLOBAL_SEQUENCE.
pub struct ZeroCopyTranslator;

impl ZeroCopyTranslator {
    pub fn new() -> Self {
        Self
    }

    /// Translate Iridium SBD to ASTS Protobuf (zero-allocation)
    /// Writes directly to output buffer, no heap allocations
    pub fn translate(
        &mut self,
        iridium_msg: &crate::iridium_sbd::IridiumSBDMessage,
        output_buffer: &mut [u8],
    ) -> Option<usize> {
        let seq = GLOBAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut asts_msg = ASTSProtobufMessage::new(0x01, seq);

        if !asts_msg.set_payload(iridium_msg.payload_slice()) {
            return None;
        }

        asts_msg.encode_to_buffer(output_buffer)
    }

    /// Translate with custom message type
    pub fn translate_with_type(
        &mut self,
        iridium_msg: &crate::iridium_sbd::IridiumSBDMessage,
        message_type: u8,
        output_buffer: &mut [u8],
    ) -> Option<usize> {
        let seq = GLOBAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut asts_msg = ASTSProtobufMessage::new(message_type, seq);

        if !asts_msg.set_payload(iridium_msg.payload_slice()) {
            return None;
        }

        asts_msg.encode_to_buffer(output_buffer)
    }
}

impl Default for ZeroCopyTranslator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iridium_sbd::IridiumSBDMessage;

    #[test]
    fn test_zero_copy_translation() {
        let iridium_data: Vec<u8> = vec![
            0x01, // protocol
            0x00, 0x05, // length = 5
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
            0x00, 0x00, // checksum
        ];

        let iridium_msg = IridiumSBDMessage::parse(&iridium_data).unwrap();
        let mut translator = ZeroCopyTranslator::new();
        let mut output_buffer = [0u8; 2048];

        let size = translator
            .translate(&iridium_msg, &mut output_buffer)
            .unwrap();
        assert!(size > 0);
        assert_eq!(output_buffer[0], 0x01); // message type
    }

    #[test]
    fn test_payload_too_large() {
        let mut asts_msg = ASTSProtobufMessage::new(0x01, 0);
        let large_payload = vec![0u8; 1025]; // Too large
        assert!(!asts_msg.set_payload(&large_payload));
    }
}
