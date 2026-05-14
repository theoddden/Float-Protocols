//! Lifetime-safe translation layer to solve "Slice-of-Death" problem
//!
//! Provides owned data alternatives to zero-allocation slices to improve DX
//! while maintaining performance through reference-counted Bytes.

use bytes::Bytes;

/// Lifetime-safe translation result with owned data
#[derive(Debug, Clone)]
pub struct SafeTranslationResult {
    pub data: Bytes,
    pub size: usize,
}

impl SafeTranslationResult {
    pub fn new(data: Bytes) -> Self {
        let size = data.len();
        Self { data, size }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

/// Arena allocator for buffer reuse with safe lifetime management
pub struct TranslationArena {
    buffers: Vec<Vec<u8>>,
    next_index: usize,
    buffer_size: usize,
}

impl TranslationArena {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let buffers = (0..pool_size).map(|_| vec![0u8; buffer_size]).collect();

        Self {
            buffers,
            next_index: 0,
            buffer_size,
        }
    }

    /// Get a buffer from the arena (safe lifetime management)
    pub fn get_buffer(&mut self) -> &mut Vec<u8> {
        let index = self.next_index % self.buffers.len();
        self.next_index += 1;
        &mut self.buffers[index]
    }

    /// Get buffer at specific index (for explicit control)
    pub fn get_buffer_at(&mut self, index: usize) -> &mut Vec<u8> {
        let actual_index = index % self.buffers.len();
        &mut self.buffers[actual_index]
    }

    /// Clone buffer to owned Bytes (safe for return values)
    pub fn clone_to_bytes(&self, buffer: &[u8], len: usize) -> Bytes {
        Bytes::copy_from_slice(&buffer[..len])
    }

    /// Clone buffer at specific index to owned Bytes
    pub fn clone_to_bytes_at(&self, index: usize, len: usize) -> Bytes {
        let buffer = &self.buffers[index % self.buffers.len()];
        Bytes::copy_from_slice(&buffer[..len])
    }
}

/// Hybrid translator: zero-allocation for hot path, safe for DX
pub struct HybridTranslator {
    arena: TranslationArena,
}

impl HybridTranslator {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        Self {
            arena: TranslationArena::new(pool_size, buffer_size),
        }
    }

    /// Zero-allocation translation (returns slice, caller must manage lifetime)
    pub fn translate_zero_alloc(
        &mut self,
        iridium_msg: &crate::iridium_sbd::IridiumSBDMessage,
        output_buffer: &mut [u8],
    ) -> Option<usize> {
        let mut translator = crate::asts_protobuf::ZeroCopyTranslator::new();
        translator.translate(iridium_msg, output_buffer)
    }

    /// Lifetime-safe translation (returns owned Bytes)
    pub fn translate_safe(
        &mut self,
        iridium_msg: &crate::iridium_sbd::IridiumSBDMessage,
    ) -> Option<SafeTranslationResult> {
        let buffer_index = self.arena.next_index;
        let buffer = self.arena.get_buffer();
        let size = self.translate_zero_alloc(iridium_msg, buffer)?;
        let data = self.arena.clone_to_bytes_at(buffer_index, size);
        Some(SafeTranslationResult::new(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iridium_sbd::IridiumSBDMessage;

    #[test]
    fn test_safe_translation() {
        let iridium_data: Vec<u8> = vec![
            0x01, // protocol
            0x00, 0x05, // length = 5
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
            0x00, 0x00, // checksum
        ];

        let iridium_msg = IridiumSBDMessage::parse(&iridium_data).unwrap();
        let mut translator = HybridTranslator::new(8, 2048);

        let result = translator.translate_safe(&iridium_msg).unwrap();
        assert_eq!(result.size, 12); // 7 bytes header + 5 bytes payload
    }
}
