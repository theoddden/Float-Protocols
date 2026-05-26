//! Dynamic buffer management to solve Protobuf "Size Trap"
//!
//! Fixed-size stack buffers can overflow with unexpected payloads.
//! This module provides dynamic sizing with bounds checking and graceful degradation.
//! Uses Vec<u8> internally for safe, automatic memory management.

#[derive(Debug)]
pub enum BufferError {
    TooLarge { requested: usize, max: usize },
    AllocationFailed,
    InvalidSize,
}

/// Dynamic buffer with configurable maximum size
/// Uses Vec<u8> internally for safe memory management (no unsafe code)
pub struct DynamicBuffer {
    buffer: Vec<u8>,
    max_capacity: usize,
}

impl DynamicBuffer {
    pub fn new(initial_capacity: usize, max_capacity: usize) -> Result<Self, BufferError> {
        if initial_capacity > max_capacity {
            return Err(BufferError::TooLarge {
                requested: initial_capacity,
                max: max_capacity,
            });
        }

        Ok(Self {
            buffer: Vec::with_capacity(initial_capacity),
            max_capacity,
        })
    }

    /// Write data to buffer, growing if necessary (up to max_capacity)
    pub fn write(&mut self, data: &[u8]) -> Result<(), BufferError> {
        if data.len() > self.max_capacity {
            return Err(BufferError::TooLarge {
                requested: data.len(),
                max: self.max_capacity,
            });
        }

        // Grow buffer if needed
        if data.len() > self.buffer.capacity() {
            let new_cap = data.len().next_power_of_two().min(self.max_capacity);
            self.buffer.reserve(new_cap);
        }

        // Write data
        self.buffer.clear();
        self.buffer.extend_from_slice(data);

        Ok(())
    }

    /// Get buffer as slice
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Get buffer as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear buffer (doesn't deallocate)
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Reset buffer to initial capacity (deallocates if larger than initial)
    pub fn reset(&mut self, initial_capacity: usize) -> Result<(), BufferError> {
        if initial_capacity > self.max_capacity {
            return Err(BufferError::TooLarge {
                requested: initial_capacity,
                max: self.max_capacity,
            });
        }

        self.buffer = Vec::with_capacity(initial_capacity);
        Ok(())
    }
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferError::TooLarge { requested, max } => {
                write!(
                    f,
                    "Buffer too large: requested {} bytes, max {} bytes",
                    requested, max
                )
            }
            BufferError::AllocationFailed => write!(f, "Buffer allocation failed"),
            BufferError::InvalidSize => write!(f, "Invalid buffer size"),
        }
    }
}

impl std::error::Error for BufferError {}

/// Configurable buffer pool with dynamic sizing
pub struct DynamicBufferPool {
    buffers: Vec<DynamicBuffer>,
    max_capacity: usize,
    next_index: usize,
}

impl DynamicBufferPool {
    pub fn new(
        pool_size: usize,
        initial_capacity: usize,
        max_capacity: usize,
    ) -> Result<Self, BufferError> {
        let mut buffers = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            buffers.push(DynamicBuffer::new(initial_capacity, max_capacity)?);
        }

        Ok(Self {
            buffers,
            max_capacity,
            next_index: 0,
        })
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&mut self) -> Result<&mut DynamicBuffer, BufferError> {
        let index = self.next_index % self.buffers.len();
        self.next_index += 1;
        Ok(&mut self.buffers[index])
    }

    /// Get buffer at specific index
    pub fn get_buffer_at(&mut self, index: usize) -> Result<&mut DynamicBuffer, BufferError> {
        let actual_index = index % self.buffers.len();
        Ok(&mut self.buffers[actual_index])
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_buffer_growth() {
        let mut buffer = DynamicBuffer::new(10, 1000).unwrap();

        // Write small data
        buffer.write(&[1, 2, 3]).unwrap();
        assert_eq!(buffer.len(), 3);

        // Write larger data (should grow)
        let large_data = vec![0u8; 500];
        buffer.write(&large_data).unwrap();
        assert_eq!(buffer.len(), 500);
        assert!(buffer.capacity() >= 500);
    }

    #[test]
    fn test_buffer_too_large() {
        let mut buffer = DynamicBuffer::new(10, 100).unwrap();

        let too_large = vec![0u8; 200];
        let result = buffer.write(&too_large);

        assert!(matches!(result, Err(BufferError::TooLarge { .. })));
    }

    #[test]
    fn test_buffer_pool() {
        let mut pool = DynamicBufferPool::new(4, 10, 1000).unwrap();

        let buffer = pool.get_buffer().unwrap();
        buffer.write(&[1, 2, 3]).unwrap();

        assert_eq!(buffer.len(), 3);
    }
}
