//! Dynamic buffer management to solve Protobuf "Size Trap"
//! 
//! Fixed-size stack buffers can overflow with unexpected payloads.
//! This module provides dynamic sizing with bounds checking and graceful degradation.

use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

#[derive(Debug)]
pub enum BufferError {
    TooLarge { requested: usize, max: usize },
    AllocationFailed,
    InvalidSize,
}

/// Dynamic buffer with configurable maximum size
pub struct DynamicBuffer {
    ptr: Option<NonNull<u8>>,
    capacity: usize,
    size: usize,
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

        let ptr = if initial_capacity == 0 {
            None
        } else {
            let layout = Layout::array::<u8>(initial_capacity)
                .map_err(|_| BufferError::InvalidSize)?;
            
            unsafe {
                let ptr = alloc(layout);
                if ptr.is_null() {
                    return Err(BufferError::AllocationFailed);
                }
                NonNull::new(ptr as *mut u8)
            }
        };

        Ok(Self {
            ptr,
            capacity: initial_capacity,
            size: 0,
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
        if data.len() > self.capacity {
            self.grow(data.len())?;
        }

        // Write data
        unsafe {
            if let Some(ptr) = self.ptr {
                std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
            }
        }

        self.size = data.len();
        Ok(())
    }

    /// Grow buffer to at least new_capacity
    fn grow(&mut self, new_capacity: usize) -> Result<(), BufferError> {
        if new_capacity > self.max_capacity {
            return Err(BufferError::TooLarge {
                requested: new_capacity,
                max: self.max_capacity,
            });
        }

        // Calculate new capacity (power of 2 for efficiency)
        let new_cap = new_capacity.next_power_of_two().min(self.max_capacity);

        unsafe {
            let new_layout = Layout::array::<u8>(new_cap)
                .map_err(|_| BufferError::InvalidSize)?;

            let new_ptr = if let Some(old_ptr) = self.ptr {
                let old_layout = Layout::array::<u8>(self.capacity)
                    .map_err(|_| BufferError::InvalidSize)?;
                
                let ptr = realloc(old_ptr.as_ptr() as *mut u8, old_layout, new_layout.size);
                if ptr.is_null() {
                    return Err(BufferError::AllocationFailed);
                }
                NonNull::new(ptr as *mut u8)
            } else {
                let ptr = alloc(new_layout);
                if ptr.is_null() {
                    return Err(BufferError::AllocationFailed);
                }
                NonNull::new(ptr as *mut u8)
            };

            self.ptr = new_ptr;
            self.capacity = new_cap;
        }

        Ok(())
    }

    /// Get buffer as slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            if let Some(ptr) = self.ptr {
                std::slice::from_raw_parts(ptr.as_ptr(), self.size)
            } else {
                &[]
            }
        }
    }

    /// Get buffer as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            if let Some(ptr) = self.ptr {
                std::slice::from_raw_parts_mut(ptr.as_ptr(), self.size)
            } else {
                &mut []
            }
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Clear buffer (doesn't deallocate)
    pub fn clear(&mut self) {
        self.size = 0;
    }

    /// Reset buffer to initial capacity (deallocates if larger than initial)
    pub fn reset(&mut self, initial_capacity: usize) -> Result<(), BufferError> {
        if initial_capacity > self.max_capacity {
            return Err(BufferError::TooLarge {
                requested: initial_capacity,
                max: self.max_capacity,
            });
        }

        unsafe {
            if let Some(ptr) = self.ptr {
                let layout = Layout::array::<u8>(self.capacity)
                    .map_err(|_| BufferError::InvalidSize)?;
                dealloc(ptr.as_ptr() as *mut u8, layout);
            }

            self.ptr = None;
            self.capacity = initial_capacity;
            self.size = 0;
        }

        Ok(())
    }
}

impl Drop for DynamicBuffer {
    fn drop(&mut self) {
        unsafe {
            if let Some(ptr) = self.ptr {
                let layout = Layout::array::<u8>(self.capacity)
                    .map_err(|_| ())
                    .unwrap(); // We know the layout is valid
                dealloc(ptr.as_ptr() as *mut u8, layout);
            }
        }
    }
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferError::TooLarge { requested, max } => {
                write!(f, "Buffer too large: requested {} bytes, max {} bytes", requested, max)
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
    pub fn new(pool_size: usize, initial_capacity: usize, max_capacity: usize) -> Result<Self, BufferError> {
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
